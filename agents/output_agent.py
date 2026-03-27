"""
datagrid — Output Agent
Writes OMOP CDM v5.4 Parquet tables + provenance to Ghost DB (and local files).
Auth0 M2M: read:validated + write:omop scopes.
"""

import os
import sys
import json
import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from auth.m2m import require_token

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")

RACE_CONCEPT_SOUTH_ASIAN = 44814660
ETHNICITY_NOT_HISPANIC   = 38003564
GENDER_MALE              = 8507
GENDER_FEMALE            = 8532
GENDER_UNKNOWN           = 0


def _gender_concept(sex_str):
    if not sex_str:
        return GENDER_UNKNOWN
    s = str(sex_str).lower()
    if s in ("male", "m", "पुरुष", "পুরুষ"):
        return GENDER_MALE
    if s in ("female", "f", "महिला", "মহিলা"):
        return GENDER_FEMALE
    return GENDER_UNKNOWN


def _patient_num(patient_id):
    try:
        return int(patient_id.replace("P", "").lstrip("0") or "0")
    except (ValueError, AttributeError):
        return 0


def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _best_entity(entity):
    if entity.get("validation_status") == "corrected" and entity.get("corrected_mapping"):
        c = entity["corrected_mapping"]
        return {
            "icd10_code":      c.get("icd10_code")      or entity.get("icd10_code"),
            "omop_concept_id": c.get("omop_concept_id") or entity.get("omop_concept_id"),
            "standard_term":   c.get("standardized_english_term") or entity.get("standardized_english_term"),
            "confidence":      c.get("confidence", entity.get("confidence", 0)),
        }
    return {
        "icd10_code":      entity.get("icd10_code"),
        "omop_concept_id": entity.get("omop_concept_id"),
        "standard_term":   entity.get("standardized_english_term"),
        "confidence":      entity.get("confidence", 0),
    }


# ---------------------------------------------------------------------------
# Table builders (unchanged logic from BioHarmonize)
# ---------------------------------------------------------------------------

def _build_person_table(validated_records):
    rows = []
    for pid, record in sorted(validated_records.items()):
        demo    = record.get("entities", {}).get("demographics", {})
        age_ent = demo.get("age") or {}
        sex_ent = demo.get("sex") or {}
        age_txt = age_ent.get("standardized_english_term", "")
        sex_txt = sex_ent.get("standardized_english_term", "")
        try:
            age_val = int("".join(filter(str.isdigit, age_txt.split()[0])))
        except (ValueError, IndexError):
            age_val = None
        rows.append({
            "person_id":            _patient_num(pid),
            "person_source_value":  pid,
            "gender_concept_id":    _gender_concept(sex_txt),
            "gender_source_value":  sex_ent.get("original_text", ""),
            "year_of_birth":        (2024 - age_val) if age_val else None,
            "race_concept_id":      RACE_CONCEPT_SOUTH_ASIAN,
            "race_source_value":    "South Asian",
            "ethnicity_concept_id": ETHNICITY_NOT_HISPANIC,
            "language_detected":    record.get("language_detected", "unknown"),
        })
    return pd.DataFrame(rows)


def _build_condition_table(validated_records):
    rows, occ_id = [], 1
    for pid, record in sorted(validated_records.items()):
        for entity in record.get("entities", {}).get("diagnoses", []):
            best = _best_entity(entity)
            rows.append({
                "condition_occurrence_id": occ_id,
                "person_id":              _patient_num(pid),
                "condition_concept_id":   _safe_int(best["omop_concept_id"]) or 0,
                "condition_start_date":   "2024-01-15",
                "condition_source_value": entity.get("original_text", ""),
                "icd10_code":             best["icd10_code"],
                "standardized_term":      best["standard_term"],
                "confidence":             best["confidence"],
                "validation_status":      entity.get("validation_status", "not_reviewed"),
                "flag":                   entity.get("flag"),
            })
            occ_id += 1
    return pd.DataFrame(rows)


def _build_drug_table(validated_records):
    rows, exp_id = [], 1
    for pid, record in sorted(validated_records.items()):
        for entity in record.get("entities", {}).get("medications", []):
            best = _best_entity(entity)
            rows.append({
                "drug_exposure_id":         exp_id,
                "person_id":                _patient_num(pid),
                "drug_concept_id":          _safe_int(best["omop_concept_id"]) or 0,
                "drug_exposure_start_date": "2024-01-15",
                "drug_source_value":        entity.get("original_text", ""),
                "standardized_term":        best["standard_term"],
                "dose_value":               entity.get("dose"),
                "sig":                      entity.get("frequency"),
                "confidence":               best["confidence"],
                "validation_status":        entity.get("validation_status", "not_reviewed"),
                "flag":                     entity.get("flag"),
            })
            exp_id += 1
    return pd.DataFrame(rows)


def _build_measurement_table(validated_records):
    rows, meas_id = [], 1
    for pid, record in sorted(validated_records.items()):
        for cat in ("lab_values", "vitals"):
            for entity in record.get("entities", {}).get(cat, []):
                best = _best_entity(entity)
                rows.append({
                    "measurement_id":           meas_id,
                    "person_id":                _patient_num(pid),
                    "measurement_concept_id":   _safe_int(best["omop_concept_id"]) or 0,
                    "measurement_date":         "2024-01-15",
                    "measurement_source_value": entity.get("original_text", ""),
                    "standardized_term":        best["standard_term"],
                    "value_as_number":          _safe_float(entity.get("value")),
                    "unit_source_value":        entity.get("unit"),
                    "confidence":               best["confidence"],
                    "validation_status":        entity.get("validation_status", "not_reviewed"),
                    "flag":                     entity.get("flag"),
                    "category":                 cat,
                })
                meas_id += 1
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main output function
# ---------------------------------------------------------------------------

@require_token("output-agent")
def write_output(validated_records: dict, pipeline_meta: dict = None,
                 ghost_db=None, auth_token: str = "") -> dict:
    """
    Write OMOP Parquet tables locally + provenance to Ghost DB.
    ghost_db: GhostDB instance for this run (optional).
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pipeline_meta = pipeline_meta or {}

    print("  [Output] Building OMOP tables...")
    tables = {
        "person":               _build_person_table(validated_records),
        "condition_occurrence": _build_condition_table(validated_records),
        "drug_exposure":        _build_drug_table(validated_records),
        "measurement":          _build_measurement_table(validated_records),
    }

    written = {}
    for name, df in tables.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.parquet")
        df.to_parquet(path, index=False, engine="pyarrow")
        written[name] = {"path": path, "rows": len(df)}
        print(f"  [Output] {name}.parquet — {len(df)} rows")

    # Build provenance
    provenance = {
        "pipeline":          "datagrid",
        "version":           "1.0",
        "run_timestamp":     datetime.datetime.utcnow().isoformat() + "Z",
        "pipeline_metadata": pipeline_meta,
        "patients":          {},
    }
    for pid, record in sorted(validated_records.items()):
        provenance["patients"][pid] = {
            "patient_id":             pid,
            "language_detected":      record.get("language_detected"),
            "modality_assessment":    record.get("modality_assessment", {}),
            "harmonization_metadata": record.get("harmonization_metadata", {}),
            "validation_summary":     record.get("validation_summary", {}),
            "flags":                  record.get("flags", []),
        }

    # Write provenance to Ghost DB if available
    if ghost_db:
        ghost_db.write_provenance(provenance)
        print(f"  [Output] Provenance written to Ghost DB ({ghost_db.db_id})")

    # Also write local provenance JSON
    prov_dir  = os.path.join(OUTPUT_DIR, "provenance")
    os.makedirs(prov_dir, exist_ok=True)
    prov_path = os.path.join(OUTPUT_DIR, "pipeline_provenance.json")
    with open(prov_path, "w", encoding="utf-8") as f:
        json.dump(provenance, f, ensure_ascii=False, indent=2)
    print(f"  [Output] pipeline_provenance.json written")

    return {
        "tables":     written,
        "provenance": prov_path,
        "row_counts": {n: v["rows"] for n, v in written.items()},
    }
