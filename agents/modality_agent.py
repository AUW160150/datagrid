"""
datagrid — Missing Modality Detection Agent
Detects which clinical modalities are present and assesses data gaps.
Uses OverClaw call_llm for full trace visibility.
Auth0 M2M: read:records scope.
"""

import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from auth.m2m import require_token

try:
    from overclaw.core.tracer import call_llm
    _OVERCLAW = True
except ImportError:
    _OVERCLAW = False

MODEL = "gpt-4o-mini"

MODALITY_WEIGHTS = {
    "clinical_note":    0.40,
    "lab_results":      0.40,
    "genomic_variants": 0.20,
}

SYSTEM_PROMPT = """You are a clinical data quality specialist for South Asian multi-modal patient datasets.

Your job is to assess the impact of missing clinical modalities for a given patient
and suggest what can still be inferred from the data that IS present.

The three modalities are:
  1. clinical_note     — physician narrative: diagnoses, medications, vitals, history
  2. lab_results       — quantitative lab values: glucose, HbA1c, lipids, renal function
  3. genomic_variants  — VCF data: pharmacogenomic and risk variants (e.g. TCF7L2, PCSK9)

For each MISSING modality, assess:
  - impact: "low" | "medium" | "high"
  - what_can_be_inferred: specific statements about remaining partial evidence
  - what_is_lost: what clinical insight is absent without this modality
  - compensating_evidence: specific fields in present modalities that partially offset the gap

Be specific — cite actual biomarkers, values, or variant names where relevant.
Return ONLY valid JSON — no prose outside the JSON.

Output schema:
{
  "patient_id": "string",
  "completeness_score": float,
  "modality_assessments": {
    "<modality_name>": {
      "present": true/false,
      "impact": "low"|"medium"|"high"|null,
      "what_can_be_inferred": "string or null",
      "what_is_lost": "string or null",
      "compensating_evidence": "string or null"
    }
  },
  "overall_recommendation": "1-2 sentences",
  "harmonization_flags": ["list of specific flags"]
}"""


def _detect_modalities(patient_record: dict) -> dict:
    fmts = {s.get("_format") for s in patient_record.get("sources", [])}
    return {
        "clinical_note":    "text" in fmts,
        "lab_results":      "csv"  in fmts,
        "genomic_variants": "vcf"  in fmts,
    }


def _base_score(present: dict) -> float:
    return round(sum(MODALITY_WEIGHTS[m] for m, p in present.items() if p), 2)


def _summarise_data(patient_record: dict) -> str:
    lines = []
    for src in patient_record.get("sources", []):
        fmt = src.get("_format")
        if fmt == "text":
            text = src.get("raw_text", "")
            lines.append(f"CLINICAL NOTE ({src.get('language_hint', 'unknown')}):")
            lines.append(text[:600] + ("…" if len(text) > 600 else ""))
        elif fmt == "csv":
            headers = src.get("headers", [])
            records = src.get("records", [])
            ref     = src.get("reference_ranges", {})
            lines.append(f"LAB RESULTS ({src.get('source_file', '')}):")
            if records:
                for col in headers[:12]:
                    lines.append(f"  {col}: {records[0].get(col, '')} [ref: {ref.get(col, '')}]")
        elif fmt == "vcf":
            lines.append(f"GENOMIC VARIANTS ({src.get('variant_count', 0)} variants):")
            for v in src.get("variants", []):
                info = v.get("info", {})
                lines.append(f"  {v.get('rsid')} {info.get('GENE','')} PHENOTYPE={info.get('PHENOTYPE','')}")
    return "\n".join(lines)


def _parse_json(raw: str) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


def _call_model(system: str, user: str) -> str:
    if _OVERCLAW:
        return call_llm(
            model=MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ]
        )
    # Fallback: direct OpenAI call
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        max_tokens=2000,
    )
    return resp.choices[0].message.content


@require_token("modality-agent")
def assess_patient(patient_record: dict, verbose: bool = False, auth_token: str = "") -> dict:
    pid     = patient_record.get("patient_id", "UNKNOWN")
    present = _detect_modalities(patient_record)
    missing = [m for m, p in present.items() if not p]

    if not missing:
        assessment = {
            "patient_id":          pid,
            "completeness_score":  1.0,
            "modality_assessments": {
                m: {"present": True, "impact": None,
                    "what_can_be_inferred": None, "what_is_lost": None,
                    "compensating_evidence": None}
                for m in MODALITY_WEIGHTS
            },
            "overall_recommendation": "All modalities present. Proceed with full harmonization.",
            "harmonization_flags":    [],
            "_source": "rule_based",
        }
    else:
        if verbose:
            print(f"  [Modality]  {pid} — {len(missing)} missing, calling {MODEL}...")
        user_prompt = f"""Patient ID: {pid}
MODALITY STATUS:
  clinical_note    : {"PRESENT" if present["clinical_note"] else "ABSENT"}
  lab_results      : {"PRESENT" if present["lab_results"] else "ABSENT"}
  genomic_variants : {"PRESENT" if present["genomic_variants"] else "ABSENT"}

MISSING: {", ".join(missing)}

DATA AVAILABLE:
{_summarise_data(patient_record)}

Assess the impact of each missing modality. Return only JSON."""

        try:
            raw        = _call_model(SYSTEM_PROMPT, user_prompt)
            assessment = _parse_json(raw)
            assessment["_source"] = "overclaw" if _OVERCLAW else "openai"
            for m, p in present.items():
                assessment.setdefault("modality_assessments", {}).setdefault(m, {})["present"] = p
        except Exception as e:
            assessment = {
                "patient_id":         pid,
                "completeness_score": _base_score(present),
                "modality_assessments": {
                    m: {"present": p, "impact": "high" if not p else None,
                        "what_can_be_inferred": None, "what_is_lost": None,
                        "compensating_evidence": None}
                    for m, p in present.items()
                },
                "overall_recommendation": f"Assessment failed ({e}). Proceeding with partial data.",
                "harmonization_flags": [f"missing_{m}" for m in missing],
                "_source": "fallback",
            }

    assessment["patient_id"]        = pid
    assessment["missing_modalities"] = missing
    assessment["present_modalities"] = [m for m, p in present.items() if p]
    patient_record["modality_assessment"] = assessment

    if verbose:
        score = assessment.get("completeness_score", 0)
        print(f"  [Modality]  {pid} — score={score:.2f}  missing={missing or 'none'}")

    return patient_record


def assess_all(patient_records: dict, verbose: bool = True) -> dict:
    print(f"\n  Assessing {len(patient_records)} patients for modality completeness...")
    incomplete = 0
    for pid in sorted(patient_records.keys()):
        assess_patient(patient_records[pid], verbose=verbose)
        if patient_records[pid]["modality_assessment"]["missing_modalities"]:
            incomplete += 1

    scores = [
        patient_records[pid]["modality_assessment"].get("completeness_score", 0)
        for pid in patient_records
    ]
    avg = sum(scores) / len(scores) if scores else 0
    print(f"  Modality check complete — {incomplete}/{len(patient_records)} incomplete | avg score: {avg:.2f}")
    return patient_records
