"""
Photon Skills — agent capability definitions for the clingrid pipeline.

Each pipeline agent exposes its capability as a Photon-compatible skill.
Skills define: what data the agent accepts, what it produces, and how
to invoke it. This follows the Photon agent skills standard
(https://github.com/photon-hq/skills).

Additionally, this module handles pipeline completion notifications
via Photon's iMessage integration — hospitals receive an iMessage when
their dataset has been harmonized and is ready for marketplace listing.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable

log = logging.getLogger(__name__)

PHOTON_API_KEY = os.getenv("PHOTON_API_KEY", "")


# ── Skill definition dataclass ────────────────────────────────────────────────

@dataclass
class Skill:
    """
    A Photon-compatible agent skill definition.

    Describes what a pipeline agent can do, what inputs it needs,
    and what outputs it produces.
    """
    name:        str
    version:     str
    description: str
    inputs:      list[dict]
    outputs:     list[dict]
    handler:     Callable | None = field(default=None, repr=False)

    def invoke(self, **kwargs) -> Any:
        if self.handler is None:
            raise NotImplementedError(f"Skill '{self.name}' has no handler registered")
        log.info("[Photon] Invoking skill: %s v%s", self.name, self.version)
        return self.handler(**kwargs)

    def to_dict(self) -> dict:
        return {
            "name":        self.name,
            "version":     self.version,
            "description": self.description,
            "inputs":      self.inputs,
            "outputs":     self.outputs,
        }


# ── Skill registry ────────────────────────────────────────────────────────────

_REGISTRY: dict[str, Skill] = {}


def register(skill: Skill):
    _REGISTRY[skill.name] = skill
    log.debug("[Photon] Registered skill: %s", skill.name)


def get_skill(name: str) -> Skill:
    if name not in _REGISTRY:
        raise KeyError(f"Photon skill '{name}' not found in registry")
    return _REGISTRY[name]


def list_skills() -> list[dict]:
    return [s.to_dict() for s in _REGISTRY.values()]


# ── clingrid skill definitions ────────────────────────────────────────────────

INGEST_CLINICAL_SKILL = Skill(
    name        = "ingest_clinical",
    version     = "1.0.0",
    description = (
        "Ingests raw South Asian clinical records (TXT clinical notes, "
        "CSV lab results, VCF genomic variants) and returns structured "
        "patient records keyed by patient_id. Handles Bengali, Hindi, "
        "and English in mixed-language documents."
    ),
    inputs  = [
        {"name": "data_dir",   "type": "path",   "required": True,  "description": "Directory containing patient files"},
        {"name": "formats",    "type": "list",   "required": False, "description": "Accepted formats: TXT, CSV, VCF (default: all)"},
        {"name": "languages",  "type": "list",   "required": False, "description": "Expected languages for validation"},
    ],
    outputs = [
        {"name": "patients",   "type": "dict",   "description": "patient_id → {sources: [...]}"},
        {"name": "file_count", "type": "int",    "description": "Total files ingested"},
        {"name": "languages",  "type": "list",   "description": "Detected languages in corpus"},
    ],
)

MODALITY_SCORE_SKILL = Skill(
    name        = "score_modality_completeness",
    version     = "1.0.0",
    description = (
        "Scores data completeness for each patient across three modalities: "
        "clinical notes (40%), lab results (40%), genomic variants (20%). "
        "Uses HydraDB semantic recall to identify which entity types are "
        "inferrable from partial data."
    ),
    inputs  = [
        {"name": "patients",     "type": "dict",  "required": True, "description": "patient_id → sources dict"},
        {"name": "hydra_client", "type": "object","required": False, "description": "HydraDB client for context recall"},
    ],
    outputs = [
        {"name": "scores",       "type": "dict",  "description": "patient_id → completeness score (0–1)"},
        {"name": "gaps",         "type": "list",  "description": "List of {patient_id, missing_modality} dicts"},
        {"name": "avg_score",    "type": "float", "description": "Corpus-level average completeness"},
    ],
)

HARMONIZE_CLINICAL_SKILL = Skill(
    name        = "harmonize_clinical",
    version     = "2.0.0",
    description = (
        "Maps Bengali/Hindi/English clinical entities to ICD-10 codes and "
        "OMOP CDM v5.4 concept IDs using GMI Cloud inference (DeepSeek-R1). "
        "Augments each LLM call with relevant OMOP concepts from HydraDB "
        "semantic recall, reducing hallucination and improving mapping accuracy."
    ),
    inputs  = [
        {"name": "patients",     "type": "dict",   "required": True,  "description": "Ingested patient records"},
        {"name": "hydra_client", "type": "object", "required": False, "description": "HydraDB client for OMOP recall"},
        {"name": "model",        "type": "str",    "required": False, "description": "GMI Cloud model ID (default: DeepSeek-R1)"},
    ],
    outputs = [
        {"name": "harmonized",   "type": "dict",   "description": "patient_id → harmonized entity dict"},
        {"name": "entities_total","type": "int",   "description": "Total entities mapped"},
        {"name": "flagged_count","type": "int",    "description": "Entities with confidence < 0.85"},
    ],
)

VALIDATE_MAPPINGS_SKILL = Skill(
    name        = "validate_omop_mappings",
    version     = "1.0.0",
    description = (
        "Second-pass validation of all low-confidence (< 0.85) entity mappings. "
        "Orchestrated as a Dify workflow node with GMI Cloud inference. "
        "Applies KDIGO 2022, ICD-10-CM 2024, and OMOP Athena reference standards. "
        "Produces a complete audit trail per patient."
    ),
    inputs  = [
        {"name": "harmonized", "type": "dict", "required": True, "description": "Output of harmonize_clinical skill"},
    ],
    outputs = [
        {"name": "validated",    "type": "dict",  "description": "patient_id → validated entity dict + audit"},
        {"name": "corrections",  "type": "int",   "description": "Total corrections applied"},
        {"name": "conf_before",  "type": "float", "description": "Avg confidence pre-validation"},
        {"name": "conf_after",   "type": "float", "description": "Avg confidence post-validation"},
    ],
)

WRITE_OMOP_SKILL = Skill(
    name        = "write_omop_output",
    version     = "1.0.0",
    description = (
        "Writes OMOP CDM v5.4 Parquet tables (person, condition_occurrence, "
        "drug_exposure, measurement) and a JSON provenance audit trail. "
        "Results are persisted to the HydraDB run tenant for marketplace listing. "
        "HydraDB tenant is then discarded — zero persistent patient data."
    ),
    inputs  = [
        {"name": "validated",    "type": "dict",  "required": True, "description": "Validated harmonized records"},
        {"name": "hydra_client", "type": "object","required": False,"description": "HydraDB client for persistence"},
        {"name": "output_dir",   "type": "path",  "required": False,"description": "Local output directory"},
    ],
    outputs = [
        {"name": "parquet_paths","type": "list",  "description": "Paths to written Parquet files"},
        {"name": "provenance",   "type": "dict",  "description": "Pipeline audit trail"},
        {"name": "row_counts",   "type": "dict",  "description": "Row counts per OMOP table"},
    ],
)

# Register all skills at import time
for _skill in [
    INGEST_CLINICAL_SKILL,
    MODALITY_SCORE_SKILL,
    HARMONIZE_CLINICAL_SKILL,
    VALIDATE_MAPPINGS_SKILL,
    WRITE_OMOP_SKILL,
]:
    register(_skill)


# ── Photon notification (iMessage on pipeline completion) ─────────────────────

def notify_hospital_completion(
    hospital_contact: str,
    hospital_name:    str,
    patients:         int,
    entities:         int,
    corrections:      int,
    omop_quality:     float,
    dataset_id:       str,
) -> bool:
    """
    Send an iMessage to the hospital contact when the pipeline completes,
    using Photon's iMessage integration.

    Requires PHOTON_API_KEY to be set. In dev mode, logs the message only.
    """
    message = (
        f"✅ clingrid | {hospital_name}\n"
        f"Your dataset is ready for marketplace listing.\n\n"
        f"  Patients:   {patients}\n"
        f"  Entities:   {entities} mapped\n"
        f"  Corrections:{corrections} applied\n"
        f"  OMOP score: {omop_quality:.1%}\n"
        f"  Dataset ID: {dataset_id}\n\n"
        f"Your estimated earnings: $4,800–$6,200/year\n"
        f"View at clingrid.health/datasets/{dataset_id}"
    )

    if not PHOTON_API_KEY:
        log.info(
            "[Photon] Notification (dev mode — no iMessage sent)\nTo: %s\n%s",
            hospital_contact, message
        )
        return False

    try:
        import httpx
        resp = httpx.post(
            "https://api.photon.so/v1/messages/send",
            headers={"Authorization": f"Bearer {PHOTON_API_KEY}"},
            json={
                "to":      hospital_contact,
                "text":    message,
                "service": "iMessage",
            },
            timeout=10.0,
        )
        resp.raise_for_status()
        log.info("[Photon] iMessage sent to %s", hospital_contact)
        return True
    except Exception as exc:
        log.warning("[Photon] iMessage notification failed: %s", exc)
        return False
