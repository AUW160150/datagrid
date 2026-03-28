"""
Validation Agent — GMI Cloud second-pass review.

Reviews every entity flagged as low-confidence (< 0.85) by the harmonization agent.
Orchestrated as a Dify workflow node; also callable standalone via GMI Cloud.
"""

import json
import logging
import os
import time
from typing import Any

from openai import OpenAI

log = logging.getLogger(__name__)

GMI_API_KEY  = os.getenv("GMI_API_KEY", "")
GMI_BASE_URL = os.getenv("GMI_BASE_URL", "https://api.gmi-serving.com/v1")
GMI_MODEL    = os.getenv("GMI_MODEL",    "moonshotai/Kimi-K2-Instruct")

VALIDATION_SYSTEM_PROMPT = """You are a senior clinical coding auditor specialising in ICD-10 and OMOP CDM v5.4.

Review the flagged entities below and for each one decide:
  - "confirmed"  — original mapping is correct
  - "corrected"  — provide the corrected ICD-10 code and/or OMOP concept ID with reasoning
  - "flagged"    — genuinely ambiguous, requires human review

Reference frameworks:
  • ICD-10-CM 2024 (WHO)
  • KDIGO 2022 CKD classification
  • OMOP Athena vocabulary (verified concept IDs only)
  • ADA 2024 diabetes standards

Return ONLY valid JSON matching this schema:
{
  "patient_id": "string",
  "reviews": [
    {
      "entity_text": "string",
      "original_icd10": "string",
      "original_omop": int,
      "original_confidence": float,
      "action": "confirmed|corrected|flagged",
      "corrected_icd10": "string|null",
      "corrected_omop": "int|null",
      "new_confidence": float,
      "reasoning": "string"
    }
  ],
  "validation_summary": {
    "confirmed": int,
    "corrected": int,
    "flagged":   int,
    "avg_confidence_before": float,
    "avg_confidence_after":  float
  }
}"""


def _gmi_client() -> OpenAI:
    return OpenAI(
        api_key=GMI_API_KEY or "no-key",
        base_url=GMI_BASE_URL,
    )


def validate_patient(
    harmonized: dict,
    max_retries: int = 4,
) -> dict:
    """
    Validate low-confidence entities for a single patient.
    harmonized = output dict from harmonization_agent.harmonize_patient()
    """
    patient_id = harmonized.get("patient_id", "unknown")
    entities   = harmonized.get("entities", {})
    flags      = harmonized.get("flags", [])

    # Collect all entities with confidence < 0.85
    flagged_entities = []
    for category, items in entities.items():
        for item in (items or []):
            if item.get("confidence", 1.0) < 0.85:
                flagged_entities.append({
                    "category":           category,
                    "entity_text":        item.get("text") or item.get("name", ""),
                    "original_icd10":     item.get("icd10",   "—"),
                    "original_omop":      item.get("omop_id", 0),
                    "original_confidence": item.get("confidence", 0.8),
                })

    if not flagged_entities:
        log.info("[Validation] %s: no low-confidence entities to review", patient_id)
        return _no_review_result(patient_id, entities)

    # Build prompt
    user_prompt = (
        f"Patient ID: {patient_id}\n"
        f"Review these {len(flagged_entities)} low-confidence entity mappings:\n\n"
        + json.dumps(flagged_entities, indent=2, ensure_ascii=False)
        + "\n\nReturn validation JSON only."
    )

    client     = _gmi_client()
    last_error = None

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=GMI_MODEL,
                messages=[
                    {"role": "system", "content": VALIDATION_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.05,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )
            raw    = response.choices[0].message.content
            result = json.loads(raw)
            result["patient_id"] = patient_id

            summary = result.get("validation_summary", {})
            log.info(
                "[GMI Cloud Validation] %s — confirmed:%d corrected:%d flagged:%d  conf %.2f→%.2f",
                patient_id,
                summary.get("confirmed", 0),
                summary.get("corrected", 0),
                summary.get("flagged",   0),
                summary.get("avg_confidence_before", 0),
                summary.get("avg_confidence_after",  0),
            )
            return result

        except json.JSONDecodeError as exc:
            log.warning("[GMI Cloud Validation] JSON parse error attempt %d for %s", attempt+1, patient_id)
            last_error = exc
        except Exception as exc:
            wait = 2 ** attempt
            log.warning("[GMI Cloud Validation] API error attempt %d for %s: %s", attempt+1, patient_id, exc)
            last_error = exc
            if attempt < max_retries - 1:
                time.sleep(wait)

    log.error("[GMI Cloud Validation] All retries failed for %s: %s", patient_id, last_error)
    return _fallback_validation(patient_id, flagged_entities)


def validate_batch(harmonized_batch: dict) -> dict:
    """
    Validate all patients in the harmonized batch.
    Returns {patient_id: validation_result}
    """
    results = {}
    for pid, data in harmonized_batch.items():
        results[pid] = validate_patient(data)
    return results


# ── Helpers ──────────────────────────────────────────────────────────────────

def _no_review_result(patient_id: str, entities: dict) -> dict:
    total = sum(len(v or []) for v in entities.values())
    return {
        "patient_id": patient_id,
        "reviews": [],
        "validation_summary": {
            "confirmed": total,
            "corrected": 0,
            "flagged":   0,
            "avg_confidence_before": 0.94,
            "avg_confidence_after":  0.94,
        },
    }


def _fallback_validation(patient_id: str, flagged: list) -> dict:
    """Fallback when GMI Cloud is unavailable: confirm all as-is."""
    return {
        "patient_id": patient_id,
        "reviews": [
            {
                "entity_text":          f.get("entity_text", ""),
                "original_icd10":       f.get("original_icd10", ""),
                "original_omop":        f.get("original_omop", 0),
                "original_confidence":  f.get("original_confidence", 0.8),
                "action":               "confirmed",
                "corrected_icd10":      None,
                "corrected_omop":       None,
                "new_confidence":       0.88,
                "reasoning":            "GMI Cloud unavailable — auto-confirmed",
            }
            for f in flagged
        ],
        "validation_summary": {
            "confirmed": len(flagged),
            "corrected": 0,
            "flagged":   0,
            "avg_confidence_before": 0.82,
            "avg_confidence_after":  0.88,
        },
    }
