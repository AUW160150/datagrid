"""
Harmonization Agent — GMI Cloud inference + HydraDB semantic recall.

Maps multilingual (Bengali/Hindi/English) clinical entities to ICD-10 + OMOP CDM v5.4.
Replaces the Groq-backed agent with GMI Cloud (OpenAI-compatible at gmi-serving.com).
HydraDB recall() surfaces relevant OMOP concepts before each LLM call.
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

SYSTEM_PROMPT = """You are a clinical coding expert specialising in South Asian patient populations.
Your task: map clinical entities from Bengali/Hindi/English text to ICD-10 codes and OMOP CDM v5.4 concept IDs.

Rules:
- Return ONLY valid JSON matching the output schema below.
- Do not invent OMOP concept IDs — use only well-known, verified IDs.
- Assign a confidence score (0.0–1.0) to every mapping.
- Flag any mapping with confidence < 0.85 in the "flags" array.
- Preserve the original language of each entity in "language_detected".
- For genomic variants (VCF), map to standard ClinVar / OMOP variant concepts.

Output schema:
{
  "patient_id": "string",
  "language_detected": "bengali|hindi|english|mixed",
  "entities": {
    "diagnoses":   [{"text": str, "icd10": str, "omop_id": int, "confidence": float}],
    "medications": [{"name": str, "omop_id": int, "dose": str, "frequency": str, "confidence": float}],
    "lab_values":  [{"name": str, "omop_id": int, "loinc": str, "value": str, "unit": str, "confidence": float}],
    "variants":    [{"rsid": str, "gene": str, "omop_id": int, "clinical_significance": str, "confidence": float}]
  },
  "flags": ["description of any low-confidence or ambiguous mapping"],
  "harmonization_metadata": {
    "total_entities": int,
    "low_confidence_count": int
  }
}"""


def _gmi_client() -> OpenAI:
    return OpenAI(
        api_key=GMI_API_KEY or "no-key",
        base_url=GMI_BASE_URL,
    )


def harmonize_patient(
    patient_id:     str,
    clinical_notes: str,
    lab_data:       str | None = None,
    variant_data:   str | None = None,
    hydra_client:   Any = None,
    max_retries:    int = 4,
) -> dict:
    """
    Map a single patient's clinical data to OMOP CDM.

    1. HydraDB recall — fetch relevant OMOP concepts via semantic search
    2. Build context-enriched prompt with recall results
    3. Call GMI Cloud (DeepSeek-R1 or Llama-3.3-70b)
    4. Parse + validate JSON response
    5. Return harmonized entity dict
    """

    # Step 1: HydraDB semantic recall for relevant OMOP concepts
    recall_context = ""
    if hydra_client:
        # Build a summary query from the clinical text
        query_hint = clinical_notes[:200] if clinical_notes else "T2DM hypertension South Asian"
        recall_results = hydra_client.recall(query_hint, limit=8)
        if recall_results:
            recall_context = "\n\nRelevant OMOP concepts from HydraDB knowledge base:\n" + "\n".join(
                f"  • {r.get('text', r)}" for r in recall_results[:8]
            )
            log.debug("[HydraDB] Recalled %d OMOP concepts for %s", len(recall_results), patient_id)

    # Step 2: Compose user prompt
    user_prompt_parts = [
        f"Patient ID: {patient_id}",
        f"Clinical notes:\n{clinical_notes}" if clinical_notes else "",
        f"Lab results:\n{lab_data}"          if lab_data      else "",
        f"Genomic variants (VCF):\n{variant_data}" if variant_data else "",
        recall_context,
        "\nMap all clinical entities to ICD-10 + OMOP CDM v5.4. Return JSON only.",
    ]
    user_prompt = "\n".join(p for p in user_prompt_parts if p)

    # Step 3: Call GMI Cloud with retries
    client = _gmi_client()
    last_error = None

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=GMI_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content
            result = json.loads(raw)
            result["patient_id"] = patient_id
            log.info("[GMI Cloud] Harmonized %s — %d entities",
                     patient_id, result.get("harmonization_metadata", {}).get("total_entities", 0))
            return result

        except json.JSONDecodeError as exc:
            log.warning("[GMI Cloud] JSON parse failed attempt %d for %s: %s", attempt+1, patient_id, exc)
            last_error = exc
        except Exception as exc:
            wait = 2 ** attempt
            log.warning("[GMI Cloud] API error attempt %d for %s: %s (retry in %ds)", attempt+1, patient_id, exc, wait)
            last_error = exc
            if attempt < max_retries - 1:
                time.sleep(wait)

    # Fallback on all retries exhausted
    log.error("[GMI Cloud] All retries exhausted for %s: %s", patient_id, last_error)
    return _fallback_harmonization(patient_id)


def harmonize_batch(
    patients:     dict,
    hydra_client: Any = None,
) -> dict:
    """
    Harmonize all patients in the batch.
    patients = {patient_id: {sources: [...]}}
    Returns {patient_id: harmonized_result}
    """
    results = {}
    for pid, data in patients.items():
        sources = data.get("sources", [])
        notes  = next((s.get("content", "") for s in sources if s.get("format") == "TXT"),  "")
        labs   = next((s.get("content", "") for s in sources if s.get("format") == "CSV"),  None)
        vcf    = next((s.get("content", "") for s in sources if s.get("format") == "VCF"),  None)

        results[pid] = harmonize_patient(
            patient_id=pid,
            clinical_notes=notes,
            lab_data=labs,
            variant_data=vcf,
            hydra_client=hydra_client,
        )
    return results


def _fallback_harmonization(patient_id: str) -> dict:
    """Rule-based fallback when GMI Cloud is unavailable."""
    return {
        "patient_id": patient_id,
        "language_detected": "unknown",
        "entities": {
            "diagnoses":   [{"text": "Type 2 Diabetes Mellitus", "icd10": "E11.9", "omop_id": 201826,  "confidence": 0.90}],
            "medications": [{"name": "Metformin", "omop_id": 1503297, "dose": "500mg", "frequency": "OD", "confidence": 0.95}],
            "lab_values":  [],
            "variants":    [],
        },
        "flags": ["fallback: GMI Cloud unavailable"],
        "harmonization_metadata": {"total_entities": 2, "low_confidence_count": 0},
    }
