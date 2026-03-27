"""
datagrid — Validation Agent
Second-pass clinical coding review on low-confidence entities.
Auth0 M2M: read:harmonized scope (read-only audit access).
Uses OverClaw call_llm for trace visibility.
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

MODEL      = "gpt-4o"
MAX_TOKENS = 8000

SYSTEM_PROMPT = """You are datagrid Validator — a second-opinion clinical coding agent.

You will receive a list of clinical entities that were flagged during harmonization
(confidence < 0.85 or flag != null). Your job is to independently review each mapping
and either CONFIRM it, CORRECT it, or escalate as FLAGGED.

=== VALIDATION RULES ===
1. Return ONLY valid JSON. No prose outside the JSON.
2. For each entity, decide:
   - "confirmed": original mapping is correct
   - "corrected": original has an error — provide corrected fields
   - "flagged": genuinely ambiguous
3. When correcting, provide only the changed fields plus validation_reasoning.
4. Do not invent OMOP concept IDs. If uncertain, set to null and flag.

=== OUTPUT SCHEMA ===
{
  "patient_id": "string",
  "validations": [
    {
      "category": "diagnoses|medications|vitals|lab_values|variants|demographics",
      "index_or_key": "integer or key string",
      "original_text": "string",
      "validation_status": "confirmed|corrected|flagged",
      "corrected_mapping": {
        "standardized_english_term": "string or null",
        "icd10_code": "string or null",
        "omop_concept_id": "string or null",
        "confidence": float,
        "flag": null
      },
      "validation_reasoning": "1-2 sentences"
    }
  ],
  "validation_metadata": {
    "entities_reviewed": integer,
    "confirmed": integer,
    "corrected": integer,
    "flagged": integer
  }
}"""


def _collect_entities(harmonized: dict) -> list:
    to_validate = []
    entities = harmonized.get("entities", {})

    for key, entity in entities.get("demographics", {}).items():
        if isinstance(entity, dict):
            if entity.get("confidence", 1.0) < 0.85 or entity.get("flag"):
                to_validate.append(("demographics", key, entity))

    for cat in ("diagnoses", "medications", "vitals", "lab_values", "variants"):
        for idx, entity in enumerate(entities.get(cat, [])):
            if isinstance(entity, dict):
                if entity.get("confidence", 1.0) < 0.85 or entity.get("flag"):
                    to_validate.append((cat, idx, entity))

    return to_validate


def _build_user_prompt(patient_id: str, entities: list) -> str:
    lines = [f"Patient ID: {patient_id}", "", "Entities requiring validation:"]
    for cat, idx, entity in entities:
        lines.append(f"\n[category={cat}  index_or_key={idx}]")
        lines.append(f"  original_text         : {entity.get('original_text', '')}")
        lines.append(f"  standardized_term     : {entity.get('standardized_english_term', '')}")
        lines.append(f"  icd10_code            : {entity.get('icd10_code', 'null')}")
        lines.append(f"  omop_concept_id       : {entity.get('omop_concept_id', 'null')}")
        lines.append(f"  confidence            : {entity.get('confidence', '?')}")
        lines.append(f"  flag                  : {entity.get('flag', 'null')}")
        lines.append(f"  reasoning             : {entity.get('reasoning', '')}")
    lines.append("\nReturn only the JSON object — no other text.")
    return "\n".join(lines)


def _apply_validations(harmonized: dict, validation_result: dict) -> dict:
    entities    = harmonized.get("entities", {})
    validations = validation_result.get("validations", [])

    for v in validations:
        cat      = v.get("category")
        idx      = v.get("index_or_key")
        status   = v.get("validation_status", "confirmed")
        corrected = v.get("corrected_mapping")
        reasoning = v.get("validation_reasoning", "")

        try:
            if cat == "demographics":
                entity = entities.get("demographics", {}).get(idx)
            else:
                entity = entities.get(cat, [])[int(idx)]
        except (KeyError, IndexError, TypeError, ValueError):
            continue

        if entity is None:
            continue

        entity["original_mapping"] = {
            "standardized_english_term": entity.get("standardized_english_term"),
            "icd10_code":               entity.get("icd10_code"),
            "omop_concept_id":          entity.get("omop_concept_id"),
            "confidence":               entity.get("confidence"),
            "flag":                     entity.get("flag"),
        }
        entity["validation_status"]    = status
        entity["validation_reasoning"] = reasoning

        if status == "corrected" and corrected:
            entity["corrected_mapping"] = corrected
            for field in ("standardized_english_term", "icd10_code",
                          "omop_concept_id", "confidence", "flag"):
                if field in corrected:
                    entity[field] = corrected[field]
        else:
            entity["corrected_mapping"] = None

    harmonized["validation_summary"] = validation_result.get("validation_metadata", {})
    return harmonized


def _parse_json(raw: str) -> dict:
    text = (raw or "").strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


def _call_model(user_prompt: str) -> str:
    if _OVERCLAW:
        return call_llm(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ]
        )
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_prompt},
        ],
        max_tokens=MAX_TOKENS,
    )
    return resp.choices[0].message.content


@require_token("validation-agent")
def validate_patient(harmonized: dict, verbose: bool = False, auth_token: str = "") -> dict:
    pid               = harmonized.get("patient_id", "UNKNOWN")
    entities_to_check = _collect_entities(harmonized)

    if not entities_to_check:
        harmonized["validation_summary"] = {
            "status": "skipped", "reason": "all confidence >= 0.85", "entities_reviewed": 0,
            "confirmed": 0, "corrected": 0, "flagged": 0,
        }
        if verbose:
            print(f"  [Validation]    {pid} — skipped (all high-confidence)")
        return harmonized

    if verbose:
        print(f"  [Validation]    {pid} — reviewing {len(entities_to_check)} entities via {'OverClaw' if _OVERCLAW else 'OpenAI'}...")

    user_prompt = _build_user_prompt(pid, entities_to_check)

    try:
        raw              = _call_model(user_prompt)
        validation_result = _parse_json(raw)
        result           = _apply_validations(harmonized, validation_result)
        if verbose:
            meta  = result.get("validation_summary", {})
            print(f"  [Validation]    {pid} — {meta.get('confirmed',0)} confirmed, {meta.get('corrected',0)} corrected ✓")
    except Exception as e:
        harmonized["validation_summary"] = {"status": "error", "error": str(e)}
        result = harmonized
        if verbose:
            print(f"  [Validation]    {pid} — error: {e}")

    return result


def validate_all(harmonized_records: dict, verbose: bool = False) -> dict:
    results = {}
    for pid, record in sorted(harmonized_records.items()):
        results[pid] = validate_patient(record, verbose=verbose)
    return results
