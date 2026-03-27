"""
datagrid — Harmonization Agent
Maps multilingual clinical records to ICD-10 + OMOP CDM v5.4.
Registered with OverClaw for automated prompt/model optimization.

OverClaw entrypoint: agents.harmonization_agent:run
  - Accepts: {patient_id, sources: [...]}
  - Returns: {patient_id, language_detected, entities: {...}, ...}

Auth0 M2M: read:records + write:harmonized scopes.
"""

import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from auth.m2m import require_token
from harmonization.omop_reference import build_reference_block

try:
    from overclaw.core.tracer import call_llm
    _OVERCLAW = True
except ImportError:
    _OVERCLAW = False

MODEL          = "llama-3.3-70b-versatile"
MODEL_OVERCLAW = "groq/llama-3.3-70b-versatile"   # litellm provider prefix
MAX_TOKENS     = 16000
GROQ_BASE      = "https://api.groq.com/openai/v1"

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

def _build_system_prompt() -> str:
    return (
        """You are datagrid, a specialist clinical NLP agent for South Asian multilingual medical data standardization.

Your task is to analyze raw patient records that may contain clinical text in Bengali (বাংলা), Hindi (हिन्दी), or English — often mixed — along with non-standard lab result tables and genomic variant data.

Extract ALL clinical entities and map them to international standards (ICD-10, OMOP CDM).

"""
        + build_reference_block()
        + """

=== OUTPUT RULES (strictly enforced) ===
1. Return ONLY valid JSON. No prose, no markdown, no explanation outside the JSON.
2. Every extracted entity must have ALL of these fields:
   - "original_text": exact text as found in the source (preserve original script)
   - "language": one of "bengali", "hindi", "english", "mixed"
   - "standardized_english_term": canonical English medical term
   - "icd10_code": ICD-10 code string, or null if not applicable
   - "omop_concept_id": OMOP concept ID string from the reference table, or null
   - "confidence": float 0.0-1.0
   - "reasoning": 1-2 sentences explaining the mapping decision
   - "flag": null | "low_confidence" | "needs_review" | "uncertain_mapping" | "no_standard_code"

3. Confidence thresholds:
   - 0.9-1.0: Direct match, unambiguous
   - 0.7-0.89: High confidence, minor ambiguity
   - 0.5-0.69: Moderate — FLAG as "low_confidence"
   - <0.5: Uncertain — FLAG as "uncertain_mapping", do NOT invent codes

4. For measurements/lab values: extract numeric value AND unit, note reference range deviation.
5. For medications: extract generic name, dose, frequency.
6. For variants: extract rsID, gene, genotype, clinical significance.

=== OUTPUT JSON SCHEMA ===
{
  "patient_id": "string",
  "language_detected": "bengali|hindi|english|mixed",
  "entities": {
    "demographics": {
      "name": { ...entity... }, "age": { ...entity... }, "sex": { ...entity... }
    },
    "diagnoses":   [ { ...entity... } ],
    "medications": [ { ...entity... } ],
    "vitals":      [ { ...entity... } ],
    "lab_values":  [ { ...entity... } ],
    "variants":    [ { ...entity... } ]
  },
  "flags": ["patient-level concerns"],
  "harmonization_metadata": {
    "total_entities": integer,
    "low_confidence_count": integer,
    "uncertain_count": integer
  }
}"""
    )


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _build_user_prompt(patient_record: dict) -> str:
    pid     = patient_record.get("patient_id", "UNKNOWN")
    sources = patient_record.get("sources", [])
    sections = []

    notes = [s for s in sources if s.get("_format") == "text"]
    if notes:
        parts = ["=== CLINICAL NOTE(S) ==="]
        for n in notes:
            parts.append(f"[Source: {n['source_file']} | Language hint: {n.get('language_hint', 'unknown')}]")
            parts.append(n.get("raw_text", ""))
        sections.append("\n".join(parts))

    csvs = [s for s in sources if s.get("_format") == "csv"]
    if csvs:
        parts = ["=== LAB RESULTS (non-standard column names — map to standard terms) ==="]
        for c in csvs:
            parts.append(f"[Source: {c['source_file']}]")
            headers = c.get("headers", [])
            records = c.get("records", [])
            ref     = c.get("reference_ranges", {})
            if records:
                rec = records[0]
                for col in headers:
                    val    = rec.get(col, "")
                    refval = ref.get(col, "")
                    parts.append(f"  {col}: {val}" + (f"  [ref: {refval}]" if refval else ""))
        sections.append("\n".join(parts))

    vcfs = [s for s in sources if s.get("_format") == "vcf"]
    if vcfs:
        parts = ["=== GENOMIC VARIANTS (VCF) ==="]
        for v in vcfs:
            parts.append(f"[Source: {v['source_file']} | Sample: {v.get('sample_id', '?')}]")
            for variant in v.get("variants", []):
                info = variant.get("info", {})
                parts.append(
                    f"  rsID={variant['rsid']}  GENE={info.get('GENE','?')}  "
                    f"GT={variant.get('genotype','?')}  CLNSIG={info.get('CLNSIG','?')}  "
                    f"AF_SAS={info.get('AF_SAS','?')}  PHENOTYPE={info.get('PHENOTYPE','?')}"
                )
        sections.append("\n".join(parts))

    body = "\n\n".join(sections)
    return (
        f"Harmonize the following patient record. Patient ID: {pid}\n\n"
        f"{body}\n\n"
        "Extract ALL entities. Return only the JSON object — no other text."
    )


# ---------------------------------------------------------------------------
# OverClaw-compatible entrypoint  (overclaw agent register datagrid agents.harmonization_agent:run)
# ---------------------------------------------------------------------------

def run(input: dict) -> dict:
    """
    OverClaw entrypoint. Accepts a patient_record dict, returns harmonized dict.
    Uses call_llm for full OverClaw trace visibility.
    """
    system_prompt = _build_system_prompt()
    user_prompt   = _build_user_prompt(input)
    patient_id    = input.get("patient_id", "UNKNOWN")

    import time, re
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ]
    for attempt in range(6):
        try:
            if _OVERCLAW:
                resp = call_llm(model=MODEL_OVERCLAW, messages=messages)
                raw = resp.choices[0].message.content if hasattr(resp, "choices") else str(resp)
            else:
                from openai import OpenAI
                client = OpenAI(api_key=os.environ["GROQ_API_KEY"], base_url=GROQ_BASE)
                resp = client.chat.completions.create(
                    model=MODEL, messages=messages, max_tokens=MAX_TOKENS,
                )
                raw = resp.choices[0].message.content
            break
        except Exception as e:
            msg = str(e)
            if "429" in msg or "rate_limit" in msg.lower() or "RateLimitError" in type(e).__name__:
                # Extract suggested wait time from error message
                match = re.search(r"try again in (\d+\.?\d*)s", msg)
                wait = float(match.group(1)) + 2 if match else (2 ** attempt) * 10
                print(f"  [Harmonization] Rate limit — waiting {wait:.0f}s (attempt {attempt+1}/6)...")
                time.sleep(wait)
            else:
                raise

    # Parse JSON
    text = (raw or "").strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    try:
        result = json.loads(text)
    except Exception as e:
        result = {
            "patient_id": patient_id,
            "error": f"JSON parse failed: {e}",
            "raw_response": text[:500],
        }

    result.setdefault("harmonization_metadata", {})
    result["harmonization_metadata"]["model"]  = MODEL
    result["harmonization_metadata"]["tracer"] = "overclaw" if _OVERCLAW else "openai-direct"
    return result


# ---------------------------------------------------------------------------
# Pipeline-facing function (adds Auth0 gate + verbose logging)
# ---------------------------------------------------------------------------

@require_token("harmonization-agent")
def harmonize_patient(patient_record: dict, verbose: bool = False, auth_token: str = "") -> dict:
    pid = patient_record.get("patient_id", "UNKNOWN")
    if verbose:
        print(f"  [Harmonization] {pid} — calling {MODEL} via {'OverClaw' if _OVERCLAW else 'OpenAI'}...")

    result = run(patient_record)

    if verbose:
        meta = result.get("harmonization_metadata", {})
        n    = meta.get("total_entities", "?")
        fl   = meta.get("low_confidence_count", 0) + meta.get("uncertain_count", 0)
        print(f"  [Harmonization] {pid} — {n} entities, {fl} flagged ✓")

    return result


def harmonize_all(patient_records: dict, verbose: bool = False) -> dict:
    results = {}
    for pid, record in sorted(patient_records.items()):
        results[pid] = harmonize_patient(record, verbose=verbose)
    return results
