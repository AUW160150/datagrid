"""
datagrid — Pipeline Orchestrator
Coordinates all agents with sponsor integrations:
  - Airbyte: ingestion source connector
  - Ghost:   fork-per-run DB for cache + job store
  - Auth0:   M2M tokens scoping each agent
  - OverClaw: call_llm tracing across all LLM calls

Pipeline: ingest → modality → harmonize → validate → output → discard DB
"""

import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agents.ingestion_agent    import ingest
from agents.modality_agent     import assess_all
from agents.harmonization_agent import harmonize_all
from agents.validation_agent   import validate_all
from agents.output_agent       import write_output
from db.ghost_client           import get_or_create, close_run

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "synthetic")


def run(data_dir: str = None, force_rerun: bool = False, verbose: bool = True) -> tuple[dict, dict]:
    """
    Full datagrid pipeline run.
    Each run gets its own Ghost DB (forked at start, discarded after output).
    """
    run_id   = str(uuid.uuid4())[:8]
    data_dir = data_dir or DATA_DIR
    t_start  = time.time()

    print()
    print("=" * 60)
    print("  datagrid Pipeline")
    print(f"  Run ID  : {run_id}")
    print(f"  Data    : {data_dir}")
    print("=" * 60)

    # ── Fork Ghost DB ────────────────────────────────────────────────────────
    print(f"\n[Ghost] Forking DB for run {run_id}...")
    db = get_or_create(run_id)

    try:
        # ── Step 1: Ingest via Airbyte ───────────────────────────────────────
        print("\n[1/5] Ingestion Agent (Airbyte connector)")
        patient_records = ingest(data_dir, verbose=verbose)
        print(f"  {len(patient_records)} patients: {', '.join(sorted(patient_records.keys()))}")

        # ── Step 2: Modality Detection ───────────────────────────────────────
        print("\n[2/5] Modality Detection Agent (OverClaw traced)")
        patient_records = assess_all(patient_records, verbose=verbose)

        # ── Step 3: Harmonization (with Ghost cache) ─────────────────────────
        print("\n[3/5] Harmonization Agent (OverClaw optimized + Ghost cache)")
        harmonized = {}
        for pid, record in sorted(patient_records.items()):
            if not force_rerun:
                cached = db.read_cache(pid, "harmonized")
                if cached:
                    print(f"  [Harmonization] {pid} — Ghost cache hit")
                    harmonized[pid] = cached
                    continue

            from agents.harmonization_agent import harmonize_patient
            result = harmonize_patient(record, verbose=verbose)
            db.write_cache(pid, "harmonized", result)
            harmonized[pid] = result

        # ── Step 4: Validation (with Ghost cache) ────────────────────────────
        print("\n[4/5] Validation Agent (Auth0 read-only audit + Ghost cache)")
        validated = {}
        for pid, record in sorted(harmonized.items()):
            if not force_rerun:
                cached = db.read_cache(pid, "validated")
                if cached:
                    print(f"  [Validation]    {pid} — Ghost cache hit")
                    validated[pid] = cached
                    continue

            from agents.validation_agent import validate_patient
            result = validate_patient(record, verbose=verbose)
            db.write_cache(pid, "validated", result)
            validated[pid] = result

        # Re-attach modality assessments (may have been stripped from cache)
        for pid in validated:
            if "modality_assessment" not in validated[pid] and pid in patient_records:
                validated[pid]["modality_assessment"] = patient_records[pid].get("modality_assessment", {})

        # ── Step 5: Output ───────────────────────────────────────────────────
        total = time.time() - t_start
        pipeline_meta = {
            "run_id":         run_id,
            "patients":       list(sorted(validated.keys())),
            "total_duration": f"{total:.1f}s",
            "ghost_db_id":    db.db_id,
            "sponsors":       ["Ghost", "Auth0", "Airbyte", "OverClaw"],
        }
        print("\n[5/5] Output Agent (Ghost provenance + OMOP Parquet)")
        output_info = write_output(validated, pipeline_meta, ghost_db=db)

        # ── Summary ──────────────────────────────────────────────────────────
        total = time.time() - t_start
        print()
        print("=" * 60)
        print("  datagrid — Run Complete")
        print(f"  Total time : {total:.1f}s")
        print(f"  Patients   : {len(validated)}")
        print(f"  Ghost DB   : {db.db_id} (discarding now)")
        print()
        for name, info in output_info["row_counts"].items():
            print(f"    {name}.parquet — {info} rows")
        print("=" * 60)

        return validated, output_info

    finally:
        # ── Discard Ghost DB ─────────────────────────────────────────────────
        close_run(run_id)
        print(f"\n[Ghost] DB {db.db_id} discarded.")
