"""
datagrid — Ingestion Agent
Pulls clinical records via the Airbyte source connector (replaces os.listdir).
Auth0 M2M token enforces read:records scope.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from auth.m2m import require_token
from connectors.airbyte_source import read_records


@require_token("ingestion-agent")
def ingest(data_dir: str, verbose: bool = False, auth_token: str = "") -> dict:
    """
    Pull all patient records from data_dir via Airbyte connector.
    Returns {patient_id: {patient_id, sources: [...]}}
    """
    if verbose:
        print(f"  [Ingestion] Reading from: {data_dir}")
        print(f"  [Ingestion] Auth scope: read:records ✓")

    patient_records = read_records(data_dir)

    if verbose:
        for pid, rec in sorted(patient_records.items()):
            fmts = [s.get("_format", "?") for s in rec.get("sources", [])]
            print(f"  [Ingestion] {pid} — {', '.join(fmts)}")

    print(f"  [Ingestion] {len(patient_records)} patients ingested via Airbyte connector")
    return patient_records
