"""
datagrid — Airbyte Source Connector
Replaces os.listdir() ingestion with a proper Airbyte source connector.
Reads clinical records (TXT / CSV / VCF / JSON) from a local directory
and emits them as Airbyte RECORD messages.

This is a custom Python source connector using the Airbyte CDK.
It defines one stream per file format:
  - clinical_notes   (.txt)
  - lab_results      (.csv)
  - genomic_variants (.vcf)
  - patient_json     (.json)

Usage (Airbyte protocol):
  python connectors/airbyte_source.py read --config config.json --catalog catalog.json

Usage (direct Python — called by ingestion_agent.py):
  from connectors.airbyte_source import read_records
  records = read_records("/path/to/data/dir")
"""

import json
import os
import sys
from typing import Any, Generator, Iterable, Mapping

# ---------------------------------------------------------------------------
# Airbyte CDK imports (falls back to direct file reading if CDK not installed)
# ---------------------------------------------------------------------------

try:
    from airbyte_cdk.sources import AbstractSource
    from airbyte_cdk.sources.streams import Stream
    from airbyte_cdk.models import (
        AirbyteCatalog,
        AirbyteStream,
        ConfiguredAirbyteCatalog,
        SyncMode,
    )
    _CDK_AVAILABLE = True
except ImportError:
    _CDK_AVAILABLE = False

# Parsers from the existing pipeline (format-specific, unchanged)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ingestion.detector import detect_format, detect_patient_id
from ingestion import parsers as _parsers_pkg
from ingestion.parsers import text_parser, csv_parser, vcf_parser, json_parser

PARSER_MAP = {
    "text": text_parser,
    "csv":  csv_parser,
    "vcf":  vcf_parser,
    "json": json_parser,
}

STREAM_NAMES = {
    "text": "clinical_notes",
    "csv":  "lab_results",
    "vcf":  "genomic_variants",
    "json": "patient_json",
}


# ---------------------------------------------------------------------------
# Core record reader — format-agnostic, Airbyte-protocol output
# ---------------------------------------------------------------------------

def _emit_records(data_dir: str) -> Generator[dict, None, None]:
    """
    Yield one Airbyte RECORD message per file.
    Each record: {type, stream, data: {patient_id, format, source_file, ...parsed}}
    """
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    for fname in sorted(os.listdir(data_dir)):
        fpath = os.path.join(data_dir, fname)
        if not os.path.isfile(fpath) or fname.endswith(".py"):
            continue

        patient_id = detect_patient_id(fname)
        if patient_id is None:
            continue

        try:
            fmt    = detect_format(fpath)
            parser = PARSER_MAP[fmt]
            parsed = parser.parse(fpath)
            parsed["_format"]    = fmt
            parsed["patient_id"] = patient_id

            yield {
                "type":   "RECORD",
                "record": {
                    "stream": STREAM_NAMES.get(fmt, fmt),
                    "data":   parsed,
                }
            }
        except Exception as e:
            yield {
                "type":   "RECORD",
                "record": {
                    "stream": "errors",
                    "data": {
                        "patient_id":  patient_id,
                        "source_file": fname,
                        "error":       str(e),
                        "_format":     "error",
                    }
                }
            }


# ---------------------------------------------------------------------------
# read_records() — direct Python API used by ingestion_agent.py
# Groups Airbyte RECORD messages back into patient_records dict.
# ---------------------------------------------------------------------------

def read_records(data_dir: str) -> dict:
    """
    Pull all clinical records from data_dir via the Airbyte connector.
    Returns {patient_id: {patient_id, sources: [...]}} — same shape as
    the original ingest_directory() so the rest of the pipeline is unchanged.
    """
    from collections import defaultdict
    patient_records: dict[str, dict] = defaultdict(lambda: {"patient_id": None, "sources": []})

    for message in _emit_records(data_dir):
        if message["type"] != "RECORD":
            continue
        record = message["record"]["data"]
        pid    = record.get("patient_id")
        if not pid:
            continue

        patient_records[pid]["patient_id"] = pid
        # Don't duplicate patient_id inside sources
        source = {k: v for k, v in record.items() if k != "patient_id"}
        patient_records[pid]["sources"].append(source)

    return dict(patient_records)


# ---------------------------------------------------------------------------
# Airbyte CDK Source class (used when running as a proper Airbyte connector)
# ---------------------------------------------------------------------------

if _CDK_AVAILABLE:
    class ClinicalRecordsStream(Stream):
        """One stream for each clinical file format."""

        def __init__(self, stream_name: str, fmt: str, data_dir: str):
            self._stream_name = stream_name
            self._fmt         = fmt
            self._data_dir    = data_dir

        @property
        def name(self) -> str:
            return self._stream_name

        def get_json_schema(self) -> Mapping[str, Any]:
            return {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type":    "object",
                "properties": {
                    "patient_id":   {"type": "string"},
                    "source_file":  {"type": "string"},
                    "_format":      {"type": "string"},
                },
                "additionalProperties": True,
            }

        def read_records(
            self,
            sync_mode,
            cursor_field=None,
            stream_slice=None,
            stream_state=None,
        ) -> Iterable[Mapping[str, Any]]:
            for message in _emit_records(self._data_dir):
                if message["type"] == "RECORD":
                    rec = message["record"]
                    if rec["stream"] == self._stream_name:
                        yield rec["data"]

    class DatagridSource(AbstractSource):
        """
        Airbyte source for datagrid clinical records.
        Config: {"data_dir": "/path/to/clinical/files"}
        """

        def check_connection(self, logger, config) -> tuple[bool, Any]:
            data_dir = config.get("data_dir", "")
            if not os.path.isdir(data_dir):
                return False, f"Directory not found: {data_dir}"
            return True, None

        def streams(self, config: Mapping[str, Any]) -> list[Stream]:
            data_dir = config.get("data_dir", "")
            return [
                ClinicalRecordsStream("clinical_notes",   "text", data_dir),
                ClinicalRecordsStream("lab_results",      "csv",  data_dir),
                ClinicalRecordsStream("genomic_variants", "vcf",  data_dir),
                ClinicalRecordsStream("patient_json",     "json", data_dir),
            ]


# ---------------------------------------------------------------------------
# CLI entrypoint for running as a standalone Airbyte connector
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not _CDK_AVAILABLE:
        print("airbyte-cdk not installed. Install with: pip install airbyte-cdk", file=sys.stderr)
        sys.exit(1)
    from airbyte_cdk.entrypoint import launch
    source = DatagridSource()
    launch(source, sys.argv[1:])
