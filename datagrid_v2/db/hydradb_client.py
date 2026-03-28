"""
HydraDB Client — replaces Ghost DB (ephemeral Postgres).

Provides per-run tenant isolation via HydraDB's multi-tenant API,
semantic OMOP knowledge indexing, and job state storage.
"""

import os
import json
import logging
from typing import Any

log = logging.getLogger(__name__)

# In-memory job store fallback when HydraDB is unavailable
_fallback_store: dict[str, dict] = {}


class HydraDBClient:
    """
    Wraps the HydraDB Python SDK for clingrid pipeline runs.

    Lifecycle per run:
      1. create_run_tenant(run_id)  → isolated namespace
      2. seed_omop_knowledge()      → index OMOP CDM reference for semantic recall
      3. write_job() / read_job()   → pipeline state storage
      4. recall(query)              → semantic OMOP concept lookup during harmonization
      5. discard()                  → delete tenant (no persistent patient data)
    """

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("HYDRADB_API_KEY", "")
        self.tenant_id: str | None = None
        self.db_id: str = ""
        self._client = None
        self._available = False
        self._jobs: dict[str, dict] = {}

        if self.api_key:
            try:
                from hydra_db_python import HydraDB  # type: ignore
                self._client = HydraDB(api_key=self.api_key)
                self._available = True
                log.info("HydraDB client initialised")
            except ImportError:
                log.warning("hydra-db-python not installed — running in fallback mode")
            except Exception as exc:
                log.warning("HydraDB init failed (%s) — running in fallback mode", exc)

    # ── Tenant lifecycle ─────────────────────────────────────────────────────

    def create_run_tenant(self, run_id: str) -> str:
        """Create an isolated HydraDB tenant for this pipeline run."""
        self.db_id = f"dg-tenant-{run_id[:8]}"

        if self._available:
            try:
                tenant = self._client.tenant.create(name=self.db_id)
                self.tenant_id = tenant.id
                log.info("HydraDB tenant created: %s", self.db_id)
                return self.db_id
            except Exception as exc:
                log.warning("HydraDB tenant.create failed (%s)", exc)

        # Fallback
        self.tenant_id = self.db_id
        _fallback_store[self.db_id] = {}
        return self.db_id

    def seed_omop_knowledge(self, omop_concepts: list[dict]) -> int:
        """
        Index OMOP CDM reference data as semantic knowledge.
        Agents can then call recall() instead of exact-match lookups.

        Each concept dict: {name, omop_id, icd10, description}
        """
        if not omop_concepts:
            return 0

        texts = [
            f"{c.get('name','')}: OMOP {c.get('omop_id','')} "
            f"ICD-10 {c.get('icd10','')} — {c.get('description','')}"
            for c in omop_concepts
        ]

        if self._available and self.tenant_id:
            try:
                self._client.upload.knowledge(
                    tenant_id=self.tenant_id,
                    texts=texts,
                    metadata=[{"source": "omop_cdm_v5.4"} for _ in texts],
                )
                log.info("Seeded %d OMOP concepts into HydraDB tenant", len(texts))
                return len(texts)
            except Exception as exc:
                log.warning("HydraDB upload.knowledge failed (%s)", exc)

        return len(texts)  # count only

    # ── Semantic recall ───────────────────────────────────────────────────────

    def recall(self, query: str, limit: int = 5) -> list[dict]:
        """
        Semantic search over indexed OMOP knowledge.
        Used by harmonization_agent to map clinical text → OMOP concept IDs.
        """
        if self._available and self.tenant_id:
            try:
                results = self._client.recall.fullRecall(
                    tenant_id=self.tenant_id,
                    query=query,
                    limit=limit,
                )
                log.debug("HydraDB recall('%s') → %d results", query, len(results))
                return results
            except Exception as exc:
                log.warning("HydraDB recall failed (%s)", exc)

        # Fallback: return empty (caller uses built-in OMOP reference)
        return []

    def recall_preferences(self, user_id: str) -> list[dict]:
        """Retrieve stored pharma buyer preferences for personalised matching."""
        if self._available and self.tenant_id:
            try:
                return self._client.recall.recallPreferences(
                    tenant_id=self.tenant_id,
                    query=user_id,
                    limit=10,
                )
            except Exception:
                pass
        return []

    def store_user_preference(self, user_id: str, preference: dict):
        """Persist pharma buyer search preference for future recommendations."""
        if self._available and self.tenant_id:
            try:
                self._client.userMemory.add(
                    tenant_id=self.tenant_id,
                    user_id=user_id,
                    content=json.dumps(preference),
                )
            except Exception as exc:
                log.debug("HydraDB userMemory.add failed (%s)", exc)

    # ── Job state (replaces Ghost DB write_job / read_job) ───────────────────

    def write_job(self, job_id: str, data: dict):
        self._jobs[job_id] = data

        if self._available and self.tenant_id:
            try:
                self._client.userMemory.add(
                    tenant_id=self.tenant_id,
                    user_id=f"job:{job_id}",
                    content=json.dumps(data, default=str),
                )
            except Exception:
                pass  # in-memory copy is the source of truth

    def read_job(self, job_id: str) -> dict | None:
        return self._jobs.get(job_id)

    # ── Teardown ─────────────────────────────────────────────────────────────

    def discard(self):
        """
        Delete the HydraDB tenant after pipeline output is written.
        Ensures no persistent patient data remains — mirrors Ghost DB's
        ephemeral lifecycle.
        """
        if self._available and self.tenant_id:
            try:
                self._client.tenant.delete(self.tenant_id)
                log.info("HydraDB tenant %s discarded", self.db_id)
            except Exception as exc:
                log.warning("HydraDB tenant.delete failed (%s)", exc)

        # Clear fallback store
        _fallback_store.pop(self.db_id, None)
        self.tenant_id = None


def get_or_create(run_id: str) -> HydraDBClient:
    """Factory: create and initialise a HydraDB client for a pipeline run."""
    client = HydraDBClient()
    client.create_run_tenant(run_id)
    return client
