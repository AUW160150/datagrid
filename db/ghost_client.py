"""
datagrid — Ghost DB Client
Each pipeline run forks its own Ghost Postgres DB, uses it, then discards it.
Wraps the Ghost CLI (ghost create / ghost connect / ghost delete).
Falls back to in-memory dict if Ghost CLI is not installed.
"""

import json
import os
import subprocess
import datetime
import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def _ghost_cmd(*args, check=True):
    """Run a ghost CLI command, return parsed JSON output."""
    result = subprocess.run(
        ["ghost", *args, "--json"],
        capture_output=True,
        text=True,
        check=check,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ghost {' '.join(args)} failed: {result.stderr.strip()}")
    return json.loads(result.stdout.strip()) if result.stdout.strip() else {}


def _ghost_available():
    try:
        subprocess.run(["ghost", "--version"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


# ---------------------------------------------------------------------------
# GhostDB — one instance per pipeline run
# ---------------------------------------------------------------------------

class GhostDB:
    """
    Represents one ephemeral Ghost Postgres DB for a single pipeline run.
    Usage:
        db = GhostDB.fork(run_id)
        db.write_cache(patient_id, stage, data)
        result = db.read_cache(patient_id, stage)
        db.write_job(job_id, status_dict)
        db.discard()
    """

    # Fallback in-memory store when Ghost CLI not available
    _fallback: dict = {}

    def __init__(self, run_id: str, db_id: str, conn_string: str, using_ghost: bool):
        self.run_id      = run_id
        self.db_id       = db_id
        self.conn_string = conn_string
        self.using_ghost = using_ghost
        self._conn       = None

    # ── Construction ────────────────────────────────────────────────────────

    @classmethod
    def fork(cls, run_id: str) -> "GhostDB":
        """Fork a new Ghost DB for this run_id. Falls back to memory if CLI absent."""
        if not _ghost_available():
            print(f"  [Ghost] CLI not found — using in-memory fallback for run {run_id}")
            return cls(run_id, run_id, "", using_ghost=False)

        name = f"datagrid-{run_id}"
        print(f"  [Ghost] Forking DB: {name}")
        info = _ghost_cmd("create", "--name", name, "--wait")
        db_id       = info.get("id", run_id)
        conn_string = info.get("connection_string") or info.get("connectionString", "")
        print(f"  [Ghost] DB ready: {db_id}")
        return cls(run_id, db_id, conn_string, using_ghost=True)

    # ── Connection ──────────────────────────────────────────────────────────

    def _get_conn(self):
        if not self.using_ghost:
            return None
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.conn_string)
            self._conn.autocommit = True
            self._ensure_schema()
        return self._conn

    def _ensure_schema(self):
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS patient_cache (
                    run_id      TEXT,
                    patient_id  TEXT,
                    stage       TEXT,
                    data        JSONB,
                    created_at  TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (run_id, patient_id, stage)
                );
                CREATE TABLE IF NOT EXISTS pipeline_jobs (
                    job_id      TEXT PRIMARY KEY,
                    run_id      TEXT,
                    status      TEXT,
                    data        JSONB,
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS omop_provenance (
                    run_id      TEXT PRIMARY KEY,
                    data        JSONB,
                    written_at  TIMESTAMPTZ DEFAULT NOW()
                );
            """)

    # ── Cache operations ─────────────────────────────────────────────────────

    def write_cache(self, patient_id: str, stage: str, data: dict):
        if not self.using_ghost:
            key = (self.run_id, patient_id, stage)
            GhostDB._fallback[key] = data
            return
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO patient_cache (run_id, patient_id, stage, data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, patient_id, stage) DO UPDATE SET data = EXCLUDED.data
            """, (self.run_id, patient_id, stage, json.dumps(data)))

    def read_cache(self, patient_id: str, stage: str) -> dict | None:
        if not self.using_ghost:
            return GhostDB._fallback.get((self.run_id, patient_id, stage))
        conn = self._get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT data FROM patient_cache
                WHERE run_id=%s AND patient_id=%s AND stage=%s
            """, (self.run_id, patient_id, stage))
            row = cur.fetchone()
        return dict(row["data"]) if row else None

    # ── Job store ────────────────────────────────────────────────────────────

    def write_job(self, job_id: str, status_dict: dict):
        if not self.using_ghost:
            GhostDB._fallback[("job", job_id)] = status_dict
            return
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_jobs (job_id, run_id, status, data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (job_id) DO UPDATE
                  SET status=%s, data=EXCLUDED.data, updated_at=NOW()
            """, (
                job_id, self.run_id,
                status_dict.get("status", "unknown"),
                json.dumps(status_dict),
                status_dict.get("status", "unknown"),
            ))

    def read_job(self, job_id: str) -> dict | None:
        if not self.using_ghost:
            return GhostDB._fallback.get(("job", job_id))
        conn = self._get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT data FROM pipeline_jobs WHERE job_id=%s", (job_id,))
            row = cur.fetchone()
        return dict(row["data"]) if row else None

    # ── Provenance ───────────────────────────────────────────────────────────

    def write_provenance(self, data: dict):
        if not self.using_ghost:
            GhostDB._fallback[("provenance", self.run_id)] = data
            return
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO omop_provenance (run_id, data)
                VALUES (%s, %s)
                ON CONFLICT (run_id) DO UPDATE SET data=EXCLUDED.data, written_at=NOW()
            """, (self.run_id, json.dumps(data)))

    # ── Teardown ─────────────────────────────────────────────────────────────

    def discard(self):
        """Close connection and delete the Ghost DB."""
        if self._conn and not self._conn.closed:
            self._conn.close()
        if not self.using_ghost:
            # Clear in-memory fallback for this run
            keys_to_del = [k for k in GhostDB._fallback if isinstance(k, tuple) and k[0] == self.run_id]
            for k in keys_to_del:
                del GhostDB._fallback[k]
            return
        try:
            print(f"  [Ghost] Discarding DB: {self.db_id}")
            subprocess.run(["ghost", "delete", self.db_id, "--confirm"], check=True)
            print(f"  [Ghost] DB {self.db_id} deleted.")
        except Exception as e:
            print(f"  [Ghost] Warning: could not delete DB {self.db_id}: {e}")


# ---------------------------------------------------------------------------
# Global run registry (maps run_id → GhostDB instance)
# ---------------------------------------------------------------------------

_active_dbs: dict[str, GhostDB] = {}


def get_or_fork(run_id: str) -> GhostDB:
    if run_id not in _active_dbs:
        _active_dbs[run_id] = GhostDB.fork(run_id)
    return _active_dbs[run_id]


def close_run(run_id: str):
    db = _active_dbs.pop(run_id, None)
    if db:
        db.discard()
