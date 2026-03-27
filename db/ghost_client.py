"""
datagrid — Ghost DB Client
Each pipeline run creates its own ephemeral Ghost Postgres DB, uses it, discards it.

Auth: GHOST_TOKEN env var (no browser login needed in scripts).
CLI path: ~/.local/bin/ghost (installed locally, not globally).

Workflow per run:
  1. ghost create --name datagrid-<run_id> --wait --json  → get db_id
  2. ghost connect <db_id>                                → get postgres connection string
  3. psycopg2.connect(conn_string) + create tables       → use for cache/job store
  4. ghost delete <db_id> --confirm                      → discard after output written
"""

import json
import os
import subprocess
import psycopg2
import psycopg2.extras

GHOST_BIN = os.path.expanduser("~/.local/bin/ghost")


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def _ghost(*args, capture=True, check=True) -> str:
    """Run ghost CLI with GHOST_TOKEN in env. Returns stdout as string."""
    env = os.environ.copy()
    # Ghost uses GHOST_TOKEN for non-interactive auth
    ghost_token = os.getenv("GHOST_TOKEN", "")
    if ghost_token:
        env["GHOST_TOKEN"] = ghost_token

    result = subprocess.run(
        [GHOST_BIN, *args],
        capture_output=capture,
        text=True,
        env=env,
        check=check,
    )
    return result.stdout.strip() if capture else ""


def _ghost_available() -> bool:
    return os.path.isfile(GHOST_BIN)


def _ghost_authed() -> bool:
    """Check if Ghost CLI is available and authenticated."""
    if not _ghost_available():
        return False
    try:
        _ghost("list", "--json")
        return True
    except subprocess.CalledProcessError:
        return False


# ---------------------------------------------------------------------------
# GhostDB — one instance per pipeline run
# ---------------------------------------------------------------------------

class GhostDB:
    """
    One ephemeral Ghost Postgres DB per pipeline run.

    Usage:
        db = GhostDB.create(run_id)
        db.write_cache(patient_id, stage, data)
        cached = db.read_cache(patient_id, stage)
        db.write_job(job_id, status)
        db.write_provenance(prov_dict)
        db.discard()   ← called in finally block, always
    """

    # In-memory fallback when Ghost not available/authed
    _fallback: dict = {}

    def __init__(self, run_id: str, db_id: str, conn_string: str, using_ghost: bool):
        self.run_id      = run_id
        self.db_id       = db_id
        self.conn_string = conn_string
        self.using_ghost = using_ghost
        self._conn       = None

    # ── Construction ─────────────────────────────────────────────────────────

    @classmethod
    def create(cls, run_id: str) -> "GhostDB":
        """
        Spin up a fresh Ghost DB for this run.
        Falls back to in-memory dict if Ghost is unavailable or not logged in.
        """
        if not _ghost_authed():
            print(f"  [Ghost] Not available/authed — using in-memory fallback for run {run_id}")
            return cls(run_id, f"memory-{run_id}", "", using_ghost=False)

        name = f"datagrid-{run_id}"
        print(f"  [Ghost] Creating DB: {name} ...")

        # Step 1: create and get the db ID
        raw  = _ghost("create", "--name", name, "--wait", "--json")
        info = json.loads(raw)
        db_id = info.get("id") or info.get("database_id") or info.get("name", name)

        # Step 2: get the postgres connection string via ghost connect
        print(f"  [Ghost] Fetching connection string for {db_id} ...")
        conn_string = _ghost("connect", db_id).strip()
        # ghost connect may return "postgresql://..." or print it in a message
        # extract the URI if it's embedded in a longer string
        if "postgresql://" in conn_string:
            for part in conn_string.split():
                if part.startswith("postgresql://"):
                    conn_string = part
                    break
        elif "postgres://" in conn_string:
            for part in conn_string.split():
                if part.startswith("postgres://"):
                    conn_string = part
                    break

        print(f"  [Ghost] DB ready: {db_id}")
        return cls(run_id, db_id, conn_string, using_ghost=True)

    # ── Connection ────────────────────────────────────────────────────────────

    def _get_conn(self):
        if not self.using_ghost:
            return None
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.conn_string)
            self._conn.autocommit = True
            self._init_schema()
        return self._conn

    def _init_schema(self):
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
                    job_id     TEXT PRIMARY KEY,
                    run_id     TEXT,
                    status     TEXT,
                    data       JSONB,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS omop_provenance (
                    run_id     TEXT PRIMARY KEY,
                    data       JSONB,
                    written_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

    # ── Cache ─────────────────────────────────────────────────────────────────

    def write_cache(self, patient_id: str, stage: str, data: dict):
        if not self.using_ghost:
            GhostDB._fallback[(self.run_id, patient_id, stage)] = data
            return
        with self._get_conn().cursor() as cur:
            cur.execute("""
                INSERT INTO patient_cache (run_id, patient_id, stage, data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, patient_id, stage)
                DO UPDATE SET data = EXCLUDED.data
            """, (self.run_id, patient_id, stage, json.dumps(data)))

    def read_cache(self, patient_id: str, stage: str) -> dict | None:
        if not self.using_ghost:
            return GhostDB._fallback.get((self.run_id, patient_id, stage))
        with self._get_conn().cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT data FROM patient_cache
                WHERE run_id=%s AND patient_id=%s AND stage=%s
            """, (self.run_id, patient_id, stage))
            row = cur.fetchone()
        return dict(row["data"]) if row else None

    # ── Job store ─────────────────────────────────────────────────────────────

    def write_job(self, job_id: str, status_dict: dict):
        if not self.using_ghost:
            GhostDB._fallback[("job", job_id)] = status_dict
            return
        with self._get_conn().cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_jobs (job_id, run_id, status, data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (job_id)
                DO UPDATE SET status=%s, data=EXCLUDED.data, updated_at=NOW()
            """, (
                job_id, self.run_id,
                status_dict.get("status", "unknown"),
                json.dumps(status_dict),
                status_dict.get("status", "unknown"),
            ))

    def read_job(self, job_id: str) -> dict | None:
        if not self.using_ghost:
            return GhostDB._fallback.get(("job", job_id))
        with self._get_conn().cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT data FROM pipeline_jobs WHERE job_id=%s", (job_id,))
            row = cur.fetchone()
        return dict(row["data"]) if row else None

    # ── Provenance ────────────────────────────────────────────────────────────

    def write_provenance(self, data: dict):
        if not self.using_ghost:
            GhostDB._fallback[("provenance", self.run_id)] = data
            return
        with self._get_conn().cursor() as cur:
            cur.execute("""
                INSERT INTO omop_provenance (run_id, data)
                VALUES (%s, %s)
                ON CONFLICT (run_id) DO UPDATE SET data=EXCLUDED.data, written_at=NOW()
            """, (self.run_id, json.dumps(data)))

    # ── Discard ───────────────────────────────────────────────────────────────

    def discard(self):
        """Close psycopg2 connection and delete the Ghost DB. Always safe to call."""
        if self._conn and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                pass

        if not self.using_ghost:
            # Clear in-memory fallback for this run
            stale = [k for k in GhostDB._fallback
                     if isinstance(k, tuple) and len(k) >= 1 and k[0] == self.run_id]
            for k in stale:
                del GhostDB._fallback[k]
            return

        try:
            print(f"  [Ghost] Deleting DB: {self.db_id}")
            _ghost("delete", self.db_id, "--confirm")
            print(f"  [Ghost] DB {self.db_id} deleted ✓")
        except Exception as e:
            print(f"  [Ghost] Warning: could not delete {self.db_id}: {e}")


# ---------------------------------------------------------------------------
# Run registry
# ---------------------------------------------------------------------------

_active: dict[str, GhostDB] = {}


def get_or_create(run_id: str) -> GhostDB:
    if run_id not in _active:
        _active[run_id] = GhostDB.create(run_id)
    return _active[run_id]


def close_run(run_id: str):
    db = _active.pop(run_id, None)
    if db:
        db.discard()
