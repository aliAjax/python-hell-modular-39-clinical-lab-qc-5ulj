import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS instrument_sessions (
                    instrument_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(instrument_id, session_id)
                );
                CREATE TABLE IF NOT EXISTS staged_qc_run (
                    instrument_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    sequence_no INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    qc_run_id TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(instrument_id, session_id, sequence_no)
                );
                CREATE INDEX IF NOT EXISTS idx_staged_qc_run_status
                    ON staged_qc_run(instrument_id, session_id, status);
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def upsert_session(self, instrument_id, session_id, actor_id):
        """Record that an instrument upload session exists; idempotent."""
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO instrument_sessions(instrument_id, session_id, created_by, created_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(instrument_id, session_id) DO NOTHING",
                (instrument_id, session_id, actor_id, utcnow()),
            )

    def list_sessions(self, instrument_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT instrument_id, session_id, created_by, created_at "
                "FROM instrument_sessions WHERE instrument_id = ? ORDER BY created_at, session_id",
                (instrument_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def apply_staged_run(self, instrument_id, session_id, sequence_no, raw_payload,
                         entity_id, entity_status, entity_data, actor_id):
        """Atomically claim a staged record and create its qc_run entity.

        Returns ("applied", entity_id) when this call created the run, or
        ("exists", None) when a concurrent/previous call already applied it.
        """
        now = utcnow()
        payload = json.dumps(raw_payload, ensure_ascii=False, sort_keys=True)
        entity_payload = json.dumps(entity_data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "INSERT OR IGNORE INTO staged_qc_run"
                    "(instrument_id, session_id, sequence_no, payload, status, qc_run_id, error, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 'pending', NULL, NULL, ?, ?)",
                    (instrument_id, session_id, sequence_no, payload, now, now),
                )
                row = connection.execute(
                    "SELECT status FROM staged_qc_run "
                    "WHERE instrument_id = ? AND session_id = ? AND sequence_no = ?",
                    (instrument_id, session_id, sequence_no),
                ).fetchone()
                if row and row["status"] == "applied":
                    connection.rollback()
                    return "exists", None
                connection.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, 'qc_run', ?, 1, ?, ?, ?, ?)",
                    (entity_id, entity_status, entity_payload, actor_id, now, now),
                )
                connection.execute(
                    "UPDATE staged_qc_run SET status = 'applied', qc_run_id = ?, error = NULL, updated_at = ? "
                    "WHERE instrument_id = ? AND session_id = ? AND sequence_no = ?",
                    (entity_id, now, instrument_id, session_id, sequence_no),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return "applied", entity_id

    def mark_staged_failed(self, instrument_id, session_id, sequence_no, raw_payload, error):
        """Persist a per-record failure so a later retry can resume from it.

        An already-applied record is never overwritten.
        """
        now = utcnow()
        payload = json.dumps(raw_payload, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO staged_qc_run"
                "(instrument_id, session_id, sequence_no, payload, status, qc_run_id, error, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'failed', NULL, ?, ?, ?) "
                "ON CONFLICT(instrument_id, session_id, sequence_no) DO UPDATE "
                "SET status = 'failed', error = excluded.error, qc_run_id = NULL, updated_at = excluded.updated_at "
                "WHERE staged_qc_run.status != 'applied'",
                (instrument_id, session_id, sequence_no, payload, str(error), now, now),
            )

    def list_staged(self, instrument_id, session_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT instrument_id, session_id, sequence_no, payload, status, qc_run_id, error, created_at, updated_at "
                "FROM staged_qc_run WHERE instrument_id = ? AND session_id = ? ORDER BY sequence_no",
                (instrument_id, session_id),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            result.append(item)
        return result

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
