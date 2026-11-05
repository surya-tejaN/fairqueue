from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol

from app.models import Job

ISO = "%Y-%m-%dT%H:%M:%S.%fZ"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.strptime(value, ISO).replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value)


def _dump(job_id: str, payload: dict, status: str, attempts: int, max_attempts: int,
          visible_at: datetime, created_at: datetime, result: Optional[dict]) -> Job:
    return Job(
        id=job_id,
        payload=payload,
        status=status,
        attempts=attempts,
        max_attempts=max_attempts,
        visible_at=visible_at,
        created_at=created_at,
        result=result,
    )


class JobStore(Protocol):
    def enqueue(self, payload: dict, max_attempts: int, idempotency_key: Optional[str]) -> Job: ...
    def get(self, job_id: str) -> Optional[Job]: ...
    def claim(self, visibility: timedelta) -> Optional[Job]: ...
    def heartbeat(self, job_id: str, visibility: timedelta) -> bool: ...
    def complete(self, job_id: str, result: dict) -> bool: ...
    def fail(self, job_id: str, error: str, backoff: timedelta) -> bool: ...


class SqliteStore:
    """Local store. Claim uses a process lock plus a compare-and-set update."""

    def __init__(self, path: str = ":memory:") -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                visible_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                result TEXT
            );
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                key TEXT PRIMARY KEY,
                job_id TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    def enqueue(self, payload: dict, max_attempts: int, idempotency_key: Optional[str]) -> Job:
        with self._lock:
            if idempotency_key:
                row = self._conn.execute(
                    "SELECT job_id FROM idempotency_keys WHERE key = ?",
                    (idempotency_key,),
                ).fetchone()
                if row:
                    existing = self.get(row["job_id"])
                    assert existing is not None
                    return existing
            now = utcnow()
            job = _dump(str(uuid.uuid4()), payload, "queued", 0, max_attempts, now, now, None)
            self._conn.execute(
                "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job.id,
                    json.dumps(job.payload),
                    job.status,
                    job.attempts,
                    job.max_attempts,
                    job.visible_at.strftime(ISO),
                    job.created_at.strftime(ISO),
                    None,
                ),
            )
            if idempotency_key:
                self._conn.execute(
                    "INSERT INTO idempotency_keys VALUES (?, ?)",
                    (idempotency_key, job.id),
                )
            self._conn.commit()
            return job

    def get(self, job_id: str) -> Optional[Job]:
        row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._row(row) if row else None

    def claim(self, visibility: timedelta) -> Optional[Job]:
        now = utcnow()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = 'queued'
                   OR (status = 'running' AND visible_at <= ?)
                ORDER BY created_at
                LIMIT 1
                """,
                (now.strftime(ISO),),
            ).fetchone()
            if row is None:
                return None
            visible = now + visibility
            attempts = row["attempts"] + 1
            status = "running" if attempts <= row["max_attempts"] else "dead"
            self._conn.execute(
                "UPDATE jobs SET status = ?, attempts = ?, visible_at = ? WHERE id = ?",
                (status, attempts, visible.strftime(ISO), row["id"]),
            )
            self._conn.commit()
            return self.get(row["id"])

    def heartbeat(self, job_id: str, visibility: timedelta) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE jobs SET visible_at = ? WHERE id = ? AND status = 'running'",
                ((utcnow() + visibility).strftime(ISO), job_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def complete(self, job_id: str, result: dict) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE jobs SET status = 'done', result = ? WHERE id = ? AND status = 'running'",
                (json.dumps(result), job_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def fail(self, job_id: str, error: str, backoff: timedelta) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                return False
            status = "dead" if row["attempts"] >= row["max_attempts"] else "queued"
            self._conn.execute(
                "UPDATE jobs SET status = ?, visible_at = ?, result = ? WHERE id = ?",
                (status, (utcnow() + backoff).strftime(ISO), json.dumps({"error": error}), job_id),
            )
            self._conn.commit()
            return True

    def _row(self, row: sqlite3.Row) -> Job:
        result = json.loads(row["result"]) if row["result"] else None
        return _dump(
            row["id"],
            json.loads(row["payload"]),
            row["status"],
            row["attempts"],
            row["max_attempts"],
            _parse(row["visible_at"]),
            _parse(row["created_at"]),
            result,
        )


class PostgresStore:
    """Production store. Claim uses FOR UPDATE SKIP LOCKED."""

    def __init__(self, dsn: str) -> None:
        import psycopg
        from psycopg.rows import dict_row

        self._psycopg = psycopg
        self._conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=False)

    def enqueue(self, payload: dict, max_attempts: int, idempotency_key: Optional[str]) -> Job:
        now = utcnow()
        with self._conn.cursor() as cur:
            if idempotency_key:
                cur.execute("SELECT job_id FROM idempotency_keys WHERE key = %s", (idempotency_key,))
                found = cur.fetchone()
                if found:
                    self._conn.commit()
                    job = self.get(found["job_id"])
                    assert job is not None
                    return job
            job_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO jobs (id, payload, status, attempts, max_attempts, visible_at, created_at)
                VALUES (%s, %s, 'queued', 0, %s, %s, %s)
                """,
                (job_id, json.dumps(payload), max_attempts, now, now),
            )
            if idempotency_key:
                cur.execute(
                    "INSERT INTO idempotency_keys (key, job_id, created_at) VALUES (%s, %s, %s)",
                    (idempotency_key, job_id, now),
                )
            self._conn.commit()
            return self.get(job_id)  # type: ignore[return-value]

    def get(self, job_id: str) -> Optional[Job]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
        return self._row(row) if row else None

    def claim(self, visibility: timedelta) -> Optional[Job]:
        now = utcnow()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM jobs
                WHERE status = 'queued' OR (status = 'running' AND visible_at <= %s)
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                (now,),
            )
            picked = cur.fetchone()
            if picked is None:
                self._conn.commit()
                return None
            visible = now + visibility
            cur.execute(
                """
                UPDATE jobs
                SET status = CASE WHEN attempts + 1 > max_attempts THEN 'dead' ELSE 'running' END,
                    attempts = attempts + 1,
                    visible_at = %s
                WHERE id = %s
                RETURNING *
                """,
                (visible, picked["id"]),
            )
            row = cur.fetchone()
            self._conn.commit()
            return self._row(row)

    def heartbeat(self, job_id: str, visibility: timedelta) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET visible_at = %s WHERE id = %s AND status = 'running'",
                (utcnow() + visibility, job_id),
            )
            updated = cur.rowcount == 1
        self._conn.commit()
        return updated

    def complete(self, job_id: str, result: dict) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = 'done', result = %s WHERE id = %s AND status = 'running'",
                (json.dumps(result), job_id),
            )
            updated = cur.rowcount == 1
        self._conn.commit()
        return updated

    def fail(self, job_id: str, error: str, backoff: timedelta) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                    visible_at = %s,
                    result = %s
                WHERE id = %s
                """,
                (utcnow() + backoff, json.dumps({"error": error}), job_id),
            )
            updated = cur.rowcount == 1
        self._conn.commit()
        return updated

    def _row(self, row: dict) -> Job:
        payload = row["payload"]
        result = row.get("result")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(result, str):
            result = json.loads(result)
        return _dump(
            row["id"], payload, row["status"], row["attempts"], row["max_attempts"],
            row["visible_at"], row["created_at"], result,
        )


def build_store() -> JobStore:
    kind = os.environ.get("FAIRQUEUE_STORE", "sqlite")
    if kind == "postgres":
        dsn = os.environ["DATABASE_URL"]
        return PostgresStore(dsn)
    path = os.environ.get("FAIRQUEUE_SQLITE", ":memory:")
    return SqliteStore(path)
