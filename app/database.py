"""SQLite persistence with a single-writer connection and idempotent inserts."""

import asyncio
import os
import sqlite3
import time

import aiosqlite

DB_PATH = os.getenv("WEBHOOK_DB_PATH", "webhooks.db")


class Database:
    def __init__(self, db_path: str | None = None):
        self._path = db_path or os.getenv("WEBHOOK_DB_PATH", DB_PATH)
        self._conn: aiosqlite.Connection | None = None
        # aiosqlite uses one background thread. The lock keeps transactions
        # from interleaving statements across coroutines on that connection.
        self._lock = asyncio.Lock()
        self._pending: list[tuple[str, str, str, str, asyncio.Future[bool]]] = []
        self._flush_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        # isolation_level=None is autocommit; multi-statement work uses BEGIN.
        self._conn = await aiosqlite.connect(self._path, isolation_level=None, timeout=5.0)
        self._conn.row_factory = aiosqlite.Row
        async with self._lock:
            # WAL lets the delivery loop read while ingest commits, and
            # busy_timeout waits out the single-writer lock instead of failing.
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")
            await self._conn.execute("PRAGMA busy_timeout=5000")
            await self._conn.execute("PRAGMA foreign_keys=ON")

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def init_schema(self) -> None:
        assert self._conn is not None
        async with self._lock:
            await self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    tenant_id TEXT NOT NULL,
                    raw_content TEXT,
                    cleaned_text TEXT NOT NULL DEFAULT '',
                    fingerprint TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    claimed_at REAL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_events_due
                ON events (status, next_attempt_at)
                """
            )

    async def insert_pending(
        self,
        event_id: str,
        idempotency_key: str,
        tenant_id: str,
        raw_content: str,
    ) -> bool:
        """Insert the event or report that an identical delivery already won the race.

        Requests that arrive in the same loop turn are committed together.
        UNIQUE(event_id) and UNIQUE(idempotency_key) still make each insert the
        atomic idempotency check: a duplicate in the same batch loses, and a
        duplicate that arrives later hits the committed row.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()
        self._pending.append((event_id, idempotency_key, tenant_id, raw_content, future))
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_inserts())
        return await future

    async def _flush_inserts(self) -> None:
        # Yield once so the rest of this burst can join the same commit.
        await asyncio.sleep(0)
        while self._pending:
            batch = self._pending
            self._pending = []
            try:
                results = await self._insert_batch(batch)
            except Exception as exc:
                for *_, future in batch:
                    if not future.done():
                        future.set_exception(exc)
                continue
            for (*_, future), inserted in zip(batch, results):
                if not future.done():
                    future.set_result(inserted)

    async def _insert_batch(
        self,
        batch: list[tuple[str, str, str, str, asyncio.Future[bool]]],
    ) -> list[bool]:
        assert self._conn is not None
        rows = [
            (event_id, idempotency_key, tenant_id, raw_content)
            for event_id, idempotency_key, tenant_id, raw_content, _future in batch
        ]
        # One hop to the SQLite thread for the whole burst, not one hop per row.
        async with self._lock:
            return await self._conn._execute(self._write_rows, rows)

    def _write_rows(self, rows: list[tuple[str, str, str, str]]) -> list[bool]:
        assert self._conn is not None
        conn = self._conn._conn
        results: list[bool] = []
        conn.execute("BEGIN IMMEDIATE")
        try:
            for event_id, idempotency_key, tenant_id, raw_content in rows:
                try:
                    conn.execute("SAVEPOINT row_insert")
                    conn.execute(
                        """
                        INSERT INTO events (
                            event_id, idempotency_key, tenant_id, raw_content,
                            status, next_attempt_at
                        )
                        VALUES (?, ?, ?, ?, 'pending', 0)
                        """,
                        (event_id, idempotency_key, tenant_id, raw_content),
                    )
                    conn.execute("RELEASE row_insert")
                    results.append(True)
                except sqlite3.IntegrityError:
                    conn.execute("ROLLBACK TO row_insert")
                    conn.execute("RELEASE row_insert")
                    results.append(False)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return results

    async def claim_due(self, limit: int, now: float | None = None) -> list[str]:
        """Atomically move due pending rows to `processing` and return their ids."""
        assert self._conn is not None
        now = time.time() if now is None else now
        async with self._lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self._conn.execute(
                    """
                    SELECT event_id FROM events
                    WHERE status = 'pending' AND next_attempt_at <= ?
                    ORDER BY next_attempt_at, id
                    LIMIT ?
                    """,
                    (now, limit),
                )
                rows = await cursor.fetchall()
                claimed: list[str] = []
                for row in rows:
                    event_id = row["event_id"]
                    await self._conn.execute(
                        """
                        UPDATE events
                        SET status = 'processing',
                            claimed_at = ?,
                            attempts = attempts + 1
                        WHERE event_id = ? AND status = 'pending'
                        """,
                        (now, event_id),
                    )
                    changed = await self._conn.execute("SELECT changes()")
                    changed_row = await changed.fetchone()
                    if changed_row is not None and int(changed_row[0]) == 1:
                        claimed.append(event_id)
                await self._conn.commit()
                return claimed
            except Exception:
                await self._conn.rollback()
                raise

    async def reclaim_processing(self) -> None:
        """Requeue rows left in `processing` by a process that is no longer running.

        Called only at startup. A single-process deploy has no live claim that
        this could steal. Running it on every worker at once is not safe.
        """
        assert self._conn is not None
        async with self._lock:
            await self._conn.execute(
                """
                UPDATE events
                SET status = 'pending', claimed_at = NULL, next_attempt_at = 0
                WHERE status = 'processing'
                """
            )

    async def get_event(self, event_id: str) -> sqlite3.Row | None:
        assert self._conn is not None
        async with self._lock:
            cursor = await self._conn.execute(
                """
                SELECT event_id, tenant_id, raw_content, cleaned_text,
                       fingerprint, status, attempts
                FROM events
                WHERE event_id = ?
                """,
                (event_id,),
            )
            return await cursor.fetchone()

    async def save_cleaned(self, event_id: str, cleaned_text: str, fingerprint: str) -> None:
        assert self._conn is not None
        async with self._lock:
            # Drop the raw payload once the redacted form is durable.
            await self._conn.execute(
                """
                UPDATE events
                SET cleaned_text = ?, fingerprint = ?, raw_content = NULL
                WHERE event_id = ?
                """,
                (cleaned_text, fingerprint, event_id),
            )

    async def mark_delivered(self, event_id: str) -> None:
        assert self._conn is not None
        async with self._lock:
            await self._conn.execute(
                """
                UPDATE events
                SET status = 'delivered', raw_content = NULL, claimed_at = NULL
                WHERE event_id = ?
                """,
                (event_id,),
            )

    async def schedule_retry(self, event_id: str, delay_seconds: float) -> None:
        assert self._conn is not None
        next_at = time.time() + delay_seconds
        async with self._lock:
            await self._conn.execute(
                """
                UPDATE events
                SET status = 'pending', claimed_at = NULL, next_attempt_at = ?
                WHERE event_id = ? AND status != 'delivered'
                """,
                (next_at, event_id),
            )

    async def mark_failed(self, event_id: str) -> None:
        assert self._conn is not None
        async with self._lock:
            await self._conn.execute(
                """
                UPDATE events
                SET status = 'failed', claimed_at = NULL, raw_content = NULL
                WHERE event_id = ? AND status != 'delivered'
                """,
                (event_id,),
            )
