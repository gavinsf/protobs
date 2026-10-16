"""
Postgres sink: writes captured messages in batches.
If this gets slow, the tap pauses. TODO Change the backpressure logic.
"""

import logging
import time
from pathlib import Path

import psycopg

_ROW_ERRORS = (psycopg.DataError, psycopg.IntegrityError)

log = logging.getLogger(__name__)

HERE = Path(__file__).parent
SQL_DIR = HERE / "sql"

SCHEMA = (SQL_DIR / "schema.sql").read_text()
INSERT = (SQL_DIR / "insert.sql").read_text()
ADD_PROGRESS = (SQL_DIR / "add_progress.sql").read_text()
READ_PROGRESS = (SQL_DIR / "read_progress.sql").read_text()
ADD_DEAD_LETTER = (SQL_DIR / "add_dead.sql").read_text()


class PostgresSink:
    def __init__(self, dsn: str, slow_after: float):
        self.dsn = dsn
        self.slow_after = slow_after
        self.last_write_seconds = 0.0
        self.conn = self._connect()
        self.conn.execute(SCHEMA)

    def write(self, rows: list[tuple]) -> None:
        start = time.monotonic()
        if self.conn.closed:
            self.conn = self._connect()

        latest: dict[tuple, int] = {}
        for source, topic, partition, offset, *_ in rows:
            key = (source, topic, partition)
            latest[key] = max(latest.get(key, -1), offset + 1)
        progress_rows = [(*key, nxt) for key, nxt in latest.items()]
        try:
            with self.conn.transaction(), self.conn.cursor() as cur:
                cur.executemany(INSERT, rows)
                cur.executemany(ADD_PROGRESS, progress_rows)
        except _ROW_ERRORS:
            log.warning(
                "batch rejected for bad data, isolating bad rows", exc_info=True
            )
            self._write_dead(rows, progress_rows)
        self.last_write_seconds = time.monotonic() - start

    def _write_dead(self, rows: list[tuple], progress_rows: list[tuple]) -> None:
        now_ms = int(time.time() * 1000)
        # One transaction for the whole batch: the good rows, the dead letters and
        # the progress update all commit together or not at all. Each row gets its
        # own savepoint so one rejected row doesn't abort the rest.
        with self.conn.transaction(), self.conn.cursor() as cur:
            for row in rows:
                try:
                    with self.conn.transaction():
                        cur.execute(INSERT, row)
                except _ROW_ERRORS as exc:
                    # row layout: source, topic, partition, offset, broker_ts_ms,
                    # captured_at_ms, headers, payload, ...
                    cur.execute(
                        ADD_DEAD_LETTER,
                        (
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            now_ms,
                            str(exc),
                            row[6],
                            row[7],
                        ),
                    )
                    log.error(
                        "dead-lettered %s[%d]@%d: %s", row[1], row[2], row[3], exc
                    )
            cur.executemany(ADD_PROGRESS, progress_rows)

    def is_slow(self) -> bool:
        return self.last_write_seconds > self.slow_after

    def load_progress(self, source: str) -> dict[tuple[str, int], int]:
        with self.conn.cursor() as cur:
            cur.execute(READ_PROGRESS, (source,))
            return {(topic, part): nxt for topic, part, nxt in cur.fetchall()}

    def _connect(self):
        return psycopg.connect(self.dsn, autocommit=True)

    def close(self) -> None:
        self.conn.close()
