from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .models import JobPosting, utc_now_iso


class JobStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    dedupe_key TEXT PRIMARY KEY,
                    fingerprint TEXT,
                    source TEXT,
                    title TEXT,
                    company TEXT,
                    location TEXT,
                    url TEXT,
                    score REAL,
                    match_reasons TEXT,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    emailed_at TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_fingerprint ON jobs(fingerprint)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_emailed ON jobs(emailed_at)"
            )

    def known_keys(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT dedupe_key FROM jobs").fetchall()
        return {r["dedupe_key"] for r in rows}

    def known_fingerprints(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT fingerprint FROM jobs WHERE fingerprint IS NOT NULL AND fingerprint != ''"
            ).fetchall()
        return {r["fingerprint"] for r in rows}

    def upsert_seen(self, job: JobPosting, emailed: bool = False) -> None:
        now = utc_now_iso()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT first_seen_at, emailed_at FROM jobs WHERE dedupe_key = ?",
                (job.dedupe_key,),
            ).fetchone()
            first_seen = existing["first_seen_at"] if existing else now
            emailed_at = existing["emailed_at"] if existing and existing["emailed_at"] else None
            if emailed:
                emailed_at = now
            conn.execute(
                """
                INSERT INTO jobs (
                    dedupe_key, fingerprint, source, title, company, location, url,
                    score, match_reasons, first_seen_at, last_seen_at, emailed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO UPDATE SET
                    fingerprint=excluded.fingerprint,
                    title=excluded.title,
                    company=excluded.company,
                    location=excluded.location,
                    url=excluded.url,
                    score=excluded.score,
                    match_reasons=excluded.match_reasons,
                    last_seen_at=excluded.last_seen_at,
                    emailed_at=COALESCE(jobs.emailed_at, excluded.emailed_at)
                """,
                (
                    job.dedupe_key,
                    job.fingerprint(),
                    job.source,
                    job.title,
                    job.company,
                    job.location,
                    job.url,
                    job.score,
                    json.dumps(job.match_reasons, ensure_ascii=False),
                    first_seen,
                    now,
                    emailed_at,
                ),
            )

    def mark_emailed(self, jobs: list[JobPosting]) -> None:
        for job in jobs:
            self.upsert_seen(job, emailed=True)
