"""Job alerter — daily matched job search + email digest for Nur Percin."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class Settings:
    alert_to: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    smtp_from: str
    smtp_use_tls: bool
    reed_api_key: str
    adzuna_app_id: str
    adzuna_app_key: str
    search_location: str
    search_country: str
    min_match_score: float
    max_jobs_per_email: int
    lookback_days: int
    db_path: Path
    profile: dict[str, Any] = field(default_factory=dict)

    @property
    def all_companies(self) -> list[str]:
        companies: list[str] = []
        for group in (self.profile.get("target_companies") or {}).values():
            companies.extend(group)
        # de-dupe preserving order
        seen: set[str] = set()
        out: list[str] = []
        for c in companies:
            key = c.casefold()
            if key not in seen:
                seen.add(key)
                out.append(c)
        return out

    @property
    def target_titles(self) -> list[str]:
        return list(self.profile.get("target_titles") or [])

    @property
    def search_queries(self) -> list[str]:
        return list(self.profile.get("search_queries") or self.target_titles)

    @property
    def source_flags(self) -> dict[str, bool]:
        return dict(self.profile.get("sources") or {})


def load_settings(
    env_path: Path | None = None,
    profile_path: Path | None = None,
) -> Settings:
    load_dotenv(env_path or ROOT / ".env")
    profile_file = profile_path or ROOT / "config" / "profile.yaml"
    with open(profile_file, encoding="utf-8") as f:
        profile = yaml.safe_load(f) or {}

    data_dir = ROOT / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        alert_to=os.getenv("ALERT_TO_EMAIL", profile.get("candidate", {}).get("email", "")),
        smtp_host=os.getenv("SMTP_HOST", "smtp-mail.outlook.com"),
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_password=os.getenv("SMTP_PASSWORD", ""),
        smtp_from=os.getenv("SMTP_FROM", os.getenv("SMTP_USER", "")),
        smtp_use_tls=os.getenv("SMTP_USE_TLS", "true").lower() in {"1", "true", "yes"},
        reed_api_key=os.getenv("REED_API_KEY", ""),
        adzuna_app_id=os.getenv("ADZUNA_APP_ID", ""),
        adzuna_app_key=os.getenv("ADZUNA_APP_KEY", ""),
        search_location=os.getenv("SEARCH_LOCATION", "United Kingdom"),
        search_country=os.getenv("SEARCH_COUNTRY", "gb"),
        min_match_score=float(os.getenv("MIN_MATCH_SCORE", "45")),
        max_jobs_per_email=int(os.getenv("MAX_JOBS_PER_EMAIL", "40")),
        lookback_days=int(os.getenv("LOOKBACK_DAYS", "3")),
        db_path=Path(os.getenv("DB_PATH", str(data_dir / "jobs.sqlite3"))),
        profile=profile,
    )
