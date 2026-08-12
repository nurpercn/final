from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class JobPosting:
    source: str
    external_id: str
    title: str
    company: str
    location: str
    url: str
    description: str = ""
    salary: str = ""
    posted_at: str = ""
    accepts_applications: bool = True
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    # filled by matcher
    score: float = 0.0
    match_reasons: list[str] = field(default_factory=list)
    company_priority: bool = False

    @property
    def dedupe_key(self) -> str:
        return f"{self.source}:{self.external_id}".lower()

    def fingerprint(self) -> str:
        """Soft de-dupe across boards: company + normalized title."""
        company = " ".join((self.company or "").casefold().split())
        title = " ".join((self.title or "").casefold().split())
        return f"{company}|{title}"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("raw", None)
        return d
