from __future__ import annotations

from typing import Sequence

from ..models import JobPosting
from .base import HttpMixin


class AdzunaSource(HttpMixin):
    """Adzuna UK job search API — aggregates many boards including Indeed-like listings."""

    name = "adzuna"

    def __init__(
        self,
        app_id: str,
        app_key: str,
        queries: Sequence[str],
        country: str = "gb",
        location: str = "United Kingdom",
        max_days_old: int = 3,
        results_per_page: int = 50,
    ):
        super().__init__(sleep_seconds=0.5)
        self.app_id = app_id
        self.app_key = app_key
        self.queries = list(queries)
        self.country = country
        self.location = location
        self.max_days_old = max_days_old
        self.results_per_page = results_per_page

    def fetch(self) -> list[JobPosting]:
        if not self.app_id or not self.app_key:
            return []
        jobs: list[JobPosting] = []
        seen: set[str] = set()
        for query in self.queries:
            url = f"https://api.adzuna.com/v1/api/jobs/{self.country}/search/1"
            params = {
                "app_id": self.app_id,
                "app_key": self.app_key,
                "what": query,
                "where": self.location,
                "results_per_page": self.results_per_page,
                "max_days_old": self.max_days_old,
                "content-type": "application/json",
            }
            resp = self.get(url, params=params)
            payload = resp.json() or {}
            for item in payload.get("results") or []:
                job_id = str(item.get("id") or "")
                if not job_id or job_id in seen:
                    continue
                seen.add(job_id)
                company = ((item.get("company") or {}).get("display_name")) or ""
                location = ((item.get("location") or {}).get("display_name")) or ""
                salary = ""
                if item.get("salary_min") or item.get("salary_max"):
                    salary = f"{item.get('salary_min', '')} - {item.get('salary_max', '')}".strip(" -")
                jobs.append(
                    JobPosting(
                        source=self.name,
                        external_id=job_id,
                        title=item.get("title") or "",
                        company=company,
                        location=location,
                        url=item.get("redirect_url") or "",
                        description=item.get("description") or "",
                        salary=salary,
                        posted_at=str(item.get("created") or ""),
                        accepts_applications=True,
                        raw=item,
                    )
                )
        return jobs
