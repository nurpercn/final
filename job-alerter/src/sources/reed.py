from __future__ import annotations

from typing import Sequence

from ..models import JobPosting
from .base import HttpMixin


class ReedSource(HttpMixin):
    """Official Reed.co.uk jobs API: https://www.reed.co.uk/developers"""

    name = "reed"

    def __init__(self, api_key: str, queries: Sequence[str], location: str, results_to_take: int = 50):
        super().__init__(sleep_seconds=0.4)
        self.api_key = api_key
        self.queries = list(queries)
        self.location = location
        self.results_to_take = results_to_take

    def fetch(self) -> list[JobPosting]:
        if not self.api_key:
            return []
        jobs: list[JobPosting] = []
        seen: set[str] = set()
        for query in self.queries:
            params = {
                "keywords": query,
                "locationName": self.location,
                "resultsToTake": self.results_to_take,
                # Prefer recently posted
                "postedByRecruitmentAgency": "false",
            }
            resp = self.session.get(
                "https://www.reed.co.uk/api/1.0/search",
                params=params,
                auth=(self.api_key, ""),
                timeout=45,
            )
            if resp.status_code == 401:
                raise RuntimeError("Reed API key invalid (401)")
            resp.raise_for_status()
            payload = resp.json() or {}
            for item in payload.get("results") or []:
                job_id = str(item.get("jobId") or "")
                if not job_id or job_id in seen:
                    continue
                seen.add(job_id)
                salary_parts = []
                if item.get("minimumSalary"):
                    salary_parts.append(str(item["minimumSalary"]))
                if item.get("maximumSalary"):
                    salary_parts.append(str(item["maximumSalary"]))
                salary = " - ".join(salary_parts)
                if item.get("currency"):
                    salary = f"{item['currency']} {salary}".strip()
                jobs.append(
                    JobPosting(
                        source=self.name,
                        external_id=job_id,
                        title=item.get("jobTitle") or "",
                        company=item.get("employerName") or "",
                        location=item.get("locationName") or "",
                        url=item.get("jobUrl") or f"https://www.reed.co.uk/jobs/{job_id}",
                        description=item.get("jobDescription") or "",
                        salary=salary,
                        posted_at=str(item.get("date") or ""),
                        accepts_applications=True,
                        raw=item,
                    )
                )
        return jobs
