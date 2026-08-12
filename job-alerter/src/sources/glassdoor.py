from __future__ import annotations

from typing import Sequence
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

from ..models import JobPosting
from .base import HttpMixin


class GlassdoorSource(HttpMixin):
    """Best-effort Glassdoor UK job search pages."""

    name = "glassdoor"

    def __init__(self, queries: Sequence[str], location: str = "United Kingdom"):
        super().__init__(sleep_seconds=1.5)
        self.queries = list(queries)
        self.location = location

    def fetch(self) -> list[JobPosting]:
        jobs: list[JobPosting] = []
        seen: set[str] = set()
        for query in self.queries:
            q = quote_plus(query)
            loc = quote_plus(self.location)
            # fromAge=3 ≈ last 3 days when supported
            url = f"https://www.glassdoor.co.uk/Job/jobs.htm?sc.keyword={q}&locT=N&locId=2&fromAge=3&locKeyword={loc}"
            try:
                resp = self.get(url)
            except Exception:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.select("a[href*='/job-listing/'], a[href*='jobListingId=']"):
                href = a.get("href") or ""
                title = a.get_text(" ", strip=True)
                if not title or len(title) < 6:
                    continue
                full = urljoin("https://www.glassdoor.co.uk", href)
                external_id = full.split("?")[0]
                if external_id in seen:
                    continue
                seen.add(external_id)
                parent = a.find_parent(["li", "article", "div"])
                company = ""
                location = self.location
                desc = ""
                if parent:
                    desc = parent.get_text(" ", strip=True)[:2000]
                    company_el = parent.select_one("[data-test='employer-name'], .EmployerProfile, .employerName")
                    if company_el:
                        company = company_el.get_text(" ", strip=True)
                jobs.append(
                    JobPosting(
                        source=self.name,
                        external_id=external_id,
                        title=title,
                        company=company,
                        location=location,
                        url=full,
                        description=desc,
                        accepts_applications=True,
                    )
                )
        return jobs
