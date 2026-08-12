from __future__ import annotations

from typing import Sequence
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

from ..models import JobPosting
from .base import HttpMixin


class CVLibrarySource(HttpMixin):
    """Best-effort CV-Library HTML search."""

    name = "cv-library"

    def __init__(self, queries: Sequence[str], location: str = "United Kingdom"):
        super().__init__(sleep_seconds=1.2)
        self.queries = list(queries)
        self.location = location

    def fetch(self) -> list[JobPosting]:
        jobs: list[JobPosting] = []
        seen: set[str] = set()
        for query in self.queries:
            params_q = quote_plus(query)
            params_l = quote_plus(self.location)
            url = (
                f"https://www.cv-library.co.uk/search-jobs"
                f"?q={params_q}&location={params_l}&distance=50&tempperm=Any&offset=0&posted=3"
            )
            try:
                resp = self.get(url)
            except Exception:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.select("a[href*='/job/']"):
                href = a.get("href") or ""
                title = a.get_text(" ", strip=True)
                if not title or len(title) < 6:
                    continue
                full = urljoin("https://www.cv-library.co.uk", href)
                # normalize tracking params away for de-dupe
                external_id = full.split("?")[0]
                if external_id in seen:
                    continue
                seen.add(external_id)
                parent = a.find_parent(["article", "li", "div"])
                company = ""
                location = self.location
                desc = ""
                if parent:
                    text = parent.get_text(" ", strip=True)
                    desc = text[:2000]
                    company_el = parent.select_one(".job__company, .company, [data-company]")
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
