from __future__ import annotations

from typing import Sequence
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

from ..models import JobPosting
from .base import HttpMixin


class LinkedInSource(HttpMixin):
    """Best-effort LinkedIn guest job search. Often rate-limited; still useful when available."""

    name = "linkedin"

    def __init__(self, queries: Sequence[str], location: str = "United Kingdom"):
        super().__init__(sleep_seconds=2.0)
        self.queries = list(queries)
        self.location = location

    def fetch(self) -> list[JobPosting]:
        jobs: list[JobPosting] = []
        seen: set[str] = set()
        for query in self.queries:
            q = quote_plus(query)
            loc = quote_plus(self.location)
            # f_TPR=r259200 => past 3 days; f_AL=true => Easy Apply / accepting apps heuristic
            url = (
                "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
                f"?keywords={q}&location={loc}&f_TPR=r259200&start=0"
            )
            try:
                resp = self.get(url)
            except Exception:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            cards = soup.select("li")
            for card in cards:
                a = card.select_one("a.base-card__full-link, a[href*='/jobs/view/']")
                if not a:
                    continue
                href = (a.get("href") or "").split("?")[0]
                if not href:
                    continue
                full = urljoin("https://www.linkedin.com", href)
                if full in seen:
                    continue
                seen.add(full)
                title_el = card.select_one("h3, .base-search-card__title")
                company_el = card.select_one("h4, .base-search-card__subtitle")
                loc_el = card.select_one(".job-search-card__location, .base-search-card__metadata")
                title = title_el.get_text(" ", strip=True) if title_el else a.get_text(" ", strip=True)
                company = company_el.get_text(" ", strip=True) if company_el else ""
                location = loc_el.get_text(" ", strip=True) if loc_el else self.location
                jobs.append(
                    JobPosting(
                        source=self.name,
                        external_id=full,
                        title=title,
                        company=company,
                        location=location,
                        url=full,
                        description=card.get_text(" ", strip=True)[:2000],
                        accepts_applications=True,
                    )
                )
        return jobs


def linkedin_manual_search_links(queries: Sequence[str], location: str = "United Kingdom") -> list[str]:
    links = []
    for q in queries[:8]:
        links.append(
            "https://www.linkedin.com/jobs/search/?"
            f"keywords={quote_plus(q)}&location={quote_plus(location)}&f_TPR=r86400"
        )
    return links
