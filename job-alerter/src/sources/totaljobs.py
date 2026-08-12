from __future__ import annotations

from typing import Sequence
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

from ..models import JobPosting
from .base import HttpMixin


class TotaljobsSource(HttpMixin):
    """Best-effort Totaljobs HTML search (public pages)."""

    name = "totaljobs"

    def __init__(self, queries: Sequence[str], location: str = "United Kingdom"):
        super().__init__(sleep_seconds=1.2)
        self.queries = list(queries)
        self.location = location

    def fetch(self) -> list[JobPosting]:
        jobs: list[JobPosting] = []
        seen: set[str] = set()
        for query in self.queries:
            q = quote_plus(query)
            loc = quote_plus(self.location)
            url = f"https://www.totaljobs.com/jobs/{q}/in-{loc}?postedwithin=3"
            try:
                resp = self.get(url)
            except Exception:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            cards = soup.select("[data-testid='job-item'], article, .job")
            if not cards:
                # Fallback: anchor heuristics
                for a in soup.select("a[href*='/job/']"):
                    href = a.get("href") or ""
                    title = a.get_text(" ", strip=True)
                    if not title or len(title) < 8:
                        continue
                    full = urljoin("https://www.totaljobs.com", href)
                    if full in seen:
                        continue
                    seen.add(full)
                    jobs.append(
                        JobPosting(
                            source=self.name,
                            external_id=full,
                            title=title,
                            company="",
                            location=self.location,
                            url=full,
                            description="",
                            accepts_applications=True,
                        )
                    )
                continue

            for card in cards:
                a = card.select_one("a[href*='/job/'], a[href*='/jobs/']")
                if not a:
                    continue
                href = a.get("href") or ""
                full = urljoin("https://www.totaljobs.com", href)
                if full in seen:
                    continue
                seen.add(full)
                title = a.get_text(" ", strip=True)
                company_el = card.select_one("[data-testid='job-item-company'], .company, .res-company")
                company = company_el.get_text(" ", strip=True) if company_el else ""
                loc_el = card.select_one("[data-testid='job-item-location'], .location")
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
