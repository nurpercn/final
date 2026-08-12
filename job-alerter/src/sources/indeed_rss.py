from __future__ import annotations

from typing import Sequence
from urllib.parse import quote_plus, urlencode

import feedparser

from ..models import JobPosting
from .base import HttpMixin


class IndeedRssSource(HttpMixin):
    """Indeed UK public RSS search feeds (no API key)."""

    name = "indeed"

    def __init__(self, queries: Sequence[str], location: str = "United Kingdom", sort: str = "date"):
        super().__init__(sleep_seconds=1.0)
        self.queries = list(queries)
        self.location = location
        self.sort = sort

    def fetch(self) -> list[JobPosting]:
        jobs: list[JobPosting] = []
        seen: set[str] = set()
        for query in self.queries:
            params = urlencode({"q": query, "l": self.location, "sort": self.sort})
            url = f"https://www.indeed.co.uk/rss?{params}"
            try:
                resp = self.get(url)
            except Exception:
                # Fallback: feedparser can also fetch
                feed = feedparser.parse(url)
            else:
                feed = feedparser.parse(resp.content)

            for entry in feed.entries or []:
                link = entry.get("link") or ""
                job_id = entry.get("id") or link
                if not job_id or job_id in seen:
                    continue
                seen.add(job_id)
                title = entry.get("title") or ""
                # Indeed RSS often encodes company in title as "Title - Company"
                company = ""
                if " - " in title:
                    parts = title.rsplit(" - ", 1)
                    if len(parts) == 2:
                        title, company = parts[0], parts[1]
                summary = entry.get("summary") or entry.get("description") or ""
                jobs.append(
                    JobPosting(
                        source=self.name,
                        external_id=str(job_id),
                        title=title.strip(),
                        company=company.strip(),
                        location=self.location,
                        url=link,
                        description=summary,
                        posted_at=str(entry.get("published") or entry.get("updated") or ""),
                        accepts_applications=True,
                        raw=dict(entry),
                    )
                )
        return jobs


def indeed_manual_search_links(queries: Sequence[str], location: str = "United Kingdom") -> list[str]:
    links = []
    for q in queries[:8]:
        links.append(
            f"https://www.indeed.co.uk/jobs?q={quote_plus(q)}&l={quote_plus(location)}&fromage=3"
        )
    return links
