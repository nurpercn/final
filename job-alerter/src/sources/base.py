from __future__ import annotations

import time
from typing import Protocol

import requests

from ..models import JobPosting


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; JobAlerter/1.0; +https://github.com/nurpercn/final) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}


class JobSource(Protocol):
    name: str

    def fetch(self) -> list[JobPosting]:
        ...


class HttpMixin:
    def __init__(self, sleep_seconds: float = 0.8):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.sleep_seconds = sleep_seconds

    def get(self, url: str, **kwargs) -> requests.Response:
        time.sleep(self.sleep_seconds)
        resp = self.session.get(url, timeout=45, **kwargs)
        resp.raise_for_status()
        return resp
