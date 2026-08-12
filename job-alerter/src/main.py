from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path

# Allow `python -m src.main` and `python src/main.py`
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_settings
from src.db import JobStore
from src.matcher import filter_and_rank
from src.models import JobPosting
from src.notifier import send_email
from src.sources import (
    AdzunaSource,
    CVLibrarySource,
    GlassdoorSource,
    IndeedRssSource,
    LinkedInSource,
    ReedSource,
    TotaljobsSource,
    indeed_manual_search_links,
    linkedin_manual_search_links,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("job-alerter")


def build_sources(settings):
    flags = settings.source_flags
    # Cap query fan-out to keep runtime/API quotas reasonable
    queries = list(settings.search_queries[:8])
    # Also search a few high-priority employers + role keywords
    priority_companies = []
    tc = settings.profile.get("target_companies") or {}
    for group in ("certification_bodies", "appliances", "hvac_cooling", "industrial_electrical"):
        priority_companies.extend((tc.get(group) or [])[:4])
    for company in priority_companies[:10]:
        queries.append(f"{company} regulatory compliance")
        queries.append(f"{company} certification")
    # de-dupe
    seen_q: set[str] = set()
    uniq_queries: list[str] = []
    for q in queries:
        k = q.casefold()
        if k not in seen_q:
            seen_q.add(k)
            uniq_queries.append(q)
    queries = uniq_queries[:18]
    location = settings.search_location
    sources = []

    if flags.get("reed", True):
        sources.append(ReedSource(settings.reed_api_key, queries, location=location))
    if flags.get("adzuna", True):
        sources.append(
            AdzunaSource(
                settings.adzuna_app_id,
                settings.adzuna_app_key,
                queries,
                country=settings.search_country,
                location=location,
                max_days_old=settings.lookback_days,
            )
        )
    if flags.get("indeed_rss", True):
        sources.append(IndeedRssSource(queries[:10], location=location))
    if flags.get("totaljobs", True):
        sources.append(TotaljobsSource(queries[:8], location=location))
    if flags.get("cv_library", True):
        sources.append(CVLibrarySource(queries[:8], location=location))
    if flags.get("glassdoor", True):
        sources.append(GlassdoorSource(queries[:6], location=location))
    if flags.get("linkedin", True):
        sources.append(LinkedInSource(queries[:10], location=location))
    return sources


def collect_jobs(settings) -> tuple[list[JobPosting], list[str]]:
    sources = build_sources(settings)
    all_jobs: list[JobPosting] = []
    errors: list[str] = []
    for source in sources:
        try:
            found = source.fetch()
            log.info("%s → %d raw listings", source.name, len(found))
            all_jobs.extend(found)
        except Exception as exc:  # noqa: BLE001 — continue other sources
            msg = f"{source.name} failed: {exc}"
            log.warning(msg)
            errors.append(msg)
            log.debug(traceback.format_exc())
    return all_jobs, errors


def dedupe_new(jobs: list[JobPosting], store: JobStore) -> list[JobPosting]:
    known_keys = store.known_keys()
    known_fps = store.known_fingerprints()
    out: list[JobPosting] = []
    local_fps: set[str] = set()
    for job in jobs:
        fp = job.fingerprint()
        if job.dedupe_key in known_keys:
            continue
        if fp in known_fps or fp in local_fps:
            continue
        local_fps.add(fp)
        out.append(job)
    return out


def run(dry_run: bool = False, send_even_if_empty: bool = False) -> int:
    settings = load_settings()
    store = JobStore(settings.db_path)

    log.info("Collecting jobs…")
    raw_jobs, errors = collect_jobs(settings)
    log.info("Collected %d raw jobs (%d source errors)", len(raw_jobs), len(errors))

    ranked = filter_and_rank(
        raw_jobs,
        target_titles=settings.target_titles,
        companies=settings.all_companies,
        cv_keywords=settings.profile.get("cv_keywords") or {},
        exclude_title_keywords=settings.profile.get("exclude_title_keywords") or [],
        min_score=settings.min_match_score,
    )
    log.info("%d jobs passed match threshold (≥ %.0f)", len(ranked), settings.min_match_score)

    new_jobs = dedupe_new(ranked, store)
    log.info("%d new jobs after de-duplication", len(new_jobs))
    new_jobs = new_jobs[: settings.max_jobs_per_email]

    # Persist all ranked (seen) so we don't re-alert forever; email only new
    for job in ranked:
        store.upsert_seen(job, emailed=False)

    manual_links = []
    manual_links.extend(indeed_manual_search_links(settings.search_queries[:6], settings.search_location))
    manual_links.extend(linkedin_manual_search_links(settings.search_queries[:6], settings.search_location))

    if dry_run:
        payload = {
            "new_jobs": [j.to_dict() for j in new_jobs],
            "errors": errors,
            "manual_search_links": manual_links,
            "raw_count": len(raw_jobs),
            "matched_count": len(ranked),
        }
        out_path = ROOT / "data" / "last_dry_run.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(send_email(settings, new_jobs, dry_run=True))
        log.info("Dry-run saved to %s", out_path)
        return 0

    if not new_jobs and not send_even_if_empty:
        log.info("No new jobs to email.")
        return 0

    subject = send_email(settings, new_jobs, dry_run=False)
    store.mark_emailed(new_jobs)
    log.info("Email sent: %s", subject)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CV-matched daily job alerter")
    parser.add_argument("--dry-run", action="store_true", help="Fetch+match, print digest, do not email")
    parser.add_argument(
        "--send-empty",
        action="store_true",
        help="Send email even when there are no new matches",
    )
    args = parser.parse_args(argv)
    return run(dry_run=args.dry_run, send_even_if_empty=args.send_empty)


if __name__ == "__main__":
    raise SystemExit(main())
