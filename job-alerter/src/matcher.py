from __future__ import annotations

import re
from typing import Iterable

from .models import JobPosting


CLOSED_PATTERNS = [
    r"no longer accepting",
    r"applications?\s+closed",
    r"position\s+filled",
    r"vacancy\s+closed",
    r"this job has expired",
    r"job\s+expired",
    r"no longer available",
    r"application deadline has passed",
]


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").casefold()).strip()


def _contains_any(haystack: str, needles: Iterable[str]) -> list[str]:
    hits: list[str] = []
    for n in needles:
        n_norm = _norm(n)
        if n_norm and n_norm in haystack:
            hits.append(n)
    return hits


def is_accepting_applications(job: JobPosting) -> bool:
    if job.accepts_applications is False:
        return False
    blob = _norm(f"{job.title} {job.description}")
    for pat in CLOSED_PATTERNS:
        if re.search(pat, blob):
            return False
    return True


def title_excluded(title: str, exclude_keywords: list[str]) -> bool:
    t = _norm(title)
    return any(_norm(k) in t for k in exclude_keywords)


def company_match(company: str, targets: list[str]) -> str | None:
    c = _norm(company)
    if not c:
        return None
    # Prefer longer / more specific names first
    for target in sorted(targets, key=lambda x: len(x), reverse=True):
        t = _norm(target)
        if not t:
            continue
        if t == c or t in c or c in t:
            return target
    return None


def score_job(
    job: JobPosting,
    *,
    target_titles: list[str],
    companies: list[str],
    cv_keywords: dict,
    exclude_title_keywords: list[str],
) -> JobPosting:
    reasons: list[str] = []
    score = 0.0

    title_n = _norm(job.title)
    company_n = _norm(job.company)
    desc_n = _norm(job.description)
    blob = f"{title_n} {company_n} {desc_n}"

    if title_excluded(job.title, exclude_title_keywords):
        job.score = 0.0
        job.match_reasons = ["excluded_title"]
        return job

    if not is_accepting_applications(job):
        job.score = 0.0
        job.match_reasons = ["not_accepting_applications"]
        return job

    # Domain words that must overlap for a title to count as a match
    domain_terms = {
        "regulatory",
        "compliance",
        "certification",
        "safety",
        "market",
        "access",
        "conformity",
        "product",
    }

    # Title similarity
    best_title_hit = None
    title_tokens = set(title_n.split())
    for t in target_titles:
        tn = _norm(t)
        if not tn:
            continue
        if title_n == tn:
            score += 45
            best_title_hit = t
            break
        t_tokens = set(tn.split())
        if not t_tokens:
            continue
        overlap = len(t_tokens & title_tokens) / len(t_tokens)
        domain_overlap = t_tokens & title_tokens & domain_terms
        # Require at least one shared domain word (avoids "Manager"-only matches)
        if not domain_overlap and tn not in title_n:
            continue
        if tn in title_n or overlap >= 0.7:
            score += 35
            best_title_hit = t
            break
        if overlap >= 0.55 and domain_overlap:
            score += 22
            best_title_hit = t
            break
    if best_title_hit:
        reasons.append(f"title≈{best_title_hit}")

    # Core regulatory/compliance language in title
    core_title_terms = [
        "regulatory affairs",
        "product compliance",
        "product safety",
        "market access",
        "certification",
        "regulatory",
        "compliance",
        "conformity",
    ]
    core_hits = [t for t in core_title_terms if t in title_n]
    if core_hits:
        score += 12
        reasons.append("core_title:" + ",".join(core_hits[:3]))
    else:
        # Without domain signal in the title, keep score low unless target company + CV hits
        score -= 15
        reasons.append("weak_title_domain")

    # Company priority
    matched_company = company_match(job.company, companies)
    if matched_company:
        score += 30
        job.company_priority = True
        reasons.append(f"target_company:{matched_company}")

    # CV keywords
    must = cv_keywords.get("must_boost") or []
    strong = cv_keywords.get("strong") or []
    nice = cv_keywords.get("nice") or []

    must_hits = _contains_any(blob, must)
    strong_hits = _contains_any(blob, strong)
    nice_hits = _contains_any(blob, nice)

    score += min(25, 5 * len(must_hits))
    score += min(20, 3 * len(strong_hits))
    score += min(10, 1.5 * len(nice_hits))

    if must_hits:
        reasons.append("cv_must:" + ", ".join(must_hits[:4]))
    if strong_hits:
        reasons.append("cv_strong:" + ", ".join(strong_hits[:5]))
    if nice_hits:
        reasons.append("cv_nice:" + ", ".join(nice_hits[:3]))

    # Location soft preference
    loc = _norm(job.location)
    if any(x in loc for x in ("united kingdom", "uk", "england", "scotland", "wales", "remote", "hybrid")):
        score += 5
        reasons.append("location_ok")

    job.score = round(score, 1)
    job.match_reasons = reasons
    return job


def filter_and_rank(
    jobs: list[JobPosting],
    *,
    target_titles: list[str],
    companies: list[str],
    cv_keywords: dict,
    exclude_title_keywords: list[str],
    min_score: float,
) -> list[JobPosting]:
    scored: list[JobPosting] = []
    for job in jobs:
        scored_job = score_job(
            job,
            target_titles=target_titles,
            companies=companies,
            cv_keywords=cv_keywords,
            exclude_title_keywords=exclude_title_keywords,
        )
        if scored_job.score >= min_score and "excluded_title" not in scored_job.match_reasons:
            if "not_accepting_applications" in scored_job.match_reasons:
                continue
            scored.append(scored_job)

    # Prefer target companies, then score
    scored.sort(key=lambda j: (j.company_priority, j.score), reverse=True)
    return scored
