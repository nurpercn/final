from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.matcher import filter_and_rank, score_job
from src.models import JobPosting


def _base_job(**kwargs) -> JobPosting:
    data = dict(
        source="test",
        external_id="1",
        title="Regulatory Affairs Manager",
        company="Intertek",
        location="United Kingdom",
        url="https://example.com/job/1",
        description="Lead CE UKCA product compliance certification IEC 60335 market access",
        accepts_applications=True,
    )
    data.update(kwargs)
    return JobPosting(**data)


def test_target_company_and_title_score_high():
    job = score_job(
        _base_job(),
        target_titles=["Regulatory Affairs Manager", "Product Compliance Manager"],
        companies=["Intertek", "SGS", "Beko"],
        cv_keywords={
            "must_boost": ["regulatory affairs", "product compliance", "market access", "certification"],
            "strong": ["UKCA", "IEC 60335"],
            "nice": ["stakeholder management"],
        },
        exclude_title_keywords=["AML", "pharmacovigilance"],
    )
    assert job.company_priority is True
    assert job.score >= 70


def test_excludes_wrong_track_titles():
    job = score_job(
        _base_job(title="AML Financial Compliance Manager", company="HSBC"),
        target_titles=["Regulatory Affairs Manager"],
        companies=["Intertek"],
        cv_keywords={"must_boost": [], "strong": [], "nice": []},
        exclude_title_keywords=["AML", "financial compliance"],
    )
    assert job.score == 0
    assert "excluded_title" in job.match_reasons


def test_closed_jobs_filtered():
    job = _base_job(description="This job has expired and is no longer accepting applications")
    ranked = filter_and_rank(
        [job],
        target_titles=["Regulatory Affairs Manager"],
        companies=["Intertek"],
        cv_keywords={"must_boost": ["regulatory affairs"], "strong": [], "nice": []},
        exclude_title_keywords=[],
        min_score=20,
    )
    assert ranked == []


def test_filter_keeps_matching_open_jobs():
    jobs = [
        _base_job(external_id="a"),
        _base_job(
            external_id="b",
            title="Junior Barista",
            company="Cafe X",
            description="coffee",
        ),
    ]
    ranked = filter_and_rank(
        jobs,
        target_titles=["Regulatory Affairs Manager"],
        companies=["Intertek", "Beko"],
        cv_keywords={
            "must_boost": ["regulatory affairs", "certification", "market access"],
            "strong": ["UKCA", "IEC 60335"],
            "nice": [],
        },
        exclude_title_keywords=[],
        min_score=45,
    )
    assert len(ranked) == 1
    assert ranked[0].external_id == "a"


def test_rejects_finance_and_qa_false_positives():
    jobs = [
        _base_job(
            external_id="fin",
            title="Head of Regulatory Reporting – Finance",
            company="Swift",
            description="finance regulatory reporting",
        ),
        _base_job(
            external_id="qa",
            title="Quality Assurance Manager",
            company="Eaton",
            description="QA systems",
        ),
        _base_job(external_id="good"),
    ]
    ranked = filter_and_rank(
        jobs,
        target_titles=["Regulatory Affairs Manager", "Certification Manager"],
        companies=["Intertek", "Eaton", "BSI"],
        cv_keywords={
            "must_boost": ["regulatory affairs", "product compliance", "certification"],
            "strong": ["UKCA"],
            "nice": [],
        },
        exclude_title_keywords=[
            "regulatory reporting",
            "quality assurance manager",
            "finance",
            "financial",
        ],
        min_score=45,
    )
    assert [j.external_id for j in ranked] == ["good"]
