from __future__ import annotations

import html
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Sequence

from .config import Settings
from .models import JobPosting


def _escape(text: str) -> str:
    return html.escape(text or "")


def build_html_digest(jobs: Sequence[JobPosting], *, candidate_name: str) -> str:
    rows = []
    for i, job in enumerate(jobs, 1):
        reasons = ", ".join(job.match_reasons[:4]) if job.match_reasons else ""
        badge = " ★ target company" if job.company_priority else ""
        rows.append(
            f"""
            <tr>
              <td style="padding:12px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;">
                <div style="font-weight:600;font-size:15px;">
                  {i}. <a href="{_escape(job.url)}" style="color:#0f4c81;text-decoration:none;">{_escape(job.title)}</a>{badge}
                </div>
                <div style="color:#374151;margin-top:4px;">{_escape(job.company)} · {_escape(job.location)}</div>
                <div style="color:#6b7280;font-size:12px;margin-top:4px;">
                  { _escape(job.source) } · score {job.score:.0f}
                  {" · " + _escape(job.salary) if job.salary else ""}
                </div>
                <div style="color:#6b7280;font-size:12px;margin-top:2px;">{_escape(reasons)}</div>
              </td>
            </tr>
            """
        )

    body = "\n".join(rows) if rows else "<tr><td>Bugün eşleşen yeni ilan yok.</td></tr>"
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;color:#111827;line-height:1.45;">
      <h2 style="color:#0f4c81;">Günlük iş eşleşmeleri — {_escape(candidate_name)}</h2>
      <p style="color:#4b5563;">CV’niz ve hedef unvan/firma listenize göre filtrelenmiş, aktif başvuru kabul eden ilanlar.</p>
      <table style="width:100%;border-collapse:collapse;">{body}</table>
      <p style="margin-top:24px;color:#9ca3af;font-size:12px;">
        job-alerter · sadece aktif / açık ilanlar · LinkedIn/Indeed için API kısıtı nedeniyle ek kaynaklar kullanıldı
      </p>
    </body></html>
    """


def build_text_digest(jobs: Sequence[JobPosting], *, candidate_name: str) -> str:
    lines = [f"Günlük iş eşleşmeleri — {candidate_name}", ""]
    if not jobs:
        lines.append("Bugün eşleşen yeni ilan yok.")
        return "\n".join(lines)
    for i, job in enumerate(jobs, 1):
        star = " [TARGET]" if job.company_priority else ""
        lines.append(f"{i}. {job.title}{star}")
        lines.append(f"   {job.company} | {job.location}")
        lines.append(f"   {job.source} | score {job.score:.0f} | {job.url}")
        if job.match_reasons:
            lines.append(f"   reasons: {', '.join(job.match_reasons[:4])}")
        lines.append("")
    return "\n".join(lines)


def send_email(settings: Settings, jobs: Sequence[JobPosting], *, dry_run: bool = False) -> str:
    candidate = (settings.profile.get("candidate") or {}).get("name", "Candidate")
    subject = f"[Job Alerter] {len(jobs)} eşleşen ilan" if jobs else "[Job Alerter] Yeni eşleşme yok"
    text = build_text_digest(jobs, candidate_name=candidate)
    html_body = build_html_digest(jobs, candidate_name=candidate)

    if dry_run:
        return text

    if not settings.smtp_user or not settings.smtp_password or not settings.alert_to:
        raise RuntimeError(
            "SMTP ayarları eksik. .env içine SMTP_USER, SMTP_PASSWORD, ALERT_TO_EMAIL yazın."
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = settings.smtp_from or settings.smtp_user
    msg["To"] = settings.alert_to
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=60) as server:
        if settings.smtp_use_tls:
            server.starttls(context=context)
        server.login(settings.smtp_user, settings.smtp_password)
        server.sendmail(msg["From"], [settings.alert_to], msg.as_string())
    return subject
