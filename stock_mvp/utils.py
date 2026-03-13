from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from dateutil import parser as dt_parser


DATE_PATTERNS = [
    "%Y.%m.%d.",
    "%Y.%m.%d",
    "%Y-%m-%d",
    "%Y/%m/%d",
]
TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "smid",
    "from",
}


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat(timespec="seconds")


def to_iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).isoformat(timespec="seconds")
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_datetime_maybe(value: str | None, base_time: datetime | None = None) -> datetime | None:
    if not value:
        return None

    text = value.strip()
    base = base_time or now_utc()

    rel_match = re.search(r"(\d+)\s*분\s*전", text)
    if rel_match:
        return base - timedelta(minutes=int(rel_match.group(1)))

    rel_match = re.search(r"(\d+)\s*시간\s*전", text)
    if rel_match:
        return base - timedelta(hours=int(rel_match.group(1)))

    rel_match = re.search(r"(\d+)\s*일\s*전", text)
    if rel_match:
        return base - timedelta(days=int(rel_match.group(1)))

    for pattern in DATE_PATTERNS:
        try:
            parsed = datetime.strptime(text, pattern)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    try:
        parsed = dt_parser.parse(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_url(url: str) -> str:
    raw = compact_text(url)
    if not raw:
        return ""

    try:
        parts = urlsplit(raw)
    except ValueError:
        return raw

    host = parts.netloc.lower()
    path = re.sub(r"/+", "/", parts.path or "/")
    query = parse_qs(parts.query, keep_blank_values=False)

    # Canonicalize Naver article URL variants to one key.
    if "news.naver.com" in host:
        m = re.search(r"/article/(\d{3})/(\d+)", path)
        if m:
            return f"https://n.news.naver.com/article/{m.group(1)}/{m.group(2)}"
        oid = _first_query_value(query, "oid")
        aid = _first_query_value(query, "aid")
        if oid and aid:
            return f"https://n.news.naver.com/article/{oid}/{aid}"

    # Canonicalize Hankyung consensus report URL.
    if "consensus.hankyung.com" in host:
        report_idx = _first_query_value(query, "report_idx")
        if report_idx:
            return f"https://consensus.hankyung.com/analysis/downpdf?report_idx={report_idx}"

    # Canonicalize Naver finance research report URL.
    if "finance.naver.com" in host and "company_read.naver" in path.lower():
        nid = _first_query_value(query, "nid")
        if nid:
            return f"https://finance.naver.com/research/company_read.naver?nid={nid}"
        path = "/research/company_read.naver"
    if "finance.naver.com" in host and "industry_read.naver" in path.lower():
        nid = _first_query_value(query, "nid")
        if nid:
            return f"https://finance.naver.com/research/industry_read.naver?nid={nid}"
        path = "/research/industry_read.naver"

    filtered = {k: v for k, v in query.items() if k.lower() not in TRACKING_QUERY_KEYS}
    if (
        "finance.naver.com" in host
        and ("company_read.naver" in path.lower() or "industry_read.naver" in path.lower())
    ):
        filtered = {k: v for k, v in filtered.items() if k.lower() != "page"}
    query_str = urlencode(sorted((k, v[0]) for k, v in filtered.items() if v), doseq=True)
    return urlunsplit((parts.scheme.lower() or "https", host, path, query_str, ""))


def document_identity_key(source: str, url: str, title: str, published_at: str | None = None) -> str:
    normalized_url = normalize_url(url)
    normalized_title = re.sub(r"\W+", "", (title or "").lower())[:120]
    day = (published_at or "")[:10] if published_at else "unknown-day"
    if normalized_url:
        return f"{source}|{normalized_url}|{day}"
    return f"{source}|title:{normalized_title}|{day}"


def url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def format_source_tag(source: str, published_at: str | None) -> str:
    if not published_at:
        return f"[{source}]"
    return f"[{source} {published_at[:16]}]"


def dedupe_document_dicts(docs: list[dict]) -> list[dict]:
    unique: list[dict] = []
    seen: set[str] = set()
    for doc in docs:
        key = document_identity_key(
            source=str(doc.get("source", "")),
            url=str(doc.get("url", "")),
            title=str(doc.get("title", "")),
            published_at=doc.get("published_at"),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(doc)
    return unique


def _first_query_value(query: dict[str, list[str]], key: str) -> str:
    values = query.get(key) or []
    return values[0] if values else ""
