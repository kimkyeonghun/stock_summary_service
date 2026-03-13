from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from urllib.parse import urlparse

from stock_mvp.config import Settings
from stock_mvp.news_ingest.rss_sources import DEFAULT_KR_RSS_SOURCES
from stock_mvp.storage import rss_repo
from stock_mvp.utils import compact_text, parse_datetime_maybe, to_iso_or_none

try:
    import feedparser  # type: ignore
except Exception:  # pragma: no cover
    feedparser = None


def fetch_rss_items(conn: sqlite3.Connection, settings: Settings, *, limit_per_source: int | None = None) -> dict[str, int]:
    if feedparser is None:
        print("[WARN] KR RSS ingest skipped: feedparser is not installed.")
        return {"sources": 0, "fetched": 0, "inserted": 0, "url_duplicates": 0, "content_duplicates": 0, "errors": 0}

    sources = resolve_rss_sources(conn, settings)
    fetched = 0
    inserted = 0
    url_duplicates = 0
    content_duplicates = 0
    errors = 0
    max_items = max(1, int(limit_per_source or settings.kr_rss_max_items_per_source))

    for source in sources:
        source_name = str(source.get("source_name") or "")
        feed_url = str(source.get("feed_url") or "")
        try:
            parsed = feedparser.parse(feed_url)
            entries = list(parsed.get("entries") or [])[:max_items]
            fetched += len(entries)
            for entry in entries:
                title = compact_text(str(entry.get("title") or ""))
                snippet = compact_text(str(entry.get("summary") or entry.get("description") or ""))
                link = compact_text(str(entry.get("link") or ""))
                if not title or not link:
                    continue
                published_at = _entry_published_at(entry)
                content_hash = hashlib.sha256(f"{title}\n{snippet}".encode("utf-8")).hexdigest()
                result = rss_repo.upsert_raw_news_item(
                    conn,
                    source_name=source_name,
                    feed_url=feed_url,
                    title=title,
                    snippet=snippet,
                    original_url=link,
                    published_at=published_at,
                    raw_payload=_to_jsonable(entry),
                    content_hash=content_hash,
                    commit=False,
                )
                if bool(result.get("inserted")):
                    inserted += 1
                    if str(result.get("status") or "") == "skipped":
                        content_duplicates += 1
                else:
                    url_duplicates += 1
        except Exception as exc:  # noqa: PERF203
            errors += 1
            print(f"[WARN] rss fetch failed source={source_name} url={feed_url} error={exc}")
    conn.commit()
    return {
        "sources": len(sources),
        "fetched": fetched,
        "inserted": inserted,
        "url_duplicates": url_duplicates,
        "content_duplicates": content_duplicates,
        "errors": errors,
    }


def resolve_rss_sources(conn: sqlite3.Connection, settings: Settings) -> list[dict[str, object]]:
    configured = _parse_feed_urls_json(settings.kr_rss_feed_urls_json)
    if configured:
        return configured
    rss_repo.seed_rss_sources(conn, DEFAULT_KR_RSS_SOURCES, commit=False)
    rows = rss_repo.list_active_rss_sources(conn)
    return [
        {
            "source_id": int(row["source_id"]),
            "source_name": str(row["source_name"]),
            "feed_url": str(row["feed_url"]),
            "category": str(row["category"] or ""),
            "polling_minutes": int(row["polling_minutes"] or 60),
        }
        for row in rows
    ]


def _parse_feed_urls_json(raw: str) -> list[dict[str, object]]:
    text = compact_text(raw)
    if not text or text == "[]":
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        print("[WARN] KR_RSS_FEED_URLS_JSON parse failed. fallback to DB-seeded sources.")
        return []
    if not isinstance(data, list):
        return []

    out: list[dict[str, object]] = []
    for idx, item in enumerate(data, start=1):
        if isinstance(item, str):
            url = compact_text(item)
            if not url:
                continue
            out.append(
                {
                    "source_name": _source_name_from_url(url, idx),
                    "feed_url": url,
                    "category": "custom",
                    "polling_minutes": 20,
                }
            )
            continue
        if isinstance(item, dict):
            url = compact_text(str(item.get("feed_url") or item.get("url") or ""))
            if not url:
                continue
            out.append(
                {
                    "source_name": compact_text(str(item.get("source_name") or _source_name_from_url(url, idx))),
                    "feed_url": url,
                    "category": compact_text(str(item.get("category") or "custom")),
                    "polling_minutes": int(item.get("polling_minutes") or 20),
                }
            )
    return out


def _source_name_from_url(url: str, idx: int) -> str:
    host = compact_text(urlparse(url).netloc).replace(".", "_")
    return f"custom_{host or 'rss'}_{idx}"


def _entry_published_at(entry: dict) -> str | None:
    for key in ("published", "updated", "pubDate", "dc:date"):
        text = compact_text(str(entry.get(key) or ""))
        if not text:
            continue
        parsed = parse_datetime_maybe(text)
        if parsed is not None:
            return to_iso_or_none(parsed)
        try:
            parsed2 = datetime.fromisoformat(text)
            return to_iso_or_none(parsed2)
        except ValueError:
            continue
    return None


def _to_jsonable(entry: object) -> dict[str, object]:
    if isinstance(entry, dict):
        out: dict[str, object] = {}
        for key, value in entry.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                out[str(key)] = value
            elif isinstance(value, list):
                out[str(key)] = [str(x)[:500] for x in value[:10]]
            else:
                out[str(key)] = str(value)[:1000]
        return out
    return {"raw": str(entry)[:1000]}
