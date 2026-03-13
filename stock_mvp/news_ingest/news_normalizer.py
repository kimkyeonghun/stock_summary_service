from __future__ import annotations

import re
import sqlite3

import requests
from bs4 import BeautifulSoup

from stock_mvp.config import Settings
from stock_mvp.storage import news_repo, rss_repo
from stock_mvp.utils import compact_text


def normalize_pending_items(conn: sqlite3.Connection, settings: Settings, *, limit: int = 500) -> dict[str, int]:
    rows = rss_repo.list_raw_items_for_normalization(conn, limit=max(1, int(limit)))
    normalized = 0
    failed = 0
    for row in rows:
        item_id = int(row["item_id"])
        try:
            title = _normalize_text(str(row["title"] or ""))
            snippet = _normalize_text(str(row["snippet"] or ""))
            body_text = _fetch_article_body(
                str(row["original_url"] or ""),
                timeout=max(3, int(settings.request_timeout_sec)),
                verify=settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl,
                trust_env=bool(settings.crawler_trust_env),
            )
            normalized_body = _normalize_text(body_text) if body_text else snippet
            paragraphs = _split_paragraphs(normalized_body or snippet)
            lead = paragraphs[0] if paragraphs else (snippet or title)
            news_repo.upsert_normalized_news_item(
                conn,
                item_id=item_id,
                normalized_title=title or snippet,
                normalized_snippet=snippet,
                normalized_body=normalized_body,
                lead_paragraph=lead,
                body_paragraphs=paragraphs[:8],
                journalist="",
                publisher=str(row["source_name"] or ""),
                published_at=str(row["published_at"] or ""),
                commit=False,
            )
            rss_repo.update_raw_item_status(conn, item_id=item_id, status="normalized", commit=False)
            normalized += 1
        except Exception as exc:  # noqa: PERF203
            failed += 1
            rss_repo.update_raw_item_status(
                conn,
                item_id=item_id,
                status="error",
                mapping_result={"error": f"normalize_failed:{exc}"},
                commit=False,
            )
    conn.commit()
    return {"scanned": len(rows), "normalized": normalized, "failed": failed}


def _fetch_article_body(url: str, *, timeout: int, verify: bool | str, trust_env: bool) -> str:
    normalized_url = compact_text(url)
    if not normalized_url:
        return ""
    session = requests.Session()
    session.trust_env = trust_env
    try:
        resp = session.get(normalized_url, timeout=timeout, verify=verify)
        resp.raise_for_status()
    except Exception:
        return ""
    soup = BeautifulSoup(resp.text, "html.parser")
    for selector in ("article", "main", "#articletxt", "#dic_area", ".article-body", ".news_cnt_detail_wrap"):
        node = soup.select_one(selector)
        if not node:
            continue
        paragraphs = [compact_text(p.get_text(" ", strip=True)) for p in node.select("p")]
        paragraphs = [p for p in paragraphs if len(p) >= 12]
        if paragraphs:
            return "\n".join(paragraphs[:10])
    whole = compact_text(soup.get_text(" ", strip=True))
    return whole[:3000]


def _normalize_text(text: str) -> str:
    value = compact_text(text)
    if not value:
        return ""
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\[[^\]]{1,30}\]", " ", value)
    value = re.sub(r"\([^)]*기자[^)]*\)", " ", value)
    value = re.sub(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _split_paragraphs(text: str) -> list[str]:
    value = str(text or "").strip()
    if not value:
        return []
    chunks = [compact_text(x) for x in re.split(r"(?:\n+|(?<=[.!?])\s{2,})", value) if compact_text(x)]
    if chunks:
        return chunks[:8]
    return [compact_text(value)]

