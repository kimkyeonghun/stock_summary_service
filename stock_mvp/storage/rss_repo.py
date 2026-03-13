from __future__ import annotations

import json
import sqlite3

from stock_mvp.utils import compact_text, normalize_url, now_utc_iso, url_hash


def upsert_rss_source(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    feed_url: str,
    category: str = "",
    polling_minutes: int = 60,
    is_active: bool = True,
    commit: bool = True,
) -> None:
    now_iso = now_utc_iso()
    conn.execute(
        """
        INSERT INTO rss_sources(source_name, feed_url, category, is_active, polling_minutes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(feed_url) DO UPDATE SET
          source_name=excluded.source_name,
          category=excluded.category,
          is_active=excluded.is_active,
          polling_minutes=excluded.polling_minutes,
          updated_at=excluded.updated_at
        """,
        (
            compact_text(source_name) or "kr_rss",
            compact_text(feed_url),
            compact_text(category),
            1 if is_active else 0,
            max(1, int(polling_minutes)),
            now_iso,
            now_iso,
        ),
    )
    if commit:
        conn.commit()


def seed_rss_sources(conn: sqlite3.Connection, sources: list[dict[str, object]], *, commit: bool = True) -> int:
    if not sources:
        return 0
    for source in sources:
        upsert_rss_source(
            conn,
            source_name=str(source.get("source_name") or ""),
            feed_url=str(source.get("feed_url") or ""),
            category=str(source.get("category") or ""),
            polling_minutes=int(source.get("polling_minutes") or 60),
            is_active=bool(source.get("is_active", True)),
            commit=False,
        )
    if commit:
        conn.commit()
    return len(sources)


def list_active_rss_sources(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT source_id, source_name, feed_url, category, is_active, polling_minutes
        FROM rss_sources
        WHERE is_active = 1
        ORDER BY source_name, source_id
        """
    ).fetchall()


def upsert_raw_news_item(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    feed_url: str,
    title: str,
    snippet: str,
    original_url: str,
    published_at: str | None,
    raw_payload: dict[str, object] | None,
    content_hash: str,
    commit: bool = True,
) -> dict[str, object]:
    normalized_url = normalize_url(original_url) or compact_text(original_url)
    original_url_hash = url_hash(normalized_url)
    now_iso = now_utc_iso()
    duplicate_row = conn.execute(
        """
        SELECT item_id
        FROM raw_news_items
        WHERE content_hash = ?
          AND original_url_hash <> ?
        ORDER BY item_id DESC
        LIMIT 1
        """,
        (content_hash, original_url_hash),
    ).fetchone()
    duplicate_of_item_id = int(duplicate_row["item_id"]) if duplicate_row else None
    status = "skipped" if duplicate_of_item_id else "new"

    cursor = conn.execute(
        """
        INSERT INTO raw_news_items(
          source_name, feed_url, title, snippet, original_url, original_url_hash, published_at,
          fetched_at, content_hash, raw_payload_json, status, duplicate_of_item_id,
          mapping_result_json, mapped_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', NULL, ?)
        ON CONFLICT(original_url_hash) DO NOTHING
        """,
        (
            compact_text(source_name),
            compact_text(feed_url),
            compact_text(title),
            compact_text(snippet),
            normalized_url,
            original_url_hash,
            compact_text(str(published_at or "")) or None,
            now_iso,
            compact_text(content_hash),
            json.dumps(raw_payload or {}, ensure_ascii=False),
            status,
            duplicate_of_item_id,
            now_iso,
        ),
    )
    inserted = int(cursor.rowcount or 0) > 0
    row = conn.execute(
        "SELECT item_id, status, duplicate_of_item_id FROM raw_news_items WHERE original_url_hash = ?",
        (original_url_hash,),
    ).fetchone()
    item_id = int(row["item_id"]) if row else 0
    row_status = str(row["status"]) if row else status
    duplicate_ref = int(row["duplicate_of_item_id"]) if row and row["duplicate_of_item_id"] else None
    if commit:
        conn.commit()
    return {
        "item_id": item_id,
        "inserted": inserted,
        "status": row_status,
        "duplicate_of_item_id": duplicate_ref,
        "url_hash": original_url_hash,
    }


def list_raw_items_for_normalization(conn: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT r.*
        FROM raw_news_items r
        LEFT JOIN normalized_news_items n ON n.item_id = r.item_id
        WHERE r.status = 'new'
          AND n.item_id IS NULL
        ORDER BY COALESCE(r.published_at, r.fetched_at) DESC, r.item_id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()


def list_normalized_items_for_mapping(conn: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          r.item_id,
          r.source_name,
          r.feed_url,
          r.original_url,
          r.published_at,
          r.status,
          n.normalized_title,
          n.normalized_snippet,
          n.normalized_body,
          n.lead_paragraph,
          n.body_paragraphs_json,
          n.publisher
        FROM raw_news_items r
        JOIN normalized_news_items n ON n.item_id = r.item_id
        WHERE r.status = 'normalized'
        ORDER BY COALESCE(r.published_at, r.fetched_at) DESC, r.item_id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()


def update_raw_item_status(
    conn: sqlite3.Connection,
    *,
    item_id: int,
    status: str,
    mapping_result: dict[str, object] | None = None,
    commit: bool = True,
) -> None:
    now_iso = now_utc_iso()
    conn.execute(
        """
        UPDATE raw_news_items
        SET
          status = ?,
          mapping_result_json = CASE WHEN ? IS NULL THEN mapping_result_json ELSE ? END,
          mapped_at = CASE
            WHEN ? IN ('mapped', 'skipped') THEN ?
            ELSE mapped_at
          END,
          updated_at = ?
        WHERE item_id = ?
        """,
        (
            compact_text(status).lower() or "new",
            None if mapping_result is None else 1,
            json.dumps(mapping_result or {}, ensure_ascii=False),
            compact_text(status).lower(),
            now_iso,
            now_iso,
            int(item_id),
        ),
    )
    if commit:
        conn.commit()

