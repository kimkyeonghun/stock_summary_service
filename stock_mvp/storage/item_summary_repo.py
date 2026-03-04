from __future__ import annotations

import json
from typing import Any

from stock_mvp.utils import now_utc_iso


def list_pending_items(
    conn,
    *,
    market: str,
    ticker_codes: list[str] | None,
    lookback_days: int,
    top_n_per_stock: int,
    min_relevance: float,
    limit: int,
) -> list[dict[str, Any]]:
    sql = """
    WITH candidates AS (
      SELECT
        d.id AS item_id,
        d.stock_code,
        s.name AS stock_name,
        lower(s.market) AS market,
        d.source,
        d.doc_type,
        d.title,
        d.body,
        d.url,
        d.url_hash,
        d.published_at,
        d.collected_at,
        COALESCE(d.relevance_score, 0) AS relevance_score,
        COALESCE(d.published_at, d.collected_at) AS event_time,
        ROW_NUMBER() OVER (
          PARTITION BY d.stock_code
          ORDER BY COALESCE(d.relevance_score, 0) DESC, COALESCE(d.published_at, d.collected_at) DESC, d.id DESC
        ) AS rn
      FROM documents d
      JOIN stocks s ON s.code = d.stock_code
      LEFT JOIN item_summaries i ON i.item_id = d.id
      WHERE s.is_active = 1
        AND lower(s.market) = lower(?)
        AND (
          i.item_id IS NULL
          OR COALESCE(i.feed_one_liner, '') = ''
          OR COALESCE(i.impact_label, '') = ''
          OR COALESCE(i.detail_bullets_json, '') = ''
        )
        AND COALESCE(d.published_at, d.collected_at) >= datetime('now', ?)
        AND COALESCE(d.relevance_score, 0) >= ?
    )
    SELECT
      item_id,
      stock_code,
      stock_name,
      market,
      source,
      doc_type,
      title,
      body,
      url,
      url_hash,
      published_at,
      collected_at
    FROM candidates
    WHERE rn <= ?
    """
    params: list[object] = [market, f"-{max(1, lookback_days)} days", float(min_relevance), max(1, int(top_n_per_stock))]
    if ticker_codes:
        placeholders = ",".join("?" for _ in ticker_codes)
        sql += f" AND stock_code IN ({placeholders})"
        params.extend(ticker_codes)
    sql += " ORDER BY event_time DESC, item_id DESC LIMIT ?"
    params.append(max(1, limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def upsert_item_summary(
    conn,
    *,
    item_id: int,
    short_summary: str,
    impact_label: str = "neutral",
    feed_one_liner: str = "",
    detail_bullets: list[str] | None = None,
    related_refs: list[dict[str, Any]] | None = None,
) -> None:
    now_iso = now_utc_iso()
    conn.execute(
        """
        INSERT INTO item_summaries(
          item_id, short_summary, impact_label, feed_one_liner, detail_bullets_json, related_refs_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
          short_summary=excluded.short_summary,
          impact_label=excluded.impact_label,
          feed_one_liner=excluded.feed_one_liner,
          detail_bullets_json=excluded.detail_bullets_json,
          related_refs_json=excluded.related_refs_json,
          updated_at=excluded.updated_at
        """,
        (
            item_id,
            short_summary,
            str(impact_label or "neutral").strip().lower(),
            str(feed_one_liner or ""),
            json.dumps(list(detail_bullets or []), ensure_ascii=False),
            json.dumps(list(related_refs or []), ensure_ascii=False),
            now_iso,
            now_iso,
        ),
    )


def get_item_summary(conn, item_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
          item_id,
          short_summary,
          impact_label,
          feed_one_liner,
          detail_bullets_json,
          related_refs_json,
          created_at,
          updated_at
        FROM item_summaries
        WHERE item_id = ?
        """,
        (item_id,),
    ).fetchone()
    return dict(row) if row else None
