from __future__ import annotations

from typing import Any

from stock_mvp.utils import now_utc_iso


def list_pending_items(
    conn,
    *,
    market: str,
    ticker_codes: list[str] | None,
    lookback_days: int,
    limit: int,
) -> list[dict[str, Any]]:
    sql = """
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
      d.collected_at
    FROM documents d
    JOIN stocks s ON s.code = d.stock_code
    LEFT JOIN item_summaries i ON i.item_id = d.id
    WHERE s.is_active = 1
      AND lower(s.market) = lower(?)
      AND i.item_id IS NULL
      AND COALESCE(d.published_at, d.collected_at) >= datetime('now', ?)
    """
    params: list[object] = [market, f"-{max(1, lookback_days)} days"]
    if ticker_codes:
        placeholders = ",".join("?" for _ in ticker_codes)
        sql += f" AND d.stock_code IN ({placeholders})"
        params.extend(ticker_codes)
    sql += " ORDER BY COALESCE(d.published_at, d.collected_at) DESC, d.id DESC LIMIT ?"
    params.append(max(1, limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def upsert_item_summary(conn, *, item_id: int, short_summary: str) -> None:
    conn.execute(
        """
        INSERT INTO item_summaries(item_id, short_summary, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
          short_summary=excluded.short_summary,
          created_at=excluded.created_at
        """,
        (item_id, short_summary, now_utc_iso()),
    )


def get_item_summary(conn, item_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT item_id, short_summary, created_at FROM item_summaries WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    return dict(row) if row else None

