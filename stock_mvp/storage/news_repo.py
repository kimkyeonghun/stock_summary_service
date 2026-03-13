from __future__ import annotations

import json
import sqlite3

from stock_mvp.utils import compact_text, now_utc_iso


def upsert_normalized_news_item(
    conn: sqlite3.Connection,
    *,
    item_id: int,
    normalized_title: str,
    normalized_snippet: str,
    normalized_body: str,
    lead_paragraph: str,
    body_paragraphs: list[str],
    journalist: str = "",
    publisher: str = "",
    published_at: str | None = None,
    commit: bool = True,
) -> None:
    now_iso = now_utc_iso()
    conn.execute(
        """
        INSERT INTO normalized_news_items(
          item_id, normalized_title, normalized_snippet, normalized_body, lead_paragraph,
          body_paragraphs_json, journalist, publisher, published_at, normalized_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
          normalized_title=excluded.normalized_title,
          normalized_snippet=excluded.normalized_snippet,
          normalized_body=excluded.normalized_body,
          lead_paragraph=excluded.lead_paragraph,
          body_paragraphs_json=excluded.body_paragraphs_json,
          journalist=excluded.journalist,
          publisher=excluded.publisher,
          published_at=excluded.published_at,
          normalized_at=excluded.normalized_at
        """,
        (
            int(item_id),
            compact_text(normalized_title),
            compact_text(normalized_snippet),
            compact_text(normalized_body),
            compact_text(lead_paragraph),
            json.dumps([compact_text(x) for x in body_paragraphs if compact_text(x)][:8], ensure_ascii=False),
            compact_text(journalist),
            compact_text(publisher),
            compact_text(str(published_at or "")) or None,
            now_iso,
        ),
    )
    if commit:
        conn.commit()


def get_normalized_news_item(conn: sqlite3.Connection, *, item_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
          item_id, normalized_title, normalized_snippet, normalized_body, lead_paragraph,
          body_paragraphs_json, journalist, publisher, published_at, normalized_at
        FROM normalized_news_items
        WHERE item_id = ?
        """,
        (int(item_id),),
    ).fetchone()

