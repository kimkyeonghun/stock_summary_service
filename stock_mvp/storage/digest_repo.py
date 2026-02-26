from __future__ import annotations

import json
from typing import Any

from stock_mvp.utils import now_utc_iso


def upsert_daily_digest(
    conn,
    *,
    entity_type: str,
    entity_id: str,
    market: str,
    digest_date: str,
    summary_8line: str,
    change_3: str,
    open_questions: str,
    refs: list[dict[str, Any]],
) -> None:
    now_iso = now_utc_iso()
    conn.execute(
        """
        INSERT INTO daily_digests(
          entity_type, entity_id, market, digest_date, summary_8line, change_3, open_questions,
          refs_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(entity_type, entity_id, market, digest_date) DO UPDATE SET
          summary_8line=excluded.summary_8line,
          change_3=excluded.change_3,
          open_questions=excluded.open_questions,
          refs_json=excluded.refs_json,
          updated_at=excluded.updated_at
        """,
        (
            entity_type,
            entity_id,
            market.lower(),
            digest_date,
            summary_8line,
            change_3,
            open_questions,
            json.dumps(refs, ensure_ascii=False),
            now_iso,
            now_iso,
        ),
    )


def get_previous_digest(
    conn,
    *,
    entity_type: str,
    entity_id: str,
    market: str,
    digest_date: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM daily_digests
        WHERE entity_type = ?
          AND entity_id = ?
          AND lower(market) = lower(?)
          AND date(digest_date) < date(?)
        ORDER BY date(digest_date) DESC, id DESC
        LIMIT 1
        """,
        (entity_type, entity_id, market, digest_date),
    ).fetchone()
    if not row:
        return None
    return _row_to_dict(row)


def get_latest_digest(
    conn,
    *,
    entity_type: str,
    entity_id: str,
    market: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM daily_digests
        WHERE entity_type = ?
          AND entity_id = ?
          AND lower(market) = lower(?)
        ORDER BY date(digest_date) DESC, id DESC
        LIMIT 1
        """,
        (entity_type, entity_id, market),
    ).fetchone()
    if not row:
        return None
    return _row_to_dict(row)


def _row_to_dict(row) -> dict[str, Any]:
    try:
        refs = json.loads(str(row["refs_json"] or "[]"))
    except json.JSONDecodeError:
        refs = []
    return {
        "id": int(row["id"]),
        "entity_type": str(row["entity_type"]),
        "entity_id": str(row["entity_id"]),
        "market": str(row["market"]).lower(),
        "digest_date": str(row["digest_date"]),
        "summary_8line": str(row["summary_8line"] or ""),
        "change_3": str(row["change_3"] or ""),
        "open_questions": str(row["open_questions"] or ""),
        "refs": refs if isinstance(refs, list) else [],
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }

