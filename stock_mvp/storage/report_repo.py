from __future__ import annotations

import json
from typing import Any

from stock_mvp.utils import now_utc_iso


def upsert_agent_report(
    conn,
    *,
    entity_type: str,
    entity_id: str,
    market: str,
    period_start: str,
    period_end: str,
    report_md: str,
    refs: list[dict[str, Any]],
) -> None:
    conn.execute(
        """
        INSERT INTO agent_reports(
          entity_type, entity_id, market, period_start, period_end, report_md, refs_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(entity_type, entity_id, market, period_start, period_end) DO UPDATE SET
          report_md=excluded.report_md,
          refs_json=excluded.refs_json,
          created_at=excluded.created_at
        """,
        (
            entity_type,
            entity_id,
            market.lower(),
            period_start,
            period_end,
            report_md,
            json.dumps(refs, ensure_ascii=False),
            now_utc_iso(),
        ),
    )


def latest_agent_report(conn, *, entity_type: str, entity_id: str, market: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM agent_reports
        WHERE entity_type = ?
          AND entity_id = ?
          AND lower(market) = lower(?)
        ORDER BY date(period_end) DESC, id DESC
        LIMIT 1
        """,
        (entity_type, entity_id, market),
    ).fetchone()
    if not row:
        return None
    try:
        refs = json.loads(str(row["refs_json"] or "[]"))
    except json.JSONDecodeError:
        refs = []
    return {
        "id": int(row["id"]),
        "entity_type": str(row["entity_type"]),
        "entity_id": str(row["entity_id"]),
        "market": str(row["market"]).lower(),
        "period_start": str(row["period_start"]),
        "period_end": str(row["period_end"]),
        "report_md": str(row["report_md"] or ""),
        "refs": refs if isinstance(refs, list) else [],
        "created_at": str(row["created_at"] or ""),
    }

