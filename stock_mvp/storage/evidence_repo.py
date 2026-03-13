from __future__ import annotations

import json
from typing import Any

from stock_mvp.utils import now_utc_iso


def get_card_by_item_id(conn, item_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM evidence_cards WHERE item_id = ?", (item_id,)).fetchone()
    return _row_to_card(row) if row else None


def find_card_by_source_url_hash(
    conn,
    *,
    source_url_hash: str,
    market: str,
    entity_type: str = "ticker",
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM evidence_cards
        WHERE source_url_hash = ?
          AND lower(market) = lower(?)
          AND entity_type = ?
        ORDER BY datetime(created_at) DESC, rowid DESC
        LIMIT 1
        """,
        (source_url_hash, market, entity_type),
    ).fetchone()
    return _row_to_card(row) if row else None


def upsert_card(conn, card: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO evidence_cards(
          card_id, item_id, entity_type, entity_id, market, source_type, source_name, url, source_url_hash,
          published_at, fact_headline, facts_json, interpretation, risk_note, topics_json, confidence_weight, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
          card_id=excluded.card_id,
          entity_type=excluded.entity_type,
          entity_id=excluded.entity_id,
          market=excluded.market,
          source_type=excluded.source_type,
          source_name=excluded.source_name,
          url=excluded.url,
          source_url_hash=excluded.source_url_hash,
          published_at=excluded.published_at,
          fact_headline=excluded.fact_headline,
          facts_json=excluded.facts_json,
          interpretation=excluded.interpretation,
          risk_note=excluded.risk_note,
          topics_json=excluded.topics_json,
          confidence_weight=excluded.confidence_weight,
          created_at=excluded.created_at
        """,
        (
            card["card_id"],
            int(card["item_id"]),
            str(card["entity_type"]),
            str(card["entity_id"]),
            str(card["market"]).lower(),
            str(card["source_type"]),
            str(card["source_name"]),
            str(card["url"]),
            str(card["source_url_hash"]),
            str(card.get("published_at") or ""),
            str(card["fact_headline"]),
            json.dumps(list(card.get("facts") or []), ensure_ascii=False),
            str(card["interpretation"]),
            str(card["risk_note"]),
            json.dumps(list(card.get("topics") or []), ensure_ascii=False),
            float(card.get("confidence_weight") or 0.0),
            now_utc_iso(),
        ),
    )


def list_cards_for_ticker(
    conn,
    *,
    ticker: str,
    market: str,
    start_date: str,
    end_date: str,
    limit: int = 200,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM evidence_cards
        WHERE entity_type = 'ticker'
          AND entity_id = ?
          AND lower(market) = lower(?)
          AND date(COALESCE(published_at, created_at)) >= date(?)
          AND date(COALESCE(published_at, created_at)) <= date(?)
        ORDER BY date(COALESCE(published_at, created_at)) DESC, confidence_weight DESC
        LIMIT ?
        """,
        (ticker, market, start_date, end_date, max(1, limit)),
    ).fetchall()
    return [_row_to_card(r) for r in rows]


def list_cards_for_sector(
    conn,
    *,
    sector_code: str,
    market: str,
    start_date: str,
    end_date: str,
    limit: int = 300,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM (
          SELECT e.*
          FROM evidence_cards e
          JOIN stock_sector_map m ON m.stock_code = e.entity_id
          JOIN stocks s ON s.code = e.entity_id
          WHERE e.entity_type = 'ticker'
            AND m.sector_code = ?
            AND lower(s.market) = lower(?)
            AND lower(e.market) = lower(?)
            AND date(COALESCE(e.published_at, e.created_at)) >= date(?)
            AND date(COALESCE(e.published_at, e.created_at)) <= date(?)
          UNION ALL
          SELECT e.*
          FROM evidence_cards e
          WHERE e.entity_type = 'sector'
            AND upper(e.entity_id) = upper(?)
            AND lower(e.market) = lower(?)
            AND date(COALESCE(e.published_at, e.created_at)) >= date(?)
            AND date(COALESCE(e.published_at, e.created_at)) <= date(?)
        ) merged
        ORDER BY date(COALESCE(published_at, created_at)) DESC, confidence_weight DESC, datetime(created_at) DESC
        LIMIT ?
        """,
        (
            sector_code,
            market,
            market,
            start_date,
            end_date,
            sector_code,
            market,
            start_date,
            end_date,
            max(1, limit),
        ),
    ).fetchall()
    return [_row_to_card(r) for r in rows]


def list_cards_by_ids(conn, card_ids: list[str]) -> list[dict[str, Any]]:
    if not card_ids:
        return []
    placeholders = ",".join("?" for _ in card_ids)
    rows = conn.execute(
        f"SELECT * FROM evidence_cards WHERE card_id IN ({placeholders})",
        tuple(card_ids),
    ).fetchall()
    indexed = {str(r["card_id"]): _row_to_card(r) for r in rows}
    return [indexed[x] for x in card_ids if x in indexed]


def _row_to_card(row) -> dict[str, Any]:
    facts = _safe_json_loads(str(row["facts_json"] or "[]"))
    topics = _safe_json_loads(str(row["topics_json"] or "[]"))
    return {
        "card_id": str(row["card_id"]),
        "item_id": int(row["item_id"]),
        "entity_type": str(row["entity_type"]),
        "entity_id": str(row["entity_id"]),
        "market": str(row["market"]).lower(),
        "source_type": str(row["source_type"]),
        "source_name": str(row["source_name"]),
        "url": str(row["url"]),
        "source_url_hash": str(row["source_url_hash"]),
        "published_at": str(row["published_at"] or ""),
        "fact_headline": str(row["fact_headline"]),
        "facts": facts if isinstance(facts, list) else [],
        "interpretation": str(row["interpretation"]),
        "risk_note": str(row["risk_note"]),
        "topics": topics if isinstance(topics, list) else [],
        "confidence_weight": float(row["confidence_weight"] or 0.0),
        "created_at": str(row["created_at"] or ""),
    }


def _safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return []
