from __future__ import annotations

import sqlite3

from stock_mvp.database import upsert_document_entity_mapping, upsert_news_entity_map


def upsert_ticker_mapping_for_document(
    conn: sqlite3.Connection,
    *,
    document_id: int,
    ticker: str,
    raw_score: float,
    reason: dict[str, object],
    is_primary: bool = True,
    commit: bool = True,
) -> None:
    normalized_score = min(1.0, max(0.0, float(raw_score)) / 12.0)
    confidence = "high" if raw_score >= 9 else "medium" if raw_score >= 7 else "low"
    upsert_document_entity_mapping(
        conn,
        document_id=document_id,
        entity_type="ticker",
        entity_id=ticker,
        score=normalized_score,
        reason=reason,
        commit=False,
    )
    upsert_news_entity_map(
        conn,
        item_id=document_id,
        entity_type="ticker",
        entity_id=ticker,
        score=float(raw_score),
        confidence=confidence,
        mapping_reason=reason,
        is_primary=is_primary,
        commit=False,
    )
    if commit:
        conn.commit()


def upsert_sector_mapping_for_document(
    conn: sqlite3.Connection,
    *,
    document_id: int,
    sector_code: str,
    raw_score: float,
    reason: dict[str, object],
    is_primary: bool = True,
    commit: bool = True,
) -> None:
    normalized_score = min(1.0, max(0.0, float(raw_score)) / 12.0)
    confidence = "high" if raw_score >= 9 else "medium" if raw_score >= 7 else "low"
    upsert_document_entity_mapping(
        conn,
        document_id=document_id,
        entity_type="sector",
        entity_id=sector_code,
        score=normalized_score,
        reason=reason,
        commit=False,
    )
    upsert_news_entity_map(
        conn,
        item_id=document_id,
        entity_type="sector",
        entity_id=sector_code,
        score=float(raw_score),
        confidence=confidence,
        mapping_reason=reason,
        is_primary=is_primary,
        commit=False,
    )
    if commit:
        conn.commit()

