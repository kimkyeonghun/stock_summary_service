from __future__ import annotations

import json
import sqlite3

from stock_mvp.config import Settings
from stock_mvp.database import insert_documents, upsert_sector_document_by_code
from stock_mvp.models import CollectedDocument
from stock_mvp.news_ingest import entity_mapper
from stock_mvp.storage import evidence_repo, mapping_repo, rss_repo
from stock_mvp.utils import compact_text, parse_datetime_maybe, url_hash


KR_RSS_DOC_SOURCE = "kr_rss"
KR_RSS_SHADOW_STOCK_CODE = "KR_SECTOR_RSS"


def map_and_route_pending_items(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 300,
    allowed_tickers: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    rows = rss_repo.list_normalized_items_for_mapping(conn, limit=max(1, int(limit)))
    mapped_ticker = 0
    mapped_sector = 0
    unassigned = 0
    routed_documents = 0
    routed_sector_documents = 0

    for row in rows:
        item_id = int(row["item_id"])
        mapping = entity_mapper.map_normalized_item(
            conn,
            item_id=item_id,
            normalized_title=str(row["normalized_title"] or ""),
            normalized_snippet=str(row["normalized_snippet"] or ""),
            normalized_body=str(row["normalized_body"] or ""),
            lead_paragraph=str(row["lead_paragraph"] or ""),
            ticker_threshold=float(settings.kr_rss_ticker_threshold),
            sector_threshold=float(settings.kr_rss_sector_threshold),
            max_tickers=max(1, int(settings.kr_rss_max_tickers_per_item)),
            allowed_tickers=allowed_tickers,
        )
        mapping_json = entity_mapper.to_json(mapping)

        if dry_run:
            if mapping.status.startswith("mapped_ticker"):
                mapped_ticker += 1
            elif mapping.status == "mapped_sector":
                mapped_sector += 1
            else:
                unassigned += 1
            rss_repo.update_raw_item_status(conn, item_id=item_id, status="normalized", mapping_result=mapping_json, commit=False)
            continue

        if mapping.status.startswith("mapped_ticker"):
            ticker_candidates = [
                candidate
                for candidate in mapping.ticker_candidates
                if candidate.score >= float(settings.kr_rss_ticker_threshold)
            ][: max(1, int(settings.kr_rss_max_tickers_per_item))]
            inserted_docs = _route_ticker_documents(
                conn,
                row=row,
                ticker_candidates=ticker_candidates,
                mapping_json=mapping_json,
            )
            mapped_ticker += 1
            routed_documents += inserted_docs
            rss_repo.update_raw_item_status(conn, item_id=item_id, status="mapped", mapping_result=mapping_json, commit=False)
            continue

        if mapping.status == "mapped_sector" and mapping.primary is not None:
            inserted_sector_doc = _route_sector_document(
                conn,
                row=row,
                sector_code=mapping.primary.entity_id,
                mapping_json=mapping_json,
            )
            mapped_sector += 1
            routed_sector_documents += 1 if inserted_sector_doc else 0
            rss_repo.update_raw_item_status(conn, item_id=item_id, status="mapped", mapping_result=mapping_json, commit=False)
            continue

        unassigned += 1
        rss_repo.update_raw_item_status(conn, item_id=item_id, status="skipped", mapping_result=mapping_json, commit=False)

    conn.commit()
    return {
        "scanned": len(rows),
        "mapped_ticker": mapped_ticker,
        "mapped_sector": mapped_sector,
        "unassigned": unassigned,
        "routed_documents": routed_documents,
        "routed_sector_documents": routed_sector_documents,
    }


def _route_ticker_documents(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    ticker_candidates: list[entity_mapper.MappingCandidate],
    mapping_json: dict[str, object],
) -> int:
    inserted_count = 0
    primary_ticker = ticker_candidates[0].entity_id if ticker_candidates else ""
    for candidate in ticker_candidates:
        doc = CollectedDocument(
            stock_code=str(candidate.entity_id),
            source=KR_RSS_DOC_SOURCE,
            doc_type="news",
            title=compact_text(str(row["normalized_title"] or "")),
            url=compact_text(str(row["original_url"] or "")),
            published_at=parse_datetime_maybe(str(row["published_at"] or "")),
            body=_build_document_body(row),
            relevance_score=min(1.0, max(0.0, float(candidate.score)) / 12.0),
            relevance_reason=f"kr_rss_mapping:{candidate.score:.2f}",
            matched_alias="",
        )
        inserted, _skipped = insert_documents(conn, [doc], commit=False)
        inserted_count += inserted
        document_id = _find_document_id(
            conn,
            stock_code=str(candidate.entity_id),
            source=KR_RSS_DOC_SOURCE,
            url=str(row["original_url"] or ""),
        )
        if document_id <= 0:
            continue
        reason = {
            "source": "kr_rss",
            "raw_item_id": int(row["item_id"]),
            "candidate_score": float(candidate.score),
            "candidate_reason": candidate.reason,
            "mapping": mapping_json,
        }
        mapping_repo.upsert_ticker_mapping_for_document(
            conn,
            document_id=document_id,
            ticker=str(candidate.entity_id),
            raw_score=float(candidate.score),
            reason=reason,
            is_primary=str(candidate.entity_id).upper() == str(primary_ticker).upper(),
            commit=False,
        )
    return inserted_count


def _route_sector_document(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    sector_code: str,
    mapping_json: dict[str, object],
) -> bool:
    inserted = upsert_sector_document_by_code(
        conn,
        sector_code=str(sector_code),
        source=KR_RSS_DOC_SOURCE,
        doc_type="news",
        title=compact_text(str(row["normalized_title"] or "")),
        url=compact_text(str(row["original_url"] or "")),
        published_at=str(row["published_at"] or ""),
        body=_build_document_body(row),
        commit=False,
    )
    _ensure_shadow_stock(conn)
    doc = CollectedDocument(
        stock_code=KR_RSS_SHADOW_STOCK_CODE,
        source=f"{KR_RSS_DOC_SOURCE}_sector",
        doc_type="news",
        title=compact_text(str(row["normalized_title"] or "")),
        url=compact_text(str(row["original_url"] or "")),
        published_at=parse_datetime_maybe(str(row["published_at"] or "")),
        body=_build_document_body(row),
        relevance_score=0.3,
        relevance_reason="kr_rss_sector_route",
        matched_alias="",
    )
    insert_documents(conn, [doc], commit=False)
    document_id = _find_document_id(
        conn,
        stock_code=KR_RSS_SHADOW_STOCK_CODE,
        source=f"{KR_RSS_DOC_SOURCE}_sector",
        url=str(row["original_url"] or ""),
    )
    if document_id > 0:
        mapping_repo.upsert_sector_mapping_for_document(
            conn,
            document_id=document_id,
            sector_code=str(sector_code),
            raw_score=float(((mapping_json.get("primary") or {}).get("score") or 0.0)),
            reason={"source": "kr_rss", "raw_item_id": int(row["item_id"]), "mapping": mapping_json},
            is_primary=True,
            commit=False,
        )
        _upsert_sector_evidence_card(
            conn,
            document_id=document_id,
            sector_code=str(sector_code),
            row=row,
        )
    return inserted


def _upsert_sector_evidence_card(conn: sqlite3.Connection, *, document_id: int, sector_code: str, row: sqlite3.Row) -> None:
    body = _build_document_body(row)
    facts = [compact_text(str(row["normalized_title"] or ""))]
    for line in entity_mapper.from_body_paragraphs_json(str(row["body_paragraphs_json"] or ""))[:2]:
        if line and line not in facts:
            facts.append(line)
    card = {
        "card_id": f"SECTOR-{sector_code}-{document_id}",
        "item_id": int(document_id),
        "entity_type": "sector",
        "entity_id": sector_code,
        "market": "kr",
        "source_type": "news",
        "source_name": KR_RSS_DOC_SOURCE,
        "url": str(row["original_url"] or ""),
        "source_url_hash": url_hash(str(row["original_url"] or "")),
        "published_at": str(row["published_at"] or ""),
        "fact_headline": facts[0] if facts else compact_text(str(row["normalized_title"] or "")),
        "facts": facts[:4],
        "interpretation": compact_text(body)[:280],
        "risk_note": "KR RSS sector mapping is rule-based and may need follow-up verification.",
        "topics": [sector_code, "kr_rss"],
        "confidence_weight": 0.62,
    }
    evidence_repo.upsert_card(conn, card)


def _build_document_body(row: sqlite3.Row) -> str:
    title = compact_text(str(row["normalized_title"] or ""))
    snippet = compact_text(str(row["normalized_snippet"] or ""))
    body = compact_text(str(row["normalized_body"] or ""))
    payload = [part for part in [title, snippet, body] if part]
    return "\n".join(payload[:3])[:6000]


def _find_document_id(conn: sqlite3.Connection, *, stock_code: str, source: str, url: str) -> int:
    row = conn.execute(
        """
        SELECT id
        FROM documents
        WHERE stock_code = ?
          AND source = ?
          AND url_hash = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (compact_text(stock_code).upper(), compact_text(source).lower(), url_hash(compact_text(url))),
    ).fetchone()
    if not row:
        return 0
    return int(row["id"])


def _ensure_shadow_stock(conn: sqlite3.Connection) -> None:
    existing = conn.execute("SELECT code FROM stocks WHERE code = ?", (KR_RSS_SHADOW_STOCK_CODE,)).fetchone()
    if existing is not None:
        return
    conn.execute(
        """
        INSERT INTO stocks(code, name, queries_json, market, exchange, currency, is_active, universe_source, rank)
        VALUES (?, ?, ?, 'KR', 'KRX', 'KRW', 0, 'system', NULL)
        """,
        (KR_RSS_SHADOW_STOCK_CODE, "KR RSS Sector Shadow", json.dumps([KR_RSS_SHADOW_STOCK_CODE])),
    )

