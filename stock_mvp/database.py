from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from stock_mvp.models import (
    CollectedDocument,
    FinancialSnapshot,
    GeneratedSummary,
    PriceBar,
    Sector,
    SectorCollectedDocument,
    SectorGeneratedSummary,
    Stock,
    StockSectorMap,
)
from stock_mvp.utils import compact_text, normalize_url, now_utc_iso, to_iso_or_none, url_hash


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS stocks (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    queries_json TEXT NOT NULL,
    market TEXT NOT NULL DEFAULT 'KR',
    exchange TEXT NOT NULL DEFAULT 'KRX',
    currency TEXT NOT NULL DEFAULT 'KRW',
    is_active INTEGER NOT NULL DEFAULT 1,
    universe_source TEXT NOT NULL DEFAULT 'manual',
    rank INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_profiles (
    stock_code TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    description_ko TEXT NOT NULL,
    description_raw TEXT,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    is_manual INTEGER NOT NULL DEFAULT 0,
    source_updated_at TEXT,
    collected_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(stock_code) REFERENCES stocks(code) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_stock_profiles_market ON stock_profiles(market, stock_code);
CREATE INDEX IF NOT EXISTS idx_stock_profiles_updated_at ON stock_profiles(updated_at);

CREATE TABLE IF NOT EXISTS sectors (
    sector_code TEXT PRIMARY KEY,
    sector_name_ko TEXT NOT NULL,
    sector_name_en TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL DEFAULT 'v1',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS sector_master_kr (
    sector_id TEXT PRIMARY KEY,
    sector_name TEXT NOT NULL,
    related_keywords_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sector_master_kr_updated_at ON sector_master_kr(updated_at DESC);

CREATE TABLE IF NOT EXISTS ticker_master_kr (
    ticker TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    corp_name TEXT NOT NULL,
    short_code TEXT NOT NULL,
    isin TEXT NOT NULL,
    market_type TEXT NOT NULL,
    base_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticker_master_kr_updated_at ON ticker_master_kr(updated_at DESC);

CREATE TABLE IF NOT EXISTS alias_master_kr (
    alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    alias TEXT NOT NULL,
    alias_type TEXT NOT NULL,
    weight REAL NOT NULL DEFAULT 1.0,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(ticker) REFERENCES ticker_master_kr(ticker) ON DELETE CASCADE,
    UNIQUE(ticker, alias, alias_type)
);
CREATE INDEX IF NOT EXISTS idx_alias_master_kr_alias ON alias_master_kr(alias, is_active);
CREATE INDEX IF NOT EXISTS idx_alias_master_kr_ticker ON alias_master_kr(ticker, is_active);

CREATE TABLE IF NOT EXISTS stock_sector_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    sector_code TEXT NOT NULL,
    mapping_source TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    as_of TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(stock_code) REFERENCES stocks(code) ON DELETE CASCADE,
    FOREIGN KEY(sector_code) REFERENCES sectors(sector_code),
    UNIQUE(stock_code, sector_code)
);
CREATE INDEX IF NOT EXISTS idx_stock_sector_map_stock ON stock_sector_map(stock_code);
CREATE INDEX IF NOT EXISTS idx_stock_sector_map_sector ON stock_sector_map(sector_code);

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    source TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    published_at TEXT,
    body TEXT NOT NULL,
    url_hash TEXT NOT NULL,
    relevance_score REAL NOT NULL DEFAULT 0,
    relevance_reason TEXT,
    matched_alias TEXT,
    collected_at TEXT NOT NULL,
    FOREIGN KEY(stock_code) REFERENCES stocks(code),
    UNIQUE(stock_code, source, url_hash)
);
CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source);
CREATE INDEX IF NOT EXISTS idx_documents_stock_recent ON documents(stock_code, COALESCE(published_at, collected_at) DESC);
CREATE INDEX IF NOT EXISTS idx_documents_stock_type_recent ON documents(
    stock_code,
    doc_type,
    COALESCE(published_at, collected_at) DESC
);

CREATE TABLE IF NOT EXISTS rss_sources (
    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    feed_url TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT '',
    is_active INTEGER NOT NULL DEFAULT 1,
    polling_minutes INTEGER NOT NULL DEFAULT 60,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_url)
);
CREATE INDEX IF NOT EXISTS idx_rss_sources_active ON rss_sources(is_active, source_name);

CREATE TABLE IF NOT EXISTS raw_news_items (
    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    feed_url TEXT NOT NULL,
    title TEXT NOT NULL,
    snippet TEXT NOT NULL DEFAULT '',
    original_url TEXT NOT NULL,
    original_url_hash TEXT NOT NULL,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'new',
    duplicate_of_item_id INTEGER,
    mapping_result_json TEXT NOT NULL DEFAULT '{}',
    mapped_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(duplicate_of_item_id) REFERENCES raw_news_items(item_id),
    UNIQUE(original_url_hash)
);
CREATE INDEX IF NOT EXISTS idx_raw_news_items_status ON raw_news_items(status, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_news_items_content_hash ON raw_news_items(content_hash);
CREATE INDEX IF NOT EXISTS idx_raw_news_items_published ON raw_news_items(published_at DESC);

CREATE TABLE IF NOT EXISTS normalized_news_items (
    item_id INTEGER PRIMARY KEY,
    normalized_title TEXT NOT NULL,
    normalized_snippet TEXT NOT NULL DEFAULT '',
    normalized_body TEXT NOT NULL DEFAULT '',
    lead_paragraph TEXT NOT NULL DEFAULT '',
    body_paragraphs_json TEXT NOT NULL DEFAULT '[]',
    journalist TEXT NOT NULL DEFAULT '',
    publisher TEXT NOT NULL DEFAULT '',
    published_at TEXT,
    normalized_at TEXT NOT NULL,
    FOREIGN KEY(item_id) REFERENCES raw_news_items(item_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_normalized_news_items_at ON normalized_news_items(normalized_at DESC);

CREATE TABLE IF NOT EXISTS document_entity_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    score REAL NOT NULL DEFAULT 0,
    reason_json TEXT NOT NULL DEFAULT '{}',
    assigned_at TEXT NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
    UNIQUE(document_id, entity_type)
);
CREATE INDEX IF NOT EXISTS idx_document_entity_map_entity ON document_entity_map(entity_type, entity_id, score DESC);
CREATE INDEX IF NOT EXISTS idx_document_entity_map_doc ON document_entity_map(document_id);

CREATE TABLE IF NOT EXISTS news_entity_map (
    map_id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    score REAL NOT NULL DEFAULT 0,
    confidence TEXT NOT NULL DEFAULT 'low',
    mapping_reason_json TEXT NOT NULL DEFAULT '{}',
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    FOREIGN KEY(item_id) REFERENCES documents(id) ON DELETE CASCADE,
    UNIQUE(item_id, entity_type, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_news_entity_map_item ON news_entity_map(item_id, is_primary DESC);
CREATE INDEX IF NOT EXISTS idx_news_entity_map_entity ON news_entity_map(entity_type, entity_id, score DESC);

CREATE TABLE IF NOT EXISTS report_pdf_extracts (
    document_id INTEGER PRIMARY KEY,
    parse_status TEXT NOT NULL,
    page_count INTEGER NOT NULL DEFAULT 0,
    text_excerpt TEXT NOT NULL DEFAULT '',
    facts_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_report_pdf_extracts_status ON report_pdf_extracts(parse_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS financial_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    source TEXT NOT NULL,
    per REAL,
    pbr REAL,
    eps REAL,
    roe REAL,
    market_cap INTEGER,
    currency TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    FOREIGN KEY(stock_code) REFERENCES stocks(code) ON DELETE CASCADE,
    UNIQUE(stock_code, as_of_date)
);
CREATE INDEX IF NOT EXISTS idx_financial_snapshots_stock ON financial_snapshots(stock_code);
CREATE INDEX IF NOT EXISTS idx_financial_snapshots_collected ON financial_snapshots(collected_at);

CREATE TABLE IF NOT EXISTS price_bars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume INTEGER,
    source TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    FOREIGN KEY(stock_code) REFERENCES stocks(code) ON DELETE CASCADE,
    UNIQUE(stock_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_price_bars_stock_date ON price_bars(stock_code, date(trade_date) DESC);
CREATE INDEX IF NOT EXISTS idx_price_bars_trade_date ON price_bars(date(trade_date) DESC);

CREATE TABLE IF NOT EXISTS sector_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_code TEXT NOT NULL,
    source TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    published_at TEXT,
    body TEXT NOT NULL,
    url_hash TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    linked_stock_count INTEGER NOT NULL DEFAULT 0,
    linked_document_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(sector_code) REFERENCES sectors(sector_code) ON DELETE CASCADE,
    UNIQUE(sector_code, source, url_hash)
);
CREATE INDEX IF NOT EXISTS idx_sector_documents_sector ON sector_documents(sector_code);
CREATE INDEX IF NOT EXISTS idx_sector_documents_published ON sector_documents(published_at);

CREATE TABLE IF NOT EXISTS sector_document_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_document_id INTEGER NOT NULL,
    document_id INTEGER NOT NULL,
    stock_code TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(sector_document_id) REFERENCES sector_documents(id) ON DELETE CASCADE,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY(stock_code) REFERENCES stocks(code) ON DELETE CASCADE,
    UNIQUE(sector_document_id, document_id)
);
CREATE INDEX IF NOT EXISTS idx_sector_document_links_sector_doc ON sector_document_links(sector_document_id);
CREATE INDEX IF NOT EXISTS idx_sector_document_links_doc ON sector_document_links(document_id);

CREATE TABLE IF NOT EXISTS summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    as_of TEXT NOT NULL,
    line1 TEXT NOT NULL,
    line2 TEXT NOT NULL,
    line3 TEXT NOT NULL,
    line4 TEXT NOT NULL,
    line5 TEXT NOT NULL,
    line6 TEXT NOT NULL,
    line7 TEXT NOT NULL,
    line8 TEXT NOT NULL,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(stock_code) REFERENCES stocks(code)
);

CREATE TABLE IF NOT EXISTS summary_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    summary_id INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    document_id INTEGER NOT NULL,
    FOREIGN KEY(summary_id) REFERENCES summaries(id) ON DELETE CASCADE,
    FOREIGN KEY(document_id) REFERENCES documents(id),
    UNIQUE(summary_id, line_no, document_id)
);

CREATE TABLE IF NOT EXISTS sector_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_code TEXT NOT NULL,
    as_of TEXT NOT NULL,
    line1 TEXT NOT NULL,
    line2 TEXT NOT NULL,
    line3 TEXT NOT NULL,
    line4 TEXT NOT NULL,
    line5 TEXT NOT NULL,
    line6 TEXT NOT NULL,
    line7 TEXT NOT NULL,
    line8 TEXT NOT NULL,
    sentiment_label TEXT NOT NULL,
    sentiment_confidence REAL NOT NULL DEFAULT 0,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(sector_code) REFERENCES sectors(sector_code)
);
CREATE INDEX IF NOT EXISTS idx_sector_summaries_sector ON sector_summaries(sector_code);
CREATE INDEX IF NOT EXISTS idx_sector_summaries_asof ON sector_summaries(as_of);

CREATE TABLE IF NOT EXISTS sector_summary_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_summary_id INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    sector_document_id INTEGER NOT NULL,
    FOREIGN KEY(sector_summary_id) REFERENCES sector_summaries(id) ON DELETE CASCADE,
    FOREIGN KEY(sector_document_id) REFERENCES sector_documents(id),
    UNIQUE(sector_summary_id, line_no, sector_document_id)
);
CREATE INDEX IF NOT EXISTS idx_sector_summary_sources_summary ON sector_summary_sources(sector_summary_id);

CREATE TABLE IF NOT EXISTS item_summaries (
    item_id INTEGER PRIMARY KEY,
    short_summary TEXT NOT NULL,
    impact_label TEXT NOT NULL DEFAULT 'neutral',
    feed_one_liner TEXT,
    detail_bullets_json TEXT NOT NULL DEFAULT '[]',
    related_refs_json TEXT NOT NULL DEFAULT '[]',
    prompt_version TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(item_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evidence_cards (
    card_id TEXT PRIMARY KEY,
    item_id INTEGER NOT NULL UNIQUE,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    market TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    url TEXT NOT NULL,
    source_url_hash TEXT NOT NULL,
    published_at TEXT,
    fact_headline TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    interpretation TEXT NOT NULL,
    risk_note TEXT NOT NULL,
    topics_json TEXT NOT NULL,
    confidence_weight REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    FOREIGN KEY(item_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_evidence_cards_entity_date ON evidence_cards(entity_type, entity_id, market, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_evidence_cards_source_url_hash ON evidence_cards(source_url_hash);

CREATE TABLE IF NOT EXISTS daily_digests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    market TEXT NOT NULL,
    digest_date TEXT NOT NULL,
    summary_8line TEXT NOT NULL,
    change_3 TEXT NOT NULL,
    open_questions TEXT NOT NULL,
    refs_json TEXT NOT NULL,
    prompt_version TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(entity_type, entity_id, market, digest_date)
);
CREATE INDEX IF NOT EXISTS idx_daily_digests_entity ON daily_digests(entity_type, entity_id, market, digest_date DESC);

CREATE TABLE IF NOT EXISTS agent_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    market TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    report_md TEXT NOT NULL,
    refs_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(entity_type, entity_id, market, period_start, period_end)
);
CREATE INDEX IF NOT EXISTS idx_agent_reports_entity ON agent_reports(entity_type, entity_id, market, period_end DESC);

CREATE TABLE IF NOT EXISTS translation_cache (
    source_hash TEXT PRIMARY KEY,
    src_text TEXT NOT NULL,
    ko_text TEXT NOT NULL,
    model TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_translation_cache_updated_at ON translation_cache(updated_at DESC);

CREATE TABLE IF NOT EXISTS opendart_corp_codes (
    stock_code TEXT PRIMARY KEY,
    corp_code TEXT NOT NULL,
    corp_name TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    FOREIGN KEY(stock_code) REFERENCES stocks(code) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_opendart_corp_codes_corp_code ON opendart_corp_codes(corp_code);
CREATE INDEX IF NOT EXISTS idx_opendart_corp_codes_updated_at ON opendart_corp_codes(updated_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    trigger_type TEXT NOT NULL DEFAULT 'manual',
    requested_stock_codes TEXT,
    stock_count INTEGER NOT NULL DEFAULT 0,
    fetched_docs INTEGER NOT NULL DEFAULT 0,
    inserted_docs INTEGER NOT NULL DEFAULT 0,
    skipped_docs INTEGER NOT NULL DEFAULT 0,
    summaries_written INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS crawler_run_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    stock_code TEXT NOT NULL,
    source TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    fetched_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

MIGRATION_NFR_URLS_KEY = "migration.naver_finance_research_urls.v1"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _migrate_general_economy_sector(conn)
    _migrate_stocks_table(conn)
    _migrate_documents_relevance_columns(conn)
    _migrate_item_summaries_columns(conn)
    _migrate_daily_digests_columns(conn)
    if _get_app_meta(conn, MIGRATION_NFR_URLS_KEY) != "done":
        _migrate_naver_finance_research_urls(conn)
        _set_app_meta(conn, MIGRATION_NFR_URLS_KEY, "done")
    conn.commit()


def upsert_stocks(conn: sqlite3.Connection, stocks: list[Stock]) -> None:
    sql = """
    INSERT INTO stocks(code, name, queries_json, market, exchange, currency, is_active, universe_source, rank)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(code) DO UPDATE SET
      name=excluded.name,
      queries_json=excluded.queries_json,
      market=excluded.market,
      exchange=excluded.exchange,
      currency=excluded.currency,
      is_active=excluded.is_active,
      universe_source=excluded.universe_source,
      rank=excluded.rank
    """
    conn.executemany(
        sql,
        [
            (
                s.code,
                s.name,
                json.dumps(s.queries, ensure_ascii=False),
                s.market,
                s.exchange,
                s.currency,
                1 if s.is_active else 0,
                s.universe_source,
                s.rank,
            )
            for s in stocks
        ],
    )
    conn.commit()


def list_stocks(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT code, name, queries_json, market, exchange, currency, is_active, universe_source, rank
        FROM stocks
        WHERE is_active = 1
        ORDER BY market, COALESCE(rank, 99999), code
        """
    ).fetchall()


def list_stocks_by_market(conn: sqlite3.Connection, market: str, active_only: bool = True) -> list[sqlite3.Row]:
    sql = """
    SELECT code, name, queries_json, market, exchange, currency, is_active, universe_source, rank
    FROM stocks
    WHERE market = ?
    """
    params: list[object] = [market]
    if active_only:
        sql += " AND is_active = 1"
    sql += " ORDER BY COALESCE(rank, 99999), code"
    return conn.execute(sql, tuple(params)).fetchall()


def list_active_stock_codes_by_market(conn: sqlite3.Connection, market: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT code
        FROM stocks
        WHERE is_active = 1
          AND upper(market) = upper(?)
        ORDER BY COALESCE(rank, 99999), code
        """,
        (market,),
    ).fetchall()
    return [str(r["code"]) for r in rows]


def upsert_opendart_corp_codes(
    conn: sqlite3.Connection,
    rows: list[tuple[str, str, str]],
    *,
    commit: bool = True,
) -> None:
    if not rows:
        return
    now_iso = now_utc_iso()
    conn.executemany(
        """
        INSERT INTO opendart_corp_codes(stock_code, corp_code, corp_name, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
          corp_code=excluded.corp_code,
          corp_name=excluded.corp_name,
          updated_at=excluded.updated_at
        """,
        [
            (
                compact_text(str(stock_code or "")).upper(),
                compact_text(str(corp_code or "")),
                compact_text(str(corp_name or "")),
                now_iso,
            )
            for stock_code, corp_code, corp_name in rows
            if compact_text(str(stock_code or "")) and compact_text(str(corp_code or ""))
        ],
    )
    if commit:
        conn.commit()


def get_opendart_corp_code_map(conn: sqlite3.Connection, *, market: str = "KR") -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT c.stock_code, c.corp_code
        FROM opendart_corp_codes c
        JOIN stocks s ON s.code = c.stock_code
        WHERE s.is_active = 1
          AND upper(s.market) = upper(?)
        ORDER BY s.code
        """,
        (market,),
    ).fetchall()
    return {str(r["stock_code"]).upper(): str(r["corp_code"]) for r in rows}


def latest_opendart_corp_code_updated_at(conn: sqlite3.Connection, *, market: str = "KR") -> str:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(c.updated_at), '') AS latest
        FROM opendart_corp_codes c
        JOIN stocks s ON s.code = c.stock_code
        WHERE s.is_active = 1
          AND upper(s.market) = upper(?)
        """,
        (market,),
    ).fetchone()
    if not row:
        return ""
    return str(row["latest"] or "")


def get_stock(conn: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT code, name, queries_json, market, exchange, currency, is_active, universe_source, rank
        FROM stocks
        WHERE code = ?
        """,
        (code,),
    ).fetchone()


def get_stock_profile(conn: sqlite3.Connection, stock_code: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
          stock_code,
          market,
          description_ko,
          description_raw,
          source,
          source_url,
          is_manual,
          source_updated_at,
          collected_at,
          updated_at
        FROM stock_profiles
        WHERE stock_code = ?
        """,
        (stock_code,),
    ).fetchone()


def upsert_stock_profile(
    conn: sqlite3.Connection,
    *,
    stock_code: str,
    market: str,
    description_ko: str,
    description_raw: str,
    source: str,
    source_url: str,
    is_manual: bool = False,
    source_updated_at: str | None = None,
    force: bool = False,
    commit: bool = True,
) -> bool:
    existing = get_stock_profile(conn, stock_code)
    existing_is_manual = bool(existing["is_manual"]) if existing is not None else False
    if existing_is_manual and not is_manual and not force:
        return False

    now_iso = now_utc_iso()
    normalized_lines = [compact_text(x) for x in str(description_ko or "").splitlines() if compact_text(x)]
    description_ko_norm = "\n".join(normalized_lines) if normalized_lines else compact_text(str(description_ko or ""))
    conn.execute(
        """
        INSERT INTO stock_profiles(
            stock_code, market, description_ko, description_raw, source, source_url,
            is_manual, source_updated_at, collected_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
            market=excluded.market,
            description_ko=excluded.description_ko,
            description_raw=excluded.description_raw,
            source=excluded.source,
            source_url=excluded.source_url,
            is_manual=excluded.is_manual,
            source_updated_at=excluded.source_updated_at,
            collected_at=excluded.collected_at,
            updated_at=excluded.updated_at
        """,
        (
            stock_code,
            market,
            description_ko_norm,
            compact_text(description_raw),
            source.strip().lower(),
            source_url.strip(),
            1 if is_manual else 0,
            compact_text(str(source_updated_at or "")),
            now_iso,
            now_iso,
        ),
    )
    if commit:
        conn.commit()
    return True


def upsert_sectors(conn: sqlite3.Connection, sectors: list[Sector]) -> None:
    sql = """
    INSERT INTO sectors(sector_code, sector_name_ko, sector_name_en, taxonomy_version, is_active, updated_at)
    VALUES(?, ?, ?, ?, ?, ?)
    ON CONFLICT(sector_code) DO UPDATE SET
      sector_name_ko=excluded.sector_name_ko,
      sector_name_en=excluded.sector_name_en,
      taxonomy_version=excluded.taxonomy_version,
      is_active=excluded.is_active,
      updated_at=excluded.updated_at
    """
    now_iso = now_utc_iso()
    conn.executemany(
        sql,
        [
            (
                s.sector_code,
                s.sector_name_ko,
                s.sector_name_en,
                s.taxonomy_version,
                1 if s.is_active else 0,
                now_iso,
            )
            for s in sectors
        ],
    )
    conn.commit()


def list_sectors(conn: sqlite3.Connection, active_only: bool = True) -> list[sqlite3.Row]:
    sql = """
    SELECT sector_code, sector_name_ko, sector_name_en, taxonomy_version, is_active, created_at, updated_at
    FROM sectors
    """
    params: list[object] = []
    if active_only:
        sql += " WHERE is_active = 1"
    sql += " ORDER BY sector_code"
    return conn.execute(sql, tuple(params)).fetchall()


def replace_stock_sector_maps(conn: sqlite3.Connection, stock_code: str, mappings: list[StockSectorMap]) -> int:
    conn.execute("DELETE FROM stock_sector_map WHERE stock_code = ?", (stock_code,))
    if mappings:
        conn.executemany(
            """
            INSERT INTO stock_sector_map(stock_code, sector_code, mapping_source, confidence, as_of, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    m.stock_code,
                    m.sector_code,
                    m.mapping_source,
                    m.confidence,
                    to_iso_or_none(m.as_of) or now_utc_iso(),
                    now_utc_iso(),
                )
                for m in mappings
            ],
        )
    conn.commit()
    return len(mappings)


def get_stock_sectors(conn: sqlite3.Connection, stock_code: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
      m.stock_code,
      m.sector_code,
      s.sector_name_ko,
      s.sector_name_en,
      s.taxonomy_version,
      m.mapping_source,
      m.confidence,
      m.as_of
    FROM stock_sector_map m
    JOIN sectors s ON s.sector_code = m.sector_code
    WHERE m.stock_code = ?
    ORDER BY m.confidence DESC, m.sector_code
    """
    return conn.execute(sql, (stock_code,)).fetchall()


def replace_universe_stocks(
    conn: sqlite3.Connection, market: str, universe_source: str, stocks: list[Stock]
) -> tuple[int, int]:
    conn.execute(
        "UPDATE stocks SET is_active = 0 WHERE market = ? AND universe_source = ?",
        (market, universe_source),
    )
    normalized: list[Stock] = []
    for idx, stock in enumerate(stocks, start=1):
        normalized.append(
            Stock(
                code=stock.code,
                name=stock.name,
                queries=stock.queries,
                market=market,
                exchange=stock.exchange,
                currency=stock.currency,
                is_active=True,
                universe_source=universe_source,
                rank=idx if stock.rank is None else stock.rank,
            )
        )
    upsert_stocks(conn, normalized)
    active_count = int(
        conn.execute(
            "SELECT COUNT(*) AS cnt FROM stocks WHERE market = ? AND universe_source = ? AND is_active = 1",
            (market, universe_source),
        ).fetchone()["cnt"]
    )
    return len(normalized), active_count


def insert_documents(conn: sqlite3.Connection, docs: list[CollectedDocument], commit: bool = True) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    insert_sql = """
    INSERT INTO documents(
      stock_code, source, doc_type, title, url, published_at, body, url_hash,
      relevance_score, relevance_reason, matched_alias, collected_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(stock_code, source, url_hash) DO NOTHING
    """
    refresh_sql = """
    UPDATE documents
    SET
      url = ?,
      title = CASE WHEN length(?) > length(title) THEN ? ELSE title END,
      published_at = COALESCE(?, published_at),
      body = CASE WHEN length(?) > length(body) THEN ? ELSE body END,
      relevance_score = CASE WHEN ? > relevance_score THEN ? ELSE relevance_score END,
      relevance_reason = CASE WHEN length(?) > 0 THEN ? ELSE relevance_reason END,
      matched_alias = CASE WHEN length(?) > 0 THEN ? ELSE matched_alias END,
      collected_at = ?
    WHERE stock_code = ? AND source = ? AND url_hash = ?
    """
    for doc in docs:
        normalized_url = normalize_url(doc.url) or doc.url
        normalized_hash = url_hash(normalized_url)
        normalized_doc_type = compact_doc_type(doc.doc_type)
        published_at_iso = to_iso_or_none(doc.published_at)
        now_iso = now_utc_iso()
        cursor = conn.execute(
            insert_sql,
            (
                doc.stock_code,
                doc.source,
                normalized_doc_type,
                doc.title,
                normalized_url,
                published_at_iso,
                doc.body,
                normalized_hash,
                float(doc.relevance_score or 0.0),
                compact_text(doc.relevance_reason),
                compact_text(doc.matched_alias),
                now_iso,
            ),
        )
        if cursor.rowcount > 0:
            inserted += 1
        else:
            skipped += 1
            # Keep prior identity while refreshing content if newly crawled text is richer.
            conn.execute(
                refresh_sql,
                (
                    normalized_url,
                    doc.title,
                    doc.title,
                    published_at_iso,
                    doc.body,
                    doc.body,
                    float(doc.relevance_score or 0.0),
                    float(doc.relevance_score or 0.0),
                    compact_text(doc.relevance_reason),
                    compact_text(doc.relevance_reason),
                    compact_text(doc.matched_alias),
                    compact_text(doc.matched_alias),
                    now_iso,
                    doc.stock_code,
                    doc.source,
                    normalized_hash,
                ),
            )
    if commit:
        conn.commit()
    return inserted, skipped


def recent_documents(
    conn: sqlite3.Connection, stock_code: str, lookback_days: int, limit: int = 80
) -> list[sqlite3.Row]:
    sql = """
    SELECT
      id, stock_code, source, doc_type, title, url, published_at, body,
      relevance_score, relevance_reason, matched_alias
    FROM documents
    WHERE stock_code = ?
      AND COALESCE(published_at, collected_at) >= datetime('now', ?)
    ORDER BY COALESCE(published_at, collected_at) DESC
    LIMIT ?
    """
    return conn.execute(sql, (stock_code, f"-{lookback_days} days", limit)).fetchall()


def latest_documents(conn: sqlite3.Connection, stock_code: str, limit: int = 100) -> list[sqlite3.Row]:
    sql = """
    SELECT
      id, stock_code, source, doc_type, title, url, published_at, body,
      relevance_score, relevance_reason, matched_alias
    FROM documents
    WHERE stock_code = ?
    ORDER BY COALESCE(published_at, collected_at) DESC
    LIMIT ?
    """
    return conn.execute(sql, (stock_code, limit)).fetchall()


def latest_documents_by_type(
    conn: sqlite3.Connection, stock_code: str, doc_type: str, limit: int = 100, order_by: str = "recent"
) -> list[sqlite3.Row]:
    order_key = str(order_by or "recent").strip().lower()
    if order_key == "relevance":
        order_clause = "relevance_score DESC, COALESCE(published_at, collected_at) DESC"
    else:
        order_clause = "COALESCE(published_at, collected_at) DESC"
    sql = """
    SELECT
      id, stock_code, source, doc_type, title, url, published_at, body,
      relevance_score, relevance_reason, matched_alias
    FROM documents
    WHERE stock_code = ? AND doc_type = ?
    ORDER BY """ + order_clause + """
    LIMIT ?
    """
    return conn.execute(sql, (stock_code, compact_doc_type(doc_type), limit)).fetchall()


def upsert_document_entity_mapping(
    conn: sqlite3.Connection,
    *,
    document_id: int,
    entity_type: str,
    entity_id: str,
    score: float,
    reason: dict[str, object] | None = None,
    assigned_at: str | None = None,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO document_entity_map(
          document_id, entity_type, entity_id, score, reason_json, assigned_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(document_id, entity_type) DO UPDATE SET
          entity_id=excluded.entity_id,
          score=excluded.score,
          reason_json=excluded.reason_json,
          assigned_at=excluded.assigned_at
        """,
        (
            int(document_id),
            compact_text(entity_type).lower() or "ticker",
            compact_text(entity_id).upper(),
            max(0.0, min(float(score), 1.0)),
            json.dumps(reason or {}, ensure_ascii=False),
            compact_text(str(assigned_at or "")) or now_utc_iso(),
        ),
    )
    if commit:
        conn.commit()


def clear_news_entity_map_for_item(conn: sqlite3.Connection, item_id: int, commit: bool = True) -> None:
    conn.execute("DELETE FROM news_entity_map WHERE item_id = ?", (int(item_id),))
    if commit:
        conn.commit()


def upsert_news_entity_map(
    conn: sqlite3.Connection,
    *,
    item_id: int,
    entity_type: str,
    entity_id: str,
    score: float,
    confidence: str,
    mapping_reason: dict[str, object] | None = None,
    is_primary: bool = False,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO news_entity_map(
          item_id, entity_type, entity_id, score, confidence, mapping_reason_json, is_primary, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_id, entity_type, entity_id) DO UPDATE SET
          score=excluded.score,
          confidence=excluded.confidence,
          mapping_reason_json=excluded.mapping_reason_json,
          is_primary=excluded.is_primary,
          created_at=excluded.created_at
        """,
        (
            int(item_id),
            compact_text(entity_type).lower(),
            compact_text(entity_id).upper(),
            max(0.0, float(score)),
            compact_text(confidence).lower() or "low",
            json.dumps(mapping_reason or {}, ensure_ascii=False),
            1 if is_primary else 0,
            now_utc_iso(),
        ),
    )
    if commit:
        conn.commit()


def upsert_sector_document_by_code(
    conn: sqlite3.Connection,
    *,
    sector_code: str,
    source: str,
    doc_type: str,
    title: str,
    url: str,
    published_at: str | None,
    body: str,
    commit: bool = True,
) -> bool:
    normalized_url = normalize_url(url) or url
    normalized_hash = url_hash(normalized_url)
    cursor = conn.execute(
        """
        INSERT INTO sector_documents(
          sector_code, source, doc_type, title, url, published_at, body, url_hash, collected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sector_code, source, url_hash) DO NOTHING
        """,
        (
            compact_text(sector_code).upper(),
            compact_text(source).lower(),
            compact_doc_type(doc_type),
            compact_text(title),
            normalized_url,
            compact_text(str(published_at or "")) or None,
            compact_text(body),
            normalized_hash,
            now_utc_iso(),
        ),
    )
    inserted = int(cursor.rowcount or 0) > 0
    if not inserted:
        conn.execute(
            """
            UPDATE sector_documents
            SET
              title = CASE WHEN length(?) > length(title) THEN ? ELSE title END,
              published_at = COALESCE(?, published_at),
              body = CASE WHEN length(?) > length(body) THEN ? ELSE body END,
              collected_at = ?
            WHERE sector_code = ? AND source = ? AND url_hash = ?
            """,
            (
                compact_text(title),
                compact_text(title),
                compact_text(str(published_at or "")) or None,
                compact_text(body),
                compact_text(body),
                now_utc_iso(),
                compact_text(sector_code).upper(),
                compact_text(source).lower(),
                normalized_hash,
            ),
        )
    if commit:
        conn.commit()
    return inserted


def recent_mapped_sector_entities(
    conn: sqlite3.Connection,
    *,
    market: str,
    lookback_days: int,
    limit: int = 50,
) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT n.entity_id
        FROM news_entity_map n
        JOIN documents d ON d.id = n.item_id
        JOIN stocks s ON s.code = d.stock_code
        WHERE n.entity_type = 'sector'
          AND n.is_primary = 1
          AND lower(s.market) = lower(?)
          AND COALESCE(d.published_at, d.collected_at) >= datetime('now', ?)
        ORDER BY n.entity_id
        LIMIT ?
        """,
        (market, f"-{max(1, int(lookback_days))} days", max(1, int(limit))),
    ).fetchall()
    return [str(r["entity_id"]) for r in rows]


def list_document_entity_mappings(
    conn: sqlite3.Connection,
    *,
    market: str | None = None,
    stock_code: str | None = None,
    limit: int = 200,
) -> list[sqlite3.Row]:
    sql = """
    SELECT
      m.document_id,
      m.entity_type,
      m.entity_id,
      m.score,
      m.reason_json,
      m.assigned_at,
      d.stock_code,
      d.source,
      d.doc_type,
      d.title,
      d.url,
      COALESCE(d.published_at, d.collected_at) AS published_at,
      s.market
    FROM document_entity_map m
    JOIN documents d ON d.id = m.document_id
    JOIN stocks s ON s.code = d.stock_code
    WHERE 1=1
    """
    params: list[object] = []
    if market:
        sql += " AND lower(s.market) = lower(?)"
        params.append(market)
    if stock_code:
        sql += " AND d.stock_code = ?"
        params.append(stock_code)
    sql += """
    ORDER BY datetime(COALESCE(d.published_at, d.collected_at)) DESC, d.id DESC
    LIMIT ?
    """
    params.append(max(1, int(limit)))
    return conn.execute(sql, tuple(params)).fetchall()


def upsert_report_pdf_extract(
    conn: sqlite3.Connection,
    *,
    document_id: int,
    parse_status: str,
    page_count: int,
    text_excerpt: str,
    facts: list[str] | None = None,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO report_pdf_extracts(
          document_id, parse_status, page_count, text_excerpt, facts_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(document_id) DO UPDATE SET
          parse_status=excluded.parse_status,
          page_count=excluded.page_count,
          text_excerpt=excluded.text_excerpt,
          facts_json=excluded.facts_json,
          updated_at=excluded.updated_at
        """,
        (
            int(document_id),
            compact_text(parse_status).lower() or "none",
            max(0, int(page_count)),
            compact_text(text_excerpt)[:2400],
            json.dumps(list(facts or []), ensure_ascii=False),
            now_utc_iso(),
        ),
    )
    if commit:
        conn.commit()


def upsert_report_pdf_extract_by_identity(
    conn: sqlite3.Connection,
    *,
    stock_code: str,
    source: str,
    url: str,
    parse_status: str,
    page_count: int,
    text_excerpt: str,
    facts: list[str] | None = None,
    commit: bool = True,
) -> bool:
    normalized_url = normalize_url(url) or url
    normalized_hash = url_hash(normalized_url)
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
        (stock_code, source, normalized_hash),
    ).fetchone()
    if row is None:
        return False
    upsert_report_pdf_extract(
        conn,
        document_id=int(row["id"]),
        parse_status=parse_status,
        page_count=page_count,
        text_excerpt=text_excerpt,
        facts=facts,
        commit=commit,
    )
    return True


def upsert_financial_snapshot(conn: sqlite3.Connection, snapshot: FinancialSnapshot, commit: bool = True) -> int:
    as_of_date = snapshot.as_of.date().isoformat()
    cursor = conn.execute(
        """
        INSERT INTO financial_snapshots(
            stock_code, as_of_date, source, per, pbr, eps, roe, market_cap, currency, collected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, as_of_date) DO UPDATE SET
            source=excluded.source,
            per=excluded.per,
            pbr=excluded.pbr,
            eps=excluded.eps,
            roe=excluded.roe,
            market_cap=excluded.market_cap,
            currency=excluded.currency,
            collected_at=excluded.collected_at
        """,
        (
            snapshot.stock_code,
            as_of_date,
            snapshot.source,
            snapshot.per,
            snapshot.pbr,
            snapshot.eps,
            snapshot.roe,
            snapshot.market_cap,
            snapshot.currency,
            now_utc_iso(),
        ),
    )
    if commit:
        conn.commit()
    return int(cursor.rowcount)


def latest_financial_snapshot(conn: sqlite3.Connection, stock_code: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
          id, stock_code, as_of_date, source, per, pbr, eps, roe, market_cap, currency, collected_at
        FROM financial_snapshots
        WHERE stock_code = ?
        ORDER BY date(as_of_date) DESC, datetime(collected_at) DESC
        LIMIT 1
        """,
        (stock_code,),
    ).fetchone()


def financial_refresh_needed(conn: sqlite3.Connection, stock_code: str, min_hours: int = 20) -> bool:
    row = conn.execute(
        """
        SELECT collected_at
        FROM financial_snapshots
        WHERE stock_code = ?
        ORDER BY datetime(collected_at) DESC
        LIMIT 1
        """,
        (stock_code,),
    ).fetchone()
    if row is None:
        return True
    raw = str(row["collected_at"] or "").strip()
    if not raw:
        return True
    try:
        last = datetime.fromisoformat(raw)
    except ValueError:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    age_sec = (now - last).total_seconds()
    return age_sec >= max(1, min_hours) * 3600


def latest_financial_snapshots(conn: sqlite3.Connection, limit: int = 120) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          s.code AS stock_code,
          s.name AS stock_name,
          s.market,
          f.as_of_date,
          f.source,
          f.per,
          f.pbr,
          f.eps,
          f.roe,
          f.market_cap,
          f.currency,
          f.collected_at
        FROM stocks s
        JOIN (
          SELECT t.*
          FROM financial_snapshots t
          JOIN (
            SELECT stock_code, MAX(date(as_of_date)) AS max_as_of
            FROM financial_snapshots
            GROUP BY stock_code
          ) m ON m.stock_code = t.stock_code AND date(t.as_of_date) = m.max_as_of
        ) f ON f.stock_code = s.code
        WHERE s.is_active = 1
        ORDER BY s.market, COALESCE(s.rank, 99999), s.code
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def upsert_price_bars(conn: sqlite3.Connection, bars: list[PriceBar], commit: bool = True) -> int:
    if not bars:
        return 0
    sql = """
    INSERT INTO price_bars(
        stock_code, trade_date, open, high, low, close, adj_close, volume, source, collected_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(stock_code, trade_date) DO UPDATE SET
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        adj_close=excluded.adj_close,
        volume=excluded.volume,
        source=excluded.source,
        collected_at=excluded.collected_at
    """
    now_iso = now_utc_iso()
    conn.executemany(
        sql,
        [
            (
                b.stock_code,
                b.trade_date.date().isoformat(),
                b.open,
                b.high,
                b.low,
                b.close,
                b.adj_close,
                b.volume,
                b.source,
                now_iso,
            )
            for b in bars
        ],
    )
    if commit:
        conn.commit()
    return len(bars)


def latest_price_trade_date(conn: sqlite3.Connection, stock_code: str) -> str | None:
    row = conn.execute(
        """
        SELECT trade_date
        FROM price_bars
        WHERE stock_code = ?
        ORDER BY date(trade_date) DESC
        LIMIT 1
        """,
        (stock_code,),
    ).fetchone()
    if row is None:
        return None
    return str(row["trade_date"] or "")


def latest_price_bars(conn: sqlite3.Connection, stock_code: str, limit: int = 365) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT stock_code, trade_date, open, high, low, close, adj_close, volume, source, collected_at
        FROM price_bars
        WHERE stock_code = ?
        ORDER BY date(trade_date) DESC
        LIMIT ?
        """,
        (stock_code, limit),
    ).fetchall()


def price_bars_in_range(
    conn: sqlite3.Connection,
    stock_code: str,
    start_date: str,
    end_date: str,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT stock_code, trade_date, open, high, low, close, adj_close, volume, source, collected_at
        FROM price_bars
        WHERE stock_code = ?
          AND date(trade_date) >= date(?)
          AND date(trade_date) <= date(?)
        ORDER BY date(trade_date) ASC
        """,
        (stock_code, start_date, end_date),
    ).fetchall()


def rebuild_sector_documents(
    conn: sqlite3.Connection,
    lookback_days: int,
    commit: bool = True,
) -> tuple[int, int, int]:
    rows = conn.execute(
        """
        SELECT
          m.sector_code,
          d.id AS document_id,
          d.stock_code,
          d.source,
          d.doc_type,
          d.title,
          d.url,
          d.published_at,
          d.body,
          d.url_hash,
          d.collected_at
        FROM documents d
        JOIN stocks s ON s.code = d.stock_code
        JOIN stock_sector_map m ON m.stock_code = d.stock_code
        WHERE s.is_active = 1
          AND COALESCE(d.published_at, d.collected_at) >= datetime('now', ?)
        ORDER BY datetime(COALESCE(d.published_at, d.collected_at)) DESC, d.id DESC
        """,
        (f"-{lookback_days} days",),
    ).fetchall()

    inserted_sector_docs = 0
    inserted_links = 0
    touched_sector_document_ids: set[int] = set()
    insert_sector_sql = """
    INSERT INTO sector_documents(
      sector_code, source, doc_type, title, url, published_at, body, url_hash, collected_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sector_code, source, url_hash) DO NOTHING
    """
    refresh_sector_sql = """
    UPDATE sector_documents
    SET
      title = CASE WHEN length(?) > length(title) THEN ? ELSE title END,
      published_at = COALESCE(?, published_at),
      body = CASE WHEN length(?) > length(body) THEN ? ELSE body END,
      collected_at = ?
    WHERE sector_code = ? AND source = ? AND url_hash = ?
    """
    for row in rows:
        cursor = conn.execute(
            insert_sector_sql,
            (
                row["sector_code"],
                row["source"],
                row["doc_type"],
                row["title"],
                row["url"],
                row["published_at"],
                row["body"],
                row["url_hash"],
                row["collected_at"],
            ),
        )
        if cursor.rowcount > 0:
            inserted_sector_docs += 1
            sector_document_id = int(cursor.lastrowid)
        else:
            conn.execute(
                refresh_sector_sql,
                (
                    row["title"],
                    row["title"],
                    row["published_at"],
                    row["body"],
                    row["body"],
                    row["collected_at"],
                    row["sector_code"],
                    row["source"],
                    row["url_hash"],
                ),
            )
            sector_document_id = int(
                conn.execute(
                    """
                    SELECT id
                    FROM sector_documents
                    WHERE sector_code = ? AND source = ? AND url_hash = ?
                    """,
                    (row["sector_code"], row["source"], row["url_hash"]),
                ).fetchone()["id"]
            )
        touched_sector_document_ids.add(sector_document_id)

        link_cursor = conn.execute(
            """
            INSERT OR IGNORE INTO sector_document_links(
              sector_document_id, document_id, stock_code, created_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (sector_document_id, row["document_id"], row["stock_code"], now_utc_iso()),
        )
        if link_cursor.rowcount > 0:
            inserted_links += 1

    if touched_sector_document_ids:
        placeholders = ",".join("?" for _ in touched_sector_document_ids)
        conn.execute(
            f"""
            UPDATE sector_documents
            SET linked_document_count = (
                  SELECT COUNT(*)
                  FROM sector_document_links l
                  WHERE l.sector_document_id = sector_documents.id
                ),
                linked_stock_count = (
                  SELECT COUNT(DISTINCT l.stock_code)
                  FROM sector_document_links l
                  WHERE l.sector_document_id = sector_documents.id
                )
            WHERE id IN ({placeholders})
            """,
            tuple(touched_sector_document_ids),
        )
    if commit:
        conn.commit()
    return inserted_sector_docs, inserted_links, len(rows)


def latest_sector_documents(
    conn: sqlite3.Connection, sector_code: str, lookback_days: int, limit: int = 120
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          id,
          sector_code,
          source,
          doc_type,
          title,
          url,
          published_at,
          body,
          linked_stock_count,
          linked_document_count
        FROM sector_documents
        WHERE sector_code = ?
          AND COALESCE(published_at, collected_at) >= datetime('now', ?)
        ORDER BY datetime(COALESCE(published_at, collected_at)) DESC, id DESC
        LIMIT ?
        """,
        (sector_code, f"-{lookback_days} days", limit),
    ).fetchall()


def upsert_sector_documents(
    conn: sqlite3.Connection,
    docs: list[SectorCollectedDocument],
    *,
    sector_code_by_name: dict[str, str] | None = None,
    commit: bool = True,
) -> tuple[int, int, int]:
    mapping = sector_code_by_name or {}
    inserted = 0
    skipped = 0
    unmapped = 0

    insert_sql = """
    INSERT INTO sector_documents(
      sector_code, source, doc_type, title, url, published_at, body, url_hash, collected_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sector_code, source, url_hash) DO NOTHING
    """
    refresh_sql = """
    UPDATE sector_documents
    SET
      title = CASE WHEN length(?) > length(title) THEN ? ELSE title END,
      published_at = COALESCE(?, published_at),
      body = CASE WHEN length(?) > length(body) THEN ? ELSE body END,
      collected_at = ?
    WHERE sector_code = ? AND source = ? AND url_hash = ?
    """

    for doc in docs:
        sector_name = _sector_name_key(doc.sector_name)
        sector_code = mapping.get(sector_name, "")
        if not sector_code:
            unmapped += 1
            continue

        normalized_url = normalize_url(doc.url) or compact_text(doc.url)
        if not normalized_url:
            skipped += 1
            continue
        key = url_hash(normalized_url)
        collected_at = now_utc_iso()
        published_at = to_iso_or_none(doc.published_at)
        title = compact_text(doc.title)
        body = compact_text(doc.body)

        cursor = conn.execute(
            insert_sql,
            (
                sector_code,
                compact_text(doc.source),
                compact_text(doc.doc_type),
                title,
                normalized_url,
                published_at,
                body,
                key,
                collected_at,
            ),
        )
        if cursor.rowcount > 0:
            inserted += 1
            continue

        conn.execute(
            refresh_sql,
            (
                title,
                title,
                published_at,
                body,
                body,
                collected_at,
                sector_code,
                compact_text(doc.source),
                key,
            ),
        )
        skipped += 1

    if commit:
        conn.commit()
    return inserted, skipped, unmapped


def _sector_name_key(value: str) -> str:
    text = compact_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"[^0-9a-z가-힣]+", "", text)
    return text


def sector_document_links(conn: sqlite3.Connection, sector_document_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT l.sector_document_id, l.document_id, l.stock_code, l.created_at
        FROM sector_document_links l
        WHERE l.sector_document_id = ?
        ORDER BY l.document_id DESC
        """,
        (sector_document_id,),
    ).fetchall()


def sector_document_distribution(conn: sqlite3.Connection, limit: int = 50) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          s.sector_code,
          s.sector_name_ko,
          COUNT(d.id) AS doc_count
        FROM sectors s
        LEFT JOIN sector_documents d ON d.sector_code = s.sector_code
        WHERE s.is_active = 1
        GROUP BY s.sector_code, s.sector_name_ko
        HAVING doc_count > 0
        ORDER BY doc_count DESC, s.sector_code
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def recent_sector_targets(conn: sqlite3.Connection, lookback_days: int, limit: int = 120) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          s.sector_code,
          s.sector_name_ko,
          s.sector_name_en,
          COUNT(d.id) AS doc_count
        FROM sectors s
        JOIN sector_documents d ON d.sector_code = s.sector_code
        WHERE s.is_active = 1
          AND COALESCE(d.published_at, d.collected_at) >= datetime('now', ?)
        GROUP BY s.sector_code, s.sector_name_ko, s.sector_name_en
        ORDER BY doc_count DESC, s.sector_code
        LIMIT ?
        """,
        (f"-{lookback_days} days", limit),
    ).fetchall()


def save_summary(conn: sqlite3.Connection, summary: GeneratedSummary, commit: bool = True) -> int:
    if not summary.lines:
        raise ValueError("summary must contain at least 1 line")
    if len(summary.lines) > 8:
        raise ValueError("summary must contain at most 8 lines")

    padded_lines = [line.text for line in summary.lines] + [""] * (8 - len(summary.lines))

    cursor = conn.execute(
        """
        INSERT INTO summaries(
            stock_code, as_of, line1, line2, line3, line4, line5, line6, line7, line8, model, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            summary.stock_code,
            summary.as_of.isoformat(timespec="seconds"),
            padded_lines[0],
            padded_lines[1],
            padded_lines[2],
            padded_lines[3],
            padded_lines[4],
            padded_lines[5],
            padded_lines[6],
            padded_lines[7],
            summary.model,
            now_utc_iso(),
        ),
    )
    summary_id = int(cursor.lastrowid)
    mapping_rows: list[tuple[int, int, int]] = []
    for idx, line in enumerate(summary.lines, start=1):
        for doc_id in line.source_doc_ids:
            mapping_rows.append((summary_id, idx, doc_id))
    if mapping_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO summary_sources(summary_id, line_no, document_id) VALUES (?, ?, ?)",
            mapping_rows,
        )
    if commit:
        conn.commit()
    return summary_id


def save_sector_summary(conn: sqlite3.Connection, summary: SectorGeneratedSummary, commit: bool = True) -> int:
    if len(summary.lines) != 8:
        raise ValueError("sector summary must contain exactly 8 lines")

    cursor = conn.execute(
        """
        INSERT INTO sector_summaries(
            sector_code, as_of, line1, line2, line3, line4, line5, line6, line7, line8,
            sentiment_label, sentiment_confidence, model, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            summary.sector_code,
            summary.as_of.isoformat(timespec="seconds"),
            summary.lines[0].text,
            summary.lines[1].text,
            summary.lines[2].text,
            summary.lines[3].text,
            summary.lines[4].text,
            summary.lines[5].text,
            summary.lines[6].text,
            summary.lines[7].text,
            summary.sentiment_label,
            summary.sentiment_confidence,
            summary.model,
            now_utc_iso(),
        ),
    )
    sector_summary_id = int(cursor.lastrowid)
    mapping_rows: list[tuple[int, int, int]] = []
    for idx, line in enumerate(summary.lines, start=1):
        for sector_doc_id in line.source_doc_ids:
            mapping_rows.append((sector_summary_id, idx, sector_doc_id))
    if mapping_rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO sector_summary_sources(sector_summary_id, line_no, sector_document_id)
            VALUES (?, ?, ?)
            """,
            mapping_rows,
        )
    if commit:
        conn.commit()
    return sector_summary_id


def latest_summary(conn: sqlite3.Connection, stock_code: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, stock_code, as_of, line1, line2, line3, line4, line5, line6, line7, line8, model, created_at
        FROM summaries
        WHERE stock_code = ?
        ORDER BY datetime(as_of) DESC, id DESC
        LIMIT 1
        """,
        (stock_code,),
    ).fetchone()


def latest_sector_summary(conn: sqlite3.Connection, sector_code: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, sector_code, as_of, line1, line2, line3, line4, line5, line6, line7, line8,
               sentiment_label, sentiment_confidence, model, created_at
        FROM sector_summaries
        WHERE sector_code = ?
        ORDER BY datetime(as_of) DESC, id DESC
        LIMIT 1
        """,
        (sector_code,),
    ).fetchone()


def latest_summaries_by_stock(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    sql = """
    WITH ranked AS (
      SELECT
        t.*,
        ROW_NUMBER() OVER (
          PARTITION BY t.stock_code
          ORDER BY datetime(t.as_of) DESC, t.id DESC
        ) AS rn
      FROM summaries t
    )
    SELECT s.code AS stock_code, s.name AS stock_name, x.id AS summary_id, x.as_of, x.line1
    FROM stocks s
    LEFT JOIN ranked x ON x.stock_code = s.code AND x.rn = 1
    WHERE s.is_active = 1
    ORDER BY s.market, COALESCE(s.rank, 99999), s.code
    """
    return conn.execute(sql).fetchall()


def latest_summary_highlights(conn: sqlite3.Connection, limit: int = 12) -> list[sqlite3.Row]:
    sql = """
    WITH ranked AS (
      SELECT
        t.*,
        ROW_NUMBER() OVER (
          PARTITION BY t.stock_code
          ORDER BY datetime(t.as_of) DESC, t.id DESC
        ) AS rn
      FROM summaries t
    )
    SELECT s.code AS stock_code, s.name AS stock_name, s.market, x.as_of, x.line1
    FROM stocks s
    JOIN ranked x ON x.stock_code = s.code AND x.rn = 1
    WHERE s.is_active = 1
    ORDER BY datetime(x.as_of) DESC, s.market, COALESCE(s.rank, 99999), s.code
    LIMIT ?
    """
    return conn.execute(sql, (limit,)).fetchall()


def latest_sector_summaries(conn: sqlite3.Connection, limit: int = 60) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          s.sector_code,
          s.sector_name_ko,
          s.sector_name_en,
          x.id AS summary_id,
          x.as_of,
          x.line1,
          x.sentiment_label,
          x.sentiment_confidence,
          x.model
        FROM sectors s
        JOIN (
          SELECT t.*
          FROM sector_summaries t
          JOIN (
            SELECT sector_code, MAX(datetime(as_of)) AS max_as_of
            FROM sector_summaries
            GROUP BY sector_code
          ) m ON m.sector_code = t.sector_code AND datetime(t.as_of) = m.max_as_of
        ) x ON x.sector_code = s.sector_code
        WHERE s.is_active = 1
        ORDER BY datetime(x.as_of) DESC, s.sector_code
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def summary_source_documents(conn: sqlite3.Connection, summary_id: int) -> list[sqlite3.Row]:
    sql = """
    SELECT ss.line_no, d.id, d.source, d.title, d.url, d.published_at
    FROM summary_sources ss
    JOIN documents d ON d.id = ss.document_id
    WHERE ss.summary_id = ?
    ORDER BY ss.line_no, d.published_at DESC
    """
    return conn.execute(sql, (summary_id,)).fetchall()


def sector_summary_source_documents(conn: sqlite3.Connection, sector_summary_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          ss.line_no,
          d.id,
          d.source,
          d.doc_type,
          d.title,
          d.url,
          d.published_at,
          d.linked_stock_count,
          d.linked_document_count
        FROM sector_summary_sources ss
        JOIN sector_documents d ON d.id = ss.sector_document_id
        WHERE ss.sector_summary_id = ?
        ORDER BY ss.line_no, d.published_at DESC
        """,
        (sector_summary_id,),
    ).fetchall()


def create_pipeline_run(
    conn: sqlite3.Connection,
    trigger_type: str,
    requested_stock_codes: str,
    stock_count: int,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO pipeline_runs(started_at, trigger_type, requested_stock_codes, stock_count, status)
        VALUES (?, ?, ?, ?, 'running')
        """,
        (now_utc_iso(), trigger_type, requested_stock_codes, stock_count),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_pipeline_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    fetched_docs: int,
    inserted_docs: int,
    skipped_docs: int,
    summaries_written: int,
    error_count: int,
    status: str,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE pipeline_runs
        SET ended_at = ?,
            fetched_docs = ?,
            inserted_docs = ?,
            skipped_docs = ?,
            summaries_written = ?,
            error_count = ?,
            status = ?,
            error_message = ?
        WHERE id = ?
        """,
        (
            now_utc_iso(),
            fetched_docs,
            inserted_docs,
            skipped_docs,
            summaries_written,
            error_count,
            status,
            error_message,
            run_id,
        ),
    )
    conn.commit()


def record_crawler_run_stat(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    stock_code: str,
    source: str,
    doc_type: str,
    fetched_count: int,
    inserted_count: int,
    skipped_count: int,
    error_message: str | None,
    attempt_count: int,
    duration_ms: int,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO crawler_run_stats(
          run_id, stock_code, source, doc_type, fetched_count, inserted_count, skipped_count,
          error_message, attempt_count, duration_ms, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            stock_code,
            source,
            doc_type,
            fetched_count,
            inserted_count,
            skipped_count,
            error_message,
            attempt_count,
            duration_ms,
            now_utc_iso(),
        ),
    )
    if commit:
        conn.commit()


def latest_pipeline_runs(conn: sqlite3.Connection, limit: int = 30) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, started_at, ended_at, trigger_type, requested_stock_codes, stock_count, fetched_docs,
               inserted_docs, skipped_docs, summaries_written, error_count, status, error_message
        FROM pipeline_runs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def crawler_stats_for_run(conn: sqlite3.Connection, run_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT run_id, stock_code, source, doc_type, fetched_count, inserted_count, skipped_count,
               error_message, attempt_count, duration_ms, created_at
        FROM crawler_run_stats
        WHERE run_id = ?
        ORDER BY stock_code, source
        """,
        (run_id,),
    ).fetchall()


def _migrate_general_economy_sector(conn: sqlite3.Connection) -> None:
    now_iso = now_utc_iso()
    keywords = [
        "환율",
        "금리",
        "물가",
        "고용",
        "경기",
        "수출",
        "수입",
        "통화정책",
        "재정정책",
        "한은",
        "FOMC",
        "CPI",
        "PPI",
        "GDP",
    ]
    conn.execute(
        """
        INSERT INTO sectors(sector_code, sector_name_ko, sector_name_en, taxonomy_version, is_active, updated_at)
        VALUES (?, ?, ?, ?, 1, ?)
        ON CONFLICT(sector_code) DO UPDATE SET
          sector_name_ko=excluded.sector_name_ko,
          sector_name_en=excluded.sector_name_en,
          taxonomy_version=excluded.taxonomy_version,
          is_active=excluded.is_active,
          updated_at=excluded.updated_at
        """,
        ("GENERAL_ECONOMY", "일반 경제", "General Economy", "v1", now_iso),
    )
    conn.execute(
        """
        INSERT INTO sector_master_kr(sector_id, sector_name, related_keywords_json, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(sector_id) DO UPDATE SET
          sector_name=excluded.sector_name,
          related_keywords_json=excluded.related_keywords_json,
          updated_at=excluded.updated_at
        """,
        ("GENERAL_ECONOMY", "일반 경제", json.dumps(keywords, ensure_ascii=False), now_iso),
    )


def _migrate_stocks_table(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(stocks)").fetchall()}
    alter_statements: list[str] = []
    if "market" not in columns:
        alter_statements.append("ALTER TABLE stocks ADD COLUMN market TEXT NOT NULL DEFAULT 'KR'")
    if "exchange" not in columns:
        alter_statements.append("ALTER TABLE stocks ADD COLUMN exchange TEXT NOT NULL DEFAULT 'KRX'")
    if "currency" not in columns:
        alter_statements.append("ALTER TABLE stocks ADD COLUMN currency TEXT NOT NULL DEFAULT 'KRW'")
    if "is_active" not in columns:
        alter_statements.append("ALTER TABLE stocks ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if "universe_source" not in columns:
        alter_statements.append("ALTER TABLE stocks ADD COLUMN universe_source TEXT NOT NULL DEFAULT 'manual'")
    if "rank" not in columns:
        alter_statements.append("ALTER TABLE stocks ADD COLUMN rank INTEGER")
    for statement in alter_statements:
        conn.execute(statement)


def _migrate_documents_relevance_columns(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(documents)").fetchall()}
    alter_statements: list[str] = []
    if "relevance_score" not in columns:
        alter_statements.append("ALTER TABLE documents ADD COLUMN relevance_score REAL NOT NULL DEFAULT 0")
    if "relevance_reason" not in columns:
        alter_statements.append("ALTER TABLE documents ADD COLUMN relevance_reason TEXT")
    if "matched_alias" not in columns:
        alter_statements.append("ALTER TABLE documents ADD COLUMN matched_alias TEXT")
    for statement in alter_statements:
        conn.execute(statement)

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_documents_stock_type_relevance ON documents(
            stock_code,
            doc_type,
            relevance_score DESC,
            COALESCE(published_at, collected_at) DESC
        )
        """
    )


def _migrate_item_summaries_columns(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(item_summaries)").fetchall()}
    alter_statements: list[str] = []
    if "impact_label" not in columns:
        alter_statements.append("ALTER TABLE item_summaries ADD COLUMN impact_label TEXT NOT NULL DEFAULT 'neutral'")
    if "feed_one_liner" not in columns:
        alter_statements.append("ALTER TABLE item_summaries ADD COLUMN feed_one_liner TEXT")
    if "detail_bullets_json" not in columns:
        alter_statements.append("ALTER TABLE item_summaries ADD COLUMN detail_bullets_json TEXT NOT NULL DEFAULT '[]'")
    if "related_refs_json" not in columns:
        alter_statements.append("ALTER TABLE item_summaries ADD COLUMN related_refs_json TEXT NOT NULL DEFAULT '[]'")
    if "prompt_version" not in columns:
        alter_statements.append("ALTER TABLE item_summaries ADD COLUMN prompt_version TEXT NOT NULL DEFAULT ''")
    if "updated_at" not in columns:
        alter_statements.append("ALTER TABLE item_summaries ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
    for statement in alter_statements:
        conn.execute(statement)
    now_iso = now_utc_iso()
    conn.execute(
        "UPDATE item_summaries SET updated_at = ? WHERE COALESCE(updated_at, '') = ''",
        (now_iso,),
    )


def _migrate_daily_digests_columns(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(daily_digests)").fetchall()}
    if "prompt_version" not in columns:
        conn.execute("ALTER TABLE daily_digests ADD COLUMN prompt_version TEXT NOT NULL DEFAULT ''")


def _migrate_naver_finance_research_urls(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, stock_code, source, url, url_hash, published_at, collected_at
        FROM documents
        WHERE source = 'naver_finance_research'
        ORDER BY datetime(COALESCE(published_at, collected_at)) DESC, id DESC
        """
    ).fetchall()
    if not rows:
        return

    keep_by_key: dict[tuple[str, str, str], int] = {}
    to_update: list[tuple[str, str, int]] = []
    to_merge: list[tuple[int, int]] = []

    for row in rows:
        row_id = int(row["id"])
        stock_code = str(row["stock_code"] or "")
        source = str(row["source"] or "")
        raw_url = str(row["url"] or "")
        canonical_url = normalize_url(raw_url) or raw_url
        canonical_hash = url_hash(canonical_url)
        dedupe_key = (stock_code, source, canonical_hash)

        keep_id = keep_by_key.get(dedupe_key)
        if keep_id is None:
            keep_by_key[dedupe_key] = row_id
            if canonical_url != raw_url or canonical_hash != str(row["url_hash"] or ""):
                to_update.append((canonical_url, canonical_hash, row_id))
            continue
        to_merge.append((row_id, keep_id))

    for canonical_url, canonical_hash, row_id in to_update:
        conn.execute(
            "UPDATE documents SET url = ?, url_hash = ? WHERE id = ?",
            (canonical_url, canonical_hash, row_id),
        )

    for source_document_id, target_document_id in to_merge:
        conn.execute(
            """
            INSERT OR IGNORE INTO summary_sources(summary_id, line_no, document_id)
            SELECT summary_id, line_no, ?
            FROM summary_sources
            WHERE document_id = ?
            """,
            (target_document_id, source_document_id),
        )
        conn.execute("DELETE FROM summary_sources WHERE document_id = ?", (source_document_id,))

        conn.execute(
            """
            INSERT OR IGNORE INTO sector_document_links(sector_document_id, document_id, stock_code, created_at)
            SELECT sector_document_id, ?, stock_code, created_at
            FROM sector_document_links
            WHERE document_id = ?
            """,
            (target_document_id, source_document_id),
        )
        conn.execute("DELETE FROM sector_document_links WHERE document_id = ?", (source_document_id,))
        conn.execute("DELETE FROM documents WHERE id = ?", (source_document_id,))


def get_app_meta_value(conn: sqlite3.Connection, key: str) -> str:
    value = _get_app_meta(conn, key)
    return compact_text(value or "")


def set_app_meta_value(conn: sqlite3.Connection, key: str, value: str, *, commit: bool = True) -> None:
    _set_app_meta(conn, compact_text(key), compact_text(value))
    if commit:
        conn.commit()


def _get_app_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    return str(row["value"])


def _set_app_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO app_meta(key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value=excluded.value,
          updated_at=excluded.updated_at
        """,
        (key, value, now_utc_iso()),
    )


def compact_doc_type(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"news", "report"}:
        return normalized
    return normalized or "news"
