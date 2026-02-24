from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from stock_mvp.models import (
    CollectedDocument,
    FinancialSnapshot,
    GeneratedSummary,
    Sector,
    SectorGeneratedSummary,
    Stock,
    StockSectorMap,
)
from stock_mvp.utils import normalize_url, now_utc_iso, to_iso_or_none, url_hash


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

CREATE TABLE IF NOT EXISTS sectors (
    sector_code TEXT PRIMARY KEY,
    sector_name_ko TEXT NOT NULL,
    sector_name_en TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL DEFAULT 'v1',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _migrate_stocks_table(conn)
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


def get_stock(conn: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT code, name, queries_json, market, exchange, currency, is_active, universe_source, rank
        FROM stocks
        WHERE code = ?
        """,
        (code,),
    ).fetchone()


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
      stock_code, source, doc_type, title, url, published_at, body, url_hash, collected_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(stock_code, source, url_hash) DO NOTHING
    """
    refresh_sql = """
    UPDATE documents
    SET
      url = ?,
      title = CASE WHEN length(?) > length(title) THEN ? ELSE title END,
      published_at = COALESCE(?, published_at),
      body = CASE WHEN length(?) > length(body) THEN ? ELSE body END,
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
    SELECT id, stock_code, source, doc_type, title, url, published_at, body
    FROM documents
    WHERE stock_code = ?
      AND COALESCE(published_at, collected_at) >= datetime('now', ?)
    ORDER BY COALESCE(published_at, collected_at) DESC
    LIMIT ?
    """
    return conn.execute(sql, (stock_code, f"-{lookback_days} days", limit)).fetchall()


def latest_documents(conn: sqlite3.Connection, stock_code: str, limit: int = 100) -> list[sqlite3.Row]:
    sql = """
    SELECT id, stock_code, source, doc_type, title, url, published_at, body
    FROM documents
    WHERE stock_code = ?
    ORDER BY COALESCE(published_at, collected_at) DESC
    LIMIT ?
    """
    return conn.execute(sql, (stock_code, limit)).fetchall()


def latest_documents_by_type(
    conn: sqlite3.Connection, stock_code: str, doc_type: str, limit: int = 100
) -> list[sqlite3.Row]:
    sql = """
    SELECT id, stock_code, source, doc_type, title, url, published_at, body
    FROM documents
    WHERE stock_code = ? AND doc_type = ?
    ORDER BY COALESCE(published_at, collected_at) DESC
    LIMIT ?
    """
    return conn.execute(sql, (stock_code, compact_doc_type(doc_type), limit)).fetchall()


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
        ORDER BY datetime(as_of) DESC
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
    SELECT s.code AS stock_code, s.name AS stock_name, x.id AS summary_id, x.as_of, x.line1
    FROM stocks s
    LEFT JOIN (
      SELECT t.*
      FROM summaries t
      JOIN (
        SELECT stock_code, MAX(datetime(as_of)) AS max_as_of
        FROM summaries
        GROUP BY stock_code
      ) m ON m.stock_code = t.stock_code AND datetime(t.as_of) = m.max_as_of
    ) x ON x.stock_code = s.code
    WHERE s.is_active = 1
    ORDER BY s.market, COALESCE(s.rank, 99999), s.code
    """
    return conn.execute(sql).fetchall()


def latest_summary_highlights(conn: sqlite3.Connection, limit: int = 12) -> list[sqlite3.Row]:
    sql = """
    SELECT s.code AS stock_code, s.name AS stock_name, s.market, x.as_of, x.line1
    FROM stocks s
    JOIN (
      SELECT t.*
      FROM summaries t
      JOIN (
        SELECT stock_code, MAX(datetime(as_of)) AS max_as_of
        FROM summaries
        GROUP BY stock_code
      ) m ON m.stock_code = t.stock_code AND datetime(t.as_of) = m.max_as_of
    ) x ON x.stock_code = s.code
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
