from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from stock_mvp.models import CollectedDocument, GeneratedSummary, Stock
from stock_mvp.utils import now_utc_iso, to_iso_or_none, url_hash


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
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _migrate_stocks_table(conn)
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


def insert_documents(conn: sqlite3.Connection, docs: list[CollectedDocument]) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    sql = """
    INSERT INTO documents(
      stock_code, source, doc_type, title, url, published_at, body, url_hash, collected_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(stock_code, source, url_hash) DO NOTHING
    """
    for doc in docs:
        cursor = conn.execute(
            sql,
            (
                doc.stock_code,
                doc.source,
                doc.doc_type,
                doc.title,
                doc.url,
                to_iso_or_none(doc.published_at),
                doc.body,
                url_hash(doc.url),
                now_utc_iso(),
            ),
        )
        if cursor.rowcount > 0:
            inserted += 1
        else:
            skipped += 1
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


def save_summary(conn: sqlite3.Connection, summary: GeneratedSummary) -> int:
    if len(summary.lines) != 8:
        raise ValueError("summary must contain exactly 8 lines")

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
            summary.lines[0].text,
            summary.lines[1].text,
            summary.lines[2].text,
            summary.lines[3].text,
            summary.lines[4].text,
            summary.lines[5].text,
            summary.lines[6].text,
            summary.lines[7].text,
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
    conn.commit()
    return summary_id


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


def summary_source_documents(conn: sqlite3.Connection, summary_id: int) -> list[sqlite3.Row]:
    sql = """
    SELECT ss.line_no, d.id, d.source, d.title, d.url, d.published_at
    FROM summary_sources ss
    JOIN documents d ON d.id = ss.document_id
    WHERE ss.summary_id = ?
    ORDER BY ss.line_no, d.published_at DESC
    """
    return conn.execute(sql, (summary_id,)).fetchall()


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
