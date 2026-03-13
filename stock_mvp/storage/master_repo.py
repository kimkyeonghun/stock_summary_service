from __future__ import annotations

import sqlite3

from stock_mvp.database import get_app_meta_value, list_stocks_by_market, set_app_meta_value
from stock_mvp.utils import compact_text, now_utc_iso


KRX_MASTER_SYNC_META_KEY = "krx_master_kr.last_sync_at"


def upsert_ticker_master_rows(
    conn: sqlite3.Connection,
    rows: list[tuple[str, str, str, str, str, str, str, str]],
    *,
    commit: bool = True,
) -> int:
    if not rows:
        return 0
    now_iso = now_utc_iso()
    conn.executemany(
        """
        INSERT INTO ticker_master_kr(
          ticker, company_name, corp_name, short_code, isin, market_type, base_date, status, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
          company_name=excluded.company_name,
          corp_name=excluded.corp_name,
          short_code=excluded.short_code,
          isin=excluded.isin,
          market_type=excluded.market_type,
          base_date=excluded.base_date,
          status=excluded.status,
          updated_at=excluded.updated_at
        """,
        [
            (
                compact_text(ticker).upper(),
                compact_text(company_name),
                compact_text(corp_name),
                compact_text(short_code),
                compact_text(isin),
                compact_text(market_type),
                compact_text(base_date),
                compact_text(status or "active"),
                now_iso,
            )
            for ticker, company_name, corp_name, short_code, isin, market_type, base_date, status in rows
            if compact_text(ticker)
        ],
    )
    if commit:
        conn.commit()
    return len(rows)


def list_ticker_master_kr(conn: sqlite3.Connection, *, active_only: bool = True) -> list[sqlite3.Row]:
    sql = """
    SELECT ticker, company_name, corp_name, short_code, isin, market_type, base_date, status, updated_at
    FROM ticker_master_kr
    """
    params: list[object] = []
    if active_only:
        sql += " WHERE lower(status) = 'active'"
    sql += " ORDER BY ticker"
    return conn.execute(sql, tuple(params)).fetchall()


def upsert_alias_rows(
    conn: sqlite3.Connection,
    rows: list[tuple[str, str, str, float, bool]],
    *,
    commit: bool = True,
) -> int:
    if not rows:
        return 0
    now_iso = now_utc_iso()
    conn.executemany(
        """
        INSERT INTO alias_master_kr(ticker, alias, alias_type, weight, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, alias, alias_type) DO UPDATE SET
          weight=excluded.weight,
          is_active=excluded.is_active,
          updated_at=excluded.updated_at
        """,
        [
            (
                compact_text(ticker).upper(),
                compact_text(alias),
                compact_text(alias_type).lower() or "manual",
                float(weight),
                1 if is_active else 0,
                now_iso,
                now_iso,
            )
            for ticker, alias, alias_type, weight, is_active in rows
            if compact_text(ticker) and compact_text(alias)
        ],
    )
    if commit:
        conn.commit()
    return len(rows)


def list_active_aliases(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT alias_id, ticker, alias, alias_type, weight, is_active
        FROM alias_master_kr
        WHERE is_active = 1
        ORDER BY length(alias) DESC, alias
        """
    ).fetchall()


def list_kr_stocks_for_master_fallback(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list_stocks_by_market(conn, "KR", active_only=True)


def get_krx_master_last_sync_at(conn: sqlite3.Connection) -> str:
    return get_app_meta_value(conn, KRX_MASTER_SYNC_META_KEY)


def set_krx_master_last_sync_at(conn: sqlite3.Connection, iso_ts: str, *, commit: bool = True) -> None:
    set_app_meta_value(conn, KRX_MASTER_SYNC_META_KEY, iso_ts, commit=commit)

