from __future__ import annotations

from datetime import datetime, timezone
import sqlite3

import requests

from stock_mvp.config import Settings
from stock_mvp.news_ingest.alias_builder import build_alias_rows_for_ticker
from stock_mvp.storage import master_repo
from stock_mvp.utils import compact_text, now_utc_iso


DATA_GO_KRX_URL = "https://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo"


def sync_krx_master(conn: sqlite3.Connection, settings: Settings, *, force: bool = False) -> dict[str, int | str]:
    refresh_days = max(1, int(settings.krx_master_refresh_days))
    last_sync = master_repo.get_krx_master_last_sync_at(conn)
    if not force and _is_recent(last_sync, refresh_days=refresh_days):
        return {"master_rows": 0, "alias_rows": 0, "mode": "cached"}

    source_rows = _fetch_from_data_go(settings)
    mode = "data_go"
    if not source_rows:
        source_rows = _fallback_rows_from_stocks(conn)
        mode = "stocks_fallback"

    master_rows: list[tuple[str, str, str, str, str, str, str, str]] = []
    alias_rows: list[tuple[str, str, str, float, bool]] = []
    for row in source_rows:
        ticker = compact_text(str(row.get("ticker") or "")).upper()
        if not ticker:
            continue
        company_name = compact_text(str(row.get("company_name") or ""))
        corp_name = compact_text(str(row.get("corp_name") or company_name))
        short_code = compact_text(str(row.get("short_code") or ticker))
        isin = compact_text(str(row.get("isin") or ""))
        market_type = compact_text(str(row.get("market_type") or "KRX"))
        base_date = compact_text(str(row.get("base_date") or datetime.now(timezone.utc).strftime("%Y%m%d")))
        status = compact_text(str(row.get("status") or "active"))
        master_rows.append((ticker, company_name, corp_name, short_code, isin, market_type, base_date, status))
        alias_rows.extend(build_alias_rows_for_ticker(ticker=ticker, company_name=company_name, corp_name=corp_name))

    master_repo.upsert_ticker_master_rows(conn, master_rows, commit=False)
    master_repo.upsert_alias_rows(conn, alias_rows, commit=False)
    master_repo.set_krx_master_last_sync_at(conn, now_utc_iso(), commit=False)
    conn.commit()
    return {"master_rows": len(master_rows), "alias_rows": len(alias_rows), "mode": mode}


def _fetch_from_data_go(settings: Settings) -> list[dict[str, str]]:
    service_key = compact_text(settings.krx_master_service_key)
    if not service_key:
        return []
    verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
    session = requests.Session()
    session.trust_env = bool(settings.crawler_trust_env)

    rows: list[dict[str, str]] = []
    page = 1
    while page <= 100:
        try:
            response = session.get(
                DATA_GO_KRX_URL,
                params={
                    "serviceKey": service_key,
                    "numOfRows": 1000,
                    "pageNo": page,
                    "resultType": "json",
                },
                timeout=max(5, int(settings.request_timeout_sec)),
                verify=verify,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            print(f"[WARN] krx master fetch failed page={page} error={exc}")
            return []

        items = (((payload.get("response") or {}).get("body") or {}).get("items") or {}).get("item") or []
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list) or not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
            ticker = compact_text(str(item.get("srtnCd") or item.get("short_code") or ""))
            if not ticker:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "company_name": compact_text(str(item.get("itmsNm") or item.get("company_name") or "")),
                    "corp_name": compact_text(str(item.get("crno") or item.get("corp_name") or "")),
                    "short_code": ticker,
                    "isin": compact_text(str(item.get("isinCd") or item.get("isin") or "")),
                    "market_type": compact_text(str(item.get("mrktCtg") or item.get("market_type") or "KRX")),
                    "base_date": compact_text(str(item.get("basDt") or item.get("base_date") or "")),
                    "status": "active",
                }
            )
        if len(items) < 1000:
            break
        page += 1

    deduped: dict[str, dict[str, str]] = {}
    for row in rows:
        ticker = compact_text(str(row.get("ticker") or "")).upper()
        if ticker:
            deduped[ticker] = row
    return list(deduped.values())


def _fallback_rows_from_stocks(conn: sqlite3.Connection) -> list[dict[str, str]]:
    rows = master_repo.list_kr_stocks_for_master_fallback(conn)
    out: list[dict[str, str]] = []
    for row in rows:
        ticker = compact_text(str(row["code"] or "")).upper()
        name = compact_text(str(row["name"] or ""))
        if not ticker or not name:
            continue
        out.append(
            {
                "ticker": ticker,
                "company_name": name,
                "corp_name": name,
                "short_code": ticker,
                "isin": "",
                "market_type": compact_text(str(row["exchange"] or "KRX")),
                "base_date": datetime.now(timezone.utc).strftime("%Y%m%d"),
                "status": "active",
            }
        )
    return out


def _is_recent(text: str, *, refresh_days: int) -> bool:
    raw = compact_text(text)
    if not raw:
        return False
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    elapsed = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
    return elapsed.days < max(1, int(refresh_days))

