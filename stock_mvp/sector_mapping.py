from __future__ import annotations

import json
import sqlite3

from stock_mvp.config import Settings
from stock_mvp.database import get_stock_sectors, list_stocks, replace_stock_sector_maps, upsert_sectors
from stock_mvp.kr_sector_naver import NaverUpjongSectorFetcher
from stock_mvp.models import Stock, StockSectorMap
from stock_mvp.sector_taxonomy import DEFAULT_SECTORS, infer_sector_maps_for_stock


def sync_sector_mapping_for_active_stocks(
    conn: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    refresh_kr_external: bool = False,
) -> tuple[int, int]:
    rows = list_stocks(conn)
    stocks = [_row_to_stock(row) for row in rows]
    return sync_sector_mapping_for_stocks(
        conn,
        stocks,
        settings=settings,
        refresh_kr_external=refresh_kr_external,
    )


def sync_sector_mapping_for_stocks(
    conn: sqlite3.Connection,
    stocks: list[Stock],
    *,
    settings: Settings | None = None,
    refresh_kr_external: bool = False,
) -> tuple[int, int]:
    upsert_sectors(conn, DEFAULT_SECTORS)

    kr_external_maps: dict[str, list[StockSectorMap]] = {}
    if refresh_kr_external and settings is not None:
        try:
            fetched = NaverUpjongSectorFetcher(settings).fetch()
            if fetched.sectors:
                upsert_sectors(conn, fetched.sectors)
            kr_external_maps = fetched.stock_maps
            print(
                f"[INFO] kr sector source=naver_upjong sectors={len(fetched.sectors)} "
                f"mapped_stocks={len(fetched.stock_maps)}"
            )
        except Exception as exc:
            print(f"[WARN] kr sector source=naver_upjong failed: {exc}")

    mapped_stock_count = 0
    mapped_sector_count = 0
    for stock in stocks:
        if stock.market.upper() == "KR" and stock.code in kr_external_maps:
            mappings = kr_external_maps[stock.code]
        elif stock.market.upper() == "KR" and not refresh_kr_external:
            existing_rows = get_stock_sectors(conn, stock.code)
            has_existing_upjong = any(str(r["mapping_source"]).startswith("naver_upjong_") for r in existing_rows)
            if has_existing_upjong:
                mapped_sector_count += len(existing_rows)
                mapped_stock_count += 1
                continue
            mappings = infer_sector_maps_for_stock(stock)
        else:
            mappings = infer_sector_maps_for_stock(stock)
        mapped_sector_count += replace_stock_sector_maps(conn, stock.code, mappings)
        mapped_stock_count += 1
    return mapped_stock_count, mapped_sector_count


def _row_to_stock(row: sqlite3.Row) -> Stock:
    return Stock(
        code=row["code"],
        name=row["name"],
        queries=json.loads(row["queries_json"]),
        market=row["market"],
        exchange=row["exchange"],
        currency=row["currency"],
        is_active=bool(row["is_active"]),
        universe_source=row["universe_source"],
        rank=row["rank"],
    )
