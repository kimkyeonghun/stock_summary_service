from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import (
    connect,
    financial_refresh_needed,
    init_db,
    list_stocks,
    upsert_financial_snapshot,
)
from stock_mvp.financials import FinancialCollector
from stock_mvp.models import Stock


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect financial snapshots for active stocks.")
    parser.add_argument("--stock-codes", type=str, default="", help="Comma-separated stock codes")
    args = parser.parse_args()

    settings = load_settings()
    collector = FinancialCollector(settings)

    written = 0
    skipped = 0
    errors = 0

    with connect(settings.db_path) as conn:
        init_db(conn)
        rows = list_stocks(conn)
        code_filter = {x.strip().upper() for x in re.split(r"[,\s;]+", args.stock_codes.strip()) if x.strip()}
        if code_filter:
            rows = [row for row in rows if str(row["code"]).upper() in code_filter]
        stocks = [_row_to_stock(row) for row in rows]
        for stock in stocks:
            if not financial_refresh_needed(conn, stock.code, min_hours=settings.financial_refresh_min_hours):
                skipped += 1
                continue
            try:
                snapshot = collector.collect(stock)
                if snapshot is None:
                    skipped += 1
                    continue
                upsert_financial_snapshot(conn, snapshot)
                written += 1
            except Exception as exc:
                errors += 1
                print(f"[WARN] financial collect failed: stock={stock.code} error={exc}")

    print("Financial collection done")
    print(f"written={written}")
    print(f"skipped={skipped}")
    print(f"errors={errors}")


def _row_to_stock(row) -> Stock:
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


if __name__ == "__main__":
    main()
