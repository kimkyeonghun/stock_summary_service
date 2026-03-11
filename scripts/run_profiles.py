from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.company_profile import CompanyProfileCollector
from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db, list_stocks, upsert_stock_profile
from stock_mvp.models import Stock


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect company profile descriptions for stocks.")
    parser.add_argument(
        "--market",
        type=str.upper,
        default=None,
        choices=["KR", "US"],
        help="Optional market filter. Use KR or US.",
    )
    parser.add_argument("--stock-codes", type=str, default="", help="Optional comma-separated stock codes")
    parser.add_argument("--force", action="store_true", help="Overwrite existing manual profile values as well.")
    args = parser.parse_args()

    code_filter = {x.strip().upper() for x in re.split(r"[,\s;]+", args.stock_codes.strip()) if x.strip()}
    settings = load_settings()
    collector = CompanyProfileCollector(settings)

    written = 0
    skipped = 0
    errors = 0
    error_details: list[str] = []

    with connect(settings.db_path) as conn:
        init_db(conn)
        rows = list_stocks(conn)
        if args.market:
            rows = [r for r in rows if str(r["market"]).upper() == args.market]
        if code_filter:
            rows = [r for r in rows if str(r["code"]).upper() in code_filter]

        stocks = [_row_to_stock(row) for row in rows]
        for stock in stocks:
            try:
                profile = collector.collect(conn, stock)
                if profile is None or not profile.description_ko.strip():
                    skipped += 1
                    continue
                ok = upsert_stock_profile(
                    conn,
                    stock_code=profile.stock_code,
                    market=profile.market,
                    description_ko=profile.description_ko,
                    description_raw=profile.description_raw,
                    source=profile.source,
                    source_url=profile.source_url,
                    is_manual=False,
                    source_updated_at=profile.source_updated_at,
                    force=args.force,
                    commit=False,
                )
                if ok:
                    written += 1
                else:
                    skipped += 1
            except Exception as exc:
                errors += 1
                detail = f"stock={stock.code} error={exc}"
                error_details.append(detail)
                print(f"[WARN] profile collect failed: {detail}")
        conn.commit()

    print("Profile collection done")
    print(f"written={written}")
    print(f"skipped={skipped}")
    print(f"errors={errors}")
    if error_details:
        print("error_details:")
        for idx, detail in enumerate(error_details, start=1):
            print(f"{idx}. {detail}")


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

