from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.translation_backfill import print_backfill_summary, run_backfill


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Korean translations for recently generated outputs.")
    parser.add_argument("--days", type=int, default=14, help="Lookback window in days")
    parser.add_argument("--scope", type=str, default="all", help="all or comma-separated: item,evidence,digest,report,profile")
    parser.add_argument("--market", type=str.upper, default="ALL", choices=["KR", "US", "ALL"])
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Max rows per scope (0 means unlimited). Recommended operational value: 1000",
    )
    parser.add_argument(
        "--translation-retries",
        type=int,
        default=0,
        help="Override translation retries for this run (default: 0)",
    )
    args = parser.parse_args()

    try:
        summary = run_backfill(
            days=max(1, int(args.days)),
            scope=args.scope,
            market=args.market,
            max_rows=args.max_rows,
            translation_retries=args.translation_retries,
        )
    except (RuntimeError, ValueError) as exc:
        print(str(exc))
        raise SystemExit(1)
    print_backfill_summary(summary)


if __name__ == "__main__":
    main()
