from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.prices import PriceCollector


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect daily price bars.")
    parser.add_argument(
        "--market",
        type=str.upper,
        required=True,
        choices=["KR", "US"],
        help="Target market. Use KR or US.",
    )
    parser.add_argument("--stock-codes", type=str, default="", help="Optional comma-separated stock codes")
    parser.add_argument("--lookback-days", type=int, default=400, help="Days to request from price source")
    args = parser.parse_args()

    settings = load_settings()
    collector = PriceCollector(settings)
    stock_codes = [x for x in re.split(r"[,\s;]+", args.stock_codes.strip()) if x] or None
    stats = collector.collect_market(
        market=args.market,
        stock_codes=stock_codes,
        lookback_days=max(5, args.lookback_days),
    )

    print("Price collection done")
    print(f"market={stats.market}")
    print(f"stock_count={stats.stock_count}")
    print(f"success_count={stats.success_count}")
    print(f"error_count={stats.error_count}")
    print(f"bars_upserted={stats.bars_upserted}")
    if stats.error_details:
        print("error_details:")
        for idx, detail in enumerate(stats.error_details, start=1):
            print(f"{idx}. {detail}")


if __name__ == "__main__":
    main()
