from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.pipeline import CollectionPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one collection cycle.")
    parser.add_argument("--stock-codes", type=str, default="", help="Comma-separated stock codes")
    args = parser.parse_args()

    settings = load_settings()
    pipeline = CollectionPipeline(settings)
    stock_codes = [x for x in re.split(r"[,\s;]+", args.stock_codes.strip()) if x] or None
    stats = pipeline.run_once(stock_codes=stock_codes)

    print("Collection done")
    print(f"run_id={stats.run_id}")
    print(f"stock_count={stats.stock_count}")
    print(f"fetched_docs={stats.fetched_docs}")
    print(f"inserted_docs={stats.inserted_docs}")
    print(f"skipped_docs={stats.skipped_docs}")
    print(f"summaries_written={stats.summaries_written}")
    print(f"error_count={stats.error_count}")


if __name__ == "__main__":
    main()
