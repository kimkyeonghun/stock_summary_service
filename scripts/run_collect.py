from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.pipeline import CollectionPipeline, PipelineBusyError


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one collection cycle.")
    parser.add_argument("--stock-codes", type=str, default="", help="Comma-separated stock codes")
    parser.add_argument(
        "--market",
        type=str.upper,
        default=None,
        choices=["KR", "US"],
        help="Optional market filter. Use KR or US.",
    )
    parser.add_argument(
        "--skip-sector",
        action="store_true",
        help="Skip sector aggregation and sector summary generation for this run.",
    )
    args = parser.parse_args()

    settings = load_settings()
    pipeline = CollectionPipeline(settings)
    stock_codes = [x for x in re.split(r"[,\s;]+", args.stock_codes.strip()) if x] or None
    market = args.market
    try:
        stats = pipeline.run_once(
            stock_codes=stock_codes,
            market=market,
            include_sector_steps=not args.skip_sector,
        )
    except PipelineBusyError as exc:
        print(f"Collection skipped: {exc}")
        raise SystemExit(2)
    except KeyboardInterrupt:
        print("Collection interrupted by user (Ctrl+C). Current run is marked as failed.")
        raise SystemExit(130)

    print("Collection done")
    print(f"run_id={stats.run_id}")
    print(f"stock_count={stats.stock_count}")
    print(f"fetched_docs={stats.fetched_docs}")
    print(f"inserted_docs={stats.inserted_docs}")
    print(f"skipped_docs={stats.skipped_docs}")
    print(f"summaries_written={stats.summaries_written}")
    print(f"sector_docs_written={stats.sector_docs_written}")
    print(f"sector_doc_links_written={stats.sector_doc_links_written}")
    print(f"sector_summaries_written={stats.sector_summaries_written}")
    print(f"sector_summary_error_count={stats.sector_summary_error_count}")
    print(f"financial_snapshots_written={stats.financial_snapshots_written}")
    print(f"financial_snapshots_skipped={stats.financial_snapshots_skipped}")
    print(f"financial_error_count={stats.financial_error_count}")
    print(f"error_count={stats.error_count}")
    if stats.error_details:
        print("error_details:")
        for idx, detail in enumerate(stats.error_details, start=1):
            print(f"{idx}. {detail}")


if __name__ == "__main__":
    main()
