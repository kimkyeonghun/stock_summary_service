from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import (
    connect,
    init_db,
    latest_sector_documents,
    recent_sector_targets,
    save_sector_summary,
)
from stock_mvp.sector_summarizer import SectorSummaryBuilder


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sector summaries from aggregated sector documents.")
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback days for sector docs")
    parser.add_argument("--limit", type=int, default=30, help="Maximum number of sectors to summarize")
    args = parser.parse_args()

    settings = load_settings()
    builder = SectorSummaryBuilder(settings)
    written = 0
    errors = 0

    with connect(settings.db_path) as conn:
        init_db(conn)
        targets = recent_sector_targets(conn, lookback_days=max(1, args.lookback_days), limit=max(1, args.limit))
        for target in targets:
            sector_code = str(target["sector_code"])
            sector_name = str(target["sector_name_ko"] or target["sector_name_en"] or sector_code)
            docs = latest_sector_documents(
                conn,
                sector_code=sector_code,
                lookback_days=max(1, args.lookback_days),
                limit=90,
            )
            try:
                summary = builder.build(sector_code=sector_code, sector_name=sector_name, docs=[dict(r) for r in docs])
                save_sector_summary(conn, summary)
                written += 1
            except Exception as exc:
                errors += 1
                print(f"[WARN] sector summarize failed: sector={sector_code} error={exc}")

    print("Sector summarize done")
    print(f"lookback_days={max(1, args.lookback_days)}")
    print(f"written={written}")
    print(f"errors={errors}")


if __name__ == "__main__":
    main()
