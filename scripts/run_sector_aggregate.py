from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db, rebuild_sector_documents, sector_document_distribution


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild sector-level deduplicated documents.")
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback days for source documents")
    parser.add_argument("--top", type=int, default=20, help="Top sectors to print by document count")
    args = parser.parse_args()

    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        sector_docs_written, sector_doc_links_written, raw_rows = rebuild_sector_documents(
            conn,
            lookback_days=max(1, args.lookback_days),
        )
        dist = sector_document_distribution(conn, limit=max(1, args.top))

    print("Sector aggregate done")
    print(f"lookback_days={max(1, args.lookback_days)}")
    print(f"raw_rows={raw_rows}")
    print(f"sector_docs_written={sector_docs_written}")
    print(f"sector_doc_links_written={sector_doc_links_written}")
    print("top_sectors:")
    for row in dist:
        print(f"- {row['sector_code']} / {row['sector_name_ko']}: {row['doc_count']}")


if __name__ == "__main__":
    main()
