from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db
from stock_mvp.news_ingest.krx_master import sync_krx_master
from stock_mvp.news_ingest.sector_router import map_and_route_pending_items


def main() -> None:
    parser = argparse.ArgumentParser(description="Map normalized RSS news into ticker/sector entities.")
    parser.add_argument("--limit", type=int, default=300, help="Max normalized items to process.")
    parser.add_argument("--dry-run", action="store_true", help="Do not route into documents/sector_documents.")
    parser.add_argument(
        "--allowed-tickers",
        default="",
        help="Optional comma-separated ticker allow-list (KR).",
    )
    args = parser.parse_args()

    settings = load_settings()
    allowed = {
        token.upper()
        for token in re.split(r"[,\s;]+", args.allowed_tickers.strip())
        if token.strip()
    }
    with connect(settings.db_path) as conn:
        init_db(conn)
        sync_krx_master(conn, settings, force=False)
        stats = map_and_route_pending_items(
            conn,
            settings,
            limit=max(1, args.limit),
            allowed_tickers=allowed or None,
            dry_run=bool(args.dry_run),
        )
    print(
        f"scanned={int(stats.get('scanned') or 0)} mapped_ticker={int(stats.get('mapped_ticker') or 0)} "
        f"mapped_sector={int(stats.get('mapped_sector') or 0)} unassigned={int(stats.get('unassigned') or 0)} "
        f"routed_documents={int(stats.get('routed_documents') or 0)} "
        f"routed_sector_documents={int(stats.get('routed_sector_documents') or 0)} dry_run={bool(args.dry_run)}"
    )


if __name__ == "__main__":
    main()

