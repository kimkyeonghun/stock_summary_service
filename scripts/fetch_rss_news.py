from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db
from stock_mvp.news_ingest.rss_fetcher import fetch_rss_items


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch KR RSS news candidates into raw_news_items.")
    parser.add_argument("--limit-per-source", type=int, default=0, help="Override per-source fetch cap.")
    args = parser.parse_args()

    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        stats = fetch_rss_items(
            conn,
            settings,
            limit_per_source=max(1, args.limit_per_source) if args.limit_per_source > 0 else None,
        )
    print(
        f"sources={int(stats.get('sources') or 0)} fetched={int(stats.get('fetched') or 0)} "
        f"inserted={int(stats.get('inserted') or 0)} url_duplicates={int(stats.get('url_duplicates') or 0)} "
        f"content_duplicates={int(stats.get('content_duplicates') or 0)} errors={int(stats.get('errors') or 0)}"
    )


if __name__ == "__main__":
    main()

