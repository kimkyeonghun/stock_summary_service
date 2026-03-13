from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db
from stock_mvp.news_ingest.news_normalizer import normalize_pending_items


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize raw RSS news items.")
    parser.add_argument("--limit", type=int, default=300, help="Max raw items to normalize.")
    args = parser.parse_args()

    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        stats = normalize_pending_items(conn, settings, limit=max(1, args.limit))
    print(
        f"scanned={int(stats.get('scanned') or 0)} normalized={int(stats.get('normalized') or 0)} "
        f"failed={int(stats.get('failed') or 0)}"
    )


if __name__ == "__main__":
    main()

