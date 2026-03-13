from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db
from stock_mvp.news_ingest.krx_master import sync_krx_master


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync KR ticker master and alias tables (data.go.kr + fallback).")
    parser.add_argument("--force", action="store_true", help="Ignore refresh interval and sync now.")
    args = parser.parse_args()

    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        stats = sync_krx_master(conn, settings, force=bool(args.force))
    print(
        f"mode={stats.get('mode')} master_rows={int(stats.get('master_rows') or 0)} "
        f"alias_rows={int(stats.get('alias_rows') or 0)}"
    )


if __name__ == "__main__":
    main()

