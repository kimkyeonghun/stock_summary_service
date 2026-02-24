from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db, upsert_stocks
from stock_mvp.sector_mapping import sync_sector_mapping_for_active_stocks
from stock_mvp.stocks import DEFAULT_STOCKS


def main() -> None:
    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        upsert_stocks(conn, DEFAULT_STOCKS)
        sync_sector_mapping_for_active_stocks(conn, settings=settings, refresh_kr_external=False)
    print(f"DB initialized at: {settings.db_path}")


if __name__ == "__main__":
    main()
