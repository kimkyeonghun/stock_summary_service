from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db
from stock_mvp.sector_mapping import sync_sector_mapping_for_active_stocks


def main() -> None:
    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        mapped_stock_count, mapped_sector_count = sync_sector_mapping_for_active_stocks(
            conn,
            settings=settings,
            refresh_kr_external=True,
        )
        sector_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM sectors WHERE is_active = 1").fetchone()["cnt"])
    print(f"Sector taxonomy synced at: {settings.db_path}")
    print(f"active_sector_count={sector_count}")
    print(f"mapped_stock_count={mapped_stock_count}")
    print(f"mapped_sector_count={mapped_sector_count}")


if __name__ == "__main__":
    main()
