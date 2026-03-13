from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db
from stock_mvp.storage import master_repo
from stock_mvp.utils import compact_text


def main() -> None:
    parser = argparse.ArgumentParser(description="Import manual alias rows into alias_master_kr from CSV.")
    parser.add_argument("csv_path", type=str, help="CSV path with columns: ticker,alias,alias_type,weight,is_active")
    args = parser.parse_args()

    rows: list[tuple[str, str, str, float, bool]] = []
    with open(args.csv_path, "r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        for raw in reader:
            ticker = compact_text(str(raw.get("ticker") or "")).upper()
            alias = compact_text(str(raw.get("alias") or ""))
            alias_type = compact_text(str(raw.get("alias_type") or "manual")).lower()
            if not ticker or not alias:
                continue
            try:
                weight = float(raw.get("weight") or 1.0)
            except ValueError:
                weight = 1.0
            is_active = str(raw.get("is_active") or "1").strip().lower() not in {"0", "false", "n", "no"}
            rows.append((ticker, alias, alias_type, weight, is_active))

    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        inserted = master_repo.upsert_alias_rows(conn, rows, commit=True)
    print(f"imported_alias_rows={inserted}")


if __name__ == "__main__":
    main()

