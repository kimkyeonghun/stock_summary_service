from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.universe import UniverseRefresher


def main() -> None:
    settings = load_settings()
    refresher = UniverseRefresher(settings)
    result = refresher.refresh_all(kr_limit=100, us_limit=100)
    print("Universe refresh done")
    print(f"kr_requested={result.kr_requested}")
    print(f"kr_active={result.kr_active}")
    print(f"us_requested={result.us_requested}")
    print(f"us_active={result.us_active}")


if __name__ == "__main__":
    main()

