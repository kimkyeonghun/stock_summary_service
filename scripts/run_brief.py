from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.briefing import send_morning_brief
from stock_mvp.config import load_settings


def main() -> None:
    settings = load_settings()
    result = send_morning_brief(settings)
    print(f"sent={result.sent}")
    print(f"message={result.message}")
    print(f"item_count={result.item_count}")


if __name__ == "__main__":
    main()

