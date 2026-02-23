from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.web import create_app


def main() -> None:
    settings = load_settings()
    app = create_app(settings)
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    debug = settings.app_env == "dev"
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()

