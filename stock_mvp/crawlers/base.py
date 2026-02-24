from __future__ import annotations

from abc import ABC, abstractmethod

import requests
import urllib3

from stock_mvp.config import Settings
from stock_mvp.models import CollectedDocument, Stock


class BaseCrawler(ABC):
    source: str
    doc_type: str

    def __init__(self, settings: Settings):
        self.settings = settings
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

    def _get(self, url: str, **kwargs):
        kwargs.setdefault("timeout", self.settings.request_timeout_sec)
        kwargs.setdefault("verify", self.verify)
        return self.session.get(url, **kwargs)

    def reset_run_state(self) -> None:
        return None

    @abstractmethod
    def collect(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        raise NotImplementedError
