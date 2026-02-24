from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urljoin

import requests
import urllib3
from bs4 import BeautifulSoup

from stock_mvp.config import Settings
from stock_mvp.models import Sector, StockSectorMap
from stock_mvp.utils import compact_text, now_utc


UPJONG_LIST_URL = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
UPJONG_DETAIL_BASE_URL = "https://finance.naver.com/sise/sise_group_detail.naver"


@dataclass(frozen=True)
class KrUpjongFetchResult:
    sectors: list[Sector]
    stock_maps: dict[str, list[StockSectorMap]]


class NaverUpjongSectorFetcher:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        # In many enterprise environments, inherited proxy vars can break direct fetch.
        self.session.trust_env = False
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        self._insecure_warning_suppressed = bool(self.verify is False)

    def fetch(self) -> KrUpjongFetchResult:
        upjongs = self._fetch_upjong_list()
        sectors: list[Sector] = []
        stock_map: dict[str, list[StockSectorMap]] = {}
        as_of = now_utc()

        for upjong_no, upjong_name in upjongs:
            sector_code = f"KR_UPJONG_{upjong_no}"
            sectors.append(
                Sector(
                    sector_code=sector_code,
                    sector_name_ko=upjong_name,
                    sector_name_en=upjong_name,
                    taxonomy_version="kr_upjong_v1",
                    is_active=True,
                )
            )
            stock_codes = self._fetch_upjong_stocks(upjong_no)
            for stock_code in stock_codes:
                stock_map.setdefault(stock_code, []).append(
                    StockSectorMap(
                        stock_code=stock_code,
                        sector_code=sector_code,
                        mapping_source="naver_upjong_v1",
                        confidence=0.92,
                        as_of=as_of,
                    )
                )

        return KrUpjongFetchResult(sectors=sectors, stock_maps=stock_map)

    def _fetch_upjong_list(self) -> list[tuple[str, str]]:
        response = self._get(UPJONG_LIST_URL)
        response.raise_for_status()
        html = response.content.decode("euc-kr", "ignore")
        soup = BeautifulSoup(html, "html.parser")

        results: list[tuple[str, str]] = []
        seen: set[str] = set()
        for link in soup.select("a[href*='sise_group_detail.naver?type=upjong&no=']"):
            name = compact_text(link.get_text(" ", strip=True))
            href = link.get("href", "")
            match = re.search(r"no=(\d+)", href)
            if not name or not match:
                continue
            upjong_no = match.group(1)
            if upjong_no in seen:
                continue
            seen.add(upjong_no)
            results.append((upjong_no, name))
        return results

    def _fetch_upjong_stocks(self, upjong_no: str) -> list[str]:
        response = self._get(
            UPJONG_DETAIL_BASE_URL,
            params={"type": "upjong", "no": upjong_no},
        )
        response.raise_for_status()
        html = response.content.decode("euc-kr", "ignore")
        soup = BeautifulSoup(html, "html.parser")

        items: list[str] = []
        seen: set[str] = set()
        for link in soup.select("a[href*='item/main.naver?code=']"):
            href = urljoin("https://finance.naver.com", link.get("href", ""))
            match = re.search(r"code=(\d{6})", href)
            if not match:
                continue
            code = match.group(1)
            if code in seen:
                continue
            seen.add(code)
            items.append(code)
        return items

    def _get(self, url: str, **kwargs):
        kwargs.setdefault("timeout", self.settings.request_timeout_sec)
        kwargs.setdefault("verify", self.verify)
        try:
            return self.session.get(url, **kwargs)
        except requests.exceptions.SSLError:
            # Corporate SSL interception environments often require either a CA bundle
            # or an insecure fallback to keep ingestion operational.
            if not self._insecure_warning_suppressed:
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                self._insecure_warning_suppressed = True
            kwargs["verify"] = False
            print(f"[WARN] kr sector source ssl verify failed, retrying insecure: url={url}")
            return self.session.get(url, **kwargs)
