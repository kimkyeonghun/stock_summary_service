from __future__ import annotations

import re
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup

from stock_mvp.config import Settings
from stock_mvp.models import FinancialSnapshot, Stock
from stock_mvp.utils import now_utc


NAVER_ITEM_MAIN_URL = "https://finance.naver.com/item/main.naver"
YAHOO_QUOTE_SUMMARY_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
YAHOO_COOKIE_BOOTSTRAP_URL = "https://fc.yahoo.com"
YAHOO_CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"


class FinancialCollector:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        # Avoid broken enterprise proxy settings by default for data endpoints.
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
        self._yahoo_crumb: str = ""

    def collect(self, stock: Stock) -> FinancialSnapshot | None:
        market = stock.market.upper()
        if market == "KR":
            return self._collect_kr(stock)
        if market == "US":
            return self._collect_us(stock)
        return None

    def _collect_kr(self, stock: Stock) -> FinancialSnapshot:
        response = self._get(
            NAVER_ITEM_MAIN_URL,
            params={"code": stock.code},
            headers={"Referer": "https://finance.naver.com/"},
        )
        response.raise_for_status()
        html = response.content.decode("euc-kr", "ignore")
        soup = BeautifulSoup(html, "html.parser")

        per = _parse_float(_tag_text(soup, "_per"))
        pbr = _parse_float(_tag_text(soup, "_pbr"))
        eps = _parse_float(_tag_text(soup, "_eps"))
        market_cap = _parse_kr_market_cap(_tag_text(soup, "_market_sum"))
        roe = _parse_kr_roe(soup)

        return FinancialSnapshot(
            stock_code=stock.code,
            as_of=now_utc(),
            source="naver_finance_main",
            per=per,
            pbr=pbr,
            eps=eps,
            roe=roe,
            market_cap=market_cap,
            currency=stock.currency or "KRW",
        )

    def _collect_us(self, stock: Stock) -> FinancialSnapshot:
        ticker = stock.code.strip().upper()
        response = None
        for attempt in range(2):
            crumb = self._ensure_yahoo_crumb(force_refresh=attempt > 0)
            params = {"modules": "price,summaryDetail,defaultKeyStatistics,financialData"}
            if crumb:
                params["crumb"] = crumb
            response = self._get(
                YAHOO_QUOTE_SUMMARY_URL.format(ticker=ticker),
                params=params,
                headers={"Referer": f"https://finance.yahoo.com/quote/{ticker}"},
            )
            if response.status_code == 401 and attempt == 0:
                # Refresh crumb and retry once.
                continue
            break
        if response is None:
            raise ValueError("yahoo quoteSummary response is missing")
        response.raise_for_status()
        data = response.json()
        result_list = ((data.get("quoteSummary") or {}).get("result") or [])
        if not result_list:
            raise ValueError(f"yahoo quoteSummary has no result: ticker={ticker}")
        result = result_list[0]

        per = _to_float(_raw_value(result, "summaryDetail", "trailingPE"))
        if per is None:
            per = _to_float(_raw_value(result, "defaultKeyStatistics", "trailingPE"))
        pbr = _to_float(_raw_value(result, "defaultKeyStatistics", "priceToBook"))
        eps = _to_float(_raw_value(result, "defaultKeyStatistics", "trailingEps"))
        roe_raw = _to_float(_raw_value(result, "financialData", "returnOnEquity"))
        roe = _normalize_roe_percent(roe_raw)
        market_cap = _to_int(_raw_value(result, "price", "marketCap"))
        currency = str((_raw_value(result, "price", "currency") or stock.currency or "USD"))

        return FinancialSnapshot(
            stock_code=stock.code,
            as_of=now_utc(),
            source="yahoo_quote_summary",
            per=per,
            pbr=pbr,
            eps=eps,
            roe=roe,
            market_cap=market_cap,
            currency=currency,
        )

    def _ensure_yahoo_crumb(self, force_refresh: bool = False) -> str:
        if self._yahoo_crumb and not force_refresh:
            return self._yahoo_crumb
        self._get(YAHOO_COOKIE_BOOTSTRAP_URL, headers={"Referer": "https://finance.yahoo.com/"})
        response = self._get(YAHOO_CRUMB_URL, headers={"Referer": "https://finance.yahoo.com/"})
        if response.status_code == 200:
            crumb = (response.text or "").strip()
            if crumb and "{" not in crumb:
                self._yahoo_crumb = crumb
                return crumb
        self._yahoo_crumb = ""
        return ""

    def _get(self, url: str, **kwargs):
        kwargs.setdefault("timeout", self.settings.request_timeout_sec)
        kwargs.setdefault("verify", self.verify)
        try:
            return self.session.get(url, **kwargs)
        except requests.exceptions.SSLError:
            kwargs["verify"] = False
            return self.session.get(url, **kwargs)


def _tag_text(soup: BeautifulSoup, tag_id: str) -> str:
    node = soup.select_one(f"#{tag_id}")
    if not node:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    text = value.strip()
    if text in {"N/A", "-", ""}:
        return None
    text = text.replace(",", "")
    text = text.replace("%", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _parse_kr_roe(soup: BeautifulSoup) -> float | None:
    for row in soup.select("div.section.cop_analysis table tr"):
        th = row.select_one("th")
        if not th:
            continue
        label = th.get_text(" ", strip=True).upper()
        if "ROE" not in label:
            continue
        values = [td.get_text(" ", strip=True) for td in row.select("td")]
        for value in values:
            parsed = _parse_float(value)
            if parsed is not None:
                return parsed
    return None


def _parse_kr_market_cap(value: str | None) -> int | None:
    if not value:
        return None
    nums = [int(x.replace(",", "")) for x in re.findall(r"\d[\d,]*", value)]
    if not nums:
        return None

    if len(nums) >= 2:
        jo, eok = nums[0], nums[1]
        return jo * 1_0000_0000_0000 + eok * 100_000_000

    only = nums[0]
    text = value
    # Typical format is "X조 Y억". In legacy encoding, 조 can appear as garbled glyphs.
    if any(token in text for token in ("조", "臁", "炼")):
        return only * 1_0000_0000_0000
    return only * 100_000_000


def _raw_value(payload: dict, *path: str):
    cur = payload
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    if isinstance(cur, dict) and "raw" in cur:
        return cur.get("raw")
    return cur


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_roe_percent(value: float | None) -> float | None:
    if value is None:
        return None
    # Yahoo often returns a ratio (e.g., 0.25). Convert to percentage for consistency.
    if -1.5 <= value <= 1.5:
        return value * 100
    return value
