from __future__ import annotations

import re
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

from stock_mvp.config import Settings
from stock_mvp.database import connect, init_db, replace_universe_stocks
from stock_mvp.models import Stock
from stock_mvp.utils import compact_text


KOSPI_MARKET_SUM_URL = "https://finance.naver.com/sise/sise_market_sum.naver"
SP500_SLICKCHARTS_URL = "https://www.slickcharts.com/sp500"
NASDAQ100_SLICKCHARTS_URL = "https://www.slickcharts.com/nasdaq100"

US_FALLBACK_TICKERS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "BRK.B",
    "TSLA",
    "AVGO",
    "JPM",
    "LLY",
    "V",
    "XOM",
    "UNH",
    "MA",
    "COST",
    "NFLX",
    "PG",
    "HD",
    "JNJ",
]


@dataclass(frozen=True)
class UniverseRefreshResult:
    kr_requested: int
    kr_active: int
    us_requested: int
    us_active: int


class UniverseRefresher:
    def __init__(self, settings: Settings):
        self.settings = settings
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
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl

    def refresh_all(self, kr_limit: int = 100, us_limit: int = 100) -> UniverseRefreshResult:
        kr_stocks: list[Stock] = []
        us_stocks: list[Stock] = []
        try:
            kr_stocks = self.fetch_kospi_top(kr_limit)
        except Exception as exc:
            print(f"[WARN] universe refresh kr failed: {exc}")
        try:
            us_stocks = self.fetch_us_large_caps(us_limit)
        except Exception as exc:
            print(f"[WARN] universe refresh us failed: {exc}")

        with connect(self.settings.db_path) as conn:
            init_db(conn)
            if kr_stocks:
                kr_requested, kr_active = replace_universe_stocks(
                    conn, market="KR", universe_source="kospi_top100", stocks=kr_stocks
                )
            else:
                kr_requested = 0
                kr_active = int(
                    conn.execute(
                        "SELECT COUNT(*) AS cnt FROM stocks WHERE market='KR' AND universe_source='kospi_top100' AND is_active=1"
                    ).fetchone()["cnt"]
                )
            if us_stocks:
                us_requested, us_active = replace_universe_stocks(
                    conn, market="US", universe_source="us_large_cap", stocks=us_stocks
                )
            else:
                us_requested = 0
                us_active = int(
                    conn.execute(
                        "SELECT COUNT(*) AS cnt FROM stocks WHERE market='US' AND universe_source='us_large_cap' AND is_active=1"
                    ).fetchone()["cnt"]
                )

        return UniverseRefreshResult(
            kr_requested=kr_requested,
            kr_active=kr_active,
            us_requested=us_requested,
            us_active=us_active,
        )

    def fetch_kospi_top(self, limit: int = 100) -> list[Stock]:
        stocks: list[Stock] = []
        seen: set[str] = set()

        for page in range(1, 20):
            response = self.session.get(
                KOSPI_MARKET_SUM_URL,
                params={"sosok": "0", "page": str(page)},
                timeout=self.settings.request_timeout_sec,
                verify=self.verify,
            )
            response.raise_for_status()

            # Naver finance pages are typically cp949/euc-kr.
            html = response.content.decode("euc-kr", "ignore")
            soup = BeautifulSoup(html, "html.parser")
            rows = soup.select("table.type_2 tr")
            page_added = 0

            for row in rows:
                link = row.select_one("a[href*='code=']")
                if not link:
                    continue
                href = link.get("href", "")
                match = re.search(r"code=(\d{6})", href)
                if not match:
                    continue
                code = match.group(1)
                if code in seen:
                    continue
                name = compact_text(link.get_text(" ", strip=True))
                if not name:
                    continue
                seen.add(code)
                rank = len(stocks) + 1
                stocks.append(
                    Stock(
                        code=code,
                        name=name,
                        queries=[name, code],
                        market="KR",
                        exchange="KRX",
                        currency="KRW",
                        universe_source="kospi_top100",
                        rank=rank,
                    )
                )
                page_added += 1
                if len(stocks) >= limit:
                    return stocks

            if page_added == 0:
                break
        return stocks

    def fetch_us_large_caps(self, limit: int = 100) -> list[Stock]:
        combined: list[Stock] = []
        seen: set[str] = set()

        for url in (SP500_SLICKCHARTS_URL, NASDAQ100_SLICKCHARTS_URL):
            try:
                parsed = self._fetch_slickcharts(url)
            except Exception as exc:
                print(f"[WARN] us universe source failed: url={url} error={exc}")
                parsed = []
            for stock in parsed:
                ticker = stock.code.upper()
                if ticker in seen:
                    continue
                seen.add(ticker)
                combined.append(stock)
                if len(combined) >= limit:
                    return self._re_rank_us(combined)

        if not combined:
            for ticker in US_FALLBACK_TICKERS:
                combined.append(
                    Stock(
                        code=ticker,
                        name=ticker,
                        queries=[ticker],
                        market="US",
                        exchange="NASDAQ",
                        currency="USD",
                        universe_source="us_large_cap",
                    )
                )
                if len(combined) >= limit:
                    break

        return self._re_rank_us(combined[:limit])

    def _fetch_slickcharts(self, url: str) -> list[Stock]:
        response = self.session.get(
            url,
            timeout=self.settings.request_timeout_sec,
            verify=self.verify,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        items: list[Stock] = []
        rows = soup.select("table tbody tr")
        for row in rows:
            ticker_link = row.select_one("a[href^='/symbol/']")
            if not ticker_link:
                continue
            ticker = compact_text(ticker_link.get_text(" ", strip=True)).upper()
            if not re.fullmatch(r"[A-Z.\-]{1,8}", ticker):
                continue

            cells = row.select("td")
            # Typical columns: rank, company, symbol, weight, price, chg, pct
            company = ""
            if len(cells) >= 2:
                company = compact_text(cells[1].get_text(" ", strip=True))
            if not company:
                company = ticker

            exchange = "NASDAQ" if "nasdaq" in url.lower() else "NYSE"
            items.append(
                Stock(
                    code=ticker,
                    name=company,
                    queries=[ticker, company],
                    market="US",
                    exchange=exchange,
                    currency="USD",
                    universe_source="us_large_cap",
                )
            )
        return items

    @staticmethod
    def _re_rank_us(stocks: list[Stock]) -> list[Stock]:
        ranked: list[Stock] = []
        for idx, stock in enumerate(stocks, start=1):
            ranked.append(
                Stock(
                    code=stock.code,
                    name=stock.name,
                    queries=stock.queries,
                    market="US",
                    exchange=stock.exchange,
                    currency="USD",
                    is_active=True,
                    universe_source="us_large_cap",
                    rank=idx,
                )
            )
        return ranked
