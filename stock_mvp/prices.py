from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests
import urllib3

from stock_mvp.config import Settings
from stock_mvp.database import connect, init_db, list_stocks_by_market, upsert_price_bars
from stock_mvp.models import PriceBar, Stock

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


YAHOO_CHART_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"


@dataclass
class PriceCollectStats:
    market: str
    stock_count: int = 0
    success_count: int = 0
    error_count: int = 0
    bars_upserted: int = 0
    error_details: list[str] = field(default_factory=list)


class PriceCollector:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        # Keep behavior consistent with existing collectors in enterprise proxy environments.
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

    def collect_market(
        self,
        *,
        market: str,
        stock_codes: list[str] | None = None,
        lookback_days: int = 400,
    ) -> PriceCollectStats:
        market_norm = market.strip().upper()
        code_set = {x.strip().upper() for x in (stock_codes or []) if x.strip()}
        with connect(self.settings.db_path) as conn:
            init_db(conn)
            rows = list_stocks_by_market(conn, market_norm, active_only=True)
            if code_set:
                rows = [r for r in rows if str(r["code"]).upper() in code_set]
            stats = PriceCollectStats(market=market_norm, stock_count=len(rows))
            for row in rows:
                stock = Stock(
                    code=str(row["code"]),
                    name=str(row["name"]),
                    queries=[],
                    market=str(row["market"]),
                    exchange=str(row["exchange"]),
                    currency=str(row["currency"]),
                    is_active=bool(row["is_active"]),
                    universe_source=str(row["universe_source"]),
                    rank=row["rank"],
                )
                try:
                    bars = self.collect_stock_bars(stock, lookback_days=lookback_days)
                    stats.bars_upserted += upsert_price_bars(conn, bars, commit=False)
                    stats.success_count += 1
                except Exception as exc:  # noqa: PERF203
                    stats.error_count += 1
                    detail = f"price market={market_norm} stock={stock.code} error={exc}"
                    stats.error_details.append(detail)
                    print(f"[WARN] {detail}")
            conn.commit()
            return stats

    def collect_stock_bars(self, stock: Stock, *, lookback_days: int) -> list[PriceBar]:
        symbol = self._to_yahoo_symbol(stock)
        response = self.session.get(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={
                "interval": "1d",
                "range": f"{max(5, int(lookback_days))}d",
                "events": "history",
                "includePrePost": "false",
            },
            timeout=self.settings.request_timeout_sec,
            verify=self.verify,
        )
        response.raise_for_status()
        data = response.json()
        chart = data.get("chart") or {}
        errors = chart.get("error")
        if errors:
            raise ValueError(f"yahoo chart error: {errors}")
        results = chart.get("result") or []
        if not results:
            raise ValueError(f"yahoo chart empty result: symbol={symbol}")
        return _parse_chart_result(stock.code, results[0])

    @staticmethod
    def _to_yahoo_symbol(stock: Stock) -> str:
        if stock.market.upper() == "US":
            return stock.code.strip().upper().replace(".", "-")
        # KRX code mapping for Yahoo: 005930.KS (KOSPI), 035420.KQ (KOSDAQ).
        code = stock.code.strip()
        if "." in code:
            return code.upper()
        exchange = stock.exchange.strip().upper()
        suffix = ".KS"
        if exchange in {"KOSDAQ", "KQ"}:
            suffix = ".KQ"
        return f"{code}{suffix}"


def _parse_chart_result(stock_code: str, result: dict) -> list[PriceBar]:
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quote = (indicators.get("quote") or [{}])[0] or {}
    adjclose_obj = (indicators.get("adjclose") or [{}])[0] or {}
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    adjcloses = adjclose_obj.get("adjclose") or []
    tz_name = str((result.get("meta") or {}).get("exchangeTimezoneName") or "").strip()

    bars: list[PriceBar] = []
    seen_dates: set[str] = set()
    for idx, ts in enumerate(timestamps):
        ts_int = _to_int(ts)
        if ts_int is None:
            continue
        trade_dt = _trade_datetime(ts_int, tz_name)
        trade_key = trade_dt.date().isoformat()
        if trade_key in seen_dates:
            continue
        close_v = _to_float(_safe_get(closes, idx))
        if close_v is None:
            continue
        bars.append(
            PriceBar(
                stock_code=stock_code,
                trade_date=trade_dt,
                open=_to_float(_safe_get(opens, idx)),
                high=_to_float(_safe_get(highs, idx)),
                low=_to_float(_safe_get(lows, idx)),
                close=close_v,
                adj_close=_to_float(_safe_get(adjcloses, idx)),
                volume=_to_int(_safe_get(volumes, idx)),
                source="yahoo_chart_1d",
            )
        )
        seen_dates.add(trade_key)
    if not bars:
        raise ValueError(f"parsed zero bars: stock={stock_code}")
    return bars


def _trade_datetime(ts: int, tz_name: str) -> datetime:
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    if ZoneInfo and tz_name:
        try:
            return dt_utc.astimezone(ZoneInfo(tz_name))
        except Exception:
            return dt_utc
    return dt_utc


def _safe_get(values, idx: int):
    if not isinstance(values, list):
        return None
    if idx < 0 or idx >= len(values):
        return None
    return values[idx]


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
