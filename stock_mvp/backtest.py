from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from stock_mvp.config import Settings
from stock_mvp.database import connect, get_stock, init_db, price_bars_in_range, upsert_price_bars, upsert_stocks
from stock_mvp.models import Stock
from stock_mvp.prices import PriceCollector


@dataclass(frozen=True)
class BacktestAsset:
    code: str
    weight: float


@dataclass(frozen=True)
class BacktestTrade:
    trade_date: str
    stock_code: str
    side: str
    quantity: float
    price: float
    gross: float
    cost: float


@dataclass(frozen=True)
class BacktestDailyPoint:
    trade_date: str
    equity: float
    daily_return: float
    drawdown: float


@dataclass(frozen=True)
class BacktestSeriesPoint:
    trade_date: str
    equity: float
    index: float


@dataclass(frozen=True)
class BacktestSummary:
    start_date: str
    end_date: str
    effective_start_date: str
    effective_end_date: str
    strategy: str
    rebalance: str
    initial_capital: float
    contribution_amount: float
    contribution_frequency: str
    contribution_count: int
    contributed_capital: float
    net_invested_capital: float
    final_equity: float
    cumulative_return: float
    cagr: float | None
    mdd: float
    volatility: float | None
    sharpe: float | None
    turnover_ratio: float
    rebalance_count: int
    trade_count: int
    benchmark_code: str | None
    benchmark_cumulative_return: float | None
    benchmark_cagr: float | None


@dataclass
class BacktestResult:
    summary: BacktestSummary
    daily: list[BacktestDailyPoint] = field(default_factory=list)
    trades: list[BacktestTrade] = field(default_factory=list)
    portfolio_series: list[BacktestSeriesPoint] = field(default_factory=list)
    benchmark_series: list[BacktestSeriesPoint] = field(default_factory=list)


class BacktestEngine:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.price_collector = PriceCollector(settings)

    def run(
        self,
        *,
        market: str,
        assets: list[BacktestAsset],
        start_date: str,
        end_date: str,
        strategy: str = "buy_and_hold",
        rebalance: str = "monthly",
        initial_capital: float = 10_000_000.0,
        fee_bps: float = 0.0,
        slippage_bps: float = 0.0,
        risk_free_rate: float = 0.03,
        benchmark_code: str | None = None,
        contribution_amount: float = 0.0,
        contribution_frequency: str = "none",
    ) -> BacktestResult:
        market_norm = market.strip().upper()
        strategy_norm = _normalize_strategy(strategy)
        rebalance_norm = _normalize_rebalance(rebalance)
        contribution_freq = _normalize_rebalance(contribution_frequency)
        if strategy_norm == "buy_and_hold":
            rebalance_norm = "none"
        if initial_capital <= 0:
            raise ValueError("initial_capital must be > 0")
        if contribution_amount < 0:
            raise ValueError("contribution_amount must be >= 0")
        if not assets:
            raise ValueError("assets must not be empty")
        weights = _normalize_weights(assets)
        start_d = _parse_date(start_date)
        end_d = _parse_date(end_date)
        if end_d < start_d:
            raise ValueError("end_date must be >= start_date")

        with connect(self.settings.db_path) as conn:
            init_db(conn)
            self._ensure_stock_rows(conn, market_norm, [a.code for a in weights], benchmark_code)
            price_map = self._load_prices(
                conn=conn,
                market=market_norm,
                codes=[a.code for a in weights],
                start_d=start_d,
                end_d=end_d,
            )
            benchmark_prices: dict[date, float] | None = None
            if benchmark_code:
                benchmark_prices = self._load_prices(
                    conn=conn,
                    market=market_norm,
                    codes=[benchmark_code],
                    start_d=start_d,
                    end_d=end_d,
                )[benchmark_code]

        common_dates = _common_trade_dates(price_map)
        if len(common_dates) < 2:
            raise ValueError("not enough common trade dates across assets")
        prices_by_code = {
            code: {d: p for d, p in series.items() if d in common_dates}
            for code, series in price_map.items()
        }
        start_eff = common_dates[0]
        end_eff = common_dates[-1]

        cost_rate = max(0.0, (fee_bps + slippage_bps) / 10_000.0)
        holdings = {a.code: 0.0 for a in weights}
        cash = float(initial_capital)
        trades: list[BacktestTrade] = []
        total_traded = 0.0
        rebalance_count = 0
        contribution_count = 0
        contributed_capital = 0.0

        # Initial allocation on first available trading day.
        first_date = common_dates[0]
        for asset in weights:
            px = prices_by_code[asset.code][first_date]
            target = initial_capital * asset.weight
            qty = target / (px * (1.0 + cost_rate))
            gross = qty * px
            cost = gross * cost_rate
            cash -= gross + cost
            holdings[asset.code] += qty
            total_traded += abs(gross)
            trades.append(
                BacktestTrade(
                    trade_date=first_date.isoformat(),
                    stock_code=asset.code,
                    side="buy",
                    quantity=qty,
                    price=px,
                    gross=gross,
                    cost=cost,
                )
            )

        daily: list[BacktestDailyPoint] = []
        peak = -math.inf
        prev_equity = 0.0
        prev_date = first_date

        for idx, cur_date in enumerate(common_dates):
            flow_today = 0.0
            if idx > 0 and contribution_amount > 0 and _should_rebalance(prev_date, cur_date, contribution_freq):
                contribution_count += 1
                contributed_capital += contribution_amount
                cash += contribution_amount
                flow_today += contribution_amount
                for asset in weights:
                    px = prices_by_code[asset.code][cur_date]
                    alloc = contribution_amount * asset.weight
                    qty = alloc / (px * (1.0 + cost_rate))
                    if qty <= 0:
                        continue
                    gross = qty * px
                    cost = gross * cost_rate
                    spend = gross + cost
                    if spend > cash:
                        qty = cash / (px * (1.0 + cost_rate))
                        if qty <= 0:
                            continue
                        gross = qty * px
                        cost = gross * cost_rate
                        spend = gross + cost
                    cash -= spend
                    holdings[asset.code] += qty
                    total_traded += abs(gross)
                    trades.append(
                        BacktestTrade(
                            trade_date=cur_date.isoformat(),
                            stock_code=asset.code,
                            side="buy",
                            quantity=qty,
                            price=px,
                            gross=gross,
                            cost=cost,
                        )
                    )

            if idx > 0 and _should_rebalance(prev_date, cur_date, rebalance_norm):
                rebalance_count += 1
                equity_before = cash + sum(
                    holdings[a.code] * prices_by_code[a.code][cur_date] for a in weights
                )
                for asset in weights:
                    px = prices_by_code[asset.code][cur_date]
                    target_value = equity_before * asset.weight
                    current_value = holdings[asset.code] * px
                    delta = target_value - current_value
                    if abs(delta) < 1e-8:
                        continue
                    if delta > 0:
                        qty = delta / (px * (1.0 + cost_rate))
                        gross = qty * px
                        cost = gross * cost_rate
                        cash -= gross + cost
                        holdings[asset.code] += qty
                        side = "buy"
                    else:
                        qty = min(holdings[asset.code], abs(delta) / px)
                        if qty <= 0:
                            continue
                        gross = qty * px
                        cost = gross * cost_rate
                        cash += gross - cost
                        holdings[asset.code] -= qty
                        side = "sell"
                    total_traded += abs(gross)
                    trades.append(
                        BacktestTrade(
                            trade_date=cur_date.isoformat(),
                            stock_code=asset.code,
                            side=side,
                            quantity=qty,
                            price=px,
                            gross=gross,
                            cost=cost,
                        )
                    )

            equity = cash + sum(holdings[a.code] * prices_by_code[a.code][cur_date] for a in weights)
            if peak < equity:
                peak = equity
            drawdown = 0.0 if peak <= 0 else (equity / peak) - 1.0
            if idx == 0 or prev_equity <= 0:
                daily_ret = 0.0
            else:
                daily_ret = ((equity - flow_today) / prev_equity) - 1.0
            daily.append(
                BacktestDailyPoint(
                    trade_date=cur_date.isoformat(),
                    equity=equity,
                    daily_return=daily_ret,
                    drawdown=drawdown,
                )
            )
            prev_equity = equity
            prev_date = cur_date

        benchmark_cum = None
        benchmark_cagr = None
        benchmark_series: list[BacktestSeriesPoint] = []
        if benchmark_code and benchmark_prices:
            benchmark_series = _build_benchmark_series(
                prices=benchmark_prices,
                reference_dates=[_parse_date(d.trade_date) for d in daily],
                base_equity=initial_capital,
            )
            bench = _benchmark_metrics(
                series=benchmark_series,
            )
            benchmark_cum = bench["cumulative_return"]
            benchmark_cagr = bench["cagr"]

        metrics = _compute_metrics(
            daily=daily,
            risk_free_rate=risk_free_rate,
        )
        portfolio_series = _build_portfolio_series(daily)
        net_invested = initial_capital + contributed_capital
        summary = BacktestSummary(
            start_date=start_d.isoformat(),
            end_date=end_d.isoformat(),
            effective_start_date=start_eff.isoformat(),
            effective_end_date=end_eff.isoformat(),
            strategy=strategy_norm,
            rebalance=rebalance_norm,
            initial_capital=initial_capital,
            contribution_amount=contribution_amount,
            contribution_frequency=contribution_freq,
            contribution_count=contribution_count,
            contributed_capital=contributed_capital,
            net_invested_capital=net_invested,
            final_equity=daily[-1].equity,
            cumulative_return=metrics["cumulative_return"],
            cagr=metrics["cagr"],
            mdd=metrics["mdd"],
            volatility=metrics["volatility"],
            sharpe=metrics["sharpe"],
            turnover_ratio=(total_traded / initial_capital) if initial_capital > 0 else 0.0,
            rebalance_count=rebalance_count,
            trade_count=len(trades),
            benchmark_code=benchmark_code,
            benchmark_cumulative_return=benchmark_cum,
            benchmark_cagr=benchmark_cagr,
        )
        return BacktestResult(
            summary=summary,
            daily=daily,
            trades=trades,
            portfolio_series=portfolio_series,
            benchmark_series=benchmark_series,
        )

    def _ensure_stock_rows(
        self,
        conn,
        market: str,
        codes: list[str],
        benchmark_code: str | None,
    ) -> None:
        all_codes = [c.strip().upper() for c in codes if c.strip()]
        if benchmark_code and benchmark_code.strip():
            all_codes.append(benchmark_code.strip().upper())
        to_insert: list[Stock] = []
        for code in all_codes:
            if get_stock(conn, code) is not None:
                continue
            exchange = "KRX" if market == "KR" else "NASDAQ"
            currency = "KRW" if market == "KR" else "USD"
            to_insert.append(
                Stock(
                    code=code,
                    name=code,
                    queries=[code],
                    market=market,
                    exchange=exchange,
                    currency=currency,
                    is_active=True,
                    universe_source="backtest_custom",
                    rank=None,
                )
            )
        if to_insert:
            upsert_stocks(conn, to_insert)

    def _load_prices(
        self,
        *,
        conn,
        market: str,
        codes: list[str],
        start_d: date,
        end_d: date,
    ) -> dict[str, dict[date, float]]:
        today = datetime.now(tz=timezone.utc).date()
        lookback_days = max(30, (today - start_d).days + 30)
        series: dict[str, dict[date, float]] = {}
        for code in codes:
            rows = price_bars_in_range(conn, code, start_d.isoformat(), end_d.isoformat())
            if not rows:
                stock_row = get_stock(conn, code)
                if stock_row is None:
                    raise ValueError(f"stock not found: {code}")
                stock = Stock(
                    code=str(stock_row["code"]),
                    name=str(stock_row["name"]),
                    queries=[],
                    market=str(stock_row["market"]),
                    exchange=str(stock_row["exchange"]),
                    currency=str(stock_row["currency"]),
                    is_active=bool(stock_row["is_active"]),
                    universe_source=str(stock_row["universe_source"]),
                    rank=stock_row["rank"],
                )
                bars = self.price_collector.collect_stock_bars(stock, lookback_days=lookback_days)
                upsert_price_bars(conn, bars, commit=False)
                rows = price_bars_in_range(conn, code, start_d.isoformat(), end_d.isoformat())
            if not rows:
                raise ValueError(f"no price bars for code={code} in range {start_d}..{end_d}")
            parsed: dict[date, float] = {}
            for r in rows:
                d = _parse_date(str(r["trade_date"]))
                px = _to_float(r["adj_close"])
                if px is None:
                    px = _to_float(r["close"])
                if px is None or px <= 0:
                    continue
                parsed[d] = px
            if not parsed:
                raise ValueError(f"invalid price bars for code={code}")
            series[code] = parsed
        conn.commit()
        return series


def _normalize_strategy(value: str) -> str:
    v = (value or "").strip().lower()
    if v in {"buy_and_hold", "buy-hold", "buyhold", "hold"}:
        return "buy_and_hold"
    if v in {"monthly_rebalance", "rebalance_monthly", "monthly"}:
        return "monthly_rebalance"
    raise ValueError("strategy must be buy_and_hold or monthly_rebalance")


def _normalize_rebalance(value: str) -> str:
    v = (value or "").strip().lower()
    if v in {"none", "no"}:
        return "none"
    if v in {"monthly", "m"}:
        return "monthly"
    if v in {"quarterly", "q"}:
        return "quarterly"
    if v in {"yearly", "annual", "y"}:
        return "yearly"
    raise ValueError("rebalance must be none|monthly|quarterly|yearly")


def _normalize_weights(assets: list[BacktestAsset]) -> list[BacktestAsset]:
    cleaned: list[BacktestAsset] = []
    total = 0.0
    for a in assets:
        code = a.code.strip().upper()
        if not code:
            continue
        w = float(a.weight)
        if w <= 0:
            continue
        cleaned.append(BacktestAsset(code=code, weight=w))
        total += w
    if not cleaned:
        raise ValueError("no valid assets")
    # If user passes percentage style (e.g., 60,40), normalize to 1.0.
    return [BacktestAsset(code=a.code, weight=a.weight / total) for a in cleaned]


def _parse_date(value: str) -> date:
    text = (value or "").strip()
    parsed = datetime.fromisoformat(text)
    return parsed.date()


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _common_trade_dates(price_map: dict[str, dict[date, float]]) -> list[date]:
    common: set[date] | None = None
    for series in price_map.values():
        dates = set(series.keys())
        if common is None:
            common = dates
        else:
            common &= dates
    if not common:
        return []
    return sorted(common)


def _should_rebalance(prev_d: date, cur_d: date, freq: str) -> bool:
    if freq == "none":
        return False
    if freq == "monthly":
        return (prev_d.year, prev_d.month) != (cur_d.year, cur_d.month)
    if freq == "quarterly":
        prev_q = (prev_d.month - 1) // 3
        cur_q = (cur_d.month - 1) // 3
        return (prev_d.year, prev_q) != (cur_d.year, cur_q)
    if freq == "yearly":
        return prev_d.year != cur_d.year
    return False


def _compute_metrics(
    *,
    daily: list[BacktestDailyPoint],
    risk_free_rate: float,
) -> dict[str, float | None]:
    if not daily:
        return {
            "cumulative_return": 0.0,
            "cagr": None,
            "mdd": 0.0,
            "volatility": None,
            "sharpe": None,
        }
    mdd = min((d.drawdown for d in daily), default=0.0)
    returns = [d.daily_return for d in daily[1:]]
    if returns:
        compounded = 1.0
        for r in returns:
            compounded *= 1.0 + r
        cumulative = compounded - 1.0
    else:
        compounded = 1.0
        cumulative = 0.0
    if len(daily) >= 2:
        years = (len(daily) - 1) / 252.0
        cagr = (compounded ** (1.0 / years) - 1.0) if years > 0 else None
    else:
        cagr = None
    if len(returns) >= 2:
        stdev_daily = statistics.stdev(returns)
        vol = stdev_daily * math.sqrt(252.0)
        avg_daily = statistics.mean(returns)
        rf_daily = risk_free_rate / 252.0
        sharpe = ((avg_daily - rf_daily) / stdev_daily) * math.sqrt(252.0) if stdev_daily > 0 else None
    else:
        vol = None
        sharpe = None
    return {
        "cumulative_return": cumulative,
        "cagr": cagr,
        "mdd": mdd,
        "volatility": vol,
        "sharpe": sharpe,
    }


def _build_portfolio_series(daily: list[BacktestDailyPoint]) -> list[BacktestSeriesPoint]:
    if not daily:
        return []
    base = daily[0].equity if daily[0].equity > 0 else 1.0
    series: list[BacktestSeriesPoint] = []
    for d in daily:
        idx = (d.equity / base) * 100.0
        series.append(BacktestSeriesPoint(trade_date=d.trade_date, equity=d.equity, index=idx))
    return series


def _build_benchmark_series(
    *,
    prices: dict[date, float],
    reference_dates: list[date],
    base_equity: float,
) -> list[BacktestSeriesPoint]:
    dates = [d for d in reference_dates if d in prices]
    if len(dates) < 2:
        return []
    first_px = prices[dates[0]]
    if first_px <= 0:
        return []
    series: list[BacktestSeriesPoint] = []
    for d in dates:
        px = prices[d]
        index = (px / first_px) * 100.0
        equity = base_equity * (px / first_px)
        series.append(BacktestSeriesPoint(trade_date=d.isoformat(), equity=equity, index=index))
    return series


def _benchmark_metrics(*, series: list[BacktestSeriesPoint]) -> dict[str, float | None]:
    if len(series) < 2:
        return {"cumulative_return": None, "cagr": None}
    first = series[0].index
    last = series[-1].index
    if first <= 0:
        return {"cumulative_return": None, "cagr": None}
    cum = (last / first) - 1.0
    years = (len(series) - 1) / 252.0
    cagr = ((last / first) ** (1.0 / years) - 1.0) if years > 0 else None
    return {"cumulative_return": cum, "cagr": cagr}
