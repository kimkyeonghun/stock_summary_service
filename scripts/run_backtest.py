from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.backtest import BacktestAsset, BacktestEngine
from stock_mvp.backtest_presets import get_portfolio_preset, list_portfolio_presets
from stock_mvp.config import load_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Run portfolio backtest.")
    parser.add_argument("--market", type=str.upper, default="", choices=["KR", "US"], help="KR or US")
    parser.add_argument(
        "--preset",
        type=str,
        default="",
        help="Preset key (e.g., all_weather, sixty_forty, three_fund, permanent, golden_butterfly)",
    )
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="Print available preset portfolios and exit",
    )
    parser.add_argument(
        "--weights",
        type=str,
        default="",
        help='Portfolio weights, e.g. "SPY:60,QQQ:40" or "005930:0.6,000660:0.4"',
    )
    parser.add_argument("--start-date", type=str, default="", help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default="", help="YYYY-MM-DD")
    parser.add_argument(
        "--strategy",
        type=str,
        default="buy_and_hold",
        choices=["buy_and_hold", "monthly_rebalance"],
        help="Backtest strategy",
    )
    parser.add_argument(
        "--rebalance",
        type=str,
        default="monthly",
        choices=["none", "monthly", "quarterly", "yearly"],
        help="Rebalance frequency (used by monthly_rebalance)",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=0.0,
        help="Initial capital (0 means auto: KR=10,000,000 / US=10,000)",
    )
    parser.add_argument(
        "--contribution-amount",
        type=float,
        default=0.0,
        help="Periodic contribution amount (0 to disable)",
    )
    parser.add_argument(
        "--contribution-frequency",
        type=str,
        default="none",
        choices=["none", "monthly", "quarterly", "yearly"],
        help="Periodic contribution frequency",
    )
    parser.add_argument("--fee-bps", type=float, default=0.0, help="Trading fee in bps")
    parser.add_argument("--slippage-bps", type=float, default=0.0, help="Slippage in bps")
    parser.add_argument("--risk-free-rate", type=float, default=0.03, help="Annual risk-free rate (0.03=3%)")
    parser.add_argument("--benchmark", type=str, default="", help="Optional benchmark ticker/code")
    args = parser.parse_args()

    if args.list_presets:
        print_presets()
        return
    if not args.start_date.strip() or not args.end_date.strip():
        raise ValueError("--start-date and --end-date are required")

    preset = get_portfolio_preset(args.preset) if args.preset.strip() else None
    if args.preset.strip() and preset is None:
        raise ValueError(f"unknown preset: {args.preset}")
    if preset and args.weights.strip():
        raise ValueError("use either --preset or --weights, not both")

    if preset:
        assets = list(preset.assets)
        market = args.market or preset.market
        benchmark_code = args.benchmark.strip().upper() or (preset.benchmark_code or "")
    else:
        if not args.weights.strip():
            raise ValueError("either --preset or --weights is required")
        if not args.market:
            raise ValueError("--market is required when using --weights")
        assets = parse_weights(args.weights)
        market = args.market
        benchmark_code = args.benchmark.strip().upper()

    settings = load_settings()
    engine = BacktestEngine(settings)
    initial_capital = args.initial_capital
    if initial_capital <= 0:
        initial_capital = 10_000.0 if market == "US" else 10_000_000.0
    result = engine.run(
        market=market,
        assets=assets,
        start_date=args.start_date,
        end_date=args.end_date,
        strategy=args.strategy,
        rebalance=args.rebalance,
        initial_capital=initial_capital,
        contribution_amount=args.contribution_amount,
        contribution_frequency=args.contribution_frequency,
        fee_bps=args.fee_bps,
        slippage_bps=args.slippage_bps,
        risk_free_rate=args.risk_free_rate,
        benchmark_code=benchmark_code or None,
    )

    s = result.summary
    print("Backtest done")
    if preset:
        print(f"preset={preset.key} ({preset.name})")
    print(f"market={market}")
    print(f"strategy={s.strategy}")
    print(f"rebalance={s.rebalance}")
    print(f"period={s.start_date}..{s.end_date}")
    print(f"effective_period={s.effective_start_date}..{s.effective_end_date}")
    print(f"initial_capital={s.initial_capital:,.2f}")
    print(f"contribution_amount={s.contribution_amount:,.2f}")
    print(f"contribution_frequency={s.contribution_frequency}")
    print(f"contribution_count={s.contribution_count}")
    print(f"contributed_capital={s.contributed_capital:,.2f}")
    print(f"net_invested_capital={s.net_invested_capital:,.2f}")
    print(f"final_equity={s.final_equity:,.2f}")
    print(f"cumulative_return={fmt_pct(s.cumulative_return)}")
    print(f"cagr={fmt_pct(s.cagr)}")
    print(f"mdd={fmt_pct(s.mdd)}")
    print(f"volatility={fmt_pct(s.volatility)}")
    print(f"sharpe={fmt_num(s.sharpe)}")
    print(f"turnover_ratio={fmt_pct(s.turnover_ratio)}")
    print(f"rebalance_count={s.rebalance_count}")
    print(f"trade_count={s.trade_count}")
    if s.benchmark_code:
        print(f"benchmark_code={s.benchmark_code}")
        print(f"benchmark_cumulative_return={fmt_pct(s.benchmark_cumulative_return)}")
        print(f"benchmark_cagr={fmt_pct(s.benchmark_cagr)}")


def parse_weights(value: str) -> list[BacktestAsset]:
    assets: list[BacktestAsset] = []
    for part in re.split(r"[,\s;]+", value.strip()):
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"invalid weight token: {part}")
        code_raw, weight_raw = part.split(":", 1)
        code = code_raw.strip().upper()
        if not code:
            continue
        weight = float(weight_raw.strip())
        assets.append(BacktestAsset(code=code, weight=weight))
    if not assets:
        raise ValueError("weights are empty")
    return assets


def print_presets() -> None:
    print("Available presets")
    for preset in list_portfolio_presets():
        weights = ", ".join([f"{a.code}:{a.weight:g}" for a in preset.assets])
        bench = preset.benchmark_code or "-"
        print(f"- key={preset.key}")
        print(f"  name={preset.name}")
        print(f"  market={preset.market}")
        print(f"  benchmark={bench}")
        print(f"  weights={weights}")
        print(f"  note={preset.description}")


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.2f}%"


def fmt_num(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.4f}"


if __name__ == "__main__":
    main()
