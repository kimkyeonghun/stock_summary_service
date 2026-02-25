from __future__ import annotations

from dataclasses import dataclass

from stock_mvp.backtest import BacktestAsset


@dataclass(frozen=True)
class PortfolioPreset:
    key: str
    name: str
    description: str
    market: str
    assets: tuple[BacktestAsset, ...]
    benchmark_code: str | None = None


_PRESETS: tuple[PortfolioPreset, ...] = (
    PortfolioPreset(
        key="all_weather",
        name="All Weather",
        description="Ray Dalio style diversified allocation for multiple macro regimes.",
        market="US",
        assets=(
            BacktestAsset(code="SPY", weight=30),
            BacktestAsset(code="IEF", weight=15),
            BacktestAsset(code="TLT", weight=40),
            BacktestAsset(code="GLD", weight=7.5),
            BacktestAsset(code="DBC", weight=7.5),
        ),
        benchmark_code="SPY",
    ),
    PortfolioPreset(
        key="sixty_forty",
        name="60/40 Classic",
        description="Most common stock-bond balanced portfolio.",
        market="US",
        assets=(
            BacktestAsset(code="SPY", weight=60),
            BacktestAsset(code="AGG", weight=40),
        ),
        benchmark_code="SPY",
    ),
    PortfolioPreset(
        key="three_fund",
        name="Three-Fund",
        description="US total market + international + bonds.",
        market="US",
        assets=(
            BacktestAsset(code="VTI", weight=50),
            BacktestAsset(code="VXUS", weight=30),
            BacktestAsset(code="BND", weight=20),
        ),
        benchmark_code="VTI",
    ),
    PortfolioPreset(
        key="permanent",
        name="Permanent Portfolio",
        description="Equal-weight stock, long bond, gold, and cash-like bonds.",
        market="US",
        assets=(
            BacktestAsset(code="SPY", weight=25),
            BacktestAsset(code="TLT", weight=25),
            BacktestAsset(code="GLD", weight=25),
            BacktestAsset(code="SHY", weight=25),
        ),
        benchmark_code="SPY",
    ),
    PortfolioPreset(
        key="golden_butterfly",
        name="Golden Butterfly",
        description="Balanced growth/defense mix with gold and long bonds.",
        market="US",
        assets=(
            BacktestAsset(code="SPY", weight=20),
            BacktestAsset(code="VBR", weight=20),
            BacktestAsset(code="TLT", weight=20),
            BacktestAsset(code="SHY", weight=20),
            BacktestAsset(code="GLD", weight=20),
        ),
        benchmark_code="SPY",
    ),
)


def list_portfolio_presets() -> list[PortfolioPreset]:
    return list(_PRESETS)


def get_portfolio_preset(key: str) -> PortfolioPreset | None:
    normalized = (key or "").strip().lower()
    if not normalized:
        return None
    for p in _PRESETS:
        if p.key == normalized:
            return p
    return None
