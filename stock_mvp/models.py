from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Stock:
    code: str
    name: str
    queries: list[str]
    market: str = "KR"
    exchange: str = "KRX"
    currency: str = "KRW"
    is_active: bool = True
    universe_source: str = "manual"
    rank: int | None = None


@dataclass(frozen=True)
class CollectedDocument:
    stock_code: str
    source: str
    doc_type: str
    title: str
    url: str
    published_at: datetime | None
    body: str
    relevance_score: float = 0.0
    relevance_reason: str = ""
    matched_alias: str = ""


@dataclass(frozen=True)
class SectorCollectedDocument:
    sector_name: str
    source: str
    doc_type: str
    title: str
    url: str
    published_at: datetime | None
    body: str


@dataclass(frozen=True)
class SummaryLine:
    text: str
    source_doc_ids: list[int]


@dataclass(frozen=True)
class GeneratedSummary:
    stock_code: str
    as_of: datetime
    lines: list[SummaryLine]
    model: str


@dataclass(frozen=True)
class Sector:
    sector_code: str
    sector_name_ko: str
    sector_name_en: str
    taxonomy_version: str = "v1"
    is_active: bool = True


@dataclass(frozen=True)
class StockSectorMap:
    stock_code: str
    sector_code: str
    mapping_source: str
    confidence: float
    as_of: datetime


@dataclass(frozen=True)
class SectorGeneratedSummary:
    sector_code: str
    as_of: datetime
    lines: list[SummaryLine]
    sentiment_label: str
    sentiment_confidence: float
    model: str


@dataclass(frozen=True)
class FinancialSnapshot:
    stock_code: str
    as_of: datetime
    source: str
    per: float | None
    pbr: float | None
    eps: float | None
    roe: float | None
    market_cap: int | None
    currency: str


@dataclass(frozen=True)
class PriceBar:
    stock_code: str
    trade_date: datetime
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    adj_close: float | None
    volume: int | None
    source: str
