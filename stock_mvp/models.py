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
