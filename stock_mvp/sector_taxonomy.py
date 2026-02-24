from __future__ import annotations

import re
from datetime import datetime

from stock_mvp.models import Sector, Stock, StockSectorMap
from stock_mvp.utils import compact_text, now_utc


TAXONOMY_VERSION = "v1"

SECTOR_UNCLASSIFIED = "UNCLASSIFIED"
SECTOR_INFORMATION_TECHNOLOGY = "INFORMATION_TECHNOLOGY"
SECTOR_SEMICONDUCTORS = "SEMICONDUCTORS"
SECTOR_FINANCIALS = "FINANCIALS"
SECTOR_HEALTH_CARE = "HEALTH_CARE"
SECTOR_ENERGY = "ENERGY"
SECTOR_MATERIALS = "MATERIALS"
SECTOR_CONSUMER_DISCRETIONARY = "CONSUMER_DISCRETIONARY"
SECTOR_CONSUMER_STAPLES = "CONSUMER_STAPLES"
SECTOR_INDUSTRIALS = "INDUSTRIALS"
SECTOR_COMMUNICATION_SERVICES = "COMMUNICATION_SERVICES"
SECTOR_PLATFORM_IT = "PLATFORM_IT"
SECTOR_UTILITIES = "UTILITIES"
SECTOR_REAL_ESTATE = "REAL_ESTATE"


DEFAULT_SECTORS: list[Sector] = [
    Sector(SECTOR_INFORMATION_TECHNOLOGY, "Information Technology", "Information Technology", TAXONOMY_VERSION),
    Sector(SECTOR_SEMICONDUCTORS, "Semiconductors", "Semiconductors", TAXONOMY_VERSION),
    Sector(SECTOR_FINANCIALS, "Financials", "Financials", TAXONOMY_VERSION),
    Sector(SECTOR_HEALTH_CARE, "Health Care", "Health Care", TAXONOMY_VERSION),
    Sector(SECTOR_ENERGY, "Energy", "Energy", TAXONOMY_VERSION),
    Sector(SECTOR_MATERIALS, "Materials", "Materials", TAXONOMY_VERSION),
    Sector(
        SECTOR_CONSUMER_DISCRETIONARY,
        "Consumer Discretionary",
        "Consumer Discretionary",
        TAXONOMY_VERSION,
    ),
    Sector(SECTOR_CONSUMER_STAPLES, "Consumer Staples", "Consumer Staples", TAXONOMY_VERSION),
    Sector(SECTOR_INDUSTRIALS, "Industrials", "Industrials", TAXONOMY_VERSION),
    Sector(
        SECTOR_COMMUNICATION_SERVICES,
        "Communication Services",
        "Communication Services",
        TAXONOMY_VERSION,
    ),
    Sector(SECTOR_PLATFORM_IT, "Platform IT", "Platform IT", TAXONOMY_VERSION),
    Sector(SECTOR_UTILITIES, "Utilities", "Utilities", TAXONOMY_VERSION),
    Sector(SECTOR_REAL_ESTATE, "Real Estate", "Real Estate", TAXONOMY_VERSION),
    Sector(SECTOR_UNCLASSIFIED, "Unclassified", "Unclassified", TAXONOMY_VERSION),
]


_KR_OVERRIDES: dict[str, list[tuple[str, float]]] = {
    "005930": [(SECTOR_INFORMATION_TECHNOLOGY, 0.95), (SECTOR_SEMICONDUCTORS, 0.92)],
    "000660": [(SECTOR_SEMICONDUCTORS, 0.96), (SECTOR_INFORMATION_TECHNOLOGY, 0.87)],
    "373220": [(SECTOR_ENERGY, 0.84), (SECTOR_INDUSTRIALS, 0.66)],
    "005380": [(SECTOR_CONSUMER_DISCRETIONARY, 0.81), (SECTOR_INDUSTRIALS, 0.62)],
    "000270": [(SECTOR_CONSUMER_DISCRETIONARY, 0.81), (SECTOR_INDUSTRIALS, 0.62)],
    "035420": [(SECTOR_PLATFORM_IT, 0.88), (SECTOR_COMMUNICATION_SERVICES, 0.54)],
    "035720": [(SECTOR_PLATFORM_IT, 0.88), (SECTOR_COMMUNICATION_SERVICES, 0.54)],
    "068270": [(SECTOR_HEALTH_CARE, 0.9)],
    "207940": [(SECTOR_HEALTH_CARE, 0.93)],
    "005490": [(SECTOR_MATERIALS, 0.9), (SECTOR_INDUSTRIALS, 0.51)],
}

_US_OVERRIDES: dict[str, list[tuple[str, float]]] = {
    "AAPL": [(SECTOR_INFORMATION_TECHNOLOGY, 0.87), (SECTOR_CONSUMER_DISCRETIONARY, 0.52)],
    "MSFT": [(SECTOR_INFORMATION_TECHNOLOGY, 0.93)],
    "NVDA": [(SECTOR_SEMICONDUCTORS, 0.95), (SECTOR_INFORMATION_TECHNOLOGY, 0.86)],
    "AMZN": [(SECTOR_CONSUMER_DISCRETIONARY, 0.82), (SECTOR_PLATFORM_IT, 0.61)],
    "GOOGL": [(SECTOR_COMMUNICATION_SERVICES, 0.86), (SECTOR_PLATFORM_IT, 0.69)],
    "META": [(SECTOR_COMMUNICATION_SERVICES, 0.86), (SECTOR_PLATFORM_IT, 0.7)],
    "TSLA": [(SECTOR_CONSUMER_DISCRETIONARY, 0.82), (SECTOR_INDUSTRIALS, 0.57)],
    "AVGO": [(SECTOR_SEMICONDUCTORS, 0.9), (SECTOR_INFORMATION_TECHNOLOGY, 0.72)],
    "JPM": [(SECTOR_FINANCIALS, 0.95)],
    "LLY": [(SECTOR_HEALTH_CARE, 0.95)],
    "V": [(SECTOR_FINANCIALS, 0.9), (SECTOR_INFORMATION_TECHNOLOGY, 0.45)],
    "XOM": [(SECTOR_ENERGY, 0.95)],
    "UNH": [(SECTOR_HEALTH_CARE, 0.94)],
    "MA": [(SECTOR_FINANCIALS, 0.9), (SECTOR_INFORMATION_TECHNOLOGY, 0.45)],
    "NFLX": [(SECTOR_COMMUNICATION_SERVICES, 0.9), (SECTOR_PLATFORM_IT, 0.62)],
    "JNJ": [(SECTOR_HEALTH_CARE, 0.94)],
}

_KEYWORD_RULES: list[tuple[re.Pattern[str], str, float]] = [
    (
        re.compile(r"(semiconductor|chip|memory|hynix|micron|반도체|메모리)", flags=re.IGNORECASE),
        SECTOR_SEMICONDUCTORS,
        0.8,
    ),
    (
        re.compile(r"(software|it|cloud|tech|전자|소프트웨어|테크)", flags=re.IGNORECASE),
        SECTOR_INFORMATION_TECHNOLOGY,
        0.62,
    ),
    (
        re.compile(r"(platform|internet|portal|search|social|media|platform|네이버|카카오|플랫폼)", flags=re.IGNORECASE),
        SECTOR_PLATFORM_IT,
        0.75,
    ),
    (
        re.compile(r"(bank|financial|insurance|securities|card|capital|은행|금융|증권|보험|카드)", flags=re.IGNORECASE),
        SECTOR_FINANCIALS,
        0.8,
    ),
    (
        re.compile(r"(bio|biologics|pharma|health|hospital|헬스|바이오|제약|의료)", flags=re.IGNORECASE),
        SECTOR_HEALTH_CARE,
        0.81,
    ),
    (
        re.compile(r"(energy|oil|gas|refin|battery|에너지|정유|가스|배터리)", flags=re.IGNORECASE),
        SECTOR_ENERGY,
        0.75,
    ),
    (
        re.compile(r"(steel|chemical|material|mining|metal|철강|화학|소재|금속)", flags=re.IGNORECASE),
        SECTOR_MATERIALS,
        0.75,
    ),
    (
        re.compile(r"(motor|auto|car|vehicle|현대차|기아|자동차)", flags=re.IGNORECASE),
        SECTOR_CONSUMER_DISCRETIONARY,
        0.72,
    ),
    (
        re.compile(r"(retail|consumer|apparel|travel|e-commerce|커머스|유통|소비재)", flags=re.IGNORECASE),
        SECTOR_CONSUMER_DISCRETIONARY,
        0.62,
    ),
    (
        re.compile(r"(food|beverage|grocery|staple|식품|음료|생활용품)", flags=re.IGNORECASE),
        SECTOR_CONSUMER_STAPLES,
        0.74,
    ),
    (
        re.compile(r"(ship|construction|industrial|machinery|조선|건설|기계|산업)", flags=re.IGNORECASE),
        SECTOR_INDUSTRIALS,
        0.66,
    ),
    (
        re.compile(r"(telecom|broadcast|entertainment|통신|방송|엔터)", flags=re.IGNORECASE),
        SECTOR_COMMUNICATION_SERVICES,
        0.68,
    ),
    (
        re.compile(r"(utility|electric|power|water|전력|가스공사|수도)", flags=re.IGNORECASE),
        SECTOR_UTILITIES,
        0.72,
    ),
    (
        re.compile(r"(reit|real estate|property|부동산|리츠)", flags=re.IGNORECASE),
        SECTOR_REAL_ESTATE,
        0.72,
    ),
]


def infer_sector_maps_for_stock(
    stock: Stock,
    *,
    as_of: datetime | None = None,
    min_confidence: float = 0.45,
    max_sectors: int = 3,
) -> list[StockSectorMap]:
    as_of_value = as_of or now_utc()
    scored: dict[str, float] = {}
    source_by_sector: dict[str, str] = {}

    override = _find_override(stock)
    if override:
        for sector_code, score in override:
            scored[sector_code] = max(scored.get(sector_code, 0.0), score)
            source_by_sector[sector_code] = "rule_override_v1"

    search_blob = _search_blob(stock)
    if search_blob:
        for pattern, sector_code, score in _KEYWORD_RULES:
            if pattern.search(search_blob):
                scored[sector_code] = max(scored.get(sector_code, 0.0), score)
                source_by_sector.setdefault(sector_code, "rule_keyword_v1")

    if SECTOR_SEMICONDUCTORS in scored:
        scored[SECTOR_INFORMATION_TECHNOLOGY] = max(scored.get(SECTOR_INFORMATION_TECHNOLOGY, 0.0), 0.6)
        source_by_sector.setdefault(SECTOR_INFORMATION_TECHNOLOGY, "rule_semiconductor_bridge_v1")

    selected: list[tuple[str, float]] = [
        (sector_code, score) for sector_code, score in scored.items() if score >= min_confidence
    ]
    selected.sort(key=lambda x: (-x[1], x[0]))
    selected = selected[: max(1, max_sectors)]

    if not selected:
        selected = [(SECTOR_UNCLASSIFIED, 0.2)]
        source_by_sector[SECTOR_UNCLASSIFIED] = "fallback_unclassified_v1"

    return [
        StockSectorMap(
            stock_code=stock.code,
            sector_code=sector_code,
            mapping_source=source_by_sector.get(sector_code, "rule_keyword_v1"),
            confidence=round(score, 4),
            as_of=as_of_value,
        )
        for sector_code, score in selected
    ]


def _find_override(stock: Stock) -> list[tuple[str, float]]:
    code = compact_text(stock.code).upper()
    if stock.market.upper() == "KR":
        return _KR_OVERRIDES.get(code, [])
    return _US_OVERRIDES.get(code, [])


def _search_blob(stock: Stock) -> str:
    parts = [stock.code, stock.name]
    parts.extend(stock.queries)
    return compact_text(" ".join(parts)).lower()
