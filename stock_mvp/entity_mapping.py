from __future__ import annotations

import re
from dataclasses import dataclass

from stock_mvp.models import Stock
from stock_mvp.relevance import evaluate_stock_document_relevance
from stock_mvp.utils import compact_text


@dataclass(frozen=True)
class EntityMappingResult:
    entity_type: str
    entity_id: str
    score: float
    assigned: bool
    reason: dict[str, object]
    raw_score: float = 0.0
    confidence: str = "low"
    is_primary: bool = True
    extra_mappings: tuple["EntityMappingResult", ...] = ()


_AMBIGUOUS_KEYWORDS = (
    "관련주",
    "테마",
    "수혜주",
    "대장주",
    "etf",
    "index",
    "지수",
)

_DEFAULT_GENERAL_ECONOMY_KEYWORDS = (
    "환율",
    "금리",
    "물가",
    "고용",
    "경기",
    "수출",
    "수입",
    "통화정책",
    "재정정책",
    "한은",
    "fomc",
    "cpi",
    "ppi",
    "gdp",
)
_GENERAL_MACRO_KEYWORDS = {
    "환율",
    "금리",
    "물가",
    "고용",
    "경기",
    "수출",
    "수입",
    "실업률",
    "소비자물가",
    "생산자물가",
}
_GENERAL_POLICY_KEYWORDS = {
    "통화정책",
    "재정정책",
    "한은",
    "기재부",
    "fomc",
    "cpi",
    "ppi",
    "gdp",
    "기준금리",
}
_NAMED_SECTOR_KEYWORDS: dict[str, tuple[str, ...]] = {
    "SEMICONDUCTORS": ("반도체", "hbm", "메모리", "파운드리", "chip", "semiconductor"),
    "AUTO": ("자동차", "전기차", "완성차", "배터리", "모빌리티"),
    "BIO": ("바이오", "제약", "신약", "임상"),
    "ENERGY": ("정유", "가스", "전력", "원유", "에너지"),
    "FINANCE": ("은행", "증권", "보험", "카드", "금융"),
}
_GENERAL_ECONOMY_SOURCE_HINTS = ("economy", "market", "macro", "finance")


def map_document_to_primary_ticker(
    *,
    title: str,
    body: str,
    url: str,
    source: str,
    doc_type: str,
    market_stocks: list[Stock],
    hinted_stock_code: str,
    min_score: float = 0.55,
    ticker_raw_min_score: float = 8.0,
    named_sector_min_score: float = 7.0,
    general_economy_min_score: float = 7.0,
    general_economy_keywords: str = "",
) -> EntityMappingResult:
    title_text = compact_text(title)
    body_text = compact_text(body)
    url_text = compact_text(url)
    hint = compact_text(hinted_stock_code).upper()

    candidates: list[tuple[str, float, str, bool]] = []
    for stock in market_stocks:
        rel = evaluate_stock_document_relevance(
            stock,
            title=title_text,
            body=body_text,
            url=url_text,
            source=source,
            doc_type=doc_type,
        )
        score = float(rel.score)
        if score <= 0:
            continue
        if _has_ambiguous_context(title_text, body_text):
            score *= 0.92
        if hint and stock.code.upper() == hint:
            score += 0.02
        candidates.append((stock.code.upper(), max(0.0, min(score, 1.0)), rel.reason, stock.code.upper() == hint))

    candidates.sort(key=lambda x: (x[1], 1 if x[3] else 0), reverse=True)
    top = candidates[0] if candidates else None
    second = candidates[1] if len(candidates) > 1 else None

    top_code = ""
    top_score = 0.0
    top_reason = "no_candidate"
    if top is not None:
        top_code, top_score, top_reason, _is_hint = top
        ambiguity_gap = top_score - (second[1] if second is not None else 0.0)
        if second is not None and ambiguity_gap < 0.08:
            top_score = max(0.0, top_score - 0.08)
    else:
        ambiguity_gap = 0.0

    ticker_raw_score = round(top_score * 10.0, 2)
    ticker_gate = max(float(min_score), float(ticker_raw_min_score) / 10.0)
    assigned = bool(top_code and top_score >= ticker_gate)
    entity_id = top_code if assigned else "UNASSIGNED"

    best_named_sector_score, named_sector_keywords = _score_named_sector_context(title_text, body_text)
    general_score, general_hits = _score_general_economy(
        title=title_text,
        body=body_text,
        url=url_text,
        source=source,
        extra_keywords=general_economy_keywords,
    )

    use_general_economy_fallback = (
        not assigned
        and best_named_sector_score < float(named_sector_min_score)
        and general_score >= float(general_economy_min_score)
    )

    ticker_result = EntityMappingResult(
        entity_type="ticker",
        entity_id=entity_id,
        score=round(max(0.0, min(top_score, 1.0)), 4),
        assigned=assigned,
        raw_score=ticker_raw_score,
        confidence=_confidence_label(ticker_raw_score),
        is_primary=not use_general_economy_fallback and assigned,
        reason={
            "status": (
                "assigned"
                if assigned
                else "general_economy_fallback"
                if use_general_economy_fallback
                else "unassigned"
            ),
            "hinted_stock_code": hint,
            "top_reason": top_reason,
            "ambiguity_gap": round(ambiguity_gap, 4),
            "ticker_raw_score": ticker_raw_score,
            "best_named_sector_score": round(best_named_sector_score, 2),
            "named_sector_keywords": named_sector_keywords,
            "general_economy_score": round(general_score, 2),
            "general_economy_keywords": general_hits,
            "top_candidates": [
                {"entity_id": code, "score": round(score, 4), "reason": reason}
                for code, score, reason, _ in candidates[:5]
            ],
        },
    )

    if not use_general_economy_fallback:
        return ticker_result

    sector_result = EntityMappingResult(
        entity_type="sector",
        entity_id="GENERAL_ECONOMY",
        score=round(min(1.0, general_score / 10.0), 4),
        assigned=True,
        raw_score=round(general_score, 2),
        confidence=_confidence_label(general_score),
        is_primary=True,
        reason={
            "status": "general_economy_assigned",
            "general_economy_score": round(general_score, 2),
            "general_economy_keywords": general_hits,
            "best_named_sector_score": round(best_named_sector_score, 2),
            "named_sector_keywords": named_sector_keywords,
        },
    )
    return EntityMappingResult(
        entity_type=ticker_result.entity_type,
        entity_id=ticker_result.entity_id,
        score=ticker_result.score,
        assigned=ticker_result.assigned,
        reason=ticker_result.reason,
        raw_score=ticker_result.raw_score,
        confidence=ticker_result.confidence,
        is_primary=False,
        extra_mappings=(sector_result,),
    )


def _has_ambiguous_context(title: str, body: str) -> bool:
    merged = f"{title} {body}".lower()
    return any(keyword in merged for keyword in _AMBIGUOUS_KEYWORDS)


def _score_named_sector_context(title: str, body: str) -> tuple[float, list[str]]:
    title_text = compact_text(title).lower()
    body_text = compact_text(body).lower()
    best_score = 0.0
    best_hits: list[str] = []
    for _sector_code, keywords in _NAMED_SECTOR_KEYWORDS.items():
        title_hits = [k for k in keywords if k in title_text]
        body_hits = [k for k in keywords if k in body_text]
        if not title_hits and not body_hits:
            continue
        unique_hits = list(dict.fromkeys([*title_hits, *body_hits]))
        score = min(10.0, 4.0 + len(title_hits) * 2.0 + max(0, len(unique_hits) - len(title_hits)))
        if score > best_score:
            best_score = score
            best_hits = unique_hits
    return best_score, best_hits[:8]


def _score_general_economy(
    *,
    title: str,
    body: str,
    url: str,
    source: str,
    extra_keywords: str,
) -> tuple[float, list[str]]:
    merged = compact_text(f"{title} {body}").lower()
    source_blob = f"{source} {url}".lower()
    configured = [compact_text(x).lower() for x in re.split(r"[,\n;]+", str(extra_keywords or "")) if compact_text(x)]
    base_terms = list(dict.fromkeys([*_DEFAULT_GENERAL_ECONOMY_KEYWORDS, *configured]))

    macro_hits = sorted({k for k in [*_GENERAL_MACRO_KEYWORDS, *base_terms] if k and k in merged})
    policy_hits = sorted({k for k in [*_GENERAL_POLICY_KEYWORDS, *base_terms] if k and k in merged})
    category_bonus = 1 if any(token in source_blob for token in _GENERAL_ECONOMY_SOURCE_HINTS) else 0
    score = min(10.0, 4.0 + float(len(macro_hits)) + float(len(policy_hits)) + float(category_bonus))
    return score, [*macro_hits[:6], *[k for k in policy_hits if k not in macro_hits][:6]]


def _confidence_label(raw_score: float) -> str:
    value = float(raw_score)
    if value >= 8.0:
        return "high"
    if value >= 7.0:
        return "medium"
    return "low"
