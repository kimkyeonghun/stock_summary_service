from __future__ import annotations

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


_AMBIGUOUS_KEYWORDS = (
    "관련주",
    "테마",
    "수혜주",
    "대장주",
    "etf",
    "index",
    "지수",
)


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

    if top is None:
        return EntityMappingResult(
            entity_type="ticker",
            entity_id="UNASSIGNED",
            score=0.0,
            assigned=False,
            reason={"status": "no_candidate", "hinted_stock_code": hint},
        )

    top_code, top_score, top_reason, _is_hint = top
    ambiguity_gap = top_score - (second[1] if second is not None else 0.0)
    if second is not None and ambiguity_gap < 0.08:
        top_score = max(0.0, top_score - 0.08)

    assigned = bool(top_score >= float(min_score))
    entity_id = top_code if assigned else "UNASSIGNED"
    return EntityMappingResult(
        entity_type="ticker",
        entity_id=entity_id,
        score=round(max(0.0, min(top_score, 1.0)), 4),
        assigned=assigned,
        reason={
            "status": "assigned" if assigned else "unassigned",
            "hinted_stock_code": hint,
            "top_reason": top_reason,
            "ambiguity_gap": round(ambiguity_gap, 4),
            "top_candidates": [
                {"entity_id": code, "score": round(score, 4), "reason": reason}
                for code, score, reason, _ in candidates[:5]
            ],
        },
    )


def _has_ambiguous_context(title: str, body: str) -> bool:
    merged = f"{title} {body}".lower()
    return any(keyword in merged for keyword in _AMBIGUOUS_KEYWORDS)
