from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from difflib import SequenceMatcher

from stock_mvp.storage import master_repo
from stock_mvp.utils import compact_text


_AMBIGUOUS_ALIAS_TOKENS = {
    "\uadf8\ub8f9",  # 그룹
    "\ud640\ub529\uc2a4",  # 홀딩스
    "\uae08\uc735",  # 금융
    "\uc99d\uad8c",  # 증권
    "\uc740\ud589",  # 은행
    "tech",
    "energy",
    "finance",
}

_GROUP_ONLY_TOKENS = {"\uadf8\ub8f9", "group", "holdings", "\ud640\ub529\uc2a4"}

_NUMERIC_CONTEXT_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?%|\d+\s*(?:\uc6d0|\uc5b5|\uc870|\ub9cc)|\uc2e4\uc801|\ub9e4\ucd9c|\uc601\uc5c5\uc774\uc775|eps|\ubaa9\ud45c\uac00|\uac00\uc774\ub358\uc2a4|\uc804\ub9dd)",
    flags=re.IGNORECASE,
)

_SECTOR_KEYWORDS: dict[str, tuple[str, ...]] = {
    "SEMICONDUCTORS": (
        "\ubc18\ub3c4\uccb4",
        "hbm",
        "\uba54\ubaa8\ub9ac",
        "\ud30c\uc6b4\ub4dc\ub9ac",
        "chip",
        "semiconductor",
    ),
    "AUTO": ("\uc790\ub3d9\ucc28", "\uc804\uae30\ucc28", "\ubc30\ud130\ub9ac", "car", "mobility"),
    "BIO": ("\ubc14\uc774\uc624", "\uc81c\uc57d", "\uc784\uc0c1", "drug", "pharma"),
    "FINANCE": ("\uc740\ud589", "\uc99d\uad8c", "\ubcf4\ud5d8", "\uae08\uc735", "bank", "finance"),
    "ENERGY": ("\uc815\uc720", "\uac00\uc2a4", "\uc804\ub825", "\uc5d0\ub108\uc9c0", "oil", "energy"),
}


@dataclass(frozen=True)
class MappingCandidate:
    entity_type: str
    entity_id: str
    score: float
    confidence: str
    reason: dict[str, object]


@dataclass(frozen=True)
class MappingResult:
    status: str
    primary: MappingCandidate | None
    ticker_candidates: list[MappingCandidate]
    sector_candidates: list[MappingCandidate]
    mapping_reason: dict[str, object]


def map_normalized_item(
    conn: sqlite3.Connection,
    *,
    item_id: int,
    normalized_title: str,
    normalized_snippet: str,
    normalized_body: str,
    lead_paragraph: str,
    ticker_threshold: float,
    sector_threshold: float,
    max_tickers: int,
    allowed_tickers: set[str] | None = None,
) -> MappingResult:
    aliases = master_repo.list_active_aliases(conn)
    title = compact_text(normalized_title).lower()
    lead = compact_text(lead_paragraph or normalized_snippet).lower()
    body = compact_text(normalized_body or normalized_snippet).lower()
    title_tokens = re.findall(r"[a-zA-Z0-9\uac00-\ud7a3]{2,}", title)
    numeric_context = bool(_NUMERIC_CONTEXT_PATTERN.search(f"{title} {lead} {body}"))

    ticker_scores: dict[str, float] = {}
    ticker_reasons: dict[str, dict[str, object]] = {}
    for row in aliases:
        ticker = compact_text(str(row["ticker"] or "")).upper()
        if not ticker:
            continue
        if allowed_tickers and ticker not in allowed_tickers:
            continue
        alias = compact_text(str(row["alias"] or ""))
        if not alias:
            continue
        alias_type = compact_text(str(row["alias_type"] or "manual")).lower()
        weight = float(row["weight"] or 1.0)
        alias_l = alias.lower()

        title_hit = _contains_alias(title, alias_l)
        lead_hit = _contains_alias(lead, alias_l)
        body_hit = _contains_alias(body, alias_l)
        fuzzy_hit = False
        if not title_hit and len(alias_l) >= 4:
            fuzzy_hit = _title_fuzzy_match(alias_l, title_tokens) >= 0.92
        if not (title_hit or lead_hit or body_hit or fuzzy_hit):
            continue

        score = 0.0
        reason = ticker_reasons.setdefault(
            ticker,
            {
                "title_exact": [],
                "title_alias": [],
                "lead_exact": [],
                "body_match": [],
                "title_fuzzy": [],
                "numeric_context": False,
                "ambiguous_penalty": [],
                "group_only_penalty": [],
            },
        )
        if title_hit:
            if alias_type in {"official_name", "corp_name"}:
                score += 6.0
                reason["title_exact"].append(alias)
            else:
                score += 5.0
                reason["title_alias"].append(alias)
        if lead_hit:
            score += 4.0
            reason["lead_exact"].append(alias)
        if body_hit:
            score += 2.0
            reason["body_match"].append(alias)
        if fuzzy_hit:
            score += 2.0
            reason["title_fuzzy"].append(alias)
        if numeric_context and (title_hit or lead_hit or body_hit):
            score += 2.0
            reason["numeric_context"] = True
        if _is_ambiguous_alias(alias_l):
            score -= 3.0
            reason["ambiguous_penalty"].append(alias)
        if _is_group_only_alias(alias_l):
            score -= 2.0
            reason["group_only_penalty"].append(alias)

        score *= max(0.5, min(1.2, weight))
        ticker_scores[ticker] = min(12.0, ticker_scores.get(ticker, 0.0) + score)

    ticker_candidates = sorted(
        [
            MappingCandidate(
                entity_type="ticker",
                entity_id=ticker,
                score=round(score, 2),
                confidence=_confidence(score),
                reason=ticker_reasons.get(ticker, {}),
            )
            for ticker, score in ticker_scores.items()
            if score > 0
        ],
        key=lambda c: c.score,
        reverse=True,
    )[:5]

    sector_candidates = _score_sectors(title=title, lead=lead, body=body, numeric_context=numeric_context)[:5]
    best_ticker = ticker_candidates[0] if ticker_candidates else None
    second_ticker = ticker_candidates[1] if len(ticker_candidates) > 1 else None
    best_sector = sector_candidates[0] if sector_candidates else None
    top_multi = [c for c in ticker_candidates if c.score >= 7][: max(1, int(max_tickers))]

    primary: MappingCandidate | None = None
    status = "unassigned"
    if best_ticker and best_ticker.score >= float(ticker_threshold):
        gap = best_ticker.score - (second_ticker.score if second_ticker else 0.0)
        if gap >= 3:
            primary = best_ticker
            status = "mapped_ticker"
        elif len(top_multi) >= 2:
            primary = best_ticker
            status = "mapped_ticker_multi"
    if primary is None and best_sector and best_sector.score >= float(sector_threshold):
        if (best_ticker is None) or (best_sector.score > best_ticker.score):
            primary = best_sector
            status = "mapped_sector"

    mapping_reason = {
        "item_id": int(item_id),
        "status": status,
        "ticker_threshold": float(ticker_threshold),
        "sector_threshold": float(sector_threshold),
        "top_ticker_candidates": [
            {"entity_id": c.entity_id, "score": c.score, "reason": c.reason} for c in ticker_candidates
        ],
        "top_sector_candidates": [
            {"entity_id": c.entity_id, "score": c.score, "reason": c.reason} for c in sector_candidates
        ],
    }
    return MappingResult(
        status=status,
        primary=primary,
        ticker_candidates=ticker_candidates,
        sector_candidates=sector_candidates,
        mapping_reason=mapping_reason,
    )


def _score_sectors(*, title: str, lead: str, body: str, numeric_context: bool) -> list[MappingCandidate]:
    candidates: list[MappingCandidate] = []
    for sector_code, keywords in _SECTOR_KEYWORDS.items():
        title_hits = [k for k in keywords if k in title]
        lead_hits = [k for k in keywords if k in lead]
        body_hits = [k for k in keywords if k in body]
        if not title_hits and not lead_hits and not body_hits:
            continue
        unique_hits = sorted(set([*title_hits, *lead_hits, *body_hits]))
        score = min(
            10.0,
            4.0
            + len(set(title_hits)) * 2.0
            + len(set(lead_hits)) * 1.5
            + len(set(body_hits)) * 1.0
            + max(0, len(unique_hits) - len(set(title_hits))) * 0.5
            + (1.0 if numeric_context else 0.0),
        )
        candidates.append(
            MappingCandidate(
                entity_type="sector",
                entity_id=sector_code,
                score=round(score, 2),
                confidence=_confidence(score),
                reason={
                    "sector_keywords": unique_hits[:10],
                    "numeric_context": numeric_context,
                },
            )
        )
    return sorted(candidates, key=lambda x: x.score, reverse=True)


def to_json(result: MappingResult) -> dict[str, object]:
    return {
        "status": result.status,
        "primary": {
            "entity_type": result.primary.entity_type,
            "entity_id": result.primary.entity_id,
            "score": result.primary.score,
            "confidence": result.primary.confidence,
            "reason": result.primary.reason,
        }
        if result.primary
        else None,
        "ticker_candidates": [
            {
                "entity_type": c.entity_type,
                "entity_id": c.entity_id,
                "score": c.score,
                "confidence": c.confidence,
                "reason": c.reason,
            }
            for c in result.ticker_candidates
        ],
        "sector_candidates": [
            {
                "entity_type": c.entity_type,
                "entity_id": c.entity_id,
                "score": c.score,
                "confidence": c.confidence,
                "reason": c.reason,
            }
            for c in result.sector_candidates
        ],
        "mapping_reason": result.mapping_reason,
    }


def from_body_paragraphs_json(text: str) -> list[str]:
    raw = compact_text(text)
    if not raw:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return [compact_text(str(x)) for x in data if compact_text(str(x))]


def _contains_alias(text: str, alias: str) -> bool:
    if not text or not alias:
        return False
    if re.fullmatch(r"[a-z0-9.\-]+", alias):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", text))
    return alias in text


def _title_fuzzy_match(alias: str, title_tokens: list[str]) -> float:
    best = 0.0
    for token in title_tokens:
        if abs(len(token) - len(alias)) > 2:
            continue
        best = max(best, SequenceMatcher(a=alias, b=token).ratio())
    return best


def _is_ambiguous_alias(alias: str) -> bool:
    if len(alias) <= 2:
        return True
    return alias in _AMBIGUOUS_ALIAS_TOKENS


def _is_group_only_alias(alias: str) -> bool:
    return any(token in alias for token in _GROUP_ONLY_TOKENS)


def _confidence(score: float) -> str:
    if score >= 9:
        return "high"
    if score >= 7:
        return "medium"
    return "low"

