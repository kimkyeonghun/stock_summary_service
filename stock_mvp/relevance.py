from __future__ import annotations

import re
from dataclasses import dataclass

from stock_mvp.models import Stock
from stock_mvp.utils import compact_text


_GENERIC_KR_TERMS = {
    "금융",
    "은행",
    "증권",
    "보험",
    "그룹",
    "홀딩스",
    "지주",
    "전자",
    "테크",
    "에너지",
    "바이오",
    "제약",
}
_GENERIC_EN_TERMS = {
    "bank",
    "financial",
    "finance",
    "group",
    "holding",
    "holdings",
    "inc",
    "corp",
    "company",
    "tech",
    "technology",
    "energy",
    "bio",
}


@dataclass(frozen=True)
class RelevanceResult:
    score: float
    matched_alias: str
    reason: str


def relevance_threshold(source: str, doc_type: str) -> float:
    src = (source or "").strip().lower()
    typ = (doc_type or "").strip().lower()
    if src == "naver_news":
        return 0.56
    if src == "naver_finance_research":
        return 0.34
    if src == "sec_edgar":
        return 0.12
    if src == "opendart":
        return 0.12
    if typ == "news":
        return 0.5
    return 0.38


def evaluate_stock_document_relevance(
    stock: Stock,
    *,
    title: str,
    body: str,
    url: str,
    source: str,
    doc_type: str,
) -> RelevanceResult:
    title_norm = compact_text(title).lower()
    body_norm = compact_text(body).lower()
    full_norm = compact_text(f"{title} {body}").lower()
    url_norm = (url or "").lower()

    score = 0.0
    matched_alias = ""
    reasons: list[str] = []

    if source == "sec_edgar":
        # SEC docs are already ticker-targeted in crawler URL path.
        score += 0.22
        reasons.append("sec_base")
    elif source == "opendart":
        # OpenDART docs are already corp_code-targeted in API response.
        score += 0.22
        reasons.append("opendart_base")

    for alias in _build_aliases(stock):
        alias_norm = alias.lower()
        if not alias_norm:
            continue
        is_code = alias_norm == stock.code.lower()
        in_title = _contains_alias(title_norm, alias_norm, is_code=is_code)
        in_body = _contains_alias(body_norm, alias_norm, is_code=is_code)
        in_url = _contains_alias(url_norm, alias_norm, is_code=is_code)
        if not (in_title or in_body or in_url):
            continue

        if not matched_alias:
            matched_alias = alias

        if is_code:
            if in_title:
                score += 0.62
                reasons.append("code:title")
            elif in_body:
                score += 0.48
                reasons.append("code:body")
            if in_url:
                score += 0.22
                reasons.append("code:url")
            continue

        alias_weight = _alias_weight(alias)
        if in_title:
            score += 0.55 * alias_weight
            reasons.append("alias:title")
        if in_body:
            score += 0.30 * alias_weight
            reasons.append("alias:body")
        if in_url:
            score += 0.15 * alias_weight
            reasons.append("alias:url")

    # Hard guard: if no alias evidence exists, reject regardless of score.
    if not reasons:
        return RelevanceResult(score=0.0, matched_alias="", reason="no_alias_match")

    # Penalize documents where only body-level weak aliases matched.
    if all(r.endswith(":body") for r in reasons):
        score -= 0.18
        reasons.append("penalty:body_only")

    # Small confidence bonus for cleaner headline-level match.
    if any(r.endswith(":title") for r in reasons):
        score += 0.06

    score = max(0.0, min(round(score, 4), 1.0))
    return RelevanceResult(score=score, matched_alias=matched_alias, reason=",".join(reasons[:6]))


def passes_relevance(result: RelevanceResult, *, source: str, doc_type: str) -> bool:
    return result.score >= relevance_threshold(source=source, doc_type=doc_type)


def _build_aliases(stock: Stock) -> list[str]:
    aliases: list[str] = []
    seen: set[str] = set()

    candidates = [stock.code, stock.name, *stock.queries]
    for raw in candidates:
        v = compact_text(str(raw or "")).strip()
        if not v:
            continue
        _push_alias(aliases, seen, v)
        # Additional normalized alias for corp suffix variants.
        for normalized in _derive_alias_variants(v):
            _push_alias(aliases, seen, normalized)
    return aliases


def _push_alias(aliases: list[str], seen: set[str], value: str) -> None:
    v = compact_text(value).strip()
    if not v:
        return
    key = v.lower()
    if key in seen:
        return
    if _is_generic_alias(v):
        return
    seen.add(key)
    aliases.append(v)


def _derive_alias_variants(value: str) -> list[str]:
    out: list[str] = []
    normalized = value.replace("주식회사", "").strip()
    if normalized and normalized != value:
        out.append(normalized)
    normalized = normalized.replace("(주)", "").strip()
    if normalized and normalized != value:
        out.append(normalized)
    return out


def _is_generic_alias(alias: str) -> bool:
    token = alias.strip()
    if not token:
        return True
    if token.isdigit():
        return len(token) != 6
    if re.fullmatch(r"[A-Za-z.\-]+", token):
        if len(token) <= 1:
            return True
        if token.lower() in _GENERIC_EN_TERMS:
            return True
        return False
    if len(token) <= 1:
        return True
    if token in _GENERIC_KR_TERMS:
        return True
    return False


def _alias_weight(alias: str) -> float:
    if alias.isdigit():
        return 1.0
    if re.fullmatch(r"[A-Za-z.\-]+", alias):
        return 1.0 if len(alias) >= 3 else 0.75
    if len(alias) >= 4:
        return 1.0
    if len(alias) == 3:
        return 0.9
    if len(alias) == 2:
        return 0.75
    return 0.6


def _contains_alias(text: str, alias: str, *, is_code: bool) -> bool:
    if not text or not alias:
        return False
    if is_code:
        return bool(re.search(rf"(?<!\d){re.escape(alias)}(?!\d)", text))
    if re.fullmatch(r"[A-Za-z.\-]+", alias):
        return bool(re.search(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", text))
    return alias in text
