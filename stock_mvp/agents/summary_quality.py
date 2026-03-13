from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Iterable

from stock_mvp.utils import compact_text


SECTION_ORDER = ("conclusion", "evidence", "risk", "checkpoint", "final")
SECTION_LABELS = {
    "conclusion": "결론",
    "evidence": "근거",
    "risk": "리스크",
    "checkpoint": "체크포인트",
    "final": "최종 판단",
}
SECTION_ALIAS = {
    "결론": "conclusion",
    "conclusion": "conclusion",
    "근거": "evidence",
    "evidence": "evidence",
    "리스크": "risk",
    "risk": "risk",
    "체크포인트": "checkpoint",
    "checkpoint": "checkpoint",
    "최종 판단": "final",
    "final": "final",
}

FORBIDDEN_ADVICE_PATTERNS = (
    r"\b강력\s*매수\b",
    r"\b매수\b",
    r"\b매도\b",
    r"\b비중\s*확대\b",
    r"\b지금\s*사\b",
    r"\b팔아야\b",
    r"\bstrong\s*buy\b",
    r"\bbuy\s*now\b",
    r"\bsell\s*now\b",
)
FORBIDDEN_ADVICE_RE = re.compile("|".join(FORBIDDEN_ADVICE_PATTERNS), flags=re.IGNORECASE)
FACT_TOKEN_RE = re.compile(
    r"(?:\b20\d{2}[./-]\d{1,2}[./-]\d{1,2}\b|\b\d+(?:[.,]\d+)?%|\b\d+(?:[.,]\d+)?\b|\b[A-Z]{2,6}\b)"
)


def sanitize_line(text: str, *, max_len: int = 220) -> str:
    value = compact_text(text)
    if not value:
        return ""
    if FORBIDDEN_ADVICE_RE.search(value):
        value = "투자 판단은 추가 확인이 필요합니다."
    return value[:max_len]


def sanitize_lines(lines: Iterable[str], *, max_len: int = 220, limit: int = 6) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in lines:
        text = sanitize_line(str(raw), max_len=max_len)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= max(1, limit):
            break
    return out


def similarity_to_title(text: str, title: str) -> float:
    left = compact_text(text).lower()
    right = compact_text(title).lower()
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def reduce_title_copy(text: str, title: str, *, max_ratio: float = 0.9) -> str:
    value = compact_text(text)
    if not value:
        return ""
    if similarity_to_title(value, title) >= max_ratio:
        return ""
    return value


def extract_fact_tokens(text: str) -> list[str]:
    return [compact_text(x) for x in FACT_TOKEN_RE.findall(str(text or "")) if compact_text(x)]


def fact_token_preservation_ratio(source_text: str, summary_text: str) -> float:
    src = extract_fact_tokens(source_text)
    if not src:
        return 1.0
    src_norm = [re.sub(r"[\s,]", "", x).upper() for x in src]
    dst_norm = re.sub(r"[\s,]", "", str(summary_text or "")).upper()
    kept = sum(1 for token in src_norm if token and token in dst_norm)
    return kept / max(1, len(src_norm))


def parse_section_line(line: str) -> tuple[str | None, str]:
    raw = compact_text(line)
    if not raw:
        return None, ""
    matched = re.match(r"^\s*([A-Za-z가-힣 ]+)\s*:\s*(.+)$", raw)
    if not matched:
        return None, raw
    prefix = compact_text(matched.group(1))
    body = compact_text(matched.group(2))
    if not body:
        return None, ""
    key = SECTION_ALIAS.get(prefix, SECTION_ALIAS.get(prefix.lower()))
    return key, body


def format_section_line(section_key: str, body: str) -> str:
    label = SECTION_LABELS.get(section_key, "기타")
    value = sanitize_line(body)
    return f"{label}: {value}" if value else ""


def has_required_sections(lines: list[str]) -> bool:
    seen: set[str] = set()
    for line in lines:
        key, _body = parse_section_line(line)
        if key:
            seen.add(key)
    required = {"conclusion", "evidence", "risk", "checkpoint", "final"}
    return required.issubset(seen)

