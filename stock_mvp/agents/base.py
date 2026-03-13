from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from stock_mvp.utils import compact_text


NUMBER_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?%?")
DATE_RE = re.compile(r"\b(?:20\d{2}[./-]\d{1,2}[./-]\d{1,2}|\d{1,2}[./-]\d{1,2})\b")

TOPIC_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("earnings", ("earnings", "profit", "revenue", "results", "guidance", "per", "eps", "roe")),
    ("demand", ("demand", "orders", "shipment", "sales", "booking", "backlog")),
    ("regulation", ("regulation", "policy", "approval", "sanction", "compliance")),
    ("valuation", ("valuation", "target price", "multiple", "p/e", "pbr", "discount")),
    ("supply_chain", ("supply", "inventory", "capacity", "utilization", "lead time")),
    ("macro", ("fx", "rate", "inflation", "macro", "yield", "dollar")),
    ("product", ("launch", "product", "release", "model", "roadmap")),
    ("litigation", ("lawsuit", "litigation", "dispute", "investigation")),
]

SOURCE_WEIGHT = {"research": 0.78, "filing": 0.66, "news": 0.46}
RISK_KEYWORDS = (
    "risk",
    "uncertain",
    "volatility",
    "downside",
    "decline",
    "delay",
    "lawsuit",
    "investigation",
    "regulation",
)


@dataclass(frozen=True)
class AgentStats:
    total: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0


def source_type_from_item(source: str, doc_type: str) -> str:
    src = (source or "").strip().lower()
    typ = (doc_type or "").strip().lower()
    if src in {"sec_edgar", "opendart"}:
        return "filing"
    if typ == "filing":
        return "filing"
    if typ == "report":
        return "research"
    return "news"


def confidence_weight(source_type: str) -> float:
    return SOURCE_WEIGHT.get((source_type or "").lower(), 0.4)


def extract_topics(*texts: str) -> list[str]:
    joined = compact_text(" ".join(texts)).lower()
    topics: list[str] = []
    for topic, keywords in TOPIC_RULES:
        if any(k.lower() in joined for k in keywords):
            topics.append(topic)
    if not topics:
        topics.append("general")
    return topics[:6]


def split_sentences(text: str, max_len: int = 220) -> list[str]:
    raw = compact_text(text)
    if not raw:
        return []
    parts = re.split(r"(?<=[.!?])\s+", raw)
    out: list[str] = []
    for part in parts:
        sentence = compact_text(part)
        if sentence:
            out.append(sentence[:max_len])
    return out


def has_fact_anchor(text: str, *, entity_hint: str = "") -> bool:
    value = compact_text(text)
    if not value:
        return False
    if NUMBER_RE.search(value):
        return True
    if DATE_RE.search(value):
        return True
    if entity_hint and entity_hint.lower() in value.lower():
        return True
    return False


def ensure_hedged_interpretation(text: str) -> str:
    value = compact_text(text)
    if not value:
        return "Based on current facts, direction may change as new evidence arrives."
    lower = value.lower()
    hedges = ("may", "could", "might", "possible", "possibility", "suggest", "likely")
    if any(h in lower for h in hedges):
        return value
    return f"{value} This may be interpreted as a tentative signal."


def ensure_risk_note(text: str) -> str:
    value = compact_text(text)
    if not value:
        return "No explicit risk statement in source."
    return value


def detect_risk_note(*texts: str) -> str:
    sentences = split_sentences(" ".join(texts))
    for sentence in sentences:
        lower = sentence.lower()
        if any(keyword in lower for keyword in RISK_KEYWORDS):
            return sentence
    return "No explicit risk statement in source."


def iso_date_utc(dt: datetime | None = None) -> str:
    current = dt or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.date().isoformat()


def date_days_ago(base_date: str, days: int) -> str:
    parsed = datetime.fromisoformat(base_date).date()
    return (parsed - timedelta(days=max(0, days))).isoformat()
