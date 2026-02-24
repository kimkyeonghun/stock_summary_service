from __future__ import annotations

from datetime import datetime, timezone

from stock_mvp.config import Settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.models import SectorGeneratedSummary, SummaryLine
from stock_mvp.utils import compact_text, format_source_tag, now_utc


POSITIVE_TERMS = ("record", "beat", "growth", "surge", "upgrade", "outperform", "improve")
NEGATIVE_TERMS = ("risk", "downgrade", "drop", "decline", "delay", "lawsuit", "weak")


class SectorSummaryBuilder:
    fallback_model_name = "sector_rule_v1"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.llm = LLMClient(settings)

    def build(self, sector_code: str, sector_name: str, docs: list[dict]) -> SectorGeneratedSummary:
        docs_sorted = sorted(
            docs,
            key=lambda d: (d.get("published_at") or "", d.get("id") or 0),
            reverse=True,
        )
        if not docs_sorted:
            return self._build_empty(sector_code)

        llm_result = self._build_with_llm(sector_code, sector_name, docs_sorted)
        if llm_result is not None:
            return llm_result
        return self._build_fallback(sector_code, sector_name, docs_sorted)

    def _build_with_llm(self, sector_code: str, sector_name: str, docs: list[dict]) -> SectorGeneratedSummary | None:
        result = self.llm.generate_json(
            system_prompt=_sector_system_prompt(),
            user_prompt=_sector_user_prompt(sector_code, sector_name, docs),
        )
        if result is None:
            return None
        parsed = _validate_llm_payload(result.payload, docs)
        if parsed is None:
            print(f"[WARN] sector summary llm invalid payload: sector_code={sector_code}")
            return None
        lines, sentiment_label, sentiment_confidence = parsed
        as_of = _latest_doc_datetime(docs) or now_utc()
        return SectorGeneratedSummary(
            sector_code=sector_code,
            as_of=as_of,
            lines=lines,
            sentiment_label=sentiment_label,
            sentiment_confidence=sentiment_confidence,
            model=f"llm:{result.model}",
        )

    def _build_fallback(self, sector_code: str, sector_name: str, docs: list[dict]) -> SectorGeneratedSummary:
        top = docs[0]
        second = docs[1] if len(docs) > 1 else top
        third = docs[2] if len(docs) > 2 else top

        sentiment_label, sentiment_conf = _fallback_sentiment(docs)

        lines = [
            SummaryLine(
                text=f"{sector_name} sector has {len(docs)} recent deduplicated documents "
                f"with mixed drivers. {format_source_tag(top['source'], top.get('published_at'))}",
                source_doc_ids=[int(top["id"])],
            ),
            SummaryLine(
                text=f"Top headline: {compact_text(str(top.get('title') or 'no title'))[:140]} "
                f"{format_source_tag(top['source'], top.get('published_at'))}",
                source_doc_ids=[int(top["id"])],
            ),
            SummaryLine(
                text=f"Follow-up signal: {compact_text(str(second.get('title') or 'no title'))[:140]} "
                f"{format_source_tag(second['source'], second.get('published_at'))}",
                source_doc_ids=[int(second["id"])],
            ),
            SummaryLine(
                text=f"Third signal: {compact_text(str(third.get('title') or 'no title'))[:140]} "
                f"{format_source_tag(third['source'], third.get('published_at'))}",
                source_doc_ids=[int(third["id"])],
            ),
            SummaryLine(
                text=(
                    f"Cross-stock spread indicates this sector document set references "
                    f"{int(top.get('linked_stock_count') or 0)} stocks in a single item. "
                    f"{format_source_tag(top['source'], top.get('published_at'))}"
                ),
                source_doc_ids=[int(top["id"])],
            ),
            SummaryLine(
                text=(
                    f"Report/news mix should be checked before trading decisions because item types can differ "
                    f"by source. {format_source_tag(second['source'], second.get('published_at'))}"
                ),
                source_doc_ids=[int(second["id"])],
            ),
            SummaryLine(
                text=(
                    "Use this as a morning context view, then verify key claims in original source links. "
                    f"{format_source_tag(third['source'], third.get('published_at'))}"
                ),
                source_doc_ids=[int(third["id"])],
            ),
            SummaryLine(
                text=(
                    "Risk note: if multiple headlines repeat the same event, avoid over-weighting duplicated impact. "
                    f"{format_source_tag(top['source'], top.get('published_at'))}"
                ),
                source_doc_ids=[int(top["id"])],
            ),
        ]
        as_of = _latest_doc_datetime(docs) or now_utc()
        return SectorGeneratedSummary(
            sector_code=sector_code,
            as_of=as_of,
            lines=lines,
            sentiment_label=sentiment_label,
            sentiment_confidence=sentiment_conf,
            model=self.fallback_model_name,
        )

    def _build_empty(self, sector_code: str) -> SectorGeneratedSummary:
        now = now_utc()
        lines = [SummaryLine(text="No recent sector documents were found.", source_doc_ids=[]) for _ in range(8)]
        return SectorGeneratedSummary(
            sector_code=sector_code,
            as_of=now,
            lines=lines,
            sentiment_label="neutral",
            sentiment_confidence=0.2,
            model=self.fallback_model_name,
        )


def _sector_system_prompt() -> str:
    return (
        "You are a financial sector analyst. "
        "Return strict JSON only with keys: sentiment_label, sentiment_confidence, lines. "
        "sentiment_label must be one of positive, neutral, negative. "
        "sentiment_confidence must be 0..1 float. "
        "lines must be exactly 8 items. "
        "Each line item must have keys: text, source_ids. "
        "source_ids must be a non-empty list of integer ids from provided documents."
    )


def _sector_user_prompt(sector_code: str, sector_name: str, docs: list[dict]) -> str:
    trimmed = []
    for d in docs[:18]:
        trimmed.append(
            {
                "id": int(d["id"]),
                "title": compact_text(str(d.get("title") or ""))[:180],
                "source": str(d.get("source") or ""),
                "doc_type": str(d.get("doc_type") or ""),
                "published_at": str(d.get("published_at") or ""),
                "linked_stock_count": int(d.get("linked_stock_count") or 0),
                "linked_document_count": int(d.get("linked_document_count") or 0),
                "body_snippet": compact_text(str(d.get("body") or ""))[:220],
            }
        )
    return (
        f"sector_code={sector_code}\n"
        f"sector_name={sector_name}\n"
        "documents_json=\n"
        f"{trimmed}\n"
        "Generate concise Korean summary lines for beginner investors."
    )


def _validate_llm_payload(payload: dict, docs: list[dict]) -> tuple[list[SummaryLine], str, float] | None:
    label = str(payload.get("sentiment_label") or "").strip().lower()
    if label not in {"positive", "neutral", "negative"}:
        return None
    try:
        conf = float(payload.get("sentiment_confidence", 0.0))
    except (TypeError, ValueError):
        return None
    conf = max(0.0, min(conf, 1.0))

    raw_lines = payload.get("lines")
    if not isinstance(raw_lines, list) or len(raw_lines) != 8:
        return None

    valid_ids = {int(d["id"]) for d in docs}
    fallback_id = int(docs[0]["id"]) if docs else 0
    lines: list[SummaryLine] = []

    for item in raw_lines:
        if not isinstance(item, dict):
            return None
        text = compact_text(str(item.get("text") or ""))
        if not text:
            return None

        src_raw = item.get("source_ids")
        if not isinstance(src_raw, list):
            src_raw = []
        src_ids: list[int] = []
        for x in src_raw:
            try:
                v = int(x)
            except (TypeError, ValueError):
                continue
            if v in valid_ids and v not in src_ids:
                src_ids.append(v)
        if not src_ids:
            src_ids = [fallback_id] if fallback_id else []
        lines.append(SummaryLine(text=text[:220], source_doc_ids=src_ids))
    return lines, label, round(conf, 4)


def _latest_doc_datetime(docs: list[dict]) -> datetime | None:
    for d in docs:
        raw = d.get("published_at")
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                continue
            try:
                parsed = datetime.fromisoformat(text)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                continue
    return None


def _fallback_sentiment(docs: list[dict]) -> tuple[str, float]:
    pos = 0
    neg = 0
    for d in docs[:30]:
        text = f"{d.get('title', '')} {d.get('body', '')}".lower()
        pos += sum(1 for term in POSITIVE_TERMS if term in text)
        neg += sum(1 for term in NEGATIVE_TERMS if term in text)

    if pos > neg:
        gap = pos - neg
        return "positive", round(min(0.55 + gap * 0.05, 0.9), 4)
    if neg > pos:
        gap = neg - pos
        return "negative", round(min(0.55 + gap * 0.05, 0.9), 4)
    return "neutral", 0.5
