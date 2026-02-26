from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from stock_mvp.config import Settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.models import GeneratedSummary, SummaryLine
from stock_mvp.utils import compact_text, now_utc


POSITIVE_TERMS = (
    "상향",
    "개선",
    "증가",
    "성장",
    "호조",
    "회복",
    "수주",
    "흑자",
    "확대",
    "강세",
    "beat",
    "upgrade",
    "outperform",
)
NEGATIVE_TERMS = (
    "하향",
    "둔화",
    "감소",
    "악화",
    "부진",
    "적자",
    "리스크",
    "지연",
    "규제",
    "불확실",
    "downgrade",
    "weak",
    "decline",
)
RISK_HINTS = ("리스크", "불확실", "지연", "규제", "소송", "부채", "변동성", "환율", "공급망")
CHECKPOINT_HINTS = ("실적", "가이던스", "공시", "출시", "수주", "정책", "금리", "회의", "승인")

ALIAS = {
    "conclusion": ("conclusion", "summary", "overall", "결론"),
    "evidences": ("evidences", "evidence", "grounds", "reasons", "근거"),
    "risks": ("risks", "risk", "risk_points", "리스크"),
    "checkpoints": ("checkpoints", "checkpoint", "watchpoints", "체크포인트"),
    "sentiment": ("sentiment", "final_sentiment", "judgement", "판단"),
}


class SummaryBuilder:
    fallback_model_name = "stock_rule_v3"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.llm = LLMClient(settings)

    def build(self, stock_code: str, docs: list[dict]) -> GeneratedSummary:
        docs_sorted = sorted(
            docs,
            key=lambda d: (d.get("published_at") or "", d.get("id") or 0),
            reverse=True,
        )
        if not docs_sorted:
            return self._build_empty(stock_code)

        llm_summary = self._build_with_llm(stock_code, docs_sorted)
        if llm_summary is not None:
            return llm_summary
        return self._build_fallback(stock_code, docs_sorted)

    def _build_with_llm(self, stock_code: str, docs: list[dict]) -> GeneratedSummary | None:
        result = self.llm.generate_json(
            system_prompt=_stock_system_prompt(),
            user_prompt=_stock_user_prompt(stock_code, docs),
            purpose="stock_summary",
        )
        if result is None:
            return None

        normalized = _normalize_llm_payload(result.payload, docs)
        if normalized is None:
            print(
                f"[WARN] stock summary llm invalid payload: stock_code={stock_code} "
                f"keys={list(result.payload.keys())[:8]}"
            )
            return None

        lines = _compose_summary_lines(
            conclusion=normalized["conclusion"],
            evidences=normalized["evidences"],
            risks=normalized["risks"],
            checkpoints=normalized["checkpoints"],
            sentiment=normalized["sentiment"],
        )
        as_of = _latest_doc_datetime(docs) or now_utc()
        return GeneratedSummary(
            stock_code=stock_code,
            as_of=as_of,
            lines=lines,
            model=f"llm:{result.model}",
        )

    def _build_fallback(self, stock_code: str, docs: list[dict]) -> GeneratedSummary:
        top = docs[0]
        second = docs[1] if len(docs) > 1 else top
        risk_doc = _find_by_terms(docs, RISK_HINTS) or top
        checkpoint_doc = _find_by_terms(docs, CHECKPOINT_HINTS) or second

        sentiment_label, sentiment_conf = _fallback_sentiment(docs)
        sentiment_ko = _sentiment_label_ko(sentiment_label)

        lines = [
            SummaryLine(text=f"결론: {_fact_sentence(top)}", source_doc_ids=[int(top["id"])]),
            SummaryLine(text=f"근거1: {_fact_sentence(top)}", source_doc_ids=[int(top["id"])]),
            SummaryLine(text=f"근거2: {_fact_sentence(second)}", source_doc_ids=[int(second["id"])]),
            SummaryLine(text=f"리스크1: {_fact_sentence(risk_doc)}", source_doc_ids=[int(risk_doc["id"])]),
            SummaryLine(
                text=f"체크포인트1: {_fact_sentence(checkpoint_doc)}",
                source_doc_ids=[int(checkpoint_doc["id"])],
            ),
            SummaryLine(
                text=f"최종판단: {sentiment_ko} (신뢰도 {sentiment_conf:.2f})",
                source_doc_ids=[int(top["id"])],
            ),
        ]
        as_of = _latest_doc_datetime(docs) or now_utc()
        return GeneratedSummary(
            stock_code=stock_code,
            as_of=as_of,
            lines=lines,
            model=self.fallback_model_name,
        )

    @staticmethod
    def _build_empty(stock_code: str) -> GeneratedSummary:
        lines = [
            SummaryLine(text="결론: 요약 가능한 최신 문서가 부족합니다.", source_doc_ids=[]),
            SummaryLine(text="체크포인트1: 다음 수집 주기 이후 다시 확인하세요.", source_doc_ids=[]),
            SummaryLine(text="최종판단: 중립 (신뢰도 0.20)", source_doc_ids=[]),
        ]
        return GeneratedSummary(
            stock_code=stock_code,
            as_of=now_utc(),
            lines=lines,
            model="stock_rule_v3",
        )


def _stock_system_prompt() -> str:
    return (
        "You are a Korean equity analyst. Return JSON only. "
        "Required keys: conclusion, evidences, risks, checkpoints, sentiment. "
        "Format:\n"
        "{\n"
        '  "conclusion": {"text":"...", "source_ids":[123]},\n'
        '  "evidences": [{"text":"...", "source_ids":[123]}],\n'
        '  "risks": [{"text":"...", "source_ids":[123]}],\n'
        '  "checkpoints": [{"text":"...", "source_ids":[123]}],\n'
        '  "sentiment": {"label":"positive|neutral|negative", "confidence":0.0}\n'
        "}\n"
        "Rules: do not copy article/report titles verbatim; paraphrase after understanding. "
        "Do not include [source ...] tags in text."
    )


def _stock_user_prompt(stock_code: str, docs: list[dict]) -> str:
    trimmed_docs: list[dict[str, Any]] = []
    for d in docs[:16]:
        trimmed_docs.append(
            {
                "id": int(d["id"]),
                "source": str(d.get("source") or ""),
                "doc_type": str(d.get("doc_type") or ""),
                "title": compact_text(str(d.get("title") or ""))[:150],
                "published_at": str(d.get("published_at") or ""),
                "body_snippet": compact_text(str(d.get("body") or ""))[:700],
            }
        )
    return (
        f"stock_code={stock_code}\n"
        "Write a concise daily investor brief in Korean:\n"
        "- conclusion: exactly 1 item\n"
        "- evidences: 1~2 items\n"
        "- risks: 1~2 items\n"
        "- checkpoints: 1~2 items\n"
        "- sentiment: final positive/neutral/negative with confidence\n"
        f"documents={trimmed_docs}"
    )


def _normalize_llm_payload(payload: dict, docs: list[dict]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    valid_ids = {int(d["id"]) for d in docs}
    fallback_id = int(docs[0]["id"]) if docs else 0

    conclusion_raw = _pick(payload, "conclusion")
    evidences_raw = _pick(payload, "evidences")
    risks_raw = _pick(payload, "risks")
    checkpoints_raw = _pick(payload, "checkpoints")
    sentiment_raw = _pick(payload, "sentiment")

    conclusion = _parse_item_flexible(conclusion_raw, valid_ids, fallback_id)
    evidences = _parse_item_list_flexible(evidences_raw, valid_ids, fallback_id, 2)
    risks = _parse_item_list_flexible(risks_raw, valid_ids, fallback_id, 2)
    checkpoints = _parse_item_list_flexible(checkpoints_raw, valid_ids, fallback_id, 2)
    sentiment = _parse_sentiment_flexible(sentiment_raw, payload)

    if conclusion is None:
        conclusion = {"text": _fact_sentence(docs[0]), "source_ids": [fallback_id]}
    if not evidences:
        evidences = [
            {"text": _fact_sentence(docs[0]), "source_ids": [fallback_id]},
        ]
        if len(docs) > 1:
            evidences.append({"text": _fact_sentence(docs[1]), "source_ids": [int(docs[1]["id"])]})
    if not risks:
        risk_doc = _find_by_terms(docs, RISK_HINTS) or docs[0]
        risks = [{"text": _fact_sentence(risk_doc), "source_ids": [int(risk_doc["id"])]}]
    if not checkpoints:
        chk_doc = _find_by_terms(docs, CHECKPOINT_HINTS) or docs[0]
        checkpoints = [{"text": _fact_sentence(chk_doc), "source_ids": [int(chk_doc["id"])]}]
    if sentiment is None:
        label, conf = _fallback_sentiment(docs)
        sentiment = {"label": label, "confidence": conf}

    return {
        "conclusion": conclusion,
        "evidences": evidences[:2],
        "risks": risks[:2],
        "checkpoints": checkpoints[:2],
        "sentiment": sentiment,
    }


def _pick(payload: dict, field: str) -> Any:
    for alias in ALIAS[field]:
        if alias in payload:
            return payload.get(alias)
    return None


def _parse_item_flexible(raw: Any, valid_ids: set[int], fallback_id: int) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        text = _clean_text(raw)
        if not text:
            return None
        return {"text": text, "source_ids": [fallback_id] if fallback_id else []}
    if isinstance(raw, dict):
        text = _clean_text(
            str(
                raw.get("text")
                or raw.get("summary")
                or raw.get("content")
                or raw.get("point")
                or raw.get("desc")
                or ""
            )
        )
        if not text:
            return None
        source_ids = _normalize_source_ids(raw.get("source_ids"), valid_ids, fallback_id)
        if not source_ids:
            source_ids = [fallback_id] if fallback_id else []
        return {"text": text, "source_ids": source_ids}
    return None


def _parse_item_list_flexible(raw: Any, valid_ids: set[int], fallback_id: int, max_items: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if raw is None:
        return items
    if isinstance(raw, (str, dict)):
        parsed = _parse_item_flexible(raw, valid_ids, fallback_id)
        return [parsed] if parsed else []
    if not isinstance(raw, list):
        return items

    for x in raw:
        parsed = _parse_item_flexible(x, valid_ids, fallback_id)
        if not parsed:
            continue
        if len(parsed["text"]) < 8:
            continue
        items.append(parsed)
        if len(items) >= max_items:
            break
    return items


def _parse_sentiment_flexible(raw: Any, payload: dict) -> dict[str, Any] | None:
    label = ""
    confidence: float | None = None

    if isinstance(raw, dict):
        label = str(raw.get("label") or raw.get("sentiment") or raw.get("polarity") or "").strip()
        confidence = _to_confidence(raw.get("confidence") or raw.get("score") or raw.get("probability"))
    elif isinstance(raw, str):
        label = raw.strip()

    if not label:
        label = str(payload.get("sentiment_label") or payload.get("label") or "").strip()
    if confidence is None:
        confidence = _to_confidence(payload.get("sentiment_confidence") or payload.get("confidence"))

    norm = _normalize_sentiment_label(label)
    if not norm:
        return None
    if confidence is None:
        confidence = 0.55 if norm != "neutral" else 0.5
    confidence = max(0.0, min(confidence, 1.0))
    return {"label": norm, "confidence": round(confidence, 2)}


def _normalize_sentiment_label(value: str) -> str:
    v = compact_text(value).lower()
    mapping = {
        "positive": "positive",
        "bullish": "positive",
        "긍정": "positive",
        "neutral": "neutral",
        "중립": "neutral",
        "negative": "negative",
        "bearish": "negative",
        "부정": "negative",
    }
    return mapping.get(v, "")


def _to_confidence(value: Any) -> float | None:
    try:
        if value is None:
            return None
        conf = float(value)
    except (TypeError, ValueError):
        return None
    if conf > 1.0 and conf <= 100:
        conf = conf / 100.0
    return conf


def _normalize_source_ids(raw: Any, valid_ids: set[int], fallback_id: int) -> list[int]:
    src_ids: list[int] = []
    if isinstance(raw, str):
        raw = [x for x in re.split(r"[,\s;]+", raw) if x]
    if isinstance(raw, list):
        for x in raw:
            try:
                doc_id = int(x)
            except (TypeError, ValueError):
                continue
            if doc_id in valid_ids and doc_id not in src_ids:
                src_ids.append(doc_id)
    if not src_ids and fallback_id:
        src_ids = [fallback_id]
    return src_ids


def _compose_summary_lines(
    conclusion: dict[str, Any],
    evidences: list[dict[str, Any]],
    risks: list[dict[str, Any]],
    checkpoints: list[dict[str, Any]],
    sentiment: dict[str, Any],
) -> list[SummaryLine]:
    lines: list[SummaryLine] = []
    lines.append(SummaryLine(text=f"결론: {conclusion['text']}", source_doc_ids=list(conclusion["source_ids"])))

    for idx, item in enumerate(evidences[:2], start=1):
        lines.append(SummaryLine(text=f"근거{idx}: {item['text']}", source_doc_ids=list(item["source_ids"])))
    for idx, item in enumerate(risks[:2], start=1):
        lines.append(SummaryLine(text=f"리스크{idx}: {item['text']}", source_doc_ids=list(item["source_ids"])))
    for idx, item in enumerate(checkpoints[:2], start=1):
        lines.append(SummaryLine(text=f"체크포인트{idx}: {item['text']}", source_doc_ids=list(item["source_ids"])))

    sentiment_ko = _sentiment_label_ko(str(sentiment["label"]))
    conf = float(sentiment.get("confidence") or 0.5)
    final_sources = _collect_source_ids(lines[:3]) or _collect_source_ids(lines)
    lines.append(
        SummaryLine(
            text=f"최종판단: {sentiment_ko} (신뢰도 {conf:.2f})",
            source_doc_ids=final_sources,
        )
    )
    return lines[:8]


def _collect_source_ids(lines: list[SummaryLine]) -> list[int]:
    src: list[int] = []
    for line in lines:
        for doc_id in line.source_doc_ids:
            if doc_id not in src:
                src.append(doc_id)
    return src[:3]


def _sentiment_label_ko(label: str) -> str:
    if label == "positive":
        return "긍정"
    if label == "negative":
        return "부정"
    return "중립"


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
        pos += sum(1 for t in POSITIVE_TERMS if t.lower() in text)
        neg += sum(1 for t in NEGATIVE_TERMS if t.lower() in text)
    if pos > neg:
        return "positive", round(min(0.55 + (pos - neg) * 0.04, 0.9), 2)
    if neg > pos:
        return "negative", round(min(0.55 + (neg - pos) * 0.04, 0.9), 2)
    return "neutral", 0.5


def _find_by_terms(docs: list[dict], terms: tuple[str, ...]) -> dict | None:
    for d in docs:
        text = f"{d.get('title', '')} {d.get('body', '')}".lower()
        if any(term.lower() in text for term in terms):
            return d
    return None


def _fact_sentence(doc: dict) -> str:
    body = compact_text(str(doc.get("body") or ""))
    title = compact_text(str(doc.get("title") or ""))
    text = body or title
    if not text:
        return "핵심 사실을 판단하기에 본문 정보가 부족합니다."

    if title and text.startswith(title):
        text = compact_text(text[len(title) :]) or text

    parts = re.split(r"(?<=[.!?다])\s+", text)
    for p in parts:
        s = _clean_text(p)
        if len(s) >= 16:
            return s
    return _clean_text(text)


def _clean_text(text: str) -> str:
    value = compact_text(text)
    if not value:
        return ""
    value = re.sub(r"\[[^\]]{1,60}\]", "", value).strip()
    value = compact_text(value)
    if len(value) > 220:
        value = value[:220].rstrip()
    return value
