from __future__ import annotations

import json
import uuid
from typing import Any

from stock_mvp.agents.base import (
    AgentStats,
    confidence_weight,
    detect_risk_note,
    ensure_hedged_interpretation,
    ensure_risk_note,
    extract_topics,
    has_fact_anchor,
    source_type_from_item,
    split_sentences,
)
from stock_mvp.config import Settings, load_settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.storage import evidence_repo, item_summary_repo
from stock_mvp.utils import compact_text, parse_datetime_maybe, url_hash


class ItemSummarizerAgent:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or load_settings()
        self.llm = LLMClient(self.settings)

    def run(
        self,
        conn,
        *,
        market: str,
        ticker_codes: list[str] | None,
        lookback_days: int = 14,
        limit: int = 500,
    ) -> AgentStats:
        rows = item_summary_repo.list_pending_items(
            conn,
            market=market.lower(),
            ticker_codes=ticker_codes,
            lookback_days=lookback_days,
            top_n_per_stock=max(1, int(self.settings.summary_top_n_per_stock)),
            min_relevance=float(self.settings.summary_min_relevance),
            limit=limit,
        )

        created = 0
        skipped = 0
        errors = 0

        for row in rows:
            item_id = int(row["item_id"])
            try:
                card = evidence_repo.get_card_by_item_id(conn, item_id=item_id)
                if card is None:
                    card = self._build_or_reuse_card(conn, row)
                    evidence_repo.upsert_card(conn, card)

                llm_item = self._build_item_summary_with_llm(item_id=item_id, row=row, card=card)
                if llm_item is not None:
                    summary_text = llm_item["short_summary"]
                    impact_label = llm_item["impact_label"]
                    feed_one_liner = llm_item["feed_one_liner"]
                    detail_bullets = llm_item["detail_bullets"]
                else:
                    summary_text = self._build_short_summary(item_id=item_id, card=card)
                    impact_label = self._detect_impact_label(row=row, card=card)
                    feed_one_liner = self._build_feed_one_liner(row=row, card=card, impact_label=impact_label)
                    detail_bullets = self._build_detail_bullets(card=card)

                related_refs = self._build_related_refs(row=row, card=card)
                item_summary_repo.upsert_item_summary(
                    conn,
                    item_id=item_id,
                    short_summary=summary_text,
                    impact_label=impact_label,
                    feed_one_liner=feed_one_liner,
                    detail_bullets=detail_bullets,
                    related_refs=related_refs,
                )
                created += 1
            except Exception as exc:
                errors += 1
                print(f"[WARN] item_summarizer failed: item_id={item_id} error={exc}")
                continue
            if created >= limit:
                break

        conn.commit()
        return AgentStats(total=len(rows), created=created, skipped=skipped, errors=errors)

    def _build_or_reuse_card(self, conn, row: dict[str, Any]) -> dict[str, Any]:
        market = str(row.get("market") or "").lower()
        source = str(row.get("source") or "")
        doc_type = str(row.get("doc_type") or "")
        source_type = source_type_from_item(source, doc_type)
        source_url_hash = str(row.get("url_hash") or "") or url_hash(str(row.get("url") or ""))

        reusable = evidence_repo.find_card_by_source_url_hash(
            conn,
            source_url_hash=source_url_hash,
            market=market,
            entity_type="ticker",
        )
        if reusable:
            cloned = dict(reusable)
            cloned["card_id"] = str(uuid.uuid4())
            cloned["item_id"] = int(row["item_id"])
            cloned["entity_type"] = "ticker"
            cloned["entity_id"] = str(row["stock_code"])
            cloned["market"] = market
            cloned["source_type"] = source_type
            cloned["source_name"] = source
            cloned["url"] = str(row.get("url") or "")
            cloned["source_url_hash"] = source_url_hash
            cloned["published_at"] = str(row.get("published_at") or "")
            return self._validate_card(cloned, entity_hint=str(row.get("stock_name") or row.get("stock_code") or ""))

        llm_card = self._build_card_with_llm(row=row, source_type=source_type, source_url_hash=source_url_hash)
        if llm_card is not None:
            return self._validate_card(llm_card, entity_hint=str(row.get("stock_name") or row.get("stock_code") or ""))

        facts = self._extract_facts(row)
        interpretation = self._build_interpretation(row, facts=facts)
        risk_note = ensure_risk_note(detect_risk_note(str(row.get("title") or ""), str(row.get("body") or "")))
        topics = extract_topics(str(row.get("title") or ""), str(row.get("body") or ""))
        card = {
            "card_id": str(uuid.uuid4()),
            "item_id": int(row["item_id"]),
            "entity_type": "ticker",
            "entity_id": str(row["stock_code"]),
            "market": market,
            "source_type": source_type,
            "source_name": source,
            "url": str(row.get("url") or ""),
            "source_url_hash": source_url_hash,
            "published_at": str(row.get("published_at") or ""),
            "fact_headline": facts[0],
            "facts": facts,
            "interpretation": interpretation,
            "risk_note": risk_note,
            "topics": topics,
            "confidence_weight": confidence_weight(source_type),
        }
        return self._validate_card(card, entity_hint=str(row.get("stock_name") or row.get("stock_code") or ""))

    def _build_card_with_llm(
        self,
        *,
        row: dict[str, Any],
        source_type: str,
        source_url_hash: str,
    ) -> dict[str, Any] | None:
        if not self.llm.enabled():
            return None

        entity_hint = str(row.get("stock_name") or row.get("stock_code") or "")
        result = self.llm.generate_json(
            system_prompt=_item_card_system_prompt(),
            user_prompt=_item_card_user_prompt(row=row),
            purpose="item_card",
        )
        if result is None:
            return None

        parsed = self._parse_llm_card_payload(
            payload=result.payload,
            entity_hint=entity_hint,
            fallback_date=str(row.get("published_at") or ""),
        )
        if parsed is None:
            print(
                f"[WARN] item_summarizer llm invalid card payload: item_id={int(row['item_id'])} "
                f"keys={list(result.payload.keys())[:8]}"
            )
            return None

        return {
            "card_id": str(uuid.uuid4()),
            "item_id": int(row["item_id"]),
            "entity_type": "ticker",
            "entity_id": str(row["stock_code"]),
            "market": str(row.get("market") or "").lower(),
            "source_type": source_type,
            "source_name": str(row.get("source") or ""),
            "url": str(row.get("url") or ""),
            "source_url_hash": source_url_hash,
            "published_at": str(row.get("published_at") or ""),
            "fact_headline": parsed["fact_headline"],
            "facts": parsed["facts"],
            "interpretation": parsed["interpretation"],
            "risk_note": parsed["risk_note"],
            "topics": parsed["topics"],
            "confidence_weight": confidence_weight(source_type),
        }

    def _parse_llm_card_payload(
        self,
        *,
        payload: dict[str, Any],
        entity_hint: str,
        fallback_date: str,
    ) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None

        fact_headline = compact_text(str(payload.get("fact_headline") or payload.get("headline") or ""))
        raw_facts = payload.get("facts")
        facts: list[str] = []
        if isinstance(raw_facts, list):
            for x in raw_facts:
                text = compact_text(str(x))
                if text and text not in facts:
                    facts.append(text[:220])
                if len(facts) >= 3:
                    break
        elif isinstance(raw_facts, str):
            text = compact_text(raw_facts)
            if text:
                facts.append(text[:220])

        if not facts and fact_headline:
            facts.append(fact_headline)
        if not facts:
            date_hint = parse_datetime_maybe(fallback_date)
            day = date_hint.date().isoformat() if date_hint else "recent"
            facts = [f"{entity_hint} source confirms an update around {day}."]

        if not any(has_fact_anchor(f, entity_hint=entity_hint) for f in facts):
            facts.insert(0, f"{entity_hint} disclosed a source-grounded update around {fallback_date[:10] or 'recent'}.")
        facts = facts[:3]

        interpretation = ensure_hedged_interpretation(str(payload.get("interpretation") or ""))
        risk_note = ensure_risk_note(str(payload.get("risk_note") or ""))

        raw_topics = payload.get("topics")
        topics: list[str] = []
        if isinstance(raw_topics, list):
            for t in raw_topics:
                topic = compact_text(str(t)).lower()
                if topic and topic not in topics:
                    topics.append(topic)
                if len(topics) >= 6:
                    break
        if not topics:
            topics = extract_topics(" ".join(facts), interpretation, risk_note)

        headline = fact_headline or facts[0]
        return {
            "fact_headline": headline,
            "facts": facts,
            "interpretation": interpretation,
            "risk_note": risk_note,
            "topics": topics,
        }

    def _extract_facts(self, row: dict[str, Any]) -> list[str]:
        title = compact_text(str(row.get("title") or ""))
        body = compact_text(str(row.get("body") or ""))
        entity_hint = compact_text(str(row.get("stock_name") or row.get("stock_code") or ""))

        candidates = split_sentences(f"{title}. {body}", max_len=220)
        facts: list[str] = []
        seen: set[str] = set()
        for sentence in candidates:
            if len(sentence) < 12:
                continue
            if sentence in seen:
                continue
            if has_fact_anchor(sentence, entity_hint=entity_hint):
                facts.append(sentence)
                seen.add(sentence)
            if len(facts) >= 3:
                break

        if not facts:
            published = str(row.get("published_at") or "")
            parsed = parse_datetime_maybe(published)
            date_text = parsed.date().isoformat() if parsed else "recent"
            fallback = f"{entity_hint} source item confirms a factual update around {date_text}."
            facts.append(fallback)

        while len(facts) < 3:
            facts.append(facts[-1])
        return facts[:3]

    def _build_interpretation(self, row: dict[str, Any], *, facts: list[str]) -> str:
        source = source_type_from_item(str(row.get("source") or ""), str(row.get("doc_type") or ""))
        topics = extract_topics(str(row.get("title") or ""), str(row.get("body") or ""))
        topic_head = topics[0] if topics else "general"
        base = (
            f"Combined {source} facts may suggest a short-term shift in {topic_head}, "
            "but confidence can change with later updates."
        )
        if facts:
            base = f"Given the lead fact '{facts[0]}', the {topic_head} signal may be evolving."
        return ensure_hedged_interpretation(base)

    def _validate_card(self, card: dict[str, Any], *, entity_hint: str) -> dict[str, Any]:
        facts = [compact_text(str(x)) for x in list(card.get("facts") or []) if compact_text(str(x))]
        if not facts:
            facts = [f"{entity_hint} source item confirms factual content."]
        if not any(has_fact_anchor(f, entity_hint=entity_hint) for f in facts):
            published_at = compact_text(str(card.get("published_at") or ""))
            extra = f"{entity_hint} fact was observed around {published_at[:10] or 'recent'} by source text."
            facts.insert(0, extra)
        card["facts"] = facts[:3]
        card["fact_headline"] = compact_text(str(card.get("fact_headline") or "")) or card["facts"][0]
        card["interpretation"] = ensure_hedged_interpretation(str(card.get("interpretation") or ""))
        card["risk_note"] = ensure_risk_note(str(card.get("risk_note") or ""))
        topics = [compact_text(str(t)).lower() for t in list(card.get("topics") or []) if compact_text(str(t))]
        card["topics"] = topics[:6] if topics else ["general"]
        return card

    def _build_short_summary(self, *, item_id: int, card: dict[str, Any]) -> str:
        item_ref = f"ITEM-{item_id}"
        facts = list(card.get("facts") or [])
        while len(facts) < 3:
            facts.append(facts[-1] if facts else "Fact confirmation requires source review.")
        lines = [
            f"[FACT] {facts[0]} (src: {item_ref})",
            f"[FACT] {facts[1]} (src: {item_ref})",
            f"[FACT] {facts[2]} (src: {item_ref})",
            f"[INTERPRETATION] {card.get('interpretation', '')} (src: {item_ref})",
            f"[RISK] {card.get('risk_note', 'No explicit risk statement in source.')} (src: {item_ref})",
        ]
        return "\n".join(lines[:5])

    def _build_item_summary_with_llm(self, *, item_id: int, row: dict[str, Any], card: dict[str, Any]) -> dict[str, Any] | None:
        if not self.llm.enabled():
            return None
        result = self.llm.generate_json(
            system_prompt=_item_summary_system_prompt(),
            user_prompt=_item_summary_user_prompt(item_id=item_id, row=row, card=card),
            purpose="item_summary",
        )
        if result is None:
            return None
        parsed = self._parse_item_summary_payload(payload=result.payload, item_id=item_id, card=card, row=row)
        if parsed is None:
            print(
                f"[WARN] item_summarizer llm invalid summary payload: item_id={item_id} "
                f"keys={list(result.payload.keys())[:8]}"
            )
            return None
        return parsed

    def _parse_item_summary_payload(
        self,
        *,
        payload: dict[str, Any],
        item_id: int,
        card: dict[str, Any],
        row: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None

        fact_lines = _read_string_list(payload.get("fact_lines"), limit=3)
        if not fact_lines:
            fact_lines = [compact_text(str(x)) for x in list(card.get("facts") or []) if compact_text(str(x))]
        if not fact_lines:
            fact_lines = [compact_text(str(card.get("fact_headline") or row.get("title") or ""))]
        fact_lines = [line[:220] for line in fact_lines if line][:3]
        if not fact_lines:
            return None
        while len(fact_lines) < 3:
            fact_lines.append(fact_lines[-1])

        interpretation = ensure_hedged_interpretation(
            str(payload.get("interpretation") or payload.get("insight") or card.get("interpretation") or "")
        )
        risk_note = ensure_risk_note(str(payload.get("risk_note") or payload.get("risk") or card.get("risk_note") or ""))

        impact_label = compact_text(str(payload.get("impact_label") or "")).lower()
        if impact_label not in {"positive", "negative", "neutral"}:
            impact_label = self._detect_impact_label(row=row, card=card)

        feed_one_liner = compact_text(str(payload.get("feed_one_liner") or payload.get("one_liner") or ""))
        if not feed_one_liner:
            feed_one_liner = self._build_feed_one_liner(row=row, card=card, impact_label=impact_label)

        detail_bullets = _read_string_list(payload.get("detail_bullets"), limit=5)
        if len(detail_bullets) < 3:
            detail_bullets = self._build_detail_bullets(card=card)

        item_ref = f"ITEM-{item_id}"
        short_summary = "\n".join(
            [
                f"[FACT] {fact_lines[0]} (src: {item_ref})",
                f"[FACT] {fact_lines[1]} (src: {item_ref})",
                f"[FACT] {fact_lines[2]} (src: {item_ref})",
                f"[INTERPRETATION] {interpretation} (src: {item_ref})",
                f"[RISK] {risk_note} (src: {item_ref})",
            ]
        )
        return {
            "short_summary": short_summary,
            "impact_label": impact_label,
            "feed_one_liner": feed_one_liner,
            "detail_bullets": detail_bullets[:5],
        }

    def _build_detail_bullets(self, *, card: dict[str, Any]) -> list[str]:
        facts = [compact_text(str(x)) for x in list(card.get("facts") or []) if compact_text(str(x))]
        interpretation = ensure_hedged_interpretation(str(card.get("interpretation") or ""))
        risk = ensure_risk_note(str(card.get("risk_note") or ""))
        bullets: list[str] = []
        for fact in facts[:3]:
            bullets.append(f"{fact}")
        bullets.append(f"해석: {interpretation}")
        bullets.append(f"리스크: {risk}")
        return bullets[:5]

    def _build_related_refs(self, *, row: dict[str, Any], card: dict[str, Any]) -> list[dict[str, Any]]:
        item_id = int(row["item_id"])
        title = compact_text(str(row.get("title") or ""))
        url = str(row.get("url") or "")
        published_at = str(row.get("published_at") or row.get("collected_at") or "")
        return [
            {
                "item_id": item_id,
                "card_id": str(card.get("card_id") or ""),
                "title": title,
                "url": url,
                "published_at": published_at,
                "source": str(row.get("source") or ""),
            }
        ]

    def _detect_impact_label(self, *, row: dict[str, Any], card: dict[str, Any]) -> str:
        text = " ".join(
            [
                str(row.get("title") or ""),
                str(row.get("body") or ""),
                str(card.get("fact_headline") or ""),
                str(card.get("interpretation") or ""),
                str(card.get("risk_note") or ""),
            ]
        ).lower()
        negative_keywords = (
            "risk",
            "downside",
            "decline",
            "drop",
            "downgrade",
            "lawsuit",
            "delay",
            "하락",
            "악재",
            "리스크",
            "부진",
            "감소",
        )
        positive_keywords = (
            "beat",
            "upgrade",
            "growth",
            "record",
            "surge",
            "strong",
            "상승",
            "호재",
            "개선",
            "증가",
            "확대",
            "신규 수주",
        )
        has_negative = any(keyword in text for keyword in negative_keywords)
        has_positive = any(keyword in text for keyword in positive_keywords)
        if has_negative and not has_positive:
            return "negative"
        if has_positive and not has_negative:
            return "positive"
        return "neutral"

    def _build_feed_one_liner(self, *, row: dict[str, Any], card: dict[str, Any], impact_label: str) -> str:
        lead = compact_text(str(card.get("fact_headline") or ""))
        if not lead:
            facts = list(card.get("facts") or [])
            lead = compact_text(str(facts[0] if facts else row.get("title") or ""))
        lead = lead[:120]
        suffix = {
            "positive": "긍정 신호로 해석될 수 있습니다.",
            "negative": "단기 변동성 확대 가능성이 있습니다.",
            "neutral": "추가 확인이 필요한 이슈입니다.",
        }.get(impact_label, "추가 확인이 필요한 이슈입니다.")
        return f"{lead} {suffix}".strip()


def _item_card_system_prompt() -> str:
    return (
        "You are an evidence extraction assistant for equity research. Return JSON only. "
        "Required keys: fact_headline, facts, interpretation, risk_note, topics. "
        "facts must be 1~3 items and should contain at least one concrete anchor "
        "(number/date/entity) when possible. "
        "interpretation must be hedged (may/could/possibility). "
        "risk_note must not be empty."
    )


def _item_card_user_prompt(*, row: dict[str, Any]) -> str:
    payload = {
        "item_id": int(row["item_id"]),
        "stock_code": str(row.get("stock_code") or ""),
        "stock_name": str(row.get("stock_name") or ""),
        "market": str(row.get("market") or ""),
        "source": str(row.get("source") or ""),
        "doc_type": str(row.get("doc_type") or ""),
        "published_at": str(row.get("published_at") or ""),
        "title": compact_text(str(row.get("title") or ""))[:200],
        "body_snippet": compact_text(str(row.get("body") or ""))[:1600],
        "url": str(row.get("url") or ""),
    }
    return f"Build evidence card from this item:\n{json.dumps(payload, ensure_ascii=False)}"


def _item_summary_system_prompt() -> str:
    return (
        "You are a Korean stock briefing writer for beginners. Return JSON only. "
        "Required keys: fact_lines, interpretation, risk_note, impact_label, feed_one_liner, detail_bullets. "
        "fact_lines should be 2~3 concise factual sentences, paraphrased after understanding the text. "
        "impact_label must be one of positive|neutral|negative. "
        "Do not output investment recommendation."
    )


def _item_summary_user_prompt(*, item_id: int, row: dict[str, Any], card: dict[str, Any]) -> str:
    payload = {
        "item_id": item_id,
        "stock_code": str(row.get("stock_code") or ""),
        "stock_name": str(row.get("stock_name") or ""),
        "source": str(row.get("source") or ""),
        "title": compact_text(str(row.get("title") or ""))[:180],
        "body_snippet": compact_text(str(row.get("body") or ""))[:1200],
        "card": {
            "fact_headline": str(card.get("fact_headline") or ""),
            "facts": list(card.get("facts") or []),
            "interpretation": str(card.get("interpretation") or ""),
            "risk_note": str(card.get("risk_note") or ""),
            "topics": list(card.get("topics") or []),
        },
    }
    return f"Generate item summary JSON from:\n{json.dumps(payload, ensure_ascii=False)}"


def _read_string_list(raw: Any, *, limit: int) -> list[str]:
    if isinstance(raw, str):
        value = compact_text(raw)
        return [value] if value else []
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for x in raw:
        text = compact_text(str(x))
        if not text:
            continue
        out.append(text)
        if len(out) >= max(1, limit):
            break
    return out
