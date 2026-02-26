from __future__ import annotations

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
from stock_mvp.storage import evidence_repo, item_summary_repo
from stock_mvp.utils import compact_text, parse_datetime_maybe, url_hash


class ItemSummarizerAgent:
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
                summary_text = self._build_short_summary(item_id=item_id, card=card)
                item_summary_repo.upsert_item_summary(conn, item_id=item_id, short_summary=summary_text)
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
