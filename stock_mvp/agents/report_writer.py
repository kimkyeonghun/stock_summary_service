from __future__ import annotations

import json
from collections import Counter
from typing import Any

from stock_mvp.agents.base import AgentStats, date_days_ago, iso_date_utc
from stock_mvp.config import Settings, load_settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.storage import digest_repo, evidence_repo, report_repo
from stock_mvp.utils import compact_text


class ReportWriterAgent:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or load_settings()
        self.llm = LLMClient(self.settings)

    def run(
        self,
        conn,
        *,
        entity_type: str,
        entity_ids: list[str],
        market: str,
        end_date: str | None = None,
        lookback_days: int = 14,
    ) -> AgentStats:
        end = end_date or iso_date_utc()
        start = date_days_ago(end, max(1, lookback_days))
        created = 0
        skipped = 0
        errors = 0

        for entity_id in entity_ids:
            try:
                cards = self._list_cards(
                    conn,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    market=market,
                    start_date=start,
                    end_date=end,
                )
                if not self._can_generate(cards):
                    skipped += 1
                    continue
                report_md, refs = self._build_report(
                    conn,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    market=market,
                    start_date=start,
                    end_date=end,
                    cards=cards,
                )
                report_repo.upsert_agent_report(
                    conn,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    market=market,
                    period_start=start,
                    period_end=end,
                    report_md=report_md,
                    refs=refs,
                )
                created += 1
            except Exception as exc:
                errors += 1
                print(
                    f"[WARN] report_writer failed: entity_type={entity_type} "
                    f"entity_id={entity_id} market={market} error={exc}"
                )

        conn.commit()
        return AgentStats(total=len(entity_ids), created=created, skipped=skipped, errors=errors)

    def _list_cards(
        self,
        conn,
        *,
        entity_type: str,
        entity_id: str,
        market: str,
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        if entity_type == "sector":
            return evidence_repo.list_cards_for_sector(
                conn,
                sector_code=entity_id,
                market=market,
                start_date=start_date,
                end_date=end_date,
                limit=320,
            )
        return evidence_repo.list_cards_for_ticker(
            conn,
            ticker=entity_id,
            market=market,
            start_date=start_date,
            end_date=end_date,
            limit=260,
        )

    @staticmethod
    def _can_generate(cards: list[dict]) -> bool:
        if len(cards) < 12:
            return False
        source_counter = Counter(str(c.get("source_type") or "") for c in cards)
        if source_counter.get("research", 0) < 2 and source_counter.get("filing", 0) < 1:
            return False
        topics: set[str] = set()
        for card in cards:
            for topic in card.get("topics") or []:
                if topic:
                    topics.add(str(topic))
        return len(topics) >= 2

    def _build_report(
        self,
        conn,
        *,
        entity_type: str,
        entity_id: str,
        market: str,
        start_date: str,
        end_date: str,
        cards: list[dict],
    ) -> tuple[str, list[dict]]:
        top_cards = cards[:10]
        source_counter = Counter(str(card.get("source_type") or "") for card in cards)
        topic_counter = Counter()
        for card in cards:
            for topic in card.get("topics") or []:
                topic_counter[str(topic)] += 1

        latest_digest = digest_repo.get_latest_digest(
            conn,
            entity_type=entity_type,
            entity_id=entity_id,
            market=market,
        )
        changed_block = latest_digest["change_3"] if latest_digest else "No material change"

        llm_result = self._build_with_llm(
            entity_type=entity_type,
            entity_id=entity_id,
            market=market,
            start_date=start_date,
            end_date=end_date,
            cards=cards,
            changed_block=changed_block,
        )
        if llm_result is not None:
            return llm_result

        refs = [{"card_id": card["card_id"], "item_id": card["item_id"]} for card in top_cards]
        top_topics = [name for name, _count in topic_counter.most_common(3)] or ["general"]
        exec_bullets = [
            f"- Evidence volume reached {len(cards)} cards across {len(top_topics)} major topics.",
            f"- Source mix: research {source_counter.get('research',0)}, filing {source_counter.get('filing',0)}, news {source_counter.get('news',0)}.",
            "- Signal quality may improve as additional filing and research evidence accumulates.",
        ]
        thesis = [
            f"- {compact_text(top_cards[0].get('fact_headline') or '')}",
            f"- Topic concentration indicates {top_topics[0]} remains a key near-term driver.",
            "- Current interpretation suggests direction may change with the next update.",
        ]
        bear_case = [
            f"- {compact_text(top_cards[0].get('risk_note') or 'No explicit risk statement in source.')}",
            "- Heavy dependence on a limited source set may bias interpretation.",
            "- Macro or regulation shifts could break prior fact patterns.",
        ]
        key_evidence = [f"- [{card['card_id']}] {compact_text(card.get('fact_headline') or '')}" for card in top_cards[:10]]
        what_to_watch = [
            f"- Next confirmation on {top_topics[0]} trend.",
            "- Follow-up from higher-confidence sources (research/filing).",
            "- Risk disclosure follow-through in subsequent items.",
        ]
        uncertainties = [
            "- Long-term impact estimate is limited by sample length.",
            "- Counterfactual scenarios are under-specified in source texts.",
            "- Causality between demand and numbers needs more disclosures.",
        ]

        report_md = "\n".join(
            [
                f"# Agent Report: {entity_type}/{entity_id} ({market.upper()})",
                f"- Period: {start_date} to {end_date}",
                "",
                "## Executive Summary",
                *exec_bullets,
                "",
                "## Thesis",
                *thesis,
                "",
                "## Bear Case",
                *bear_case,
                "",
                "## Key Evidence",
                *key_evidence[:10],
                "",
                "## What Changed Recently",
                changed_block,
                "",
                "## What to Watch",
                *what_to_watch,
                "",
                "## Uncertainties",
                *uncertainties,
            ]
        )
        return report_md, refs

    def _build_with_llm(
        self,
        *,
        entity_type: str,
        entity_id: str,
        market: str,
        start_date: str,
        end_date: str,
        cards: list[dict[str, Any]],
        changed_block: str,
    ) -> tuple[str, list[dict[str, Any]]] | None:
        if not self.llm.enabled():
            return None

        result = self.llm.generate_json(
            system_prompt=_report_system_prompt(),
            user_prompt=_report_user_prompt(
                entity_type=entity_type,
                entity_id=entity_id,
                market=market,
                start_date=start_date,
                end_date=end_date,
                cards=cards,
                changed_block=changed_block,
            ),
            purpose="agent_report",
        )
        if result is None:
            return None

        parsed = _parse_report_payload(result.payload, cards=cards)
        if parsed is None:
            print(
                f"[WARN] report_writer llm invalid payload: entity_type={entity_type} "
                f"entity_id={entity_id} keys={list(result.payload.keys())[:8]}"
            )
            return None

        refs = [{"card_id": card["card_id"], "item_id": card["item_id"]} for card in parsed["evidence_cards"]]
        report_md = _compose_report_markdown(
            entity_type=entity_type,
            entity_id=entity_id,
            market=market,
            start_date=start_date,
            end_date=end_date,
            executive_summary=parsed["executive_summary"],
            thesis=parsed["thesis"],
            bear_case=parsed["bear_case"],
            evidence_cards=parsed["evidence_cards"],
            what_changed=parsed["what_changed"],
            what_to_watch=parsed["what_to_watch"],
            uncertainties=parsed["uncertainties"],
        )
        return report_md, refs


def _report_system_prompt() -> str:
    return (
        "You are a cautious equity analyst. Return JSON only. "
        "Required keys: executive_summary, thesis, bear_case, key_evidence_card_ids, "
        "what_changed, what_to_watch, uncertainties. "
        "Each section should contain concise bullet-style items. "
        "Do not provide buy/sell recommendations."
    )


def _report_user_prompt(
    *,
    entity_type: str,
    entity_id: str,
    market: str,
    start_date: str,
    end_date: str,
    cards: list[dict[str, Any]],
    changed_block: str,
) -> str:
    card_rows: list[dict[str, Any]] = []
    for card in cards[:50]:
        card_rows.append(
            {
                "card_id": str(card.get("card_id") or ""),
                "item_id": int(card.get("item_id") or 0),
                "source_type": str(card.get("source_type") or ""),
                "fact_headline": compact_text(str(card.get("fact_headline") or ""))[:180],
                "facts": [compact_text(str(x))[:180] for x in list(card.get("facts") or [])[:2]],
                "interpretation": compact_text(str(card.get("interpretation") or ""))[:180],
                "risk_note": compact_text(str(card.get("risk_note") or ""))[:180],
                "topics": [compact_text(str(x)) for x in list(card.get("topics") or [])[:4]],
                "published_at": str(card.get("published_at") or ""),
            }
        )
    payload = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "market": market,
        "period": {"start_date": start_date, "end_date": end_date},
        "cards": card_rows,
        "latest_change_3": changed_block,
    }
    return f"Generate report JSON from:\n{json.dumps(payload, ensure_ascii=False)}"


def _parse_report_payload(payload: dict[str, Any], *, cards: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    executive_summary = _read_bullets(payload.get("executive_summary"), min_items=3, max_items=3)
    thesis = _read_bullets(payload.get("thesis"), min_items=3, max_items=3)
    bear_case = _read_bullets(payload.get("bear_case"), min_items=3, max_items=3)
    what_changed = _read_bullets(payload.get("what_changed"), min_items=1, max_items=3)
    what_to_watch = _read_bullets(payload.get("what_to_watch"), min_items=3, max_items=3)
    uncertainties = _read_bullets(payload.get("uncertainties"), min_items=3, max_items=3)

    if not (executive_summary and thesis and bear_case and what_changed and what_to_watch and uncertainties):
        return None

    indexed = {str(card.get("card_id") or ""): card for card in cards}
    key_ids = payload.get("key_evidence_card_ids")
    if isinstance(key_ids, str):
        key_ids = [x for x in key_ids.replace(" ", "").split(",") if x]
    if not isinstance(key_ids, list):
        key_ids = []
    evidence_cards: list[dict[str, Any]] = []
    for x in key_ids:
        key = str(x)
        if key in indexed and indexed[key] not in evidence_cards:
            evidence_cards.append(indexed[key])
        if len(evidence_cards) >= 10:
            break
    if len(evidence_cards) < 5:
        evidence_cards = cards[: min(10, len(cards))]
    if not evidence_cards:
        return None

    return {
        "executive_summary": executive_summary,
        "thesis": thesis,
        "bear_case": bear_case,
        "evidence_cards": evidence_cards,
        "what_changed": what_changed,
        "what_to_watch": what_to_watch,
        "uncertainties": uncertainties,
    }


def _read_bullets(raw: Any, *, min_items: int, max_items: int) -> list[str]:
    if isinstance(raw, str):
        raw = [x for x in raw.split("\n") if compact_text(x)]
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for x in raw:
        text = compact_text(str(x))
        if not text:
            continue
        out.append(text[:220])
        if len(out) >= max(1, max_items):
            break
    if len(out) < min_items:
        return []
    return out


def _compose_report_markdown(
    *,
    entity_type: str,
    entity_id: str,
    market: str,
    start_date: str,
    end_date: str,
    executive_summary: list[str],
    thesis: list[str],
    bear_case: list[str],
    evidence_cards: list[dict[str, Any]],
    what_changed: list[str],
    what_to_watch: list[str],
    uncertainties: list[str],
) -> str:
    key_evidence: list[str] = []
    for card in evidence_cards[:10]:
        facts = list(card.get("facts") or [])
        headline = compact_text(str(card.get("fact_headline") or (facts[0] if facts else "")))
        key_evidence.append(f"- [{card['card_id']}] {headline}")
    return "\n".join(
        [
            f"# Agent Report: {entity_type}/{entity_id} ({market.upper()})",
            f"- Period: {start_date} to {end_date}",
            "",
            "## Executive Summary",
            *[f"- {x}" for x in executive_summary],
            "",
            "## Thesis",
            *[f"- {x}" for x in thesis],
            "",
            "## Bear Case",
            *[f"- {x}" for x in bear_case],
            "",
            "## Key Evidence",
            *key_evidence,
            "",
            "## What Changed Recently",
            *[f"- {x}" for x in what_changed],
            "",
            "## What to Watch",
            *[f"- {x}" for x in what_to_watch],
            "",
            "## Uncertainties",
            *[f"- {x}" for x in uncertainties],
        ]
    )
