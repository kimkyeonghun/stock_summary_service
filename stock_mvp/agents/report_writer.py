from __future__ import annotations

from collections import Counter

from stock_mvp.agents.base import AgentStats, date_days_ago, iso_date_utc
from stock_mvp.storage import digest_repo, evidence_repo, report_repo
from stock_mvp.utils import compact_text


class ReportWriterAgent:
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
        refs = [{"card_id": card["card_id"], "item_id": card["item_id"]} for card in top_cards]
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
