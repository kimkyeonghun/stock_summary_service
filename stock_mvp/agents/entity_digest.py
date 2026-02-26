from __future__ import annotations

import json
from typing import Any

from stock_mvp.agents.base import AgentStats, date_days_ago, iso_date_utc
from stock_mvp.storage import digest_repo, evidence_repo
from stock_mvp.utils import compact_text


class EntityDigestAgent:
    def run(
        self,
        conn,
        *,
        entity_type: str,
        entity_ids: list[str],
        market: str,
        digest_date: str | None = None,
        lookback_days: int = 7,
    ) -> AgentStats:
        date_text = digest_date or iso_date_utc()
        start_date = date_days_ago(date_text, max(1, lookback_days))
        created = 0
        errors = 0

        for entity_id in entity_ids:
            try:
                payload = self._build_one(
                    conn,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    market=market,
                    start_date=start_date,
                    end_date=date_text,
                )
                digest_repo.upsert_daily_digest(
                    conn,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    market=market,
                    digest_date=date_text,
                    summary_8line=payload["summary_8line"],
                    change_3=payload["change_3"],
                    open_questions=payload["open_questions"],
                    refs=payload["refs"],
                )
                created += 1
            except Exception as exc:
                errors += 1
                print(
                    f"[WARN] entity_digest failed: entity_type={entity_type} "
                    f"entity_id={entity_id} market={market} error={exc}"
                )

        conn.commit()
        return AgentStats(total=len(entity_ids), created=created, errors=errors)

    def _build_one(
        self,
        conn,
        *,
        entity_type: str,
        entity_id: str,
        market: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        cards = self._list_cards(
            conn,
            entity_type=entity_type,
            entity_id=entity_id,
            market=market,
            start_date=start_date,
            end_date=end_date,
        )
        aliases = self._make_aliases(cards)
        refs = [{"alias": aliases[c["card_id"]], "card_id": c["card_id"], "item_id": c["item_id"]} for c in cards[:20]]

        summary_lines = self._build_8_lines(cards, aliases)
        previous = digest_repo.get_previous_digest(
            conn,
            entity_type=entity_type,
            entity_id=entity_id,
            market=market,
            digest_date=end_date,
        )
        change_3 = self._build_change_3(cards, aliases, previous)
        open_q = self._build_open_questions(cards, aliases)
        return {
            "summary_8line": "\n".join(summary_lines),
            "change_3": change_3,
            "open_questions": open_q,
            "refs": refs,
        }

    def _list_cards(
        self,
        conn,
        *,
        entity_type: str,
        entity_id: str,
        market: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        if entity_type == "sector":
            cards = evidence_repo.list_cards_for_sector(
                conn,
                sector_code=entity_id,
                market=market,
                start_date=start_date,
                end_date=end_date,
                limit=240,
            )
        else:
            cards = evidence_repo.list_cards_for_ticker(
                conn,
                ticker=entity_id,
                market=market,
                start_date=start_date,
                end_date=end_date,
                limit=200,
            )

        # Daily digest default scope is news + research.
        return [c for c in cards if str(c.get("source_type") or "") in {"news", "research"}]

    @staticmethod
    def _make_aliases(cards: list[dict[str, Any]]) -> dict[str, str]:
        aliases: dict[str, str] = {}
        for idx, card in enumerate(cards, start=1):
            aliases[card["card_id"]] = f"C{idx}"
        return aliases

    def _build_8_lines(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> list[str]:
        if not cards:
            return [
                "1) [Core Fact] No material fact available (cards: -)",
                "2) [Core Fact] No material fact available (cards: -)",
                "3) [Earnings/Numbers] No material number update (cards: -)",
                "4) [Demand/Sector] No material sector demand update (cards: -)",
                "5) [Interpretation] Limited evidence may imply a neutral stance (cards: -)",
                "6) [Interpretation] Additional data could change interpretation (cards: -)",
                "7) [Risk] No explicit risk statement in source. (cards: -)",
                "8) [Bottom line] Data remains insufficient for a stronger conclusion (cards: -)",
            ]

        def fact(idx: int, fallback: str) -> tuple[str, str]:
            if idx < len(cards):
                card = cards[idx]
                facts = list(card.get("facts") or [])
                text = compact_text(facts[0] if facts else card.get("fact_headline") or fallback)
                return text, aliases[card["card_id"]]
            return fallback, "-"

        f1, c1 = fact(0, "No recent core fact update")
        f2, c2 = fact(1, "No additional core fact update")
        num_fact, c3 = self._pick_number_fact(cards, aliases)
        demand_fact, c4 = self._pick_topic_fact(cards, aliases, topic="demand")
        interp1, c5 = self._pick_interpretation(cards, aliases, idx=0)
        interp2, c6 = self._pick_interpretation(cards, aliases, idx=1)
        risk, c7 = self._pick_risk(cards, aliases)
        bottom = "Facts are constructive, but interpretation should remain provisional."

        return [
            f"1) [Core Fact] {f1} (cards: {c1})",
            f"2) [Core Fact] {f2} (cards: {c2})",
            f"3) [Earnings/Numbers] {num_fact} (cards: {c3})",
            f"4) [Demand/Sector] {demand_fact} (cards: {c4})",
            f"5) [Interpretation] {interp1} (cards: {c5})",
            f"6) [Interpretation] {interp2} (cards: {c6})",
            f"7) [Risk] {risk} (cards: {c7})",
            f"8) [Bottom line] {bottom} (cards: {c1},{c2})",
        ]

    def _pick_number_fact(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            for fact in card.get("facts") or []:
                if any(ch.isdigit() for ch in str(fact)):
                    return compact_text(str(fact)), aliases[card["card_id"]]
        return "Numeric update is currently limited", "-"

    def _pick_topic_fact(
        self,
        cards: list[dict[str, Any]],
        aliases: dict[str, str],
        *,
        topic: str,
    ) -> tuple[str, str]:
        for card in cards:
            topics = [str(x) for x in card.get("topics") or []]
            if topic in topics or (topic == "demand" and "supply_chain" in topics):
                facts = list(card.get("facts") or [])
                return compact_text(str(facts[0] if facts else card.get("fact_headline") or "")), aliases[card["card_id"]]
        return "No clear demand or sector signal is confirmed", "-"

    def _pick_interpretation(self, cards: list[dict[str, Any]], aliases: dict[str, str], *, idx: int) -> tuple[str, str]:
        if idx < len(cards):
            card = cards[idx]
            return compact_text(str(card.get("interpretation") or "Interpretation is limited by available evidence.")), aliases[card["card_id"]]
        return "Interpretation remains weak due to limited evidence", "-"

    def _pick_risk(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            risk = compact_text(str(card.get("risk_note") or ""))
            if risk:
                return risk, aliases[card["card_id"]]
        return "No explicit risk statement in source.", "-"

    def _build_change_3(
        self,
        cards: list[dict[str, Any]],
        aliases: dict[str, str],
        previous: dict[str, Any] | None,
    ) -> str:
        if not cards:
            return "No material change"

        previous_card_ids: set[str] = set()
        if previous:
            for ref in previous.get("refs") or []:
                card_id = str(ref.get("card_id") or "")
                if card_id:
                    previous_card_ids.add(card_id)

        changes: list[str] = []
        for card in cards:
            if card["card_id"] in previous_card_ids:
                continue
            headline = compact_text(str(card.get("fact_headline") or ""))
            if not headline:
                continue
            sign = "+"
            if any(k in headline.lower() for k in ("risk", "downgrade", "decline", "weak", "cut")):
                sign = "-"
            changes.append(f"{sign} {headline} (cards: {aliases.get(card['card_id'], '-')})")
            if len(changes) >= 3:
                break

        if not changes:
            return "No material change"
        return "\n".join(changes)

    def _build_open_questions(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> str:
        if not cards:
            return "Q1) Additional evidence is needed\nQ2) Additional evidence is needed"
        top = cards[0]
        first_alias = aliases.get(top["card_id"], "-")
        topics = [str(x) for x in top.get("topics") or []]
        topic = topics[0] if topics else "core topic"
        q1 = f"Q1) Can the {topic} signal be confirmed by the next disclosure or research update? (cards: {first_alias})"
        q2 = f"Q2) How material is the current risk factor to the next period outcome? (cards: {first_alias})"
        return f"{q1}\n{q2}"


def parse_digest_refs(refs_json_or_list: Any) -> list[dict[str, Any]]:
    if isinstance(refs_json_or_list, list):
        return refs_json_or_list
    if isinstance(refs_json_or_list, str):
        try:
            parsed = json.loads(refs_json_or_list)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []
