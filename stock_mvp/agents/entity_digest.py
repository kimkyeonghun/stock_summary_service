from __future__ import annotations

import json
from typing import Any

from stock_mvp.agents.base import AgentStats, date_days_ago, iso_date_utc
from stock_mvp.config import Settings, load_settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.storage import digest_repo, evidence_repo
from stock_mvp.utils import compact_text


LINE_LABELS = [
    "[Core Fact]",
    "[Core Fact]",
    "[Earnings/Numbers]",
    "[Demand/Sector]",
    "[Interpretation]",
    "[Interpretation]",
    "[Risk]",
    "[Bottom line]",
]


class EntityDigestAgent:
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

        previous = digest_repo.get_previous_digest(
            conn,
            entity_type=entity_type,
            entity_id=entity_id,
            market=market,
            digest_date=end_date,
        )

        llm_payload = self._build_with_llm(
            entity_type=entity_type,
            entity_id=entity_id,
            market=market,
            start_date=start_date,
            end_date=end_date,
            cards=cards,
            aliases=aliases,
            previous=previous,
        )
        if llm_payload is not None:
            return {
                "summary_8line": llm_payload["summary_8line"],
                "change_3": llm_payload["change_3"],
                "open_questions": llm_payload["open_questions"],
                "refs": refs,
            }

        summary_lines = self._build_8_lines(cards, aliases)
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

        return [c for c in cards if str(c.get("source_type") or "") in {"news", "research"}]

    @staticmethod
    def _make_aliases(cards: list[dict[str, Any]]) -> dict[str, str]:
        aliases: dict[str, str] = {}
        for idx, card in enumerate(cards, start=1):
            aliases[card["card_id"]] = f"C{idx}"
        return aliases

    def _build_with_llm(
        self,
        *,
        entity_type: str,
        entity_id: str,
        market: str,
        start_date: str,
        end_date: str,
        cards: list[dict[str, Any]],
        aliases: dict[str, str],
        previous: dict[str, Any] | None,
    ) -> dict[str, str] | None:
        if not self.llm.enabled():
            return None

        result = self.llm.generate_json(
            system_prompt=_digest_system_prompt(),
            user_prompt=_digest_user_prompt(
                entity_type=entity_type,
                entity_id=entity_id,
                market=market,
                start_date=start_date,
                end_date=end_date,
                cards=cards,
                aliases=aliases,
                previous=previous,
            ),
            purpose="daily_digest",
        )
        if result is None:
            return None

        parsed = _parse_digest_payload(result.payload, alias_values=set(aliases.values()))
        if parsed is None:
            print(
                f"[WARN] entity_digest llm invalid payload: entity_type={entity_type} "
                f"entity_id={entity_id} keys={list(result.payload.keys())[:8]}"
            )
            return None

        summary_8line = self._compose_8line_from_payload(parsed["summary_lines"])
        change_3 = self._compose_change_from_payload(parsed["change_lines"])
        open_questions = self._compose_questions_from_payload(parsed["open_questions"])
        return {
            "summary_8line": summary_8line,
            "change_3": change_3,
            "open_questions": open_questions,
        }

    def _compose_8line_from_payload(self, lines: list[dict[str, Any]]) -> str:
        out: list[str] = []
        for idx, item in enumerate(lines[:8]):
            text = compact_text(str(item.get("text") or "")) or "No material update."
            cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
            card_text = ",".join(cards[:3]) if cards else "-"
            out.append(f"{idx + 1}) {LINE_LABELS[idx]} {text} (cards: {card_text})")
        while len(out) < 8:
            idx = len(out)
            out.append(f"{idx + 1}) {LINE_LABELS[idx]} No material update. (cards: -)")
        return "\n".join(out[:8])

    @staticmethod
    def _compose_change_from_payload(change_lines: list[dict[str, Any]]) -> str:
        if not change_lines:
            return "No material change"
        out: list[str] = []
        for item in change_lines[:3]:
            sign = compact_text(str(item.get("sign") or "+"))
            if sign not in {"+", "-"}:
                sign = "+"
            text = compact_text(str(item.get("text") or "No material change"))
            cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
            card_text = ",".join(cards[:3]) if cards else "-"
            out.append(f"{sign} {text} (cards: {card_text})")
        return "\n".join(out) if out else "No material change"

    @staticmethod
    def _compose_questions_from_payload(open_questions: list[dict[str, Any]]) -> str:
        out: list[str] = []
        for idx, item in enumerate(open_questions[:2], start=1):
            text = compact_text(str(item.get("text") or ""))
            if not text:
                continue
            cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
            card_text = ",".join(cards[:2]) if cards else "-"
            out.append(f"Q{idx}) {text} (cards: {card_text})")
        while len(out) < 2:
            idx = len(out) + 1
            out.append(f"Q{idx}) Additional evidence is needed (cards: -)")
        return "\n".join(out[:2])

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


def _digest_system_prompt() -> str:
    return (
        "You are a market digest writer for beginner investors. Return JSON only. "
        "Required keys: summary_lines, change_3, open_questions. "
        "summary_lines must be exactly 8 items. "
        "Each summary line item: {text, cards}. cards must contain aliases like C1,C2. "
        "change_3 should be up to 3 items with {sign,text,cards}. sign is '+' or '-'. "
        "open_questions should be exactly 2 items with {text,cards}. "
        "No investment recommendation language."
    )


def _digest_user_prompt(
    *,
    entity_type: str,
    entity_id: str,
    market: str,
    start_date: str,
    end_date: str,
    cards: list[dict[str, Any]],
    aliases: dict[str, str],
    previous: dict[str, Any] | None,
) -> str:
    card_rows: list[dict[str, Any]] = []
    for card in cards[:40]:
        card_rows.append(
            {
                "alias": aliases.get(card["card_id"], ""),
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
    previous_payload = {
        "summary_8line": str(previous.get("summary_8line") or "") if previous else "",
        "change_3": str(previous.get("change_3") or "") if previous else "",
    }
    payload = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "market": market,
        "period": {"start_date": start_date, "end_date": end_date},
        "line_labels": LINE_LABELS,
        "cards": card_rows,
        "previous_digest": previous_payload,
    }
    return f"Generate daily digest JSON from:\n{json.dumps(payload, ensure_ascii=False)}"


def _parse_digest_payload(payload: dict[str, Any], *, alias_values: set[str]) -> dict[str, list[dict[str, Any]]] | None:
    if not isinstance(payload, dict):
        return None

    summary_raw = payload.get("summary_lines")
    if not isinstance(summary_raw, list):
        return None
    summary_lines: list[dict[str, Any]] = []
    for item in summary_raw:
        if not isinstance(item, dict):
            continue
        text = compact_text(str(item.get("text") or ""))
        if not text:
            continue
        cards = _normalize_aliases(item.get("cards"), alias_values=alias_values)
        summary_lines.append({"text": text[:220], "cards": cards})
        if len(summary_lines) >= 8:
            break
    if len(summary_lines) < 8:
        return None

    change_lines: list[dict[str, Any]] = []
    change_raw = payload.get("change_3")
    if isinstance(change_raw, list):
        for item in change_raw:
            if not isinstance(item, dict):
                continue
            text = compact_text(str(item.get("text") or ""))
            if not text:
                continue
            sign = compact_text(str(item.get("sign") or "+"))
            if sign not in {"+", "-"}:
                sign = "+"
            cards = _normalize_aliases(item.get("cards"), alias_values=alias_values)
            change_lines.append({"sign": sign, "text": text[:220], "cards": cards})
            if len(change_lines) >= 3:
                break

    open_raw = payload.get("open_questions")
    if not isinstance(open_raw, list):
        return None
    open_questions: list[dict[str, Any]] = []
    for item in open_raw:
        if isinstance(item, dict):
            text = compact_text(str(item.get("text") or ""))
            cards = _normalize_aliases(item.get("cards"), alias_values=alias_values)
        else:
            text = compact_text(str(item))
            cards = []
        if not text:
            continue
        open_questions.append({"text": text[:220], "cards": cards})
        if len(open_questions) >= 2:
            break
    if len(open_questions) < 2:
        return None

    return {
        "summary_lines": summary_lines[:8],
        "change_lines": change_lines[:3],
        "open_questions": open_questions[:2],
    }


def _normalize_aliases(raw: Any, *, alias_values: set[str]) -> list[str]:
    if isinstance(raw, str):
        raw = [x for x in raw.replace(" ", "").split(",") if x]
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for x in raw:
        alias = compact_text(str(x)).upper()
        if alias and alias in alias_values and alias not in out:
            out.append(alias)
    return out[:3]


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
