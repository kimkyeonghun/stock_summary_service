from __future__ import annotations

import json
from typing import Any

from stock_mvp.agents.base import AgentStats, date_days_ago, iso_date_utc
from stock_mvp.config import Settings, load_settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.storage import digest_repo, evidence_repo
from stock_mvp.utils import compact_text


MIN_SUMMARY_LINES = 5
SECTION_LABELS = {
    "core": "[핵심요약]",
    "evidence": "[근거]",
    "risk": "[리스크]",
}
DIGEST_PROMPT_VERSION = "m4_digest_v1"


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
        total = len(entity_ids)
        if total > 0:
            print(
                "[PROGRESS] entity_digest start "
                f"entity_type={entity_type} market={market.upper()} total={total}"
            )

        for idx, entity_id in enumerate(entity_ids, start=1):
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
                    prompt_version=DIGEST_PROMPT_VERSION,
                )
                created += 1
            except Exception as exc:
                errors += 1
                print(
                    f"[WARN] entity_digest failed: entity_type={entity_type} "
                    f"entity_id={entity_id} market={market} error={exc}"
                )
            if idx == 1 or idx == total or idx % 20 == 0:
                print(
                    "[PROGRESS] entity_digest "
                    f"{idx}/{total} created={created} errors={errors}"
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

        summary_lines = self._build_summary_lines(cards, aliases)
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

        summary_8line = self._compose_variable_summary_from_payload(parsed["summary_lines"])
        change_3 = self._compose_change_from_payload(parsed["change_lines"])
        open_questions = self._compose_questions_from_payload(parsed["open_questions"])
        return {
            "summary_8line": summary_8line,
            "change_3": change_3,
            "open_questions": open_questions,
        }

    def _compose_variable_summary_from_payload(self, lines: list[dict[str, Any]]) -> str:
        out: list[str] = []
        for idx, item in enumerate(lines):
            text = compact_text(str(item.get("text") or "")) or "유의미한 업데이트가 없습니다."
            cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
            section = _normalize_section_key(item.get("section"))
            label = SECTION_LABELS.get(section, SECTION_LABELS["core"])
            card_text = ",".join(cards[:3]) if cards else "-"
            out.append(f"{idx + 1}) {label} {text} (cards: {card_text})")
        if len(out) < MIN_SUMMARY_LINES:
            for idx in range(len(out), MIN_SUMMARY_LINES):
                out.append(f"{idx + 1}) {SECTION_LABELS['core']} 유의미한 업데이트가 없습니다. (cards: -)")
        return "\n".join(out)

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
            out.append(f"Q{idx}) 추가 근거 확인이 필요합니다. (cards: -)")
        return "\n".join(out[:2])

    def _build_summary_lines(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> list[str]:
        if not cards:
            return [
                "1) [핵심요약] 확인 가능한 핵심 사실이 아직 충분하지 않습니다. (cards: -)",
                "2) [핵심요약] 현재 데이터만으로 뚜렷한 추세를 단정하기 어렵습니다. (cards: -)",
                "3) [근거] 수치 또는 실적 관련 업데이트가 제한적입니다. (cards: -)",
                "4) [근거] 수요·섹터 방향을 확정할 신호가 부족합니다. (cards: -)",
                "5) [리스크] 명시적 리스크 언급이 없습니다. (cards: -)",
            ]

        def fact(idx: int, fallback: str) -> tuple[str, str]:
            if idx < len(cards):
                card = cards[idx]
                facts = list(card.get("facts") or [])
                text = compact_text(facts[0] if facts else card.get("fact_headline") or fallback)
                return text, aliases[card["card_id"]]
            return fallback, "-"

        f1, c1 = fact(0, "최근 핵심 사실 업데이트가 제한적입니다.")
        f2, c2 = fact(1, "추가 핵심 사실 업데이트가 제한적입니다.")
        num_fact, c3 = self._pick_number_fact(cards, aliases)
        demand_fact, c4 = self._pick_topic_fact(cards, aliases, topic="demand")
        risk, c5 = self._pick_risk(cards, aliases)

        return [
            f"1) [핵심요약] {f1} (cards: {c1})",
            f"2) [핵심요약] {f2} (cards: {c2})",
            f"3) [근거] {num_fact} (cards: {c3})",
            f"4) [근거] {demand_fact} (cards: {c4})",
            f"5) [리스크] {risk} (cards: {c5})",
        ]

    def _pick_number_fact(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            for fact in card.get("facts") or []:
                if any(ch.isdigit() for ch in str(fact)):
                    return compact_text(str(fact)), aliases[card["card_id"]]
        return "숫자 기반 업데이트가 현재 제한적입니다.", "-"

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
                text = compact_text(str(facts[0] if facts else card.get("fact_headline") or ""))
                return text or "수요/섹터 관련 신호가 제한적입니다.", aliases[card["card_id"]]
        return "명확한 수요/섹터 신호가 확인되지 않았습니다.", "-"

    def _pick_interpretation(self, cards: list[dict[str, Any]], aliases: dict[str, str], *, idx: int) -> tuple[str, str]:
        if idx < len(cards):
            card = cards[idx]
            text = compact_text(str(card.get("interpretation") or ""))
            if text:
                return text, aliases[card["card_id"]]
        return "해석 가능한 근거가 아직 제한적입니다.", "-"

    def _pick_risk(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            risk = compact_text(str(card.get("risk_note") or ""))
            if risk:
                return risk, aliases[card["card_id"]]
        return "명시적 리스크 언급이 없습니다.", "-"

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
            return "Q1) 추가 근거 확인이 필요합니다.\nQ2) 추가 근거 확인이 필요합니다."
        top = cards[0]
        first_alias = aliases.get(top["card_id"], "-")
        topics = [str(x) for x in top.get("topics") or []]
        topic = topics[0] if topics else "핵심 토픽"
        q1 = f"Q1) 다음 공시/리포트에서 {topic} 신호가 재확인될 수 있을까요? (cards: {first_alias})"
        q2 = f"Q2) 현재 리스크 요인이 다음 구간 성과에 얼마나 유의미할까요? (cards: {first_alias})"
        return f"{q1}\n{q2}"


def _digest_system_prompt() -> str:
    return (
        "You are a market digest writer for beginner investors. Return JSON only. "
        "Required keys: summary_lines, change_3, open_questions. "
        "summary_lines must contain at least 5 items (no fixed max). "
        "Each summary line item: {section,text,cards}. section must be one of core,evidence,risk. "
        "Required section coverage in summary_lines: core>=2, evidence>=2, risk>=1. "
        "cards must contain aliases like C1,C2. "
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
        "summary_section_schema": {
            "core": "핵심요약",
            "evidence": "근거",
            "risk": "리스크",
        },
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
        raw_section = item.get("section")
        if compact_text(str(raw_section or "")):
            section = _normalize_section_key(raw_section)
        else:
            section = _infer_section_by_index(len(summary_lines))
        cards = _normalize_aliases(item.get("cards"), alias_values=alias_values)
        summary_lines.append({"section": section, "text": text[:220], "cards": cards})
    if len(summary_lines) < MIN_SUMMARY_LINES:
        return None
    core_count = sum(1 for line in summary_lines if line["section"] == "core")
    evidence_count = sum(1 for line in summary_lines if line["section"] == "evidence")
    risk_count = sum(1 for line in summary_lines if line["section"] == "risk")
    if core_count < 2 or evidence_count < 2 or risk_count < 1:
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
        "summary_lines": summary_lines,
        "change_lines": change_lines[:3],
        "open_questions": open_questions[:2],
    }


def _normalize_section_key(value: Any) -> str:
    key = compact_text(str(value or "")).lower()
    if key in {"core", "summary", "핵심", "핵심요약"}:
        return "core"
    if key in {"evidence", "fact", "근거", "팩트"}:
        return "evidence"
    if key in {"risk", "리스크", "위험"}:
        return "risk"
    return "core"


def _infer_section_by_index(idx: int) -> str:
    if idx < 2:
        return "core"
    if idx < 4:
        return "evidence"
    return "risk"


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
