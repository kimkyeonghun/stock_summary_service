from __future__ import annotations

import json
from typing import Any

from stock_mvp.agents.base import AgentStats, date_days_ago, iso_date_utc
from stock_mvp.agents.prompts import SUMMARY_STYLE_GUIDE_V1
from stock_mvp.agents.summary_quality import (
    parse_section_line,
    format_section_line,
    has_required_sections,
    sanitize_line,
    sanitize_lines,
)
from stock_mvp.agents.translator import Translator
from stock_mvp.config import Settings, load_settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.storage import digest_repo, evidence_repo
from stock_mvp.utils import compact_text


MIN_SUMMARY_LINES = 5
MAX_SUMMARY_LINES = 6
SECTION_ORDER = ("conclusion", "evidence", "risk", "checkpoint", "final")
SECTION_LABELS = {
    "conclusion": "결론",
    "evidence": "근거",
    "risk": "리스크",
    "checkpoint": "체크포인트",
    "final": "최종 판단",
}
DIGEST_PROMPT_VERSION = "m4_digest_v2"
NO_CHANGE_TEXT = "No material change"


class EntityDigestAgent:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or load_settings()
        self.llm = LLMClient(self.settings)
        self.translator = Translator(self.settings)

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
            summary_text = llm_payload["summary_8line"]
            change_text = llm_payload["change_3"]
            question_text = llm_payload["open_questions"]
        else:
            summary_text = "\n".join(self._build_summary_lines(cards, aliases))
            change_text = self._build_change_3(cards, aliases, previous)
            question_text = self._build_open_questions(cards, aliases)

        translated_bundle = self.translator.translate_structured_to_ko(
            conn,
            {
                "summary_8line": summary_text,
                "change_3": change_text,
                "open_questions": question_text,
            },
            purpose="digest_bundle",
        )
        summary_text = str(translated_bundle.get("summary_8line") or "")
        change_text = str(translated_bundle.get("change_3") or "")
        question_text = str(translated_bundle.get("open_questions") or "")

        summary_text = self._quality_guard_summary_text(summary_text, cards=cards, aliases=aliases)
        change_text = self._quality_guard_change_text(change_text)
        question_text = self._quality_guard_questions_text(question_text)

        return {
            "summary_8line": summary_text,
            "change_3": change_text,
            "open_questions": question_text,
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

        prompt_cards = _compress_cards_for_prompt(cards, max_cards=120)

        # Step 2) Map-Reduce: when evidence is large, summarize chunk-first then reduce.
        if len(prompt_cards) >= 24:
            reduced = self._build_with_llm_map_reduce(
                entity_type=entity_type,
                entity_id=entity_id,
                market=market,
                start_date=start_date,
                end_date=end_date,
                cards=prompt_cards,
                previous=previous,
            )
            if reduced is not None:
                return reduced

        # Step 1 + 3) Budget-aware prompt builder with compressed cards.
        card_caps = (40, 30, 24, 16, 12)
        for attempt, cap in enumerate(card_caps, start=1):
            prompt_budget = max(2800, int(self.settings.llm_hard_max_input_chars * (0.72 - (attempt - 1) * 0.08)))
            result = self.llm.generate_json(
                system_prompt=_digest_system_prompt(),
                user_prompt=_digest_user_prompt(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    market=market,
                    start_date=start_date,
                    end_date=end_date,
                    cards=prompt_cards[:cap],
                    aliases=aliases,
                    previous=previous,
                    text_limit=160 if cap >= 30 else 120,
                    max_prompt_chars=prompt_budget,
                ),
                purpose=f"daily_digest_attempt_{attempt}",
            )
            if result is None:
                continue

            parsed = _parse_digest_payload(result.payload, alias_values=set(aliases.values()))
            if parsed is None:
                print(
                    f"[WARN] entity_digest llm invalid payload: entity_type={entity_type} "
                    f"entity_id={entity_id} attempt={attempt} cap={cap} keys={list(result.payload.keys())[:8]}"
                )
                continue

            return {
                "summary_8line": self._compose_summary_from_payload(parsed["summary_lines"]),
                "change_3": self._compose_change_from_payload(parsed["change_lines"]),
                "open_questions": self._compose_questions_from_payload(parsed["open_questions"]),
            }

        return None

    def _build_with_llm_map_reduce(
        self,
        *,
        entity_type: str,
        entity_id: str,
        market: str,
        start_date: str,
        end_date: str,
        cards: list[dict[str, Any]],
        previous: dict[str, Any] | None,
    ) -> dict[str, str] | None:
        chunk_size = 12
        max_chunks = 8
        chunks: list[list[dict[str, Any]]] = []
        for idx in range(0, len(cards), chunk_size):
            if len(chunks) >= max_chunks:
                break
            chunks.append(cards[idx : idx + chunk_size])
        if len(chunks) < 2:
            return None

        partials: list[dict[str, Any]] = []
        for chunk_idx, chunk in enumerate(chunks, start=1):
            chunk_aliases = self._make_aliases(chunk)
            prompt_budget = max(2200, int(self.settings.llm_hard_max_input_chars * 0.52))
            result = self.llm.generate_json(
                system_prompt=_digest_system_prompt(),
                user_prompt=_digest_user_prompt(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    market=market,
                    start_date=start_date,
                    end_date=end_date,
                    cards=chunk,
                    aliases=chunk_aliases,
                    previous=None,
                    text_limit=110,
                    max_prompt_chars=prompt_budget,
                ),
                purpose=f"daily_digest_map_chunk_{chunk_idx}",
            )
            if result is None:
                continue
            parsed = _parse_digest_payload(result.payload, alias_values=set(chunk_aliases.values()))
            if parsed is None:
                continue
            partials.append(
                {
                    "chunk_index": chunk_idx,
                    "summary_lines": parsed["summary_lines"],
                    "change_lines": parsed["change_lines"],
                    "open_questions": parsed["open_questions"],
                }
            )

        if len(partials) < 2:
            return None

        reduce_result = self.llm.generate_json(
            system_prompt=_digest_reduce_system_prompt(),
            user_prompt=_digest_reduce_user_prompt(
                entity_type=entity_type,
                entity_id=entity_id,
                market=market,
                start_date=start_date,
                end_date=end_date,
                partials=partials,
                previous=previous,
            ),
            purpose="daily_digest_reduce",
        )
        if reduce_result is None:
            return None
        parsed_reduce = _parse_digest_payload(reduce_result.payload, alias_values=set())
        if parsed_reduce is None:
            return None
        return {
            "summary_8line": self._compose_summary_from_payload(parsed_reduce["summary_lines"]),
            "change_3": self._compose_change_from_payload(parsed_reduce["change_lines"]),
            "open_questions": self._compose_questions_from_payload(parsed_reduce["open_questions"]),
        }

    def _compose_summary_from_payload(self, lines: list[dict[str, Any]]) -> str:
        ordered = _order_summary_lines(lines)
        out: list[str] = []
        for item in ordered:
            text = sanitize_line(str(item.get("text") or ""))
            if not text:
                continue
            section = _normalize_section_key(item.get("section"))
            cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
            out.append(self._format_line_with_cards(section=section, text=text, cards=cards))
            if len(out) >= MAX_SUMMARY_LINES:
                break
        out = sanitize_lines(out, max_len=220, limit=MAX_SUMMARY_LINES)
        if has_required_sections(out):
            return "\n".join(out)
        fallback = self._build_empty_summary_lines()
        return "\n".join(fallback)

    @staticmethod
    def _compose_change_from_payload(change_lines: list[dict[str, Any]]) -> str:
        if not change_lines:
            return NO_CHANGE_TEXT
        out: list[str] = []
        for item in change_lines[:3]:
            sign = compact_text(str(item.get("sign") or "+"))
            if sign not in {"+", "-"}:
                sign = "+"
            text = sanitize_line(str(item.get("text") or NO_CHANGE_TEXT))
            cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
            card_text = ",".join(cards[:3]) if cards else "-"
            out.append(f"{sign} {text} (cards: {card_text})")
        return "\n".join(out) if out else NO_CHANGE_TEXT

    @staticmethod
    def _compose_questions_from_payload(open_questions: list[dict[str, Any]]) -> str:
        out: list[str] = []
        for idx, item in enumerate(open_questions[:2], start=1):
            text = sanitize_line(str(item.get("text") or ""))
            if not text:
                continue
            cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
            card_text = ",".join(cards[:2]) if cards else "-"
            out.append(f"Q{idx}) {text} (cards: {card_text})")
        while len(out) < 2:
            idx = len(out) + 1
            out.append(f"Q{idx}) 추가 확인이 필요합니다. (cards: -)")
        return "\n".join(out[:2])

    def _build_summary_lines(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> list[str]:
        if not cards:
            return self._build_empty_summary_lines()

        headline, c1 = self._pick_fact(cards, aliases, idx=0, fallback="핵심 업데이트는 확인됐지만 해석은 추가 확인이 필요합니다.")
        evidence_1, c2 = self._pick_number_fact(cards, aliases)
        evidence_2, c3 = self._pick_topic_fact(cards, aliases, topic="demand")
        risk, c4 = self._pick_risk(cards, aliases)
        checkpoint, c5 = self._pick_checkpoint(cards, aliases)
        final, c6 = self._pick_final(cards, aliases)

        lines = [
            self._format_line_with_cards(section="conclusion", text=headline, cards=[c1] if c1 != "-" else []),
            self._format_line_with_cards(section="evidence", text=evidence_1, cards=[c2] if c2 != "-" else []),
        ]
        if compact_text(evidence_2) and evidence_2 != evidence_1:
            lines.append(self._format_line_with_cards(section="evidence", text=evidence_2, cards=[c3] if c3 != "-" else []))
        lines.extend(
            [
                self._format_line_with_cards(section="risk", text=risk, cards=[c4] if c4 != "-" else []),
                self._format_line_with_cards(section="checkpoint", text=checkpoint, cards=[c5] if c5 != "-" else []),
                self._format_line_with_cards(section="final", text=final, cards=[c6] if c6 != "-" else []),
            ]
        )
        lines = sanitize_lines(lines, max_len=220, limit=MAX_SUMMARY_LINES)
        if has_required_sections(lines):
            return lines
        return self._build_empty_summary_lines()

    def _build_empty_summary_lines(self) -> list[str]:
        return [
            self._format_line_with_cards(section="conclusion", text="확인 가능한 핵심 신호가 아직 제한적입니다.", cards=[]),
            self._format_line_with_cards(section="evidence", text="정량 근거가 충분히 축적되지 않았습니다.", cards=[]),
            self._format_line_with_cards(section="risk", text="자료 부족으로 해석 오차가 커질 수 있습니다.", cards=[]),
            self._format_line_with_cards(section="checkpoint", text="다음 공시·리포트에서 동일 신호 재확인이 필요합니다.", cards=[]),
            self._format_line_with_cards(section="final", text="현재는 방향을 단정하기보다 추가 데이터를 확인하는 구간입니다.", cards=[]),
        ]

    def _pick_fact(self, cards: list[dict[str, Any]], aliases: dict[str, str], *, idx: int, fallback: str) -> tuple[str, str]:
        if idx < len(cards):
            card = cards[idx]
            headline = compact_text(str(card.get("fact_headline") or ""))
            if headline:
                return headline, aliases.get(card["card_id"], "-")
            facts = [compact_text(str(x)) for x in list(card.get("facts") or []) if compact_text(str(x))]
            if facts:
                return facts[0], aliases.get(card["card_id"], "-")
        return fallback, "-"

    def _pick_number_fact(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            for fact in card.get("facts") or []:
                text = compact_text(str(fact))
                if text and any(ch.isdigit() for ch in text):
                    return text, aliases.get(card["card_id"], "-")
        return "숫자 기반 업데이트는 제한적입니다.", "-"

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
                facts = [compact_text(str(x)) for x in list(card.get("facts") or []) if compact_text(str(x))]
                if facts:
                    return facts[0], aliases.get(card["card_id"], "-")
                headline = compact_text(str(card.get("fact_headline") or ""))
                if headline:
                    return headline, aliases.get(card["card_id"], "-")
        return "수요/업황 방향성은 추가 근거 확인이 필요합니다.", "-"

    def _pick_risk(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            risk = compact_text(str(card.get("risk_note") or ""))
            if risk:
                return risk, aliases.get(card["card_id"], "-")
        return "명시적 리스크 언급이 부족합니다.", "-"

    def _pick_checkpoint(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            topics = [compact_text(str(x)) for x in list(card.get("topics") or []) if compact_text(str(x))]
            if topics:
                topic = topics[0]
                text = f"다음 공시·리포트에서 '{topic}' 관련 신호가 재확인되는지 점검이 필요합니다."
                return text, aliases.get(card["card_id"], "-")
        return "다음 분기 공시와 수요 지표 변화를 확인할 필요가 있습니다.", "-"

    def _pick_final(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> tuple[str, str]:
        for card in cards:
            interpretation = sanitize_line(str(card.get("interpretation") or ""))
            if interpretation:
                text = f"{interpretation} 단정적 결론보다 후속 데이터 확인이 필요합니다."
                return text, aliases.get(card["card_id"], "-")
        return "신호는 존재하지만 확정적 판단보다 추가 확인이 우선입니다.", "-"

    def _build_change_3(
        self,
        cards: list[dict[str, Any]],
        aliases: dict[str, str],
        previous: dict[str, Any] | None,
    ) -> str:
        if not cards:
            return NO_CHANGE_TEXT

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
            headline = sanitize_line(str(card.get("fact_headline") or ""))
            if not headline:
                continue
            sign = "+"
            if any(k in headline.lower() for k in ("risk", "downgrade", "decline", "weak", "cut", "하락", "악화")):
                sign = "-"
            changes.append(f"{sign} {headline} (cards: {aliases.get(card['card_id'], '-')})")
            if len(changes) >= 3:
                break

        if not changes:
            return NO_CHANGE_TEXT
        return "\n".join(changes)

    def _build_open_questions(self, cards: list[dict[str, Any]], aliases: dict[str, str]) -> str:
        if not cards:
            return (
                "Q1) 다음 공시에서 실적과 수요 신호가 함께 확인될까요? (cards: -)\n"
                "Q2) 현재 리스크가 다음 분기 숫자에 반영될 시점은 언제일까요? (cards: -)"
            )
        top = cards[0]
        first_alias = aliases.get(top["card_id"], "-")
        topics = [compact_text(str(x)) for x in top.get("topics") or [] if compact_text(str(x))]
        topic = topics[0] if topics else "핵심 지표"
        q1 = f"Q1) 다음 업데이트에서 '{topic}' 신호가 강화될까요? (cards: {first_alias})"
        q2 = f"Q2) 현재 리스크가 실적·밸류에이션에 미칠 영향은 얼마나 될까요? (cards: {first_alias})"
        return f"{q1}\n{q2}"

    def _quality_guard_summary_text(self, text: str, *, cards: list[dict[str, Any]], aliases: dict[str, str]) -> str:
        lines = _split_non_empty_lines(text)
        lines = sanitize_lines(lines, max_len=220, limit=MAX_SUMMARY_LINES)
        if not has_required_sections(lines):
            lines = self._build_summary_lines(cards, aliases)
        return "\n".join(lines[:MAX_SUMMARY_LINES])

    @staticmethod
    def _quality_guard_change_text(text: str) -> str:
        lines = sanitize_lines(_split_non_empty_lines(text), max_len=220, limit=3)
        return "\n".join(lines) if lines else NO_CHANGE_TEXT

    @staticmethod
    def _quality_guard_questions_text(text: str) -> str:
        lines = sanitize_lines(_split_non_empty_lines(text), max_len=220, limit=2)
        while len(lines) < 2:
            lines.append(f"Q{len(lines) + 1}) 추가 확인이 필요합니다. (cards: -)")
        return "\n".join(lines[:2])

    @staticmethod
    def _format_line_with_cards(*, section: str, text: str, cards: list[str]) -> str:
        line = format_section_line(section, text)
        if not line:
            line = format_section_line(section, "추가 확인이 필요합니다.")
        card_text = ",".join([compact_text(str(x)) for x in cards if compact_text(str(x))][:3]) or "-"
        return f"{line} (cards: {card_text})"


def _digest_system_prompt() -> str:
    return (
        "You are a market digest writer for beginner investors. Return JSON only. "
        "Required keys: summary_lines, change_3, open_questions. "
        "summary_lines must contain 5~6 items. "
        "Each summary line item: {section,text,cards}. "
        "section must be one of conclusion,evidence,risk,checkpoint,final. "
        "Required coverage: conclusion>=1, evidence>=1, risk>=1, checkpoint>=1, final>=1. "
        "cards should contain aliases like C1,C2 when available. "
        "change_3 should be up to 3 items with {sign,text,cards}. sign is '+' or '-'. "
        "open_questions should be exactly 2 items with {text,cards}. "
        "No investment recommendation language. "
        f"{SUMMARY_STYLE_GUIDE_V1}"
    )


def _digest_reduce_system_prompt() -> str:
    return (
        "You are consolidating chunk-level market digests. Return JSON only. "
        "Required keys: summary_lines, change_3, open_questions. "
        "summary_lines must contain 5~6 items with {section,text,cards}. "
        "section must be one of conclusion,evidence,risk,checkpoint,final. "
        "change_3 should be up to 3 items with {sign,text,cards}. sign is '+' or '-'. "
        "open_questions should be exactly 2 items with {text,cards}. "
        "Do not invent facts; merge only what is supported by chunk summaries. "
        "No investment recommendation language. "
        f"{SUMMARY_STYLE_GUIDE_V1}"
    )


def _digest_reduce_user_prompt(
    *,
    entity_type: str,
    entity_id: str,
    market: str,
    start_date: str,
    end_date: str,
    partials: list[dict[str, Any]],
    previous: dict[str, Any] | None,
) -> str:
    normalized_partials: list[dict[str, Any]] = []
    for part in partials[:8]:
        summary_lines = []
        for row in list(part.get("summary_lines") or [])[:6]:
            summary_lines.append(
                {
                    "section": _normalize_section_key(row.get("section")),
                    "text": sanitize_line(str(row.get("text") or ""), max_len=200),
                    "cards": [compact_text(str(x)) for x in list(row.get("cards") or [])[:3] if compact_text(str(x))],
                }
            )
        change_lines = []
        for row in list(part.get("change_lines") or [])[:3]:
            sign = compact_text(str(row.get("sign") or "+"))
            if sign not in {"+", "-"}:
                sign = "+"
            change_lines.append(
                {
                    "sign": sign,
                    "text": sanitize_line(str(row.get("text") or ""), max_len=200),
                    "cards": [compact_text(str(x)) for x in list(row.get("cards") or [])[:3] if compact_text(str(x))],
                }
            )
        open_questions = []
        for row in list(part.get("open_questions") or [])[:2]:
            open_questions.append(
                {
                    "text": sanitize_line(str(row.get("text") or ""), max_len=200),
                    "cards": [compact_text(str(x)) for x in list(row.get("cards") or [])[:3] if compact_text(str(x))],
                }
            )
        normalized_partials.append(
            {
                "chunk_index": int(part.get("chunk_index") or 0),
                "summary_lines": summary_lines,
                "change_3": change_lines,
                "open_questions": open_questions,
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
        "partial_digests": normalized_partials,
        "previous_digest": previous_payload,
    }
    return f"Merge chunk digests into one final daily digest JSON:\n{json.dumps(payload, ensure_ascii=False)}"


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
    text_limit: int = 180,
    max_prompt_chars: int = 9000,
) -> str:
    previous_payload = {
        "summary_8line": str(previous.get("summary_8line") or "") if previous else "",
        "change_3": str(previous.get("change_3") or "") if previous else "",
    }
    payload_base = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "market": market,
        "period": {"start_date": start_date, "end_date": end_date},
        "summary_section_schema": {
            "conclusion": "결론",
            "evidence": "근거",
            "risk": "리스크",
            "checkpoint": "체크포인트",
            "final": "최종 판단",
        },
        "cards": [],
        "previous_digest": previous_payload,
    }
    base_chars = len(json.dumps(payload_base, ensure_ascii=False))
    card_rows = _build_digest_card_rows_with_budget(
        cards=cards,
        aliases=aliases,
        text_limit=text_limit,
        max_prompt_chars=max_prompt_chars,
        payload_overhead_chars=base_chars + 40,
    )
    payload = dict(payload_base)
    payload["cards"] = card_rows
    return f"Generate daily digest JSON from:\n{json.dumps(payload, ensure_ascii=False)}"


def _compress_cards_for_prompt(cards: list[dict[str, Any]], *, max_cards: int = 120) -> list[dict[str, Any]]:
    compressed: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for card in cards:
        headline = compact_text(str(card.get("fact_headline") or ""))
        first_fact = compact_text(str((card.get("facts") or [""])[0] if isinstance(card.get("facts"), list) else ""))
        published_at = compact_text(str(card.get("published_at") or ""))
        key = (headline.lower()[:120], first_fact.lower()[:120], published_at[:10])
        if key in seen:
            continue
        seen.add(key)
        copied = dict(card)
        copied["fact_headline"] = headline
        copied["interpretation"] = compact_text(str(card.get("interpretation") or ""))
        copied["risk_note"] = compact_text(str(card.get("risk_note") or ""))
        copied["facts"] = [compact_text(str(x)) for x in list(card.get("facts") or []) if compact_text(str(x))][:2]
        copied["topics"] = [compact_text(str(x)) for x in list(card.get("topics") or []) if compact_text(str(x))][:4]
        compressed.append(copied)
        if len(compressed) >= max(1, max_cards):
            break
    return compressed


def _build_digest_card_rows_with_budget(
    *,
    cards: list[dict[str, Any]],
    aliases: dict[str, str],
    text_limit: int,
    max_prompt_chars: int,
    payload_overhead_chars: int,
) -> list[dict[str, Any]]:
    hard_max = max(2200, int(max_prompt_chars))
    max_len = max(70, int(text_limit))
    rows: list[dict[str, Any]] = []
    for card in cards[:80]:
        card_id_value = card.get("card_id")
        alias_value = aliases.get(card_id_value, aliases.get(str(card_id_value or ""), ""))
        row = {
            "alias": alias_value,
            "card_id": str(card_id_value or ""),
            "item_id": int(card.get("item_id") or 0),
            "source_type": str(card.get("source_type") or ""),
            "fact_headline": compact_text(str(card.get("fact_headline") or ""))[:max_len],
            "facts": [compact_text(str(x))[:max_len] for x in list(card.get("facts") or [])[:2]],
            "interpretation": compact_text(str(card.get("interpretation") or ""))[:max_len],
            "risk_note": compact_text(str(card.get("risk_note") or ""))[:max_len],
            "topics": [compact_text(str(x))[:32] for x in list(card.get("topics") or [])[:3]],
            "published_at": str(card.get("published_at") or ""),
        }

        projected = rows + [row]
        projected_chars = payload_overhead_chars + len(json.dumps(projected, ensure_ascii=False))
        if projected_chars > hard_max:
            if rows:
                break
            # Keep at least one card with aggressively compact fields.
            tiny = dict(row)
            tiny["fact_headline"] = tiny["fact_headline"][:80]
            tiny["facts"] = [compact_text(str(x))[:80] for x in tiny.get("facts", [])[:1]]
            tiny["interpretation"] = tiny["interpretation"][:80]
            tiny["risk_note"] = tiny["risk_note"][:80]
            tiny["topics"] = [compact_text(str(x))[:20] for x in tiny.get("topics", [])[:2]]
            rows.append(tiny)
            break
        rows.append(row)
    return rows


def _parse_digest_payload(payload: dict[str, Any], *, alias_values: set[str]) -> dict[str, list[dict[str, Any]]] | None:
    if not isinstance(payload, dict):
        return None

    summary_raw = payload.get("summary_lines")
    if summary_raw is None:
        summary_raw = payload.get("summary_8line") or payload.get("summary")
    summary_lines = _coerce_summary_lines(summary_raw, alias_values=alias_values)
    if not summary_lines:
        return None

    summary_lines = _repair_summary_lines(summary_lines)
    if len(summary_lines) < MIN_SUMMARY_LINES:
        return None
    formatted_for_check = [format_section_line(row["section"], row["text"]) for row in summary_lines]
    if not has_required_sections([x for x in formatted_for_check if x]):
        return None

    change_raw = payload.get("change_3")
    if change_raw is None:
        change_raw = payload.get("changes")
    change_lines = _coerce_change_lines(change_raw, alias_values=alias_values)

    open_raw = payload.get("open_questions")
    if open_raw is None:
        open_raw = payload.get("questions")
    open_questions = _coerce_open_questions(open_raw, alias_values=alias_values)
    if len(open_questions) < 2:
        base = open_questions[:]
        while len(base) < 2:
            base.append({"text": "Additional follow-up is needed.", "cards": []})
        open_questions = base[:2]

    return {
        "summary_lines": summary_lines,
        "change_lines": change_lines[:3],
        "open_questions": open_questions[:2],
    }


def _coerce_summary_lines(raw: Any, *, alias_values: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(raw, str):
        raw = _split_non_empty_lines(raw)
    if isinstance(raw, dict):
        raw = raw.get("lines") or raw.get("items") or []
    if not isinstance(raw, list):
        return out
    for idx, item in enumerate(raw):
        section = _infer_section_by_index(idx)
        cards: list[str] = []
        text = ""
        if isinstance(item, dict):
            text = sanitize_line(
                str(item.get("text") or item.get("line") or item.get("summary") or ""),
                max_len=220,
            )
            if compact_text(str(item.get("section") or "")):
                section = _normalize_section_key(item.get("section"))
            else:
                guessed, body = parse_section_line(text)
                if guessed:
                    section = guessed
                    text = sanitize_line(body, max_len=220)
            cards = _normalize_aliases(item.get("cards"), alias_values=alias_values)
        else:
            text = sanitize_line(str(item), max_len=220)
            guessed, body = parse_section_line(text)
            if guessed:
                section = guessed
                text = sanitize_line(body, max_len=220)
        if not text:
            continue
        out.append({"section": section, "text": text, "cards": cards})
        if len(out) >= MAX_SUMMARY_LINES:
            break
    return out


def _repair_summary_lines(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not lines:
        return []
    grouped: dict[str, list[dict[str, Any]]] = {k: [] for k in SECTION_ORDER}
    for item in lines:
        section = _normalize_section_key(item.get("section"))
        text = sanitize_line(str(item.get("text") or ""), max_len=220)
        cards = [compact_text(str(x)) for x in list(item.get("cards") or []) if compact_text(str(x))]
        if not text:
            continue
        grouped[section].append({"section": section, "text": text, "cards": cards[:3]})

    repaired: list[dict[str, Any]] = []
    placeholders = {
        "conclusion": "No material change observed in the period.",
        "evidence": "Evidence remains mixed and requires follow-up.",
        "risk": "Uncertainty remains around near-term direction.",
        "checkpoint": "Track next disclosures and volume/price reaction.",
        "final": "Maintain a neutral interpretation until confirmation.",
    }
    for section in SECTION_ORDER:
        if grouped[section]:
            repaired.append(grouped[section][0])
        else:
            repaired.append({"section": section, "text": placeholders[section], "cards": []})
    extra_evidence = grouped["evidence"][1:2]
    repaired.extend(extra_evidence)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for idx, row in enumerate(repaired):
        section = _normalize_section_key(row.get("section") or _infer_section_by_index(idx))
        text = sanitize_line(str(row.get("text") or ""), max_len=220)
        if not text:
            continue
        key = (section, text.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append({"section": section, "text": text, "cards": list(row.get("cards") or [])[:3]})
        if len(out) >= MAX_SUMMARY_LINES:
            break
    return out


def _coerce_change_lines(raw: Any, *, alias_values: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(raw, str):
        raw = _split_non_empty_lines(raw)
    if isinstance(raw, dict):
        raw = raw.get("lines") or raw.get("items") or []
    if not isinstance(raw, list):
        return out
    for item in raw:
        sign = "+"
        cards: list[str] = []
        text = ""
        if isinstance(item, dict):
            text = sanitize_line(str(item.get("text") or item.get("line") or ""), max_len=220)
            sign_raw = compact_text(str(item.get("sign") or item.get("direction") or "+"))
            if sign_raw in {"+", "-"}:
                sign = sign_raw
            elif sign_raw.lower() in {"down", "decrease", "negative"}:
                sign = "-"
            cards = _normalize_aliases(item.get("cards"), alias_values=alias_values)
        else:
            text = sanitize_line(str(item), max_len=220)
            if text.startswith("-"):
                sign = "-"
                text = sanitize_line(text[1:], max_len=220)
            elif text.startswith("+"):
                sign = "+"
                text = sanitize_line(text[1:], max_len=220)
        if not text:
            continue
        out.append({"sign": sign, "text": text, "cards": cards})
        if len(out) >= 3:
            break
    return out


def _coerce_open_questions(raw: Any, *, alias_values: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(raw, str):
        raw = _split_non_empty_lines(raw)
    if isinstance(raw, dict):
        raw = raw.get("lines") or raw.get("items") or []
    if not isinstance(raw, list):
        return out
    for item in raw:
        cards: list[str] = []
        if isinstance(item, dict):
            text = sanitize_line(str(item.get("text") or item.get("question") or ""), max_len=220)
            cards = _normalize_aliases(item.get("cards"), alias_values=alias_values)
        else:
            text = sanitize_line(str(item), max_len=220)
        if not text:
            continue
        out.append({"text": text, "cards": cards})
        if len(out) >= 2:
            break
    return out


def _normalize_section_key(value: Any) -> str:
    key = compact_text(str(value or "")).lower()
    if key in {"conclusion", "core", "summary", "핵심", "핵심요약", "결론"}:
        return "conclusion"
    if key in {"evidence", "fact", "근거", "팩트"}:
        return "evidence"
    if key in {"risk", "리스크", "위험"}:
        return "risk"
    if key in {"checkpoint", "check", "질문", "확인", "체크포인트"}:
        return "checkpoint"
    if key in {"final", "bottom", "sentiment", "최종", "판단", "최종판단"}:
        return "final"
    return "conclusion"


def _infer_section_by_index(idx: int) -> str:
    order = ("conclusion", "evidence", "evidence", "risk", "checkpoint", "final")
    if idx < len(order):
        return order[idx]
    return "final"


def _order_summary_lines(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {k: [] for k in SECTION_ORDER}
    extra: list[dict[str, Any]] = []
    for item in lines:
        section = _normalize_section_key(item.get("section"))
        if section in grouped:
            grouped[section].append(item)
        else:
            extra.append(item)
    ordered: list[dict[str, Any]] = []
    ordered.extend(grouped["conclusion"][:1])
    ordered.extend(grouped["evidence"][:2])
    ordered.extend(grouped["risk"][:1])
    ordered.extend(grouped["checkpoint"][:1])
    ordered.extend(grouped["final"][:1])
    ordered.extend(extra)
    return ordered[:MAX_SUMMARY_LINES]


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


def _split_non_empty_lines(text: str) -> list[str]:
    return [compact_text(line) for line in str(text or "").splitlines() if compact_text(line)]


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
