from __future__ import annotations

import re
from datetime import datetime

from stock_mvp.models import GeneratedSummary, SummaryLine
from stock_mvp.utils import format_source_tag, now_utc


POSITIVE_WORDS = {
    "상향",
    "개선",
    "증가",
    "성장",
    "회복",
    "호조",
    "흑자",
    "수주",
    "신제품",
    "확대",
}
NEGATIVE_WORDS = {
    "하향",
    "감소",
    "악화",
    "둔화",
    "부진",
    "적자",
    "리스크",
    "지연",
    "규제",
    "우려",
}
RISK_WORDS = {"변동성", "불확실", "지연", "규제", "환율", "금리", "경쟁", "소송", "임상", "실패"}
CHECKPOINT_WORDS = {"실적", "가이던스", "공시", "수주", "출시", "정책", "환율", "금리", "분기"}
NUM_PATTERN = re.compile(r"(\d+(?:\.\d+)?%|\d+(?:,\d{3})+원|\d+(?:\.\d+)?조원|\d+(?:\.\d+)?억원)")


class SummaryBuilder:
    model_name = "rule_based_v1"

    def build(self, stock_code: str, docs: list[dict]) -> GeneratedSummary:
        if not docs:
            now = now_utc()
            empty_tag = "[no_source]"
            lines = [
                SummaryLine(f"오늘 기준 주요 신규 문서가 부족합니다. {empty_tag}", []),
                SummaryLine(f"핵심 이슈 1: 추가 데이터 수집이 필요합니다. {empty_tag}", []),
                SummaryLine(f"핵심 이슈 2: 출처가 누적되면 자동 요약이 개선됩니다. {empty_tag}", []),
                SummaryLine(f"긍정 포인트: 확인 가능한 호재 데이터가 제한적입니다. {empty_tag}", []),
                SummaryLine(f"부정 포인트: 확인 가능한 악재 데이터가 제한적입니다. {empty_tag}", []),
                SummaryLine(f"숫자 변화: 집계 가능한 수치 문장이 없습니다. {empty_tag}", []),
                SummaryLine(f"단기 체크포인트: 다음 공시/실적 이벤트를 확인하세요. {empty_tag}", []),
                SummaryLine(f"리스크: 단일 정보로 판단하지 말고 출처를 교차 검증하세요. {empty_tag}", []),
            ]
            return GeneratedSummary(stock_code=stock_code, as_of=now, lines=lines, model=self.model_name)

        docs_sorted = sorted(
            docs,
            key=lambda d: (d.get("published_at") or "", d.get("id") or 0),
            reverse=True,
        )

        pos_score = sum(self._score_keywords(d, POSITIVE_WORDS) for d in docs_sorted)
        neg_score = sum(self._score_keywords(d, NEGATIVE_WORDS) for d in docs_sorted)

        top1 = docs_sorted[0]
        top2 = docs_sorted[1] if len(docs_sorted) > 1 else docs_sorted[0]
        pos_doc = self._find_by_keywords(docs_sorted, POSITIVE_WORDS) or top1
        neg_doc = self._find_by_keywords(docs_sorted, NEGATIVE_WORDS) or top1
        risk_doc = self._find_by_keywords(docs_sorted, RISK_WORDS) or top1
        chk_doc = self._find_by_keywords(docs_sorted, CHECKPOINT_WORDS) or top1
        num_doc, nums = self._find_numbers_doc(docs_sorted)
        if num_doc is None:
            num_doc = top1
            nums = []

        if pos_score > neg_score:
            conclusion = "최근 기사와 리포트 흐름은 단기적으로 우호적인 신호가 상대적으로 많습니다."
        elif neg_score > pos_score:
            conclusion = "최근 기사와 리포트 흐름은 단기적으로 보수적 해석이 필요한 신호가 더 많습니다."
        else:
            conclusion = "최근 기사와 리포트 흐름은 뚜렷한 한 방향보다 혼조 양상에 가깝습니다."

        line_texts = [
            (conclusion, [top1]),
            (f"핵심 이슈 1: {self._safe_title(top1['title'])}", [top1]),
            (f"핵심 이슈 2: {self._safe_title(top2['title'])}", [top2]),
            (f"긍정 포인트: {self._positive_point_text(pos_doc)}", [pos_doc]),
            (f"부정 포인트: {self._negative_point_text(neg_doc)}", [neg_doc]),
            (self._numeric_line_text(nums), [num_doc]),
            (f"단기 체크포인트: {self._checkpoint_text(chk_doc)}", [chk_doc]),
            (f"리스크/주의: {self._risk_text(risk_doc)}", [risk_doc]),
        ]

        lines: list[SummaryLine] = []
        for text, src_docs in line_texts:
            source_doc = src_docs[0]
            source_tag = format_source_tag(source_doc["source"], source_doc.get("published_at"))
            lines.append(SummaryLine(text=f"{text} {source_tag}", source_doc_ids=[source_doc["id"]]))

        as_of = now_utc()
        latest_time = self._latest_datetime(docs_sorted)
        if latest_time:
            as_of = latest_time
        return GeneratedSummary(stock_code=stock_code, as_of=as_of, lines=lines, model=self.model_name)

    @staticmethod
    def _safe_title(title: str) -> str:
        return re.sub(r"\s+", " ", title).strip()[:120]

    @staticmethod
    def _score_keywords(doc: dict, keywords: set[str]) -> int:
        text = f"{doc.get('title', '')} {doc.get('body', '')}".lower()
        return sum(1 for kw in keywords if kw.lower() in text)

    def _find_by_keywords(self, docs: list[dict], keywords: set[str]) -> dict | None:
        best = None
        best_score = 0
        for doc in docs:
            score = self._score_keywords(doc, keywords)
            if score > best_score:
                best = doc
                best_score = score
        return best

    @staticmethod
    def _latest_datetime(docs: list[dict]) -> datetime | None:
        for doc in docs:
            raw = doc.get("published_at")
            if isinstance(raw, datetime):
                return raw
        return None

    @staticmethod
    def _positive_point_text(doc: dict) -> str:
        base = doc.get("body") or doc.get("title", "")
        snippet = base.strip()[:90]
        if not snippet:
            snippet = "확인된 호재가 제한적이어서 추가 근거 축적이 필요합니다."
        return snippet

    @staticmethod
    def _negative_point_text(doc: dict) -> str:
        base = doc.get("body") or doc.get("title", "")
        snippet = base.strip()[:90]
        if not snippet:
            snippet = "확인된 악재가 제한적이어서 추가 근거 축적이 필요합니다."
        return snippet

    @staticmethod
    def _checkpoint_text(doc: dict) -> str:
        text = f"{doc.get('title', '')} {doc.get('body', '')}".strip()
        for kw in CHECKPOINT_WORDS:
            if kw in text:
                return f"다음 {kw} 관련 업데이트가 단기 방향성에 중요합니다."
        return "다음 실적/공시 업데이트 전후로 변동성 확대 여부를 확인하세요."

    @staticmethod
    def _risk_text(doc: dict) -> str:
        text = f"{doc.get('title', '')} {doc.get('body', '')}".strip()
        for kw in RISK_WORDS:
            if kw in text:
                return f"{kw} 관련 불확실성이 커질 수 있으니 단일 뉴스 추종은 피하세요."
        return "출처가 적은 단기 재료만으로 매수/매도 결정을 내리지 않는 것이 좋습니다."

    def _find_numbers_doc(self, docs: list[dict]) -> tuple[dict | None, list[str]]:
        for doc in docs:
            text = f"{doc.get('title', '')} {doc.get('body', '')}"
            matches = NUM_PATTERN.findall(text)
            if matches:
                return doc, matches[:2]
        return None, []

    @staticmethod
    def _numeric_line_text(nums: list[str]) -> str:
        if not nums:
            return "숫자 변화: 기사/리포트 내 정량 수치 표현이 제한적이었습니다."
        if len(nums) == 1:
            return f"숫자 변화: 최근 문서에서 {nums[0]} 수치가 언급되었습니다."
        return f"숫자 변화: 최근 문서에서 {nums[0]}, {nums[1]} 수치가 함께 확인됩니다."

