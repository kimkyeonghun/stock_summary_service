from __future__ import annotations

import re
from dataclasses import dataclass

import requests
import urllib3
from bs4 import BeautifulSoup

from stock_mvp.agents.translator import Translator
from stock_mvp.config import Settings
from stock_mvp.database import latest_documents
from stock_mvp.llm_client import LLMClient
from stock_mvp.models import Stock
from stock_mvp.utils import compact_text


NAVER_ITEM_MAIN_URL = "https://finance.naver.com/item/main.naver"
YAHOO_QUOTE_SUMMARY_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
YAHOO_COOKIE_BOOTSTRAP_URL = "https://fc.yahoo.com"
YAHOO_CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"


@dataclass(frozen=True)
class CollectedStockProfile:
    stock_code: str
    market: str
    description_ko: str
    description_raw: str
    source: str
    source_url: str
    source_updated_at: str = ""


class CompanyProfileCollector:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        self.llm = LLMClient(settings)
        self.translator = Translator(settings)
        self._yahoo_crumb: str = ""

    def collect(self, conn, stock: Stock) -> CollectedStockProfile | None:
        market = str(stock.market or "").upper()
        result: CollectedStockProfile | None = None
        if market == "KR":
            result = self._collect_kr_naver(stock)
            if result is not None:
                return self._translate_profile(conn, result)
            result = self._collect_from_documents(conn, stock)
            return self._translate_profile(conn, result) if result is not None else None
        if market == "US":
            result = self._collect_us_yahoo(stock)
            if result is not None:
                return self._translate_profile(conn, result)
            result = self._collect_from_documents(conn, stock)
            return self._translate_profile(conn, result) if result is not None else None
        result = self._collect_from_documents(conn, stock)
        return self._translate_profile(conn, result) if result is not None else None

    def _translate_profile(self, conn, profile: CollectedStockProfile) -> CollectedStockProfile:
        translated = self.translator.translate_text_to_ko(
            conn,
            profile.description_ko,
            purpose="company_profile_description",
        )
        if not translated:
            translated = profile.description_ko
        return CollectedStockProfile(
            stock_code=profile.stock_code,
            market=profile.market,
            description_ko=translated,
            description_raw=profile.description_raw,
            source=profile.source,
            source_url=profile.source_url,
            source_updated_at=profile.source_updated_at,
        )

    def _collect_kr_naver(self, stock: Stock) -> CollectedStockProfile | None:
        try:
            response = self._get(
                NAVER_ITEM_MAIN_URL,
                params={"code": stock.code},
                headers={"Referer": "https://finance.naver.com/"},
            )
            response.raise_for_status()
            html = response.content.decode("euc-kr", "ignore")
            soup = BeautifulSoup(html, "html.parser")
            raw = self._extract_kr_profile_text(soup)
            if not raw:
                return None
            lines = self._normalize_lines(raw, max_lines=5)
            if len(lines) < 2:
                return None
            return CollectedStockProfile(
                stock_code=stock.code,
                market="KR",
                description_ko="\n".join(lines),
                description_raw=raw,
                source="naver_profile",
                source_url=f"{NAVER_ITEM_MAIN_URL}?code={stock.code}",
            )
        except Exception:
            return None

    def _collect_us_yahoo(self, stock: Stock) -> CollectedStockProfile | None:
        ticker = stock.code.strip().upper()
        try:
            response = None
            for attempt in range(2):
                crumb = self._ensure_yahoo_crumb(force_refresh=attempt > 0)
                params = {"modules": "assetProfile"}
                if crumb:
                    params["crumb"] = crumb
                response = self._get(
                    YAHOO_QUOTE_SUMMARY_URL.format(ticker=ticker),
                    params=params,
                    headers={"Referer": f"https://finance.yahoo.com/quote/{ticker}"},
                )
                if response.status_code == 401 and attempt == 0:
                    continue
                break
            if response is None:
                return None
            response.raise_for_status()
            data = response.json()
            result_list = ((data.get("quoteSummary") or {}).get("result") or [])
            if not result_list:
                return None
            raw = compact_text(str(((result_list[0].get("assetProfile") or {}).get("longBusinessSummary") or "")))
            if not raw:
                return None

            lines = self._summarize_us_profile_to_ko_lines(stock=stock, raw=raw)
            if not lines:
                return None
            return CollectedStockProfile(
                stock_code=stock.code,
                market="US",
                description_ko="\n".join(lines[:5]),
                description_raw=raw,
                source="yahoo_profile",
                source_url=f"https://finance.yahoo.com/quote/{ticker}/profile",
            )
        except Exception:
            return None

    def _collect_from_documents(self, conn, stock: Stock) -> CollectedStockProfile | None:
        rows = latest_documents(conn, stock.code, limit=120)
        if not rows:
            return None

        snippets = self._build_doc_snippets(rows, max_items=20)
        lines = self._summarize_docs_with_llm(stock=stock, snippets=snippets)
        if not lines:
            lines = self._summarize_docs_rule(stock=stock, snippets=snippets)
        if not lines:
            return None

        return CollectedStockProfile(
            stock_code=stock.code,
            market=str(stock.market or "").upper(),
            description_ko="\n".join(lines[:5]),
            description_raw="\n".join(snippets[:12]),
            source="derived_docs",
            source_url="",
        )

    def _build_doc_snippets(self, rows: list, *, max_items: int) -> list[str]:
        snippets: list[str] = []
        for row in rows:
            title = compact_text(str(row["title"] or ""))
            body = compact_text(str(row["body"] or ""))
            body_short = body[:180]
            merged = compact_text(f"{title} {body_short}")
            if not merged:
                continue
            snippets.append(merged)
            if len(snippets) >= max_items:
                break
        return snippets

    def _summarize_docs_with_llm(self, *, stock: Stock, snippets: list[str]) -> list[str]:
        if not snippets or not self.llm.enabled():
            return []
        system_prompt = (
            "당신은 한국어 금융 데이터 에디터입니다. "
            "출력은 JSON 객체만 반환하세요. "
            "투자 추천 문구(매수/매도/비중확대 등)는 금지입니다."
        )
        user_prompt = (
            f"종목명: {stock.name}\n"
            f"종목코드: {stock.code}\n"
            f"시장: {stock.market}\n\n"
            "아래 뉴스/리포트 발췌를 바탕으로 해당 기업을 소개하는 4~5문장을 한국어로 작성하세요.\n"
            "- 회사 정체성/주요 사업 중심\n"
            "- 최근 이슈는 보조 정보로만 반영\n"
            "- 추정/단정 표현 최소화\n"
            "- 투자 조언 금지\n\n"
            "응답 형식:\n"
            "{\"lines\": [\"문장1\", \"문장2\", \"문장3\", \"문장4\"]}\n\n"
            "자료:\n"
            + "\n".join(f"- {x}" for x in snippets[:20])
        )
        result = self.llm.generate_json(system_prompt, user_prompt, purpose="company_profile")
        if result is None:
            return []
        payload = result.payload or {}
        raw_lines = payload.get("lines")
        if not isinstance(raw_lines, list):
            return []
        lines = [compact_text(str(x)) for x in raw_lines if compact_text(str(x))]
        return lines[:5]

    def _summarize_docs_rule(self, *, stock: Stock, snippets: list[str]) -> list[str]:
        keywords = self._extract_keywords(" ".join(snippets), limit=6)
        market_label = "국내" if str(stock.market or "").upper() == "KR" else "미국"
        lines: list[str] = [
            f"{stock.name}({stock.code})는 {market_label} 증시에 상장된 기업입니다.",
        ]
        if keywords:
            lines.append(f"최근 수집 문서에서는 {'·'.join(keywords)} 관련 주제가 반복적으로 언급됩니다.")
        lines.append("기업 개요 원문 접근이 제한된 환경을 고려해, 뉴스·리포트 기반으로 설명을 구성했습니다.")
        lines.append("위 내용은 회사 소개 목적의 참고 정보이며, 투자 권유가 아닙니다.")
        return lines[:5]

    def _summarize_us_profile_to_ko_lines(self, *, stock: Stock, raw: str) -> list[str]:
        if self.llm.enabled():
            system_prompt = (
                "당신은 한국어 금융 문서 편집자입니다. "
                "JSON만 반환하세요. 투자 권유 표현은 금지합니다."
            )
            user_prompt = (
                f"아래는 {stock.name}({stock.code})의 영문 회사 소개입니다.\n"
                "핵심을 한국어 4~5문장으로 요약하세요.\n"
                "응답 형식: {\"lines\": [\"...\", \"...\", \"...\"]}\n\n"
                f"원문:\n{raw}"
            )
            result = self.llm.generate_json(system_prompt, user_prompt, purpose="company_profile")
            if result is not None:
                payload = result.payload or {}
                raw_lines = payload.get("lines")
                if isinstance(raw_lines, list):
                    lines = [compact_text(str(x)) for x in raw_lines if compact_text(str(x))]
                    if lines:
                        return lines[:5]
        return self._normalize_lines(raw, max_lines=5)

    def _extract_kr_profile_text(self, soup: BeautifulSoup) -> str:
        candidates: list[str] = []

        for th in soup.select("th"):
            label = compact_text(th.get_text(" ", strip=True))
            if "기업개요" in label:
                td = th.find_next("td")
                if td:
                    text = compact_text(td.get_text(" ", strip=True))
                    if text:
                        candidates.append(text)

        for text_node in soup.find_all(string=re.compile("기업개요")):
            parent = text_node.parent
            if parent is None:
                continue
            sibling = parent.find_next_sibling()
            if sibling:
                text = compact_text(sibling.get_text(" ", strip=True))
                if len(text) >= 20:
                    candidates.append(text)

        meta_desc = soup.select_one("meta[property='og:description']")
        if meta_desc and meta_desc.get("content"):
            text = compact_text(str(meta_desc.get("content")))
            if len(text) >= 30:
                candidates.append(text)

        if not candidates:
            return ""
        candidates = sorted(candidates, key=len, reverse=True)
        return candidates[0]

    def _normalize_lines(self, text: str, *, max_lines: int) -> list[str]:
        cleaned = compact_text(text)
        if not cleaned:
            return []
        # Keep sentence boundaries for fixed 4~5 line rendering.
        chunks = re.split(r"(?<=[.!?。])\s+|(?<=다\.)\s+", cleaned)
        lines = [compact_text(x) for x in chunks if compact_text(x)]
        if not lines:
            lines = [cleaned]
        if len(lines) < 4 and len(cleaned) > 240:
            lines = self._chunk_by_length(cleaned, chunk_size=110)
        return lines[:max_lines]

    @staticmethod
    def _chunk_by_length(text: str, *, chunk_size: int) -> list[str]:
        out: list[str] = []
        cur = []
        cur_len = 0
        for token in text.split(" "):
            t = compact_text(token)
            if not t:
                continue
            if cur_len + len(t) + 1 > chunk_size and cur:
                out.append(" ".join(cur))
                cur = [t]
                cur_len = len(t)
            else:
                cur.append(t)
                cur_len += len(t) + 1
        if cur:
            out.append(" ".join(cur))
        return out

    @staticmethod
    def _extract_keywords(text: str, *, limit: int) -> list[str]:
        stop = {
            "관련",
            "기반",
            "최근",
            "기업",
            "시장",
            "주가",
            "투자",
            "뉴스",
            "리포트",
            "대한",
            "통해",
            "에서",
            "으로",
            "및",
            "등",
        }
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9.+-]{1,}|[가-힣]{2,}", text)
        counts: dict[str, int] = {}
        for token in tokens:
            t = token.strip().lower()
            if t in stop:
                continue
            if t.isdigit():
                continue
            counts[t] = counts.get(t, 0) + 1
        ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
        return [k for k, _ in ranked[:limit]]

    def _ensure_yahoo_crumb(self, *, force_refresh: bool = False) -> str:
        if self._yahoo_crumb and not force_refresh:
            return self._yahoo_crumb
        self._get(YAHOO_COOKIE_BOOTSTRAP_URL, headers={"Referer": "https://finance.yahoo.com/"})
        response = self._get(YAHOO_CRUMB_URL, headers={"Referer": "https://finance.yahoo.com/"})
        if response.status_code == 200:
            crumb = compact_text(response.text or "")
            if crumb and "{" not in crumb:
                self._yahoo_crumb = crumb
                return crumb
        self._yahoo_crumb = ""
        return ""

    def _get(self, url: str, **kwargs):
        kwargs.setdefault("timeout", self.settings.request_timeout_sec)
        kwargs.setdefault("verify", self.verify)
        try:
            return self.session.get(url, **kwargs)
        except requests.exceptions.SSLError:
            kwargs["verify"] = False
            return self.session.get(url, **kwargs)
