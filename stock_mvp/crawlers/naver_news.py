from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from stock_mvp.models import CollectedDocument, Stock
from stock_mvp.utils import compact_text, now_utc, parse_datetime_maybe

from .base import BaseCrawler


class NaverNewsCrawler(BaseCrawler):
    source = "naver_news"
    doc_type = "news"
    api_url = "https://openapi.naver.com/v1/search/news.json"
    search_url = "https://m.search.naver.com/search.naver"

    def collect(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        if stock.market != "KR":
            return []
        if self.settings.naver_client_id and self.settings.naver_client_secret:
            api_docs = self._collect_from_openapi(stock, limit)
            if api_docs:
                return api_docs
        return self._collect_from_mobile_search(stock, limit)

    def _collect_from_openapi(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        query = stock.queries[0]
        headers = {
            "X-Naver-Client-Id": self.settings.naver_client_id,
            "X-Naver-Client-Secret": self.settings.naver_client_secret,
        }
        response = self._get(
            self.api_url,
            headers=headers,
            params={"query": query, "display": min(max(limit, 1), 100), "sort": "date"},
        )
        response.raise_for_status()
        payload = response.json()

        docs: list[CollectedDocument] = []
        for item in payload.get("items", []):
            raw_title = item.get("title", "")
            raw_desc = item.get("description", "")
            title = compact_text(self._strip_tags(raw_title))
            body = compact_text(self._strip_tags(raw_desc)) or title
            url = item.get("originallink") or item.get("link") or ""
            if not title or not url:
                continue
            published_at = parse_datetime_maybe(item.get("pubDate"))
            docs.append(
                CollectedDocument(
                    stock_code=stock.code,
                    source=self.source,
                    doc_type=self.doc_type,
                    title=title,
                    url=url,
                    published_at=published_at,
                    body=body,
                )
            )
            if len(docs) >= limit:
                break
        return docs

    def _collect_from_mobile_search(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        query = stock.queries[0]
        response = self._get(
            self.search_url,
            params={"where": "m_news", "query": query, "sort": "1"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        docs: list[CollectedDocument] = []
        seen_urls: set[str] = set()
        links = soup.select("a[href*='n.news.naver.com/article']")
        for link in links:
            title = compact_text(link.get("title") or link.get_text(" ", strip=True))
            if len(title) < 8:
                continue
            raw_url = link.get("href")
            if not raw_url:
                continue
            url = urljoin("https://search.naver.com", raw_url)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            container = link.find_parent("li") or link.find_parent("div")
            raw_context = compact_text(container.get_text(" ", strip=True) if container else "")
            body = raw_context.replace(title, "", 1).strip()[:300]
            if not body:
                body = title

            infos = self._extract_time_candidates(raw_context)
            published_at = None
            for info in reversed(infos):
                parsed = parse_datetime_maybe(info)
                if parsed:
                    if parsed > now_utc():
                        continue
                    published_at = parsed
                    break

            docs.append(
                CollectedDocument(
                    stock_code=stock.code,
                    source=self.source,
                    doc_type=self.doc_type,
                    title=title,
                    url=url,
                    published_at=published_at,
                    body=body,
                )
            )
            if len(docs) >= limit:
                break

        return docs

    @staticmethod
    def _strip_tags(value: str) -> str:
        return re.sub(r"<[^>]+>", " ", value or "")

    @staticmethod
    def _extract_time_candidates(text: str) -> list[str]:
        patterns = [
            r"\d+\s*분\s*전",
            r"\d+\s*시간\s*전",
            r"\d+\s*일\s*전",
            r"\d{4}[./-]\d{1,2}[./-]\d{1,2}\.?",
        ]
        candidates: list[str] = []
        for pattern in patterns:
            candidates.extend(re.findall(pattern, text))
        return candidates

