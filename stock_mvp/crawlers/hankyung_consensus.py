from __future__ import annotations

import re
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup

from stock_mvp.models import CollectedDocument, Stock
from stock_mvp.utils import compact_text, parse_datetime_maybe

from .base import BaseCrawler


class HankyungConsensusCrawler(BaseCrawler):
    source = "hankyung_consensus"
    doc_type = "report"
    base_url = "https://consensus.hankyung.com"
    list_path = "/analysis/list"

    def __init__(self, settings):
        super().__init__(settings)
        self._page_rows_cache: dict[int, list[dict[str, object]]] = {}

    def reset_run_state(self) -> None:
        self._page_rows_cache.clear()

    def collect(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        if stock.market != "KR":
            return []
        docs: list[CollectedDocument] = []
        seen_urls: set[str] = set()
        for page in range(1, 6):
            page_docs = self._collect_with_params(
                stock,
                {"report_type": "CO", "pagenum": "20", "now_page": str(page)},
                limit=limit,
            )
            if not page_docs:
                break
            for doc in page_docs:
                if doc.url in seen_urls:
                    continue
                seen_urls.add(doc.url)
                docs.append(doc)
                if len(docs) >= limit:
                    return docs
        return docs

    def _collect_with_params(self, stock: Stock, params: dict[str, str], limit: int) -> list[CollectedDocument]:
        try:
            page = int(str(params.get("now_page") or "1"))
        except ValueError:
            page = 1
        rows = self._load_page_rows(page)
        docs: list[CollectedDocument] = []
        for row in rows:
            title = str(row.get("title") or "")
            full_url = str(row.get("url") or "")
            row_text = str(row.get("row_text") or "")
            published_at = row.get("published_at")
            if not self._looks_related(stock, title, row_text):
                continue
            docs.append(
                CollectedDocument(
                    stock_code=stock.code,
                    source=self.source,
                    doc_type=self.doc_type,
                    title=title,
                    url=full_url,
                    published_at=published_at,
                    body=row_text,
                )
            )
            if len(docs) >= limit:
                return docs

        return docs

    def _load_page_rows(self, page: int) -> list[dict[str, object]]:
        cached = self._page_rows_cache.get(page)
        if cached is not None:
            return cached
        url = f"{self.base_url}{self.list_path}"
        response = self._get(
            url,
            params={"report_type": "CO", "pagenum": "20", "now_page": str(page)},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        parsed_rows: list[dict[str, object]] = []
        for row in soup.select("table tbody tr"):
            link = row.select_one("a[href]")
            if not link:
                continue
            cells = row.select("td")
            title = compact_text(cells[1].get_text(" ", strip=True) if len(cells) > 1 else link.get_text(" ", strip=True))
            href = link.get("href")
            if not href:
                continue
            full_url = urljoin(self.base_url, href)
            row_text = compact_text(row.get_text(" ", strip=True))
            parsed_rows.append(
                {
                    "title": title,
                    "url": full_url,
                    "row_text": row_text,
                    "published_at": self._parse_date_from_text(row_text),
                }
            )
        self._page_rows_cache[page] = parsed_rows
        return parsed_rows

    @staticmethod
    def _looks_related(stock: Stock, *texts: str) -> bool:
        merged = " ".join(texts)
        for q in stock.queries:
            if q and q.lower() in merged.lower():
                return True
        return False

    @staticmethod
    def _parse_date_from_text(text: str):
        match = re.search(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", text)
        if not match:
            return None
        return parse_datetime_maybe(match.group(0))

    def debug_url(self, query: str) -> str:
        return f"{self.base_url}{self.list_path}?{urlencode({'report_type': 'CO', 'search_text': query})}"
