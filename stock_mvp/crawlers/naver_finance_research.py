from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from stock_mvp.models import CollectedDocument, Stock
from stock_mvp.utils import compact_text, parse_datetime_maybe

from .base import BaseCrawler


class NaverFinanceResearchCrawler(BaseCrawler):
    source = "naver_finance_research"
    doc_type = "report"
    base_url = "https://finance.naver.com"
    list_path = "/research/company_list.naver"

    def collect(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        if stock.market != "KR":
            return []

        docs: list[CollectedDocument] = []
        seen_urls: set[str] = set()

        # Scan recent pages until enough reports for the stock are collected.
        for page in range(1, 15):
            page_docs = self._collect_page(stock=stock, page=page, limit=limit)
            if not page_docs:
                continue
            for doc in page_docs:
                if doc.url in seen_urls:
                    continue
                seen_urls.add(doc.url)
                docs.append(doc)
                if len(docs) >= limit:
                    return docs

        return docs

    def _collect_page(self, stock: Stock, page: int, limit: int) -> list[CollectedDocument]:
        response = self._get(
            f"{self.base_url}{self.list_path}",
            params={"page": str(page)},
        )
        response.raise_for_status()

        # Naver finance pages are commonly encoded with euc-kr/cp949.
        html = response.content.decode("euc-kr", "ignore")
        soup = BeautifulSoup(html, "html.parser")

        rows = soup.select("table.type_1 tr")
        docs: list[CollectedDocument] = []
        for row in rows:
            title_link = row.select_one("a[href*='company_read.naver']")
            if not title_link:
                continue

            item_link = row.select_one("a[href*='item/main.naver?code=']")
            item_code = self._extract_code(item_link.get("href", "") if item_link else "")
            if item_code and item_code != stock.code:
                continue

            title = compact_text(title_link.get_text(" ", strip=True))
            href = title_link.get("href", "")
            if not title or not href:
                continue

            # Fallback match if code was not found in row.
            row_text = compact_text(row.get_text(" ", strip=True))
            if not item_code and not self._looks_related(stock, title, row_text):
                continue

            url = urljoin(self.base_url, href)

            date_text = ""
            date_cell = row.select_one("td.date")
            if date_cell:
                date_text = compact_text(date_cell.get_text(" ", strip=True))
            published_at = parse_datetime_maybe(date_text)

            docs.append(
                CollectedDocument(
                    stock_code=stock.code,
                    source=self.source,
                    doc_type=self.doc_type,
                    title=title,
                    url=url,
                    published_at=published_at,
                    body=row_text or title,
                )
            )
            if len(docs) >= limit:
                break
        return docs

    @staticmethod
    def _extract_code(href: str) -> str:
        match = re.search(r"code=(\d{6})", href or "")
        if not match:
            return ""
        return match.group(1)

    @staticmethod
    def _looks_related(stock: Stock, *texts: str) -> bool:
        merged = " ".join(texts).lower()
        for q in stock.queries:
            if q and q.lower() in merged:
                return True
        return False

