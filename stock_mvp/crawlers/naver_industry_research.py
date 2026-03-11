from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from stock_mvp.models import SectorCollectedDocument, Stock
from stock_mvp.utils import compact_text, normalize_url, parse_datetime_maybe

from .base import BaseCrawler


class NaverIndustryResearchCrawler(BaseCrawler):
    source = "naver_finance_industry_research"
    doc_type = "report"
    base_url = "https://finance.naver.com"
    list_path = "/research/industry_list.naver"
    detail_max_chars = 7000

    def __init__(self, settings):
        super().__init__(settings)
        self._page_rows_cache: dict[int, list[dict[str, object]]] = {}
        self._detail_cache: dict[str, str] = {}

    def reset_run_state(self) -> None:
        self._page_rows_cache.clear()
        self._detail_cache.clear()

    # BaseCrawler contract (not used for sector report flow).
    def collect(self, stock: Stock, limit: int):  # type: ignore[override]
        return []

    def collect_sector_reports(self, *, limit: int, max_pages: int = 12) -> list[SectorCollectedDocument]:
        docs: list[SectorCollectedDocument] = []
        seen_urls: set[str] = set()
        hard_limit = max(1, limit)
        max_page = max(1, max_pages)
        for page in range(1, max_page + 1):
            rows = self._load_page_rows(page)
            if not rows:
                continue
            for row in rows:
                title = str(row.get("title") or "")
                href = str(row.get("href") or "")
                sector_name = str(row.get("sector_name") or "")
                published_at = row.get("published_at")
                row_text = str(row.get("row_text") or "")
                if not title or not href:
                    continue
                report_url = self._resolve_report_url(href)
                norm = normalize_url(report_url) or report_url
                if norm in seen_urls:
                    continue
                seen_urls.add(norm)
                body = self._build_report_body(report_url=report_url, row_text=row_text, title=title)
                docs.append(
                    SectorCollectedDocument(
                        sector_name=sector_name,
                        source=self.source,
                        doc_type=self.doc_type,
                        title=title,
                        url=report_url,
                        published_at=published_at if published_at is not None else None,
                        body=body,
                    )
                )
                if len(docs) >= hard_limit:
                    return docs
        return docs

    def _load_page_rows(self, page: int) -> list[dict[str, object]]:
        cached = self._page_rows_cache.get(page)
        if cached is not None:
            return cached

        response = self._get(
            f"{self.base_url}{self.list_path}",
            params={"page": str(page)},
        )
        response.raise_for_status()

        html = self._decode_finance_html(response.content)
        soup = BeautifulSoup(html, "html.parser")

        rows: list[dict[str, object]] = []
        for tr in soup.select("table.type_1 tr"):
            title_link = tr.select_one("a[href*='industry_read.naver']")
            if not title_link:
                continue
            title = compact_text(title_link.get_text(" ", strip=True))
            href = compact_text(title_link.get("href", ""))
            if not title or not href:
                continue
            row_text = compact_text(tr.get_text(" ", strip=True))
            sector_name = self._extract_sector_name(tr, title=title)
            published_at = self._extract_published_at(tr)
            rows.append(
                {
                    "title": title,
                    "href": href,
                    "sector_name": sector_name,
                    "published_at": published_at,
                    "row_text": row_text,
                }
            )

        self._page_rows_cache[page] = rows
        return rows

    def _build_report_body(self, *, report_url: str, row_text: str, title: str) -> str:
        base = compact_text(row_text or title)
        detail = self._load_report_detail(report_url)
        if detail:
            body = f"{base}\n\n{detail}".strip()
        else:
            body = base
        return compact_text(body)[: self.detail_max_chars]

    def _load_report_detail(self, report_url: str) -> str:
        cached = self._detail_cache.get(report_url)
        if cached is not None:
            return cached
        try:
            response = self._get(report_url)
            response.raise_for_status()
            html = self._decode_finance_html(response.content)
            soup = BeautifulSoup(html, "html.parser")
            text = self._extract_detail_text(soup)
            self._detail_cache[report_url] = text
            return text
        except Exception as exc:
            print(f"[WARN] naver_industry_research detail parse failed: url={report_url} error={exc}")
            self._detail_cache[report_url] = ""
            return ""

    def _extract_detail_text(self, soup: BeautifulSoup) -> str:
        selectors = [
            "table.view_r td.view_cnt",
            "table.view_r",
            "div#content",
            "div.articleCont",
            "body",
        ]
        best = ""
        for selector in selectors:
            node = soup.select_one(selector)
            if node is None:
                continue
            text = compact_text(node.get_text(" ", strip=True))
            if len(text) > len(best):
                best = text
        return best[: self.detail_max_chars]

    def _extract_sector_name(self, tr, *, title: str) -> str:
        # Most rows have first textual cell as sector name.
        cells = [compact_text(td.get_text(" ", strip=True)) for td in tr.select("td")]
        date_re = re.compile(r"\d{4}\.\d{2}\.\d{2}")
        for cell in cells:
            if not cell:
                continue
            if cell == title or title in cell:
                continue
            if date_re.search(cell):
                continue
            if cell.endswith("증권"):
                continue
            if len(cell) <= 1:
                continue
            return cell
        # Fallback: extract from title prefix like [반도체] ...
        bracket = re.search(r"^\s*\[(.+?)\]", title)
        if bracket:
            return compact_text(bracket.group(1))
        return ""

    def _extract_published_at(self, tr):
        date_cell = tr.select_one("td.date")
        if date_cell:
            return parse_datetime_maybe(compact_text(date_cell.get_text(" ", strip=True)))
        text = compact_text(tr.get_text(" ", strip=True))
        match = re.search(r"\d{4}\.\d{2}\.\d{2}", text)
        if match:
            return parse_datetime_maybe(match.group(0))
        return None

    @staticmethod
    def _decode_finance_html(payload: bytes) -> str:
        for encoding in ("euc-kr", "cp949", "utf-8"):
            try:
                return payload.decode(encoding)
            except UnicodeDecodeError:
                continue
        return payload.decode("utf-8", "ignore")

    def _resolve_report_url(self, href: str) -> str:
        raw = compact_text(href)
        if not raw:
            return self.base_url
        if raw.startswith("http://") or raw.startswith("https://"):
            return normalize_url(raw) or raw
        if raw.startswith("industry_read.naver"):
            resolved = urljoin(f"{self.base_url}/research/", raw)
            return normalize_url(resolved) or resolved
        resolved = urljoin(self.base_url, raw)
        return normalize_url(resolved) or resolved
