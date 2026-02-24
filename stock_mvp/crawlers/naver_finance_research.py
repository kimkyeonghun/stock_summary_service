from __future__ import annotations

import re
from io import BytesIO
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from stock_mvp.models import CollectedDocument, Stock
from stock_mvp.utils import compact_text, normalize_url, parse_datetime_maybe

from .base import BaseCrawler

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency fallback at runtime
    PdfReader = None  # type: ignore[assignment]


class NaverFinanceResearchCrawler(BaseCrawler):
    source = "naver_finance_research"
    doc_type = "report"
    base_url = "https://finance.naver.com"
    list_path = "/research/company_list.naver"
    detail_max_chars = 2500
    pdf_max_pages = 12
    pdf_max_chars = 12000
    body_max_chars = 16000

    def __init__(self, settings):
        super().__init__(settings)
        self._page_rows_cache: dict[int, list[dict[str, object]]] = {}
        self._report_detail_cache: dict[str, tuple[str, str]] = {}
        self._pdf_text_cache: dict[str, str] = {}
        self._warned_missing_pdf_reader = False

    def reset_run_state(self) -> None:
        self._page_rows_cache.clear()
        self._report_detail_cache.clear()
        self._pdf_text_cache.clear()

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
                doc_key = normalize_url(doc.url) or doc.url
                if doc_key in seen_urls:
                    continue
                seen_urls.add(doc_key)
                docs.append(doc)
                if len(docs) >= limit:
                    return docs

        return docs

    def _collect_page(self, stock: Stock, page: int, limit: int) -> list[CollectedDocument]:
        page_rows = self._load_page_rows(page)
        if not page_rows:
            return []

        docs: list[CollectedDocument] = []
        for row in page_rows:
            item_code = str(row.get("item_code") or "")
            if item_code and item_code != stock.code:
                continue

            title = str(row.get("title") or "")
            href = str(row.get("href") or "")
            row_text = str(row.get("row_text") or "")
            if not title or not href:
                continue
            if not item_code and not self._looks_related(stock, title, row_text):
                continue

            url = self._resolve_report_url(href)
            published_at = row.get("published_at")
            body = self._build_report_body(report_url=url, row_text=row_text, title=title)
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

        rows = soup.select("table.type_1 tr")
        parsed_rows: list[dict[str, object]] = []
        for row in rows:
            title_link = row.select_one("a[href*='company_read.naver']")
            if not title_link:
                continue

            item_link = row.select_one("a[href*='item/main.naver?code=']")
            item_code = self._extract_code(item_link.get("href", "") if item_link else "")

            title = compact_text(title_link.get_text(" ", strip=True))
            href = title_link.get("href", "")
            if not title or not href:
                continue

            row_text = compact_text(row.get_text(" ", strip=True))
            date_text = ""
            date_cell = row.select_one("td.date")
            if date_cell:
                date_text = compact_text(date_cell.get_text(" ", strip=True))
            published_at = parse_datetime_maybe(date_text)
            parsed_rows.append(
                {
                    "item_code": item_code,
                    "title": title,
                    "href": href,
                    "row_text": row_text,
                    "published_at": published_at,
                }
            )
        self._page_rows_cache[page] = parsed_rows
        return parsed_rows

    def _build_report_body(self, report_url: str, row_text: str, title: str) -> str:
        base_body = row_text or title
        detail_text = ""
        pdf_text = ""
        try:
            detail_text, pdf_url = self._load_report_detail(report_url)
            if pdf_url:
                pdf_text = self._extract_pdf_text(pdf_url)
        except Exception as exc:
            print(f"[WARN] naver_finance_research detail parse failed: url={report_url} error={exc}")

        body_parts = [base_body]
        if detail_text:
            body_parts.append(detail_text[: self.detail_max_chars])
        if pdf_text:
            body_parts.append(pdf_text)
        merged = compact_text(" ".join(p for p in body_parts if p))
        if not merged:
            merged = title
        if len(merged) > self.body_max_chars:
            merged = merged[: self.body_max_chars]
        return merged

    def _load_report_detail(self, report_url: str) -> tuple[str, str]:
        cached = self._report_detail_cache.get(report_url)
        if cached is not None:
            return cached

        response = self._get(
            report_url,
            headers={"Referer": f"{self.base_url}{self.list_path}"},
        )
        response.raise_for_status()
        html = self._decode_finance_html(response.content)
        soup = BeautifulSoup(html, "html.parser")

        detail_text = self._extract_detail_text(soup)
        pdf_url = self._extract_pdf_url(soup, report_url, html)

        parsed = (detail_text, pdf_url)
        self._report_detail_cache[report_url] = parsed
        return parsed

    def _extract_detail_text(self, soup: BeautifulSoup) -> str:
        selectors = (
            "td.view_cnt",
            "div.view_cnt",
            "div.report_viewer",
            "div#contentarea_left",
        )
        candidates: list[str] = []
        for selector in selectors:
            for node in soup.select(selector):
                text = compact_text(node.get_text(" ", strip=True))
                if len(text) >= 80:
                    candidates.append(text)
        if not candidates:
            return ""
        best = max(candidates, key=len)
        if len(best) > self.detail_max_chars:
            return best[: self.detail_max_chars]
        return best

    def _extract_pdf_url(self, soup: BeautifulSoup, base_url: str, html: str) -> str:
        candidates: list[str] = []
        for link in soup.select("a[href]"):
            href = compact_text(str(link.get("href") or ""))
            if not href:
                continue
            text = compact_text(link.get_text(" ", strip=True)).lower()
            if href.lower().startswith("javascript:"):
                candidates.extend(self._extract_urls_from_script(href, base_url))
            else:
                absolute = urljoin(base_url, href)
                if self._looks_like_pdf_ref(href) or self._looks_like_pdf_ref(absolute) or "pdf" in text or "원문" in text:
                    candidates.append(absolute)
            onclick = compact_text(str(link.get("onclick") or ""))
            if onclick:
                candidates.extend(self._extract_urls_from_script(onclick, base_url))

        for script in soup.select("script"):
            script_text = script.get_text(" ", strip=True)
            if script_text:
                candidates.extend(self._extract_urls_from_script(script_text, base_url))
        candidates.extend(self._extract_urls_from_script(html, base_url))

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            normalized = compact_text(candidate)
            if (
                not normalized
                or normalized.lower().startswith("javascript:")
                or normalized.lower().startswith("mailto:")
                or normalized.startswith("#")
                or normalized in seen
            ):
                continue
            seen.add(normalized)
            deduped.append(normalized)

        for candidate in deduped:
            if self._looks_like_pdf_ref(candidate):
                return candidate
        for candidate in deduped:
            lower = candidate.lower()
            if "download" in lower or "downpdf" in lower:
                return candidate
        return ""

    def _extract_urls_from_script(self, script: str, base_url: str) -> list[str]:
        values: list[str] = []
        if not script:
            return values

        for token in re.findall(r"""['"]([^'"]+)['"]""", script):
            raw = compact_text(token)
            if not raw:
                continue
            if raw.lower().startswith("javascript:") or raw.lower().startswith("mailto:"):
                continue
            if self._looks_like_pdf_ref(raw) or "download" in raw.lower():
                values.append(urljoin(base_url, raw))

        for token in re.findall(r"(https?://[^\s'\"<>]+)", script):
            raw = compact_text(token)
            if self._looks_like_pdf_ref(raw) or "download" in raw.lower():
                values.append(raw)
        return values

    def _extract_pdf_text(self, pdf_url: str) -> str:
        cached = self._pdf_text_cache.get(pdf_url)
        if cached is not None:
            return cached

        if PdfReader is None:
            if not self._warned_missing_pdf_reader:
                print("[WARN] pypdf is not installed. Naver report PDF text extraction is disabled.")
                self._warned_missing_pdf_reader = True
            self._pdf_text_cache[pdf_url] = ""
            return ""

        try:
            response = self._get(
                pdf_url,
                headers={"Referer": self.base_url},
            )
            response.raise_for_status()
            reader = PdfReader(BytesIO(response.content))

            chunks: list[str] = []
            total_chars = 0
            for page in reader.pages[: self.pdf_max_pages]:
                try:
                    page_text = compact_text(page.extract_text() or "")
                except Exception:
                    page_text = ""
                if not page_text:
                    continue
                chunks.append(page_text)
                total_chars += len(page_text)
                if total_chars >= self.pdf_max_chars:
                    break

            parsed = compact_text(" ".join(chunks))
            if len(parsed) > self.pdf_max_chars:
                parsed = parsed[: self.pdf_max_chars]
            self._pdf_text_cache[pdf_url] = parsed
            return parsed
        except Exception as exc:
            print(f"[WARN] naver_finance_research pdf parse failed: url={pdf_url} error={exc}")
            self._pdf_text_cache[pdf_url] = ""
            return ""

    @staticmethod
    def _looks_like_pdf_ref(value: str) -> bool:
        lowered = (value or "").lower()
        if not lowered:
            return False
        return (
            ".pdf" in lowered
            or "downpdf" in lowered
            or "pdfdownload" in lowered
            or ("pdf" in lowered and ("download" in lowered or "attach" in lowered))
        )

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
        if raw.startswith("company_read.naver"):
            resolved = urljoin(f"{self.base_url}/research/", raw)
            return normalize_url(resolved) or resolved
        resolved = urljoin(self.base_url, raw)
        return normalize_url(resolved) or resolved

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
