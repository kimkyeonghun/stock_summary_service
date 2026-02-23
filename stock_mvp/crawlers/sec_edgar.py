from __future__ import annotations

import re

from stock_mvp.models import CollectedDocument, Stock
from stock_mvp.utils import compact_text, parse_datetime_maybe

from .base import BaseCrawler


class SecEdgarCrawler(BaseCrawler):
    source = "sec_edgar"
    doc_type = "report"
    ticker_map_url = "https://www.sec.gov/files/company_tickers.json"
    submissions_url = "https://data.sec.gov/submissions/CIK{cik}.json"
    target_forms = {"10-K", "10-Q", "8-K"}

    def __init__(self, settings):
        super().__init__(settings)
        self._ticker_to_cik: dict[str, str] = {}
        self._ticker_map_loaded = False
        self.session.headers.update(
            {
                "User-Agent": settings.sec_user_agent,
                "Accept-Encoding": "gzip, deflate",
            }
        )

    def collect(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        if stock.market != "US":
            return []

        cik = self._get_cik_for_ticker(stock.code)
        if not cik:
            return []

        response = self._get(self.submissions_url.format(cik=cik))
        response.raise_for_status()
        payload = response.json()

        recent = payload.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        accession_numbers = recent.get("accessionNumber", [])
        primary_documents = recent.get("primaryDocument", [])

        docs: list[CollectedDocument] = []
        for idx, form in enumerate(forms):
            if form not in self.target_forms:
                continue
            if idx >= len(filing_dates) or idx >= len(accession_numbers) or idx >= len(primary_documents):
                continue

            filing_date = filing_dates[idx]
            accession_number = accession_numbers[idx]
            primary_document = primary_documents[idx]
            if not accession_number or not primary_document:
                continue

            accession_nodash = accession_number.replace("-", "")
            cik_int = str(int(cik))
            url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/"
                f"{primary_document}"
            )

            title = compact_text(f"{stock.code} {form} filing ({filing_date})")
            body = compact_text(
                f"SEC filing detected: ticker={stock.code}, form={form}, filing_date={filing_date}."
            )
            docs.append(
                CollectedDocument(
                    stock_code=stock.code,
                    source=self.source,
                    doc_type=self.doc_type,
                    title=title,
                    url=url,
                    published_at=parse_datetime_maybe(filing_date),
                    body=body,
                )
            )
            if len(docs) >= limit:
                break
        return docs

    def _get_cik_for_ticker(self, ticker: str) -> str:
        normalized = re.sub(r"\W+", "", (ticker or "").upper())
        if not normalized:
            return ""
        if not self._ticker_map_loaded:
            try:
                self._load_ticker_map()
            except Exception as exc:
                # Avoid repeated network failures for each stock in one run.
                print(f"[WARN] sec ticker map load failed: {exc}")
                self._ticker_map_loaded = True
                return ""
        return self._ticker_to_cik.get(normalized, "")

    def _load_ticker_map(self) -> None:
        response = self._get(self.ticker_map_url)
        response.raise_for_status()
        payload = response.json()
        mapping: dict[str, str] = {}

        if isinstance(payload, dict):
            for value in payload.values():
                if not isinstance(value, dict):
                    continue
                ticker = re.sub(r"\W+", "", str(value.get("ticker", "")).upper())
                cik = str(value.get("cik_str", "")).strip()
                if not ticker or not cik:
                    continue
                mapping[ticker] = cik.zfill(10)

        self._ticker_to_cik = mapping
        self._ticker_map_loaded = True
