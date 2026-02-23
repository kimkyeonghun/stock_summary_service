from __future__ import annotations

import json
import time
from dataclasses import dataclass

import requests

from stock_mvp.config import Settings
from stock_mvp.crawlers.hankyung_consensus import HankyungConsensusCrawler
from stock_mvp.crawlers.naver_finance_research import NaverFinanceResearchCrawler
from stock_mvp.crawlers.naver_news import NaverNewsCrawler
from stock_mvp.crawlers.sec_edgar import SecEdgarCrawler
from stock_mvp.database import (
    create_pipeline_run,
    connect,
    finish_pipeline_run,
    init_db,
    insert_documents,
    list_stocks,
    recent_documents,
    record_crawler_run_stat,
    save_summary,
    upsert_stocks,
)
from stock_mvp.models import Stock
from stock_mvp.stocks import DEFAULT_STOCKS
from stock_mvp.summarizer import SummaryBuilder
from stock_mvp.utils import dedupe_document_dicts


@dataclass
class PipelineStats:
    run_id: int = 0
    stock_count: int = 0
    fetched_docs: int = 0
    inserted_docs: int = 0
    skipped_docs: int = 0
    summaries_written: int = 0
    error_count: int = 0


class CollectionPipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.summary_builder = SummaryBuilder()
        self.crawlers = [
            NaverNewsCrawler(settings),
            HankyungConsensusCrawler(settings),
            NaverFinanceResearchCrawler(settings),
            SecEdgarCrawler(settings),
        ]

    def run_once(self, stock_codes: list[str] | None = None, trigger_type: str = "manual") -> PipelineStats:
        with connect(self.settings.db_path) as conn:
            init_db(conn)
            stock_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM stocks").fetchone()["cnt"])
            if stock_count == 0:
                upsert_stocks(conn, DEFAULT_STOCKS)
            selected = self._selected_stocks(conn, stock_codes)
            requested = ",".join(stock_codes) if stock_codes else ""
            run_id = create_pipeline_run(
                conn,
                trigger_type=trigger_type,
                requested_stock_codes=requested,
                stock_count=len(selected),
            )
            stats = PipelineStats(run_id=run_id, stock_count=len(selected))

            try:
                for stock in selected:
                    for crawler in self.crawlers:
                        per_source = self._limit_for_crawler(crawler.source, crawler.doc_type)
                        docs, attempts, error_message, duration_ms = self._collect_with_retries(
                            crawler=crawler,
                            stock=stock,
                            limit=per_source,
                        )
                        if error_message:
                            stats.error_count += 1
                            print(f"[WARN] crawler={crawler.source} stock={stock.code} error={error_message}")

                        inserted, skipped = insert_documents(conn, docs)
                        stats.fetched_docs += len(docs)
                        stats.inserted_docs += inserted
                        stats.skipped_docs += skipped

                        record_crawler_run_stat(
                            conn,
                            run_id=run_id,
                            stock_code=stock.code,
                            source=crawler.source,
                            doc_type=crawler.doc_type,
                            fetched_count=len(docs),
                            inserted_count=inserted,
                            skipped_count=skipped,
                            error_message=error_message,
                            attempt_count=attempts,
                            duration_ms=duration_ms,
                        )

                    docs_for_summary = recent_documents(
                        conn,
                        stock_code=stock.code,
                        lookback_days=self.settings.summary_lookback_days,
                        limit=80,
                    )
                    summary_docs = dedupe_document_dicts([dict(r) for r in docs_for_summary])
                    summary = self.summary_builder.build(stock_code=stock.code, docs=summary_docs)
                    save_summary(conn, summary)
                    stats.summaries_written += 1

                finish_pipeline_run(
                    conn,
                    run_id,
                    fetched_docs=stats.fetched_docs,
                    inserted_docs=stats.inserted_docs,
                    skipped_docs=stats.skipped_docs,
                    summaries_written=stats.summaries_written,
                    error_count=stats.error_count,
                    status="completed",
                )
            except Exception as exc:
                finish_pipeline_run(
                    conn,
                    run_id,
                    fetched_docs=stats.fetched_docs,
                    inserted_docs=stats.inserted_docs,
                    skipped_docs=stats.skipped_docs,
                    summaries_written=stats.summaries_written,
                    error_count=stats.error_count + 1,
                    status="failed",
                    error_message=str(exc),
                )
                raise

        if self.settings.enable_telegram_error_alert and stats.error_count >= self.settings.ops_error_alert_threshold:
            self._send_error_alert(stats)

        return stats

    @staticmethod
    def _selected_stocks(conn, stock_codes: list[str] | None) -> list[Stock]:
        rows = list_stocks(conn)
        if stock_codes:
            code_set = {c.strip().upper() for c in stock_codes if c.strip()}
            rows = [r for r in rows if str(r["code"]).upper() in code_set]
        return [row_to_stock(r) for r in rows]

    def _limit_for_crawler(self, source: str, doc_type: str) -> int:
        if source == "naver_news":
            return max(1, self.settings.naver_news_per_stock)
        if source == "hankyung_consensus":
            return max(1, self.settings.hankyung_reports_per_stock)
        if source == "naver_finance_research":
            return max(1, self.settings.naver_finance_reports_per_stock)
        if source == "sec_edgar":
            return max(1, self.settings.sec_reports_per_stock)
        if doc_type == "news":
            return max(1, self.settings.news_per_stock)
        return max(1, self.settings.reports_per_stock)

    def _collect_with_retries(self, crawler, stock: Stock, limit: int) -> tuple[list, int, str | None, int]:
        max_retries = max(0, self.settings.crawler_max_retries)
        attempts = 0
        error_message: str | None = None
        start = time.perf_counter()
        docs = []
        for _ in range(max_retries + 1):
            attempts += 1
            try:
                docs = crawler.collect(stock, limit)
                error_message = None
                break
            except Exception as exc:  # noqa: PERF203
                error_message = str(exc)
        duration_ms = int((time.perf_counter() - start) * 1000)
        return docs, attempts, error_message, duration_ms

    def _send_error_alert(self, stats: PipelineStats) -> None:
        if not self.settings.telegram_bot_token or not self.settings.telegram_chat_id:
            return
        text = (
            "Collection Alert\n"
            f"run_id={stats.run_id}\n"
            f"stock_count={stats.stock_count}\n"
            f"fetched_docs={stats.fetched_docs}\n"
            f"inserted_docs={stats.inserted_docs}\n"
            f"skipped_docs={stats.skipped_docs}\n"
            f"summaries_written={stats.summaries_written}\n"
            f"error_count={stats.error_count}\n"
            f"threshold={self.settings.ops_error_alert_threshold}"
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage",
                data={"chat_id": self.settings.telegram_chat_id, "text": text},
                timeout=self.settings.request_timeout_sec,
            )
        except Exception:
            pass


def row_to_stock(row) -> Stock:
    return Stock(
        code=row["code"],
        name=row["name"],
        queries=json.loads(row["queries_json"]),
        market=row["market"],
        exchange=row["exchange"],
        currency=row["currency"],
        is_active=bool(row["is_active"]),
        universe_source=row["universe_source"],
        rank=row["rank"],
    )
