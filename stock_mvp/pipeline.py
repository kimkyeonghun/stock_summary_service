from __future__ import annotations

import json
import time
from dataclasses import replace
from dataclasses import dataclass, field

import requests

from stock_mvp.agents.entity_digest import EntityDigestAgent
from stock_mvp.agents.item_summarizer import ItemSummarizerAgent
from stock_mvp.agents.report_writer import ReportWriterAgent
from stock_mvp.config import Settings
from stock_mvp.crawlers.naver_finance_research import NaverFinanceResearchCrawler
from stock_mvp.crawlers.naver_news import NaverNewsCrawler
from stock_mvp.crawlers.sec_edgar import SecEdgarCrawler
from stock_mvp.database import (
    create_pipeline_run,
    connect,
    financial_refresh_needed,
    finish_pipeline_run,
    init_db,
    insert_documents,
    list_stocks,
    record_crawler_run_stat,
    upsert_financial_snapshot,
    upsert_stocks,
)
from stock_mvp.financials import FinancialCollector
from stock_mvp.models import Stock
from stock_mvp.relevance import evaluate_stock_document_relevance, passes_relevance
from stock_mvp.stocks import DEFAULT_STOCKS


@dataclass
class PipelineStats:
    run_id: int = 0
    stock_count: int = 0
    fetched_docs: int = 0
    inserted_docs: int = 0
    skipped_docs: int = 0
    summaries_written: int = 0
    item_summaries_written: int = 0
    ticker_digests_written: int = 0
    ticker_reports_written: int = 0
    sector_digests_written: int = 0
    sector_reports_written: int = 0
    agent_error_count: int = 0
    sector_docs_written: int = 0
    sector_doc_links_written: int = 0
    sector_summaries_written: int = 0
    sector_summary_error_count: int = 0
    financial_snapshots_written: int = 0
    financial_snapshots_skipped: int = 0
    financial_error_count: int = 0
    error_count: int = 0
    error_details: list[str] = field(default_factory=list)


class PipelineBusyError(RuntimeError):
    pass


class CollectionPipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.item_summarizer_agent = ItemSummarizerAgent()
        self.entity_digest_agent = EntityDigestAgent()
        self.report_writer_agent = ReportWriterAgent()
        self.financial_collector = FinancialCollector(settings)
        self.crawlers = [
            NaverNewsCrawler(settings),
            NaverFinanceResearchCrawler(settings),
            SecEdgarCrawler(settings),
        ]

    def run_once(
        self,
        stock_codes: list[str] | None = None,
        market: str | None = None,
        trigger_type: str = "manual",
        include_sector_steps: bool = True,
    ) -> PipelineStats:
        with connect(self.settings.db_path) as conn:
            init_db(conn)
            stock_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM stocks").fetchone()["cnt"])
            if stock_count == 0:
                upsert_stocks(conn, DEFAULT_STOCKS)
            selected = self._selected_stocks(conn, stock_codes, market=market)
            requested = ",".join(stock_codes) if stock_codes else ""
            run_id = self._create_pipeline_run_with_lock(
                conn,
                trigger_type=trigger_type,
                requested_stock_codes=requested,
                stock_count=len(selected),
            )
            stats = PipelineStats(run_id=run_id, stock_count=len(selected))
            for crawler in self.crawlers:
                if hasattr(crawler, "reset_run_state"):
                    crawler.reset_run_state()

            try:
                for stock in selected:
                    for crawler in self.crawlers:
                        per_source = self._limit_for_crawler(crawler.source, crawler.doc_type)
                        docs, attempts, error_message, duration_ms = self._collect_with_retries(
                            crawler=crawler,
                            stock=stock,
                            limit=per_source,
                        )
                        filtered_docs, relevance_dropped = self._filter_docs_by_relevance(
                            stock=stock,
                            source=crawler.source,
                            doc_type=crawler.doc_type,
                            docs=docs,
                        )
                        if error_message:
                            stats.error_count += 1
                            detail = (
                                f"crawler source={crawler.source} stock={stock.code} "
                                f"attempts={attempts} error={error_message}"
                            )
                            stats.error_details.append(detail)
                            print(f"[WARN] {detail}")

                        inserted, skipped = insert_documents(conn, filtered_docs, commit=False)
                        stats.fetched_docs += len(docs)
                        stats.inserted_docs += inserted
                        stats.skipped_docs += skipped + relevance_dropped
                        if relevance_dropped > 0:
                            print(
                                f"[INFO] relevance filtered: source={crawler.source} "
                                f"stock={stock.code} dropped={relevance_dropped}/{len(docs)}"
                        )

                        record_crawler_run_stat(
                            conn,
                            run_id=run_id,
                            stock_code=stock.code,
                            source=crawler.source,
                            doc_type=crawler.doc_type,
                            fetched_count=len(docs),
                            inserted_count=inserted,
                            skipped_count=skipped + relevance_dropped,
                            error_message=error_message,
                            attempt_count=attempts,
                            duration_ms=duration_ms,
                            commit=False,
                        )

                    if self.settings.enable_financial_collection:
                        refresh_needed = financial_refresh_needed(
                            conn,
                            stock.code,
                            min_hours=self.settings.financial_refresh_min_hours,
                        )
                        if refresh_needed:
                            try:
                                snapshot = self.financial_collector.collect(stock)
                                if snapshot is not None:
                                    upsert_financial_snapshot(conn, snapshot, commit=False)
                                    stats.financial_snapshots_written += 1
                                else:
                                    stats.financial_snapshots_skipped += 1
                            except Exception as exc:
                                stats.financial_error_count += 1
                                stats.error_count += 1
                                detail = f"financial stock={stock.code} error={exc}"
                                stats.error_details.append(detail)
                                print(f"[WARN] {detail}")
                        else:
                            stats.financial_snapshots_skipped += 1
                    conn.commit()

                self._run_agent_steps(conn, selected=selected, include_sector_steps=include_sector_steps, stats=stats)
                # Keep legacy field for pipeline_runs compatibility.
                stats.summaries_written = stats.ticker_digests_written

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
            except BaseException as exc:
                stats.error_details.append(f"pipeline_failed error={exc}")
                error_message = str(exc).strip() or exc.__class__.__name__
                finish_pipeline_run(
                    conn,
                    run_id,
                    fetched_docs=stats.fetched_docs,
                    inserted_docs=stats.inserted_docs,
                    skipped_docs=stats.skipped_docs,
                    summaries_written=stats.summaries_written,
                    error_count=stats.error_count + 1,
                    status="failed",
                    error_message=error_message,
                )
                raise

        if self.settings.enable_telegram_error_alert and stats.error_count >= self.settings.ops_error_alert_threshold:
            self._send_error_alert(stats)

        return stats

    def _run_agent_steps(
        self,
        conn,
        *,
        selected: list[Stock],
        include_sector_steps: bool,
        stats: PipelineStats,
    ) -> None:
        by_market = self._group_stock_codes_by_market(selected)
        for market_code, ticker_codes in by_market.items():
            market = market_code.lower()
            item_stats = self.item_summarizer_agent.run(
                conn,
                market=market,
                ticker_codes=ticker_codes,
                lookback_days=self.settings.summary_lookback_days,
                limit=max(200, len(ticker_codes) * 20),
            )
            digest_stats = self.entity_digest_agent.run(
                conn,
                entity_type="ticker",
                entity_ids=ticker_codes,
                market=market,
                lookback_days=self.settings.summary_lookback_days,
            )
            report_stats = self.report_writer_agent.run(
                conn,
                entity_type="ticker",
                entity_ids=ticker_codes,
                market=market,
                lookback_days=14,
            )

            stats.item_summaries_written += item_stats.created
            stats.ticker_digests_written += digest_stats.created
            stats.ticker_reports_written += report_stats.created
            self._merge_agent_errors(
                stats,
                market=market_code,
                scope="ticker",
                item_errors=item_stats.errors,
                digest_errors=digest_stats.errors,
                report_errors=report_stats.errors,
            )

            if not include_sector_steps:
                continue

            sector_codes = self._sector_codes_for_stocks(conn, market=market_code, stock_codes=ticker_codes)
            if not sector_codes:
                continue

            sector_digest_stats = self.entity_digest_agent.run(
                conn,
                entity_type="sector",
                entity_ids=sector_codes,
                market=market,
                lookback_days=self.settings.summary_lookback_days,
            )
            sector_report_stats = self.report_writer_agent.run(
                conn,
                entity_type="sector",
                entity_ids=sector_codes,
                market=market,
                lookback_days=14,
            )
            stats.sector_digests_written += sector_digest_stats.created
            stats.sector_reports_written += sector_report_stats.created
            # Keep legacy field names in sync.
            stats.sector_summaries_written = stats.sector_digests_written
            self._merge_agent_errors(
                stats,
                market=market_code,
                scope="sector",
                item_errors=0,
                digest_errors=sector_digest_stats.errors,
                report_errors=sector_report_stats.errors,
            )

    @staticmethod
    def _group_stock_codes_by_market(selected: list[Stock]) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for stock in selected:
            market = str(stock.market or "").upper()
            grouped.setdefault(market, []).append(stock.code)
        return grouped

    @staticmethod
    def _sector_codes_for_stocks(conn, *, market: str, stock_codes: list[str]) -> list[str]:
        if not stock_codes:
            return []
        placeholders = ",".join("?" for _ in stock_codes)
        sql = f"""
        SELECT DISTINCT m.sector_code
        FROM stock_sector_map m
        JOIN stocks s ON s.code = m.stock_code
        WHERE lower(s.market) = lower(?)
          AND s.is_active = 1
          AND m.stock_code IN ({placeholders})
        ORDER BY m.sector_code
        """
        rows = conn.execute(sql, (market.lower(), *stock_codes)).fetchall()
        return [str(r["sector_code"]) for r in rows]

    @staticmethod
    def _merge_agent_errors(
        stats: PipelineStats,
        *,
        market: str,
        scope: str,
        item_errors: int,
        digest_errors: int,
        report_errors: int,
    ) -> None:
        if item_errors:
            detail = f"agent item_summary market={market} scope={scope} errors={item_errors}"
            stats.error_details.append(detail)
            print(f"[WARN] {detail}")
        if digest_errors:
            detail = f"agent digest market={market} scope={scope} errors={digest_errors}"
            stats.error_details.append(detail)
            print(f"[WARN] {detail}")
        if report_errors:
            detail = f"agent report market={market} scope={scope} errors={report_errors}"
            stats.error_details.append(detail)
            print(f"[WARN] {detail}")
        total = item_errors + digest_errors + report_errors
        stats.agent_error_count += total
        stats.error_count += total

    @staticmethod
    def _create_pipeline_run_with_lock(
        conn,
        *,
        trigger_type: str,
        requested_stock_codes: str,
        stock_count: int,
    ) -> int:
        conn.execute("BEGIN IMMEDIATE")
        running = conn.execute(
            """
            SELECT id, trigger_type, started_at
            FROM pipeline_runs
            WHERE status = 'running'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if running is not None:
            conn.rollback()
            raise PipelineBusyError(
                "another collection run is already running "
                f"(run_id={int(running['id'])}, trigger={running['trigger_type']}, started_at={running['started_at']})"
            )
        return create_pipeline_run(
            conn,
            trigger_type=trigger_type,
            requested_stock_codes=requested_stock_codes,
            stock_count=stock_count,
        )

    @staticmethod
    def _selected_stocks(conn, stock_codes: list[str] | None, market: str | None = None) -> list[Stock]:
        rows = list_stocks(conn)
        if market:
            market_upper = market.strip().upper()
            rows = [r for r in rows if str(r["market"]).upper() == market_upper]
        if stock_codes:
            code_set = {c.strip().upper() for c in stock_codes if c.strip()}
            rows = [r for r in rows if str(r["code"]).upper() in code_set]
        return [row_to_stock(r) for r in rows]

    def _limit_for_crawler(self, source: str, doc_type: str) -> int:
        if source == "naver_news":
            return max(1, self.settings.naver_news_per_stock)
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

    @staticmethod
    def _filter_docs_by_relevance(stock: Stock, source: str, doc_type: str, docs: list) -> tuple[list, int]:
        kept: list = []
        dropped = 0
        for doc in docs:
            result = evaluate_stock_document_relevance(
                stock,
                title=str(doc.title or ""),
                body=str(doc.body or ""),
                url=str(doc.url or ""),
                source=source,
                doc_type=doc_type,
            )
            if not passes_relevance(result, source=source, doc_type=doc_type):
                dropped += 1
                continue
            kept.append(
                replace(
                    doc,
                    relevance_score=result.score,
                    relevance_reason=result.reason,
                    matched_alias=result.matched_alias,
                )
            )
        return kept, dropped

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
            f"item_summaries_written={stats.item_summaries_written}\n"
            f"ticker_digests_written={stats.ticker_digests_written}\n"
            f"ticker_reports_written={stats.ticker_reports_written}\n"
            f"sector_digests_written={stats.sector_digests_written}\n"
            f"sector_reports_written={stats.sector_reports_written}\n"
            f"agent_error_count={stats.agent_error_count}\n"
            f"sector_docs_written={stats.sector_docs_written}\n"
            f"sector_doc_links_written={stats.sector_doc_links_written}\n"
            f"sector_summaries_written={stats.sector_summaries_written}\n"
            f"sector_summary_error_count={stats.sector_summary_error_count}\n"
            f"financial_snapshots_written={stats.financial_snapshots_written}\n"
            f"financial_snapshots_skipped={stats.financial_snapshots_skipped}\n"
            f"financial_error_count={stats.financial_error_count}\n"
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
