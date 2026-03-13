from __future__ import annotations

import json
import re
import time
from dataclasses import replace
from dataclasses import dataclass, field

import requests

from stock_mvp.agents.entity_digest import EntityDigestAgent
from stock_mvp.agents.item_summarizer import ItemSummarizerAgent
from stock_mvp.agents.report_writer import ReportWriterAgent
from stock_mvp.agents.translator import get_translation_metrics, reset_translation_metrics
from stock_mvp.agents.base import confidence_weight, extract_topics, source_type_from_item, split_sentences
from stock_mvp.config import Settings
from stock_mvp.crawlers.naver_finance_research import NaverFinanceResearchCrawler
from stock_mvp.crawlers.naver_industry_research import NaverIndustryResearchCrawler
from stock_mvp.crawlers.naver_news import NaverNewsCrawler
from stock_mvp.crawlers.opendart_disclosure import OpenDartDisclosureCrawler
from stock_mvp.crawlers.sec_edgar import SecEdgarCrawler
from stock_mvp.database import (
    clear_news_entity_map_for_item,
    create_pipeline_run,
    connect,
    financial_refresh_needed,
    finish_pipeline_run,
    init_db,
    insert_documents,
    list_sectors,
    list_stocks,
    recent_mapped_sector_entities,
    record_crawler_run_stat,
    upsert_news_entity_map,
    upsert_document_entity_mapping,
    upsert_financial_snapshot,
    upsert_report_pdf_extract_by_identity,
    upsert_sector_document_by_code,
    upsert_sector_documents,
    upsert_stocks,
)
from stock_mvp.entity_mapping import map_document_to_primary_ticker
from stock_mvp.financials import FinancialCollector
from stock_mvp.models import SectorCollectedDocument, Stock
from stock_mvp.news_ingest import run_kr_rss_news_stage
from stock_mvp.relevance import evaluate_stock_document_relevance, passes_relevance
from stock_mvp.stocks import DEFAULT_STOCKS
from stock_mvp.storage import evidence_repo
from stock_mvp.translation_backfill import run_incremental_backfill
from stock_mvp.utils import compact_text, normalize_url, now_utc_iso, to_iso_or_none, url_hash


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
    general_economy_mapped: int = 0
    rss_source_count: int = 0
    rss_raw_fetched: int = 0
    rss_raw_inserted: int = 0
    rss_raw_url_duplicates: int = 0
    rss_raw_content_duplicates: int = 0
    rss_normalized: int = 0
    rss_mapped_ticker: int = 0
    rss_mapped_sector: int = 0
    rss_unassigned: int = 0
    rss_routed_documents: int = 0
    rss_routed_sector_documents: int = 0
    financial_snapshots_written: int = 0
    financial_snapshots_skipped: int = 0
    financial_error_count: int = 0
    error_count: int = 0
    error_details: list[str] = field(default_factory=list)
    collect_phase_elapsed_sec: float = 0.0
    sector_collect_elapsed_sec: float = 0.0
    agent_phase_elapsed_sec: float = 0.0
    total_elapsed_sec: float = 0.0
    translation_calls: int = 0
    translation_cache_hits: int = 0
    translation_elapsed_sec: float = 0.0
    translation_fail_count: int = 0


class PipelineBusyError(RuntimeError):
    pass


class CollectionPipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.item_summarizer_agent = ItemSummarizerAgent(settings)
        self.entity_digest_agent = EntityDigestAgent(settings)
        self.report_writer_agent = ReportWriterAgent(settings)
        self.financial_collector = FinancialCollector(settings)
        self.industry_research_crawler = NaverIndustryResearchCrawler(settings)
        self.crawlers = [
            NaverNewsCrawler(settings),
            NaverFinanceResearchCrawler(settings),
            SecEdgarCrawler(settings),
            OpenDartDisclosureCrawler(settings),
        ]

    def run_once(
        self,
        stock_codes: list[str] | None = None,
        market: str | None = None,
        trigger_type: str = "manual",
        include_agent_steps: bool = True,
        include_sector_steps: bool = True,
        collect_news: bool = True,
        collect_reports: bool = True,
        collect_filings: bool = True,
        collect_financials: bool = True,
    ) -> PipelineStats:
        run_started = time.perf_counter()
        reset_translation_metrics()
        run_started_at_iso = now_utc_iso()
        with connect(self.settings.db_path) as conn:
            init_db(conn)
            stock_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM stocks").fetchone()["cnt"])
            if stock_count == 0:
                upsert_stocks(conn, DEFAULT_STOCKS)
            selected = self._selected_stocks(conn, stock_codes, market=market)
            market_stock_map = self._stocks_by_market(conn)
            requested = ",".join(stock_codes) if stock_codes else ""
            run_id = self._create_pipeline_run_with_lock(
                conn,
                trigger_type=trigger_type,
                requested_stock_codes=requested,
                stock_count=len(selected),
            )
            run_row = conn.execute("SELECT started_at FROM pipeline_runs WHERE id = ?", (run_id,)).fetchone()
            if run_row and str(run_row["started_at"] or "").strip():
                run_started_at_iso = str(run_row["started_at"])
            stats = PipelineStats(run_id=run_id, stock_count=len(selected))
            for crawler in self.crawlers:
                if hasattr(crawler, "reset_run_state"):
                    crawler.reset_run_state()
            self.industry_research_crawler.reset_run_state()

            try:
                self._prepare_crawlers_for_run(conn, stats=stats, collect_filings=collect_filings)
                total_stocks = len(selected)
                collect_started = time.perf_counter()
                for stock_idx, stock in enumerate(selected, start=1):
                    print(
                        "[PROGRESS] collect stock "
                        f"{stock_idx}/{total_stocks} code={stock.code} market={stock.market}"
                    )
                    for crawler_idx, crawler in enumerate(self.crawlers, start=1):
                        if not self._is_crawler_market_compatible(crawler.source, stock.market):
                            continue
                        if crawler.doc_type == "news" and not collect_news:
                            print(
                                "[INFO] collect source skipped by policy "
                                f"stock={stock.code} source={crawler.source} doc_type={crawler.doc_type}"
                            )
                            continue
                        if crawler.doc_type == "report" and not collect_reports:
                            print(
                                "[INFO] collect source skipped by policy "
                                f"stock={stock.code} source={crawler.source} doc_type={crawler.doc_type}"
                            )
                            continue
                        if crawler.doc_type == "filing" and not collect_filings:
                            print(
                                "[INFO] collect source skipped by policy "
                                f"stock={stock.code} source={crawler.source} doc_type={crawler.doc_type}"
                            )
                            continue
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
                            market_stocks=market_stock_map.get(stock.market.upper(), []),
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
                        self._upsert_entity_mappings_for_docs(
                            conn,
                            stats=stats,
                            docs=filtered_docs,
                            source=crawler.source,
                            doc_type=crawler.doc_type,
                            market_stocks=market_stock_map.get(stock.market.upper(), []),
                        )
                        self._upsert_pdf_extracts_if_any(conn, crawler, stock_code=stock.code)
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
                        print(
                            "[PROGRESS] collect source done "
                            f"stock={stock.code} source={crawler.source} "
                            f"{crawler_idx}/{len(self.crawlers)} fetched={len(docs)} "
                            f"inserted={inserted} skipped={skipped + relevance_dropped} "
                            f"attempts={attempts} duration_ms={duration_ms}"
                        )

                    if collect_financials and self.settings.enable_financial_collection:
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
                    print(
                        "[PROGRESS] collect stock done "
                        f"{stock_idx}/{total_stocks} code={stock.code} "
                        f"fetched_docs={stats.fetched_docs} inserted_docs={stats.inserted_docs} "
                        f"skipped_docs={stats.skipped_docs}"
                    )
                    conn.commit()
                stats.collect_phase_elapsed_sec = time.perf_counter() - collect_started

                has_kr_targets = any(str(stock.market or "").upper() == "KR" for stock in selected)
                if collect_news and has_kr_targets:
                    print("[PROGRESS] KR RSS ingest stage start")
                    try:
                        allowed_tickers = {
                            str(stock.code or "").upper()
                            for stock in selected
                            if str(stock.market or "").upper() == "KR"
                        }
                        rss_stage = run_kr_rss_news_stage(
                            conn,
                            self.settings,
                            allowed_tickers=allowed_tickers if allowed_tickers else None,
                        )
                        stats.rss_source_count += int(rss_stage.source_count)
                        stats.rss_raw_fetched += int(rss_stage.raw_fetched)
                        stats.rss_raw_inserted += int(rss_stage.raw_inserted)
                        stats.rss_raw_url_duplicates += int(rss_stage.raw_url_duplicates)
                        stats.rss_raw_content_duplicates += int(rss_stage.raw_content_duplicates)
                        stats.rss_normalized += int(rss_stage.normalized)
                        stats.rss_mapped_ticker += int(rss_stage.mapped_ticker)
                        stats.rss_mapped_sector += int(rss_stage.mapped_sector)
                        stats.rss_unassigned += int(rss_stage.unassigned)
                        stats.rss_routed_documents += int(rss_stage.routed_documents)
                        stats.rss_routed_sector_documents += int(rss_stage.routed_sector_documents)
                        if int(rss_stage.errors) > 0:
                            stats.error_count += int(rss_stage.errors)
                            stats.error_details.append(
                                "kr_rss_ingest "
                                f"errors={int(rss_stage.errors)} "
                                f"master_mode={rss_stage.master_mode}"
                            )
                        print(
                            "[PROGRESS] KR RSS ingest stage done "
                            f"sources={rss_stage.source_count} raw_fetched={rss_stage.raw_fetched} "
                            f"raw_inserted={rss_stage.raw_inserted} normalized={rss_stage.normalized} "
                            f"mapped_ticker={rss_stage.mapped_ticker} mapped_sector={rss_stage.mapped_sector} "
                            f"unassigned={rss_stage.unassigned} routed_docs={rss_stage.routed_documents}"
                        )
                    except Exception as exc:
                        stats.error_count += 1
                        detail = f"kr_rss_ingest failed error={exc}"
                        stats.error_details.append(detail)
                        print(f"[WARN] {detail}")

                sector_started = time.perf_counter()
                self._collect_sector_industry_reports(
                    conn,
                    selected=selected,
                    stats=stats,
                    run_id=run_id,
                    enabled=include_sector_steps,
                )
                stats.sector_collect_elapsed_sec = time.perf_counter() - sector_started

                if include_agent_steps:
                    agent_started = time.perf_counter()
                    print("[PROGRESS] agent pipeline start")
                    self._run_agent_steps(conn, selected=selected, include_sector_steps=include_sector_steps, stats=stats)
                    # Keep legacy field for pipeline_runs compatibility.
                    stats.summaries_written = stats.ticker_digests_written
                    backfill_scanned = 0
                    backfill_written = 0
                    backfill_skipped = 0
                    backfill_errors = 0
                    try:
                        backfill_markets = sorted(
                            {
                                str(stock.market or "").lower()
                                for stock in selected
                                if str(stock.market or "").strip()
                            }
                        )
                        if not backfill_markets:
                            backfill_markets = [""]
                        print(
                            "[PROGRESS] translation_backfill incremental start "
                            f"markets={','.join(m.upper() for m in backfill_markets if m) or 'ALL'} "
                            f"run_started_at={run_started_at_iso}"
                        )
                        for market_code in backfill_markets:
                            counters = run_incremental_backfill(
                                conn,
                                settings=self.settings,
                                run_started_at=run_started_at_iso,
                                market=market_code,
                                scopes={"item", "evidence", "digest", "report"},
                                max_rows=0,
                                translation_retries=0,
                            )
                            for counter in counters.values():
                                backfill_scanned += counter.scanned
                                backfill_written += counter.written
                                backfill_skipped += counter.skipped
                                backfill_errors += counter.errors
                        detail = (
                            "translation_backfill incremental "
                            f"scanned={backfill_scanned} written={backfill_written} "
                            f"skipped={backfill_skipped} errors={backfill_errors}"
                        )
                        print(f"[PROGRESS] {detail}")
                        if backfill_errors > 0:
                            stats.error_count += backfill_errors
                            stats.error_details.append(f"[WARN] {detail}")
                    except Exception as exc:
                        stats.error_count += 1
                        detail = f"translation_backfill incremental failed error={exc}"
                        stats.error_details.append(detail)
                        print(f"[WARN] {detail}")
                    stats.agent_phase_elapsed_sec = time.perf_counter() - agent_started
                    print(
                        "[PROGRESS] agent pipeline done "
                        f"item_summaries={stats.item_summaries_written} "
                        f"ticker_digests={stats.ticker_digests_written} "
                        f"ticker_reports={stats.ticker_reports_written} "
                        f"sector_digests={stats.sector_digests_written} "
                        f"sector_reports={stats.sector_reports_written}"
                    )

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

        stats.total_elapsed_sec = time.perf_counter() - run_started
        translation_metrics = get_translation_metrics()
        stats.translation_calls = translation_metrics.calls
        stats.translation_cache_hits = translation_metrics.cache_hits
        stats.translation_elapsed_sec = translation_metrics.elapsed_sec
        stats.translation_fail_count = translation_metrics.fail_count
        if self.settings.enable_telegram_error_alert and stats.error_count >= self.settings.ops_error_alert_threshold:
            self._send_error_alert(stats)

        return stats

    def _prepare_crawlers_for_run(self, conn, *, stats: PipelineStats, collect_filings: bool) -> None:
        if not collect_filings:
            return
        for crawler in self.crawlers:
            if not isinstance(crawler, OpenDartDisclosureCrawler):
                continue
            try:
                crawler.prepare_run(conn=conn)
            except Exception as exc:
                crawler.mark_unavailable(f"prepare_run_failed: {exc}")
                stats.error_count += 1
                detail = f"crawler source={crawler.source} prepare_run error={exc}"
                stats.error_details.append(detail)
                print(f"[WARN] {detail}")

    def _run_agent_steps(
        self,
        conn,
        *,
        selected: list[Stock],
        include_sector_steps: bool,
        stats: PipelineStats,
    ) -> None:
        summary_remaining = self._normalize_run_cap(self.settings.summary_max_items_per_run)
        digest_remaining = self._normalize_run_cap(self.settings.digest_max_entities_per_run)
        report_remaining = self._normalize_run_cap(self.settings.report_max_entities_per_run)
        print(
            "[PROGRESS] agent caps "
            f"summary_items={self._cap_label(summary_remaining)} "
            f"digest_entities={self._cap_label(digest_remaining)} "
            f"report_entities={self._cap_label(report_remaining)}"
        )

        by_market = self._group_stock_codes_by_market(selected)
        for market_code, ticker_codes in by_market.items():
            market = market_code.lower()
            item_total = item_created = item_errors = 0
            digest_created = digest_errors = 0
            report_created = report_errors = 0

            if summary_remaining > 0:
                item_limit = min(max(1, len(ticker_codes) * 20), summary_remaining)
                item_stats = self.item_summarizer_agent.run(
                    conn,
                    market=market,
                    ticker_codes=ticker_codes,
                    lookback_days=self.settings.summary_lookback_days,
                    limit=item_limit,
                )
                item_total = item_stats.total
                item_created = item_stats.created
                item_errors = item_stats.errors
                summary_remaining = max(0, summary_remaining - item_total)
            else:
                print(f"[INFO] item_summarizer skipped by cap: market={market_code}")

            ticker_digest_ids: list[str] = []
            if digest_remaining > 0:
                ticker_digest_ids = ticker_codes[:digest_remaining]
                if ticker_digest_ids:
                    digest_stats = self.entity_digest_agent.run(
                        conn,
                        entity_type="ticker",
                        entity_ids=ticker_digest_ids,
                        market=market,
                        lookback_days=self.settings.summary_lookback_days,
                    )
                    digest_created = digest_stats.created
                    digest_errors = digest_stats.errors
                digest_remaining = max(0, digest_remaining - len(ticker_digest_ids))
            else:
                print(f"[INFO] entity_digest ticker skipped by cap: market={market_code}")

            ticker_report_ids: list[str] = []
            if report_remaining > 0:
                ticker_report_ids = ticker_codes[:report_remaining]
                if ticker_report_ids:
                    report_stats = self.report_writer_agent.run(
                        conn,
                        entity_type="ticker",
                        entity_ids=ticker_report_ids,
                        market=market,
                        lookback_days=14,
                    )
                    report_created = report_stats.created
                    report_errors = report_stats.errors
                report_remaining = max(0, report_remaining - len(ticker_report_ids))
            else:
                print(f"[INFO] report_writer ticker skipped by cap: market={market_code}")

            stats.item_summaries_written += item_created
            stats.ticker_digests_written += digest_created
            stats.ticker_reports_written += report_created
            self._merge_agent_errors(
                stats,
                market=market_code,
                scope="ticker",
                item_errors=item_errors,
                digest_errors=digest_errors,
                report_errors=report_errors,
            )

            if not include_sector_steps:
                continue

            sector_codes = self._sector_codes_for_stocks(conn, market=market_code, stock_codes=ticker_codes)
            mapped_sector_codes = recent_mapped_sector_entities(
                conn,
                market=market_code.lower(),
                lookback_days=max(1, self.settings.summary_lookback_days),
                limit=60,
            )
            if mapped_sector_codes:
                sector_codes = sorted(set(sector_codes) | set(mapped_sector_codes))
            if not sector_codes:
                continue

            sector_digest_created = sector_digest_errors = 0
            sector_report_created = sector_report_errors = 0

            sector_digest_ids: list[str] = []
            if digest_remaining > 0:
                sector_digest_ids = sector_codes[:digest_remaining]
                if sector_digest_ids:
                    sector_digest_stats = self.entity_digest_agent.run(
                        conn,
                        entity_type="sector",
                        entity_ids=sector_digest_ids,
                        market=market,
                        lookback_days=self.settings.summary_lookback_days,
                    )
                    sector_digest_created = sector_digest_stats.created
                    sector_digest_errors = sector_digest_stats.errors
                digest_remaining = max(0, digest_remaining - len(sector_digest_ids))
            else:
                print(f"[INFO] entity_digest sector skipped by cap: market={market_code}")

            sector_report_ids: list[str] = []
            if report_remaining > 0:
                sector_report_ids = sector_codes[:report_remaining]
                if sector_report_ids:
                    sector_report_stats = self.report_writer_agent.run(
                        conn,
                        entity_type="sector",
                        entity_ids=sector_report_ids,
                        market=market,
                        lookback_days=14,
                    )
                    sector_report_created = sector_report_stats.created
                    sector_report_errors = sector_report_stats.errors
                report_remaining = max(0, report_remaining - len(sector_report_ids))
            else:
                print(f"[INFO] report_writer sector skipped by cap: market={market_code}")

            stats.sector_digests_written += sector_digest_created
            stats.sector_reports_written += sector_report_created
            # Keep legacy field names in sync.
            stats.sector_summaries_written = stats.sector_digests_written
            self._merge_agent_errors(
                stats,
                market=market_code,
                scope="sector",
                item_errors=0,
                digest_errors=sector_digest_errors,
                report_errors=sector_report_errors,
            )

    @staticmethod
    def _group_stock_codes_by_market(selected: list[Stock]) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for stock in selected:
            market = str(stock.market or "").upper()
            grouped.setdefault(market, []).append(stock.code)
        return grouped

    @staticmethod
    def _normalize_run_cap(value: int) -> int:
        cap = int(value)
        if cap <= 0:
            return 10**9
        return cap

    @staticmethod
    def _cap_label(value: int) -> str:
        return "unlimited" if value >= 10**9 else str(value)

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

    @staticmethod
    def _stocks_by_market(conn) -> dict[str, list[Stock]]:
        grouped: dict[str, list[Stock]] = {}
        for row in list_stocks(conn):
            stock = row_to_stock(row)
            grouped.setdefault(stock.market.upper(), []).append(stock)
        return grouped

    def _limit_for_crawler(self, source: str, doc_type: str) -> int:
        if source == "naver_news":
            return max(1, self.settings.naver_news_per_stock)
        if source == "naver_finance_research":
            return max(1, self.settings.naver_finance_reports_per_stock)
        if source == "sec_edgar":
            return max(1, self.settings.sec_reports_per_stock)
        if source == "opendart":
            return max(1, self.settings.opendart_max_per_stock)
        if doc_type == "news":
            return max(1, self.settings.news_per_stock)
        return max(1, self.settings.reports_per_stock)

    @staticmethod
    def _is_crawler_market_compatible(source: str, market: str) -> bool:
        src = str(source or "").strip().lower()
        mkt = str(market or "").strip().upper()
        if src == "sec_edgar":
            return mkt == "US"
        if src == "opendart":
            return mkt == "KR"
        return True

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

    def _filter_docs_by_relevance(
        self,
        stock: Stock,
        source: str,
        doc_type: str,
        docs: list,
        market_stocks: list[Stock],
    ) -> tuple[list, int]:
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
            # Store-all mode: keep all collected docs and defer strict filtering to downstream ranking/UI.
            if self.settings.collect_store_all_docs:
                mapped = map_document_to_primary_ticker(
                    title=str(doc.title or ""),
                    body=str(doc.body or ""),
                    url=str(doc.url or ""),
                    source=source,
                    doc_type=doc_type,
                    market_stocks=market_stocks,
                    hinted_stock_code=stock.code,
                )
                mapped_score = float(result.score)
                if mapped.entity_id != stock.code.upper():
                    mapped_score = min(mapped_score, 0.18)
                kept.append(
                    replace(
                        doc,
                        relevance_score=mapped_score,
                        relevance_reason=result.reason,
                        matched_alias=result.matched_alias if mapped.entity_id == stock.code.upper() else "",
                    )
                )
                continue
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

    def _collect_sector_industry_reports(
        self,
        conn,
        *,
        selected: list[Stock],
        stats: PipelineStats,
        run_id: int,
        enabled: bool,
    ) -> None:
        if not enabled:
            return
        has_kr = any(str(s.market or "").upper() == "KR" for s in selected)
        if not has_kr:
            return

        limit = max(1, int(self.settings.naver_industry_reports_per_run))
        print(
            "[PROGRESS] sector industry collect start "
            f"source={self.industry_research_crawler.source} limit={limit}"
        )
        docs, attempts, error_message, duration_ms = self._collect_industry_reports_with_retries(limit=limit)
        if error_message:
            stats.error_count += 1
            detail = (
                f"crawler source={self.industry_research_crawler.source} stock=KR_SECTOR "
                f"attempts={attempts} error={error_message}"
            )
            stats.error_details.append(detail)
            print(f"[WARN] {detail}")

        sector_rows = list_sectors(conn, active_only=True)
        sector_code_by_name = self._sector_code_by_name(sector_rows)
        inserted, skipped, unmapped = upsert_sector_documents(
            conn,
            docs,
            sector_code_by_name=sector_code_by_name,
            commit=False,
        )
        stats.sector_docs_written += inserted

        if unmapped > 0:
            print(
                "[INFO] naver_industry_research unmapped sector reports "
                f"count={unmapped}/{len(docs)}"
            )

        record_crawler_run_stat(
            conn,
            run_id=run_id,
            stock_code="KR_SECTOR",
            source=self.industry_research_crawler.source,
            doc_type=self.industry_research_crawler.doc_type,
            fetched_count=len(docs),
            inserted_count=inserted,
            skipped_count=skipped + unmapped,
            error_message=error_message,
            attempt_count=attempts,
            duration_ms=duration_ms,
            commit=False,
        )
        print(
            "[PROGRESS] sector industry collect done "
            f"fetched={len(docs)} inserted={inserted} skipped={skipped + unmapped} "
            f"unmapped={unmapped} attempts={attempts} duration_ms={duration_ms}"
        )
        conn.commit()

    def _collect_industry_reports_with_retries(self, *, limit: int) -> tuple[list[SectorCollectedDocument], int, str | None, int]:
        max_retries = max(0, self.settings.crawler_max_retries)
        attempts = 0
        error_message: str | None = None
        start = time.perf_counter()
        docs: list[SectorCollectedDocument] = []
        for _ in range(max_retries + 1):
            attempts += 1
            try:
                docs = self.industry_research_crawler.collect_sector_reports(limit=limit)
                error_message = None
                break
            except Exception as exc:  # noqa: PERF203
                error_message = str(exc)
        duration_ms = int((time.perf_counter() - start) * 1000)
        return docs, attempts, error_message, duration_ms

    @staticmethod
    def _sector_code_by_name(sector_rows) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for row in sector_rows:
            sector_code = str(row["sector_code"])
            for raw in (str(row["sector_name_ko"] or ""), str(row["sector_name_en"] or "")):
                name = compact_sector_name(raw)
                if name:
                    mapping.setdefault(name, sector_code)

        # Additional aliases for common variants.
        extra: dict[str, str] = {}
        for key, code in list(mapping.items()):
            short = key.replace("업종", "").replace("산업", "").strip()
            if short:
                extra.setdefault(short, code)
        mapping.update(extra)
        return mapping

    def _upsert_entity_mappings_for_docs(
        self,
        conn,
        *,
        stats: PipelineStats,
        docs: list,
        source: str,
        doc_type: str,
        market_stocks: list[Stock],
    ) -> None:
        for doc in docs:
            mapped = map_document_to_primary_ticker(
                title=str(doc.title or ""),
                body=str(doc.body or ""),
                url=str(doc.url or ""),
                source=source,
                doc_type=doc_type,
                market_stocks=market_stocks,
                hinted_stock_code=str(doc.stock_code or ""),
                ticker_raw_min_score=float(self.settings.kr_rss_ticker_threshold),
                named_sector_min_score=float(self.settings.kr_rss_sector_threshold),
                general_economy_min_score=float(self.settings.general_economy_min_score),
                general_economy_keywords=str(self.settings.general_economy_keywords or ""),
            )
            doc_id = self._find_document_id(
                conn,
                stock_code=str(doc.stock_code or ""),
                source=str(doc.source or ""),
                url=str(doc.url or ""),
            )
            if doc_id <= 0:
                continue
            clear_news_entity_map_for_item(conn, doc_id, commit=False)
            upsert_document_entity_mapping(
                conn,
                document_id=doc_id,
                entity_type=mapped.entity_type,
                entity_id=mapped.entity_id,
                score=mapped.score,
                reason=mapped.reason,
                commit=False,
            )
            has_primary_sector = any(
                extra.is_primary and extra.entity_type == "sector" for extra in mapped.extra_mappings
            )
            upsert_news_entity_map(
                conn,
                item_id=doc_id,
                entity_type="ticker",
                entity_id=str(mapped.entity_id or "UNASSIGNED"),
                score=float(mapped.raw_score or 0.0),
                confidence=self._score_confidence_label(float(mapped.raw_score or 0.0)),
                mapping_reason=mapped.reason,
                is_primary=bool(mapped.assigned and not has_primary_sector),
                commit=False,
            )
            for extra in mapped.extra_mappings:
                upsert_document_entity_mapping(
                    conn,
                    document_id=doc_id,
                    entity_type=extra.entity_type,
                    entity_id=extra.entity_id,
                    score=extra.score,
                    reason=extra.reason,
                    commit=False,
                )
                upsert_news_entity_map(
                    conn,
                    item_id=doc_id,
                    entity_type=extra.entity_type,
                    entity_id=extra.entity_id,
                    score=float(extra.raw_score or (extra.score * 10.0)),
                    confidence=self._score_confidence_label(float(extra.raw_score or (extra.score * 10.0))),
                    mapping_reason=extra.reason,
                    is_primary=bool(extra.is_primary),
                    commit=False,
                )
                if extra.entity_type == "sector" and str(extra.entity_id).upper() == "GENERAL_ECONOMY" and extra.is_primary:
                    inserted = upsert_sector_document_by_code(
                        conn,
                        sector_code="GENERAL_ECONOMY",
                        source=str(doc.source or ""),
                        doc_type=str(doc.doc_type or ""),
                        title=str(doc.title or ""),
                        url=str(doc.url or ""),
                        published_at=to_iso_or_none(doc.published_at),
                        body=str(doc.body or ""),
                        commit=False,
                    )
                    if inserted:
                        stats.sector_docs_written += 1
                    self._upsert_general_economy_sector_card(
                        conn,
                        item_id=doc_id,
                        source=str(doc.source or ""),
                        doc_type=str(doc.doc_type or ""),
                        url=str(doc.url or ""),
                        title=str(doc.title or ""),
                        body=str(doc.body or ""),
                        published_at=to_iso_or_none(doc.published_at),
                        market=str(market_stocks[0].market if market_stocks else "KR"),
                    )
                    stats.general_economy_mapped += 1
                    keywords = list(extra.reason.get("general_economy_keywords") or [])
                    print(
                        "[INFO] mapped_general_economy "
                        f"item_id={doc_id} score={float(extra.raw_score or 0.0):.2f} keywords={keywords}"
                    )

    @staticmethod
    def _score_confidence_label(raw_score: float) -> str:
        if raw_score >= 8.0:
            return "high"
        if raw_score >= 7.0:
            return "medium"
        return "low"

    def _upsert_general_economy_sector_card(
        self,
        conn,
        *,
        item_id: int,
        source: str,
        doc_type: str,
        url: str,
        title: str,
        body: str,
        published_at: str | None,
        market: str,
    ) -> None:
        existing = evidence_repo.get_card_by_item_id(conn, item_id=item_id)
        if existing and str(existing.get("entity_type") or "").lower() != "sector":
            return
        source_type = source_type_from_item(source, doc_type)
        sentences = split_sentences(body, max_len=220)
        facts = [compact_text(title)] if compact_text(title) else []
        for sentence in sentences[:2]:
            line = compact_text(sentence)
            if line and line not in facts:
                facts.append(line)
        if not facts:
            facts = ["거시경제 관련 업데이트가 관측되었습니다."]
        card = {
            "card_id": f"SECTOR-GENERAL_ECONOMY-{int(item_id)}",
            "item_id": int(item_id),
            "entity_type": "sector",
            "entity_id": "GENERAL_ECONOMY",
            "market": str(market or "KR").lower(),
            "source_type": source_type,
            "source_name": str(source or ""),
            "url": str(url or ""),
            "source_url_hash": url_hash(str(url or "")),
            "published_at": str(published_at or ""),
            "fact_headline": facts[0],
            "facts": facts[:4],
            "interpretation": "거시 변수 변화가 기업 전반의 단기 밸류에이션과 수급에 영향을 줄 수 있습니다.",
            "risk_note": "개별 기업의 펀더멘털과 직접 연결되지 않을 수 있어 과잉 해석을 주의해야 합니다.",
            "topics": extract_topics(title, body, "일반 경제"),
            "confidence_weight": confidence_weight(source_type),
        }
        evidence_repo.upsert_card(conn, card)

    def _upsert_pdf_extracts_if_any(self, conn, crawler, *, stock_code: str) -> None:
        if not hasattr(crawler, "consume_pdf_extract_records"):
            return
        try:
            records = crawler.consume_pdf_extract_records()
        except Exception:
            records = []
        if not isinstance(records, list):
            return
        for record in records:
            if not isinstance(record, dict):
                continue
            if str(record.get("stock_code") or "").strip().upper() != stock_code.upper():
                continue
            upsert_report_pdf_extract_by_identity(
                conn,
                stock_code=stock_code,
                source=str(record.get("source") or ""),
                url=str(record.get("url") or ""),
                parse_status=str(record.get("parse_status") or ""),
                page_count=int(record.get("page_count") or 0),
                text_excerpt=str(record.get("text_excerpt") or ""),
                facts=list(record.get("facts") or []),
                commit=False,
            )

    @staticmethod
    def _find_document_id(conn, *, stock_code: str, source: str, url: str) -> int:
        normalized_url = normalize_url(url) or url
        key = url_hash(normalized_url)
        row = conn.execute(
            """
            SELECT id
            FROM documents
            WHERE stock_code = ? AND source = ? AND url_hash = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (stock_code, source, key),
        ).fetchone()
        if row is None:
            return 0
        return int(row["id"])

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
            f"general_economy_mapped={stats.general_economy_mapped}\n"
            f"rss_source_count={stats.rss_source_count}\n"
            f"rss_raw_fetched={stats.rss_raw_fetched}\n"
            f"rss_raw_inserted={stats.rss_raw_inserted}\n"
            f"rss_raw_url_duplicates={stats.rss_raw_url_duplicates}\n"
            f"rss_raw_content_duplicates={stats.rss_raw_content_duplicates}\n"
            f"rss_normalized={stats.rss_normalized}\n"
            f"rss_mapped_ticker={stats.rss_mapped_ticker}\n"
            f"rss_mapped_sector={stats.rss_mapped_sector}\n"
            f"rss_unassigned={stats.rss_unassigned}\n"
            f"rss_routed_documents={stats.rss_routed_documents}\n"
            f"rss_routed_sector_documents={stats.rss_routed_sector_documents}\n"
            f"financial_snapshots_written={stats.financial_snapshots_written}\n"
            f"financial_snapshots_skipped={stats.financial_snapshots_skipped}\n"
            f"financial_error_count={stats.financial_error_count}\n"
            f"translation_calls={stats.translation_calls}\n"
            f"translation_cache_hits={stats.translation_cache_hits}\n"
            f"translation_elapsed_sec={stats.translation_elapsed_sec:.2f}\n"
            f"translation_fail_count={stats.translation_fail_count}\n"
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


def compact_sector_name(value: str) -> str:
    text = compact_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"[^0-9a-z가-힣]+", "", text)
    return text
