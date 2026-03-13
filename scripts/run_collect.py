from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.config import load_settings
from stock_mvp.database import connect
from stock_mvp.pipeline import CollectionPipeline, PipelineBusyError


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one collection cycle.")
    parser.add_argument("--stock-codes", type=str, default="", help="Comma-separated stock codes")
    parser.add_argument(
        "--market",
        type=str.upper,
        default=None,
        choices=["KR", "US"],
        help="Optional market filter. Use KR or US.",
    )
    parser.add_argument(
        "--skip-sector",
        action="store_true",
        help="Skip sector-level digest/report generation for this run.",
    )
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Collect and store documents only (skip all summarization agents).",
    )
    args = parser.parse_args()

    settings = load_settings()
    pipeline = CollectionPipeline(settings)
    stock_codes = [x for x in re.split(r"[,\s;]+", args.stock_codes.strip()) if x] or None
    market = args.market
    wall_started = time.perf_counter()
    try:
        stats = pipeline.run_once(
            stock_codes=stock_codes,
            market=market,
            include_agent_steps=not args.collect_only,
            include_sector_steps=not args.skip_sector,
        )
    except PipelineBusyError as exc:
        print(f"Collection skipped: {exc}")
        raise SystemExit(2)
    except KeyboardInterrupt:
        print("Collection interrupted by user (Ctrl+C). Current run is marked as failed.")
        raise SystemExit(130)

    print("Collection done")
    print(f"run_id={stats.run_id}")
    print(f"stock_count={stats.stock_count}")
    print(f"fetched_docs={stats.fetched_docs}")
    print(f"inserted_docs={stats.inserted_docs}")
    print(f"skipped_docs={stats.skipped_docs}")
    print(f"summaries_written={stats.summaries_written}")
    print(f"item_summaries_written={stats.item_summaries_written}")
    print(f"ticker_digests_written={stats.ticker_digests_written}")
    print(f"ticker_reports_written={stats.ticker_reports_written}")
    print(f"sector_digests_written={stats.sector_digests_written}")
    print(f"sector_reports_written={stats.sector_reports_written}")
    print(f"agent_error_count={stats.agent_error_count}")
    print(f"sector_docs_written={stats.sector_docs_written}")
    print(f"sector_doc_links_written={stats.sector_doc_links_written}")
    print(f"sector_summaries_written={stats.sector_summaries_written}")
    print(f"sector_summary_error_count={stats.sector_summary_error_count}")
    print(f"general_economy_mapped={stats.general_economy_mapped}")
    print(f"rss_source_count={stats.rss_source_count}")
    print(f"rss_raw_fetched={stats.rss_raw_fetched}")
    print(f"rss_raw_inserted={stats.rss_raw_inserted}")
    print(f"rss_raw_url_duplicates={stats.rss_raw_url_duplicates}")
    print(f"rss_raw_content_duplicates={stats.rss_raw_content_duplicates}")
    print(f"rss_normalized={stats.rss_normalized}")
    print(f"rss_mapped_ticker={stats.rss_mapped_ticker}")
    print(f"rss_mapped_sector={stats.rss_mapped_sector}")
    print(f"rss_unassigned={stats.rss_unassigned}")
    print(f"rss_routed_documents={stats.rss_routed_documents}")
    print(f"rss_routed_sector_documents={stats.rss_routed_sector_documents}")
    print(f"financial_snapshots_written={stats.financial_snapshots_written}")
    print(f"financial_snapshots_skipped={stats.financial_snapshots_skipped}")
    print(f"financial_error_count={stats.financial_error_count}")
    print(f"translation_calls={stats.translation_calls}")
    print(f"translation_cache_hits={stats.translation_cache_hits}")
    print(f"translation_elapsed_sec={stats.translation_elapsed_sec:.2f}")
    print(f"translation_fail_count={stats.translation_fail_count}")
    print(f"error_count={stats.error_count}")
    wall_elapsed = time.perf_counter() - wall_started
    print("timings:")
    print(f"- total_elapsed_sec={stats.total_elapsed_sec:.2f}")
    print(f"- collect_phase_elapsed_sec={stats.collect_phase_elapsed_sec:.2f}")
    print(f"- sector_collect_elapsed_sec={stats.sector_collect_elapsed_sec:.2f}")
    print(f"- agent_phase_elapsed_sec={stats.agent_phase_elapsed_sec:.2f}")
    print(f"- cli_wall_elapsed_sec={wall_elapsed:.2f}")
    _print_run_timing_summary(settings.db_path, run_id=stats.run_id)
    if stats.error_details:
        print("error_details:")
        for idx, detail in enumerate(stats.error_details, start=1):
            print(f"{idx}. {detail}")


def _print_run_timing_summary(db_path: Path, *, run_id: int) -> None:
    with connect(db_path) as conn:
        run_row = conn.execute(
            """
            SELECT started_at, ended_at
            FROM pipeline_runs
            WHERE id = ?
            """,
            (run_id,),
        ).fetchone()

        stats_rows = conn.execute(
            """
            SELECT source, COUNT(*) AS calls, SUM(duration_ms) AS total_ms,
                   AVG(duration_ms) AS avg_ms, MAX(duration_ms) AS max_ms
            FROM crawler_run_stats
            WHERE run_id = ?
            GROUP BY source
            ORDER BY total_ms DESC
            """,
            (run_id,),
        ).fetchall()

        slow_rows = conn.execute(
            """
            SELECT stock_code, source, fetched_count, inserted_count, skipped_count, duration_ms
            FROM crawler_run_stats
            WHERE run_id = ?
            ORDER BY duration_ms DESC, stock_code, source
            LIMIT 8
            """,
            (run_id,),
        ).fetchall()

    if run_row is not None:
        started = _parse_iso(str(run_row["started_at"] or ""))
        ended = _parse_iso(str(run_row["ended_at"] or ""))
        if started and ended:
            print(f"run_window_sec={(ended - started).total_seconds():.2f}")

    if stats_rows:
        print("source_timing_ms:")
        for row in stats_rows:
            print(
                f"- source={row['source']} calls={int(row['calls'] or 0)} "
                f"total={int(row['total_ms'] or 0)} avg={int(row['avg_ms'] or 0)} "
                f"max={int(row['max_ms'] or 0)}"
            )

    if slow_rows:
        print("slowest_calls_ms:")
        for row in slow_rows:
            print(
                f"- stock={row['stock_code']} source={row['source']} "
                f"duration={int(row['duration_ms'] or 0)} fetched={int(row['fetched_count'] or 0)} "
                f"inserted={int(row['inserted_count'] or 0)} skipped={int(row['skipped_count'] or 0)}"
            )


def _parse_iso(text: str) -> dt.datetime | None:
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


if __name__ == "__main__":
    main()
