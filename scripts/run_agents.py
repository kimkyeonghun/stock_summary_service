from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_mvp.agents.entity_digest import EntityDigestAgent
from stock_mvp.agents.item_summarizer import ItemSummarizerAgent
from stock_mvp.agents.report_writer import ReportWriterAgent
from stock_mvp.config import load_settings
from stock_mvp.database import connect, init_db


def main() -> None:
    parser = argparse.ArgumentParser(description="Run BriefAlpha agent pipeline (item -> digest -> report).")
    parser.add_argument("--market", choices=["kr", "us"], default="kr", help="Market scope")
    parser.add_argument("--scope", choices=["ticker", "sector"], default="ticker", help="Entity scope")
    parser.add_argument(
        "--entities",
        default="",
        help="Comma-separated entity ids. ticker codes for scope=ticker, sector codes for scope=sector",
    )
    parser.add_argument("--item-lookback-days", type=int, default=14)
    parser.add_argument("--digest-lookback-days", type=int, default=7)
    parser.add_argument("--report-lookback-days", type=int, default=14)
    parser.add_argument("--item-limit", type=int, default=500)
    parser.add_argument("--skip-item", action="store_true")
    parser.add_argument("--skip-digest", action="store_true")
    parser.add_argument("--skip-report", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    market = args.market.lower()
    scope = args.scope.lower()
    entities = [x for x in re.split(r"[,\s;]+", args.entities.strip()) if x]

    with connect(settings.db_path) as conn:
        init_db(conn)
        target_entities = resolve_entities(conn, scope=scope, market=market, requested=entities)
        if not target_entities:
            print("No target entities found.")
            return

        item_agent = ItemSummarizerAgent()
        digest_agent = EntityDigestAgent()
        report_agent = ReportWriterAgent()

        if not args.skip_item:
            ticker_codes = (
                target_entities
                if scope == "ticker"
                else resolve_tickers_for_sectors(conn, market=market, sector_codes=target_entities)
            )
            item_stats = item_agent.run(
                conn,
                market=market,
                ticker_codes=ticker_codes,
                lookback_days=max(1, args.item_lookback_days),
                limit=max(1, args.item_limit),
            )
            print(
                f"item_summaries total={item_stats.total} created={item_stats.created} "
                f"skipped={item_stats.skipped} errors={item_stats.errors}"
            )

        if not args.skip_digest:
            digest_stats = digest_agent.run(
                conn,
                entity_type=scope,
                entity_ids=target_entities,
                market=market,
                lookback_days=max(1, args.digest_lookback_days),
            )
            print(
                f"daily_digests total={digest_stats.total} created={digest_stats.created} "
                f"errors={digest_stats.errors}"
            )

        if not args.skip_report:
            report_stats = report_agent.run(
                conn,
                entity_type=scope,
                entity_ids=target_entities,
                market=market,
                lookback_days=max(1, args.report_lookback_days),
            )
            print(
                f"agent_reports total={report_stats.total} created={report_stats.created} "
                f"skipped={report_stats.skipped} errors={report_stats.errors}"
            )


def resolve_entities(conn, *, scope: str, market: str, requested: list[str]) -> list[str]:
    if scope == "ticker":
        rows = conn.execute(
            """
            SELECT code
            FROM stocks
            WHERE is_active = 1
              AND lower(market) = lower(?)
            ORDER BY COALESCE(rank, 99999), code
            """,
            (market,),
        ).fetchall()
        codes = [str(r["code"]) for r in rows]
        if requested:
            req = {x.upper() for x in requested}
            codes = [c for c in codes if c.upper() in req]
        return codes

    rows = conn.execute(
        """
        SELECT DISTINCT m.sector_code
        FROM stock_sector_map m
        JOIN stocks s ON s.code = m.stock_code
        WHERE s.is_active = 1
          AND lower(s.market) = lower(?)
        ORDER BY m.sector_code
        """,
        (market,),
    ).fetchall()
    codes = [str(r["sector_code"]) for r in rows]
    if requested:
        req = {x.upper() for x in requested}
        codes = [c for c in codes if c.upper() in req]
    return codes


def resolve_tickers_for_sectors(conn, *, market: str, sector_codes: list[str]) -> list[str]:
    if not sector_codes:
        return []
    placeholders = ",".join("?" for _ in sector_codes)
    sql = f"""
    SELECT DISTINCT m.stock_code
    FROM stock_sector_map m
    JOIN stocks s ON s.code = m.stock_code
    WHERE lower(s.market) = lower(?)
      AND s.is_active = 1
      AND m.sector_code IN ({placeholders})
    ORDER BY m.stock_code
    """
    rows = conn.execute(sql, (market, *sector_codes)).fetchall()
    return [str(r["stock_code"]) for r in rows]


if __name__ == "__main__":
    main()
