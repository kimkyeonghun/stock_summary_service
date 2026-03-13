from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from stock_mvp.config import Settings
from stock_mvp.news_ingest.krx_master import sync_krx_master
from stock_mvp.news_ingest.news_normalizer import normalize_pending_items
from stock_mvp.news_ingest.rss_fetcher import fetch_rss_items
from stock_mvp.news_ingest.sector_router import map_and_route_pending_items


@dataclass
class KrRssStageStats:
    source_count: int = 0
    raw_fetched: int = 0
    raw_inserted: int = 0
    raw_url_duplicates: int = 0
    raw_content_duplicates: int = 0
    normalized: int = 0
    normalize_failed: int = 0
    mapped_ticker: int = 0
    mapped_sector: int = 0
    unassigned: int = 0
    routed_documents: int = 0
    routed_sector_documents: int = 0
    errors: int = 0
    master_mode: str = ""
    master_rows: int = 0
    alias_rows: int = 0


def run_kr_rss_news_stage(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    allowed_tickers: set[str] | None = None,
) -> KrRssStageStats:
    stats = KrRssStageStats()
    if not settings.enable_kr_rss_ingest:
        return stats

    master = sync_krx_master(conn, settings, force=False)
    stats.master_mode = str(master.get("mode") or "")
    stats.master_rows = int(master.get("master_rows") or 0)
    stats.alias_rows = int(master.get("alias_rows") or 0)

    fetched = fetch_rss_items(conn, settings)
    stats.source_count = int(fetched.get("sources") or 0)
    stats.raw_fetched = int(fetched.get("fetched") or 0)
    stats.raw_inserted = int(fetched.get("inserted") or 0)
    stats.raw_url_duplicates = int(fetched.get("url_duplicates") or 0)
    stats.raw_content_duplicates = int(fetched.get("content_duplicates") or 0)
    stats.errors += int(fetched.get("errors") or 0)

    normalized = normalize_pending_items(conn, settings, limit=2000)
    stats.normalized = int(normalized.get("normalized") or 0)
    stats.normalize_failed = int(normalized.get("failed") or 0)
    if stats.normalize_failed:
        stats.errors += stats.normalize_failed

    mapped = map_and_route_pending_items(
        conn,
        settings,
        limit=2000,
        allowed_tickers=allowed_tickers,
        dry_run=False,
    )
    stats.mapped_ticker = int(mapped.get("mapped_ticker") or 0)
    stats.mapped_sector = int(mapped.get("mapped_sector") or 0)
    stats.unassigned = int(mapped.get("unassigned") or 0)
    stats.routed_documents = int(mapped.get("routed_documents") or 0)
    stats.routed_sector_documents = int(mapped.get("routed_sector_documents") or 0)
    return stats

