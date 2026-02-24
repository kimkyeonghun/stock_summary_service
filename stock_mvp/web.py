from __future__ import annotations

import atexit
import json
import re
from typing import Any

from flask import Flask, jsonify, redirect, render_template, request, url_for

from stock_mvp.briefing import send_morning_brief
from stock_mvp.config import Settings, load_settings
from stock_mvp.database import (
    crawler_stats_for_run,
    connect,
    get_stock,
    get_stock_sectors,
    init_db,
    latest_documents_by_type,
    latest_financial_snapshot,
    latest_financial_snapshots,
    latest_pipeline_runs,
    latest_sector_summaries,
    latest_summaries_by_stock,
    latest_summary,
    latest_sector_summary,
    sector_summary_source_documents,
    summary_source_documents,
    upsert_stocks,
)
from stock_mvp.pipeline import CollectionPipeline, PipelineBusyError
from stock_mvp.sector_mapping import sync_sector_mapping_for_active_stocks
from stock_mvp.stocks import DEFAULT_STOCKS
from stock_mvp.universe import UniverseRefresher
from stock_mvp.utils import compact_text, document_identity_key


def create_app(settings: Settings | None = None) -> Flask:
    cfg = settings or load_settings()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SETTINGS"] = cfg

    with connect(cfg.db_path) as conn:
        init_db(conn)
        stock_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM stocks").fetchone()["cnt"])
        if stock_count == 0:
            upsert_stocks(conn, DEFAULT_STOCKS)
        sync_sector_mapping_for_active_stocks(conn, settings=cfg, refresh_kr_external=False)

    pipeline = CollectionPipeline(cfg)
    universe_refresher = UniverseRefresher(cfg)
    scheduler = None
    if cfg.enable_scheduler:
        from stock_mvp.scheduler import start_scheduler

        scheduler = start_scheduler(
            pipeline,
            cfg,
            morning_brief_job=lambda: send_morning_brief(cfg),
            universe_refresh_job=lambda: universe_refresher.refresh_all(kr_limit=100, us_limit=100),
        )
        atexit.register(lambda: _shutdown_scheduler(scheduler))

    @app.route("/")
    def index():
        with connect(cfg.db_path) as conn:
            rows = latest_summaries_by_stock(conn)
        return render_template("index.html", rows=rows)

    @app.route("/stock/<code>")
    def stock_detail(code: str):
        with connect(cfg.db_path) as conn:
            stock_row = get_stock(conn, code)
            if stock_row is None:
                return redirect(url_for("index"))
            sector_rows = get_stock_sectors(conn, code)
            financial_row = latest_financial_snapshot(conn, code)
            summary_row = latest_summary(conn, code)
            source_rows_raw = summary_source_documents(conn, int(summary_row["id"])) if summary_row else []
            news_rows = latest_documents_by_type(conn, code, doc_type="news", limit=100)
            report_rows = latest_documents_by_type(conn, code, doc_type="report", limit=100)

        financial = _build_financial_view(dict(financial_row)) if financial_row else None
        stock = {
            "code": stock_row["code"],
            "name": stock_row["name"],
            "queries": json.loads(stock_row["queries_json"]),
            "sectors": [str(r["sector_name_ko"] or r["sector_name_en"]) for r in sector_rows],
            "financial": financial,
        }
        summary_sections = _build_summary_sections(summary_row, source_rows_raw)
        return render_template(
            "stock_detail.html",
            stock=stock,
            summary=summary_row,
            summary_sections=summary_sections,
            news_rows=news_rows,
            report_rows=report_rows,
            doc_initial_limit=10,
        )

    @app.route("/collect", methods=["POST"])
    def collect_now():
        codes_raw = request.form.get("stock_codes", "").strip()
        stock_codes = [x for x in re.split(r"[,\s;]+", codes_raw) if x] or None
        try:
            stats = pipeline.run_once(stock_codes=stock_codes, trigger_type="web_manual")
        except PipelineBusyError as exc:
            return jsonify({"message": str(exc)}), 409
        return jsonify(
            {
                "run_id": stats.run_id,
                "stock_count": stats.stock_count,
                "fetched_docs": stats.fetched_docs,
                "inserted_docs": stats.inserted_docs,
                "skipped_docs": stats.skipped_docs,
                "summaries_written": stats.summaries_written,
                "sector_docs_written": stats.sector_docs_written,
                "sector_doc_links_written": stats.sector_doc_links_written,
                "sector_summaries_written": stats.sector_summaries_written,
                "sector_summary_error_count": stats.sector_summary_error_count,
                "financial_snapshots_written": stats.financial_snapshots_written,
                "financial_snapshots_skipped": stats.financial_snapshots_skipped,
                "financial_error_count": stats.financial_error_count,
                "error_count": stats.error_count,
            }
        )

    @app.route("/health")
    def health():
        return {"status": "ok", "env": cfg.app_env}

    @app.route("/brief/send", methods=["POST"])
    def send_brief_now():
        result = send_morning_brief(cfg)
        return jsonify({"sent": result.sent, "message": result.message, "item_count": result.item_count})

    @app.route("/universe/refresh", methods=["POST"])
    def refresh_universe_now():
        result = universe_refresher.refresh_all(kr_limit=100, us_limit=100)
        return jsonify(
            {
                "kr_requested": result.kr_requested,
                "kr_active": result.kr_active,
                "us_requested": result.us_requested,
                "us_active": result.us_active,
            }
        )

    @app.route("/ops/runs")
    def ops_runs():
        limit_raw = request.args.get("limit", "30")
        try:
            limit = max(1, min(int(limit_raw), 200))
        except ValueError:
            limit = 30
        with connect(cfg.db_path) as conn:
            rows = latest_pipeline_runs(conn, limit=limit)
        return jsonify([dict(r) for r in rows])

    @app.route("/ops/runs/<int:run_id>")
    def ops_run_detail(run_id: int):
        with connect(cfg.db_path) as conn:
            rows = crawler_stats_for_run(conn, run_id)
        return jsonify([dict(r) for r in rows])

    @app.route("/ops/financials")
    def ops_financials():
        limit_raw = request.args.get("limit", "120")
        market = request.args.get("market", "").strip().upper()
        stock_code = request.args.get("stock_code", "").strip().upper()
        sort_key = request.args.get("sort", "as_of_desc").strip().lower()
        try:
            limit = max(1, min(int(limit_raw), 500))
        except ValueError:
            limit = 120
        with connect(cfg.db_path) as conn:
            rows = [dict(r) for r in latest_financial_snapshots(conn, limit=2000)]

        if market:
            rows = [r for r in rows if str(r.get("market") or "").upper() == market]
        if stock_code:
            rows = [r for r in rows if str(r.get("stock_code") or "").upper() == stock_code]

        if sort_key == "as_of_asc":
            rows.sort(key=lambda r: (str(r.get("as_of_date") or ""), str(r.get("stock_code") or "")))
        elif sort_key == "market_rank":
            pass
        else:
            rows.sort(key=lambda r: (str(r.get("as_of_date") or ""), str(r.get("stock_code") or "")), reverse=True)

        rows = rows[:limit]
        return jsonify([_build_financial_view(r) for r in rows])

    @app.route("/ops/sector-summaries")
    def ops_sector_summaries():
        limit_raw = request.args.get("limit", "30")
        try:
            limit = max(1, min(int(limit_raw), 200))
        except ValueError:
            limit = 30
        with connect(cfg.db_path) as conn:
            rows = latest_sector_summaries(conn, limit=limit)
        return jsonify([dict(r) for r in rows])

    @app.route("/ops/sector-summaries/<sector_code>")
    def ops_sector_summary_detail(sector_code: str):
        with connect(cfg.db_path) as conn:
            summary_row = latest_sector_summary(conn, sector_code)
            if summary_row is None:
                return jsonify({"message": "not found", "sector_code": sector_code}), 404
            source_rows = sector_summary_source_documents(conn, int(summary_row["id"]))
        return jsonify({"summary": dict(summary_row), "sources": [dict(r) for r in source_rows]})

    return app


def _shutdown_scheduler(scheduler):
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)


def _build_summary_sections(summary_row: Any, source_rows: list) -> list[dict[str, Any]]:
    if not summary_row:
        return []

    line_to_sources = _group_sources_by_line(source_rows)
    sections: dict[str, dict[str, Any]] = {
        "conclusion": {"title": "결론 요약", "items": []},
        "evidence": {"title": "근거", "items": []},
        "risk": {"title": "리스크", "items": []},
        "checkpoint": {"title": "체크포인트", "items": []},
        "final": {"title": "최종 판단", "items": []},
        "other": {"title": "기타", "items": []},
    }

    for line_no in range(1, 9):
        raw_text = compact_text(str(summary_row[f"line{line_no}"] or ""))
        if not raw_text:
            continue
        section_key = _detect_summary_section(raw_text)
        sections[section_key]["items"].append(
            {
                "line_no": line_no,
                "text": raw_text,
                "sources": line_to_sources.get(line_no, []),
            }
        )

    ordered_keys = ("conclusion", "evidence", "risk", "checkpoint", "final", "other")
    results: list[dict[str, Any]] = []
    for key in ordered_keys:
        items = sections[key]["items"]
        if not items:
            continue
        results.append({"key": key, "title": sections[key]["title"], "items": items})
    return results


def _detect_summary_section(text: str) -> str:
    lowered = text.lower()
    if lowered.startswith("결론"):
        return "conclusion"
    if lowered.startswith("근거"):
        return "evidence"
    if lowered.startswith("리스크"):
        return "risk"
    if lowered.startswith("체크포인트"):
        return "checkpoint"
    if lowered.startswith("최종판단") or lowered.startswith("최종 판단"):
        return "final"
    return "other"


def _group_sources_by_line(rows: list) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    seen_by_line: dict[int, set[str]] = {}
    for row in rows:
        line_no = int(row["line_no"])
        source = str(row["source"])
        title = str(row["title"])
        url = str(row["url"])
        published_at = row["published_at"]
        key = document_identity_key(source=source, url=url, title=title, published_at=published_at)

        if line_no not in grouped:
            grouped[line_no] = []
            seen_by_line[line_no] = set()
        if key in seen_by_line[line_no]:
            continue
        seen_by_line[line_no].add(key)
        grouped[line_no].append(
            {
                "source": source,
                "title": title,
                "url": url,
                "published_at": published_at,
            }
        )
    return grouped


def _build_financial_view(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    per = _to_float(row.get("per"))
    pbr = _to_float(row.get("pbr"))
    eps = _to_float(row.get("eps"))
    roe = _to_float(row.get("roe"))
    market_cap = _to_float(row.get("market_cap"))
    currency = str(row.get("currency") or "").upper()

    row["per_display"] = _format_decimal(per, 2)
    row["pbr_display"] = _format_decimal(pbr, 2)
    row["eps_display"] = _format_number(eps, 0)
    row["roe_display"] = _format_decimal(roe, 2)
    row["market_cap_display"] = _format_market_cap(market_cap, currency)
    row["as_of_display"] = str(row.get("as_of_date") or "-")
    row["source_display"] = str(row.get("source") or "-")
    return row


def _format_market_cap(value: float | None, currency: str) -> str:
    if value is None:
        return "-"
    cur = currency.upper()
    if cur == "KRW" and value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T KRW ({value:,.0f})"
    if cur == "USD" and value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B USD ({value:,.0f})"
    if cur:
        return f"{value:,.0f} {cur}"
    return f"{value:,.0f}"


def _format_decimal(value: float | None, digits: int) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def _format_number(value: float | None, digits: int) -> str:
    if value is None:
        return "-"
    if digits <= 0:
        return f"{round(value):,.0f}"
    return f"{value:,.{digits}f}"


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
