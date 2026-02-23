from __future__ import annotations

import atexit
import json
import re

from flask import Flask, jsonify, redirect, render_template, request, url_for

from stock_mvp.briefing import send_morning_brief
from stock_mvp.config import Settings, load_settings
from stock_mvp.database import (
    crawler_stats_for_run,
    connect,
    get_stock,
    init_db,
    latest_documents,
    latest_pipeline_runs,
    latest_summaries_by_stock,
    latest_summary,
    summary_source_documents,
    upsert_stocks,
)
from stock_mvp.pipeline import CollectionPipeline
from stock_mvp.stocks import DEFAULT_STOCKS
from stock_mvp.universe import UniverseRefresher
from stock_mvp.utils import document_identity_key


def create_app(settings: Settings | None = None) -> Flask:
    cfg = settings or load_settings()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SETTINGS"] = cfg

    with connect(cfg.db_path) as conn:
        init_db(conn)
        stock_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM stocks").fetchone()["cnt"])
        if stock_count == 0:
            upsert_stocks(conn, DEFAULT_STOCKS)

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
            summary_row = latest_summary(conn, code)
            source_rows_raw = summary_source_documents(conn, int(summary_row["id"])) if summary_row else []
            doc_rows = latest_documents(conn, code, limit=50)

        stock = {
            "code": stock_row["code"],
            "name": stock_row["name"],
            "queries": json.loads(stock_row["queries_json"]),
        }
        source_rows = _group_source_rows(source_rows_raw)
        return render_template(
            "stock_detail.html",
            stock=stock,
            summary=summary_row,
            source_rows=source_rows,
            doc_rows=doc_rows,
        )

    @app.route("/collect", methods=["POST"])
    def collect_now():
        codes_raw = request.form.get("stock_codes", "").strip()
        stock_codes = [x for x in re.split(r"[,\s;]+", codes_raw) if x] or None
        stats = pipeline.run_once(stock_codes=stock_codes, trigger_type="web_manual")
        return jsonify(
            {
                "run_id": stats.run_id,
                "stock_count": stats.stock_count,
                "fetched_docs": stats.fetched_docs,
                "inserted_docs": stats.inserted_docs,
                "skipped_docs": stats.skipped_docs,
                "summaries_written": stats.summaries_written,
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

    return app


def _shutdown_scheduler(scheduler):
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)


def _group_source_rows(rows: list) -> list[dict]:
    grouped: dict[str, dict] = {}
    for row in rows:
        line_no = int(row["line_no"])
        source = str(row["source"])
        title = str(row["title"])
        url = str(row["url"])
        published_at = row["published_at"]
        key = document_identity_key(source=source, url=url, title=title, published_at=published_at)

        if key not in grouped:
            grouped[key] = {
                "source": source,
                "title": title,
                "url": url,
                "published_at": published_at,
                "line_nos": {line_no},
                "duplicate_count": 1,
            }
        else:
            grouped[key]["line_nos"].add(line_no)
            grouped[key]["duplicate_count"] += 1

    results: list[dict] = []
    for item in grouped.values():
        lines = sorted(item["line_nos"])
        results.append(
            {
                "source": item["source"],
                "title": item["title"],
                "url": item["url"],
                "published_at": item["published_at"],
                "line_nos_label": ",".join(str(x) for x in lines),
                "first_line_no": lines[0],
                "duplicate_count": item["duplicate_count"],
            }
        )

    results.sort(key=lambda x: (x["first_line_no"], x["published_at"] or ""), reverse=False)
    return results
