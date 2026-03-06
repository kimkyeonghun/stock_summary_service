from __future__ import annotations

import atexit
import json
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any

from flask import Flask, abort, jsonify, redirect, render_template, request, session, url_for
from zoneinfo import ZoneInfo

from stock_mvp.backtest import BacktestAsset, BacktestEngine
from stock_mvp.backtest_presets import get_portfolio_preset, list_portfolio_presets
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
    list_sectors,
    list_stocks_by_market,
    upsert_stocks,
)
from stock_mvp.pipeline import CollectionPipeline
from stock_mvp.prices import PriceCollector
from stock_mvp.sector_mapping import sync_sector_mapping_for_active_stocks
from stock_mvp.stocks import DEFAULT_STOCKS
from stock_mvp.universe import UniverseRefresher
from stock_mvp.utils import compact_text, document_identity_key, normalize_url


def create_app(settings: Settings | None = None) -> Flask:
    cfg = settings or load_settings()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SETTINGS"] = cfg
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "stock-mvp-dev-session-key")

    @app.template_filter("fmt_dt")
    def fmt_dt_filter(value: Any, market: str | None = None) -> str:
        market_norm = str(market or "").strip().upper()
        if market_norm not in {"KR", "US"}:
            market_norm = ""
        return _format_datetime_display(value, market=market_norm or None)

    with connect(cfg.db_path) as conn:
        init_db(conn)
        stock_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM stocks").fetchone()["cnt"])
        if stock_count == 0:
            upsert_stocks(conn, DEFAULT_STOCKS)
        sync_sector_mapping_for_active_stocks(conn, settings=cfg, refresh_kr_external=False)

    pipeline = CollectionPipeline(cfg)
    universe_refresher = UniverseRefresher(cfg)
    price_collector = PriceCollector(cfg)
    backtest_engine = BacktestEngine(cfg)
    scheduler = None
    if cfg.enable_scheduler:
        from stock_mvp.scheduler import start_scheduler

        scheduler = start_scheduler(
            pipeline,
            cfg,
            morning_brief_job=lambda: send_morning_brief(cfg),
            universe_refresh_job=lambda: universe_refresher.refresh_all(kr_limit=100, us_limit=100),
            price_collect_kr_job=lambda: _run_price_collect_job(price_collector, market="KR"),
            price_collect_us_job=lambda: _run_price_collect_job(price_collector, market="US"),
        )
        atexit.register(lambda: _shutdown_scheduler(scheduler))

    @app.route("/")
    def index():
        preferred = _get_session_market(default="KR")
        return redirect(url_for("dashboard", market=preferred.lower()))

    @app.route("/<market>")
    def dashboard(market: str):
        market_norm = _normalize_market_or_404(market)
        _set_session_market(market_norm)
        feed_stock_param = str(request.args.get("feed_stock") or "").strip().upper()
        with connect(cfg.db_path) as conn:
            market_stocks = list_stocks_by_market(conn, market_norm, active_only=True)
            market_codes = {str(r["code"]) for r in market_stocks}
            digest_by_code = _latest_ticker_digests_for_market(conn, market_norm)
            rows = []
            for stock_row in market_stocks:
                code = str(stock_row["code"])
                digest = digest_by_code.get(code)
                rows.append(
                    {
                        "stock_code": code,
                        "stock_name": str(stock_row["name"]),
                        "line1": _digest_line1(digest),
                        "as_of": str(digest["digest_date"]) if digest else "",
                    }
                )
            sector_rows = _list_sectors_for_market(conn, market_norm)
            sector_name_map = {
                str(r["sector_code"]): str(r["sector_name_ko"] or r["sector_name_en"])
                for r in sector_rows
            }

            summary_map = {str(r["stock_code"]): r for r in rows}
            subscribed_stock_codes = [
                c for c in _get_watchlist_items("stocks", market_norm) if c in market_codes
            ]
            subscribed_sector_codes = [
                c for c in _get_watchlist_items("sectors", market_norm) if c in sector_name_map
            ]

            subscribed_stocks: list[dict[str, str]] = []
            for code in subscribed_stock_codes:
                summary_row = summary_map.get(code)
                stock_row = next((s for s in market_stocks if str(s["code"]) == code), None)
                if stock_row is None:
                    continue
                subscribed_stocks.append(
                    {
                        "code": code,
                        "name": str(stock_row["name"]),
                        "line1": str(summary_row["line1"] or "") if summary_row else "",
                        "as_of": str(summary_row["as_of"] or "") if summary_row else "",
                    }
                )

            subscribed_sectors: list[dict[str, str]] = []
            for sector_code in subscribed_sector_codes:
                latest_row = _latest_entity_digest(conn, entity_type="sector", entity_id=sector_code, market=market_norm)
                subscribed_sectors.append(
                    {
                        "sector_code": sector_code,
                        "sector_name": sector_name_map.get(sector_code, sector_code),
                        "line1": _digest_line1(latest_row),
                        "as_of": str(latest_row["digest_date"] or "") if latest_row else "",
                    }
                )
            feed_filter_options: list[dict[str, Any]] = []
            selected_feed_stock = ""
            if subscribed_stocks:
                subscribed_name_by_code = {str(r["code"]): str(r["name"]) for r in subscribed_stocks}
                if feed_stock_param in subscribed_name_by_code:
                    selected_feed_stock = feed_stock_param
                feed_source_codes = [selected_feed_stock] if selected_feed_stock else list(subscribed_stock_codes)
                feed_filter_options.append(
                    {
                        "code": "",
                        "label": "전체",
                        "is_active": not selected_feed_stock,
                        "url": url_for("dashboard", market=market_norm.lower()),
                    }
                )
                for stock in subscribed_stocks:
                    code = str(stock["code"])
                    feed_filter_options.append(
                        {
                            "code": code,
                            "label": f"{stock['name']} ({code})",
                            "is_active": selected_feed_stock == code,
                            "url": url_for("dashboard", market=market_norm.lower(), feed_stock=code),
                        }
                    )
            else:
                feed_source_codes = [str(r["code"]) for r in market_stocks]
            feed_items = _latest_item_feed(conn, market=market_norm, stock_codes=feed_source_codes, limit=30)

        rows = rows[:20]
        return render_template(
            "index.html",
            current_market=market_norm,
            nav_links=_nav_links(market_norm),
            active_page="dashboard",
            rows=rows,
            subscribed_stocks=subscribed_stocks,
            subscribed_sectors=subscribed_sectors,
            feed_items=feed_items,
            feed_has_more=len(feed_items) > 10,
            feed_filter_options=feed_filter_options,
            has_subscriptions=bool(subscribed_stocks or subscribed_sectors),
        )

    @app.route("/<market>/watchlist")
    def watchlist_page(market: str):
        market_norm = _normalize_market_or_404(market)
        _set_session_market(market_norm)
        with connect(cfg.db_path) as conn:
            stock_rows = list_stocks_by_market(conn, market_norm, active_only=True)
            sector_rows = _list_sectors_for_market(conn, market_norm)

        subscribed_stocks = set(_get_watchlist_items("stocks", market_norm))
        subscribed_sectors = set(_get_watchlist_items("sectors", market_norm))
        stocks = [
            {
                "code": str(r["code"]),
                "name": str(r["name"]),
                "is_subscribed": str(r["code"]) in subscribed_stocks,
            }
            for r in stock_rows[:200]
        ]
        sectors = [
            {
                "sector_code": str(r["sector_code"]),
                "sector_name": str(r["sector_name_ko"] or r["sector_name_en"]),
                "is_subscribed": str(r["sector_code"]) in subscribed_sectors,
            }
            for r in sector_rows
        ]
        return render_template(
            "watchlist.html",
            current_market=market_norm,
            nav_links=_nav_links(market_norm),
            active_page="watchlist",
            stocks=stocks,
            sectors=sectors,
        )

    @app.route("/<market>/watchlist/stocks/<code>", methods=["POST"])
    def toggle_watchlist_stock(market: str, code: str):
        market_norm = _normalize_market_or_404(market)
        code_norm = code.strip().upper()
        with connect(cfg.db_path) as conn:
            stock_row = get_stock(conn, code_norm)
            if stock_row is None:
                abort(404)
            if str(stock_row["market"]).upper() != market_norm:
                return redirect(url_for("watchlist_page", market=str(stock_row["market"]).lower()))
        _toggle_watchlist_item("stocks", market_norm, code_norm)
        next_url = request.form.get("next", "").strip()
        if next_url:
            return redirect(next_url)
        return redirect(url_for("watchlist_page", market=market_norm.lower()))

    @app.route("/<market>/watchlist/sectors/<sector_code>", methods=["POST"])
    def toggle_watchlist_sector(market: str, sector_code: str):
        market_norm = _normalize_market_or_404(market)
        sector_code_norm = sector_code.strip().upper()
        with connect(cfg.db_path) as conn:
            sector_rows = _list_sectors_for_market(conn, market_norm)
            sector_codes = {str(r["sector_code"]).upper() for r in sector_rows}
        if sector_code_norm not in sector_codes:
            abort(404)
        _toggle_watchlist_item("sectors", market_norm, sector_code_norm)
        next_url = request.form.get("next", "").strip()
        if next_url:
            return redirect(next_url)
        return redirect(url_for("watchlist_page", market=market_norm.lower()))

    @app.route("/stock/<code>")
    def stock_detail_redirect(code: str):
        code_norm = code.strip().upper()
        with connect(cfg.db_path) as conn:
            stock_row = get_stock(conn, code_norm)
            if stock_row is None:
                return redirect(url_for("index"))
        market_norm = str(stock_row["market"]).upper()
        return redirect(url_for("stock_detail", market=market_norm.lower(), code=code_norm))

    @app.route("/<market>/stock/<code>")
    def stock_detail(market: str, code: str):
        market_norm = _normalize_market_or_404(market)
        _set_session_market(market_norm)
        code_norm = code.strip().upper()
        doc_sort = str(request.args.get("doc_sort", "recent") or "recent").strip().lower()
        if doc_sort not in {"recent", "relevance"}:
            doc_sort = "recent"
        with connect(cfg.db_path) as conn:
            stock_row = get_stock(conn, code_norm)
            if stock_row is None:
                return redirect(url_for("dashboard", market=market_norm.lower()))
            stock_market = str(stock_row["market"]).upper()
            if stock_market != market_norm:
                return redirect(url_for("stock_detail", market=stock_market.lower(), code=code_norm))
            sector_rows = get_stock_sectors(conn, code_norm)
            financial_row = latest_financial_snapshot(conn, code_norm)
            digest_row = _latest_entity_digest(conn, entity_type="ticker", entity_id=code_norm, market=market_norm)
            digest_view = _build_digest_view(conn, digest_row)
            report_view = _latest_agent_report(conn, entity_type="ticker", entity_id=code_norm, market=market_norm)
            item_summary_rows = _latest_item_summaries(conn, code_norm, limit=50)
            news_rows_raw = latest_documents_by_type(conn, code_norm, doc_type="news", limit=240, order_by=doc_sort)
            report_rows_raw = latest_documents_by_type(conn, code_norm, doc_type="report", limit=180, order_by=doc_sort)
            news_rows = _curate_document_rows(news_rows_raw, doc_type="news", limit=100)
            report_rows = _curate_document_rows(report_rows_raw, doc_type="report", limit=100)
            sector_briefs: list[dict[str, str]] = []
            for sector_row in sector_rows:
                sector_code = str(sector_row["sector_code"])
                sector_digest = _latest_entity_digest(conn, entity_type="sector", entity_id=sector_code, market=market_norm)
                sector_briefs.append(
                    {
                        "sector_code": sector_code,
                        "sector_name": str(sector_row["sector_name_ko"] or sector_row["sector_name_en"]),
                        "line1": _digest_line1(sector_digest),
                        "as_of": str(sector_digest["digest_date"] or "") if sector_digest else "",
                    }
                )

        financial = _build_financial_view(dict(financial_row)) if financial_row else None
        stock = {
            "code": stock_row["code"],
            "name": stock_row["name"],
            "queries": json.loads(stock_row["queries_json"]),
            "sectors": [str(r["sector_name_ko"] or r["sector_name_en"]) for r in sector_rows],
            "sector_briefs": sector_briefs,
            "financial": financial,
        }
        subscribed = code_norm in set(_get_watchlist_items("stocks", market_norm))
        return render_template(
            "stock_detail.html",
            current_market=market_norm,
            nav_links=_nav_links(market_norm),
            active_page="dashboard",
            stock=stock,
            digest=digest_row,
            digest_view=digest_view,
            agent_report=report_view,
            item_summaries=item_summary_rows,
            news_rows=news_rows,
            report_rows=report_rows,
            doc_sort=doc_sort,
            doc_initial_limit=10,
            is_subscribed=subscribed,
        )

    @app.route("/<market>/item/<int:item_id>")
    def item_detail(market: str, item_id: int):
        market_norm = _normalize_market_or_404(market)
        _set_session_market(market_norm)
        with connect(cfg.db_path) as conn:
            row = _item_detail_payload(conn, item_id=item_id)
            if row is None:
                return redirect(url_for("dashboard", market=market_norm.lower()))
            stock_market = str(row["market"]).upper()
            if stock_market != market_norm:
                return redirect(url_for("item_detail", market=stock_market.lower(), item_id=item_id))
            related_rows = _related_documents_for_item(conn, stock_code=str(row["stock_code"]), exclude_item_id=item_id, limit=10)
        return render_template(
            "item_detail.html",
            current_market=market_norm,
            nav_links=_nav_links(market_norm),
            active_page="dashboard",
            item=row,
            related_rows=related_rows,
        )

    @app.route("/backtest")
    def backtest_page_redirect():
        preferred = _get_session_market(default="KR")
        return redirect(url_for("backtest_page", market=preferred.lower()))

    @app.route("/<market>/backtest")
    def backtest_page(market: str):
        market_norm = _normalize_market_or_404(market)
        _set_session_market(market_norm)
        return render_template(
            "backtest.html",
            current_market=market_norm,
            nav_links=_nav_links(market_norm),
            active_page="backtest",
        )

    @app.route("/health")
    def health():
        return {"status": "ok", "env": cfg.app_env}

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
        market = request.args.get("market", "").strip().upper()
        try:
            limit = max(1, min(int(limit_raw), 200))
        except ValueError:
            limit = 30
        with connect(cfg.db_path) as conn:
            rows = _latest_sector_digests(conn, market=market or None, limit=limit)
        return jsonify(rows)

    @app.route("/ops/sector-summaries/<sector_code>")
    def ops_sector_summary_detail(sector_code: str):
        market = request.args.get("market", "").strip().upper() or "KR"
        with connect(cfg.db_path) as conn:
            digest_row = _latest_entity_digest(
                conn,
                entity_type="sector",
                entity_id=sector_code.strip().upper(),
                market=market,
            )
            if digest_row is None:
                return jsonify({"message": "not found", "sector_code": sector_code, "market": market}), 404
            digest_view = _build_digest_view(conn, digest_row)
            report_view = _latest_agent_report(
                conn,
                entity_type="sector",
                entity_id=sector_code.strip().upper(),
                market=market,
            )
        return jsonify({"digest": digest_row, "digest_view": digest_view, "report": report_view})

    @app.route("/api/backtest/presets")
    def api_backtest_presets():
        presets = []
        for p in list_portfolio_presets():
            presets.append(
                {
                    "key": p.key,
                    "name": p.name,
                    "description": p.description,
                    "market": p.market,
                    "benchmark_code": p.benchmark_code,
                    "assets": [{"code": a.code, "weight": a.weight} for a in p.assets],
                }
            )
        return jsonify({"presets": presets})

    @app.route("/api/backtest/run", methods=["POST"])
    def api_backtest_run():
        payload = request.get_json(silent=True) or {}
        run_id = str(uuid.uuid4())
        try:
            args = _parse_backtest_request(payload)
        except ValueError as exc:
            return jsonify({"message": str(exc), "run_id": run_id}), 400

        print(
            f"[INFO] backtest run start run_id={run_id} market={args['market']} "
            f"strategy={args['strategy']} rebalance={args['rebalance']} "
            f"assets={len(args['assets'])} preset={args.get('preset_key') or '-'} "
            f"period={args['start_date']}..{args['end_date']}"
        )
        try:
            result = backtest_engine.run(
                market=args["market"],
                assets=args["assets"],
                start_date=args["start_date"],
                end_date=args["end_date"],
                strategy=args["strategy"],
                rebalance=args["rebalance"],
                initial_capital=args["initial_capital"],
                fee_bps=args["fee_bps"],
                slippage_bps=args["slippage_bps"],
                risk_free_rate=args["risk_free_rate"],
                benchmark_code=args["benchmark_code"],
                contribution_amount=args["contribution_amount"],
                contribution_frequency=args["contribution_frequency"],
            )
        except ValueError as exc:
            print(f"[INFO] backtest run invalid run_id={run_id} error={exc}")
            return jsonify({"message": str(exc), "run_id": run_id}), 400
        except Exception as exc:
            print(f"[WARN] backtest run failed run_id={run_id} error={exc}")
            return jsonify({"message": "backtest execution failed", "run_id": run_id}), 500

        print(
            f"[INFO] backtest run done run_id={run_id} final_equity={result.summary.final_equity:.2f} "
            f"trade_count={result.summary.trade_count} daily_points={len(result.daily)}"
        )

        comparison_series = [
            {
                "key": args.get("preset_key") or "primary",
                "label": args.get("preset_name") or "Primary Portfolio",
                "points": [
                    {"trade_date": p.trade_date, "equity": p.equity, "index": p.index} for p in result.portfolio_series
                ],
            }
        ]
        if args.get("include_benchmark_in_compare") and result.summary.benchmark_code and result.benchmark_series:
            comparison_series.append(
                {
                    "key": f"benchmark:{result.summary.benchmark_code}",
                    "label": f"Benchmark {result.summary.benchmark_code}",
                    "points": [
                        {"trade_date": p.trade_date, "equity": p.equity, "index": p.index}
                        for p in result.benchmark_series
                    ],
                }
            )
        for key in args.get("compare_preset_keys", []):
            preset = get_portfolio_preset(key)
            if preset is None:
                continue
            if args.get("preset_key") and key == args["preset_key"]:
                continue
            compare_result = backtest_engine.run(
                market=args["market"],
                assets=list(preset.assets),
                start_date=args["start_date"],
                end_date=args["end_date"],
                strategy=args["strategy"],
                rebalance=args["rebalance"],
                initial_capital=args["initial_capital"],
                fee_bps=args["fee_bps"],
                slippage_bps=args["slippage_bps"],
                risk_free_rate=args["risk_free_rate"],
                benchmark_code=None,
                contribution_amount=args["contribution_amount"],
                contribution_frequency=args["contribution_frequency"],
            )
            comparison_series.append(
                {
                    "key": key,
                    "label": preset.name,
                    "points": [
                        {"trade_date": p.trade_date, "equity": p.equity, "index": p.index}
                        for p in compare_result.portfolio_series
                    ],
                }
            )

        return jsonify(
            {
                "run_id": run_id,
                "input": {
                    "market": args["market"],
                    "preset_key": args.get("preset_key"),
                    "preset_name": args.get("preset_name"),
                    "strategy": args["strategy"],
                    "rebalance": args["rebalance"],
                    "start_date": args["start_date"],
                    "end_date": args["end_date"],
                    "initial_capital": args["initial_capital"],
                    "contribution_amount": args["contribution_amount"],
                    "contribution_frequency": args["contribution_frequency"],
                    "fee_bps": args["fee_bps"],
                    "slippage_bps": args["slippage_bps"],
                    "risk_free_rate": args["risk_free_rate"],
                    "benchmark_code": args["benchmark_code"],
                    "compare_preset_keys": args.get("compare_preset_keys", []),
                    "include_benchmark_in_compare": args.get("include_benchmark_in_compare", True),
                    "assets": [{"code": a.code, "weight": a.weight} for a in args["assets"]],
                },
                "summary": {
                    "start_date": result.summary.start_date,
                    "end_date": result.summary.end_date,
                    "effective_start_date": result.summary.effective_start_date,
                    "effective_end_date": result.summary.effective_end_date,
                    "strategy": result.summary.strategy,
                    "rebalance": result.summary.rebalance,
                    "initial_capital": result.summary.initial_capital,
                    "contribution_amount": result.summary.contribution_amount,
                    "contribution_frequency": result.summary.contribution_frequency,
                    "contribution_count": result.summary.contribution_count,
                    "contributed_capital": result.summary.contributed_capital,
                    "net_invested_capital": result.summary.net_invested_capital,
                    "final_equity": result.summary.final_equity,
                    "cumulative_return": result.summary.cumulative_return,
                    "cagr": result.summary.cagr,
                    "mdd": result.summary.mdd,
                    "volatility": result.summary.volatility,
                    "sharpe": result.summary.sharpe,
                    "turnover_ratio": result.summary.turnover_ratio,
                    "rebalance_count": result.summary.rebalance_count,
                    "trade_count": result.summary.trade_count,
                    "benchmark_code": result.summary.benchmark_code,
                    "benchmark_cumulative_return": result.summary.benchmark_cumulative_return,
                    "benchmark_cagr": result.summary.benchmark_cagr,
                },
                "daily": [
                    {
                        "trade_date": d.trade_date,
                        "equity": d.equity,
                        "daily_return": d.daily_return,
                        "drawdown": d.drawdown,
                    }
                    for d in result.daily
                ],
                "trades": [
                    {
                        "trade_date": t.trade_date,
                        "stock_code": t.stock_code,
                        "side": t.side,
                        "quantity": t.quantity,
                        "price": t.price,
                        "gross": t.gross,
                        "cost": t.cost,
                    }
                    for t in result.trades
                ],
                "portfolio_series": [
                    {"trade_date": p.trade_date, "equity": p.equity, "index": p.index}
                    for p in result.portfolio_series
                ],
                "benchmark_series": [
                    {"trade_date": p.trade_date, "equity": p.equity, "index": p.index}
                    for p in result.benchmark_series
                ],
                "comparison_series": comparison_series,
            }
        )

    return app


def _shutdown_scheduler(scheduler):
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)


def _run_price_collect_job(collector: PriceCollector, market: str) -> None:
    stats = collector.collect_market(
        market=market,
        lookback_days=max(5, collector.settings.price_lookback_days),
    )
    print(
        f"[INFO] scheduled price collect market={market} "
        f"stock_count={stats.stock_count} success_count={stats.success_count} "
        f"error_count={stats.error_count} bars_upserted={stats.bars_upserted}"
    )


def _normalize_market_or_404(raw: str) -> str:
    market = str(raw or "").strip().upper()
    if market not in {"KR", "US"}:
        abort(404)
    return market


def _nav_links(current_market: str) -> dict[str, str]:
    market = _normalize_market_or_404(current_market)
    return {
        "dashboard": url_for("dashboard", market=market.lower()),
        "watchlist": url_for("watchlist_page", market=market.lower()),
        "backtest": url_for("backtest_page", market=market.lower()),
        "toggle_kr": url_for("dashboard", market="kr"),
        "toggle_us": url_for("dashboard", market="us"),
    }


def _session_watchlist() -> dict[str, dict[str, list[str]]]:
    raw = session.get("watchlist")
    if not isinstance(raw, dict):
        raw = {}
    stocks = raw.get("stocks")
    sectors = raw.get("sectors")
    if not isinstance(stocks, dict):
        stocks = {}
    if not isinstance(sectors, dict):
        sectors = {}
    normalized = {
        "stocks": {
            "KR": _normalize_watchlist_values(stocks.get("KR")),
            "US": _normalize_watchlist_values(stocks.get("US")),
        },
        "sectors": {
            "KR": _normalize_watchlist_values(sectors.get("KR")),
            "US": _normalize_watchlist_values(sectors.get("US")),
        },
    }
    session["watchlist"] = normalized
    session.modified = True
    return normalized


def _normalize_watchlist_values(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        token = str(item or "").strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _get_watchlist_items(kind: str, market: str) -> list[str]:
    market_norm = _normalize_market_or_404(market)
    watchlist = _session_watchlist()
    if kind not in {"stocks", "sectors"}:
        return []
    return list(watchlist[kind][market_norm])


def _toggle_watchlist_item(kind: str, market: str, value: str) -> None:
    market_norm = _normalize_market_or_404(market)
    token = str(value or "").strip().upper()
    if not token:
        return
    watchlist = _session_watchlist()
    if kind not in {"stocks", "sectors"}:
        return
    current = watchlist[kind][market_norm]
    if token in current:
        current = [x for x in current if x != token]
    else:
        current.append(token)
    watchlist[kind][market_norm] = current
    session["watchlist"] = watchlist
    session.modified = True


def _set_session_market(market: str) -> None:
    session["market"] = _normalize_market_or_404(market)
    session.modified = True


def _get_session_market(default: str = "KR") -> str:
    raw = session.get("market")
    market = str(raw or default).strip().upper()
    if market not in {"KR", "US"}:
        return _normalize_market_or_404(default)
    return market


def _list_sectors_for_market(conn, market: str) -> list[Any]:
    market_norm = _normalize_market_or_404(market)
    rows = conn.execute(
        """
        SELECT DISTINCT
          s.sector_code,
          s.sector_name_ko,
          s.sector_name_en,
          s.taxonomy_version,
          s.is_active,
          s.created_at,
          s.updated_at
        FROM sectors s
        JOIN stock_sector_map m ON m.sector_code = s.sector_code
        JOIN stocks st ON st.code = m.stock_code
        WHERE s.is_active = 1
          AND st.is_active = 1
          AND st.market = ?
        ORDER BY s.sector_code
        """,
        (market_norm,),
    ).fetchall()
    if rows:
        return rows
    if market_norm == "KR":
        return list_sectors(conn, active_only=True)
    return []


def _latest_ticker_digests_for_market(conn, market: str) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        WITH ranked AS (
          SELECT
            d.*,
            ROW_NUMBER() OVER (
              PARTITION BY d.entity_id
              ORDER BY date(d.digest_date) DESC, d.id DESC
            ) AS rn
          FROM daily_digests d
          WHERE d.entity_type = 'ticker'
            AND lower(d.market) = lower(?)
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        """,
        (market,),
    ).fetchall()
    return {str(r["entity_id"]): dict(r) for r in rows}


def _latest_sector_digests(conn, *, market: str | None, limit: int) -> list[dict[str, Any]]:
    if market:
        rows = conn.execute(
            """
            SELECT d.*
            FROM daily_digests d
            WHERE d.entity_type = 'sector'
              AND lower(d.market) = lower(?)
            ORDER BY date(d.digest_date) DESC, d.id DESC
            LIMIT ?
            """,
            (market, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT d.*
            FROM daily_digests d
            WHERE d.entity_type = 'sector'
            ORDER BY date(d.digest_date) DESC, d.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def _latest_entity_digest(conn, *, entity_type: str, entity_id: str, market: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM daily_digests
        WHERE entity_type = ?
          AND entity_id = ?
          AND lower(market) = lower(?)
        ORDER BY date(digest_date) DESC, id DESC
        LIMIT 1
        """,
        (entity_type, entity_id, market),
    ).fetchone()
    return dict(row) if row else None


def _latest_agent_report(conn, *, entity_type: str, entity_id: str, market: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM agent_reports
        WHERE entity_type = ?
          AND entity_id = ?
          AND lower(market) = lower(?)
        ORDER BY date(period_end) DESC, id DESC
        LIMIT 1
        """,
        (entity_type, entity_id, market),
    ).fetchone()
    if row is None:
        return None
    payload = dict(row)
    payload["refs"] = _safe_json_loads(payload.get("refs_json"), default=[])
    return payload


def _latest_item_feed(conn, *, market: str, stock_codes: list[str], limit: int) -> list[dict[str, Any]]:
    if not stock_codes:
        return []
    placeholders = ",".join("?" for _ in stock_codes)
    rows = conn.execute(
        f"""
        SELECT
          i.item_id,
          i.short_summary,
          i.impact_label,
          i.feed_one_liner,
          i.detail_bullets_json,
          i.related_refs_json,
          d.stock_code,
          s.name AS stock_name,
          d.doc_type,
          d.title,
          d.url,
          d.source,
          COALESCE(d.published_at, d.collected_at) AS published_at,
          d.relevance_score,
          d.matched_alias
        FROM item_summaries i
        JOIN documents d ON d.id = i.item_id
        JOIN stocks s ON s.code = d.stock_code
        WHERE lower(s.market) = lower(?)
          AND d.stock_code IN ({placeholders})
        ORDER BY datetime(COALESCE(d.published_at, d.collected_at)) DESC, i.item_id DESC
        LIMIT ?
        """,
        (market, *stock_codes, max(1, limit * 4)),
    ).fetchall()
    curated_rows = _curate_item_summary_rows(rows, limit=limit)
    output: list[dict[str, Any]] = []
    for raw in curated_rows:
        impact = _impact_view(str(raw.get("impact_label") or "neutral"))
        bullets = _safe_json_loads(raw.get("detail_bullets_json"), default=[])
        refs = _safe_json_loads(raw.get("related_refs_json"), default=[])
        one_liner = _item_one_liner(raw)
        source_kind = _classify_source_kind(
            doc_type=str(raw.get("doc_type") or ""),
            source=str(raw.get("source") or ""),
        )
        output.append(
            {
                "item_id": int(raw["item_id"]),
                "stock_code": str(raw["stock_code"]),
                "stock_name": str(raw["stock_name"]),
                "title": str(raw.get("title") or ""),
                "url": str(raw.get("url") or ""),
                "source": str(raw.get("source") or ""),
                "published_at": str(raw.get("published_at") or ""),
                "impact_label": impact["impact_label"],
                "impact_emoji": impact["impact_emoji"],
                "impact_text": impact["impact_text"],
                "impact_css": impact["impact_css"],
                "show_impact": impact["show_impact"],
                "source_kind": source_kind,
                "source_kind_label": _source_kind_label(source_kind),
                "feed_one_liner": str(raw.get("feed_one_liner") or ""),
                "one_liner": one_liner,
                "similar_count": int(raw.get("_similar_count") or 0),
                "detail_bullets": bullets if isinstance(bullets, list) else [],
                "related_refs": refs if isinstance(refs, list) else [],
            }
        )
    return output


def _latest_item_summaries(conn, stock_code: str, *, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          i.item_id,
          i.short_summary,
          i.impact_label,
          i.feed_one_liner,
          i.detail_bullets_json,
          i.related_refs_json,
          i.created_at,
          d.doc_type,
          d.title,
          d.url,
          d.source,
          COALESCE(d.published_at, d.collected_at) AS published_at,
          d.relevance_score,
          d.matched_alias,
          e.card_id
        FROM item_summaries i
        JOIN documents d ON d.id = i.item_id
        LEFT JOIN evidence_cards e ON e.item_id = i.item_id
        WHERE d.stock_code = ?
        ORDER BY datetime(COALESCE(d.published_at, d.collected_at)) DESC, i.item_id DESC
        LIMIT ?
        """,
        (stock_code, max(1, limit * 4)),
    ).fetchall()
    curated_rows = _curate_item_summary_rows(rows, limit=limit)
    result: list[dict[str, Any]] = []
    for raw in curated_rows:
        raw["summary_lines"] = [compact_text(x) for x in str(raw.get("short_summary") or "").splitlines() if compact_text(x)]
        raw["detail_bullets"] = _safe_json_loads(raw.get("detail_bullets_json"), default=[])
        raw["related_refs"] = _safe_json_loads(raw.get("related_refs_json"), default=[])
        raw.update(_impact_view(str(raw.get("impact_label") or "neutral")))
        raw["source_kind"] = _classify_source_kind(
            doc_type=str(raw.get("doc_type") or ""),
            source=str(raw.get("source") or ""),
        )
        raw["source_kind_label"] = _source_kind_label(str(raw["source_kind"]))
        raw["one_liner"] = _item_one_liner(raw)
        result.append(raw)
    return result


def _item_detail_payload(conn, *, item_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
          i.item_id,
          i.short_summary,
          i.impact_label,
          i.feed_one_liner,
          i.detail_bullets_json,
          i.related_refs_json,
          i.created_at,
          d.stock_code,
          s.name AS stock_name,
          s.market,
          d.doc_type,
          d.title,
          d.url,
          d.source,
          COALESCE(d.published_at, d.collected_at) AS published_at
        FROM item_summaries i
        JOIN documents d ON d.id = i.item_id
        JOIN stocks s ON s.code = d.stock_code
        WHERE i.item_id = ?
        LIMIT 1
        """,
        (item_id,),
    ).fetchone()
    if row is None:
        return None
    payload = dict(row)
    payload["summary_lines"] = [compact_text(x) for x in str(payload.get("short_summary") or "").splitlines() if compact_text(x)]
    payload["detail_bullets"] = _safe_json_loads(payload.get("detail_bullets_json"), default=[])
    payload["related_refs"] = _safe_json_loads(payload.get("related_refs_json"), default=[])
    payload.update(_impact_view(str(payload.get("impact_label") or "neutral")))
    payload["source_kind"] = _classify_source_kind(
        doc_type=str(payload.get("doc_type") or ""),
        source=str(payload.get("source") or ""),
    )
    payload["source_kind_label"] = _source_kind_label(str(payload["source_kind"]))
    payload["one_liner"] = _item_one_liner(payload)
    return payload


def _related_documents_for_item(conn, *, stock_code: str, exclude_item_id: int, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          id,
          title,
          url,
          source,
          COALESCE(published_at, collected_at) AS published_at
        FROM documents
        WHERE stock_code = ?
          AND id != ?
        ORDER BY datetime(COALESCE(published_at, collected_at)) DESC, id DESC
        LIMIT ?
        """,
        (stock_code, exclude_item_id, max(1, limit * 4)),
    ).fetchall()
    return _curate_document_rows(rows, doc_type="", limit=limit, apply_relevance=False)


def _curate_item_summary_rows(rows: list[Any], *, limit: int) -> list[dict[str, Any]]:
    ranked_rows = _rank_rows_for_display(rows, doc_type="mixed")
    output: list[dict[str, Any]] = []
    seen_url_by_scope: dict[str, dict[str, int]] = {}
    seen_title_by_scope: dict[str, dict[str, int]] = {}
    seen_title_norm_by_scope: dict[str, list[tuple[str, set[str], int]]] = {}
    seen_one_liner_norm_by_scope: dict[str, list[tuple[str, set[str], float, int]]] = {}
    for ranked in ranked_rows:
        raw = dict(ranked)
        scope = str(raw.get("stock_code") or "").upper()
        doc_type = str(raw.get("doc_type") or "").strip().lower() or "news"
        if not _is_displayable_doc(raw, doc_type=doc_type):
            continue
        seen_url = seen_url_by_scope.setdefault(scope, {})
        seen_title = seen_title_by_scope.setdefault(scope, {})
        seen_title_norms = seen_title_norm_by_scope.setdefault(scope, [])
        seen_one_liner_norms = seen_one_liner_norm_by_scope.setdefault(scope, [])
        url_key = normalize_url(str(raw.get("url") or ""))
        title_key = _title_cluster_key(str(raw.get("title") or ""))
        title_norm, title_tokens = _normalize_title_for_similarity(str(raw.get("title") or ""))
        one_liner = _item_one_liner(raw)
        one_liner_norm, one_liner_tokens = _normalize_title_for_similarity(one_liner)
        one_liner_ts = _event_timestamp(str(raw.get("published_at") or raw.get("collected_at") or ""))
        dup_idx: int | None = None
        if url_key and url_key in seen_url:
            dup_idx = seen_url[url_key]
        if dup_idx is None and title_key and title_key in seen_title:
            dup_idx = seen_title[title_key]
        if dup_idx is None and title_norm:
            dup_idx = _find_similar_title_rep_index(title_norm, title_tokens, seen_title_norms)
        if dup_idx is None and one_liner_norm:
            dup_idx = _find_similar_one_liner_rep_index(
                one_liner_norm,
                one_liner_tokens,
                one_liner_ts,
                seen_one_liner_norms,
            )
        if dup_idx is not None:
            if 0 <= dup_idx < len(output):
                output[dup_idx]["_similar_count"] = int(output[dup_idx].get("_similar_count") or 0) + 1
            continue

        if len(output) >= max(1, limit):
            continue
        raw["_similar_count"] = 0
        output.append(raw)
        rep_idx = len(output) - 1
        if url_key:
            seen_url[url_key] = rep_idx
        if title_key:
            seen_title[title_key] = rep_idx
        if title_norm:
            seen_title_norms.append((title_norm, title_tokens, rep_idx))
        if one_liner_norm:
            seen_one_liner_norms.append((one_liner_norm, one_liner_tokens, one_liner_ts, rep_idx))
    return output


def _curate_document_rows(
    rows: list[Any],
    *,
    doc_type: str,
    limit: int,
    apply_relevance: bool = True,
) -> list[dict[str, Any]]:
    ranked_rows = _rank_rows_for_display(rows, doc_type=doc_type) if apply_relevance else [dict(r) for r in rows]
    output: list[dict[str, Any]] = []
    seen_url_by_scope: dict[str, set[str]] = {}
    seen_title_by_scope: dict[str, set[str]] = {}
    seen_title_norm_by_scope: dict[str, list[tuple[str, set[str]]]] = {}
    for raw in ranked_rows:
        scope = str(raw.get("stock_code") or "").upper()
        if apply_relevance and not _is_displayable_doc(raw, doc_type=doc_type):
            continue
        seen_url = seen_url_by_scope.setdefault(scope, set())
        seen_title = seen_title_by_scope.setdefault(scope, set())
        seen_title_norms = seen_title_norm_by_scope.setdefault(scope, [])
        url_key = normalize_url(str(raw.get("url") or ""))
        title_key = _title_cluster_key(str(raw.get("title") or ""))
        title_norm, title_tokens = _normalize_title_for_similarity(str(raw.get("title") or ""))
        if url_key and url_key in seen_url:
            continue
        if title_key and title_key in seen_title:
            continue
        if title_norm and _is_similar_title_seen(title_norm, title_tokens, seen_title_norms):
            continue
        if url_key:
            seen_url.add(url_key)
        if title_key:
            seen_title.add(title_key)
        if title_norm:
            seen_title_norms.append((title_norm, title_tokens))
        source_kind = _classify_source_kind(
            doc_type=str(raw.get("doc_type") or doc_type or ""),
            source=str(raw.get("source") or ""),
        )
        raw["source_kind"] = source_kind
        raw["source_kind_label"] = _source_kind_label(source_kind)
        output.append(raw)
        if len(output) >= max(1, limit):
            break
    return output


def _rank_rows_for_display(rows: list[Any], *, doc_type: str) -> list[dict[str, Any]]:
    raw_rows = [dict(r) for r in rows]
    if not raw_rows:
        return raw_rows

    token_sets: list[set[str]] = []
    for row in raw_rows:
        _norm, tokens = _normalize_title_for_similarity(str(row.get("title") or ""))
        token_sets.append(tokens)

    n = len(raw_rows)
    neighbors: list[list[tuple[int, float]]] = [[] for _ in range(n)]
    degree_scores = [0.0 for _ in range(n)]

    for i in range(n):
        tokens_i = token_sets[i]
        if not tokens_i:
            continue
        for j in range(i + 1, n):
            tokens_j = token_sets[j]
            if not tokens_j:
                continue
            overlap = len(tokens_i & tokens_j)
            if overlap < 2:
                continue
            base = min(len(tokens_i), len(tokens_j))
            if base <= 0:
                continue
            sim = overlap / base
            if sim < 0.34:
                continue
            w = min(1.0, sim)
            neighbors[i].append((j, w))
            neighbors[j].append((i, w))
            degree_scores[i] += w
            degree_scores[j] += w

    eigen = _power_iteration_centrality(neighbors)
    degree_norm = _min_max_norm(degree_scores)
    centrality = [
        (0.65 * eigen[i]) + (0.35 * degree_norm[i])
        for i in range(n)
    ]

    event_ts = [_event_timestamp(str(r.get("published_at") or r.get("collected_at") or "")) for r in raw_rows]
    fresh = _min_max_norm(event_ts)

    for i, row in enumerate(raw_rows):
        relevance = _clamp01(_to_float(row.get("relevance_score")) or 0.0)
        source = str(row.get("source") or "").strip().lower()
        source_prior = _source_rank_prior(source=source, doc_type=doc_type)
        rank_score = (
            0.45 * relevance
            + 0.35 * centrality[i]
            + 0.15 * fresh[i]
            + 0.05 * source_prior
        )
        row["_rank_score"] = round(rank_score, 6)
        row["_rank_centrality"] = round(centrality[i], 6)
        row["_rank_relevance"] = round(relevance, 6)
        row["_rank_freshness"] = round(fresh[i], 6)

    raw_rows.sort(
        key=lambda r: (
            float(r.get("_rank_score") or 0.0),
            _event_timestamp(str(r.get("published_at") or r.get("collected_at") or "")),
            int(r.get("id") or 0),
        ),
        reverse=True,
    )
    return raw_rows


def _power_iteration_centrality(neighbors: list[list[tuple[int, float]]], max_iter: int = 30) -> list[float]:
    n = len(neighbors)
    if n == 0:
        return []
    if all(len(v) == 0 for v in neighbors):
        return [0.0 for _ in range(n)]

    vec = [1.0 / n for _ in range(n)]
    for _ in range(max_iter):
        new_vec = [0.0 for _ in range(n)]
        for i in range(n):
            total = 0.0
            for j, w in neighbors[i]:
                total += w * vec[j]
            new_vec[i] = total
        norm = math.sqrt(sum(v * v for v in new_vec))
        if norm <= 1e-12:
            break
        new_vec = [v / norm for v in new_vec]
        delta = sum(abs(new_vec[i] - vec[i]) for i in range(n))
        vec = new_vec
        if delta < 1e-6:
            break
    return _min_max_norm(vec)


def _min_max_norm(values: list[float]) -> list[float]:
    if not values:
        return []
    low = min(values)
    high = max(values)
    if high - low <= 1e-12:
        return [0.0 for _ in values]
    return [(v - low) / (high - low) for v in values]


def _event_timestamp(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        dt = datetime.fromisoformat(text)
        return dt.timestamp()
    except ValueError:
        return 0.0


def _classify_source_kind(*, doc_type: str, source: str) -> str:
    kind = str(doc_type or "").strip().lower()
    src = str(source or "").strip().lower()
    if kind in {"news", "report", "filing"}:
        return kind
    if src in {"sec_edgar", "dart", "krx_dart", "opendart"}:
        return "filing"
    if "edgar" in src or "dart" in src or "filing" in src:
        return "filing"
    if "research" in src or "report" in src or "consensus" in src:
        return "report"
    return "news"


def _source_kind_label(kind: str) -> str:
    token = str(kind or "").strip().lower()
    if token == "report":
        return "리포트"
    if token == "filing":
        return "공시"
    return "뉴스"


def _source_rank_prior(*, source: str, doc_type: str) -> float:
    src = str(source or "").strip().lower()
    kind = str(doc_type or "").strip().lower()
    if kind == "report":
        return 0.9
    if src == "sec_edgar":
        return 0.8
    if src == "naver_finance_research":
        return 0.75
    if src == "naver_news":
        return 0.55
    return 0.5


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _is_displayable_doc(raw: dict[str, Any], *, doc_type: str) -> bool:
    title = compact_text(str(raw.get("title") or ""))
    if len(title) < 6:
        return False
    source = str(raw.get("source") or "").strip().lower()
    score = _to_float(raw.get("relevance_score")) or 0.0
    alias = compact_text(str(raw.get("matched_alias") or ""))
    threshold = _display_threshold(doc_type=doc_type, source=source)
    if alias:
        return score >= max(0.08, threshold - 0.12)
    return score >= threshold


def _display_threshold(*, doc_type: str, source: str) -> float:
    kind = str(doc_type or "").strip().lower()
    src = str(source or "").strip().lower()
    if kind == "report":
        return 0.12
    if src == "sec_edgar":
        return 0.05
    if src == "naver_news":
        return 0.30
    return 0.22


def _title_cluster_key(title: str) -> str:
    value = compact_text(title).lower()
    if not value:
        return ""
    value = re.sub(r"\[[^\]]{1,40}\]", " ", value)
    value = re.sub(r"\([^)]{1,40}\)", " ", value)
    value = re.sub(r"[^0-9a-z가-힣]+", " ", value)
    tokens = [tok for tok in value.split() if tok and len(tok) > 1]
    if not tokens:
        return ""
    return " ".join(tokens[:12])


def _normalize_title_for_similarity(title: str) -> tuple[str, set[str]]:
    value = compact_text(title).lower()
    if not value:
        return "", set()
    value = re.sub(r"\[[^\]]{1,40}\]", " ", value)
    value = re.sub(r"\([^)]{1,40}\)", " ", value)
    value = re.sub(r"[^0-9a-z가-힣]+", " ", value)
    stopwords = {
        "속보",
        "단독",
        "종합",
        "영상",
        "인터뷰",
        "기자",
        "뉴스",
        "리포트",
        "증권",
        "강조했다",
        "밝혔다",
        "전했다",
    }
    tokens: list[str] = []
    for token in value.split():
        canonical = _canonicalize_similarity_token(token)
        if len(canonical) <= 1 or canonical in stopwords:
            continue
        tokens.append(canonical)
    if not tokens:
        return "", set()
    normalized = " ".join(tokens[:20])
    return normalized, set(tokens[:20])


def _canonicalize_similarity_token(token: str) -> str:
    t = compact_text(str(token or "")).lower()
    if not t:
        return ""
    if len(t) > 3 and t.startswith("한국"):
        t = t[2:]
    if "주주총회" in t or "주총" in t:
        return "주총"
    if "거버넌스포럼" in t:
        return "거버넌스포럼"

    # Strip common Korean postpositions/endings for better overlap matching.
    while len(t) > 2 and t[-1] in {"이", "가", "은", "는", "을", "를", "의", "에", "도", "만", "로", "와", "과"}:
        t = t[:-1]
    for suffix in ("에게", "에서", "으로", "라고", "이고", "이며", "하다", "했다", "한다", "됐다", "되었다"):
        if len(t) > len(suffix) + 1 and t.endswith(suffix):
            t = t[: -len(suffix)]
            break
    return t


def _is_similar_title_seen(
    current_title: str,
    current_tokens: set[str],
    seen_titles: list[tuple[str, set[str]]],
) -> bool:
    if not current_title:
        return False
    for seen_title, seen_tokens in seen_titles:
        if current_title == seen_title:
            return True
        if len(current_title) >= 12 and len(seen_title) >= 12:
            if current_title in seen_title or seen_title in current_title:
                return True
        if not current_tokens or not seen_tokens:
            continue
        overlap = len(current_tokens & seen_tokens)
        small = min(len(current_tokens), len(seen_tokens))
        large = max(len(current_tokens), len(seen_tokens))
        if small > 0 and overlap >= 3 and (overlap / small) >= 0.8:
            return True
        if large > 0 and overlap >= 4 and (overlap / large) >= 0.67:
            return True
    return False


def _find_similar_title_rep_index(
    current_title: str,
    current_tokens: set[str],
    seen_titles: list[tuple[str, set[str], int]],
) -> int | None:
    if not current_title:
        return None
    for seen_title, seen_tokens, rep_idx in seen_titles:
        if current_title == seen_title:
            return rep_idx
        if len(current_title) >= 12 and len(seen_title) >= 12:
            if current_title in seen_title or seen_title in current_title:
                return rep_idx
        if not current_tokens or not seen_tokens:
            continue
        overlap = len(current_tokens & seen_tokens)
        small = min(len(current_tokens), len(seen_tokens))
        large = max(len(current_tokens), len(seen_tokens))
        if small > 0 and overlap >= 3 and (overlap / small) >= 0.8:
            return rep_idx
        if large > 0 and overlap >= 4 and (overlap / large) >= 0.67:
            return rep_idx
    return None


def _is_similar_one_liner_seen(
    current_text: str,
    current_tokens: set[str],
    current_ts: float,
    seen_texts: list[tuple[str, set[str], float]],
) -> bool:
    if not current_text:
        return False
    for seen_text, seen_tokens, seen_ts in seen_texts:
        if current_text == seen_text:
            return True
        if len(current_text) >= 16 and len(seen_text) >= 16:
            if current_text in seen_text or seen_text in current_text:
                return True
        if not current_tokens or not seen_tokens:
            continue
        overlap = len(current_tokens & seen_tokens)
        small = min(len(current_tokens), len(seen_tokens))
        large = max(len(current_tokens), len(seen_tokens))
        if small <= 0 or large <= 0:
            continue

        # Strict rule for very close paraphrases.
        if overlap >= 4 and (overlap / small) >= 0.55:
            return True

        # Lenient rule for same-event coverage close in time.
        time_gap = abs(float(current_ts or 0.0) - float(seen_ts or 0.0))
        if time_gap <= 7 * 24 * 3600:
            if overlap >= 4 and ((overlap / small) >= 0.32 or (overlap / large) >= 0.28):
                return True
            ratio = _sequence_similarity(current_text, seen_text)
            if ratio >= 0.62:
                return True
            if ratio >= 0.55 and _shared_event_keyword_count(current_text, seen_text) >= 1:
                return True
    return False


def _find_similar_one_liner_rep_index(
    current_text: str,
    current_tokens: set[str],
    current_ts: float,
    seen_texts: list[tuple[str, set[str], float, int]],
) -> int | None:
    if not current_text:
        return None
    for seen_text, seen_tokens, seen_ts, rep_idx in seen_texts:
        if current_text == seen_text:
            return rep_idx
        if len(current_text) >= 16 and len(seen_text) >= 16:
            if current_text in seen_text or seen_text in current_text:
                return rep_idx
        if not current_tokens or not seen_tokens:
            continue
        overlap = len(current_tokens & seen_tokens)
        small = min(len(current_tokens), len(seen_tokens))
        large = max(len(current_tokens), len(seen_tokens))
        if small <= 0 or large <= 0:
            continue
        if overlap >= 4 and (overlap / small) >= 0.55:
            return rep_idx
        time_gap = abs(float(current_ts or 0.0) - float(seen_ts or 0.0))
        if time_gap <= 7 * 24 * 3600:
            if overlap >= 4 and ((overlap / small) >= 0.32 or (overlap / large) >= 0.28):
                return rep_idx
            ratio = _sequence_similarity(current_text, seen_text)
            if ratio >= 0.62:
                return rep_idx
            if ratio >= 0.55 and _shared_event_keyword_count(current_text, seen_text) >= 1:
                return rep_idx
    return None


def _sequence_similarity(a: str, b: str) -> float:
    left = compact_text(str(a or ""))
    right = compact_text(str(b or ""))
    if not left or not right:
        return 0.0
    return float(SequenceMatcher(None, left, right).ratio())


def _shared_event_keyword_count(a: str, b: str) -> int:
    keywords = (
        "주주총회",
        "주총",
        "경영권",
        "지배구조",
        "거버넌스",
        "실적",
        "가이던스",
        "수주",
        "규제",
        "소송",
        "제재",
        "합병",
        "인수",
        "공급망",
        "배당",
        "증자",
    )
    left = compact_text(str(a or ""))
    right = compact_text(str(b or ""))
    if not left or not right:
        return 0
    return sum(1 for key in keywords if key in left and key in right)


def _impact_view(impact_label: str) -> dict[str, str]:
    label = (impact_label or "neutral").strip().lower()
    if label == "positive":
        return {
            "impact_label": "positive",
            "impact_emoji": "😀",
            "impact_text": "호재",
            "impact_css": "text-emerald-600",
            "show_impact": "1",
        }
    if label == "negative":
        return {
            "impact_label": "negative",
            "impact_emoji": "😡",
            "impact_text": "악재",
            "impact_css": "text-red-600",
            "show_impact": "1",
        }
    return {
        "impact_label": "neutral",
        "impact_emoji": "",
        "impact_text": "",
        "impact_css": "text-gray-500",
        "show_impact": "",
    }


def _item_one_liner(raw: dict[str, Any]) -> str:
    title = compact_text(str(raw.get("title") or ""))
    direct = compact_text(str(raw.get("feed_one_liner") or ""))
    if direct and not _is_title_like(direct, title):
        return direct[:120]
    short_summary = str(raw.get("short_summary") or "")
    fallback_line = ""
    if short_summary:
        for line in short_summary.splitlines():
            cleaned = _clean_summary_line(line)
            if cleaned:
                if not fallback_line:
                    fallback_line = cleaned
                if not _is_title_like(cleaned, title):
                    return cleaned[:120]
    if direct:
        return direct[:120]
    if fallback_line:
        return fallback_line[:120]
    return title[:120]


def _clean_summary_line(text: str) -> str:
    value = compact_text(str(text or ""))
    if not value:
        return ""
    value = re.sub(r"^\[(FACT|INTERPRETATION|RISK)\]\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\(src:\s*ITEM-[^)]+\)", "", value, flags=re.IGNORECASE)
    value = compact_text(value)
    return value


def _is_title_like(candidate: str, title: str) -> bool:
    c = re.sub(r"\s+", " ", compact_text(candidate)).strip().lower()
    t = re.sub(r"\s+", " ", compact_text(title)).strip().lower()
    if not c or not t:
        return False
    if c == t:
        return True
    if c in t or t in c:
        return True
    c_tokens = [tok for tok in re.split(r"[^\w가-힣]+", c) if tok]
    t_tokens = {tok for tok in re.split(r"[^\w가-힣]+", t) if tok}
    if not c_tokens or not t_tokens:
        return False
    overlap = sum(1 for tok in c_tokens if tok in t_tokens)
    return overlap / max(1, len(c_tokens)) >= 0.7


def _build_digest_view(conn, digest_row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not digest_row:
        return None
    summary_lines = _split_non_empty_lines(str(digest_row.get("summary_8line") or ""))
    change_lines = _split_non_empty_lines(str(digest_row.get("change_3") or ""))
    question_lines = _split_non_empty_lines(str(digest_row.get("open_questions") or ""))
    refs = _safe_json_loads(digest_row.get("refs_json"), default=[])
    ref_sources = _resolve_digest_ref_sources(conn, refs)
    return {
        "summary_lines": summary_lines,
        "change_lines": change_lines,
        "question_lines": question_lines,
        "refs": refs,
        "ref_sources": ref_sources,
    }


def _resolve_digest_ref_sources(conn, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    card_ids = [str(r.get("card_id") or "") for r in refs if str(r.get("card_id") or "")]
    if not card_ids:
        return []
    placeholders = ",".join("?" for _ in card_ids)
    rows = conn.execute(
        f"""
        SELECT
          e.card_id,
          e.item_id,
          e.url AS evidence_url,
          e.source_name,
          e.source_type,
          e.published_at AS evidence_published_at,
          d.title,
          d.url AS document_url,
          d.source AS document_source,
          COALESCE(d.published_at, d.collected_at) AS document_published_at
        FROM evidence_cards e
        LEFT JOIN documents d ON d.id = e.item_id
        WHERE e.card_id IN ({placeholders})
        """,
        tuple(card_ids),
    ).fetchall()
    by_card = {str(r["card_id"]): dict(r) for r in rows}

    output: list[dict[str, Any]] = []
    for ref in refs:
        card_id = str(ref.get("card_id") or "")
        if not card_id:
            continue
        source = by_card.get(card_id)
        if not source:
            continue
        output.append(
            {
                "alias": str(ref.get("alias") or ""),
                "card_id": card_id,
                "item_id": source.get("item_id"),
                "title": str(source.get("title") or source.get("source_name") or card_id),
                "url": str(source.get("document_url") or source.get("evidence_url") or ""),
                "source": str(source.get("document_source") or source.get("source_name") or ""),
                "published_at": str(source.get("document_published_at") or source.get("evidence_published_at") or ""),
            }
        )
    return output


def _digest_line1(digest_row: dict[str, Any] | None) -> str:
    if not digest_row:
        return ""
    lines = _split_non_empty_lines(str(digest_row.get("summary_8line") or ""))
    if not lines:
        return ""
    line1 = lines[0]
    line1 = re.sub(r"^\s*\d+\)\s*", "", line1)
    line1 = re.sub(r"^\s*\[[^\]]+\]\s*", "", line1)
    line1 = re.sub(r"\s*\(cards:\s*[^)]*\)\s*$", "", line1).strip()
    return line1


def _split_non_empty_lines(text: str) -> list[str]:
    return [compact_text(x) for x in str(text or "").splitlines() if compact_text(x)]


def _safe_json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


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
    normalized = compact_text(text).lower()
    if normalized.startswith("결론") or normalized.startswith("conclusion"):
        return "conclusion"
    if normalized.startswith("근거") or normalized.startswith("evidence"):
        return "evidence"
    if normalized.startswith("리스크") or normalized.startswith("risk"):
        return "risk"
    if normalized.startswith("체크포인트") or normalized.startswith("checkpoint"):
        return "checkpoint"
    if normalized.startswith("최종 판단") or normalized.startswith("final"):
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


def _format_datetime_display(value: Any, *, market: str | None = None) -> str:
    text = compact_text(str(value or ""))
    if not text:
        return "-"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text

    candidate = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return text

    if dt.tzinfo is None:
        return dt.strftime("%Y-%m-%d %H:%M")

    market_norm = str(market or "").strip().upper()
    if market_norm == "KR":
        dt_local = _to_market_tz(dt, market="KR")
        return f"{dt_local.strftime('%Y-%m-%d %H:%M')} KST"
    if market_norm == "US":
        dt_local = _to_market_tz(dt, market="US")
        return f"{dt_local.strftime('%Y-%m-%d %H:%M')} ET"

    return f"{dt.strftime('%Y-%m-%d %H:%M')} {_offset_label(dt)}".strip()


def _to_market_tz(dt: datetime, *, market: str) -> datetime:
    market_norm = market.strip().upper()
    if market_norm == "KR":
        try:
            return dt.astimezone(ZoneInfo("Asia/Seoul"))
        except Exception:
            return dt.astimezone(timezone(timedelta(hours=9)))
    if market_norm == "US":
        try:
            return dt.astimezone(ZoneInfo("America/New_York"))
        except Exception:
            return dt
    return dt


def _offset_label(dt: datetime) -> str:
    offset = dt.utcoffset()
    if offset is None:
        return ""
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


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


def _parse_backtest_request(payload: dict[str, Any]) -> dict[str, Any]:
    preset_key = str(payload.get("preset") or "").strip().lower()
    raw_weights = payload.get("weights")
    market_raw = str(payload.get("market") or "").strip().upper()
    if preset_key and raw_weights:
        raise ValueError("use either preset or weights, not both")
    if not preset_key and not raw_weights:
        raise ValueError("preset or weights is required")

    assets: list[BacktestAsset]
    preset_name: str | None = None
    benchmark_code = str(payload.get("benchmark_code") or "").strip().upper() or None
    market = market_raw
    if preset_key:
        preset = get_portfolio_preset(preset_key)
        if preset is None:
            raise ValueError(f"unknown preset: {preset_key}")
        assets = list(preset.assets)
        preset_name = preset.name
        market = market or preset.market
        if not benchmark_code and preset.benchmark_code:
            benchmark_code = preset.benchmark_code
    else:
        assets = _parse_backtest_weights(raw_weights)
    if market not in {"KR", "US"}:
        raise ValueError("market must be KR or US")

    start_date = str(payload.get("start_date") or "").strip()
    end_date = str(payload.get("end_date") or "").strip()
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")
    _validate_iso_date(start_date, "start_date")
    _validate_iso_date(end_date, "end_date")

    strategy = str(payload.get("strategy") or "buy_and_hold").strip().lower()
    if strategy not in {"buy_and_hold", "monthly_rebalance"}:
        raise ValueError("strategy must be buy_and_hold or monthly_rebalance")
    rebalance = str(payload.get("rebalance") or "monthly").strip().lower()
    if rebalance not in {"none", "monthly", "quarterly", "yearly"}:
        raise ValueError("rebalance must be none|monthly|quarterly|yearly")

    initial_capital_raw = payload.get("initial_capital")
    if initial_capital_raw is None or str(initial_capital_raw).strip() == "":
        initial_capital = 10_000.0 if market == "US" else 10_000_000.0
    else:
        initial_capital = _parse_positive_float(initial_capital_raw, "initial_capital")
    contribution_amount = _parse_non_negative_float(payload.get("contribution_amount", 0.0), "contribution_amount")
    contribution_frequency = str(payload.get("contribution_frequency") or "none").strip().lower()
    if contribution_frequency not in {"none", "monthly", "quarterly", "yearly"}:
        raise ValueError("contribution_frequency must be none|monthly|quarterly|yearly")
    fee_bps = _parse_non_negative_float(payload.get("fee_bps", 0.0), "fee_bps")
    slippage_bps = _parse_non_negative_float(payload.get("slippage_bps", 0.0), "slippage_bps")
    risk_free_rate = _parse_non_negative_float(payload.get("risk_free_rate", 0.03), "risk_free_rate")
    compare_preset_keys = _parse_compare_preset_keys(payload.get("compare_presets"))
    include_benchmark_in_compare = _parse_bool(payload.get("include_benchmark_in_compare", True), default=True)
    for key in compare_preset_keys:
        p = get_portfolio_preset(key)
        if p is None:
            raise ValueError(f"unknown compare preset: {key}")
        if p.market != market:
            raise ValueError(f"compare preset market mismatch: {key} is {p.market}, request market is {market}")

    return {
        "preset_key": preset_key or None,
        "preset_name": preset_name,
        "market": market,
        "assets": assets,
        "benchmark_code": benchmark_code,
        "start_date": start_date,
        "end_date": end_date,
        "strategy": strategy,
        "rebalance": rebalance,
        "initial_capital": initial_capital,
        "contribution_amount": contribution_amount,
        "contribution_frequency": contribution_frequency,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "risk_free_rate": risk_free_rate,
        "compare_preset_keys": compare_preset_keys,
        "include_benchmark_in_compare": include_benchmark_in_compare,
    }


def _parse_backtest_weights(raw_weights: Any) -> list[BacktestAsset]:
    items: list[BacktestAsset] = []
    if isinstance(raw_weights, str):
        parts = [p.strip() for p in re.split(r"[,\s;]+", raw_weights.strip()) if p.strip()]
        for part in parts:
            if ":" not in part:
                raise ValueError(f"invalid weight token: {part}")
            code_raw, weight_raw = part.split(":", 1)
            code = code_raw.strip().upper()
            if not code:
                continue
            try:
                weight = float(weight_raw.strip())
            except ValueError as exc:
                raise ValueError(f"invalid weight value: {part}") from exc
            items.append(BacktestAsset(code=code, weight=weight))
    elif isinstance(raw_weights, list):
        for row in raw_weights:
            if not isinstance(row, dict):
                raise ValueError("weights list items must be objects")
            code = str(row.get("code") or "").strip().upper()
            if not code:
                continue
            try:
                weight = float(row.get("weight"))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid weight for code={code}") from exc
            items.append(BacktestAsset(code=code, weight=weight))
    else:
        raise ValueError("weights must be string or list")
    if not items:
        raise ValueError("weights are empty")
    return items


def _parse_compare_preset_keys(raw: Any) -> list[str]:
    if raw is None:
        return []
    keys: list[str] = []
    if isinstance(raw, str):
        parts = [p.strip().lower() for p in re.split(r"[,\s;]+", raw.strip()) if p.strip()]
        for p in parts:
            if p not in keys:
                keys.append(p)
        return keys
    if isinstance(raw, list):
        for item in raw:
            key = str(item or "").strip().lower()
            if not key:
                continue
            if key not in keys:
                keys.append(key)
        return keys
    raise ValueError("compare_presets must be string or list")


def _validate_iso_date(value: str, field_name: str) -> None:
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _parse_positive_float(value: Any, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return parsed


def _parse_non_negative_float(value: Any, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return parsed


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "t", "yes", "y", "on"}

