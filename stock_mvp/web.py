from __future__ import annotations

import atexit
import json
import os
import re
import uuid
from datetime import datetime
from typing import Any

from flask import Flask, abort, jsonify, redirect, render_template, request, session, url_for

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
    latest_sector_summaries,
    latest_summaries_by_stock,
    latest_summary,
    latest_sector_summary,
    list_sectors,
    list_stocks_by_market,
    sector_summary_source_documents,
    summary_source_documents,
    upsert_stocks,
)
from stock_mvp.pipeline import CollectionPipeline, PipelineBusyError
from stock_mvp.prices import PriceCollector
from stock_mvp.sector_mapping import sync_sector_mapping_for_active_stocks
from stock_mvp.stocks import DEFAULT_STOCKS
from stock_mvp.universe import UniverseRefresher
from stock_mvp.utils import compact_text, document_identity_key


def create_app(settings: Settings | None = None) -> Flask:
    cfg = settings or load_settings()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SETTINGS"] = cfg
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "stock-mvp-dev-session-key")

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
        with connect(cfg.db_path) as conn:
            market_stocks = list_stocks_by_market(conn, market_norm, active_only=True)
            market_codes = {str(r["code"]) for r in market_stocks}
            all_summary_rows = latest_summaries_by_stock(conn)
            rows = [r for r in all_summary_rows if str(r["stock_code"]) in market_codes]
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
                latest_row = latest_sector_summary(conn, sector_code)
                subscribed_sectors.append(
                    {
                        "sector_code": sector_code,
                        "sector_name": sector_name_map.get(sector_code, sector_code),
                        "line1": str(latest_row["line1"] or "") if latest_row else "",
                        "as_of": str(latest_row["as_of"] or "") if latest_row else "",
                    }
                )

        rows = rows[:20]
        return render_template(
            "index.html",
            current_market=market_norm,
            nav_links=_nav_links(market_norm),
            active_page="dashboard",
            rows=rows,
            subscribed_stocks=subscribed_stocks,
            subscribed_sectors=subscribed_sectors,
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
        with connect(cfg.db_path) as conn:
            stock_row = get_stock(conn, code_norm)
            if stock_row is None:
                return redirect(url_for("dashboard", market=market_norm.lower()))
            stock_market = str(stock_row["market"]).upper()
            if stock_market != market_norm:
                return redirect(url_for("stock_detail", market=stock_market.lower(), code=code_norm))
            sector_rows = get_stock_sectors(conn, code_norm)
            financial_row = latest_financial_snapshot(conn, code_norm)
            summary_row = latest_summary(conn, code_norm)
            source_rows_raw = summary_source_documents(conn, int(summary_row["id"])) if summary_row else []
            news_rows = latest_documents_by_type(conn, code_norm, doc_type="news", limit=100)
            report_rows = latest_documents_by_type(conn, code_norm, doc_type="report", limit=100)
            sector_briefs: list[dict[str, str]] = []
            for sector_row in sector_rows:
                sector_code = str(sector_row["sector_code"])
                sector_summary = latest_sector_summary(conn, sector_code)
                sector_briefs.append(
                    {
                        "sector_code": sector_code,
                        "sector_name": str(sector_row["sector_name_ko"] or sector_row["sector_name_en"]),
                        "line1": str(sector_summary["line1"] or "") if sector_summary else "",
                        "as_of": str(sector_summary["as_of"] or "") if sector_summary else "",
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
        summary_sections = _build_summary_sections(summary_row, source_rows_raw)
        subscribed = code_norm in set(_get_watchlist_items("stocks", market_norm))
        return render_template(
            "stock_detail.html",
            current_market=market_norm,
            nav_links=_nav_links(market_norm),
            active_page="dashboard",
            stock=stock,
            summary=summary_row,
            summary_sections=summary_sections,
            news_rows=news_rows,
            report_rows=report_rows,
            doc_initial_limit=10,
            is_subscribed=subscribed,
        )

    def _run_collect_now():
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

    @app.route("/collect", methods=["POST"])
    def collect_now():
        return _run_collect_now()

    @app.route("/<market>/collect", methods=["POST"])
    def collect_now_market(market: str):
        _normalize_market_or_404(market)
        return _run_collect_now()

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

