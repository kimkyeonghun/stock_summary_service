from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Callable

from stock_mvp.agents.translator import Translator, get_translation_metrics, reset_translation_metrics
from stock_mvp.config import Settings, load_settings
from stock_mvp.database import connect, init_db


ALLOWED_SCOPES = {"item", "evidence", "digest", "report", "profile"}
INCREMENTAL_DEFAULT_SCOPES = {"item", "evidence", "digest", "report"}
PROGRESS_EVERY = 50


@dataclass
class BackfillCounter:
    scanned: int = 0
    written: int = 0
    skipped: int = 0
    errors: int = 0


@dataclass
class BackfillRunSummary:
    mode: str
    scopes: set[str]
    market: str
    max_rows: int
    translation_retries: int
    elapsed_sec: float
    counters: dict[str, BackfillCounter]
    translation_calls: int
    translation_cache_hits: int
    translation_elapsed_sec: float
    translation_fail_count: int
    days: int | None = None
    run_started_at: str | None = None


def run_backfill(
    *,
    days: int = 14,
    scope: str | set[str] | list[str] | tuple[str, ...] = "all",
    market: str = "ALL",
    max_rows: int = 0,
    translation_retries: int = 0,
    settings: Settings | None = None,
    reset_metrics: bool = True,
) -> BackfillRunSummary:
    scopes = _parse_scopes(scope)
    market_upper = str(market or "ALL").strip().upper()
    market_filter = "" if market_upper == "ALL" else market_upper.lower()
    settings_obj = settings or load_settings()

    translator = Translator(settings_obj)
    translator.set_max_retries(translation_retries)
    if not translator.enabled:
        raise RuntimeError("Translation is disabled. Check TRANSLATION_ENABLED/TRANSLATION_PROVIDER/API key.")

    if reset_metrics:
        reset_translation_metrics()

    started = time.perf_counter()
    counters = {scope_name: BackfillCounter() for scope_name in scopes}
    with connect(settings_obj.db_path) as conn:
        init_db(conn)
        _run_backfill_with_existing_connection(
            conn,
            translator,
            scopes=scopes,
            days=max(1, int(days)),
            market=market_filter,
            max_rows=max_rows,
            counters=counters,
        )
        conn.commit()

    elapsed = time.perf_counter() - started
    metrics = get_translation_metrics()
    return BackfillRunSummary(
        mode="days",
        scopes=scopes,
        market=market_upper,
        max_rows=int(max_rows),
        translation_retries=max(0, int(translation_retries)),
        elapsed_sec=elapsed,
        counters=counters,
        translation_calls=metrics.calls,
        translation_cache_hits=metrics.cache_hits,
        translation_elapsed_sec=metrics.elapsed_sec,
        translation_fail_count=metrics.fail_count,
        days=max(1, int(days)),
    )


def run_incremental_backfill(
    conn,
    *,
    settings: Settings,
    run_started_at: str,
    market: str = "",
    scopes: set[str] | None = None,
    max_rows: int = 0,
    translation_retries: int = 0,
) -> dict[str, BackfillCounter]:
    active_scopes = set(scopes or INCREMENTAL_DEFAULT_SCOPES)
    active_scopes &= INCREMENTAL_DEFAULT_SCOPES
    if not active_scopes:
        return {}

    translator = Translator(settings)
    translator.set_max_retries(translation_retries)
    if not translator.enabled:
        raise RuntimeError("Translation is disabled. Check TRANSLATION_ENABLED/TRANSLATION_PROVIDER/API key.")

    market_filter = str(market or "").strip().lower()
    counters = {scope_name: BackfillCounter() for scope_name in active_scopes}

    if "item" in active_scopes:
        backfill_item_summaries_since(
            conn,
            translator,
            run_started_at=run_started_at,
            market=market_filter,
            max_rows=max_rows,
            counter=counters["item"],
        )
    if "evidence" in active_scopes:
        backfill_evidence_cards_since(
            conn,
            translator,
            run_started_at=run_started_at,
            market=market_filter,
            max_rows=max_rows,
            counter=counters["evidence"],
        )
    if "digest" in active_scopes:
        backfill_daily_digests_since(
            conn,
            translator,
            run_started_at=run_started_at,
            market=market_filter,
            max_rows=max_rows,
            counter=counters["digest"],
        )
    if "report" in active_scopes:
        backfill_agent_reports_since(
            conn,
            translator,
            run_started_at=run_started_at,
            market=market_filter,
            max_rows=max_rows,
            counter=counters["report"],
        )
    return counters


def print_backfill_summary(summary: BackfillRunSummary) -> None:
    print("translate_backfill done")
    if summary.mode == "days":
        print(f"days={summary.days}")
    elif summary.run_started_at:
        print(f"run_started_at={summary.run_started_at}")
    print(f"market={summary.market}")
    print(f"max_rows_per_scope={summary.max_rows if summary.max_rows > 0 else 'unlimited'}")
    print(f"translation_retries={summary.translation_retries}")
    print(f"scope={','.join(sorted(summary.scopes))}")
    for scope in sorted(summary.scopes):
        counter = summary.counters[scope]
        print(
            f"{scope}: scanned={counter.scanned} written={counter.written} "
            f"skipped={counter.skipped} errors={counter.errors}"
        )
    print(f"translation_calls={summary.translation_calls}")
    print(f"translation_cache_hits={summary.translation_cache_hits}")
    print(f"translation_elapsed_sec={summary.translation_elapsed_sec:.2f}")
    print(f"translation_fail_count={summary.translation_fail_count}")
    print(f"elapsed_sec={summary.elapsed_sec:.2f}")


def _run_backfill_with_existing_connection(
    conn,
    translator: Translator,
    *,
    scopes: set[str],
    days: int,
    market: str,
    max_rows: int,
    counters: dict[str, BackfillCounter],
) -> None:
    if "item" in scopes:
        backfill_item_summaries(
            conn,
            translator,
            days=days,
            market=market,
            max_rows=max_rows,
            counter=counters["item"],
        )
    if "evidence" in scopes:
        backfill_evidence_cards(
            conn,
            translator,
            days=days,
            market=market,
            max_rows=max_rows,
            counter=counters["evidence"],
        )
    if "digest" in scopes:
        backfill_daily_digests(
            conn,
            translator,
            days=days,
            market=market,
            max_rows=max_rows,
            counter=counters["digest"],
        )
    if "report" in scopes:
        backfill_agent_reports(
            conn,
            translator,
            days=days,
            market=market,
            max_rows=max_rows,
            counter=counters["report"],
        )
    if "profile" in scopes:
        backfill_stock_profiles(
            conn,
            translator,
            days=days,
            market=market,
            max_rows=max_rows,
            counter=counters["profile"],
        )


def _parse_scopes(raw: str | set[str] | list[str] | tuple[str, ...]) -> set[str]:
    if isinstance(raw, (set, list, tuple)):
        parts = [str(x).strip().lower() for x in raw if str(x).strip()]
    else:
        parts = [x.strip().lower() for x in re.split(r"[,\s;]+", str(raw or "")) if x.strip()]
    if not parts or "all" in parts:
        return set(ALLOWED_SCOPES)
    scopes = set(parts)
    unknown = scopes - ALLOWED_SCOPES
    if unknown:
        raise ValueError(f"Unknown scope(s): {', '.join(sorted(unknown))}")
    return scopes


def backfill_item_summaries(
    conn,
    translator: Translator,
    *,
    days: int,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_item_rows_for_days(conn, days=days, market=market, max_rows=max_rows)
    _log_scope_start(scope="item", total=len(rows), days=days, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="item",
        counter=counter,
        processor=_process_item_summary_row,
    )


def backfill_evidence_cards(
    conn,
    translator: Translator,
    *,
    days: int,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_evidence_rows_for_days(conn, days=days, market=market, max_rows=max_rows)
    _log_scope_start(scope="evidence", total=len(rows), days=days, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="evidence",
        counter=counter,
        processor=_process_evidence_row,
    )


def backfill_daily_digests(
    conn,
    translator: Translator,
    *,
    days: int,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_digest_rows_for_days(conn, days=days, market=market, max_rows=max_rows)
    _log_scope_start(scope="digest", total=len(rows), days=days, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="digest",
        counter=counter,
        processor=_process_digest_row,
    )


def backfill_agent_reports(
    conn,
    translator: Translator,
    *,
    days: int,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_report_rows_for_days(conn, days=days, market=market, max_rows=max_rows)
    _log_scope_start(scope="report", total=len(rows), days=days, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="report",
        counter=counter,
        processor=_process_report_row,
    )


def backfill_stock_profiles(
    conn,
    translator: Translator,
    *,
    days: int,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_profile_rows_for_days(conn, days=days, market=market, max_rows=max_rows)
    _log_scope_start(scope="profile", total=len(rows), days=days, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="profile",
        counter=counter,
        processor=_process_profile_row,
    )


def backfill_item_summaries_since(
    conn,
    translator: Translator,
    *,
    run_started_at: str,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_item_rows_since(conn, run_started_at=run_started_at, market=market, max_rows=max_rows)
    _log_scope_start_since(scope="item", total=len(rows), run_started_at=run_started_at, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="item",
        counter=counter,
        processor=_process_item_summary_row,
    )


def backfill_evidence_cards_since(
    conn,
    translator: Translator,
    *,
    run_started_at: str,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_evidence_rows_since(conn, run_started_at=run_started_at, market=market, max_rows=max_rows)
    _log_scope_start_since(scope="evidence", total=len(rows), run_started_at=run_started_at, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="evidence",
        counter=counter,
        processor=_process_evidence_row,
    )


def backfill_daily_digests_since(
    conn,
    translator: Translator,
    *,
    run_started_at: str,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_digest_rows_since(conn, run_started_at=run_started_at, market=market, max_rows=max_rows)
    _log_scope_start_since(scope="digest", total=len(rows), run_started_at=run_started_at, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="digest",
        counter=counter,
        processor=_process_digest_row,
    )


def backfill_agent_reports_since(
    conn,
    translator: Translator,
    *,
    run_started_at: str,
    market: str,
    max_rows: int,
    counter: BackfillCounter,
) -> None:
    rows = _query_report_rows_since(conn, run_started_at=run_started_at, market=market, max_rows=max_rows)
    _log_scope_start_since(scope="report", total=len(rows), run_started_at=run_started_at, market=market)
    _process_scope_rows(
        conn,
        translator,
        rows=rows,
        scope="report",
        counter=counter,
        processor=_process_report_row,
    )


def _query_item_rows_for_days(conn, *, days: int, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="s.market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT
          i.item_id,
          i.short_summary,
          COALESCE(i.feed_one_liner, '') AS feed_one_liner,
          COALESCE(i.detail_bullets_json, '[]') AS detail_bullets_json
        FROM item_summaries i
        JOIN documents d ON d.id = i.item_id
        JOIN stocks s ON s.code = d.stock_code
        WHERE date(COALESCE(d.published_at, d.collected_at)) >= date('now', ?)
          {market_sql}
        ORDER BY i.item_id DESC
        {limit_sql}
        """,
        (f"-{days} days", *params, *limit_params),
    ).fetchall()


def _query_item_rows_since(conn, *, run_started_at: str, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="s.market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT
          i.item_id,
          i.short_summary,
          COALESCE(i.feed_one_liner, '') AS feed_one_liner,
          COALESCE(i.detail_bullets_json, '[]') AS detail_bullets_json
        FROM item_summaries i
        JOIN documents d ON d.id = i.item_id
        JOIN stocks s ON s.code = d.stock_code
        WHERE datetime(COALESCE(i.updated_at, i.created_at)) >= datetime(?)
          {market_sql}
        ORDER BY i.item_id DESC
        {limit_sql}
        """,
        (run_started_at, *params, *limit_params),
    ).fetchall()


def _query_evidence_rows_for_days(conn, *, days: int, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT
          card_id,
          fact_headline,
          COALESCE(facts_json, '[]') AS facts_json,
          interpretation,
          risk_note
        FROM evidence_cards
        WHERE date(COALESCE(published_at, created_at)) >= date('now', ?)
          {market_sql}
        ORDER BY created_at DESC
        {limit_sql}
        """,
        (f"-{days} days", *params, *limit_params),
    ).fetchall()


def _query_evidence_rows_since(conn, *, run_started_at: str, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT
          card_id,
          fact_headline,
          COALESCE(facts_json, '[]') AS facts_json,
          interpretation,
          risk_note
        FROM evidence_cards
        WHERE datetime(created_at) >= datetime(?)
          {market_sql}
        ORDER BY created_at DESC
        {limit_sql}
        """,
        (run_started_at, *params, *limit_params),
    ).fetchall()


def _query_digest_rows_for_days(conn, *, days: int, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT id, summary_8line, change_3, open_questions
        FROM daily_digests
        WHERE date(digest_date) >= date('now', ?)
          {market_sql}
        ORDER BY digest_date DESC, id DESC
        {limit_sql}
        """,
        (f"-{days} days", *params, *limit_params),
    ).fetchall()


def _query_digest_rows_since(conn, *, run_started_at: str, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT id, summary_8line, change_3, open_questions
        FROM daily_digests
        WHERE datetime(COALESCE(updated_at, created_at)) >= datetime(?)
          {market_sql}
        ORDER BY digest_date DESC, id DESC
        {limit_sql}
        """,
        (run_started_at, *params, *limit_params),
    ).fetchall()


def _query_report_rows_for_days(conn, *, days: int, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT id, report_md
        FROM agent_reports
        WHERE date(period_end) >= date('now', ?)
          {market_sql}
        ORDER BY period_end DESC, id DESC
        {limit_sql}
        """,
        (f"-{days} days", *params, *limit_params),
    ).fetchall()


def _query_report_rows_since(conn, *, run_started_at: str, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT id, report_md
        FROM agent_reports
        WHERE datetime(created_at) >= datetime(?)
          {market_sql}
        ORDER BY period_end DESC, id DESC
        {limit_sql}
        """,
        (run_started_at, *params, *limit_params),
    ).fetchall()


def _query_profile_rows_for_days(conn, *, days: int, market: str, max_rows: int):
    market_sql, params = _market_filter_sql(alias="market", market=market)
    limit_sql, limit_params = _limit_sql_params(max_rows)
    return conn.execute(
        f"""
        SELECT stock_code, description_ko
        FROM stock_profiles
        WHERE date(updated_at) >= date('now', ?)
          {market_sql}
        ORDER BY updated_at DESC, stock_code
        {limit_sql}
        """,
        (f"-{days} days", *params, *limit_params),
    ).fetchall()


def _process_item_summary_row(conn, translator: Translator, row) -> bool:
    detail_bullets = _safe_json_list(row["detail_bullets_json"])
    translated = translator.translate_structured_to_ko(
        conn,
        {
            "short_summary": str(row["short_summary"] or ""),
            "feed_one_liner": str(row["feed_one_liner"] or ""),
            "detail_bullets": detail_bullets,
        },
        purpose="bf_item_bundle",
    )
    short_summary = str(translated.get("short_summary") or "")
    feed_one = str(translated.get("feed_one_liner") or "")
    detail_ko = [str(x) for x in list(translated.get("detail_bullets") or [])]
    if (
        short_summary == str(row["short_summary"] or "")
        and feed_one == str(row["feed_one_liner"] or "")
        and detail_ko == detail_bullets
    ):
        return False
    conn.execute(
        """
        UPDATE item_summaries
        SET short_summary = ?,
            feed_one_liner = ?,
            detail_bullets_json = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE item_id = ?
        """,
        (short_summary, feed_one, json.dumps(detail_ko, ensure_ascii=False), int(row["item_id"])),
    )
    return True


def _process_evidence_row(conn, translator: Translator, row) -> bool:
    facts = _safe_json_list(row["facts_json"])
    translated = translator.translate_structured_to_ko(
        conn,
        {
            "fact_headline": str(row["fact_headline"] or ""),
            "facts": facts,
            "interpretation": str(row["interpretation"] or ""),
            "risk_note": str(row["risk_note"] or ""),
        },
        purpose="bf_evidence_bundle",
    )
    fact_headline = str(translated.get("fact_headline") or "")
    facts_ko = [str(x) for x in list(translated.get("facts") or [])]
    interpretation = str(translated.get("interpretation") or "")
    risk_note = str(translated.get("risk_note") or "")
    if (
        fact_headline == str(row["fact_headline"] or "")
        and facts_ko == facts
        and interpretation == str(row["interpretation"] or "")
        and risk_note == str(row["risk_note"] or "")
    ):
        return False
    conn.execute(
        """
        UPDATE evidence_cards
        SET fact_headline = ?,
            facts_json = ?,
            interpretation = ?,
            risk_note = ?
        WHERE card_id = ?
        """,
        (fact_headline, json.dumps(facts_ko, ensure_ascii=False), interpretation, risk_note, str(row["card_id"])),
    )
    return True


def _process_digest_row(conn, translator: Translator, row) -> bool:
    translated = translator.translate_structured_to_ko(
        conn,
        {
            "summary_8line": str(row["summary_8line"] or ""),
            "change_3": str(row["change_3"] or ""),
            "open_questions": str(row["open_questions"] or ""),
        },
        purpose="bf_digest_bundle",
    )
    summary = str(translated.get("summary_8line") or "")
    change = str(translated.get("change_3") or "")
    questions = str(translated.get("open_questions") or "")
    if (
        summary == str(row["summary_8line"] or "")
        and change == str(row["change_3"] or "")
        and questions == str(row["open_questions"] or "")
    ):
        return False
    conn.execute(
        """
        UPDATE daily_digests
        SET summary_8line = ?,
            change_3 = ?,
            open_questions = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (summary, change, questions, int(row["id"])),
    )
    return True


def _process_report_row(conn, translator: Translator, row) -> bool:
    report_md = translator.translate_markdown_to_ko(conn, str(row["report_md"] or ""), purpose="bf_agent_report")
    if report_md == str(row["report_md"] or ""):
        return False
    conn.execute(
        """
        UPDATE agent_reports
        SET report_md = ?
        WHERE id = ?
        """,
        (report_md, int(row["id"])),
    )
    return True


def _process_profile_row(conn, translator: Translator, row) -> bool:
    description = translator.translate_text_to_ko(conn, str(row["description_ko"] or ""), purpose="bf_stock_profile")
    if description == str(row["description_ko"] or ""):
        return False
    conn.execute(
        """
        UPDATE stock_profiles
        SET description_ko = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE stock_code = ?
        """,
        (description, str(row["stock_code"])),
    )
    return True


def _process_scope_rows(
    conn,
    translator: Translator,
    *,
    rows,
    scope: str,
    counter: BackfillCounter,
    processor: Callable[[object, Translator, object], bool],
) -> None:
    started = time.perf_counter()
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        counter.scanned += 1
        try:
            wrote = bool(processor(conn, translator, row))
            if wrote:
                counter.written += 1
            else:
                counter.skipped += 1
        except Exception:
            counter.errors += 1
        _maybe_log_scope_progress(scope=scope, idx=idx, total=total, counter=counter, started=started)


def _limit_sql_params(max_rows: int) -> tuple[str, tuple[object, ...]]:
    if max_rows > 0:
        return "LIMIT ?", (max_rows,)
    return "", ()


def _market_filter_sql(*, alias: str, market: str) -> tuple[str, tuple[str, ...]]:
    if not market:
        return "", ()
    return f"AND lower({alias}) = lower(?)", (market,)


def _safe_json_list(raw: object) -> list[str]:
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if not isinstance(raw, str):
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(x) for x in parsed]


def _log_scope_start(*, scope: str, total: int, days: int, market: str) -> None:
    market_label = market.upper() if market else "ALL"
    print(f"[PROGRESS] translate_backfill scope={scope} start total_rows={total} days={days} market={market_label}")


def _log_scope_start_since(*, scope: str, total: int, run_started_at: str, market: str) -> None:
    market_label = market.upper() if market else "ALL"
    print(
        "[PROGRESS] translate_backfill scope="
        f"{scope} start total_rows={total} run_started_at={run_started_at} market={market_label}"
    )


def _maybe_log_scope_progress(
    *,
    scope: str,
    idx: int,
    total: int,
    counter: BackfillCounter,
    started: float,
) -> None:
    if total <= 0:
        return
    if not (idx == 1 or idx == total or idx % PROGRESS_EVERY == 0):
        return
    elapsed = time.perf_counter() - started
    rows_per_sec = idx / elapsed if elapsed > 0 else 0.0
    metrics = get_translation_metrics()
    print(
        f"[PROGRESS] translate_backfill scope={scope} {idx}/{total} "
        f"written={counter.written} skipped={counter.skipped} errors={counter.errors} "
        f"translation_calls={metrics.calls} cache_hits={metrics.cache_hits} "
        f"elapsed_sec={elapsed:.1f} rows_per_sec={rows_per_sec:.2f}"
    )
