from __future__ import annotations

import re
from typing import Any, Callable

from stock_mvp.config import Settings
from stock_mvp.pipeline import CollectionPipeline, PipelineBusyError


def start_scheduler(
    pipeline: CollectionPipeline,
    settings: Settings,
    morning_brief_job: Callable[[], None] | None = None,
    universe_refresh_job: Callable[[], None] | None = None,
    price_collect_kr_job: Callable[[], None] | None = None,
    price_collect_us_job: Callable[[], None] | None = None,
) -> Any:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = BackgroundScheduler(timezone="Asia/Seoul")

    collect_times = parse_hhmm_schedule(settings.collect_schedule_kst)
    sector_time = parse_hhmm(settings.sector_refresh_time_kst)
    if collect_times:
        designated_sector_time = sector_time if sector_time in collect_times else collect_times[0]
        if sector_time and sector_time != designated_sector_time:
            print(
                "[WARN] sector refresh time is not in collect schedule, "
                f"falling back to first collect slot={designated_sector_time[0]:02d}:{designated_sector_time[1]:02d}"
            )
        for hour, minute in collect_times:
            include_full_run = (hour, minute) == designated_sector_time
            scheduler.add_job(
                lambda include_full=include_full_run: _run_collect_job(
                    pipeline,
                    include_agent_steps=include_full,
                    include_sector_steps=include_full,
                    collect_news=True,
                    collect_reports=include_full,
                    collect_filings=False,
                    collect_financials=True,
                ),
                trigger=CronTrigger(hour=hour, minute=minute, timezone="Asia/Seoul"),
                id=f"collect_{hour:02d}{minute:02d}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
    else:
        scheduler.add_job(
            lambda: _run_collect_job(
                pipeline,
                include_agent_steps=False,
                include_sector_steps=False,
                collect_news=True,
                collect_reports=False,
                collect_filings=False,
                collect_financials=True,
            ),
            trigger="interval",
            minutes=max(settings.collect_interval_min, 15),
            id="collect_interval_fallback",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        if sector_time:
            scheduler.add_job(
                lambda: _run_collect_job(
                    pipeline,
                    include_agent_steps=True,
                    include_sector_steps=True,
                    collect_news=True,
                    collect_reports=True,
                    collect_filings=False,
                    collect_financials=True,
                ),
                trigger=CronTrigger(hour=sector_time[0], minute=sector_time[1], timezone="Asia/Seoul"),
                id="collect_sector_daily",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )

    disclosure_time = parse_hhmm(settings.kr_disclosure_time_kst)
    disclosure_months = parse_month_schedule(settings.kr_disclosure_schedule_months)
    if (
        settings.enable_kr_disclosure_schedule
        and disclosure_time
        and disclosure_months
    ):
        disclosure_day = min(max(int(settings.kr_disclosure_day_of_month), 1), 28)
        month_expr = ",".join(str(m) for m in disclosure_months)
        scheduler.add_job(
            lambda: _run_collect_job(
                pipeline,
                market="KR",
                include_agent_steps=False,
                include_sector_steps=False,
                collect_news=False,
                collect_reports=False,
                collect_filings=True,
                collect_financials=False,
            ),
            trigger=CronTrigger(
                month=month_expr,
                day=disclosure_day,
                hour=disclosure_time[0],
                minute=disclosure_time[1],
                timezone="Asia/Seoul",
            ),
            id="collect_kr_disclosure_quarterly",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    morning_time = parse_hhmm(settings.morning_brief_time_kst)
    if morning_brief_job and morning_time:
        scheduler.add_job(
            morning_brief_job,
            trigger=CronTrigger(hour=morning_time[0], minute=morning_time[1], timezone="Asia/Seoul"),
            id="morning_brief",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    refresh_time = parse_hhmm(settings.universe_refresh_time_kst)
    if universe_refresh_job and refresh_time:
        day = min(max(settings.universe_refresh_day_of_month, 1), 28)
        scheduler.add_job(
            universe_refresh_job,
            trigger=CronTrigger(
                day=day,
                hour=refresh_time[0],
                minute=refresh_time[1],
                timezone="Asia/Seoul",
            ),
            id="universe_refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    if settings.enable_price_collection:
        kr_time = parse_hhmm(settings.price_collect_kr_time_kst)
        us_time = parse_hhmm(settings.price_collect_us_time_kst)
        if kr_time and us_time and kr_time == us_time:
            us_time = _add_minutes(us_time, 1)
            print(
                "[WARN] KR/US price collect times matched; "
                f"US price collect time shifted to {us_time[0]:02d}:{us_time[1]:02d}"
            )
        if price_collect_kr_job and kr_time:
            scheduler.add_job(
                price_collect_kr_job,
                trigger=CronTrigger(hour=kr_time[0], minute=kr_time[1], timezone="Asia/Seoul"),
                id="price_collect_kr_daily",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        if price_collect_us_job and us_time:
            scheduler.add_job(
                price_collect_us_job,
                trigger=CronTrigger(hour=us_time[0], minute=us_time[1], timezone="Asia/Seoul"),
                id="price_collect_us_daily",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )

    scheduler.start()
    return scheduler


def _run_collect_job(
    pipeline: CollectionPipeline,
    *,
    market: str | None = None,
    include_agent_steps: bool,
    include_sector_steps: bool,
    collect_news: bool,
    collect_reports: bool,
    collect_filings: bool,
    collect_financials: bool,
) -> None:
    try:
        pipeline.run_once(
            market=market,
            trigger_type="scheduled_collect",
            include_agent_steps=include_agent_steps,
            include_sector_steps=include_sector_steps,
            collect_news=collect_news,
            collect_reports=collect_reports,
            collect_filings=collect_filings,
            collect_financials=collect_financials,
        )
    except PipelineBusyError as exc:
        print(f"[INFO] scheduled collect skipped: {exc}")


def parse_hhmm_schedule(value: str) -> list[tuple[int, int]]:
    parts = [p.strip() for p in (value or "").split(",") if p.strip()]
    times: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for part in parts:
        parsed = parse_hhmm(part)
        if parsed is None:
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        times.append(parsed)
    return sorted(times)


def parse_hhmm(value: str) -> tuple[int, int] | None:
    if not value:
        return None
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", value.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def parse_month_schedule(value: str) -> list[int]:
    parts = [p.strip() for p in (value or "").split(",") if p.strip()]
    months: list[int] = []
    seen: set[int] = set()
    for part in parts:
        if not part.isdigit():
            continue
        num = int(part)
        if num < 1 or num > 12:
            continue
        if num in seen:
            continue
        seen.add(num)
        months.append(num)
    return sorted(months)


def _add_minutes(value: tuple[int, int], minutes: int) -> tuple[int, int]:
    base_total = value[0] * 60 + value[1]
    normalized = (base_total + minutes) % (24 * 60)
    return normalized // 60, normalized % 60
