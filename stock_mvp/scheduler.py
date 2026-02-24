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
            include_sector_steps = (hour, minute) == designated_sector_time
            scheduler.add_job(
                lambda include_sector=include_sector_steps: _run_collect_job(
                    pipeline,
                    include_sector_steps=include_sector,
                ),
                trigger=CronTrigger(hour=hour, minute=minute, timezone="Asia/Seoul"),
                id=f"collect_{hour:02d}{minute:02d}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
    else:
        scheduler.add_job(
            lambda: _run_collect_job(pipeline, include_sector_steps=False),
            trigger="interval",
            minutes=max(settings.collect_interval_min, 15),
            id="collect_interval_fallback",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        if sector_time:
            scheduler.add_job(
                lambda: _run_collect_job(pipeline, include_sector_steps=True),
                trigger=CronTrigger(hour=sector_time[0], minute=sector_time[1], timezone="Asia/Seoul"),
                id="collect_sector_daily",
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

    scheduler.start()
    return scheduler


def _run_collect_job(pipeline: CollectionPipeline, include_sector_steps: bool) -> None:
    try:
        pipeline.run_once(trigger_type="scheduled_collect", include_sector_steps=include_sector_steps)
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
