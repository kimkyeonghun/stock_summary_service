from __future__ import annotations

import re
from typing import Any, Callable

from stock_mvp.config import Settings
from stock_mvp.pipeline import CollectionPipeline


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
    if collect_times:
        for hour, minute in collect_times:
            scheduler.add_job(
                lambda: pipeline.run_once(trigger_type="scheduled_collect"),
                trigger=CronTrigger(hour=hour, minute=minute, timezone="Asia/Seoul"),
                id=f"collect_{hour:02d}{minute:02d}",
                replace_existing=True,
            )
    else:
        scheduler.add_job(
            lambda: pipeline.run_once(trigger_type="scheduled_collect"),
            trigger="interval",
            minutes=max(settings.collect_interval_min, 15),
            id="collect_interval_fallback",
            replace_existing=True,
        )

    morning_time = parse_hhmm(settings.morning_brief_time_kst)
    if morning_brief_job and morning_time:
        scheduler.add_job(
            morning_brief_job,
            trigger=CronTrigger(hour=morning_time[0], minute=morning_time[1], timezone="Asia/Seoul"),
            id="morning_brief",
            replace_existing=True,
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
        )

    scheduler.start()
    return scheduler


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
