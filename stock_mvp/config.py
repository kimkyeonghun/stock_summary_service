from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    app_env: str
    db_path: Path
    collect_interval_min: int
    enable_scheduler: bool
    collect_schedule_kst: str
    sector_refresh_time_kst: str
    morning_brief_time_kst: str
    universe_refresh_day_of_month: int
    universe_refresh_time_kst: str
    crawler_max_retries: int
    ops_error_alert_threshold: int
    enable_telegram_error_alert: bool
    request_timeout_sec: int
    verify_ssl: bool
    ca_bundle_path: str
    news_per_stock: int
    reports_per_stock: int
    naver_news_per_stock: int
    naver_finance_reports_per_stock: int
    sec_reports_per_stock: int
    summary_lookback_days: int
    naver_client_id: str
    naver_client_secret: str
    telegram_bot_token: str
    telegram_chat_id: str
    sec_user_agent: str
    llm_provider: str
    llm_model: str
    llm_api_base: str
    llm_api_key: str
    llm_temperature: float
    llm_max_tokens: int
    llm_request_timeout_sec: int
    llm_trust_env: bool
    enable_financial_collection: bool
    financial_refresh_min_hours: int


def _parse_bool(value: str, default: bool = False) -> bool:
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def load_settings() -> Settings:
    load_dotenv()
    db_path = Path(os.getenv("DB_PATH", "data/stock_mvp.db")).resolve()
    return Settings(
        app_env=os.getenv("APP_ENV", "dev"),
        db_path=db_path,
        collect_interval_min=int(os.getenv("COLLECT_INTERVAL_MIN", "60")),
        enable_scheduler=_parse_bool(os.getenv("ENABLE_SCHEDULER", "false")),
        collect_schedule_kst=os.getenv("COLLECT_SCHEDULE_KST", "00:00,06:00,12:00,18:00").strip(),
        sector_refresh_time_kst=os.getenv("SECTOR_REFRESH_TIME_KST", "00:00").strip(),
        morning_brief_time_kst=os.getenv("MORNING_BRIEF_TIME_KST", "07:00").strip(),
        universe_refresh_day_of_month=int(os.getenv("UNIVERSE_REFRESH_DAY_OF_MONTH", "1")),
        universe_refresh_time_kst=os.getenv("UNIVERSE_REFRESH_TIME_KST", "05:30").strip(),
        crawler_max_retries=int(os.getenv("CRAWLER_MAX_RETRIES", "1")),
        ops_error_alert_threshold=int(os.getenv("OPS_ERROR_ALERT_THRESHOLD", "5")),
        enable_telegram_error_alert=_parse_bool(os.getenv("ENABLE_TELEGRAM_ERROR_ALERT", "false")),
        request_timeout_sec=int(os.getenv("REQUEST_TIMEOUT_SEC", "10")),
        verify_ssl=_parse_bool(os.getenv("VERIFY_SSL", "true"), default=True),
        ca_bundle_path=os.getenv("CA_BUNDLE_PATH", "").strip(),
        news_per_stock=int(os.getenv("NEWS_PER_STOCK", "20")),
        reports_per_stock=int(os.getenv("REPORTS_PER_STOCK", "10")),
        naver_news_per_stock=int(os.getenv("NAVER_NEWS_PER_STOCK", os.getenv("NEWS_PER_STOCK", "20"))),
        naver_finance_reports_per_stock=int(
            os.getenv("NAVER_FINANCE_REPORTS_PER_STOCK", os.getenv("REPORTS_PER_STOCK", "10"))
        ),
        sec_reports_per_stock=int(os.getenv("SEC_REPORTS_PER_STOCK", os.getenv("REPORTS_PER_STOCK", "10"))),
        summary_lookback_days=int(os.getenv("SUMMARY_LOOKBACK_DAYS", "7")),
        naver_client_id=os.getenv("NAVER_CLIENT_ID", "").strip(),
        naver_client_secret=os.getenv("NAVER_CLIENT_SECRET", "").strip(),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        sec_user_agent=os.getenv("SEC_USER_AGENT", "stock-mvp/0.1 (contact: local-user)").strip(),
        llm_provider=os.getenv("LLM_PROVIDER", "none").strip().lower(),
        llm_model=os.getenv("LLM_MODEL", "gpt-4o-mini").strip(),
        llm_api_base=os.getenv("LLM_API_BASE", "").strip(),
        llm_api_key=_resolve_llm_api_key(),
        llm_temperature=float(os.getenv("LLM_TEMPERATURE", "0.2")),
        llm_max_tokens=int(os.getenv("LLM_MAX_TOKENS", "900")),
        llm_request_timeout_sec=int(os.getenv("LLM_REQUEST_TIMEOUT_SEC", "30")),
        llm_trust_env=_parse_bool(os.getenv("LLM_TRUST_ENV", "false")),
        enable_financial_collection=_parse_bool(os.getenv("ENABLE_FINANCIAL_COLLECTION", "true"), default=True),
        financial_refresh_min_hours=int(os.getenv("FINANCIAL_REFRESH_MIN_HOURS", "20")),
    )


def _resolve_llm_api_key() -> str:
    direct = os.getenv("LLM_API_KEY", "").strip()
    if direct:
        return direct
    provider = os.getenv("LLM_PROVIDER", "none").strip().lower()
    if provider == "gemini":
        return os.getenv("GEMINI_API_KEY", "").strip()
    if provider == "openai":
        return os.getenv("OPENAI_API_KEY", "").strip()
    if provider == "openrouter":
        return os.getenv("OPENROUTER_API_KEY", "").strip()
    return ""
