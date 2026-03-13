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
    enable_morning_brief_schedule: bool
    enable_kr_disclosure_schedule: bool
    kr_disclosure_schedule_months: str
    kr_disclosure_day_of_month: int
    kr_disclosure_time_kst: str
    universe_refresh_day_of_month: int
    universe_refresh_time_kst: str
    crawler_max_retries: int
    ops_error_alert_threshold: int
    enable_telegram_error_alert: bool
    request_timeout_sec: int
    crawler_trust_env: bool
    verify_ssl: bool
    ca_bundle_path: str
    enable_pdf_ocr_fallback: bool
    pdf_ocr_max_pages: int
    pdf_ocr_lang: str
    tesseract_cmd: str
    news_per_stock: int
    reports_per_stock: int
    naver_news_per_stock: int
    naver_finance_reports_per_stock: int
    naver_industry_reports_per_run: int
    sec_reports_per_stock: int
    opendart_max_per_stock: int
    opendart_lookback_days: int
    opendart_core_keywords: str
    opendart_corp_code_refresh_days: int
    summary_lookback_days: int
    collect_store_all_docs: bool
    summary_top_n_per_stock: int
    summary_min_relevance: float
    enable_kr_rss_ingest: bool
    kr_rss_feed_urls_json: str
    kr_rss_max_items_per_source: int
    kr_rss_ticker_threshold: float
    kr_rss_sector_threshold: float
    kr_rss_max_tickers_per_item: int
    krx_master_service_key: str
    krx_master_refresh_days: int
    general_economy_min_score: float
    general_economy_keywords: str
    summary_max_items_per_run: int
    digest_max_entities_per_run: int
    report_max_entities_per_run: int
    naver_client_id: str
    naver_client_secret: str
    telegram_bot_token: str
    telegram_chat_id: str
    sec_user_agent: str
    opendart_api_key: str
    llm_provider: str
    llm_model: str
    llm_api_base: str
    llm_api_key: str
    llm_temperature: float
    llm_max_tokens: int
    llm_request_timeout_sec: int
    llm_trust_env: bool
    llm_daily_budget_usd: float
    llm_job_budget_usd: float
    llm_soft_budget_ratio: float
    llm_budget_model: str
    llm_budget_flush_every_calls: int
    llm_hard_max_input_chars: int
    llm_cost_input_per_1k_usd: float
    llm_cost_output_per_1k_usd: float
    translation_enabled: bool
    translation_provider: str
    translation_model: str
    translation_api_key: str
    translation_max_retries: int
    translation_timeout_sec: int
    enable_financial_collection: bool
    financial_refresh_min_hours: int
    enable_price_collection: bool
    price_collect_kr_time_kst: str
    price_collect_us_time_kst: str
    price_lookback_days: int


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
        enable_morning_brief_schedule=_parse_bool(os.getenv("ENABLE_MORNING_BRIEF_SCHEDULE", "false")),
        enable_kr_disclosure_schedule=_parse_bool(os.getenv("ENABLE_KR_DISCLOSURE_SCHEDULE", "true"), default=True),
        kr_disclosure_schedule_months=os.getenv("KR_DISCLOSURE_SCHEDULE_MONTHS", "1,4,7,10").strip(),
        kr_disclosure_day_of_month=int(os.getenv("KR_DISCLOSURE_DAY_OF_MONTH", "2")),
        kr_disclosure_time_kst=os.getenv("KR_DISCLOSURE_TIME_KST", "06:10").strip(),
        universe_refresh_day_of_month=int(os.getenv("UNIVERSE_REFRESH_DAY_OF_MONTH", "1")),
        universe_refresh_time_kst=os.getenv("UNIVERSE_REFRESH_TIME_KST", "05:30").strip(),
        crawler_max_retries=int(os.getenv("CRAWLER_MAX_RETRIES", "1")),
        ops_error_alert_threshold=int(os.getenv("OPS_ERROR_ALERT_THRESHOLD", "5")),
        enable_telegram_error_alert=_parse_bool(os.getenv("ENABLE_TELEGRAM_ERROR_ALERT", "false")),
        request_timeout_sec=int(os.getenv("REQUEST_TIMEOUT_SEC", "10")),
        crawler_trust_env=_parse_bool(os.getenv("CRAWLER_TRUST_ENV", "false")),
        verify_ssl=_parse_bool(os.getenv("VERIFY_SSL", "true"), default=True),
        ca_bundle_path=os.getenv("CA_BUNDLE_PATH", "").strip(),
        enable_pdf_ocr_fallback=_parse_bool(os.getenv("ENABLE_PDF_OCR_FALLBACK", "false")),
        pdf_ocr_max_pages=int(os.getenv("PDF_OCR_MAX_PAGES", "4")),
        pdf_ocr_lang=os.getenv("PDF_OCR_LANG", "kor+eng").strip(),
        tesseract_cmd=os.getenv("TESSERACT_CMD", "").strip(),
        news_per_stock=int(os.getenv("NEWS_PER_STOCK", "20")),
        reports_per_stock=int(os.getenv("REPORTS_PER_STOCK", "10")),
        naver_news_per_stock=int(os.getenv("NAVER_NEWS_PER_STOCK", os.getenv("NEWS_PER_STOCK", "20"))),
        naver_finance_reports_per_stock=int(
            os.getenv("NAVER_FINANCE_REPORTS_PER_STOCK", os.getenv("REPORTS_PER_STOCK", "10"))
        ),
        naver_industry_reports_per_run=int(os.getenv("NAVER_INDUSTRY_REPORTS_PER_RUN", "60")),
        sec_reports_per_stock=int(os.getenv("SEC_REPORTS_PER_STOCK", os.getenv("REPORTS_PER_STOCK", "10"))),
        opendart_max_per_stock=int(os.getenv("OPENDART_MAX_PER_STOCK", "12")),
        opendart_lookback_days=int(os.getenv("OPENDART_LOOKBACK_DAYS", "120")),
        opendart_core_keywords=os.getenv(
            "OPENDART_CORE_KEYWORDS",
            "사업보고서,반기보고서,분기보고서,주요사항보고서,유상증자결정,무상증자결정,전환사채권발행결정,"
            "신주인수권부사채권발행결정,단일판매·공급계약체결,타법인주식및출자증권취득결정",
        ).strip(),
        opendart_corp_code_refresh_days=int(os.getenv("OPENDART_CORP_CODE_REFRESH_DAYS", "30")),
        summary_lookback_days=int(os.getenv("SUMMARY_LOOKBACK_DAYS", "7")),
        collect_store_all_docs=_parse_bool(os.getenv("COLLECT_STORE_ALL_DOCS", "true"), default=True),
        summary_top_n_per_stock=int(os.getenv("SUMMARY_TOP_N_PER_STOCK", "10")),
        summary_min_relevance=float(os.getenv("SUMMARY_MIN_RELEVANCE", "0")),
        enable_kr_rss_ingest=_parse_bool(os.getenv("ENABLE_KR_RSS_INGEST", "true"), default=True),
        kr_rss_feed_urls_json=os.getenv("KR_RSS_FEED_URLS_JSON", "[]").strip(),
        kr_rss_max_items_per_source=int(os.getenv("KR_RSS_MAX_ITEMS_PER_SOURCE", "200")),
        kr_rss_ticker_threshold=float(os.getenv("KR_RSS_TICKER_THRESHOLD", "8")),
        kr_rss_sector_threshold=float(os.getenv("KR_RSS_SECTOR_THRESHOLD", "7")),
        kr_rss_max_tickers_per_item=int(os.getenv("KR_RSS_MAX_TICKERS_PER_ITEM", "3")),
        krx_master_service_key=os.getenv("KRX_MASTER_SERVICE_KEY", "").strip(),
        krx_master_refresh_days=int(os.getenv("KRX_MASTER_REFRESH_DAYS", "1")),
        general_economy_min_score=float(os.getenv("GENERAL_ECONOMY_MIN_SCORE", "7")),
        general_economy_keywords=os.getenv(
            "GENERAL_ECONOMY_KEYWORDS",
            "환율,금리,물가,고용,경기,수출,수입,통화정책,재정정책,한은,FOMC,CPI,PPI,GDP",
        ).strip(),
        summary_max_items_per_run=int(os.getenv("SUMMARY_MAX_ITEMS_PER_RUN", "400")),
        digest_max_entities_per_run=int(os.getenv("DIGEST_MAX_ENTITIES_PER_RUN", "80")),
        report_max_entities_per_run=int(os.getenv("REPORT_MAX_ENTITIES_PER_RUN", "40")),
        naver_client_id=os.getenv("NAVER_CLIENT_ID", "").strip(),
        naver_client_secret=os.getenv("NAVER_CLIENT_SECRET", "").strip(),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        sec_user_agent=os.getenv("SEC_USER_AGENT", "stock-mvp/0.1 (contact: local-user)").strip(),
        opendart_api_key=os.getenv("OPENDART_API_KEY", "").strip(),
        llm_provider=os.getenv("LLM_PROVIDER", "none").strip().lower(),
        llm_model=os.getenv("LLM_MODEL", "gpt-4o-mini").strip(),
        llm_api_base=os.getenv("LLM_API_BASE", "").strip(),
        llm_api_key=_resolve_llm_api_key(),
        llm_temperature=float(os.getenv("LLM_TEMPERATURE", "0.2")),
        llm_max_tokens=int(os.getenv("LLM_MAX_TOKENS", "900")),
        llm_request_timeout_sec=int(os.getenv("LLM_REQUEST_TIMEOUT_SEC", "30")),
        llm_trust_env=_parse_bool(os.getenv("LLM_TRUST_ENV", "false")),
        llm_daily_budget_usd=float(os.getenv("LLM_DAILY_BUDGET_USD", "0")),
        llm_job_budget_usd=float(os.getenv("LLM_JOB_BUDGET_USD", "0")),
        llm_soft_budget_ratio=float(os.getenv("LLM_SOFT_BUDGET_RATIO", "0.8")),
        llm_budget_model=os.getenv("LLM_BUDGET_MODEL", "").strip(),
        llm_budget_flush_every_calls=int(os.getenv("LLM_BUDGET_FLUSH_EVERY_CALLS", "20")),
        llm_hard_max_input_chars=int(os.getenv("LLM_HARD_MAX_INPUT_CHARS", "12000")),
        llm_cost_input_per_1k_usd=float(os.getenv("LLM_COST_INPUT_PER_1K_USD", "0.0004")),
        llm_cost_output_per_1k_usd=float(os.getenv("LLM_COST_OUTPUT_PER_1K_USD", "0.0008")),
        translation_enabled=_parse_bool(os.getenv("TRANSLATION_ENABLED", "true"), default=True),
        translation_provider=os.getenv("TRANSLATION_PROVIDER", "openai").strip().lower(),
        translation_model=os.getenv("TRANSLATION_MODEL", "gpt-4o-mini").strip(),
        translation_api_key=_resolve_translation_api_key(),
        translation_max_retries=int(os.getenv("TRANSLATION_MAX_RETRIES", "2")),
        translation_timeout_sec=int(os.getenv("TRANSLATION_TIMEOUT_SEC", "20")),
        enable_financial_collection=_parse_bool(os.getenv("ENABLE_FINANCIAL_COLLECTION", "true"), default=True),
        financial_refresh_min_hours=int(os.getenv("FINANCIAL_REFRESH_MIN_HOURS", "20")),
        enable_price_collection=_parse_bool(os.getenv("ENABLE_PRICE_COLLECTION", "true"), default=True),
        price_collect_kr_time_kst=os.getenv("PRICE_COLLECT_KR_TIME_KST", "16:40").strip(),
        price_collect_us_time_kst=os.getenv("PRICE_COLLECT_US_TIME_KST", "07:10").strip(),
        price_lookback_days=int(os.getenv("PRICE_LOOKBACK_DAYS", "400")),
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


def _resolve_translation_api_key() -> str:
    direct = os.getenv("TRANSLATION_API_KEY", "").strip()
    if direct:
        return direct
    provider = os.getenv("TRANSLATION_PROVIDER", "openai").strip().lower()
    if provider == "openai":
        return os.getenv("OPENAI_API_KEY", "").strip() or os.getenv("LLM_API_KEY", "").strip()
    if provider == "gemini":
        return os.getenv("GEMINI_API_KEY", "").strip()
    if provider == "openrouter":
        return os.getenv("OPENROUTER_API_KEY", "").strip()
    return ""
