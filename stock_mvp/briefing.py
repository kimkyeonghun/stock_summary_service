from __future__ import annotations

from dataclasses import dataclass
import os

import requests

from stock_mvp.config import Settings
from stock_mvp.database import connect, latest_summary_highlights


@dataclass(frozen=True)
class BriefResult:
    sent: bool
    message: str
    item_count: int


def build_morning_brief(settings: Settings, limit: int = 12) -> str:
    with connect(settings.db_path) as conn:
        rows = latest_summary_highlights(conn, limit=limit)

    if not rows:
        return "Morning Brief\n\nNo summaries available yet."

    lines: list[str] = ["Morning Brief", ""]
    for row in rows:
        market = row["market"]
        code = row["stock_code"]
        name = row["stock_name"]
        line1 = row["line1"]
        lines.append(f"- [{market}] {name} ({code})")
        lines.append(f"  {line1}")
    return "\n".join(lines)


def send_telegram_message(settings: Settings, text: str) -> BriefResult:
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return BriefResult(
            sent=False,
            message="Telegram credentials are missing. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.",
            item_count=0,
        )

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl

    chunks = _split_message_chunks(text, max_len=180)
    for idx, chunk in enumerate(chunks, start=1):
        try:
            resp = _post_telegram(
                url=url,
                payload={
                    "chat_id": settings.telegram_chat_id,
                    "text": chunk,
                    "disable_web_page_preview": "true",
                },
                timeout=settings.request_timeout_sec,
                verify=verify,
            )
        except requests.RequestException as exc:
            return BriefResult(sent=False, message=f"Telegram request failed at chunk {idx}: {exc}", item_count=0)

        if not resp.ok:
            return BriefResult(
                sent=False,
                message=f"Telegram send failed at chunk {idx}: {resp.status_code} {resp.text[:120]}",
                item_count=0,
            )
    return BriefResult(sent=True, message="Telegram send success", item_count=text.count("\n- ["))


def send_morning_brief(settings: Settings, limit: int = 12) -> BriefResult:
    text = build_morning_brief(settings=settings, limit=limit)
    return send_telegram_message(settings=settings, text=text)


def _post_telegram(url: str, payload: dict, timeout: int, verify: bool | str):
    session = requests.Session()
    session.trust_env = False if _has_proxy_env() else True

    try:
        # Prefer POST first.
        return session.post(url, data=payload, timeout=timeout, verify=verify)
    except requests.RequestException:
        # In some networks POST to Telegram is blocked while GET still works.
        return session.get(url, params=payload, timeout=timeout, verify=verify)


def _has_proxy_env() -> bool:
    keys = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")
    return any((os.getenv(k, "") or "").strip() for k in keys)


def _split_message_chunks(text: str, max_len: int = 180) -> list[str]:
    src = (text or "").strip()
    if not src:
        return [""]

    lines = src.splitlines()
    chunks: list[str] = []
    current = ""
    for line in lines:
        candidate = line if not current else f"{current}\n{line}"
        if len(candidate) <= max_len:
            current = candidate
        else:
            if current:
                chunks.append(current)
                current = line[:max_len]
            else:
                chunks.append(line[:max_len])
                current = ""
    if current:
        chunks.append(current)
    return chunks
