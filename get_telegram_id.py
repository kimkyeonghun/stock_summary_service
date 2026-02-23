from __future__ import annotations

import argparse
import os
from typing import Any

import requests
from dotenv import load_dotenv


def _extract_chat_info(update: dict[str, Any]) -> dict[str, Any] | None:
    msg = (
        update.get("message")
        or update.get("channel_post")
        or update.get("edited_message")
        or update.get("edited_channel_post")
    )

    if not msg and update.get("callback_query"):
        msg = update["callback_query"].get("message")

    if not msg:
        return None

    chat = msg.get("chat") or {}
    if not chat:
        return None

    text = msg.get("text") or msg.get("caption")
    return {
        "update_id": update.get("update_id"),
        "chat_id": chat.get("id"),
        "chat_type": chat.get("type"),
        "chat_title": chat.get("title"),
        "username": chat.get("username"),
        "text": text,
    }


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Fetch Telegram chat_id candidates from bot updates.")
    parser.add_argument("--token", default=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(), help="Telegram bot token")
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="How many recent updates to inspect (default: 20)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="HTTP timeout seconds (default: 15)",
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Ignore HTTP(S)_PROXY env and connect directly.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification (not recommended).",
    )
    parser.add_argument(
        "--ca-bundle",
        default=os.getenv("CA_BUNDLE_PATH", "").strip(),
        help="Custom CA bundle path for TLS verification.",
    )
    args = parser.parse_args()

    token = (args.token or "").strip()
    if not token:
        print("ERROR: TELEGRAM_BOT_TOKEN is missing.")
        print("Set it in .env or pass --token.")
        return 1

    limit = max(1, min(args.limit, 100))
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    verify = _resolve_verify(args.insecure, args.ca_bundle)
    if verify is False:
        print("WARN: TLS verification is disabled (--insecure or VERIFY_SSL=false).")

    try:
        data = _fetch_updates(url=url, limit=limit, timeout=args.timeout, direct=args.direct, verify=verify)
    except requests.RequestException as exc:
        print(f"ERROR: network/request failed: {exc}")
        print("Tip: if your shell has invalid proxy vars, run with --direct")
        print("Tip: if your environment intercepts TLS, pass --ca-bundle <path> or --insecure")
        return 2
    except ValueError as exc:
        print(f"ERROR: invalid JSON response: {exc}")
        return 3

    if not data.get("ok"):
        print("ERROR: Telegram API returned failure.")
        print(data)
        return 4

    results = data.get("result", [])
    print(f"ok=True updates={len(results)}")

    extracted: list[dict[str, Any]] = []
    for update in results:
        info = _extract_chat_info(update)
        if info:
            extracted.append(info)

    if not extracted:
        print("No chat info found in updates.")
        print("Try:")
        print("1) Open your bot chat and send /start")
        print("2) Send any text in target group/channel where bot is present")
        print("3) Run this script again")
        return 0

    # Deduplicate by chat_id, keep the latest seen update.
    dedup: dict[int, dict[str, Any]] = {}
    for item in extracted:
        chat_id = item.get("chat_id")
        if isinstance(chat_id, int):
            dedup[chat_id] = item

    print("\nDetected chats (latest per chat_id):")
    for chat_id, item in sorted(dedup.items(), key=lambda kv: kv[0]):
        print(
            {
                "chat_id": chat_id,
                "chat_type": item.get("chat_type"),
                "chat_title": item.get("chat_title"),
                "username": item.get("username"),
                "sample_text": item.get("text"),
            }
        )

    first_chat_id = next(iter(dedup.keys()))
    print("\nUse one of the above chat_id values in .env:")
    print(f"TELEGRAM_CHAT_ID={first_chat_id}")
    return 0

def _fetch_updates(url: str, limit: int, timeout: int, direct: bool, verify: bool | str) -> dict[str, Any]:
    params = {"limit": limit, "timeout": 5}

    if direct:
        session = requests.Session()
        session.trust_env = False
        response = session.get(url, params=params, timeout=timeout, verify=verify)
        response.raise_for_status()
        return response.json()

    try:
        response = requests.get(url, params=params, timeout=timeout, verify=verify)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ProxyError:
        # Auto fallback for common local dead-proxy setup.
        if _looks_like_dead_local_proxy():
            print("WARN: Detected dead local proxy env. Retrying with direct connection...")
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, params=params, timeout=timeout, verify=verify)
            response.raise_for_status()
            return response.json()
        raise


def _looks_like_dead_local_proxy() -> bool:
    proxy_candidates = [
        os.getenv("HTTP_PROXY", ""),
        os.getenv("HTTPS_PROXY", ""),
        os.getenv("http_proxy", ""),
        os.getenv("https_proxy", ""),
    ]
    joined = " ".join(proxy_candidates).lower()
    return "127.0.0.1:9" in joined or "localhost:9" in joined


def _resolve_verify(insecure: bool, ca_bundle: str) -> bool | str:
    if insecure:
        return False

    env_verify = (os.getenv("VERIFY_SSL", "true") or "").strip().lower()
    if env_verify in {"0", "false", "no", "off"}:
        return False

    if ca_bundle:
        return ca_bundle

    return True


if __name__ == "__main__":
    raise SystemExit(main())
