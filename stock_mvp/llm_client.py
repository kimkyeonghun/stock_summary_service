from __future__ import annotations

import atexit
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path

import requests
import urllib3

from stock_mvp.config import Settings


@dataclass(frozen=True)
class LLMJsonResult:
    payload: dict
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0


class LLMClient:
    _DAILY_META_PREFIX = "llm_budget.daily."

    def __init__(self, settings: Settings):
        self.settings = settings
        self.provider = settings.llm_provider.lower().strip()
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        self.session.trust_env = settings.llm_trust_env
        self._job_spend_usd = 0.0
        self._cache: dict[str, LLMJsonResult] = {}
        self._daily_usage_cache: dict[str, dict] = {}
        self._daily_usage_dirty_counts: dict[str, int] = {}
        self._budget_flush_every_calls = max(1, int(settings.llm_budget_flush_every_calls))
        atexit.register(self.flush_pending_budget_usage)

    def enabled(self) -> bool:
        if self.provider == "none":
            return False
        if self.provider == "ollama":
            return True
        return bool(self.settings.llm_api_key)

    def generate_json(self, system_prompt: str, user_prompt: str, *, purpose: str = "general") -> LLMJsonResult | None:
        if not self.enabled():
            return None

        system_prompt, user_prompt = self._apply_input_hard_cap(system_prompt, user_prompt)
        daily_usage = self._load_daily_usage() if self._budget_enabled() else self._empty_daily_usage()
        model_name = self._choose_model(daily_usage)

        cache_key = self._cache_key(model_name, system_prompt, user_prompt)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return LLMJsonResult(payload=dict(cached.payload), model=cached.model)

        estimated_input_tokens = _estimate_tokens(len(system_prompt) + len(user_prompt))
        estimated_output_tokens = max(1, int(self.settings.llm_max_tokens))
        estimated_cost = self._estimate_cost_usd(estimated_input_tokens, estimated_output_tokens)
        if self._would_exceed_budget(estimated_cost, daily_usage):
            return None

        try:
            result = self._call_provider(model_name=model_name, system_prompt=system_prompt, user_prompt=user_prompt)
            if result is None:
                return None
            prompt_tokens = result.prompt_tokens if result.prompt_tokens > 0 else estimated_input_tokens
            completion_tokens = result.completion_tokens
            if completion_tokens <= 0:
                completion_tokens = min(
                    max(1, _estimate_tokens(len(json.dumps(result.payload, ensure_ascii=False)))),
                    max(1, int(self.settings.llm_max_tokens)),
                )
            actual_cost = self._estimate_cost_usd(prompt_tokens, completion_tokens)
            self._job_spend_usd += actual_cost
            if self._budget_enabled():
                self._stage_daily_usage(
                    self._merge_daily_usage(
                        daily_usage=daily_usage,
                        purpose=purpose,
                        model=result.model,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        spent_usd=actual_cost,
                    )
                )
            self._put_cache(cache_key, result)
            return result
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "?"
            body = ""
            if exc.response is not None:
                body = (exc.response.text or "").strip().replace("\n", " ")[:220]
            print(
                f"[WARN] llm provider={self.provider} model={model_name} http_error={code} "
                f"body={body or '(empty)'}"
            )
        except Exception as exc:
            print(f"[WARN] llm provider={self.provider} model={model_name} call failed: {exc}")
        return None

    def _call_provider(self, *, model_name: str, system_prompt: str, user_prompt: str) -> LLMJsonResult | None:
        if self.provider == "gemini":
            return self._call_gemini(system_prompt, user_prompt, model_name=model_name)
        if self.provider == "openai":
            return self._call_openai(system_prompt, user_prompt, model_name=model_name)
        if self.provider == "openrouter":
            return self._call_openrouter(system_prompt, user_prompt, model_name=model_name)
        if self.provider == "ollama":
            return self._call_ollama(system_prompt, user_prompt, model_name=model_name)
        return None

    def _call_openai(self, system_prompt: str, user_prompt: str, *, model_name: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "https://api.openai.com/v1"
        url = f"{base.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.settings.llm_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.settings.llm_temperature,
            "max_tokens": self.settings.llm_max_tokens,
            "response_format": {"type": "json_object"},
        }
        response = self.session.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.settings.llm_request_timeout_sec,
            verify=self.verify,
        )
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        parsed = _parse_json_content(content)
        if parsed is None:
            return None
        usage = data.get("usage") or {}
        return LLMJsonResult(
            payload=parsed,
            model=str(data.get("model") or model_name),
            prompt_tokens=_to_int(usage.get("prompt_tokens")),
            completion_tokens=_to_int(usage.get("completion_tokens")),
        )

    def _call_gemini(self, system_prompt: str, user_prompt: str, *, model_name: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "https://generativelanguage.googleapis.com/v1beta"
        url = f"{base.rstrip('/')}/models/{model_name}:generateContent"
        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": self.settings.llm_api_key,
        }
        payload = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": user_prompt}],
                }
            ],
            "generationConfig": {
                "temperature": self.settings.llm_temperature,
                "maxOutputTokens": self.settings.llm_max_tokens,
                "responseMimeType": "application/json",
            },
        }
        response = self.session.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.settings.llm_request_timeout_sec,
            verify=self.verify,
        )
        response.raise_for_status()
        data = response.json()
        candidates = data.get("candidates") or []
        if not candidates:
            return None
        content_obj = candidates[0].get("content") or {}
        parts = content_obj.get("parts") or []
        text_chunks = [str(p.get("text") or "") for p in parts if isinstance(p, dict)]
        content = "\n".join(x for x in text_chunks if x).strip()
        parsed = _parse_json_content(content)
        if parsed is None:
            return None
        usage = data.get("usageMetadata") or {}
        return LLMJsonResult(
            payload=parsed,
            model=model_name,
            prompt_tokens=_to_int(usage.get("promptTokenCount")),
            completion_tokens=_to_int(usage.get("candidatesTokenCount")),
        )

    def _call_openrouter(self, system_prompt: str, user_prompt: str, *, model_name: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "https://openrouter.ai/api/v1"
        url = f"{base.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.settings.llm_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.settings.llm_temperature,
            "max_tokens": self.settings.llm_max_tokens,
            "response_format": {"type": "json_object"},
        }
        response = self.session.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.settings.llm_request_timeout_sec,
            verify=self.verify,
        )
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        parsed = _parse_json_content(content)
        if parsed is None:
            return None
        usage = data.get("usage") or {}
        return LLMJsonResult(
            payload=parsed,
            model=str(data.get("model") or model_name),
            prompt_tokens=_to_int(usage.get("prompt_tokens")),
            completion_tokens=_to_int(usage.get("completion_tokens")),
        )

    def _call_ollama(self, system_prompt: str, user_prompt: str, *, model_name: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "http://127.0.0.1:11434"
        url = f"{base.rstrip('/')}/api/chat"
        payload = {
            "model": model_name,
            "stream": False,
            "format": "json",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "options": {
                "temperature": self.settings.llm_temperature,
            },
        }
        response = self.session.post(
            url,
            json=payload,
            timeout=self.settings.llm_request_timeout_sec,
            verify=self.verify,
        )
        response.raise_for_status()
        data = response.json()
        message = data.get("message") or {}
        content = str(message.get("content") or "")
        parsed = _parse_json_content(content)
        if parsed is None:
            return None
        return LLMJsonResult(
            payload=parsed,
            model=str(data.get("model") or model_name),
            prompt_tokens=_to_int(data.get("prompt_eval_count")),
            completion_tokens=_to_int(data.get("eval_count")),
        )

    def _apply_input_hard_cap(self, system_prompt: str, user_prompt: str) -> tuple[str, str]:
        max_chars = int(self.settings.llm_hard_max_input_chars)
        if max_chars <= 0:
            return system_prompt, user_prompt
        total_chars = len(system_prompt) + len(user_prompt)
        if total_chars <= max_chars:
            return system_prompt, user_prompt
        remaining_for_user = max(0, max_chars - len(system_prompt))
        if remaining_for_user == 0:
            cut_system = system_prompt[: max_chars // 2]
            cut_user = user_prompt[: max(0, max_chars - len(cut_system))]
            print(
                f"[WARN] llm prompt hard-capped: total_chars={total_chars} "
                f"max_chars={max_chars} system_trimmed=1 user_trimmed=1"
            )
            return cut_system, cut_user
        print(f"[WARN] llm prompt hard-capped: total_chars={total_chars} max_chars={max_chars} user_trimmed=1")
        return system_prompt, user_prompt[:remaining_for_user]

    def _budget_enabled(self) -> bool:
        return (
            self.settings.llm_daily_budget_usd > 0
            or self.settings.llm_job_budget_usd > 0
            or bool(self.settings.llm_budget_model)
        )

    def _would_exceed_budget(self, estimated_cost_usd: float, daily_usage: dict) -> bool:
        if estimated_cost_usd <= 0:
            return False
        job_budget = float(self.settings.llm_job_budget_usd)
        if job_budget > 0 and (self._job_spend_usd + estimated_cost_usd) > job_budget:
            print(
                f"[WARN] llm job budget exceeded: job_spend={self._job_spend_usd:.6f} "
                f"est={estimated_cost_usd:.6f} budget={job_budget:.6f}"
            )
            return True
        daily_budget = float(self.settings.llm_daily_budget_usd)
        if daily_budget > 0:
            spent = float(daily_usage.get("spent_usd") or 0.0)
            if (spent + estimated_cost_usd) > daily_budget:
                print(
                    f"[WARN] llm daily budget exceeded: daily_spend={spent:.6f} "
                    f"est={estimated_cost_usd:.6f} budget={daily_budget:.6f}"
                )
                return True
        return False

    def _choose_model(self, daily_usage: dict) -> str:
        default_model = self.settings.llm_model
        budget_model = self.settings.llm_budget_model.strip()
        if not budget_model:
            return default_model
        daily_budget = float(self.settings.llm_daily_budget_usd)
        if daily_budget <= 0:
            return budget_model
        spent = float(daily_usage.get("spent_usd") or 0.0)
        ratio = spent / daily_budget if daily_budget > 0 else 0.0
        threshold = max(0.0, min(float(self.settings.llm_soft_budget_ratio), 1.0))
        if ratio >= threshold:
            print(
                f"[INFO] llm soft-budget model switch: spend_ratio={ratio:.3f} "
                f"threshold={threshold:.3f} model={budget_model}"
            )
            return budget_model
        return default_model

    def _estimate_cost_usd(self, input_tokens: int, output_tokens: int) -> float:
        in_rate = max(0.0, float(self.settings.llm_cost_input_per_1k_usd))
        out_rate = max(0.0, float(self.settings.llm_cost_output_per_1k_usd))
        if in_rate == 0 and out_rate == 0:
            return 0.0
        return (max(0, input_tokens) / 1000.0) * in_rate + (max(0, output_tokens) / 1000.0) * out_rate

    def _cache_key(self, model_name: str, system_prompt: str, user_prompt: str) -> str:
        raw = f"{self.provider}|{model_name}|{system_prompt}|{user_prompt}"
        return sha1(raw.encode("utf-8")).hexdigest()

    def _put_cache(self, key: str, result: LLMJsonResult) -> None:
        self._cache[key] = result
        if len(self._cache) <= 256:
            return
        oldest_key = next(iter(self._cache.keys()))
        self._cache.pop(oldest_key, None)

    def _load_daily_usage(self) -> dict:
        key = self._daily_meta_key()
        cached = self._daily_usage_cache.get(key)
        if cached is not None:
            return dict(cached)

        raw = self._get_meta_value(key)
        if not raw:
            usage = self._empty_daily_usage()
            self._daily_usage_cache[key] = usage
            return dict(usage)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            usage = self._empty_daily_usage()
            self._daily_usage_cache[key] = usage
            return dict(usage)
        if not isinstance(parsed, dict):
            usage = self._empty_daily_usage()
            self._daily_usage_cache[key] = usage
            return dict(usage)
        usage = self._empty_daily_usage()
        usage["calls"] = int(parsed.get("calls") or 0)
        usage["input_tokens"] = int(parsed.get("input_tokens") or 0)
        usage["output_tokens"] = int(parsed.get("output_tokens") or 0)
        usage["spent_usd"] = float(parsed.get("spent_usd") or 0.0)
        usage["by_purpose"] = parsed.get("by_purpose") if isinstance(parsed.get("by_purpose"), dict) else {}
        usage["by_model"] = parsed.get("by_model") if isinstance(parsed.get("by_model"), dict) else {}
        self._daily_usage_cache[key] = dict(usage)
        return usage

    def _merge_daily_usage(
        self,
        *,
        daily_usage: dict,
        purpose: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        spent_usd: float,
    ) -> dict:
        usage = dict(daily_usage)
        usage["calls"] = int(usage.get("calls") or 0) + 1
        usage["input_tokens"] = int(usage.get("input_tokens") or 0) + max(0, prompt_tokens)
        usage["output_tokens"] = int(usage.get("output_tokens") or 0) + max(0, completion_tokens)
        usage["spent_usd"] = round(float(usage.get("spent_usd") or 0.0) + max(0.0, spent_usd), 8)

        purpose_key = (purpose or "general").strip().lower() or "general"
        by_purpose = usage.get("by_purpose")
        if not isinstance(by_purpose, dict):
            by_purpose = {}
        purpose_entry = by_purpose.get(purpose_key)
        if not isinstance(purpose_entry, dict):
            purpose_entry = {"calls": 0, "spent_usd": 0.0}
        purpose_entry["calls"] = int(purpose_entry.get("calls") or 0) + 1
        purpose_entry["spent_usd"] = round(float(purpose_entry.get("spent_usd") or 0.0) + max(0.0, spent_usd), 8)
        by_purpose[purpose_key] = purpose_entry
        usage["by_purpose"] = by_purpose

        model_key = (model or "unknown").strip() or "unknown"
        by_model = usage.get("by_model")
        if not isinstance(by_model, dict):
            by_model = {}
        model_entry = by_model.get(model_key)
        if not isinstance(model_entry, dict):
            model_entry = {"calls": 0, "spent_usd": 0.0}
        model_entry["calls"] = int(model_entry.get("calls") or 0) + 1
        model_entry["spent_usd"] = round(float(model_entry.get("spent_usd") or 0.0) + max(0.0, spent_usd), 8)
        by_model[model_key] = model_entry
        usage["by_model"] = by_model
        return usage

    def _save_daily_usage(self, usage: dict) -> None:
        key = self._daily_meta_key()
        self._set_meta_value(key, json.dumps(usage, ensure_ascii=False))
        self._daily_usage_cache[key] = dict(usage)
        self._daily_usage_dirty_counts[key] = 0

    def _stage_daily_usage(self, usage: dict) -> None:
        key = self._daily_meta_key()
        self._daily_usage_cache[key] = dict(usage)
        dirty = int(self._daily_usage_dirty_counts.get(key) or 0) + 1
        self._daily_usage_dirty_counts[key] = dirty
        if dirty >= self._budget_flush_every_calls:
            self._save_daily_usage(usage)

    def flush_pending_budget_usage(self) -> None:
        for key, usage in list(self._daily_usage_cache.items()):
            dirty = int(self._daily_usage_dirty_counts.get(key) or 0)
            if dirty <= 0:
                continue
            self._set_meta_value(key, json.dumps(usage, ensure_ascii=False))
            self._daily_usage_dirty_counts[key] = 0

    @classmethod
    def _empty_daily_usage(cls) -> dict:
        return {
            "calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "spent_usd": 0.0,
            "by_purpose": {},
            "by_model": {},
        }

    def _daily_meta_key(self) -> str:
        day = datetime.now(timezone.utc).date().isoformat()
        return f"{self._DAILY_META_PREFIX}{day}"

    def _get_meta_value(self, key: str) -> str:
        db_path = Path(self.settings.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
            if row is None:
                return ""
            return str(row["value"] or "")

    def _set_meta_value(self, key: str, value: str) -> None:
        db_path = Path(self.settings.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO app_meta(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value=excluded.value,
                  updated_at=excluded.updated_at
                """,
                (key, value, _utc_iso()),
            )
            conn.commit()


def _parse_json_content(content: str) -> dict | None:
    raw = (content or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        candidate = raw[start : end + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return None
    return None


def _estimate_tokens(char_count: int) -> int:
    if char_count <= 0:
        return 0
    return max(1, int(math.ceil(char_count / 4.0)))


def _to_int(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
