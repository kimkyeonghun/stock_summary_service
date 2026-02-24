from __future__ import annotations

import json
from dataclasses import dataclass

import requests
import urllib3

from stock_mvp.config import Settings


@dataclass(frozen=True)
class LLMJsonResult:
    payload: dict
    model: str


class LLMClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.provider = settings.llm_provider.lower().strip()
        self.verify = settings.ca_bundle_path if settings.ca_bundle_path else settings.verify_ssl
        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        self.session.trust_env = settings.llm_trust_env

    def enabled(self) -> bool:
        if self.provider == "none":
            return False
        if self.provider == "ollama":
            return True
        return bool(self.settings.llm_api_key)

    def generate_json(self, system_prompt: str, user_prompt: str) -> LLMJsonResult | None:
        if not self.enabled():
            return None
        try:
            if self.provider == "gemini":
                return self._call_gemini(system_prompt, user_prompt)
            if self.provider == "openai":
                return self._call_openai(system_prompt, user_prompt)
            if self.provider == "openrouter":
                return self._call_openrouter(system_prompt, user_prompt)
            if self.provider == "ollama":
                return self._call_ollama(system_prompt, user_prompt)
        except Exception as exc:
            print(f"[WARN] llm provider={self.provider} call failed: {exc}")
        return None

    def _call_openai(self, system_prompt: str, user_prompt: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "https://api.openai.com/v1"
        url = f"{base.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.settings.llm_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.settings.llm_model,
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
        return LLMJsonResult(payload=parsed, model=str(data.get("model") or self.settings.llm_model))

    def _call_gemini(self, system_prompt: str, user_prompt: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "https://generativelanguage.googleapis.com/v1beta"
        model = self.settings.llm_model
        url = f"{base.rstrip('/')}/models/{model}:generateContent"
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
        return LLMJsonResult(payload=parsed, model=model)

    def _call_openrouter(self, system_prompt: str, user_prompt: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "https://openrouter.ai/api/v1"
        url = f"{base.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.settings.llm_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.settings.llm_model,
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
        return LLMJsonResult(payload=parsed, model=str(data.get("model") or self.settings.llm_model))

    def _call_ollama(self, system_prompt: str, user_prompt: str) -> LLMJsonResult | None:
        base = self.settings.llm_api_base or "http://127.0.0.1:11434"
        url = f"{base.rstrip('/')}/api/chat"
        payload = {
            "model": self.settings.llm_model,
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
        model_name = str(data.get("model") or self.settings.llm_model)
        return LLMJsonResult(payload=parsed, model=model_name)


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
