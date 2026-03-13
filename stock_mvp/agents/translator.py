from __future__ import annotations

import copy
import json
import re
import time
from dataclasses import dataclass, replace
from hashlib import sha1
from typing import Any

from stock_mvp.config import Settings
from stock_mvp.llm_client import LLMClient
from stock_mvp.storage import translation_cache_repo
from stock_mvp.utils import compact_text


FACT_TOKEN_RE = re.compile(
    r"(?:\b20\d{2}[./-]\d{1,2}[./-]\d{1,2}\b|\b\d+(?:[.,]\d+)?%|\b\d+(?:[.,]\d+)?\b|\b[A-Z]{2,6}\b)"
)
HANGUL_RE = re.compile(r"[\uac00-\ud7a3]")
LATIN_RE = re.compile(r"[A-Za-z]")

BATCH_MAX_ITEMS = 12
BATCH_MAX_CHARS = 6000


@dataclass
class TranslationMetrics:
    calls: int = 0
    cache_hits: int = 0
    elapsed_sec: float = 0.0
    fail_count: int = 0


_METRICS = TranslationMetrics()


def reset_translation_metrics() -> None:
    _METRICS.calls = 0
    _METRICS.cache_hits = 0
    _METRICS.elapsed_sec = 0.0
    _METRICS.fail_count = 0


def get_translation_metrics() -> TranslationMetrics:
    return TranslationMetrics(
        calls=_METRICS.calls,
        cache_hits=_METRICS.cache_hits,
        elapsed_sec=_METRICS.elapsed_sec,
        fail_count=_METRICS.fail_count,
    )


def is_korean_dominant(text: str) -> bool:
    value = compact_text(text)
    if not value:
        return True
    hangul = len(HANGUL_RE.findall(value))
    latin = len(LATIN_RE.findall(value))
    if hangul == 0:
        return False
    return hangul >= max(6, int(latin * 1.2))


def validate_fact_token_preservation(src: str, dst: str) -> bool:
    src_tokens = [_canon_token(x) for x in FACT_TOKEN_RE.findall(src)]
    src_tokens = [x for x in src_tokens if x]
    if not src_tokens:
        return True
    dst_norm = _canon_text(dst)
    if not dst_norm:
        return False
    for token in src_tokens:
        if token not in dst_norm:
            return False
    return True


class Translator:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.enabled = bool(settings.translation_enabled)
        self.model = settings.translation_model or "gpt-4o-mini"
        self.max_retries = max(0, int(settings.translation_max_retries))
        self.llm = self._build_llm()
        if self.llm is None:
            self.enabled = False

    def set_max_retries(self, retries: int) -> None:
        self.max_retries = max(0, int(retries))

    def translate_text_to_ko(self, conn, text: str, purpose: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        if not self.enabled:
            return value
        if is_korean_dominant(value):
            return value

        source_hash = self._hash_text(value, purpose=purpose)
        cached = translation_cache_repo.get_translation(conn, source_hash=source_hash)
        if cached is not None:
            _METRICS.cache_hits += 1
            return compact_text(str(cached.get("ko_text") or "")) or value

        translated = self._translate_with_retries(value=value, purpose=purpose)
        if not translated:
            _METRICS.fail_count += 1
            return value
        translation_cache_repo.upsert_translation(
            conn,
            source_hash=source_hash,
            src_text=value,
            ko_text=translated,
            model=self.model,
            commit=False,
        )
        return translated

    def translate_lines_to_ko(self, conn, lines: list[str], purpose: str) -> list[str]:
        out: list[str] = []
        for idx, line in enumerate(list(lines or []), start=1):
            text = compact_text(str(line))
            if not text:
                continue
            out.append(self.translate_text_to_ko(conn, text, purpose=f"{purpose}_line{idx}"))
        return out

    def translate_batch_to_ko(self, conn, texts: list[str], purpose: str) -> list[str]:
        values = [str(x or "").strip() for x in list(texts or [])]
        if not values:
            return []
        if not self.enabled:
            return values

        outputs = list(values)
        pending_by_hash: dict[str, dict[str, Any]] = {}
        cache_seen: dict[str, str | None] = {}

        for idx, value in enumerate(values):
            if not value:
                outputs[idx] = ""
                continue
            if is_korean_dominant(value):
                continue

            source_hash = self._hash_text(value, purpose=f"{purpose}_batch")
            if source_hash not in cache_seen:
                cached = translation_cache_repo.get_translation(conn, source_hash=source_hash)
                cache_seen[source_hash] = compact_text(str(cached.get("ko_text") or "")) if cached is not None else None
            cached_text = cache_seen[source_hash]
            if cached_text:
                outputs[idx] = cached_text
                _METRICS.cache_hits += 1
                continue

            bucket = pending_by_hash.get(source_hash)
            if bucket is None:
                pending_by_hash[source_hash] = {"hash": source_hash, "text": value, "indexes": [idx]}
            else:
                bucket["indexes"].append(idx)

        pending = list(pending_by_hash.values())
        if not pending:
            return outputs

        for chunk in _split_batch_chunks(pending, max_items=BATCH_MAX_ITEMS, max_chars=BATCH_MAX_CHARS):
            chunk_texts = [str(x["text"]) for x in chunk]
            chunk_translated = self._translate_batch_with_retries(values=chunk_texts, purpose=purpose)
            if len(chunk_translated) != len(chunk_texts):
                chunk_translated = []

            if not chunk_translated:
                for entry in chunk:
                    source_text = str(entry["text"])
                    translated = self._translate_with_retries(value=source_text, purpose=f"{purpose}_fallback")
                    if not translated:
                        _METRICS.fail_count += 1
                        translated = source_text
                    self._write_batch_result(conn, entry=entry, translated=translated, outputs=outputs)
                continue

            for entry, translated in zip(chunk, chunk_translated):
                self._write_batch_result(conn, entry=entry, translated=translated, outputs=outputs)

        return outputs

    def translate_structured_to_ko(self, conn, payload: Any, purpose: str) -> Any:
        if not self.enabled:
            return payload
        cloned = copy.deepcopy(payload)
        paths: list[tuple[Any, ...]] = []
        values: list[str] = []
        _collect_text_nodes(cloned, path=(), paths=paths, values=values)
        if not values:
            return cloned
        translated = self.translate_batch_to_ko(conn, values, purpose=f"{purpose}_structured")
        for path, value in zip(paths, translated):
            _set_nested_value(cloned, path, value)
        return cloned

    def translate_markdown_to_ko(self, conn, md_text: str, purpose: str) -> str:
        value = str(md_text or "").strip()
        if not value:
            return ""
        if not self.enabled:
            return value
        source_hash = self._hash_text(value, purpose=f"{purpose}_md")
        cached = translation_cache_repo.get_translation(conn, source_hash=source_hash)
        if cached is not None:
            _METRICS.cache_hits += 1
            return str(cached.get("ko_text") or "") or value

        translated = self._translate_with_retries(value=value, purpose=f"{purpose}_markdown", markdown=True)
        if not translated:
            _METRICS.fail_count += 1
            return value
        translation_cache_repo.upsert_translation(
            conn,
            source_hash=source_hash,
            src_text=value,
            ko_text=translated,
            model=self.model,
            commit=False,
        )
        return translated

    def _write_batch_result(self, conn, *, entry: dict[str, Any], translated: str, outputs: list[str]) -> None:
        source_text = str(entry["text"])
        source_hash = str(entry["hash"])
        value = translated or source_text
        if not validate_fact_token_preservation(source_text, value):
            repaired = self._translate_with_retries(value=source_text, purpose="translation_repair")
            if repaired:
                value = repaired
            else:
                _METRICS.fail_count += 1
                value = source_text

        if value == source_text and not is_korean_dominant(value):
            _METRICS.fail_count += 1

        for idx in list(entry.get("indexes") or []):
            outputs[int(idx)] = value

        if value != source_text:
            translation_cache_repo.upsert_translation(
                conn,
                source_hash=source_hash,
                src_text=source_text,
                ko_text=value,
                model=self.model,
                commit=False,
            )

    def _build_llm(self) -> LLMClient | None:
        provider = (self.settings.translation_provider or "").strip().lower()
        api_key = (self.settings.translation_api_key or "").strip()
        if not provider or provider == "none":
            return None
        if provider != "ollama" and not api_key:
            return None

        api_base = self.settings.llm_api_base
        if provider == "openai" and "generativelanguage.googleapis.com" in api_base:
            api_base = ""
        translated_settings = replace(
            self.settings,
            llm_provider=provider,
            llm_model=self.model,
            llm_api_key=api_key,
            llm_api_base=api_base,
            llm_request_timeout_sec=max(1, int(self.settings.translation_timeout_sec)),
        )
        return LLMClient(translated_settings)

    def _translate_with_retries(self, *, value: str, purpose: str, markdown: bool = False) -> str:
        if self.llm is None:
            return ""
        for attempt in range(self.max_retries + 1):
            strict = attempt > 0
            started = time.perf_counter()
            result = self.llm.generate_json(
                system_prompt=_translation_system_prompt(markdown=markdown, strict=strict),
                user_prompt=_translation_user_prompt(value=value),
                purpose="translation",
            )
            _METRICS.elapsed_sec += time.perf_counter() - started
            _METRICS.calls += 1
            if result is None:
                continue
            payload = result.payload or {}
            raw_text = str(payload.get("text") or "")
            translated = _normalize_text_block(raw_text, keep_newlines=markdown or ("\n" in value))
            if not translated:
                continue
            if validate_fact_token_preservation(value, translated):
                return translated
        return ""

    def _translate_batch_with_retries(self, *, values: list[str], purpose: str) -> list[str]:
        if self.llm is None or not values:
            return []
        for attempt in range(self.max_retries + 1):
            strict = attempt > 0
            started = time.perf_counter()
            result = self.llm.generate_json(
                system_prompt=_translation_batch_system_prompt(strict=strict),
                user_prompt=_translation_batch_user_prompt(values=values),
                purpose="translation",
            )
            _METRICS.elapsed_sec += time.perf_counter() - started
            _METRICS.calls += 1
            if result is None:
                continue
            payload = result.payload or {}
            raw_items = payload.get("translations")
            if not isinstance(raw_items, list) or len(raw_items) != len(values):
                continue

            translated_items: list[str] = []
            valid = True
            for src, raw in zip(values, raw_items):
                translated = _normalize_text_block(str(raw or ""), keep_newlines=("\n" in src))
                if not translated or not validate_fact_token_preservation(src, translated):
                    valid = False
                    break
                translated_items.append(translated)
            if valid:
                return translated_items
        return []

    def _hash_text(self, text: str, *, purpose: str) -> str:
        payload = f"{self.settings.translation_provider}|{self.model}|{purpose}|{text}"
        return sha1(payload.encode("utf-8")).hexdigest()


def _translation_system_prompt(*, markdown: bool, strict: bool) -> str:
    style = (
        "Translate the input into natural Korean for retail investors. "
        "Keep readability high with concise paraphrasing."
    )
    if markdown:
        style += " Preserve markdown headings, bullets, and line breaks."
    constraints = (
        "Return JSON only with key 'text'. "
        "Do not add investment recommendation language. "
        "Do not drop numbers, dates, percentages, currency values, or ticker symbols."
    )
    if strict:
        constraints += " Strictly preserve all factual tokens exactly."
    return f"{style} {constraints}"


def _translation_batch_system_prompt(*, strict: bool) -> str:
    constraints = (
        "Translate each input sentence into Korean for beginner investors. "
        "Return JSON only with key 'translations' as a string array in the exact same order and length. "
        "Do not drop numbers, dates, percentages, currency values, or ticker symbols. "
        "Do not add investment recommendation language."
    )
    if strict:
        constraints += " Strictly preserve all factual tokens exactly for each item."
    return constraints


def _translation_user_prompt(*, value: str) -> str:
    return f"Translate to Korean:\n{value}"


def _translation_batch_user_prompt(*, values: list[str]) -> str:
    payload = {"texts": values}
    return f"Translate each text to Korean and return JSON:\n{json.dumps(payload, ensure_ascii=False)}"


def _canon_text(text: str) -> str:
    return re.sub(r"[\s,]", "", str(text or "")).upper()


def _canon_token(token: str) -> str:
    return _canon_text(token)


def _normalize_text_block(text: str, *, keep_newlines: bool) -> str:
    raw = str(text or "")
    if not raw:
        return ""
    if not keep_newlines:
        return compact_text(raw)
    lines = [compact_text(x) for x in raw.splitlines()]
    lines = [x for x in lines if x]
    return "\n".join(lines)


def _collect_text_nodes(node: Any, *, path: tuple[Any, ...], paths: list[tuple[Any, ...]], values: list[str]) -> None:
    if isinstance(node, str):
        paths.append(path)
        values.append(node)
        return
    if isinstance(node, list):
        for idx, value in enumerate(node):
            _collect_text_nodes(value, path=(*path, idx), paths=paths, values=values)
        return
    if isinstance(node, dict):
        for key, value in node.items():
            _collect_text_nodes(value, path=(*path, key), paths=paths, values=values)


def _set_nested_value(root: Any, path: tuple[Any, ...], value: str) -> None:
    if not path:
        return
    cur = root
    for key in path[:-1]:
        cur = cur[key]
    cur[path[-1]] = value


def _split_batch_chunks(
    pending: list[dict[str, Any]],
    *,
    max_items: int,
    max_chars: int,
) -> list[list[dict[str, Any]]]:
    chunks: list[list[dict[str, Any]]] = []
    chunk: list[dict[str, Any]] = []
    chunk_chars = 0
    for entry in pending:
        text = str(entry.get("text") or "")
        text_chars = max(1, len(text))
        if chunk and (len(chunk) >= max_items or chunk_chars + text_chars > max_chars):
            chunks.append(chunk)
            chunk = []
            chunk_chars = 0
        chunk.append(entry)
        chunk_chars += text_chars
    if chunk:
        chunks.append(chunk)
    return chunks
