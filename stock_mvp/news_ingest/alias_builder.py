from __future__ import annotations

import re

from stock_mvp.utils import compact_text


def build_alias_rows_for_ticker(
    *,
    ticker: str,
    company_name: str,
    corp_name: str,
) -> list[tuple[str, str, str, float, bool]]:
    aliases: list[tuple[str, str, str, float, bool]] = []
    seen: set[tuple[str, str]] = set()

    def _push(alias: str, alias_type: str, weight: float) -> None:
        value = _normalize_alias(alias)
        if not value:
            return
        key = (value.lower(), alias_type)
        if key in seen:
            return
        seen.add(key)
        aliases.append((ticker.upper(), value, alias_type, float(weight), True))

    _push(company_name, "official_name", 1.0)
    _push(corp_name, "corp_name", 0.95)

    for short_name in _derive_short_names(company_name, corp_name):
        _push(short_name, "short_name", 0.9)

    return aliases


def _derive_short_names(company_name: str, corp_name: str) -> list[str]:
    names = [company_name, corp_name]
    out: list[str] = []
    for raw in names:
        name = compact_text(raw)
        if not name:
            continue
        candidate = name
        candidate = re.sub(r"^\(?주\)?식회사\s*", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s*주식회사$", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s*(Inc|Corp|Corporation|Holdings?)\.?$", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"^\(?주\)?\s*", "", candidate, flags=re.IGNORECASE)
        candidate = candidate.replace("㈜", "")
        candidate = compact_text(candidate)
        if candidate and candidate != name:
            out.append(candidate)
    return list(dict.fromkeys(out))


def _normalize_alias(alias: str) -> str:
    value = compact_text(alias)
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value).strip()
    if value.isdigit():
        return ""
    if len(value) < 2:
        return ""
    return value
