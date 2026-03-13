from __future__ import annotations

import io
import re
import zipfile
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

from stock_mvp.database import (
    connect,
    get_opendart_corp_code_map,
    init_db,
    latest_opendart_corp_code_updated_at,
    list_active_stock_codes_by_market,
    upsert_opendart_corp_codes,
)
from stock_mvp.models import CollectedDocument, Stock
from stock_mvp.utils import compact_text, parse_datetime_maybe

from .base import BaseCrawler


class OpenDartDisclosureCrawler(BaseCrawler):
    source = "opendart"
    doc_type = "filing"
    corp_code_url = "https://opendart.fss.or.kr/api/corpCode.xml"
    disclosure_list_url = "https://opendart.fss.or.kr/api/list.json"

    def __init__(self, settings):
        super().__init__(settings)
        self._stock_to_corp: dict[str, str] = {}
        self._corp_map_loaded = False
        self._warned_no_api_key = False
        self._disabled_reason = ""
        self._warned_disabled = False

    def reset_run_state(self) -> None:
        self._corp_map_loaded = False
        self._disabled_reason = ""
        self._warned_disabled = False

    def mark_unavailable(self, reason: str) -> None:
        self._disabled_reason = compact_text(str(reason or "")) or "unavailable"

    def prepare_run(self, conn=None) -> None:
        self._ensure_corp_code_map(conn=conn)

    def collect(self, stock: Stock, limit: int) -> list[CollectedDocument]:
        if str(stock.market or "").upper() != "KR":
            return []
        if self._disabled_reason:
            if not self._warned_disabled:
                print(f"[WARN] opendart skipped: {self._disabled_reason}")
                self._warned_disabled = True
            return []
        if not self.settings.opendart_api_key:
            if not self._warned_no_api_key:
                print("[WARN] opendart skipped: missing OPENDART_API_KEY")
                self._warned_no_api_key = True
            return []

        self._ensure_corp_code_map()
        corp_code = self._stock_to_corp.get(str(stock.code or "").upper(), "")
        if not corp_code:
            return []

        now = datetime.now(timezone.utc)
        lookback_days = max(1, int(self.settings.opendart_lookback_days))
        bgn_de = (now - timedelta(days=lookback_days)).strftime("%Y%m%d")
        end_de = now.strftime("%Y%m%d")
        rows = self._fetch_disclosure_rows(
            corp_code=corp_code,
            bgn_de=bgn_de,
            end_de=end_de,
            max_rows=max(1, int(limit)),
        )
        if not rows:
            return []

        core_keywords = self._core_keywords()
        docs: list[CollectedDocument] = []
        for row in rows:
            report_nm = compact_text(str(row.get("report_nm") or ""))
            if core_keywords and not any(k in report_nm for k in core_keywords):
                continue

            rcp_no = compact_text(str(row.get("rcept_no") or ""))
            if not rcp_no:
                continue
            rcp_dt = compact_text(str(row.get("rcept_dt") or ""))
            flr_nm = compact_text(str(row.get("flr_nm") or ""))
            corp_name = compact_text(str(row.get("corp_name") or stock.name or ""))
            rm = compact_text(str(row.get("rm") or ""))
            note = f", note={rm}" if rm else ""
            body = compact_text(
                f"OpenDART disclosure detected: corp={corp_name}, report={report_nm}, "
                f"date={rcp_dt}, filer={flr_nm}{note}."
            )
            url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"
            title = compact_text(f"{stock.code} {report_nm} ({rcp_dt})")
            docs.append(
                CollectedDocument(
                    stock_code=stock.code,
                    source=self.source,
                    doc_type=self.doc_type,
                    title=title,
                    url=url,
                    published_at=parse_datetime_maybe(rcp_dt),
                    body=body,
                )
            )
            if len(docs) >= limit:
                break
        return docs

    def _ensure_corp_code_map(self, conn=None) -> None:
        if self._corp_map_loaded:
            return

        if conn is None:
            with connect(self.settings.db_path) as local_conn:
                init_db(local_conn)
                self._ensure_corp_code_map(conn=local_conn)
            return

        refresh_days = max(1, int(self.settings.opendart_corp_code_refresh_days))
        local_map = get_opendart_corp_code_map(conn, market="KR")
        latest_updated = latest_opendart_corp_code_updated_at(conn, market="KR")
        stale = self._is_stale(latest_updated, refresh_days=refresh_days)
        if not local_map or stale:
            fetched_map = self._download_corp_code_map()
            active_codes = set(list_active_stock_codes_by_market(conn, "KR"))
            rows = [
                (stock_code, corp_code, corp_name)
                for stock_code, (corp_code, corp_name) in fetched_map.items()
                if stock_code in active_codes
            ]
            upsert_opendart_corp_codes(conn, rows, commit=False)
            local_map = get_opendart_corp_code_map(conn, market="KR")
        self._stock_to_corp = {k.upper(): v for k, v in local_map.items()}
        self._corp_map_loaded = True

    def _download_corp_code_map(self) -> dict[str, tuple[str, str]]:
        response = self._get(self.corp_code_url, params={"crtfc_key": self.settings.opendart_api_key})
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            xml_name = next((n for n in zf.namelist() if n.lower().endswith(".xml")), "")
            if not xml_name:
                return {}
            xml_bytes = zf.read(xml_name)
        root = ET.fromstring(xml_bytes)
        out: dict[str, tuple[str, str]] = {}
        for item in root.findall(".//list"):
            corp_code = compact_text(item.findtext("corp_code") or "")
            corp_name = compact_text(item.findtext("corp_name") or "")
            stock_code = compact_text(item.findtext("stock_code") or "")
            if not corp_code or not re.fullmatch(r"\d{8}", corp_code):
                continue
            if not re.fullmatch(r"\d{6}", stock_code):
                continue
            out[stock_code] = (corp_code, corp_name)
        return out

    def _fetch_disclosure_rows(
        self,
        *,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        max_rows: int,
    ) -> list[dict]:
        out: list[dict] = []
        page_no = 1
        page_count = min(100, max(20, max_rows * 4))
        while len(out) < max_rows and page_no <= 20:
            response = self._get(
                self.disclosure_list_url,
                params={
                    "crtfc_key": self.settings.opendart_api_key,
                    "corp_code": corp_code,
                    "bgn_de": bgn_de,
                    "end_de": end_de,
                    "sort": "date",
                    "sort_mth": "desc",
                    "page_no": page_no,
                    "page_count": page_count,
                },
            )
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "")
            if status == "013":  # no data
                break
            if status and status != "000":
                message = compact_text(str(payload.get("message") or ""))
                raise RuntimeError(f"opendart list failed: status={status} message={message}")
            rows = payload.get("list") or []
            if not isinstance(rows, list) or not rows:
                break
            for row in rows:
                if isinstance(row, dict):
                    out.append(row)
                    if len(out) >= max_rows:
                        break
            total_page = int(payload.get("total_page") or 1)
            if page_no >= total_page:
                break
            page_no += 1
        return out[:max_rows]

    @staticmethod
    def _is_stale(latest_updated: str, *, refresh_days: int) -> bool:
        text = compact_text(latest_updated)
        if not text:
            return True
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return True
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
        return age > timedelta(days=max(1, int(refresh_days)))

    def _core_keywords(self) -> list[str]:
        raw = str(self.settings.opendart_core_keywords or "")
        keywords = [compact_text(x) for x in re.split(r"[,\n;]+", raw) if compact_text(x)]
        seen: set[str] = set()
        out: list[str] = []
        for item in keywords:
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out
