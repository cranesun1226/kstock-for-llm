from __future__ import annotations

import io
import json
import time
import zipfile
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET


class OpenDartError(RuntimeError):
    """Base error for OpenDART issues."""


class OpenDartNoDataError(OpenDartError):
    """Raised when OpenDART reports no data."""


class OpenDartClient:
    _BASE_URL = "https://opendart.fss.or.kr/api"
    _USER_AGENT = "kstock-for-llm/0.1"

    def __init__(self, api_key: str, min_interval: float = 0.25) -> None:
        self.api_key = api_key
        self.min_interval = min_interval
        self._last_request_at = 0.0
        self._corp_codes_cache: list[dict[str, str]] | None = None

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request_at = time.monotonic()

    def _request(self, endpoint: str, params: dict[str, Any]) -> bytes:
        self._throttle()
        url = f"{self._BASE_URL}/{endpoint}?{urlencode(params)}"
        request = Request(url, headers={"User-Agent": self._USER_AGENT})
        try:
            with urlopen(request, timeout=30) as response:
                return response.read()
        except HTTPError as exc:
            raise OpenDartError(f"OpenDART HTTP error {exc.code}: {url}") from exc
        except URLError as exc:
            raise OpenDartError(f"OpenDART request failed: {url}") from exc

    def _request_json(self, endpoint: str, **params: Any) -> dict[str, Any]:
        payload = {"crtfc_key": self.api_key, **params}
        response = self._request(endpoint, payload)
        dataset = json.loads(response.decode("utf-8"))
        self._check_status(dataset.get("status"), dataset.get("message"))
        return dataset

    def _request_zip_bytes(self, endpoint: str, **params: Any) -> bytes:
        payload = {"crtfc_key": self.api_key, **params}
        content = self._request(endpoint, payload)
        if content[:2] == b"PK":
            return content

        try:
            root = ET.fromstring(content.decode("utf-8"))
        except (UnicodeDecodeError, ET.ParseError) as exc:
            raise OpenDartError(f"Unexpected binary response from {endpoint}") from exc

        status = root.findtext("status")
        message = root.findtext("message")
        self._check_status(status, message)
        raise OpenDartError(f"{endpoint} did not return a ZIP payload.")

    @staticmethod
    def _check_status(status: str | None, message: str | None) -> None:
        if status in (None, "000"):
            return
        if status == "013":
            raise OpenDartNoDataError(message or "No data returned.")
        raise OpenDartError(f"OpenDART status={status}: {message}")

    def get_corp_codes(self) -> list[dict[str, str]]:
        if self._corp_codes_cache is not None:
            return self._corp_codes_cache

        zip_bytes = self._request_zip_bytes("corpCode.xml")
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            xml_name = next(
                (info.filename for info in zf.infolist() if info.filename.endswith(".xml")),
                None,
            )
            if xml_name is None:
                raise OpenDartError("corpCode.xml ZIP did not contain an XML file.")
            xml_bytes = zf.read(xml_name)

        root = ET.fromstring(xml_bytes)
        records: list[dict[str, str]] = []
        for node in root.findall("list"):
            records.append(
                {
                    "corp_code": (node.findtext("corp_code") or "").strip(),
                    "corp_name": (node.findtext("corp_name") or "").strip(),
                    "corp_eng_name": (node.findtext("corp_eng_name") or "").strip(),
                    "stock_code": (node.findtext("stock_code") or "").strip(),
                    "modify_date": (node.findtext("modify_date") or "").strip(),
                }
            )
        self._corp_codes_cache = records
        return records

    def resolve_company(
        self, *, stock_code: str | None = None, corp_code: str | None = None
    ) -> dict[str, str]:
        records = self.get_corp_codes()
        target = None
        if stock_code:
            target = next((item for item in records if item["stock_code"] == stock_code), None)
        elif corp_code:
            target = next((item for item in records if item["corp_code"] == corp_code), None)

        if target is None:
            needle = stock_code or corp_code or "<missing>"
            raise OpenDartError(f"Could not resolve company for identifier: {needle}")

        company_info = self.get_company_info(target["corp_code"])
        return {
            **target,
            "corp_cls": str(company_info.get("corp_cls", "")).strip(),
            "jurir_no": str(company_info.get("jurir_no", "")).strip(),
            "bizr_no": str(company_info.get("bizr_no", "")).strip(),
            "ceo_nm": str(company_info.get("ceo_nm", "")).strip(),
            "corp_name_eng": str(company_info.get("corp_name_eng", "")).strip(),
            "acc_mt": str(company_info.get("acc_mt", "")).strip() or "12",
        }

    def get_company_info(self, corp_code: str) -> dict[str, Any]:
        return self._request_json("company.json", corp_code=corp_code)

    def search_filings(
        self,
        *,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        pblntf_detail_ty: str,
        last_reprt_at: str = "Y",
        page_count: int = 100,
    ) -> list[dict[str, Any]]:
        dataset = self._request_json(
            "list.json",
            corp_code=corp_code,
            bgn_de=bgn_de,
            end_de=end_de,
            pblntf_ty="A",
            pblntf_detail_ty=pblntf_detail_ty,
            last_reprt_at=last_reprt_at,
            sort="date",
            sort_mth="desc",
            page_no=1,
            page_count=page_count,
        )
        return list(dataset.get("list", []))

    def download_document(self, rcept_no: str) -> bytes:
        return self._request_zip_bytes("document.xml", rcept_no=rcept_no)

    def download_xbrl(self, rcept_no: str, reprt_code: str = "11011") -> bytes:
        return self._request_zip_bytes(
            "fnlttXbrl.xml", rcept_no=rcept_no, reprt_code=reprt_code
        )

    def fetch_financial_statement_all(
        self, corp_code: str, business_year: int, reprt_code: str, fs_div: str
    ) -> list[dict[str, Any]]:
        dataset = self._request_json(
            "fnlttSinglAcntAll.json",
            corp_code=corp_code,
            bsns_year=str(business_year),
            reprt_code=reprt_code,
            fs_div=fs_div,
        )
        return list(dataset.get("list", []))
    @staticmethod
    def save_bytes(path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
