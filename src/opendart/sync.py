from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import OpenDartClient, OpenDartNoDataError
from .parsers import parse_sections_from_document_zip, write_sections_json
from .settings import Settings
from .storage import Database


ANNUAL_REPORT_CODE = "11011"


@dataclass(frozen=True)
class SyncResult:
    corp_code: str
    stock_code: str
    corp_name: str
    business_year: int
    rcept_no: str
    report_nm: str
    raw_document_path: Path
    raw_xbrl_path: Path | None
    sections_path: Path
    financial_facts_path: Path
    sections_count: int
    financial_facts_count: int


def _normalize_report_name(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def select_best_annual_filing(
    filings: list[dict[str, Any]], *, business_year: int, fiscal_month: str
) -> dict[str, Any]:
    if not filings:
        raise RuntimeError("No annual report filings were returned.")

    target_suffix = f"({business_year}.{int(fiscal_month):02d})"

    def score(item: dict[str, Any]) -> tuple[int, str]:
        report_nm = _normalize_report_name(str(item.get("report_nm", "")))
        value = 0
        if "사업보고서" in report_nm:
            value += 10
        if target_suffix in report_nm:
            value += 100
        if str(business_year) in report_nm:
            value += 5
        if report_nm.startswith("[기재정정]") or report_nm.startswith("[첨부정정]"):
            value -= 2
        return value, str(item.get("rcept_dt", ""))

    ranked = sorted(filings, key=score, reverse=True)
    return ranked[0]


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def sync_annual_report(
    *,
    settings: Settings,
    stock_code: str | None = None,
    corp_code: str | None = None,
    business_year: int,
) -> SyncResult:
    client = OpenDartClient(settings.api_key)
    database = Database(settings.database_path)

    try:
        issuer = client.resolve_company(stock_code=stock_code, corp_code=corp_code)
        database.upsert_issuer(issuer)

        filings = client.search_filings(
            corp_code=issuer["corp_code"],
            bgn_de=f"{business_year}0101",
            end_de=f"{business_year + 1}1231",
            pblntf_detail_ty="A001",
            last_reprt_at="Y",
        )
        filing = select_best_annual_filing(
            filings, business_year=business_year, fiscal_month=issuer["acc_mt"]
        )

        rcept_no = str(filing["rcept_no"])
        stock_code_value = issuer["stock_code"]
        raw_base_dir = settings.raw_dir / stock_code_value / str(business_year) / rcept_no
        silver_base_dir = settings.silver_dir / stock_code_value / str(business_year) / rcept_no
        raw_document_path = raw_base_dir / "document.zip"
        raw_xbrl_path = raw_base_dir / "xbrl.zip"

        document_zip = client.download_document(rcept_no)
        client.save_bytes(raw_document_path, document_zip)

        xbrl_path_for_db: str | None = None
        try:
            xbrl_zip = client.download_xbrl(rcept_no, ANNUAL_REPORT_CODE)
        except OpenDartNoDataError:
            xbrl_zip = None
        if xbrl_zip is not None:
            client.save_bytes(raw_xbrl_path, xbrl_zip)
            xbrl_path_for_db = str(raw_xbrl_path)
        else:
            raw_xbrl_path = None

        database.upsert_filing(
            {
                "rcept_no": rcept_no,
                "corp_code": issuer["corp_code"],
                "stock_code": stock_code_value,
                "corp_name": issuer["corp_name"],
                "report_nm": filing["report_nm"],
                "reprt_code": ANNUAL_REPORT_CODE,
                "rcept_dt": filing["rcept_dt"],
                "flr_nm": filing.get("flr_nm"),
                "is_final": 1,
                "raw_document_path": str(raw_document_path),
                "raw_xbrl_path": xbrl_path_for_db,
            }
        )

        sections = parse_sections_from_document_zip(document_zip)
        sections_path = silver_base_dir / "sections.json"
        write_sections_json(sections_path, sections)
        database.replace_sections(rcept_no, sections)

        all_facts: list[dict[str, Any]] = []
        facts_by_division: dict[str, list[dict[str, Any]]] = {}
        for fs_div in ("CFS", "OFS"):
            try:
                facts = client.fetch_financial_statement_all(
                    issuer["corp_code"], business_year, ANNUAL_REPORT_CODE, fs_div
                )
            except OpenDartNoDataError:
                facts = []
            facts_by_division[fs_div] = facts
            if facts:
                database.replace_financial_facts(
                    rcept_no=rcept_no,
                    corp_code=issuer["corp_code"],
                    stock_code=stock_code_value,
                    business_year=business_year,
                    reprt_code=ANNUAL_REPORT_CODE,
                    fs_div=fs_div,
                    facts=facts,
                )
                for fact in facts:
                    all_facts.append({"fs_div": fs_div, **fact})

        financial_facts_path = silver_base_dir / "financial_facts.json"
        _write_json(financial_facts_path, all_facts)

        manifest_path = silver_base_dir / "manifest.json"
        _write_json(
            manifest_path,
            {
                "corp_code": issuer["corp_code"],
                "stock_code": stock_code_value,
                "corp_name": issuer["corp_name"],
                "business_year": business_year,
                "rcept_no": rcept_no,
                "report_nm": filing["report_nm"],
                "raw_document_path": str(raw_document_path),
                "raw_xbrl_path": str(raw_xbrl_path) if raw_xbrl_path else None,
                "sections_path": str(sections_path),
                "financial_facts_path": str(financial_facts_path),
                "sections_count": len(sections),
                "financial_facts_count": len(all_facts),
            },
        )

        return SyncResult(
            corp_code=issuer["corp_code"],
            stock_code=stock_code_value,
            corp_name=issuer["corp_name"],
            business_year=business_year,
            rcept_no=rcept_no,
            report_nm=str(filing["report_nm"]),
            raw_document_path=raw_document_path,
            raw_xbrl_path=raw_xbrl_path,
            sections_path=sections_path,
            financial_facts_path=financial_facts_path,
            sections_count=len(sections),
            financial_facts_count=len(all_facts),
        )
    finally:
        database.close()
