from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import OpenDartClient, OpenDartNoDataError
from .derived import (
    build_qa_checks,
    build_section_chunks,
    PRIORITY_ARCHIVE,
    PRIORITY_CONDITIONAL,
    PRIORITY_CORE,
    summarize_chunk_priority_counts,
    summarize_qa_status,
    summarize_section_priority_counts,
    write_chunks_jsonl,
    write_qa_checks_json,
)
from .parsers import parse_sections_from_document_zip, write_sections_json
from .settings import Settings
from .storage import Database


ANNUAL_REPORT_CODE = "11011"
REPORT_PERIOD_RE = re.compile(r"\((\d{4})\.(\d{2})\)")
REPORT_KIND_SLUGS = (
    ("사업보고서", "annual-report"),
    ("반기보고서", "half-year-report"),
    ("분기보고서", "quarter-report"),
)


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
    chunks_path: Path
    core_chunks_path: Path
    conditional_chunks_path: Path
    qa_checks_path: Path
    sections_count: int
    chunks_count: int
    core_chunks_count: int
    conditional_chunks_count: int
    financial_facts_count: int
    qa_status: str


def _normalize_report_name(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def derive_report_kind_slug(report_nm: str) -> str:
    normalized = _normalize_report_name(report_nm)
    for needle, slug in REPORT_KIND_SLUGS:
        if needle in normalized:
            if normalized.startswith("[기재정정]") or normalized.startswith("[첨부정정]"):
                return f"amended-{slug}"
            return slug
    return "filing"


def build_filing_storage_slug(report_nm: str, rcept_no: str, rcept_dt: str) -> str:
    period_match = REPORT_PERIOD_RE.search(report_nm)
    fiscal_period = (
        f"{period_match.group(1)}-{period_match.group(2)}"
        if period_match is not None
        else "unknown-period"
    )
    filing_date = rcept_dt if len(rcept_dt) == 8 and rcept_dt.isdigit() else "unknown-date"
    receipt_suffix = rcept_no[-6:] if len(rcept_no) >= 6 else (rcept_no or "unknown")
    return "_".join(
        [
            derive_report_kind_slug(report_nm),
            fiscal_period,
            filing_date,
            receipt_suffix,
        ]
    )


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


def _public_path(project_root: Path, path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except (OSError, ValueError):
        return str(path)


def _file_artifact(
    *,
    rcept_no: str,
    layer: str,
    artifact_role: str,
    artifact_format: str,
    path: Path,
) -> dict[str, Any]:
    payload = path.read_bytes()
    return {
        "rcept_no": rcept_no,
        "layer": layer,
        "artifact_role": artifact_role,
        "artifact_format": artifact_format,
        "path": str(path),
        "byte_size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def sync_annual_report(
    *,
    settings: Settings,
    stock_code: str | None = None,
    corp_code: str | None = None,
    business_year: int,
) -> SyncResult:
    client = OpenDartClient(settings.api_key)
    database = Database(settings.database_path)
    sync_run_id = database.begin_sync_run(
        stock_code=stock_code,
        corp_code=corp_code,
        business_year=business_year,
    )

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
        report_kind = derive_report_kind_slug(str(filing["report_nm"]))
        filing_storage_slug = build_filing_storage_slug(
            str(filing["report_nm"]),
            rcept_no,
            str(filing.get("rcept_dt", "")),
        )
        raw_base_dir = settings.raw_dir / stock_code_value / str(business_year) / filing_storage_slug
        silver_base_dir = (
            settings.silver_dir / stock_code_value / str(business_year) / filing_storage_slug
        )
        gold_base_dir = settings.gold_dir / stock_code_value / str(business_year) / filing_storage_slug
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
                "business_year": business_year,
                "fiscal_month": issuer["acc_mt"],
                "report_nm": filing["report_nm"],
                "report_kind": report_kind,
                "reprt_code": ANNUAL_REPORT_CODE,
                "rcept_dt": filing["rcept_dt"],
                "flr_nm": filing.get("flr_nm"),
                "is_final": 1,
                "storage_key": filing_storage_slug,
                "raw_document_path": str(raw_document_path),
                "raw_xbrl_path": xbrl_path_for_db,
                "silver_base_path": str(silver_base_dir),
                "gold_base_path": str(gold_base_dir),
                "manifest_path": None,
                "sections_count": 0,
                "chunks_count": 0,
                "financial_facts_count": 0,
                "core_sections_count": 0,
                "conditional_sections_count": 0,
                "archive_sections_count": 0,
                "core_chunks_count": 0,
                "conditional_chunks_count": 0,
                "archive_chunks_count": 0,
                "qa_status": None,
            }
        )

        sections = parse_sections_from_document_zip(document_zip)
        section_priority_counts = summarize_section_priority_counts(sections)
        sections_path = silver_base_dir / "sections.json"
        write_sections_json(sections_path, sections)
        database.replace_sections(rcept_no, sections)
        chunks = build_section_chunks(sections)
        chunk_priority_counts = summarize_chunk_priority_counts(chunks)
        core_chunks = [chunk for chunk in chunks if chunk.is_retrieval_candidate]
        conditional_chunks = [chunk for chunk in chunks if chunk.is_conditional_candidate]
        chunks_path = gold_base_dir / "chunks.jsonl"
        core_chunks_path = gold_base_dir / "core_chunks.jsonl"
        conditional_chunks_path = gold_base_dir / "conditional_chunks.jsonl"
        write_chunks_jsonl(chunks_path, chunks)
        write_chunks_jsonl(core_chunks_path, core_chunks)
        write_chunks_jsonl(conditional_chunks_path, conditional_chunks)
        database.replace_section_chunks(rcept_no, chunks)

        all_facts: list[dict[str, Any]] = []
        for fs_div in ("CFS", "OFS"):
            try:
                facts = client.fetch_financial_statement_all(
                    issuer["corp_code"], business_year, ANNUAL_REPORT_CODE, fs_div
                )
            except OpenDartNoDataError:
                facts = []
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
        qa_checks = build_qa_checks(sections, all_facts, chunks)
        qa_checks_path = gold_base_dir / "qa_checks.json"
        write_qa_checks_json(qa_checks_path, qa_checks)
        database.replace_qa_checks(rcept_no, qa_checks)
        qa_status = summarize_qa_status(qa_checks)

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
                "report_kind": report_kind,
                "storage_key": filing_storage_slug,
                "canonical_db_path": _public_path(settings.project_root, settings.database_path),
                "raw_document_path": _public_path(settings.project_root, raw_document_path),
                "raw_xbrl_path": _public_path(settings.project_root, raw_xbrl_path),
                "sections_path": _public_path(settings.project_root, sections_path),
                "financial_facts_path": _public_path(settings.project_root, financial_facts_path),
                "chunks_path": _public_path(settings.project_root, chunks_path),
                "core_chunks_path": _public_path(settings.project_root, core_chunks_path),
                "conditional_chunks_path": _public_path(
                    settings.project_root, conditional_chunks_path
                ),
                "qa_checks_path": _public_path(settings.project_root, qa_checks_path),
                "sections_count": len(sections),
                "chunks_count": len(chunks),
                "core_sections_count": section_priority_counts[PRIORITY_CORE],
                "conditional_sections_count": section_priority_counts[PRIORITY_CONDITIONAL],
                "archive_sections_count": section_priority_counts[PRIORITY_ARCHIVE],
                "core_chunks_count": chunk_priority_counts[PRIORITY_CORE],
                "conditional_chunks_count": chunk_priority_counts[PRIORITY_CONDITIONAL],
                "archive_chunks_count": chunk_priority_counts[PRIORITY_ARCHIVE],
                "core_retrieval_chunks_count": len(core_chunks),
                "conditional_retrieval_chunks_count": len(conditional_chunks),
                "financial_facts_count": len(all_facts),
                "qa_status": qa_status,
                "qa_check_count": len(qa_checks),
                "default_retrieval_strategy": (
                    "Use core_chunks.jsonl by default and open "
                    "conditional_chunks.jsonl only for governance/audit/shareholder questions."
                ),
            },
        )
        database.upsert_filing(
            {
                "rcept_no": rcept_no,
                "corp_code": issuer["corp_code"],
                "stock_code": stock_code_value,
                "corp_name": issuer["corp_name"],
                "business_year": business_year,
                "fiscal_month": issuer["acc_mt"],
                "report_nm": filing["report_nm"],
                "report_kind": report_kind,
                "reprt_code": ANNUAL_REPORT_CODE,
                "rcept_dt": filing["rcept_dt"],
                "flr_nm": filing.get("flr_nm"),
                "is_final": 1,
                "storage_key": filing_storage_slug,
                "raw_document_path": str(raw_document_path),
                "raw_xbrl_path": xbrl_path_for_db,
                "silver_base_path": str(silver_base_dir),
                "gold_base_path": str(gold_base_dir),
                "manifest_path": str(manifest_path),
                "sections_count": len(sections),
                "chunks_count": len(chunks),
                "financial_facts_count": len(all_facts),
                "core_sections_count": section_priority_counts[PRIORITY_CORE],
                "conditional_sections_count": section_priority_counts[PRIORITY_CONDITIONAL],
                "archive_sections_count": section_priority_counts[PRIORITY_ARCHIVE],
                "core_chunks_count": chunk_priority_counts[PRIORITY_CORE],
                "conditional_chunks_count": chunk_priority_counts[PRIORITY_CONDITIONAL],
                "archive_chunks_count": chunk_priority_counts[PRIORITY_ARCHIVE],
                "qa_status": qa_status,
            }
        )
        artifacts = [
            _file_artifact(
                rcept_no=rcept_no,
                layer="raw",
                artifact_role="document_zip",
                artifact_format="zip",
                path=raw_document_path,
            ),
            _file_artifact(
                rcept_no=rcept_no,
                layer="silver",
                artifact_role="sections_json",
                artifact_format="json",
                path=sections_path,
            ),
            _file_artifact(
                rcept_no=rcept_no,
                layer="silver",
                artifact_role="financial_facts_json",
                artifact_format="json",
                path=financial_facts_path,
            ),
            _file_artifact(
                rcept_no=rcept_no,
                layer="silver",
                artifact_role="manifest_json",
                artifact_format="json",
                path=manifest_path,
            ),
            _file_artifact(
                rcept_no=rcept_no,
                layer="gold",
                artifact_role="chunks_jsonl",
                artifact_format="jsonl",
                path=chunks_path,
            ),
            _file_artifact(
                rcept_no=rcept_no,
                layer="gold",
                artifact_role="core_chunks_jsonl",
                artifact_format="jsonl",
                path=core_chunks_path,
            ),
            _file_artifact(
                rcept_no=rcept_no,
                layer="gold",
                artifact_role="conditional_chunks_jsonl",
                artifact_format="jsonl",
                path=conditional_chunks_path,
            ),
            _file_artifact(
                rcept_no=rcept_no,
                layer="gold",
                artifact_role="qa_checks_json",
                artifact_format="json",
                path=qa_checks_path,
            ),
        ]
        if raw_xbrl_path is not None:
            artifacts.append(
                _file_artifact(
                    rcept_no=rcept_no,
                    layer="raw",
                    artifact_role="xbrl_zip",
                    artifact_format="zip",
                    path=raw_xbrl_path,
                )
            )
        database.replace_filing_artifacts(rcept_no, artifacts)
        database.finish_sync_run(
            sync_run_id,
            status="success",
            stock_code=stock_code_value,
            corp_code=issuer["corp_code"],
            rcept_no=rcept_no,
            report_nm=str(filing["report_nm"]),
            message=(
                f"sections={len(sections)}, chunks={len(chunks)}, "
                f"core_chunks={len(core_chunks)}, conditional_chunks={len(conditional_chunks)}, "
                f"financial_facts={len(all_facts)}, qa_status={qa_status}"
            ),
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
            chunks_path=chunks_path,
            core_chunks_path=core_chunks_path,
            conditional_chunks_path=conditional_chunks_path,
            qa_checks_path=qa_checks_path,
            sections_count=len(sections),
            chunks_count=len(chunks),
            core_chunks_count=len(core_chunks),
            conditional_chunks_count=len(conditional_chunks),
            financial_facts_count=len(all_facts),
            qa_status=qa_status,
        )
    except Exception as exc:
        database.finish_sync_run(
            sync_run_id,
            status="failed",
            stock_code=stock_code,
            corp_code=corp_code,
            message=str(exc),
        )
        raise
    finally:
        database.close()
