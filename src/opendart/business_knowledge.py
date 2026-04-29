from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .client import OpenDartClient, OpenDartNoDataError
from .parsers import Section, parse_sections_from_document_zip


ANNUAL_REPORT_DETAIL_TYPE = "A001"
BUSINESS_SECTION_TITLE = "II. 사업의 내용"
DART_VIEWER_BASE_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo="
DEFAULT_LOOKBACK_DAYS = 455
DEFAULT_MAX_CHARS_PER_FILE = 5_000_000
DEFAULT_MAX_FILES = 20
DEFAULT_WINDOW_DAYS = 90
FAILURES_PARTIAL_JSONL_NAME = "failures.partial.jsonl"
INVENTORY_JSON_NAME = "inventory.json"
PROGRESS_JSON_NAME = "progress.json"
SECTIONS_PARTIAL_JSONL_NAME = "business_sections.partial.jsonl"
MARKET_NAMES = {
    "Y": "KOSPI",
    "K": "KOSDAQ",
}
MARKET_SORT_ORDER = {
    "Y": 0,
    "K": 1,
}
REPORT_PERIOD_RE = re.compile(r"\((\d{4})\.(\d{2})\)")
WHITESPACE_RE = re.compile(r"\s+")
UNICODE_ROMAN_TRANSLATION = str.maketrans(
    {
        "Ⅰ": "I",
        "Ⅱ": "II",
        "Ⅲ": "III",
        "Ⅳ": "IV",
        "Ⅴ": "V",
        "Ⅵ": "VI",
        "Ⅶ": "VII",
        "Ⅷ": "VIII",
        "Ⅸ": "IX",
        "Ⅹ": "X",
    }
)

ProgressLogger = Callable[[str], None]


@dataclass(frozen=True)
class ReportPeriod:
    year: int
    month: int

    @property
    def label(self) -> str:
        return f"{self.year}.{self.month:02d}"

    def sort_key(self) -> tuple[int, int]:
        return self.year, self.month


@dataclass(frozen=True)
class BusinessFilingCandidate:
    corp_cls: str
    corp_code: str
    stock_code: str
    corp_name: str
    report_nm: str
    rcept_no: str
    rcept_dt: str
    flr_nm: str | None = None
    rm: str | None = None
    source_window_start: str | None = None
    source_window_end: str | None = None

    @classmethod
    def from_api_row(
        cls, row: dict[str, Any], *, window_start: str | None = None, window_end: str | None = None
    ) -> BusinessFilingCandidate:
        return cls(
            corp_cls=str(row.get("corp_cls") or "").strip(),
            corp_code=str(row.get("corp_code") or "").strip(),
            stock_code=str(row.get("stock_code") or "").strip(),
            corp_name=str(row.get("corp_name") or "").strip(),
            report_nm=str(row.get("report_nm") or "").strip(),
            rcept_no=str(row.get("rcept_no") or "").strip(),
            rcept_dt=str(row.get("rcept_dt") or "").strip(),
            flr_nm=(str(row.get("flr_nm")).strip() if row.get("flr_nm") is not None else None),
            rm=(str(row.get("rm")).strip() if row.get("rm") is not None else None),
            source_window_start=window_start,
            source_window_end=window_end,
        )

    @property
    def market_name(self) -> str:
        return MARKET_NAMES.get(self.corp_cls, self.corp_cls or "UNKNOWN")

    @property
    def report_period(self) -> ReportPeriod | None:
        return parse_report_period(self.report_nm)

    @property
    def dart_url(self) -> str:
        return f"{DART_VIEWER_BASE_URL}{self.rcept_no}"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        period = self.report_period
        payload["market_name"] = self.market_name
        payload["report_period"] = period.label if period else None
        payload["dart_url"] = self.dart_url
        return payload


@dataclass(frozen=True)
class BusinessKnowledgeDocument:
    candidate: BusinessFilingCandidate
    sections: list[Section]
    all_sections_count: int
    selected_candidate_attempts: list[dict[str, Any]]

    @property
    def business_char_count(self) -> int:
        return sum(len(section.body) for section in self.sections)

    @property
    def business_section_count(self) -> int:
        return len(self.sections)

    def to_summary_dict(self) -> dict[str, Any]:
        candidate = self.candidate.to_dict()
        return {
            **candidate,
            "all_sections_count": self.all_sections_count,
            "business_sections_count": self.business_section_count,
            "business_char_count": self.business_char_count,
        }


@dataclass(frozen=True)
class BusinessKnowledgeFailure:
    stock_code: str
    corp_code: str
    corp_name: str
    corp_cls: str
    candidate_count: int
    attempts: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WrittenKnowledgeArtifacts:
    output_dir: Path
    markdown_paths: list[Path]
    jsonl_path: Path
    failures_path: Path
    manifest_path: Path
    progress_path: Path
    inventory_path: Path | None
    partial_jsonl_path: Path
    partial_failures_path: Path
    selected_count: int
    failure_count: int


@dataclass(frozen=True)
class BusinessKnowledgeBuildResult:
    candidates_count: int
    stock_count: int
    documents: list[BusinessKnowledgeDocument]
    failures: list[BusinessKnowledgeFailure]
    artifacts: WrittenKnowledgeArtifacts


def parse_report_period(report_nm: str) -> ReportPeriod | None:
    match = REPORT_PERIOD_RE.search(report_nm or "")
    if match is None:
        return None
    return ReportPeriod(year=int(match.group(1)), month=int(match.group(2)))


def is_annual_report_name(report_nm: str) -> bool:
    return "사업보고서" in (report_nm or "")


def normalize_heading_text(value: str) -> str:
    translated = (value or "").translate(UNICODE_ROMAN_TRANSLATION)
    return WHITESPACE_RE.sub(" ", translated.replace("\u00a0", " ")).strip()


def is_business_content_heading_path(heading_path: str) -> bool:
    top_level = normalize_heading_text((heading_path or "").split(" > ")[0])
    compact = top_level.replace(" ", "").replace("．", ".")
    return compact.startswith("II.") and "사업의내용" in compact


def filter_business_content_sections(sections: Iterable[Section]) -> list[Section]:
    return [
        section
        for section in sections
        if is_business_content_heading_path(section.heading_path) and section.body.strip()
    ]


def iter_date_windows(
    start_date: date, end_date: date, *, window_days: int = DEFAULT_WINDOW_DAYS
) -> Iterable[tuple[date, date]]:
    if window_days < 1:
        raise ValueError("window_days must be at least 1.")
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")

    cursor = start_date
    while cursor <= end_date:
        window_end = min(end_date, cursor + timedelta(days=window_days - 1))
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def compact_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def iso_date(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def default_start_date(end_date: date, *, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> date:
    return end_date - timedelta(days=lookback_days - 1)


def collect_market_annual_filing_candidates(
    client: OpenDartClient,
    *,
    markets: list[str],
    start_date: date,
    end_date: date,
    last_reprt_at: str = "N",
    page_count: int = 100,
    window_days: int = DEFAULT_WINDOW_DAYS,
    progress: ProgressLogger | None = None,
) -> list[BusinessFilingCandidate]:
    candidates: dict[tuple[str, str], BusinessFilingCandidate] = {}
    for market in markets:
        for window_start, window_end in iter_date_windows(
            start_date, end_date, window_days=window_days
        ):
            _emit(
                progress,
                (
                    f"[inventory] {MARKET_NAMES.get(market, market)} "
                    f"{window_start.isoformat()}~{window_end.isoformat()} start"
                ),
            )
            page_no = 1
            total_page = 1
            while page_no <= total_page:
                try:
                    dataset = client.search_filings_page(
                        bgn_de=compact_date(window_start),
                        end_de=compact_date(window_end),
                        pblntf_detail_ty=ANNUAL_REPORT_DETAIL_TYPE,
                        last_reprt_at=last_reprt_at,
                        corp_cls=market,
                        page_no=page_no,
                        page_count=page_count,
                    )
                except OpenDartNoDataError:
                    _emit(
                        progress,
                        (
                            f"[inventory] {MARKET_NAMES.get(market, market)} "
                            f"{window_start.isoformat()}~{window_end.isoformat()} no data"
                        ),
                    )
                    break

                total_page = int(dataset.get("total_page") or 1)
                page_added = 0
                for row in dataset.get("list", []):
                    if not is_annual_report_name(str(row.get("report_nm") or "")):
                        continue
                    candidate = BusinessFilingCandidate.from_api_row(
                        row,
                        window_start=compact_date(window_start),
                        window_end=compact_date(window_end),
                    )
                    if not candidate.stock_code or not candidate.rcept_no:
                        continue
                    candidates[(candidate.stock_code, candidate.rcept_no)] = candidate
                    page_added += 1

                _emit(
                    progress,
                    (
                        f"[inventory] {MARKET_NAMES.get(market, market)} "
                        f"{window_start.isoformat()}~{window_end.isoformat()} "
                        f"page {page_no}/{total_page} annual_rows={page_added} "
                        f"unique_candidates={len(candidates)}"
                    ),
                )

                page_no += 1

    return sorted(
        candidates.values(),
        key=lambda item: (
            _market_sort_key(item.corp_cls),
            item.stock_code,
            item.report_period.sort_key() if item.report_period else (0, 0),
            item.rcept_dt,
            item.rcept_no,
        ),
    )


def group_candidates_by_stock(
    candidates: Iterable[BusinessFilingCandidate],
) -> dict[str, list[BusinessFilingCandidate]]:
    grouped: dict[str, list[BusinessFilingCandidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.stock_code, []).append(candidate)
    return grouped


def rank_filing_candidates(
    candidates: list[BusinessFilingCandidate], *, business_year: int | None = None
) -> list[BusinessFilingCandidate]:
    candidates_with_period = [candidate for candidate in candidates if candidate.report_period]
    if business_year is not None:
        primary = [
            candidate
            for candidate in candidates_with_period
            if candidate.report_period and candidate.report_period.year == business_year
        ]
    elif candidates_with_period:
        latest_period = max(
            candidate.report_period.sort_key()
            for candidate in candidates_with_period
            if candidate.report_period is not None
        )
        primary = [
            candidate
            for candidate in candidates_with_period
            if candidate.report_period and candidate.report_period.sort_key() == latest_period
        ]
    else:
        primary = list(candidates)

    if not primary:
        primary = list(candidates)

    primary_keys = {candidate.rcept_no for candidate in primary}
    fallback = [candidate for candidate in candidates if candidate.rcept_no not in primary_keys]
    return sorted(primary, key=_candidate_try_key, reverse=True) + sorted(
        fallback, key=_candidate_overall_key, reverse=True
    )


def select_business_documents(
    client: OpenDartClient,
    candidates: list[BusinessFilingCandidate],
    *,
    business_year: int | None = None,
    limit: int | None = None,
    output_dir: Path | None = None,
    checkpoint_every: int = 1,
    progress: ProgressLogger | None = None,
) -> tuple[list[BusinessKnowledgeDocument], list[BusinessKnowledgeFailure]]:
    if checkpoint_every < 1:
        raise ValueError("checkpoint_every must be at least 1.")

    documents: list[BusinessKnowledgeDocument] = []
    failures: list[BusinessKnowledgeFailure] = []
    grouped = group_candidates_by_stock(candidates)
    stock_codes = sorted(
        grouped,
        key=lambda stock_code: (
            _market_sort_key(grouped[stock_code][0].corp_cls),
            stock_code,
        ),
    )
    if limit is not None:
        stock_codes = stock_codes[:limit]

    checkpoint_paths = initialize_checkpoint_files(output_dir) if output_dir else None
    started_at = datetime.now(timezone.utc).isoformat()
    total_count = len(stock_codes)
    if checkpoint_paths:
        write_progress_checkpoint(
            checkpoint_paths["progress"],
            status="running",
            started_at=started_at,
            processed_count=0,
            total_count=total_count,
            selected_count=0,
            failure_count=0,
            current=None,
        )

    _emit(progress, f"[process] stock_codes={total_count} checkpoint_every={checkpoint_every}")

    for processed_count, stock_code in enumerate(stock_codes, start=1):
        stock_candidates = rank_filing_candidates(grouped[stock_code], business_year=business_year)
        attempts: list[dict[str, Any]] = []
        selected: BusinessKnowledgeDocument | None = None
        first_candidate = stock_candidates[0]
        _emit(
            progress,
            (
                f"[process] {processed_count}/{total_count} {stock_code} "
                f"{first_candidate.corp_name} candidates={len(stock_candidates)}"
            ),
        )
        for candidate in stock_candidates:
            attempt = {
                "rcept_no": candidate.rcept_no,
                "report_nm": candidate.report_nm,
                "rcept_dt": candidate.rcept_dt,
            }
            _emit(
                progress,
                (
                    f"[download] {processed_count}/{total_count} {candidate.stock_code} "
                    f"{candidate.corp_name} rcept_no={candidate.rcept_no} "
                    f"report={candidate.report_nm}"
                ),
            )
            try:
                payload = client.download_document(candidate.rcept_no)
                all_sections = parse_sections_from_document_zip(payload)
                business_sections = filter_business_content_sections(all_sections)
                attempt.update(
                    {
                        "status": "ok",
                        "all_sections_count": len(all_sections),
                        "business_sections_count": len(business_sections),
                    }
                )
                attempts.append(attempt)
                if business_sections:
                    business_char_count = sum(len(section.body) for section in business_sections)
                    selected = BusinessKnowledgeDocument(
                        candidate=candidate,
                        sections=business_sections,
                        all_sections_count=len(all_sections),
                        selected_candidate_attempts=attempts,
                    )
                    _emit(
                        progress,
                        (
                            f"[ok] {processed_count}/{total_count} {candidate.stock_code} "
                            f"{candidate.corp_name} business_sections={len(business_sections)} "
                            f"chars={business_char_count}"
                        ),
                    )
                    break
                _emit(
                    progress,
                    (
                        f"[warn] {processed_count}/{total_count} {candidate.stock_code} "
                        f"{candidate.corp_name} no II. business sections in {candidate.rcept_no}"
                    ),
                )
            except Exception as exc:  # noqa: BLE001 - capture per-company failures for batch output
                attempt.update(
                    {
                        "status": "error",
                        "error_type": type(exc).__name__,
                        "message": _redact_error_message(client, exc),
                    }
                )
                attempts.append(attempt)
                _emit(
                    progress,
                    (
                        f"[error] {processed_count}/{total_count} {candidate.stock_code} "
                        f"{candidate.corp_name} {type(exc).__name__}: "
                        f"{attempt['message']}"
                    ),
                )

        if selected is not None:
            documents.append(selected)
            if checkpoint_paths:
                append_business_document_jsonl(checkpoint_paths["sections_partial"], selected)
                if processed_count % checkpoint_every == 0:
                    write_progress_checkpoint(
                        checkpoint_paths["progress"],
                        status="running",
                        started_at=started_at,
                        processed_count=processed_count,
                        total_count=total_count,
                        selected_count=len(documents),
                        failure_count=len(failures),
                        current=selected.candidate.to_dict(),
                    )
            continue

        failure = BusinessKnowledgeFailure(
            stock_code=first_candidate.stock_code,
            corp_code=first_candidate.corp_code,
            corp_name=first_candidate.corp_name,
            corp_cls=first_candidate.corp_cls,
            candidate_count=len(stock_candidates),
            attempts=attempts,
        )
        failures.append(failure)
        _emit(
            progress,
            (
                f"[fail] {processed_count}/{total_count} {first_candidate.stock_code} "
                f"{first_candidate.corp_name} attempts={len(attempts)}"
            ),
        )
        if checkpoint_paths:
            append_failure_jsonl(checkpoint_paths["failures_partial"], failure)
            if processed_count % checkpoint_every == 0:
                write_progress_checkpoint(
                    checkpoint_paths["progress"],
                    status="running",
                    started_at=started_at,
                    processed_count=processed_count,
                    total_count=total_count,
                    selected_count=len(documents),
                    failure_count=len(failures),
                    current=first_candidate.to_dict(),
                )

    if checkpoint_paths:
        write_progress_checkpoint(
            checkpoint_paths["progress"],
            status="selection_completed",
            started_at=started_at,
            processed_count=total_count,
            total_count=total_count,
            selected_count=len(documents),
            failure_count=len(failures),
            current=None,
        )
    _emit(
        progress,
        (
            f"[process] completed selected={len(documents)} failures={len(failures)} "
            f"processed={total_count}"
        ),
    )

    return documents, failures


def initialize_checkpoint_files(output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "progress": output_dir / PROGRESS_JSON_NAME,
        "sections_partial": output_dir / SECTIONS_PARTIAL_JSONL_NAME,
        "failures_partial": output_dir / FAILURES_PARTIAL_JSONL_NAME,
    }
    paths["sections_partial"].write_text("", encoding="utf-8")
    paths["failures_partial"].write_text("", encoding="utf-8")
    return paths


def write_progress_checkpoint(
    path: Path,
    *,
    status: str,
    started_at: str,
    processed_count: int,
    total_count: int,
    selected_count: int,
    failure_count: int,
    current: dict[str, Any] | None,
) -> None:
    payload = {
        "status": status,
        "started_at": started_at,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "processed_count": processed_count,
        "total_count": total_count,
        "remaining_count": max(0, total_count - processed_count),
        "selected_count": selected_count,
        "failure_count": failure_count,
        "current": current,
    }
    write_json_atomic(path, payload)


def append_business_document_jsonl(path: Path, document: BusinessKnowledgeDocument) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for payload in iter_business_section_payloads(document):
            handle.write(json.dumps(payload, ensure_ascii=False))
            handle.write("\n")


def append_failure_jsonl(path: Path, failure: BusinessKnowledgeFailure) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(failure.to_dict(), ensure_ascii=False))
        handle.write("\n")


def iter_business_section_payloads(
    document: BusinessKnowledgeDocument,
) -> Iterable[dict[str, Any]]:
    candidate_payload = document.candidate.to_dict()
    for section in document.sections:
        yield {
            **candidate_payload,
            "section_ordinal": section.ordinal,
            "heading_path": section.heading_path,
            "heading": section.heading,
            "source_tag": section.source_tag,
            "text": section.body.strip(),
            "char_count": len(section.body.strip()),
        }


def write_business_knowledge_outputs(
    *,
    output_dir: Path,
    documents: list[BusinessKnowledgeDocument],
    failures: list[BusinessKnowledgeFailure],
    candidates_count: int,
    stock_count: int,
    markets: list[str],
    start_date: date,
    end_date: date,
    business_year: int | None = None,
    max_chars_per_file: int = DEFAULT_MAX_CHARS_PER_FILE,
    max_files: int = DEFAULT_MAX_FILES,
    inventory_path: Path | None = None,
) -> WrittenKnowledgeArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    rendered_documents = [(document, render_business_document_markdown(document)) for document in documents]
    shards = split_rendered_documents(
        rendered_documents,
        max_chars_per_file=max_chars_per_file,
        max_files=max_files,
    )

    markdown_paths: list[Path] = []
    shard_summaries: list[dict[str, Any]] = []
    for index, shard in enumerate(shards, start=1):
        path = output_dir / f"business_sections_{index:03d}.md"
        body = "".join(rendered for _, rendered in shard)
        header = render_shard_header(
            shard_index=index,
            shard_count=len(shards),
            document_count=len(shard),
            markets=markets,
            start_date=start_date,
            end_date=end_date,
            business_year=business_year,
        )
        path.write_text(header + body, encoding="utf-8")
        markdown_paths.append(path)
        shard_summaries.append(file_summary(path) | {"document_count": len(shard)})

    jsonl_path = output_dir / "business_sections.jsonl"
    write_business_sections_jsonl(jsonl_path, documents)

    failures_path = output_dir / "failures.jsonl"
    write_failures_jsonl(failures_path, failures)

    progress_path = output_dir / PROGRESS_JSON_NAME
    partial_jsonl_path = output_dir / SECTIONS_PARTIAL_JSONL_NAME
    partial_failures_path = output_dir / FAILURES_PARTIAL_JSONL_NAME
    manifest_path = output_dir / "manifest.json"
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "OpenDART document.xml",
        "scope": "KOSPI/KOSDAQ annual reports, II. 사업의 내용 only",
        "markets": [{"code": market, "name": MARKET_NAMES.get(market, market)} for market in markets],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "business_year": business_year,
        "candidates_count": candidates_count,
        "stock_count": stock_count,
        "selected_count": len(documents),
        "failure_count": len(failures),
        "business_sections_count": sum(document.business_section_count for document in documents),
        "business_char_count": sum(document.business_char_count for document in documents),
        "max_chars_per_file": max_chars_per_file,
        "max_files": max_files,
        "markdown_files": shard_summaries,
        "jsonl_file": file_summary(jsonl_path),
        "failures_file": file_summary(failures_path),
        "inventory_file": optional_file_summary(inventory_path),
        "progress_file": optional_file_summary(progress_path),
        "partial_jsonl_file": optional_file_summary(partial_jsonl_path),
        "partial_failures_file": optional_file_summary(partial_failures_path),
        "documents": [document.to_summary_dict() for document in documents],
        "failures": [failure.to_dict() for failure in failures],
    }
    write_json_atomic(manifest_path, manifest)

    return WrittenKnowledgeArtifacts(
        output_dir=output_dir,
        markdown_paths=markdown_paths,
        jsonl_path=jsonl_path,
        failures_path=failures_path,
        manifest_path=manifest_path,
        progress_path=progress_path,
        inventory_path=inventory_path,
        partial_jsonl_path=partial_jsonl_path,
        partial_failures_path=partial_failures_path,
        selected_count=len(documents),
        failure_count=len(failures),
    )


def build_business_knowledge(
    *,
    settings_data_dir: Path,
    api_key: str,
    markets: list[str],
    start_date: date,
    end_date: date,
    output_dir: Path | None = None,
    business_year: int | None = None,
    limit: int | None = None,
    last_reprt_at: str = "N",
    max_chars_per_file: int = DEFAULT_MAX_CHARS_PER_FILE,
    max_files: int = DEFAULT_MAX_FILES,
    checkpoint_every: int = 1,
    progress: ProgressLogger | None = None,
) -> BusinessKnowledgeBuildResult:
    client = OpenDartClient(api_key)
    destination = output_dir or default_output_dir(settings_data_dir)
    destination.mkdir(parents=True, exist_ok=True)

    _emit(
        progress,
        (
            f"[start] output_dir={destination} markets={','.join(markets)} "
            f"period={start_date.isoformat()}~{end_date.isoformat()} "
            f"business_year={business_year or 'latest'}"
        ),
    )
    candidates = collect_market_annual_filing_candidates(
        client,
        markets=markets,
        start_date=start_date,
        end_date=end_date,
        last_reprt_at=last_reprt_at,
        progress=progress,
    )
    stock_count = len(group_candidates_by_stock(candidates))
    inventory_path = write_inventory_snapshot(
        destination / INVENTORY_JSON_NAME,
        candidates=candidates,
        markets=markets,
        start_date=start_date,
        end_date=end_date,
        business_year=business_year,
        stock_count=stock_count,
    )
    _emit(
        progress,
        (
            f"[inventory] saved candidates={len(candidates)} "
            f"stock_codes={stock_count} path={inventory_path}"
        ),
    )
    documents, failures = select_business_documents(
        client,
        candidates,
        business_year=business_year,
        limit=limit,
        output_dir=destination,
        checkpoint_every=checkpoint_every,
        progress=progress,
    )
    artifacts = write_business_knowledge_outputs(
        output_dir=destination,
        documents=documents,
        failures=failures,
        candidates_count=len(candidates),
        stock_count=stock_count,
        markets=markets,
        start_date=start_date,
        end_date=end_date,
        business_year=business_year,
        max_chars_per_file=max_chars_per_file,
        max_files=max_files,
        inventory_path=inventory_path,
    )
    write_completed_progress(
        artifacts.progress_path,
        total_count=min(stock_count, limit) if limit is not None else stock_count,
        selected_count=len(documents),
        failure_count=len(failures),
        manifest_path=artifacts.manifest_path,
        markdown_paths=artifacts.markdown_paths,
    )
    _emit(
        progress,
        (
            f"[done] selected={len(documents)} failures={len(failures)} "
            f"markdown_files={len(artifacts.markdown_paths)} manifest={artifacts.manifest_path}"
        ),
    )
    return BusinessKnowledgeBuildResult(
        candidates_count=len(candidates),
        stock_count=stock_count,
        documents=documents,
        failures=failures,
        artifacts=artifacts,
    )


def default_output_dir(data_dir: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d")
    return data_dir / "gold" / "business_knowledge" / stamp


def split_rendered_documents(
    rendered_documents: list[tuple[BusinessKnowledgeDocument, str]],
    *,
    max_chars_per_file: int,
    max_files: int,
) -> list[list[tuple[BusinessKnowledgeDocument, str]]]:
    if max_chars_per_file < 1:
        raise ValueError("max_chars_per_file must be at least 1.")
    if max_files < 1:
        raise ValueError("max_files must be at least 1.")

    shards: list[list[tuple[BusinessKnowledgeDocument, str]]] = []
    current: list[tuple[BusinessKnowledgeDocument, str]] = []
    current_chars = 0
    for item in rendered_documents:
        _, rendered = item
        if current and current_chars + len(rendered) > max_chars_per_file:
            shards.append(current)
            current = []
            current_chars = 0
        current.append(item)
        current_chars += len(rendered)

    if current:
        shards.append(current)
    if not shards:
        shards.append([])
    if len(shards) > max_files:
        estimated_needed = len(shards)
        raise ValueError(
            f"Knowledge output would require {estimated_needed} markdown files, "
            f"but max_files={max_files}. Increase --max-files or --max-chars-per-file."
        )
    return shards


def render_shard_header(
    *,
    shard_index: int,
    shard_count: int,
    document_count: int,
    markets: list[str],
    start_date: date,
    end_date: date,
    business_year: int | None,
) -> str:
    market_names = ", ".join(MARKET_NAMES.get(market, market) for market in markets)
    business_year_line = (
        f"대상 사업연도: {business_year}\n" if business_year is not None else "대상 사업연도: 최신 사업보고서\n"
    )
    return (
        "# 한국 상장사 사업의 내용 지식 베이스\n\n"
        f"파일: {shard_index}/{shard_count}\n"
        f"포함 회사 수: {document_count}\n"
        f"시장: {market_names}\n"
        f"{business_year_line}"
        f"공시 검색 기간: {start_date.isoformat()} ~ {end_date.isoformat()}\n"
        "범위: 각 회사 사업보고서의 `II. 사업의 내용` 섹션만 포함\n"
        "출처: OpenDART 공시서류원본파일(document.xml)\n\n"
    )


def render_business_document_markdown(document: BusinessKnowledgeDocument) -> str:
    candidate = document.candidate
    period = candidate.report_period.label if candidate.report_period else "unknown"
    lines = [
        f"## {candidate.stock_code} {candidate.corp_name} | {candidate.market_name} | {candidate.report_nm}",
        "",
        f"- 종목코드: {candidate.stock_code}",
        f"- 회사명: {candidate.corp_name}",
        f"- 시장: {candidate.market_name}",
        f"- 사업보고서 기간: {period}",
        f"- 보고서명: {candidate.report_nm}",
        f"- DART 접수번호: {candidate.rcept_no}",
        f"- 접수일: {iso_date(candidate.rcept_dt)}",
        f"- 원문: {candidate.dart_url}",
        "",
    ]
    for section in document.sections:
        lines.append(f"### {section.heading_path}")
        lines.append("")
        lines.append(section.body.strip())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n\n"


def write_business_sections_jsonl(path: Path, documents: list[BusinessKnowledgeDocument]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for document in documents:
            for payload in iter_business_section_payloads(document):
                handle.write(json.dumps(payload, ensure_ascii=False))
                handle.write("\n")


def write_failures_jsonl(path: Path, failures: list[BusinessKnowledgeFailure]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for failure in failures:
            handle.write(json.dumps(failure.to_dict(), ensure_ascii=False))
            handle.write("\n")


def write_inventory_snapshot(
    path: Path,
    *,
    candidates: list[BusinessFilingCandidate],
    markets: list[str],
    start_date: date,
    end_date: date,
    business_year: int | None,
    stock_count: int,
) -> Path:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "markets": [{"code": market, "name": MARKET_NAMES.get(market, market)} for market in markets],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "business_year": business_year,
        "candidates_count": len(candidates),
        "stock_count": stock_count,
        "candidates": [candidate.to_dict() for candidate in candidates],
    }
    write_json_atomic(path, payload)
    return path


def write_completed_progress(
    path: Path,
    *,
    total_count: int,
    selected_count: int,
    failure_count: int,
    manifest_path: Path,
    markdown_paths: list[Path],
) -> None:
    existing = read_json_object(path)
    payload = {
        **existing,
        "status": "completed",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "processed_count": total_count,
        "total_count": total_count,
        "remaining_count": 0,
        "selected_count": selected_count,
        "failure_count": failure_count,
        "manifest_path": public_path(manifest_path),
        "markdown_paths": [public_path(path) for path in markdown_paths],
        "current": None,
    }
    write_json_atomic(path, payload)


def read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def public_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except (OSError, ValueError):
        return str(path)


def file_summary(path: Path) -> dict[str, Any]:
    payload = path.read_bytes()
    try:
        char_count = len(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        char_count = None
    return {
        "path": public_path(path),
        "byte_size": len(payload),
        "char_count": char_count,
        "token_estimate": math.ceil((char_count or 0) / 4),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def optional_file_summary(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return file_summary(path)


def parse_market_codes(value: str) -> list[str]:
    markets: list[str] = []
    for raw_part in (value or "").split(","):
        part = raw_part.strip().upper()
        if not part:
            continue
        if part in {"KOSPI", "Y"}:
            code = "Y"
        elif part in {"KOSDAQ", "K"}:
            code = "K"
        else:
            raise ValueError(f"Unsupported market code: {raw_part}")
        if code not in markets:
            markets.append(code)
    if not markets:
        raise ValueError("At least one market must be specified.")
    return markets


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _emit(progress: ProgressLogger | None, message: str) -> None:
    if progress is not None:
        progress(message)


def _candidate_try_key(candidate: BusinessFilingCandidate) -> tuple[int, str, str]:
    return _amendment_priority(candidate.report_nm), candidate.rcept_dt, candidate.rcept_no


def _market_sort_key(corp_cls: str) -> int:
    return MARKET_SORT_ORDER.get(corp_cls, 99)


def _candidate_overall_key(candidate: BusinessFilingCandidate) -> tuple[tuple[int, int], int, str, str]:
    period = candidate.report_period.sort_key() if candidate.report_period else (0, 0)
    return period, _amendment_priority(candidate.report_nm), candidate.rcept_dt, candidate.rcept_no


def _amendment_priority(report_nm: str) -> int:
    normalized = WHITESPACE_RE.sub(" ", report_nm or "").strip()
    if normalized.startswith("[기재정정]"):
        return 3
    if normalized.startswith("[첨부정정]"):
        return 1
    return 2


def _redact_error_message(client: OpenDartClient, exc: Exception) -> str:
    message = str(exc)
    api_key = getattr(client, "api_key", "")
    if api_key:
        message = message.replace(api_key, "<redacted>")
    return message[:500]
