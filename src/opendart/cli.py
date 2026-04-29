from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from .business_knowledge import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_CHARS_PER_FILE,
    DEFAULT_MAX_FILES,
    build_business_knowledge,
    default_start_date,
    parse_iso_date,
    parse_market_codes,
)
from .settings import load_settings
from .sync import sync_annual_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="opendart",
        description="Sync Korean listed company annual reports from OpenDART.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_report = subparsers.add_parser(
        "sync-report",
        help="Sync a single company's annual report for one business year.",
    )
    identity_group = sync_report.add_mutually_exclusive_group(required=True)
    identity_group.add_argument("--stock-code", help="Six-digit stock code, e.g. 005930.")
    identity_group.add_argument("--corp-code", help="Eight-digit OpenDART corp code.")
    sync_report.add_argument("--year", type=int, required=True, help="Business year, e.g. 2025.")

    business_knowledge = subparsers.add_parser(
        "build-business-knowledge",
        help="Build GPT knowledge files from II. 사업의 내용 for listed KOSPI/KOSDAQ issuers.",
    )
    business_knowledge.add_argument(
        "--markets",
        default="Y,K",
        help="Comma-separated markets: Y/K or KOSPI/KOSDAQ. Default: Y,K.",
    )
    business_knowledge.add_argument(
        "--start-date",
        help="Disclosure search start date in YYYY-MM-DD. Default: end-date minus lookback-days.",
    )
    business_knowledge.add_argument(
        "--end-date",
        help="Disclosure search end date in YYYY-MM-DD. Default: today.",
    )
    business_knowledge.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Search lookback when --start-date is omitted. Default: {DEFAULT_LOOKBACK_DAYS}.",
    )
    business_knowledge.add_argument(
        "--business-year",
        type=int,
        help="Prefer reports whose report period year matches this value, e.g. 2025.",
    )
    business_knowledge.add_argument(
        "--output-dir",
        help="Output directory. Default: data/gold/business_knowledge/YYYYMMDD.",
    )
    business_knowledge.add_argument(
        "--limit",
        type=int,
        help="Process only the first N stock codes after inventory collection.",
    )
    business_knowledge.add_argument(
        "--last-reprt-at",
        choices=("Y", "N"),
        default="N",
        help="OpenDART last_reprt_at value for inventory. N keeps correction candidates. Default: N.",
    )
    business_knowledge.add_argument(
        "--max-chars-per-file",
        type=int,
        default=DEFAULT_MAX_CHARS_PER_FILE,
        help=(
            "Maximum approximate characters per Markdown knowledge shard. "
            f"Default: {DEFAULT_MAX_CHARS_PER_FILE}."
        ),
    )
    business_knowledge.add_argument(
        "--max-files",
        type=int,
        default=DEFAULT_MAX_FILES,
        help=f"Maximum Markdown knowledge shard count. Default: {DEFAULT_MAX_FILES}.",
    )
    business_knowledge.add_argument(
        "--checkpoint-every",
        type=int,
        default=1,
        help=(
            "Update progress.json every N processed companies. "
            "Partial JSONL files are appended after every completed company. Default: 1."
        ),
    )
    business_knowledge.add_argument(
        "--quiet",
        action="store_true",
        help="Disable progress logs. Intermediate progress files are still written.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_settings()

    if args.command == "sync-report":
        result = sync_annual_report(
            settings=settings,
            stock_code=args.stock_code,
            corp_code=args.corp_code,
            business_year=args.year,
        )
        print(f"Synced {result.corp_name} ({result.stock_code}) / {result.business_year}")
        print(f"rcept_no: {result.rcept_no}")
        print(f"report_nm: {result.report_nm}")
        print(f"document_zip: {result.raw_document_path}")
        print(f"xbrl_zip: {result.raw_xbrl_path or '<not available>'}")
        print(f"sections_json: {result.sections_path} ({result.sections_count} sections)")
        print(f"chunks_jsonl: {result.chunks_path} ({result.chunks_count} chunks)")
        print(f"core_chunks_jsonl: {result.core_chunks_path} ({result.core_chunks_count} chunks)")
        print(
            "conditional_chunks_jsonl: "
            f"{result.conditional_chunks_path} ({result.conditional_chunks_count} chunks)"
        )
        print(
            f"financial_facts_json: {result.financial_facts_path} "
            f"({result.financial_facts_count} facts)"
        )
        print(f"qa_checks_json: {result.qa_checks_path} (qa_status={result.qa_status})")
        return 0

    if args.command == "build-business-knowledge":
        end_date = parse_iso_date(args.end_date) if args.end_date else date.today()
        start_date = (
            parse_iso_date(args.start_date)
            if args.start_date
            else default_start_date(end_date, lookback_days=args.lookback_days)
        )
        output_dir = Path(args.output_dir) if args.output_dir else None
        progress = None if args.quiet else _stderr_progress
        result = build_business_knowledge(
            settings_data_dir=settings.data_dir,
            api_key=settings.api_key,
            markets=parse_market_codes(args.markets),
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir,
            business_year=args.business_year,
            limit=args.limit,
            last_reprt_at=args.last_reprt_at,
            max_chars_per_file=args.max_chars_per_file,
            max_files=args.max_files,
            checkpoint_every=args.checkpoint_every,
            progress=progress,
        )
        print(f"Collected candidates: {result.candidates_count}")
        print(f"Candidate stock codes: {result.stock_count}")
        print(f"Selected companies: {len(result.documents)}")
        print(f"Failures: {len(result.failures)}")
        print(f"output_dir: {result.artifacts.output_dir}")
        print(f"progress: {result.artifacts.progress_path}")
        if result.artifacts.inventory_path:
            print(f"inventory: {result.artifacts.inventory_path}")
        print(f"partial_jsonl: {result.artifacts.partial_jsonl_path}")
        print(f"partial_failures_jsonl: {result.artifacts.partial_failures_path}")
        for path in result.artifacts.markdown_paths:
            print(f"markdown: {path}")
        print(f"jsonl: {result.artifacts.jsonl_path}")
        print(f"failures_jsonl: {result.artifacts.failures_path}")
        print(f"manifest: {result.artifacts.manifest_path}")
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


def _stderr_progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)
