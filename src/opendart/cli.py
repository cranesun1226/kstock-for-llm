from __future__ import annotations

import argparse

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
        print(
            f"financial_facts_json: {result.financial_facts_path} "
            f"({result.financial_facts_count} facts)"
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2
