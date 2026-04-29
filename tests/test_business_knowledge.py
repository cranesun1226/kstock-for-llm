from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from opendart.business_knowledge import (  # noqa: E402
    BusinessFilingCandidate,
    BusinessKnowledgeDocument,
    filter_business_content_sections,
    parse_market_codes,
    rank_filing_candidates,
    select_business_documents,
    write_business_knowledge_outputs,
)
from opendart.parsers import Section  # noqa: E402


class BusinessKnowledgeTests(unittest.TestCase):
    def test_filter_business_content_sections_handles_unicode_roman_heading(self) -> None:
        sections = [
            Section(
                heading_path="Ⅰ. 회사의 개요 > 1. 회사",
                heading="1. 회사",
                body="개요",
                ordinal=1,
                source_tag="TITLE",
            ),
            Section(
                heading_path="Ⅱ. 사업의 내용 > 1. 사업의 개요",
                heading="1. 사업의 개요",
                body="사업 설명",
                ordinal=2,
                source_tag="TITLE",
            ),
            Section(
                heading_path="III. 재무에 관한 사항 > 1. 재무",
                heading="1. 재무",
                body="재무 설명",
                ordinal=3,
                source_tag="TITLE",
            ),
        ]

        business_sections = filter_business_content_sections(sections)

        self.assertEqual(len(business_sections), 1)
        self.assertEqual(business_sections[0].ordinal, 2)

    def test_rank_filing_candidates_prefers_content_correction_over_attachment(self) -> None:
        candidates = [
            _candidate("[첨부정정]사업보고서 (2025.12)", "20260423000699", "20260423"),
            _candidate("[기재정정]사업보고서 (2025.12)", "20260423000414", "20260423"),
            _candidate("사업보고서 (2025.12)", "20260317000890", "20260317"),
            _candidate("사업보고서 (2024.12)", "20250311001260", "20250311"),
        ]

        ranked = rank_filing_candidates(candidates, business_year=2025)

        self.assertEqual([candidate.rcept_no for candidate in ranked[:3]], [
            "20260423000414",
            "20260317000890",
            "20260423000699",
        ])

    def test_select_business_documents_falls_back_to_next_candidate(self) -> None:
        candidates = [
            _candidate("[기재정정]사업보고서 (2025.12)", "bad", "20260423"),
            _candidate("사업보고서 (2025.12)", "good", "20260317"),
        ]
        client = _FakeClient(
            {
                "bad": RuntimeError("missing document"),
                "good": _document_zip(
                    """
                    <DOCUMENT>
                      <TITLE>II. 사업의 내용</TITLE>
                      <P>회사는 테스트 사업을 영위합니다.</P>
                    </DOCUMENT>
                    """
                ),
            }
        )

        documents, failures = select_business_documents(client, candidates)

        self.assertEqual(len(documents), 1)
        self.assertEqual(len(failures), 0)
        self.assertEqual(documents[0].candidate.rcept_no, "good")
        self.assertEqual(documents[0].business_section_count, 1)
        self.assertEqual(documents[0].selected_candidate_attempts[0]["status"], "error")

    def test_select_business_documents_writes_partial_progress_files(self) -> None:
        candidates = [_candidate("사업보고서 (2025.12)", "good", "20260317")]
        client = _FakeClient(
            {
                "good": _document_zip(
                    """
                    <DOCUMENT>
                      <TITLE>II. 사업의 내용</TITLE>
                      <P>회사는 테스트 사업을 영위합니다.</P>
                    </DOCUMENT>
                    """
                ),
            }
        )
        logs: list[str] = []

        with tempfile.TemporaryDirectory() as tmpdir:
            documents, failures = select_business_documents(
                client,
                candidates,
                output_dir=Path(tmpdir),
                progress=logs.append,
            )
            progress = json.loads((Path(tmpdir) / "progress.json").read_text(encoding="utf-8"))
            partial_lines = (
                Path(tmpdir)
                / "business_sections.partial.jsonl"
            ).read_text(encoding="utf-8").splitlines()
            failure_partial = (Path(tmpdir) / "failures.partial.jsonl").read_text(
                encoding="utf-8"
            )

        self.assertEqual(len(documents), 1)
        self.assertEqual(len(failures), 0)
        self.assertEqual(progress["status"], "selection_completed")
        self.assertEqual(progress["processed_count"], 1)
        self.assertEqual(len(partial_lines), 1)
        self.assertEqual(failure_partial, "")
        self.assertTrue(any(message.startswith("[ok]") for message in logs))

    def test_write_business_knowledge_outputs_creates_markdown_jsonl_and_manifest(self) -> None:
        document = BusinessKnowledgeDocument(
            candidate=_candidate("사업보고서 (2025.12)", "20260317000890", "20260317"),
            sections=[
                Section(
                    heading_path="II. 사업의 내용 > 1. 사업의 개요",
                    heading="1. 사업의 개요",
                    body="테스트 사업 설명",
                    ordinal=7,
                    source_tag="TITLE",
                )
            ],
            all_sections_count=10,
            selected_candidate_attempts=[],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = write_business_knowledge_outputs(
                output_dir=Path(tmpdir),
                documents=[document],
                failures=[],
                candidates_count=1,
                stock_count=1,
                markets=["Y"],
                start_date=date(2026, 1, 1),
                end_date=date(2026, 4, 29),
                business_year=2025,
                max_chars_per_file=500,
                max_files=2,
            )
            markdown = artifacts.markdown_paths[0].read_text(encoding="utf-8")
            jsonl_lines = artifacts.jsonl_path.read_text(encoding="utf-8").splitlines()
            manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))

        self.assertIn("II. 사업의 내용 > 1. 사업의 개요", markdown)
        self.assertEqual(len(jsonl_lines), 1)
        self.assertEqual(json.loads(jsonl_lines[0])["stock_code"], "066970")
        self.assertEqual(manifest["selected_count"], 1)
        self.assertEqual(manifest["business_sections_count"], 1)

    def test_parse_market_codes_accepts_names_and_deduplicates(self) -> None:
        self.assertEqual(parse_market_codes("kospi,K,KOSDAQ,Y"), ["Y", "K"])


def _candidate(report_nm: str, rcept_no: str, rcept_dt: str) -> BusinessFilingCandidate:
    return BusinessFilingCandidate(
        corp_cls="Y",
        corp_code="001",
        stock_code="066970",
        corp_name="테스트",
        report_nm=report_nm,
        rcept_no=rcept_no,
        rcept_dt=rcept_dt,
    )


def _document_zip(xml_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("document.xml", xml_text)
    return buffer.getvalue()


class _FakeClient:
    api_key = "secret"

    def __init__(self, responses: dict[str, bytes | Exception]) -> None:
        self.responses = responses

    def download_document(self, rcept_no: str) -> bytes:
        response = self.responses[rcept_no]
        if isinstance(response, Exception):
            raise response
        return response


if __name__ == "__main__":
    unittest.main()
