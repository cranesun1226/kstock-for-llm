from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from opendart.derived import (  # noqa: E402
    build_qa_checks,
    build_section_chunks,
    profile_section,
    summarize_qa_status,
    write_chunks_jsonl,
)
from opendart.parsers import Section  # noqa: E402


class DerivedTests(unittest.TestCase):
    def test_profile_section_marks_cover_and_counts_body(self) -> None:
        section = Section(
            heading_path="document",
            heading="document",
            body="표지\n회사명",
            ordinal=1,
            source_tag="DOCUMENT",
        )

        profile = profile_section(section)

        self.assertTrue(profile.is_cover)
        self.assertEqual(profile.section_type, "cover")
        self.assertEqual(profile.body_line_count, 2)
        self.assertEqual(profile.section_key, "section-0001")

    def test_build_section_chunks_splits_long_sections(self) -> None:
        long_body = "\n".join(f"라인 {index} " + ("A" * 120) for index in range(20))
        section = Section(
            heading_path="I. 회사의 개요 > 1. 주요 내용",
            heading="1. 주요 내용",
            body=long_body,
            ordinal=3,
            source_tag="TITLE",
        )

        chunks = build_section_chunks([section], target_chars=400, max_chars=500, min_chars=200)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunk.retrieval_text.startswith(section.heading_path) for chunk in chunks))
        self.assertTrue(all(chunk.chunk_key.startswith("section-0003-chunk-") for chunk in chunks))

    def test_build_qa_checks_reports_core_metrics_and_status(self) -> None:
        sections = [
            Section(
                heading_path="document",
                heading="document",
                body="표지",
                ordinal=1,
                source_tag="DOCUMENT",
            ),
            Section(
                heading_path="I. 회사의 개요 > 1. 주요 사업",
                heading="1. 주요 사업",
                body="반도체와 모바일 사업을 영위합니다.",
                ordinal=2,
                source_tag="TITLE",
            ),
        ]
        chunks = build_section_chunks(sections, target_chars=80, max_chars=120, min_chars=20)
        financial_facts = [
            {"fs_div": "CFS", "account_nm": "자산총계"},
            {"fs_div": "CFS", "account_nm": "매출액"},
            {"fs_div": "CFS", "account_nm": "영업이익"},
            {"fs_div": "CFS", "account_nm": "당기순이익"},
        ]

        checks = build_qa_checks(sections, financial_facts, chunks)

        status_by_name = {check.check_name: check.status for check in checks}
        self.assertEqual(status_by_name["core_metric_coverage"], "pass")
        self.assertEqual(summarize_qa_status(checks), "fail")

    def test_write_chunks_jsonl_creates_line_delimited_output(self) -> None:
        section = Section(
            heading_path="I. 사업의 내용",
            heading="I. 사업의 내용",
            body="첫 문단\n둘째 문단",
            ordinal=1,
            source_tag="TITLE",
        )
        chunks = build_section_chunks([section], target_chars=20, max_chars=30, min_chars=5)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "chunks.jsonl"
            write_chunks_jsonl(path, chunks)
            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), len(chunks))
        first_payload = json.loads(lines[0])
        self.assertEqual(first_payload["section_key"], "section-0001")


if __name__ == "__main__":
    unittest.main()
