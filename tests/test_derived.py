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
    filter_chunks_by_priority,
    infer_section_priority,
    PRIORITY_ARCHIVE,
    PRIORITY_CONDITIONAL,
    PRIORITY_CORE,
    profile_section,
    summarize_chunk_priority_counts,
    summarize_section_priority_counts,
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
        self.assertEqual(profile.section_priority, PRIORITY_ARCHIVE)
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
        self.assertTrue(all(chunk.section_priority == PRIORITY_CORE for chunk in chunks))
        self.assertTrue(all(chunk.is_retrieval_candidate for chunk in chunks))

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

    def test_infer_section_priority_splits_core_conditional_archive(self) -> None:
        core_section = Section(
            heading_path="II. 사업의 내용 > 1. 사업의 개요",
            heading="1. 사업의 개요",
            body="사업 설명",
            ordinal=1,
            source_tag="TITLE",
        )
        conditional_section = Section(
            heading_path="VII. 주주에 관한 사항 > 1. 최대주주 및 그 특수관계인의 주식소유 현황",
            heading="1. 최대주주 및 그 특수관계인의 주식소유 현황",
            body="주주 설명",
            ordinal=2,
            source_tag="TITLE",
        )
        archive_section = Section(
            heading_path="XII. 상세표 > 1. 연결대상 종속회사 현황(상세)",
            heading="1. 연결대상 종속회사 현황(상세)",
            body="상세 표",
            ordinal=3,
            source_tag="TITLE",
        )

        self.assertEqual(infer_section_priority(core_section), PRIORITY_CORE)
        self.assertEqual(infer_section_priority(conditional_section), PRIORITY_CONDITIONAL)
        self.assertEqual(infer_section_priority(archive_section), PRIORITY_ARCHIVE)

    def test_chunk_priority_helpers_split_delivery_pools(self) -> None:
        sections = [
            Section(
                heading_path="II. 사업의 내용 > 1. 사업의 개요",
                heading="1. 사업의 개요",
                body="A" * 200,
                ordinal=1,
                source_tag="TITLE",
            ),
            Section(
                heading_path="V. 회계감사인의 감사의견 등 > 감사의견",
                heading="감사의견",
                body="B" * 200,
                ordinal=2,
                source_tag="TITLE",
            ),
            Section(
                heading_path="XII. 상세표 > 1. 연결대상 종속회사 현황(상세)",
                heading="1. 연결대상 종속회사 현황(상세)",
                body="C" * 200,
                ordinal=3,
                source_tag="TITLE",
            ),
        ]

        chunks = build_section_chunks(sections, target_chars=120, max_chars=160, min_chars=80)
        counts = summarize_chunk_priority_counts(chunks)
        section_counts = summarize_section_priority_counts(sections)

        self.assertGreater(counts[PRIORITY_CORE], 0)
        self.assertGreater(counts[PRIORITY_CONDITIONAL], 0)
        self.assertGreater(counts[PRIORITY_ARCHIVE], 0)
        self.assertEqual(section_counts[PRIORITY_CORE], 1)
        self.assertEqual(section_counts[PRIORITY_CONDITIONAL], 1)
        self.assertEqual(section_counts[PRIORITY_ARCHIVE], 1)
        self.assertTrue(
            any(
                chunk.is_retrieval_candidate
                for chunk in filter_chunks_by_priority(chunks, PRIORITY_CORE)
            )
        )
        self.assertTrue(
            any(
                chunk.is_conditional_candidate
                for chunk in filter_chunks_by_priority(chunks, PRIORITY_CONDITIONAL)
            )
        )
        self.assertTrue(
            all(
                (not chunk.is_retrieval_candidate and not chunk.is_conditional_candidate)
                for chunk in filter_chunks_by_priority(chunks, PRIORITY_ARCHIVE)
            )
        )

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
