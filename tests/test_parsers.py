from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from opendart.parsers import parse_sections_from_xml_text
from opendart.sync import select_best_annual_filing


class ParserTests(unittest.TestCase):
    def test_parse_sections_preserves_heading_path(self) -> None:
        xml_text = """
        <DOCUMENT>
          <TITLE>I. 사업의 내용</TITLE>
          <P>회사는 반도체와 모바일 기기를 제조합니다.</P>
          <SUBTITLE>1. 주요 제품</SUBTITLE>
          <P>메모리 반도체, 스마트폰</P>
          <SUBTITLE>가. 시장 현황</SUBTITLE>
          <P>글로벌 수요가 증가했습니다.</P>
        </DOCUMENT>
        """

        sections = parse_sections_from_xml_text(xml_text)

        self.assertGreaterEqual(len(sections), 3)
        self.assertEqual(sections[0].heading, "I. 사업의 내용")
        self.assertIn("I. 사업의 내용 > 1. 주요 제품", sections[1].heading_path)
        self.assertTrue(sections[-1].body.endswith("글로벌 수요가 증가했습니다."))

    def test_select_best_annual_filing_prefers_matching_business_year(self) -> None:
        filings = [
            {
                "rcept_no": "20260310000002",
                "report_nm": "[기재정정]사업보고서 (2024.12)",
                "rcept_dt": "20260310",
            },
            {
                "rcept_no": "20260315000001",
                "report_nm": "사업보고서 (2025.12)",
                "rcept_dt": "20260315",
            },
        ]

        selected = select_best_annual_filing(
            filings, business_year=2025, fiscal_month="12"
        )

        self.assertEqual(selected["rcept_no"], "20260315000001")


if __name__ == "__main__":
    unittest.main()
