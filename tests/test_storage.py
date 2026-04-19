from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from opendart.storage import Database  # noqa: E402


class StorageTests(unittest.TestCase):
    def test_database_initializes_expanded_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(Path(tmpdir) / "opendart.db")
            try:
                tables = {
                    row["name"]
                    for row in db.connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                self.assertTrue(
                    {
                        "issuers",
                        "filings",
                        "sections",
                        "financial_facts",
                        "filing_artifacts",
                        "section_chunks",
                        "qa_checks",
                        "sync_runs",
                    }.issubset(tables)
                )
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
