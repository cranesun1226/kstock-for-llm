from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .derived import QaCheck, SectionChunk, profile_section
from .parsers import Section


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        with self.connection:
            self.connection.execute("PRAGMA foreign_keys = ON")
        self._initialize()

    def _initialize(self) -> None:
        with self.connection:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS issuers (
                    corp_code TEXT PRIMARY KEY,
                    stock_code TEXT NOT NULL,
                    corp_name TEXT NOT NULL,
                    corp_eng_name TEXT,
                    modify_date TEXT,
                    corp_cls TEXT,
                    jurir_no TEXT,
                    bizr_no TEXT,
                    ceo_nm TEXT,
                    acc_mt TEXT,
                    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS filings (
                    rcept_no TEXT PRIMARY KEY,
                    corp_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    corp_name TEXT NOT NULL,
                    business_year INTEGER,
                    fiscal_month TEXT,
                    report_nm TEXT NOT NULL,
                    report_kind TEXT,
                    reprt_code TEXT NOT NULL,
                    rcept_dt TEXT NOT NULL,
                    flr_nm TEXT,
                    is_final INTEGER NOT NULL DEFAULT 1,
                    storage_key TEXT,
                    raw_document_path TEXT NOT NULL,
                    raw_xbrl_path TEXT,
                    silver_base_path TEXT,
                    gold_base_path TEXT,
                    manifest_path TEXT,
                    sections_count INTEGER NOT NULL DEFAULT 0,
                    chunks_count INTEGER NOT NULL DEFAULT 0,
                    financial_facts_count INTEGER NOT NULL DEFAULT 0,
                    qa_status TEXT,
                    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rcept_no TEXT NOT NULL,
                    section_key TEXT,
                    heading_path TEXT NOT NULL,
                    parent_heading_path TEXT,
                    heading TEXT NOT NULL,
                    heading_level INTEGER,
                    body TEXT NOT NULL,
                    body_char_count INTEGER,
                    body_line_count INTEGER,
                    body_hash TEXT,
                    ordinal INTEGER NOT NULL,
                    source_tag TEXT NOT NULL,
                    section_type TEXT,
                    is_cover INTEGER NOT NULL DEFAULT 0,
                    is_noise INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS financial_facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rcept_no TEXT NOT NULL,
                    corp_code TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    bsns_year INTEGER NOT NULL,
                    reprt_code TEXT NOT NULL,
                    fs_div TEXT NOT NULL,
                    sj_div TEXT,
                    sj_nm TEXT,
                    fact_key TEXT,
                    account_id TEXT,
                    account_nm TEXT,
                    account_detail TEXT,
                    thstrm_amount TEXT,
                    thstrm_amount_value INTEGER,
                    thstrm_add_amount TEXT,
                    frmtrm_amount TEXT,
                    frmtrm_amount_value INTEGER,
                    frmtrm_q_amount TEXT,
                    frmtrm_add_amount TEXT,
                    bfefrmtrm_amount TEXT,
                    bfefrmtrm_amount_value INTEGER,
                    currency TEXT,
                    ord TEXT
                );

                CREATE TABLE IF NOT EXISTS filing_artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rcept_no TEXT NOT NULL,
                    layer TEXT NOT NULL,
                    artifact_role TEXT NOT NULL,
                    artifact_format TEXT NOT NULL,
                    path TEXT NOT NULL,
                    byte_size INTEGER,
                    sha256 TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS section_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rcept_no TEXT NOT NULL,
                    section_key TEXT NOT NULL,
                    section_ordinal INTEGER NOT NULL,
                    chunk_ordinal INTEGER NOT NULL,
                    chunk_key TEXT NOT NULL,
                    heading_path TEXT NOT NULL,
                    heading TEXT NOT NULL,
                    source_tag TEXT NOT NULL,
                    section_type TEXT NOT NULL,
                    text TEXT NOT NULL,
                    retrieval_text TEXT NOT NULL,
                    body_char_start INTEGER NOT NULL,
                    body_char_end INTEGER NOT NULL,
                    char_count INTEGER NOT NULL,
                    token_estimate INTEGER NOT NULL,
                    is_retrieval_candidate INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS qa_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rcept_no TEXT NOT NULL,
                    check_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metric_value REAL,
                    details TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT,
                    corp_code TEXT,
                    business_year INTEGER,
                    rcept_no TEXT,
                    report_nm TEXT,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    finished_at TEXT
                );
                """
            )
        self._migrate_schema()
        with self.connection:
            self.connection.executescript(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_filings_storage_key
                ON filings(stock_code, business_year, storage_key);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_sections_rcept_ordinal
                ON sections(rcept_no, ordinal);

                CREATE INDEX IF NOT EXISTS idx_sections_heading_path
                ON sections(rcept_no, heading_path);

                CREATE INDEX IF NOT EXISTS idx_financial_facts_lookup
                ON financial_facts(stock_code, bsns_year, fs_div, account_nm);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_filing_artifacts_unique
                ON filing_artifacts(rcept_no, layer, artifact_role);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_section_chunks_unique
                ON section_chunks(rcept_no, section_ordinal, chunk_ordinal);

                CREATE INDEX IF NOT EXISTS idx_section_chunks_retrieval
                ON section_chunks(rcept_no, is_retrieval_candidate);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_checks_unique
                ON qa_checks(rcept_no, check_name);

                CREATE INDEX IF NOT EXISTS idx_sync_runs_status
                ON sync_runs(status, created_at);
                """
            )

    def _migrate_schema(self) -> None:
        self._ensure_column("filings", "business_year", "INTEGER")
        self._ensure_column("filings", "fiscal_month", "TEXT")
        self._ensure_column("filings", "report_kind", "TEXT")
        self._ensure_column("filings", "storage_key", "TEXT")
        self._ensure_column("filings", "silver_base_path", "TEXT")
        self._ensure_column("filings", "gold_base_path", "TEXT")
        self._ensure_column("filings", "manifest_path", "TEXT")
        self._ensure_column("filings", "sections_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("filings", "chunks_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("filings", "financial_facts_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("filings", "qa_status", "TEXT")

        self._ensure_column("sections", "section_key", "TEXT")
        self._ensure_column("sections", "parent_heading_path", "TEXT")
        self._ensure_column("sections", "heading_level", "INTEGER")
        self._ensure_column("sections", "body_char_count", "INTEGER")
        self._ensure_column("sections", "body_line_count", "INTEGER")
        self._ensure_column("sections", "body_hash", "TEXT")
        self._ensure_column("sections", "section_type", "TEXT")
        self._ensure_column("sections", "is_cover", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("sections", "is_noise", "INTEGER NOT NULL DEFAULT 0")

        self._ensure_column("financial_facts", "fact_key", "TEXT")
        self._ensure_column("financial_facts", "thstrm_amount_value", "INTEGER")
        self._ensure_column("financial_facts", "frmtrm_amount_value", "INTEGER")
        self._ensure_column("financial_facts", "bfefrmtrm_amount_value", "INTEGER")

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}

    def _ensure_column(self, table_name: str, column_name: str, definition: str) -> None:
        if column_name in self._table_columns(table_name):
            return
        with self.connection:
            self.connection.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"
            )

    def upsert_issuer(self, issuer: dict[str, Any]) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO issuers (
                    corp_code, stock_code, corp_name, corp_eng_name, modify_date,
                    corp_cls, jurir_no, bizr_no, ceo_nm, acc_mt
                )
                VALUES (
                    :corp_code, :stock_code, :corp_name, :corp_eng_name, :modify_date,
                    :corp_cls, :jurir_no, :bizr_no, :ceo_nm, :acc_mt
                )
                ON CONFLICT(corp_code) DO UPDATE SET
                    stock_code=excluded.stock_code,
                    corp_name=excluded.corp_name,
                    corp_eng_name=excluded.corp_eng_name,
                    modify_date=excluded.modify_date,
                    corp_cls=excluded.corp_cls,
                    jurir_no=excluded.jurir_no,
                    bizr_no=excluded.bizr_no,
                    ceo_nm=excluded.ceo_nm,
                    acc_mt=excluded.acc_mt,
                    synced_at=CURRENT_TIMESTAMP
                """,
                issuer,
            )

    def upsert_filing(self, filing: dict[str, Any]) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO filings (
                    rcept_no, corp_code, stock_code, corp_name, business_year, fiscal_month,
                    report_nm, report_kind, reprt_code, rcept_dt, flr_nm, is_final,
                    storage_key, raw_document_path, raw_xbrl_path, silver_base_path,
                    gold_base_path, manifest_path, sections_count, chunks_count,
                    financial_facts_count, qa_status
                )
                VALUES (
                    :rcept_no, :corp_code, :stock_code, :corp_name, :business_year,
                    :fiscal_month, :report_nm, :report_kind, :reprt_code, :rcept_dt, :flr_nm,
                    :is_final, :storage_key, :raw_document_path, :raw_xbrl_path,
                    :silver_base_path, :gold_base_path, :manifest_path, :sections_count,
                    :chunks_count, :financial_facts_count, :qa_status
                )
                ON CONFLICT(rcept_no) DO UPDATE SET
                    corp_code=excluded.corp_code,
                    stock_code=excluded.stock_code,
                    corp_name=excluded.corp_name,
                    business_year=excluded.business_year,
                    fiscal_month=excluded.fiscal_month,
                    report_nm=excluded.report_nm,
                    report_kind=excluded.report_kind,
                    reprt_code=excluded.reprt_code,
                    rcept_dt=excluded.rcept_dt,
                    flr_nm=excluded.flr_nm,
                    is_final=excluded.is_final,
                    storage_key=excluded.storage_key,
                    raw_document_path=excluded.raw_document_path,
                    raw_xbrl_path=excluded.raw_xbrl_path,
                    silver_base_path=excluded.silver_base_path,
                    gold_base_path=excluded.gold_base_path,
                    manifest_path=excluded.manifest_path,
                    sections_count=excluded.sections_count,
                    chunks_count=excluded.chunks_count,
                    financial_facts_count=excluded.financial_facts_count,
                    qa_status=excluded.qa_status,
                    synced_at=CURRENT_TIMESTAMP
                """,
                filing,
            )

    def replace_sections(self, rcept_no: str, sections: list[Section]) -> None:
        with self.connection:
            self.connection.execute("DELETE FROM sections WHERE rcept_no = ?", (rcept_no,))
            self.connection.executemany(
                """
                INSERT INTO sections (
                    rcept_no, section_key, heading_path, parent_heading_path, heading,
                    heading_level, body, body_char_count, body_line_count, body_hash,
                    ordinal, source_tag, section_type, is_cover, is_noise
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        rcept_no,
                        profile_section(section).section_key,
                        section.heading_path,
                        profile_section(section).parent_heading_path,
                        section.heading,
                        profile_section(section).heading_level,
                        section.body,
                        profile_section(section).body_char_count,
                        profile_section(section).body_line_count,
                        profile_section(section).body_hash,
                        section.ordinal,
                        section.source_tag,
                        profile_section(section).section_type,
                        int(profile_section(section).is_cover),
                        int(profile_section(section).is_noise),
                    )
                    for section in sections
                ],
            )

    def replace_financial_facts(
        self,
        *,
        rcept_no: str,
        corp_code: str,
        stock_code: str,
        business_year: int,
        reprt_code: str,
        fs_div: str,
        facts: list[dict[str, Any]],
    ) -> None:
        with self.connection:
            self.connection.execute(
                "DELETE FROM financial_facts WHERE rcept_no = ? AND fs_div = ?",
                (rcept_no, fs_div),
            )
            self.connection.executemany(
                """
                INSERT INTO financial_facts (
                    rcept_no, corp_code, stock_code, bsns_year, reprt_code, fs_div,
                    sj_div, sj_nm, fact_key, account_id, account_nm, account_detail,
                    thstrm_amount, thstrm_amount_value, thstrm_add_amount, frmtrm_amount,
                    frmtrm_amount_value, frmtrm_q_amount, frmtrm_add_amount,
                    bfefrmtrm_amount, bfefrmtrm_amount_value, currency, ord
                )
                VALUES (
                    :rcept_no, :corp_code, :stock_code, :bsns_year, :reprt_code, :fs_div,
                    :sj_div, :sj_nm, :fact_key, :account_id, :account_nm, :account_detail,
                    :thstrm_amount, :thstrm_amount_value, :thstrm_add_amount, :frmtrm_amount,
                    :frmtrm_amount_value, :frmtrm_q_amount, :frmtrm_add_amount,
                    :bfefrmtrm_amount, :bfefrmtrm_amount_value, :currency, :ord
                )
                """,
                [
                    {
                        "rcept_no": rcept_no,
                        "corp_code": corp_code,
                        "stock_code": stock_code,
                        "bsns_year": business_year,
                        "reprt_code": reprt_code,
                        "fs_div": fs_div,
                        "sj_div": fact.get("sj_div"),
                        "sj_nm": fact.get("sj_nm"),
                        "fact_key": self._build_fact_key(fs_div, fact),
                        "account_id": fact.get("account_id"),
                        "account_nm": fact.get("account_nm"),
                        "account_detail": fact.get("account_detail"),
                        "thstrm_amount": fact.get("thstrm_amount"),
                        "thstrm_amount_value": self._parse_int(fact.get("thstrm_amount")),
                        "thstrm_add_amount": fact.get("thstrm_add_amount"),
                        "frmtrm_amount": fact.get("frmtrm_amount"),
                        "frmtrm_amount_value": self._parse_int(fact.get("frmtrm_amount")),
                        "frmtrm_q_amount": fact.get("frmtrm_q_amount"),
                        "frmtrm_add_amount": fact.get("frmtrm_add_amount"),
                        "bfefrmtrm_amount": fact.get("bfefrmtrm_amount"),
                        "bfefrmtrm_amount_value": self._parse_int(
                            fact.get("bfefrmtrm_amount")
                        ),
                        "currency": fact.get("currency"),
                        "ord": fact.get("ord"),
                    }
                    for fact in facts
                ],
            )

    def replace_filing_artifacts(self, rcept_no: str, artifacts: list[dict[str, Any]]) -> None:
        with self.connection:
            self.connection.execute(
                "DELETE FROM filing_artifacts WHERE rcept_no = ?",
                (rcept_no,),
            )
            self.connection.executemany(
                """
                INSERT INTO filing_artifacts (
                    rcept_no, layer, artifact_role, artifact_format, path, byte_size, sha256
                )
                VALUES (
                    :rcept_no, :layer, :artifact_role, :artifact_format, :path, :byte_size,
                    :sha256
                )
                """,
                artifacts,
            )

    def replace_section_chunks(self, rcept_no: str, chunks: list[SectionChunk]) -> None:
        with self.connection:
            self.connection.execute(
                "DELETE FROM section_chunks WHERE rcept_no = ?",
                (rcept_no,),
            )
            self.connection.executemany(
                """
                INSERT INTO section_chunks (
                    rcept_no, section_key, section_ordinal, chunk_ordinal, chunk_key,
                    heading_path, heading, source_tag, section_type, text, retrieval_text,
                    body_char_start, body_char_end, char_count, token_estimate,
                    is_retrieval_candidate
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        rcept_no,
                        chunk.section_key,
                        chunk.section_ordinal,
                        chunk.chunk_ordinal,
                        chunk.chunk_key,
                        chunk.heading_path,
                        chunk.heading,
                        chunk.source_tag,
                        chunk.section_type,
                        chunk.text,
                        chunk.retrieval_text,
                        chunk.body_char_start,
                        chunk.body_char_end,
                        chunk.char_count,
                        chunk.token_estimate,
                        int(chunk.is_retrieval_candidate),
                    )
                    for chunk in chunks
                ],
            )

    def replace_qa_checks(self, rcept_no: str, checks: list[QaCheck]) -> None:
        with self.connection:
            self.connection.execute("DELETE FROM qa_checks WHERE rcept_no = ?", (rcept_no,))
            self.connection.executemany(
                """
                INSERT INTO qa_checks (
                    rcept_no, check_name, status, metric_value, details
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        rcept_no,
                        check.check_name,
                        check.status,
                        check.metric_value,
                        check.details,
                    )
                    for check in checks
                ],
            )

    def begin_sync_run(
        self,
        *,
        stock_code: str | None,
        corp_code: str | None,
        business_year: int,
    ) -> int:
        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO sync_runs (
                    stock_code, corp_code, business_year, status, message
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (stock_code, corp_code, business_year, "running", None),
            )
        return int(cursor.lastrowid)

    def finish_sync_run(
        self,
        run_id: int,
        *,
        status: str,
        message: str | None = None,
        corp_code: str | None = None,
        rcept_no: str | None = None,
        report_nm: str | None = None,
        stock_code: str | None = None,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                UPDATE sync_runs
                SET stock_code = COALESCE(?, stock_code),
                    corp_code = COALESCE(?, corp_code),
                    rcept_no = COALESCE(?, rcept_no),
                    report_nm = COALESCE(?, report_nm),
                    status = ?,
                    message = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (stock_code, corp_code, rcept_no, report_nm, status, message, run_id),
            )

    @staticmethod
    def _parse_int(value: Any) -> int | None:
        text = str(value or "").replace(",", "").strip()
        if not text or text in {"-", "None", "nan"}:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    @staticmethod
    def _build_fact_key(fs_div: str, fact: dict[str, Any]) -> str:
        parts = [
            fs_div,
            str(fact.get("sj_div") or ""),
            str(fact.get("account_id") or ""),
            str(fact.get("account_nm") or ""),
            str(fact.get("account_detail") or ""),
        ]
        return "|".join(parts)

    def close(self) -> None:
        self.connection.close()
