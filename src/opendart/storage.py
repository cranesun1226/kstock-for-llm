from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .parsers import Section


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
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
                    report_nm TEXT NOT NULL,
                    reprt_code TEXT NOT NULL,
                    rcept_dt TEXT NOT NULL,
                    flr_nm TEXT,
                    is_final INTEGER NOT NULL DEFAULT 1,
                    raw_document_path TEXT NOT NULL,
                    raw_xbrl_path TEXT,
                    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rcept_no TEXT NOT NULL,
                    heading_path TEXT NOT NULL,
                    heading TEXT NOT NULL,
                    body TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    source_tag TEXT NOT NULL
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
                    account_id TEXT,
                    account_nm TEXT,
                    account_detail TEXT,
                    thstrm_amount TEXT,
                    thstrm_add_amount TEXT,
                    frmtrm_amount TEXT,
                    frmtrm_q_amount TEXT,
                    frmtrm_add_amount TEXT,
                    bfefrmtrm_amount TEXT,
                    currency TEXT,
                    ord TEXT
                );
                """
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
                    rcept_no, corp_code, stock_code, corp_name, report_nm, reprt_code,
                    rcept_dt, flr_nm, is_final, raw_document_path, raw_xbrl_path
                )
                VALUES (
                    :rcept_no, :corp_code, :stock_code, :corp_name, :report_nm, :reprt_code,
                    :rcept_dt, :flr_nm, :is_final, :raw_document_path, :raw_xbrl_path
                )
                ON CONFLICT(rcept_no) DO UPDATE SET
                    corp_code=excluded.corp_code,
                    stock_code=excluded.stock_code,
                    corp_name=excluded.corp_name,
                    report_nm=excluded.report_nm,
                    reprt_code=excluded.reprt_code,
                    rcept_dt=excluded.rcept_dt,
                    flr_nm=excluded.flr_nm,
                    is_final=excluded.is_final,
                    raw_document_path=excluded.raw_document_path,
                    raw_xbrl_path=excluded.raw_xbrl_path,
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
                    rcept_no, heading_path, heading, body, ordinal, source_tag
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        rcept_no,
                        section.heading_path,
                        section.heading,
                        section.body,
                        section.ordinal,
                        section.source_tag,
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
                    sj_div, sj_nm, account_id, account_nm, account_detail,
                    thstrm_amount, thstrm_add_amount, frmtrm_amount, frmtrm_q_amount,
                    frmtrm_add_amount, bfefrmtrm_amount, currency, ord
                )
                VALUES (
                    :rcept_no, :corp_code, :stock_code, :bsns_year, :reprt_code, :fs_div,
                    :sj_div, :sj_nm, :account_id, :account_nm, :account_detail,
                    :thstrm_amount, :thstrm_add_amount, :frmtrm_amount, :frmtrm_q_amount,
                    :frmtrm_add_amount, :bfefrmtrm_amount, :currency, :ord
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
                        "account_id": fact.get("account_id"),
                        "account_nm": fact.get("account_nm"),
                        "account_detail": fact.get("account_detail"),
                        "thstrm_amount": fact.get("thstrm_amount"),
                        "thstrm_add_amount": fact.get("thstrm_add_amount"),
                        "frmtrm_amount": fact.get("frmtrm_amount"),
                        "frmtrm_q_amount": fact.get("frmtrm_q_amount"),
                        "frmtrm_add_amount": fact.get("frmtrm_add_amount"),
                        "bfefrmtrm_amount": fact.get("bfefrmtrm_amount"),
                        "currency": fact.get("currency"),
                        "ord": fact.get("ord"),
                    }
                    for fact in facts
                ],
            )

    def close(self) -> None:
        self.connection.close()
