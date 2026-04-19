from __future__ import annotations

import io
import re
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET


HEADING_TAGS = {
    "TITLE": 1,
    "SUBTITLE": 2,
    "SECTION": 2,
    "CHAPTER": 1,
    "ARTICLE": 2,
    "HEAD": 2,
}

ROMAN_RE = re.compile(r"^(?:[IVXLCM]+)\.\s+")
DIGIT_RE = re.compile(r"^\d+\.\s+")
KOREAN_ALPHA_RE = re.compile(r"^[가-힣]\.\s+")
PAREN_RE = re.compile(r"^\(\d+\)\s+|^\([A-Za-z가-힣]\)\s+")
LETTER_RE = re.compile(r"^[A-Za-z]\.\s+")
MAJOR_HEADING_RE = re.compile(r"(?:사항|현황|내용|구조|의견|정책|개요|추이)$")
WHITESPACE_RE = re.compile(r"\s+")


@dataclass
class Section:
    heading_path: str
    heading: str
    body: str
    ordinal: int
    source_tag: str

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


def decode_xml_bytes(payload: bytes) -> str:
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="replace")


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return WHITESPACE_RE.sub(" ", value.replace("\u00a0", " ")).strip()


def _iter_text_nodes(root: ET.Element) -> Iterable[tuple[str, str]]:
    for element in root.iter():
        tag = element.tag.split("}")[-1].upper()
        direct_text = _clean_text(element.text)
        if direct_text:
            yield tag, direct_text
        tail_text = _clean_text(element.tail)
        if tail_text:
            yield f"{tag}_TAIL", tail_text


def infer_heading_level(tag: str, text: str) -> int | None:
    normalized_tag = tag.replace("_TAIL", "")
    if normalized_tag in HEADING_TAGS and len(text) <= 120:
        return HEADING_TAGS[normalized_tag]
    if len(text) > 120:
        return None
    if ROMAN_RE.match(text):
        return 1
    if DIGIT_RE.match(text):
        return 2
    if KOREAN_ALPHA_RE.match(text):
        return 3
    if PAREN_RE.match(text):
        return 4
    if LETTER_RE.match(text):
        return 4
    if 2 <= len(text) <= 40 and MAJOR_HEADING_RE.search(text):
        return 2
    return None


def parse_sections_from_xml_text(xml_text: str) -> list[Section]:
    root = ET.fromstring(xml_text)
    lines = list(_iter_text_nodes(root))

    sections: list[Section] = []
    stack: list[tuple[int, str]] = []
    current_heading = "document"
    current_path = "document"
    current_tag = "DOCUMENT"
    body_lines: list[str] = []

    def flush() -> None:
        body = "\n".join(body_lines).strip()
        if body:
            sections.append(
                Section(
                    heading_path=current_path,
                    heading=current_heading,
                    body=body,
                    ordinal=len(sections) + 1,
                    source_tag=current_tag,
                )
            )

    for tag, text in lines:
        if not text:
            continue
        level = infer_heading_level(tag, text)
        if level is not None:
            flush()
            body_lines = []
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, text))
            current_heading = text
            current_path = " > ".join(item[1] for item in stack)
            current_tag = tag
            continue

        if not body_lines or body_lines[-1] != text:
            body_lines.append(text)

    flush()

    if sections:
        return sections

    fallback_body = "\n".join(text for _tag, text in lines).strip()
    if not fallback_body:
        return []
    return [
        Section(
            heading_path="document",
            heading="document",
            body=fallback_body,
            ordinal=1,
            source_tag="DOCUMENT",
        )
    ]


def _pick_primary_xml_file(zf: zipfile.ZipFile) -> str:
    candidates: list[tuple[int, str]] = []
    for info in zf.infolist():
        if not info.filename.lower().endswith(".xml"):
            continue
        payload = zf.read(info.filename)
        score = len(payload)
        candidates.append((score, info.filename))
    if not candidates:
        raise ValueError("No XML files were found in the document ZIP.")
    candidates.sort(reverse=True)
    return candidates[0][1]


def parse_sections_from_document_zip(payload: bytes) -> list[Section]:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        xml_filename = _pick_primary_xml_file(zf)
        xml_text = decode_xml_bytes(zf.read(xml_filename))
    return parse_sections_from_xml_text(xml_text)


def write_sections_json(path: Path, sections: list[Section]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps([section.to_dict() for section in sections], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
