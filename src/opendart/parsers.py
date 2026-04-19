from __future__ import annotations

import io
import re
import zipfile
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable


HEADING_TAGS = {
    "CHAPTER": 1,
    "HEAD": 2,
    "ARTICLE": 2,
    "SECTION": 2,
    "SECTION-1": 1,
    "SECTION-2": 2,
    "SECTION-3": 3,
    "SUBTITLE": 2,
    "TITLE": 1,
}

ROMAN_RE = re.compile(r"^(?:[IVXLCM]+)\.\s+")
DIGIT_RE = re.compile(r"^\d+\.\s+")
KOREAN_ALPHA_RE = re.compile(r"^[가-힣]\.\s+")
PAREN_RE = re.compile(r"^\(\d+\)\s+|^\([A-Za-z가-힣]\)\s+")
LETTER_RE = re.compile(r"^[A-Za-z]\.\s+")
MAJOR_HEADING_RE = re.compile(r"(?:사항|현황|내용|구조|의견|정책|개요|추이)$")
WHITESPACE_RE = re.compile(r"\s+")
ANGLE_FRAGMENT_RE = re.compile(r"<([^<>]+)>")
VALID_TAG_FRAGMENT_RE = re.compile(
    r"^/?[A-Za-z_][A-Za-z0-9_.:-]*"
    r"(?:\s+[A-Za-z_:][-A-Za-z0-9_.:]*\s*=\s*(?:\"[^\"]*\"|'[^']*'))*\s*/?$"
)


@dataclass
class Section:
    heading_path: str
    heading: str
    body: str
    ordinal: int
    source_tag: str

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


@dataclass(frozen=True)
class _TextNode:
    tag: str
    text: str
    attrs: dict[str, str]


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


def _preprocess_markup_text(value: str) -> str:
    text = value.replace("\ufeff", "")

    def escape_suspicious_angle_fragment(match: re.Match[str]) -> str:
        inner = match.group(1)
        candidate = inner.strip()
        if candidate.startswith(("!", "?")):
            return match.group(0)
        if VALID_TAG_FRAGMENT_RE.match(candidate):
            return match.group(0)
        return f"&lt;{inner}&gt;"

    return ANGLE_FRAGMENT_RE.sub(escape_suspicious_angle_fragment, text)


class _DartMarkupParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._stack: list[tuple[str, dict[str, str]]] = []
        self._body_depth = 0
        self._seen_body = False
        self.nodes: list[_TextNode] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.upper()
        attr_map = {str(key).upper(): (value or "") for key, value in attrs}
        self._stack.append((normalized_tag, attr_map))
        if normalized_tag == "BODY":
            self._body_depth += 1
            self._seen_body = True

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.upper()
        if normalized_tag == "BODY" and self._body_depth > 0:
            self._body_depth -= 1

        for index in range(len(self._stack) - 1, -1, -1):
            if self._stack[index][0] == normalized_tag:
                del self._stack[index:]
                return

    def handle_data(self, data: str) -> None:
        if self._seen_body and self._body_depth == 0:
            return

        text = _clean_text(data)
        if not text:
            return

        tag, attrs = self._current_context()
        self.nodes.append(_TextNode(tag=tag, text=text, attrs=attrs))

    def _current_context(self) -> tuple[str, dict[str, str]]:
        for tag, attrs in reversed(self._stack):
            if tag not in {"DOCUMENT", "BODY"}:
                return tag, attrs
        if self._stack:
            return self._stack[-1]
        return "DOCUMENT", {}


def _iter_text_nodes(markup_text: str) -> Iterable[_TextNode]:
    parser = _DartMarkupParser()
    parser.feed(_preprocess_markup_text(markup_text))
    parser.close()
    return parser.nodes


def infer_heading_level(tag: str, text: str, attrs: dict[str, str] | None = None) -> int | None:
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
    normalized_tag = tag.replace("_TAIL", "").upper()
    if normalized_tag in HEADING_TAGS:
        return HEADING_TAGS[normalized_tag]
    if attrs and attrs.get("USERMARK", "").upper() == "B" and 2 <= len(text) <= 60:
        return 3
    if 2 <= len(text) <= 40 and MAJOR_HEADING_RE.search(text):
        return 2
    return None


def parse_sections_from_xml_text(xml_text: str) -> list[Section]:
    lines = list(_iter_text_nodes(xml_text))

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

    for node in lines:
        tag = node.tag
        text = node.text
        if not text:
            continue
        level = infer_heading_level(tag, text, node.attrs)
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

    fallback_body = "\n".join(node.text for node in lines).strip()
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
