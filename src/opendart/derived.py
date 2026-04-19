from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .parsers import Section


IMAGE_FILE_RE = re.compile(r"\b[\w./-]+\.(?:jpg|jpeg|png|gif|bmp|svg)\b", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")
NUMERIC_TOP_LEVEL_RE = re.compile(r"^\d+-\d+\.")
CORE_FACT_ALIASES = {
    "assets_total": {"자산총계"},
    "revenue": {"매출액", "영업수익", "수익(매출액)", "매출및지분법손익"},
    "operating_income": {"영업이익"},
    "net_income": {"당기순이익", "당기순이익(손실)", "당기순이익귀속지배기업주주지분"},
}
PRIORITY_CORE = "core"
PRIORITY_CONDITIONAL = "conditional"
PRIORITY_ARCHIVE = "archive"
TOP_LEVEL_CORE_PREFIXES = {
    "I. 회사의 개요",
    "II. 사업의 내용",
    "III. 재무에 관한 사항",
    "IV. 이사의 경영진단 및 분석의견",
    "XI. 그 밖에 투자자 보호를 위하여 필요한 사항",
}
TOP_LEVEL_CONDITIONAL_PREFIXES = {
    "V. 회계감사인의 감사의견 등",
    "VII. 주주에 관한 사항",
    "X. 대주주 등과의 거래내용",
}
ARCHIVE_TOP_LEVEL_PREFIXES = {
    "document",
    "【 대표이사 등의 확인 】",
    "XII. 상세표",
}
CONDITIONAL_VI_NEEDLES = {
    "가. 이사회 구성 개요",
    "위원회의 설치현황",
    "지배구조",
}
CONDITIONAL_VIII_NEEDLES = {
    "바. 직원 등의 현황",
    "(육아지원제도)",
    "(유연근무제도)",
    "복리후생",
    "안전보건",
    "교육훈련",
}
CONDITIONAL_IX_NEEDLES = {
    "(요약)",
    "관련법령상의 규제내용 등",
    "타법인 출자 현황(요약)",
}


@dataclass(frozen=True)
class SectionProfile:
    section_key: str
    parent_heading_path: str | None
    heading_level: int
    body_char_count: int
    body_line_count: int
    body_hash: str
    section_type: str
    section_priority: str
    is_cover: bool
    is_noise: bool

    def to_dict(self) -> dict[str, str | int | bool | None]:
        return asdict(self)


@dataclass(frozen=True)
class SectionChunk:
    section_key: str
    section_ordinal: int
    chunk_ordinal: int
    chunk_key: str
    heading_path: str
    heading: str
    source_tag: str
    section_type: str
    section_priority: str
    text: str
    retrieval_text: str
    body_char_start: int
    body_char_end: int
    char_count: int
    token_estimate: int
    is_retrieval_candidate: bool
    is_conditional_candidate: bool

    def to_dict(self) -> dict[str, str | int | bool]:
        return asdict(self)


@dataclass(frozen=True)
class QaCheck:
    check_name: str
    status: str
    metric_value: float | None
    details: str

    def to_dict(self) -> dict[str, str | float | None]:
        return asdict(self)


def _normalize_space(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def infer_section_type(section: Section) -> str:
    heading_path = section.heading_path
    heading = section.heading

    if heading_path == "document":
        return "cover"
    if "목     차" in heading_path or "목차" in heading_path:
        return "toc"
    if "대표이사 등의 확인" in heading_path:
        return "certification"
    if "상세표" in heading_path or "(상세)" in heading or heading.endswith("(상세)"):
        return "appendix"
    return "body"


def infer_section_priority(section: Section) -> str:
    heading_path = section.heading_path
    top_level = heading_path.split(" > ")[0]

    if top_level in ARCHIVE_TOP_LEVEL_PREFIXES:
        return PRIORITY_ARCHIVE
    if top_level in TOP_LEVEL_CORE_PREFIXES:
        return PRIORITY_CORE
    if top_level in TOP_LEVEL_CONDITIONAL_PREFIXES:
        return PRIORITY_CONDITIONAL

    if top_level.startswith("VI. "):
        if any(needle in heading_path for needle in CONDITIONAL_VI_NEEDLES):
            return PRIORITY_CONDITIONAL
        return PRIORITY_ARCHIVE

    if top_level.startswith("VIII. "):
        if any(needle in heading_path for needle in CONDITIONAL_VIII_NEEDLES):
            return PRIORITY_CONDITIONAL
        return PRIORITY_ARCHIVE

    if top_level.startswith("IX. "):
        if any(needle in heading_path for needle in CONDITIONAL_IX_NEEDLES):
            return PRIORITY_CONDITIONAL
        return PRIORITY_ARCHIVE

    if NUMERIC_TOP_LEVEL_RE.match(top_level):
        if "증권의 발행" in top_level or "자금" in top_level:
            return PRIORITY_CONDITIONAL
        return PRIORITY_ARCHIVE

    return PRIORITY_CONDITIONAL


def _is_noise_text(text: str) -> bool:
    normalized = _normalize_space(text)
    if not normalized:
        return True
    if normalized in {"☞ 본문 위치로 이동"}:
        return True
    if IMAGE_FILE_RE.fullmatch(normalized):
        return True
    return False


def profile_section(section: Section) -> SectionProfile:
    parent_heading_path = None
    heading_level = 0
    if section.heading_path != "document":
        parts = section.heading_path.split(" > ")
        heading_level = len(parts)
        if len(parts) > 1:
            parent_heading_path = " > ".join(parts[:-1])

    body_lines = [line.strip() for line in section.body.splitlines() if line.strip()]
    body_text = section.body.strip()
    section_type = infer_section_type(section)
    section_priority = infer_section_priority(section)
    is_cover = section_type == "cover"
    is_noise = _is_noise_text(body_text)

    return SectionProfile(
        section_key=f"section-{section.ordinal:04d}",
        parent_heading_path=parent_heading_path,
        heading_level=heading_level,
        body_char_count=len(body_text),
        body_line_count=len(body_lines),
        body_hash=_stable_text_hash(body_text),
        section_type=section_type,
        section_priority=section_priority,
        is_cover=is_cover,
        is_noise=is_noise,
    )


def _stable_text_hash(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _iter_body_segments(body: str, *, max_segment_chars: int) -> list[tuple[str, int, int]]:
    segments: list[tuple[str, int, int]] = []
    offset = 0

    for raw_line in body.splitlines(True):
        raw_no_newline = raw_line.rstrip("\n")
        if not raw_no_newline.strip():
            offset += len(raw_line)
            continue

        leading_ws = len(raw_no_newline) - len(raw_no_newline.lstrip())
        trimmed = raw_no_newline.strip()
        start = offset + leading_ws

        if len(trimmed) <= max_segment_chars:
            segments.append((trimmed, start, start + len(trimmed)))
        else:
            segments.extend(
                _split_long_segment(
                    trimmed,
                    start_offset=start,
                    max_segment_chars=max_segment_chars,
                )
            )

        offset += len(raw_line)

    return segments


def _split_long_segment(
    text: str, *, start_offset: int, max_segment_chars: int
) -> list[tuple[str, int, int]]:
    segments: list[tuple[str, int, int]] = []
    cursor = 0

    while cursor < len(text):
        end = min(cursor + max_segment_chars, len(text))
        if end < len(text):
            split_at = text.rfind(" ", cursor, end)
            if split_at > cursor + max_segment_chars // 2:
                end = split_at

        piece = text[cursor:end].strip()
        if piece:
            adjusted_start = start_offset + cursor
            segments.append((piece, adjusted_start, adjusted_start + len(piece)))

        cursor = end
        while cursor < len(text) and text[cursor].isspace():
            cursor += 1

    return segments


def build_section_chunks(
    sections: list[Section],
    *,
    target_chars: int = 1200,
    max_chars: int = 1600,
    min_chars: int = 300,
) -> list[SectionChunk]:
    chunks: list[SectionChunk] = []

    for section in sections:
        profile = profile_section(section)
        segments = _iter_body_segments(section.body, max_segment_chars=max_chars)
        if not segments:
            continue

        bucket_lines: list[str] = []
        bucket_start: int | None = None
        bucket_end = 0
        bucket_size = 0
        section_chunk_ordinal = 0

        def flush_bucket() -> None:
            nonlocal bucket_lines, bucket_start, bucket_end, bucket_size, section_chunk_ordinal
            text = "\n".join(bucket_lines).strip()
            if not text or bucket_start is None:
                bucket_lines = []
                bucket_start = None
                bucket_end = 0
                bucket_size = 0
                return

            section_chunk_ordinal += 1
            retrieval_text = f"{section.heading_path}\n\n{text}"
            chunks.append(
                SectionChunk(
                    section_key=profile.section_key,
                    section_ordinal=section.ordinal,
                    chunk_ordinal=section_chunk_ordinal,
                    chunk_key=f"{profile.section_key}-chunk-{section_chunk_ordinal:03d}",
                    heading_path=section.heading_path,
                    heading=section.heading,
                    source_tag=section.source_tag,
                    section_type=profile.section_type,
                    section_priority=profile.section_priority,
                    text=text,
                    retrieval_text=retrieval_text,
                    body_char_start=bucket_start,
                    body_char_end=bucket_end,
                    char_count=len(text),
                    token_estimate=max(1, math.ceil(len(retrieval_text) / 4)),
                    is_retrieval_candidate=(
                        profile.section_priority == PRIORITY_CORE
                        and not profile.is_cover
                        and not profile.is_noise
                        and len(text) >= 80
                    ),
                    is_conditional_candidate=(
                        profile.section_priority == PRIORITY_CONDITIONAL
                        and not profile.is_cover
                        and not profile.is_noise
                        and len(text) >= 80
                    ),
                )
            )
            bucket_lines = []
            bucket_start = None
            bucket_end = 0
            bucket_size = 0

        for segment_text, start, end in segments:
            prospective_size = bucket_size + len(segment_text) + (1 if bucket_lines else 0)
            if bucket_lines and prospective_size > max_chars and bucket_size >= min_chars:
                flush_bucket()

            if bucket_start is None:
                bucket_start = start

            bucket_lines.append(segment_text)
            bucket_end = end
            bucket_size = bucket_size + len(segment_text) + (1 if len(bucket_lines) > 1 else 0)

            if bucket_size >= target_chars:
                flush_bucket()

        flush_bucket()

    return chunks


def build_qa_checks(
    sections: list[Section],
    financial_facts: list[dict[str, Any]],
    chunks: list[SectionChunk],
) -> list[QaCheck]:
    profiles = [profile_section(section) for section in sections]
    checks: list[QaCheck] = []

    sections_count = len(sections)
    checks.append(
        QaCheck(
            check_name="sections_present",
            status="pass" if sections_count >= 50 else ("warn" if sections_count > 0 else "fail"),
            metric_value=float(sections_count),
            details=f"Parsed {sections_count} sections from the filing body.",
        )
    )

    facts_count = len(financial_facts)
    checks.append(
        QaCheck(
            check_name="financial_facts_present",
            status="pass" if facts_count >= 20 else ("warn" if facts_count > 0 else "fail"),
            metric_value=float(facts_count),
            details=f"Loaded {facts_count} financial facts from OpenDART.",
        )
    )

    oversized_sections = [profile for profile in profiles if profile.body_char_count > 4000]
    max_section_chars = max((profile.body_char_count for profile in profiles), default=0)
    checks.append(
        QaCheck(
            check_name="oversized_sections",
            status="warn" if oversized_sections else "pass",
            metric_value=float(len(oversized_sections)),
            details=(
                f"{len(oversized_sections)} sections exceeded 4,000 characters; "
                f"max section length={max_section_chars}."
            ),
        )
    )

    noise_sections = [profile for profile in profiles if profile.is_noise]
    noise_ratio = (len(noise_sections) / sections_count) if sections_count else 0.0
    checks.append(
        QaCheck(
            check_name="noise_section_ratio",
            status="warn" if noise_ratio > 0.10 else "pass",
            metric_value=round(noise_ratio, 4),
            details=(
                f"{len(noise_sections)} sections were flagged as noise-like "
                f"({noise_ratio:.2%} of all sections)."
            ),
        )
    )

    retrieval_candidates = [chunk for chunk in chunks if chunk.is_retrieval_candidate]
    conditional_candidates = [chunk for chunk in chunks if chunk.is_conditional_candidate]
    checks.append(
        QaCheck(
            check_name="retrieval_chunks_present",
            status="pass"
            if len(retrieval_candidates) >= 20
            else ("warn" if retrieval_candidates else "fail"),
            metric_value=float(len(retrieval_candidates)),
            details=(
                f"Generated {len(chunks)} chunks, "
                f"{len(retrieval_candidates)} core retrieval chunks and "
                f"{len(conditional_candidates)} conditional retrieval chunks."
            ),
        )
    )

    account_names = {
        _normalize_space(str(fact.get("account_nm", "")))
        for fact in financial_facts
        if str(fact.get("fs_div", "")) == "CFS"
    } | {
        _normalize_space(str(fact.get("account_nm", "")))
        for fact in financial_facts
        if str(fact.get("fs_div", "")) == "OFS"
    }
    matched_groups = sum(
        1 for aliases in CORE_FACT_ALIASES.values() if account_names.intersection(aliases)
    )
    checks.append(
        QaCheck(
            check_name="core_metric_coverage",
            status="pass" if matched_groups == 4 else ("warn" if matched_groups >= 2 else "fail"),
            metric_value=float(matched_groups),
            details=(
                f"Matched {matched_groups}/4 core metric groups "
                f"(assets, revenue, operating income, net income)."
            ),
        )
    )

    has_cover = any(profile.is_cover for profile in profiles)
    checks.append(
        QaCheck(
            check_name="cover_section_detected",
            status="pass" if has_cover else "warn",
            metric_value=1.0 if has_cover else 0.0,
            details="Detected a cover section at the start of the filing."
            if has_cover
            else "No dedicated cover section was detected.",
        )
    )

    return checks


def summarize_section_priority_counts(sections: list[Section]) -> dict[str, int]:
    counts = {
        PRIORITY_CORE: 0,
        PRIORITY_CONDITIONAL: 0,
        PRIORITY_ARCHIVE: 0,
    }
    for section in sections:
        counts[profile_section(section).section_priority] += 1
    return counts


def summarize_chunk_priority_counts(chunks: list[SectionChunk]) -> dict[str, int]:
    counts = {
        PRIORITY_CORE: 0,
        PRIORITY_CONDITIONAL: 0,
        PRIORITY_ARCHIVE: 0,
    }
    for chunk in chunks:
        counts[chunk.section_priority] += 1
    return counts


def filter_chunks_by_priority(chunks: list[SectionChunk], priority: str) -> list[SectionChunk]:
    return [chunk for chunk in chunks if chunk.section_priority == priority]


def summarize_qa_status(checks: list[QaCheck]) -> str:
    statuses = {check.status for check in checks}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    if "pass" in statuses:
        return "pass"
    return "unknown"


def write_chunks_jsonl(path: Path, chunks: list[SectionChunk]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for chunk in chunks:
            handle.write(json.dumps(chunk.to_dict(), ensure_ascii=False))
            handle.write("\n")


def write_qa_checks_json(path: Path, checks: list[QaCheck]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps([check.to_dict() for check in checks], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
