# kstock-for-llm

OpenDART 기반으로 한국 상장사의 사업보고서를 수집하고, LLM/RAG 에서 쓰기 좋은 형태로 정규화하는 프로젝트의 초기 vertical slice 입니다.

현재 패치에서 제공하는 기능:

- `stock_code` 또는 `corp_code` 로 OpenDART 회사 식별
- 특정 사업연도의 최신 사업보고서 탐색
- 공시 원문 XML ZIP 저장
- 재무제표 XBRL ZIP 저장
- 기본 섹션 파싱 후 JSON/SQLite 저장
- 전체 재무제표 fact JSON/SQLite 저장

## Quickstart

### 1. 의존성 설치

```bash
python3 -m pip install -r requirements.txt
```

현재 vertical slice 는 표준 라이브러리만 사용하므로, 위 명령은 사실상 no-op 입니다.

### 2. `.env` 설정

```bash
OPENDART_API_KEY=your_api_key_here
```

선택 설정:

```bash
OPENDART_DATA_DIR=data
OPENDART_DB_PATH=data/opendart.db
```

### 3. 삼성전자 2025 사업보고서 동기화

```bash
PYTHONPATH=src python3 -m opendart sync-report --stock-code 005930 --year 2025
```

반복 실행이 많다면 아래처럼 세션에 한 번 설정해두면 편합니다.

```bash
export PYTHONPATH=src
python3 -m opendart --help
```

정상 실행 시 아래 결과가 생성됩니다.

- `data/opendart.db`
- `data/raw/005930/2025/annual-report_2025-12_20260310_002820/document.zip`
- `data/raw/005930/2025/annual-report_2025-12_20260310_002820/xbrl.zip` 또는 미존재
- `data/silver/005930/2025/annual-report_2025-12_20260310_002820/sections.json`
- `data/silver/005930/2025/annual-report_2025-12_20260310_002820/financial_facts.json`
- `data/silver/005930/2025/annual-report_2025-12_20260310_002820/manifest.json`
- `data/gold/005930/2025/annual-report_2025-12_20260310_002820/chunks.jsonl`
- `data/gold/005930/2025/annual-report_2025-12_20260310_002820/core_chunks.jsonl`
- `data/gold/005930/2025/annual-report_2025-12_20260310_002820/conditional_chunks.jsonl`
- `data/gold/005930/2025/annual-report_2025-12_20260310_002820/qa_checks.json`

`opendart.db` 에는 현재 아래 계층이 함께 적재됩니다.

- canonical: `issuers`, `filings`, `sections`, `financial_facts`, `filing_artifacts`
- derived: `section_chunks`, `qa_checks`
- operations: `sync_runs`

`core_chunks.jsonl` 는 LLM 기본 공급용 chunk pool 이고,
`conditional_chunks.jsonl` 는 감사/주주/지배구조/특수관계자 거래 등
질문이 맞을 때만 추가로 여는 chunk pool 입니다.

현재 chunk 우선순위는 아래처럼 구분됩니다.

- `core`: `I. 회사의 개요`, `II. 사업의 내용`, `III. 재무에 관한 사항`, `IV. 이사의 경영진단 및 분석의견`, `XI. 그 밖에 투자자 보호를 위하여 필요한 사항`
- `conditional`: `V. 회계감사인의 감사의견 등`, `VII. 주주에 관한 사항`, `X. 대주주 등과의 거래내용`, 일부 지배구조/직원/계열회사 요약 구간
- `archive`: `XII. 상세표`, 표지, 대표이사 확인, 상세 임원 현황, 대규모 표/부속자료

## 현재 범위

이번 단계는 단일 회사·단일 연도의 vertical slice 검증입니다.

- `KRX master sync`
- `배치 스케줄링`
- `vector index`
- `agent tool orchestration`

는 아직 포함하지 않았습니다.

## 문서

- 전략 문서: [docs/dart-business-report-rag-strategy.md](docs/dart-business-report-rag-strategy.md)
