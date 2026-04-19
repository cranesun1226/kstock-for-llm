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
KSTOCK_DATA_DIR=data
KSTOCK_DB_PATH=data/app.db
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

- `data/app.db`
- `data/raw/005930/2025/<rcept_no>/document.zip`
- `data/raw/005930/2025/<rcept_no>/xbrl.zip` 또는 미존재
- `data/silver/005930/2025/<rcept_no>/sections.json`
- `data/silver/005930/2025/<rcept_no>/financial_facts.json`
- `data/silver/005930/2025/<rcept_no>/manifest.json`

## 현재 범위

이번 단계는 단일 회사·단일 연도의 vertical slice 검증입니다.

- `KRX master sync`
- `배치 스케줄링`
- `vector index`
- `agent tool orchestration`

는 아직 포함하지 않았습니다.

## 문서

- 전략 문서: [docs/dart-business-report-rag-strategy.md](docs/dart-business-report-rag-strategy.md)
