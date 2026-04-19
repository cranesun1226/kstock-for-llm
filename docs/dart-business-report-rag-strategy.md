# DART 사업보고서 RAG/Agent 구축 조사 및 실행 전략

조사 기준일: 2026-04-19 (KST)

## 1. 결론 먼저

이 프로젝트는 "사업보고서 PDF/HTML을 통째로 벡터DB에 넣는 RAG"로 시작하면 거의 확실하게 한계에 부딪힙니다. 가장 좋은 방향은 아래 3계층을 분리하는 것입니다.

1. `공식 structured layer`
   OpenDART API에서 바로 주는 정형 데이터(공시목록, 사업보고서 주요정보, 주요/전체 재무제표, XBRL taxonomy)
2. `raw filing layer`
   사업보고서 원문 XML, XBRL zip, 첨부문서/첨부파일, viewer page
3. `llm retrieval layer`
   섹션 단위 chunk, 계정/연도/회사 메타데이터, 요약 트리, hybrid retrieval, agent tools

핵심 포인트는 다음입니다.

- 숫자 질문은 벡터 검색이 아니라 `정형 fact retrieval`로 풀어야 합니다.
- 서술형 질문은 사업보고서 원문/XML을 `섹션 구조`로 나눈 뒤 retrieval 해야 합니다.
- 장문 문서를 한 번에 프롬프트에 밀어 넣는 방식은 품질이 흔들립니다.
- 기존 GitHub 라이브러리는 "좋은 참고 구현"이지, "장기 운영형 데이터 플랫폼" 자체는 아닙니다.
- 우리 프로젝트의 1차 목표는 "한국 상장사 전체에 대한 신뢰 가능한 canonical filing store"를 만드는 것입니다.

## 2. 이번 조사에서 확인한 것

### 2.1 공식 DART/OpenDART로 어디까지 가능한가

OpenDART는 공식적으로 아래를 제공합니다.

- 공시검색 `list.json` / `list.xml`
- 기업개황 `company.json` / `company.xml`
- 공시서류원본파일 `document.xml`
- 정기보고서 주요정보 API 다수
- 정기보고서 재무정보 API 다수
- XBRL 재무제표 원문파일 `fnlttXbrl.xml`
- XBRL taxonomy `xbrlTaxonomy.json`

공식 소개 페이지에서도 DART 공시원문 XML 다운로드, 사업보고서 주요항목/재무계정 데이터 활용, 대용량 재무정보 활용을 명시하고 있습니다.

핵심적으로 이번 프로젝트에 직접 연결되는 공식 경로는 아래입니다.

#### A. 사업보고서 목록 찾기

공시검색 API:

- URL: `https://opendart.fss.or.kr/api/list.json`
- 사업보고서 상세유형: `A001`
- 반기보고서: `A002`
- 분기보고서: `A003`
- 최종보고서만 검색: `last_reprt_at=Y`

즉, "모든 KOSPI/KOSDAQ 종목의 최신 사업보고서"는 이 API로 충분히 inventory 를 만들 수 있습니다.

#### B. 사업보고서 원문 XML 받기

공시서류원본파일 API:

- URL: `https://opendart.fss.or.kr/api/document.xml`
- 입력: `rcept_no`
- 결과: Zip binary

이 Zip 안의 XML이 narrative RAG의 가장 중요한 원천 데이터입니다.

#### C. 재무제표 XBRL 원문 받기

재무제표 원본파일(XBRL) API:

- URL: `https://opendart.fss.or.kr/api/fnlttXbrl.xml`
- 입력: `rcept_no`, `reprt_code`
- 결과: Zip binary

이건 숫자 질의, 계정 정규화, 재무제표 비교, 주석 연결의 핵심입니다.

#### D. 전체 재무제표 fact 받기

단일회사 전체 재무제표 API:

- URL: `https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json`
- 2015년 이후 정보 제공
- `fs_div`로 연결/별도 구분 가능
- `sj_div`, `account_id`, `account_nm`, `thstrm_amount`, `frmtrm_amount` 등 제공

이 API는 "XBRL을 직접 파싱하지 않고도" 상당수 재무 질문을 안정적으로 처리하게 해줍니다.

#### E. taxonomy 받기

XBRL taxonomy API:

- URL: `https://opendart.fss.or.kr/api/xbrlTaxonomy.json`
- `sj_div`별 표준 계정체계 제공

이 taxonomy 는 계정명 동의어 처리, 표준 계정 mapping, cross-company 비교의 기준점이 됩니다.

#### F. 사업보고서 주요정보 API

OpenDART는 배당, 최대주주, 소액주주, 임원, 직원, 타법인출자, 증자/감자, 자기주식 등 사업보고서 주요정보를 개별 API로 제공합니다. 즉 "사업보고서 전체를 LLM이 읽어서 찾아내게 하는 질문" 중 상당수는 사실 API direct lookup 으로 처리 가능합니다.

### 2.2 KOSPI/KOSDAQ 전체 종목 universe 는 어떻게 잡는가

OpenDART `corpCode.xml` 에는 회사 고유번호와 종목코드가 포함되지만, 시장구분 마스터를 안정적으로 운영하려면 KRX 쪽 상장종목 master 가 같이 필요합니다.

이번 조사에서 확인한 공식 보완 소스:

- 공공데이터포털 `금융위원회_KRX상장종목정보`
- 시장구분, 종목명, 단축코드, 법인명 등을 제공
- 설명상 "영업일 하루 뒤 오후 1시 이후 업데이트"라고 되어 있음
- 개발계정 트래픽은 `10,000`

주의할 점:

- 같은 페이지 메타데이터 영역에는 `업데이트 주기 실시간`으로도 표시되어 있어 설명과 메타데이터가 완전히 일치하지 않습니다.
- 따라서 실제 운영에서는 "KRX master 는 일 단위 batch 기준"으로 보는 편이 안전합니다.

## 3. GitHub 오픈소스 조사

### 3.1 `FinanceData/OpenDartReader`

Repo:

- https://github.com/FinanceData/OpenDartReader

확인한 구조:

- `dart.py`
- `dart_list.py`
- `dart_report.py`
- `dart_finstate.py`
- `dart_share.py`
- `dart_event.py`
- `dart_regstate.py`
- `dart_utils.py`

구조 해석:

- `dart.py` 가 facade 역할을 합니다.
- `dart_list.py` 는 corp code, 공시목록, 원문 XML download 를 감쌉니다.
- `dart_report.py` 는 사업보고서 주요정보 API keyword map wrapper 입니다.
- `dart_finstate.py` 는 주요계정, 전체재무제표, XBRL 원문, taxonomy wrapper 입니다.
- `dart_utils.py` 는 DART 웹 페이지에서 하위문서/첨부문서/첨부파일을 긁는 보조 유틸입니다.

장점:

- 진입장벽이 매우 낮습니다.
- 공식 API wrapper 로는 충분히 실용적입니다.
- 종목코드/회사명 -> corp_code 변환 UX 가 좋습니다.
- 첨부문서, 첨부파일, 하위 문서 접근 유틸이 있어 실제 공시 문서 수집에 도움이 됩니다.

한계:

- production ingestion pipeline 보다는 notebook/user convenience 중심입니다.
- `docs_cache` 로컬 파일 캐시, pandas 반환, 직접 `requests.get` 호출이 섞여 있어 대규모 ETL 의 추적성과 재현성이 약합니다.
- error handling / backoff / throttling / idempotency 설계가 약합니다.
- 문서 구조 normalization 이 거의 없습니다.
- `dart_utils.py` 는 DART 웹 화면 구조에 의존하는 scraping 성격이 강합니다.

판단:

- `reference implementation` 으로는 좋습니다.
- 하지만 이 프로젝트의 코어 ingestion layer 로 그대로 채택하는 것은 추천하지 않습니다.

### 3.2 `josw123/dart-fss`

Repo:

- https://github.com/josw123/dart-fss

확인한 구조:

- 루트: `dart_fss/`, `docs/`, `pyproject.toml`
- 핵심 모듈:
  - `dart_fss/api/...`
  - `dart_fss/corp/corp_list.py`
  - `dart_fss/corp/corp.py`
  - `dart_fss/filings/reports.py`
  - `dart_fss/fs/extract.py`

구조 해석:

- `api` 레이어는 OpenDART endpoint wrapper
- `corp` 레이어는 회사 object model + market enrichment
- `filings/reports.py` 는 실제 DART 보고서, 첨부파일, 하위문서, page tree, xbrl attachment 를 object 로 다룸
- `fs/extract.py` 는 HTML/XBRL 기반 재무제표 추출, 병합, 정리 로직

장점:

- OpenDART wrapper 수준을 넘어 실제 공시문서 분석기로 발전해 있습니다.
- `Report` 객체가 관련보고서, 첨부보고서, 첨부파일, 페이지, XBRL viewer 를 묶어줍니다.
- XBRL이 있으면 우선 사용하고, 없으면 HTML fallback 을 시도합니다.
- 여러 기간의 재무제표를 merge 하는 로직이 있습니다.
- `pyproject.toml` 기준 `arelle-release` 의존성을 사용합니다. 즉 XBRL 파싱 스택이 비교적 진지합니다.

한계:

- 라이브러리 자체가 꽤 큰 파서이기 때문에 운영 서비스용 핵심 parser 로 그대로 의존하면 디버깅 비용이 큽니다.
- HTML 구조 추정, 제목 regex, 표 파싱, 여러 fallback 로직이 섞여 있어 장기 유지보수 비용이 큽니다.
- object graph 와 pandas 중심 구조라 데이터 레이크/warehouse 친화적이지 않습니다.
- "질문 답변용 canonical corpus" 보다는 "재무제표를 뽑아보는 라이브러리"에 더 가깝습니다.

판단:

- `수집/파싱 아이디어 참고용`으로는 매우 유용합니다.
- 특히 `Report` 객체가 어떻게 DART 원문, 첨부, XBRL 을 연결하는지 참고할 가치가 큽니다.
- 하지만 우리 프로젝트는 더 작은 단위의 명시적 pipeline 으로 재구성하는 편이 낫습니다.

### 3.3 기타 참고 가치가 있는 오픈소스

- `sharebook-kr/pykrx`
  - KRX 시세/시장 데이터 쪽 참고용
  - 다만 공식 데이터 플랫폼 코어보다는 부가 시장데이터 보완용
- `Arelle`
  - 범용 XBRL 파서/검증 스택
  - DART XBRL 을 장기적으로 깊게 다루려면 직접 검토 가치가 큼
- `dgunning/edgartools`
  - 미국 SEC filing 생태계에서 filing + xbrl + retrieval usable object model 을 어떻게 묶는지 참고할 만함

## 4. 우리 프로젝트에 대한 판단

### 4.1 무엇을 직접 만들고, 무엇을 재사용할 것인가

직접 만들 것:

- issuer master 통합 로직
- filing inventory 동기화
- raw download orchestration
- XML/XBRL/attachment parsing pipeline
- canonical schema
- chunking / embedding / indexing
- agent tool layer
- evaluation / freshness / provenance

부분 재사용 또는 참고할 것:

- OpenDartReader: endpoint naming, 간단한 wrapper UX, attachment 탐색 힌트
- dart-fss: Report/page/xbrl 연결 방식, XBRL fallback 아이디어
- Arelle: 깊은 XBRL 처리 필요 시

직접 코어로 채택하지 않을 것:

- OpenDartReader 전체
- dart-fss 전체

이유:

- 장기 운영형 RAG 시스템은 "라이브러리 사용성"보다 "데이터 계층 분리, 추적 가능성, 재처리 가능성, 증분 수집"이 더 중요하기 때문입니다.

## 5. 추천 아키텍처

### 5.1 데이터 레이어

#### Layer 0. Issuer Master

테이블 예시:

- `issuers`
- `issuer_market_history`
- `issuer_aliases`
- `issuer_identifiers`

핵심 필드:

- `corp_code`
- `stock_code`
- `market` (`KOSPI`, `KOSDAQ`, `KONEX`, etc.)
- `corp_name_ko`
- `corp_name_en`
- `isin`
- `biz_reg_no`
- `active_from`
- `active_to`

데이터 소스:

- OpenDART `corpCode.xml`
- 금융위원회_KRX상장종목정보

#### Layer 1. Filing Catalog

테이블 예시:

- `filings`
- `filing_versions`
- `filing_artifacts`

핵심 필드:

- `rcept_no`
- `corp_code`
- `stock_code`
- `report_nm`
- `pblntf_ty`
- `pblntf_detail_ty`
- `rcept_dt`
- `is_final`
- `source_url`
- `parent_rcept_no`

포인트:

- 정정/첨부추가/변경등록 등을 lineage 로 남겨야 합니다.
- `last_reprt_at=Y` 로 최신본을 찾더라도, 원본 lineage 는 별도 보관하는 것이 좋습니다.

#### Layer 2. Raw Filing Store

저장 대상:

- `document.xml` zip 원본
- `fnlttXbrl.xml` zip 원본
- attachment file 원본
- viewer page raw html

원칙:

- raw 는 가공 없이 immutable 저장
- 파일 해시 기록
- 재처리 가능해야 함

#### Layer 3. Normalized Facts

테이블 예시:

- `filing_sections`
- `filing_section_chunks`
- `xbrl_facts`
- `xbrl_contexts`
- `xbrl_units`
- `report_major_items`
- `financial_statements_api_facts`

원칙:

- structured fact 와 narrative chunk 를 분리
- 출처를 항상 `rcept_no`, `section_path`, `page`, `file_name`, `offset` 와 연결

#### Layer 4. Retrieval Index

인덱스 예시:

- dense vector index
- sparse/BM25 index
- summary tree index

메타데이터 예시:

- `corp_code`
- `stock_code`
- `market`
- `fiscal_year`
- `reprt_code`
- `section_type`
- `heading_path`
- `chunk_type`
- `is_latest`
- `source_priority`

### 5.2 파싱 전략

가장 중요한 원칙은 `질문 유형별로 파싱 소스를 다르게` 가져가는 것입니다.

#### 숫자/표/재무 질문

우선순위:

1. `fnlttSinglAcntAll.json`
2. `fnlttSinglAcnt.json` / `fnlttMultiAcnt.json`
3. `fnlttXbrl.xml` 직접 파싱
4. HTML/table fallback

이유:

- 숫자는 정형 API가 있으면 정형 API를 신뢰하는 편이 오류가 적습니다.
- XBRL 직접 파싱은 coverage 를 넓히지만 구현 복잡도가 올라갑니다.
- HTML table parsing 은 최후 fallback 으로 두는 게 맞습니다.

#### 서술형 질문

우선순위:

1. `document.xml` 원문 XML
2. attachment 문서
3. viewer page html

이유:

- 사업의 내용, 위험요인, 주요 계약, 종속회사 현황, 경영진 의견 등은 narrative 중심입니다.
- 이 부분은 원문 XML의 섹션 구조를 살려서 저장해야 retrieval 품질이 좋아집니다.

### 5.3 Chunking 전략

권장:

- 1차 chunk: `문서 섹션 단위`
- 2차 chunk: 긴 섹션만 `800~1500 token` 내외로 재분할
- `10~15% overlap`
- 각 chunk 에 부모 섹션 요약(summary)와 heading path 저장

예시 metadata:

- `heading_path`: `III. 재무에 관한 사항 > 5. 재무제표 주석 > 금융상품 위험관리`
- `section_level`
- `chunk_order`
- `section_summary`
- `evidence_span`

고정 길이 chunk 만 쓰면 안 되는 이유:

- 사업보고서는 제목 구조가 강합니다.
- "사업의 내용", "재무에 관한 사항", "이사의 경영진단 및 분석의견" 같은 섹션 경계를 보존해야 agent 가 근거를 설명할 수 있습니다.

### 5.4 LLM/RAG 제공 방식

최고 성능을 내려면 "한 가지 retrieval"이 아니라 최소 3가지를 조합해야 합니다.

#### Retrieval A. Structured retrieval

대상:

- 매출, 영업이익, 자산총계, 직원수, 배당, 최대주주, 임원보수 등

형태:

- SQL / key-value lookup / canonical fact store

반환:

- JSON
- table
- source citation (`rcept_no`, account_id, year)

#### Retrieval B. Narrative retrieval

대상:

- 사업 내용
- 리스크
- 회사의 전략
- 주요 계약
- 종속회사/계열사 설명

형태:

- hybrid search (BM25 + dense)
- section-aware reranking

#### Retrieval C. Hierarchical retrieval

대상:

- "올해와 작년을 비교해서 핵심 변화 요약"
- "이 회사의 장기 리스크를 알려줘"
- "사업보고서 전체에서 투자자가 중요하게 봐야 할 점"

형태:

- 연도 요약
- 섹션 요약
- chunk retrieval

즉, user question 이 들어오면:

1. question classifier 가 질문 유형을 분류
2. structured / narrative / comparative route 를 선택
3. 필요한 경우 둘 이상을 병합
4. answer composer 가 근거와 함께 출력

### 5.5 왜 long-context stuffing 만으로는 부족한가

이번 조사에서 참고한 논문:

- `Lost in the Middle: How Language Models Use Long Contexts`
- `RAPTOR: Recursive Abstractive Processing for Tree-Organized Retrieval`

시사점:

- 긴 문맥 전체를 그냥 던져도 모델이 중간 정보 활용을 잘 못할 수 있습니다.
- 문서 전체 요약과 세부 chunk 를 함께 쓰는 hierarchical retrieval 이 긴 문서 QA에 더 유리합니다.

따라서 권장 방식:

- "원문 전체를 한 번에 먹이기"보다
- "요약 트리 + 정밀 chunk retrieval + structured facts" 조합이 낫습니다.

## 6. Agent 설계 제안

### 6.1 Agent 가 직접 읽어야 하는 것

- 섹션 요약
- chunk evidence
- 비교용 정형 fact

### 6.2 Agent 가 tool 로 호출해야 하는 것

- `get_latest_annual_report(corp_code, year=None)`
- `search_sections(corp_code, year, query, section_type=None)`
- `get_major_report_item(corp_code, year, item_name)`
- `get_financial_fact(corp_code, year, account_id|account_nm, fs_div='CFS')`
- `compare_financial_facts(corp_code, years, accounts)`
- `list_attachments(rcept_no)`
- `get_raw_section(rcept_no, section_id)`

### 6.3 Agent 출력 원칙

- 숫자는 가능하면 structured fact 에서만 답변
- 서술형 답변에도 반드시 `rcept_no` 와 section citation 포함
- "최신 사업보고서 기준"과 "특정 연도 기준"을 명확히 구분
- 정정 공시가 있으면 최신본 사용 여부를 답변에 드러냄

## 7. 저장소 구조 추천

초기 추천 구조:

```text
src/kstock/
  config/
    settings.py

  clients/
    opendart_client.py
    krx_client.py

  ingestion/
    issuer_master.py
    filing_catalog.py
    raw_downloader.py
    scheduler.py

  parsers/
    document_xml_parser.py
    xbrl_parser.py
    attachment_parser.py
    sectionizer.py

  normalize/
    filings.py
    facts.py
    taxonomy.py

  storage/
    models.py
    repositories.py
    object_store.py

  index/
    chunker.py
    embedder.py
    sparse_index.py
    vector_index.py
    summary_tree.py

  retrieval/
    structured.py
    narrative.py
    hybrid.py
    reranker.py

  agents/
    tools.py
    planner.py
    answering.py

  eval/
    gold_questions.py
    scorer.py
    regression.py

  cli/
    main.py

data/
  raw/
  bronze/
  silver/
  gold/

docs/
  dart-business-report-rag-strategy.md
```

## 8. 구현 순서 제안

### Phase 1. 최소 ingestion backbone

목표:

- KOSPI/KOSDAQ issuer master 확보
- 사업보고서 inventory 확보
- raw XML/XBRL download

우선 구현:

1. OpenDART client
2. KRX listed master client
3. issuer sync
4. filing search sync
5. raw artifact downloader

완료 기준:

- 특정 연도 전체 상장사의 사업보고서 raw archive 를 안정적으로 적재 가능

### Phase 2. canonical parsing

목표:

- narrative sections + structured facts 분리 저장

우선 구현:

1. `document.xml` section parser
2. `fnlttSinglAcntAll.json` ingest
3. taxonomy loader
4. section metadata schema
5. citation/provenance schema

완료 기준:

- "삼성전자 2025 사업보고서에서 위험요인 section 찾기"
- "POSCO DX 2025 사업보고서 기준 연결 영업이익 확인"
같은 질의를 deterministic 하게 지원 가능

### Phase 3. retrieval + QA

목표:

- RAG baseline

우선 구현:

1. section-aware chunking
2. dense embedding
3. BM25
4. hybrid retrieval
5. answer composer

완료 기준:

- citation 포함 QA baseline 완성

### Phase 4. agent/harness

목표:

- 비교, 요약, 다단계 질의

우선 구현:

1. company/year selection tool
2. structured fact tool
3. narrative search tool
4. compare tool
5. evidence-grounded final response formatter

완료 기준:

- "코스닥 AI 업체들 중 최근 사업보고서 기준 인력 증가와 적자 지속이 동시에 나타나는 회사 찾아줘"
같은 질의를 tool chain 으로 해결 가능

## 9. 기술 선택 제안

### 9.1 DB / Index

초기:

- `PostgreSQL + pgvector`
- raw 파일은 로컬 파일시스템 또는 object store
- batch 분석용 `Parquet + DuckDB`

이유:

- metadata filter + SQL + vector 를 한 곳에서 다루기 좋음
- 초기 운영 복잡도가 낮음
- later stage 에 OpenSearch 로 분리해도 migration 경로가 명확함

### 9.2 Embedding / Retrieval

권장:

- dense embedding + BM25 hybrid
- reranker 추가
- section summary / year summary tree 별도 보관

이유:

- 한국어 회사명, 종목코드, 계정명, 약어, 고유명사 때문에 sparse 검색이 매우 중요함
- 반대로 의미 검색 때문에 dense 도 필요함

### 9.3 Parser

권장:

- narrative: 직접 XML parser
- 숫자: OpenDART structured API 우선
- 깊은 XBRL: 필요 시 Arelle 계열 파서

이유:

- "무조건 XBRL 직접 파싱"으로 시작하면 초반 속도가 매우 느려집니다.
- 먼저 API 로 커버 가능한 정형 영역을 안정화하고, 이후 XBRL deep parsing 으로 넓히는 게 맞습니다.

## 10. 리스크와 주의사항

### 10.1 요청 제한

공식 개발가이드의 여러 API 는 에러 코드 `020` 을 요청 제한 초과로 설명하며, 일반적으로 `20,000건 이상의 요청`에서 발생할 수 있다고 안내합니다. 커뮤니티 라이브러리들은 별도로 분당 과다 요청 제한도 주의사항으로 언급합니다. 따라서 실제 운영에서는 보수적으로 rate limit 을 잡아야 합니다.

실무 권장:

- 전역 rate limiter
- endpoint 별 budget
- raw download queue 분리
- retry with jitter
- 429/020/012 감지

### 10.2 커버리지 차이

- 전체 재무제표 API는 `2015년 이후`
- 금융업/특수업종은 표준화 coverage 차이가 있을 수 있음
- 서술형 섹션은 회사마다 제목/구조가 조금씩 다름

### 10.3 정정 공시

- 최신본만 보지 말고 lineage 저장 필요
- user facing answer 는 기본 최신본 기준
- audit/research 모드에서는 과거 정정본도 조회 가능해야 함

### 10.4 provenance

RAG 품질보다 더 중요한 건 "근거를 재현 가능하게 남기는 것"입니다.

최소한 아래는 항상 저장해야 합니다.

- `rcept_no`
- source url
- file hash
- section path
- page or node id
- parse version
- chunk version
- embedding version

## 11. 최종 추천

가장 추천하는 방향은 아래입니다.

1. OpenDART 공식 API 를 ingestion backbone 으로 삼는다.
2. 사업보고서 원문 XML 과 XBRL zip 을 raw 로 모두 저장한다.
3. 숫자와 서술을 분리한 canonical schema 를 만든다.
4. hybrid retrieval + hierarchical summaries 를 붙인다.
5. agent 는 raw text reader 가 아니라 `tool user` 로 설계한다.

즉, 이 프로젝트의 본질은 "DART wrapper 만들기"가 아니라 아래입니다.

`한국 상장사 사업보고서를 기계가 신뢰 가능하게 읽고, 비교하고, 인용할 수 있는 canonical research platform 만들기`

## 12. 바로 다음 액션 제안

제가 다음 턴에서 바로 이어서 할 수 있는 가장 좋은 작업은 아래 3가지 중 하나입니다.

1. `src/` 기준으로 실제 프로젝트 skeleton 코드까지 생성
2. OpenDART/KRX client 와 초기 ingestion 코드부터 구현
3. docs 를 더 쪼개서 `architecture.md`, `data-model.md`, `roadmap.md` 로 분리

## 13. 참고 링크

### 공식

- OpenDART 소개: https://opendart.fss.or.kr/intro/main.do
- 공시검색 개발가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001
- 공시서류원본파일 개발가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003
- 정기보고서 주요정보 목록: https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS002
- 정기보고서 재무정보 목록: https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS003
- 재무제표 원본파일(XBRL): https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019019
- 단일회사 전체 재무제표: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020
- XBRL taxonomy: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2020001
- KRX 상장종목정보: https://www.data.go.kr/data/15094775/openapi.do

### GitHub / 라이브러리

- OpenDartReader: https://github.com/FinanceData/OpenDartReader
- dart-fss: https://github.com/josw123/dart-fss
- pykrx: https://github.com/sharebook-kr/pykrx
- Arelle: https://github.com/arelle
- edgartools: https://github.com/dgunning/edgartools

### 논문

- RAPTOR: https://arxiv.org/abs/2401.18059
- Lost in the Middle: https://arxiv.org/abs/2307.03172
