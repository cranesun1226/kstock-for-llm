# Data

이 폴더에는 OpenDART에서 내려받아 생성한 공개 샘플/지식 산출물이 들어 있습니다.

## 포함 범위

- `raw/005930/...`: 삼성전자 2025 사업보고서 원문 ZIP과 XBRL ZIP
- `silver/005930/...`: 파싱된 섹션, 재무 fact, manifest
- `gold/005930/...`: RAG용 chunk와 QA check
- `gold/business_knowledge/20260429/`: 코스피/코스닥 2025 사업보고서의 `II. 사업의 내용` Markdown shard와 manifest
- `opendart.db`: 위 단일 회사 sync 결과를 담은 SQLite 예시 DB

## Git에 포함하지 않는 파일

일반 GitHub repository에는 100MiB 초과 단일 파일을 올릴 수 없습니다. 그래서 아래 파일은 로컬에는 남겨두되 Git에서는 제외합니다.

- `gold/business_knowledge/*/business_sections.jsonl`
- `gold/business_knowledge/*/*.partial.jsonl`

필요하면 `PYTHONPATH=src python3 -m opendart build-business-knowledge`로 다시 생성하거나, Git LFS/GitHub Releases 같은 별도 배포 방식을 사용하세요.

## 주의

데이터 출처는 OpenDART 공시 원문입니다. 이 저장소의 소스코드는 MIT License로 배포되지만, OpenDART에서 받은 공시 원문과 파생 데이터의 이용은 OpenDART/FSS 정책을 함께 따라야 합니다.

이 데이터는 투자 자문이 아니며, 실제 판단에는 DART 원문 공시를 함께 확인해야 합니다.
