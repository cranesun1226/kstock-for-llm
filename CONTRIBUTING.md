# Contributing

kstock-for-llm에 관심을 가져주셔서 감사합니다. 이 프로젝트는 한국 기업 공시를 LLM/RAG에서 쓰기 좋은 데이터로 바꾸는 작고 실용적인 파이프라인을 지향합니다.

## 개발 환경

```bash
git clone https://github.com/cranesun1226/kstock-for-llm.git
cd kstock-for-llm

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
export PYTHONPATH=src
```

OpenDART를 실제로 호출하는 변경을 테스트하려면 `.env.example`을 복사해 로컬 `.env`를 만들고 `OPENDART_API_KEY`를 설정하세요.

## 변경 전 체크

- `.env`, API key, 로컬 DB, OpenDART 원문 ZIP, 생성된 `data/` 산출물은 커밋하지 않습니다.
- 파서나 chunk 생성 로직을 바꿀 때는 가능한 한 작은 fixture 또는 단위 테스트를 함께 추가합니다.
- 공개 공시 데이터라도 대용량 원문 파일은 저장소에 넣지 않습니다.
- 투자 판단을 직접 권유하는 문구는 코드와 문서에 넣지 않습니다.

## 테스트

```bash
PYTHONPATH=src python3 -m unittest discover -s tests
```

네트워크 호출이 필요한 테스트는 기본 테스트 suite에 넣지 말고, 재현 방법을 PR 설명에 따로 적어주세요.

## Pull Request

PR에는 다음을 포함해주세요.

- 바꾼 내용의 요약
- 실행한 테스트 명령과 결과
- OpenDART API 동작이나 산출물 구조가 바뀌는 경우, 호환성 영향
- 새로 생긴 한계나 후속 작업
