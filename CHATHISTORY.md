# BriefAlpha Chat History

## 목적
- 세션이 바뀌어도 작업 맥락을 잃지 않기 위한 기록 파일.
- "대화기록이 없어도 이어서 작업" 가능하도록, 결정/변경/다음 할 일을 남긴다.

## 기록 규칙
1. 각 세션 시작 시 최신 항목 1개를 읽고 시작.
2. 각 세션 종료 시 아래 템플릿으로 1개 항목 추가.
3. 상세 로그보다 "결정/변경/다음 액션" 위주로 짧게 기록.
4. 민감정보(API Key, 토큰, 비밀번호)는 절대 기록하지 않음.

## 세션 템플릿
```md
## [YYYY-MM-DD HH:MM] Session Title
- Context:
- User Request:
- Decisions:
- Changes:
  - files:
  - behavior:
- Commands Run:
- Validation:
- Open Issues:
- Next Actions:
```

---

## [2026-02-26 17:00] Agent 기반 요약 체계 전환 + 웹 연동
- Context:
  - 기존 `summaries` 중심 요약 구조에서 에이전트 기반 구조로 전환 필요.
- User Request:
  - item/sector/ticker/report 산출물을 일관 규칙으로 생성하고, 웹에서도 확인 가능하게 반영.
- Decisions:
  - 수집 파이프라인은 유지, 요약 산출물은 신규 테이블(`item_summaries/evidence_cards/daily_digests/agent_reports`) 중심으로 전환.
  - 기존 `run_collect` 경로에서 요약 생성을 신규 에이전트로 교체.
- Changes:
  - files:
    - `stock_mvp/database.py`
    - `stock_mvp/agents/*`
    - `stock_mvp/storage/*`
    - `scripts/run_agents.py`
    - `stock_mvp/pipeline.py`
    - `stock_mvp/web.py`
    - `stock_mvp/templates/index.html`
    - `stock_mvp/templates/stock_detail.html`
    - `scripts/run_collect.py`
  - behavior:
    - 수집 후 item/evidence/digest/report가 생성됨.
    - 웹에서 digest/item summary/report 중심으로 확인 가능.
- Commands Run:
  - `python -m py_compile ...`
  - `python scripts/run_agents.py ...`
  - `python scripts/run_collect.py ...`
  - `python -c "from stock_mvp.web import create_app ..."`
- Validation:
  - 컴파일 통과
  - 앱 초기화 성공 (`routes 22`)
- Open Issues:
  - 화면/문구/포맷 세부 개선 필요.
  - 일부 레거시 헬퍼 정리 여지 있음.
- Next Actions:
  1. 화면 polish 및 요약 UX 개선
  2. 종목-뉴스 매핑 정확도 개선
  3. PDF 처리 품질 강화

---

## [NEXT SESSION] Append Here
- Context:
- User Request:
- Decisions:
- Changes:
- Commands Run:
- Validation:
- Open Issues:
- Next Actions:

