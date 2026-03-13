# BriefAlpha Handoff

## 1) Project Snapshot
- 서비스: BriefAlpha (주식 뉴스/리포트 수집 + 요약/브리핑)
- 현재 핵심 파이프라인:
  - 수집: `scripts/run_collect.py` -> `stock_mvp/pipeline.py`
  - 요약 엔진(신규): `stock_mvp/agents/*`
    - Item: `item_summaries` + `evidence_cards`
    - Entity Daily Digest: `daily_digests`
    - AI Report: `agent_reports`

## 2) What Is Implemented
- DB 테이블 추가됨:
  - `item_summaries`
  - `evidence_cards`
  - `daily_digests`
  - `agent_reports`
- 수집 후 요약 흐름이 기존 `summaries` 중심에서 에이전트 중심으로 교체됨:
  - `stock_mvp/pipeline.py`
- 웹에서 신규 산출물 확인 가능하도록 연동됨:
  - 대시보드: ticker digest line1 기반 목록
  - 종목 상세: 8-line digest, Change-3, Open Questions, Evidence refs, item-level 요약, agent report
  - 파일: `stock_mvp/web.py`, `stock_mvp/templates/index.html`, `stock_mvp/templates/stock_detail.html`

## 3) Local Run (Home PC)
1. 저장소 동기화
   - `git pull origin <branch>`
2. 가상환경/의존성
   - `conda activate <your_env>`
   - `pip install -r requirements.txt`
3. 환경변수
   - `.env.example` 참고해서 `.env` 구성
4. DB 초기화/보정
   - `python scripts/bootstrap_db.py`
5. 수집+요약 실행
   - `python scripts/run_collect.py --market KR`
6. 웹 실행
   - `python scripts/run_server.py`
   - 브라우저: `http://127.0.0.1:5000`

## 4) Quick Verification Checklist
- 대시보드(`/kr`, `/us`)에서 종목 카드에 digest 1줄 표시되는지
- 종목 상세에서 아래 블록이 보이는지
  - Daily Digest 8-line
  - Change-3
  - Open Questions
  - Evidence Refs
  - 최근 Item 요약
  - AI Report(조건 충족 시)
- `/ops/runs`에서 최신 run 상태 확인

## 5) Known Notes
- `stock_mvp/web.py`에 구 요약용 헬퍼 일부가 남아있을 수 있으나 현재 핵심 화면은 신규 에이전트 테이블 기준으로 동작.
- Windows PowerShell profile 경고(`profile.ps1`)는 명령 실행 자체에는 큰 영향 없음.
- 요약 품질가드 경고 확인 포인트:
  - `item_summarizer llm invalid summary payload`
  - `entity_digest llm invalid payload`
  - 위 경고가 반복되면 `stock_mvp/agents/summary_quality.py` 기준으로 fallback이 동작한 상태이므로 프롬프트/입력 본문 길이를 점검해야 함.

## 6) Suggested Next Work
1. 종목-뉴스 매핑 정확도 고도화 (오탐 줄이기)
2. PDF 파싱 품질 향상 (리포트 본문 품질)
3. 요약 프롬프트/포맷 고도화
4. 섹터 요약 전용 페이지 추가
5. 백테스트 UX 연동 강화
6. 로그인/사용자별 데이터 분리

## 7) Session Resume Prompt (추천)
아래 문장을 새 세션 첫 메시지로 사용:

`HANDOFF.md와 CHATHISTORY.md를 먼저 읽고, 현재 브랜치 상태를 점검한 뒤 남은 작업 우선순위 3개를 제안해줘.`
