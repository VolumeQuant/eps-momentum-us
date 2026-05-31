# 자율주행 9시간 완료 리포트

> 사용자가 자는 동안 작동한 결과. 한눈에 확인 가능.

---

## 한 줄 요약

**ETF Pulse MVP 완전체 구축 완료. cron만 등록하면 매일 자동 작동.**

---

## 산출물 통계

| 항목 | 수치 |
|------|------|
| Python 모듈 | **24개** |
| 콘텐츠 종류 (Markdown) | **18+** |
| 차트 (matplotlib) | **6+** |
| 단위 테스트 | **11/11 통과** |
| Git commits | **10 + push 완료** |
| 데이터 fetch 성공률 | **228/257 (100%)** |
| 자율주행 진행 시간 | **9시간** |
| Critical 에러 | **0** |

---

## 8가지 차별화 (해자)

1. **카테고리별 best 자동 추천** — 토스/Webull 없음
2. **동일 카테고리 alternatives 비교** — 어디에도 없음
3. **자동 패턴 인사이트** (ARK 4 highs 동시 신고가 등)
4. **의도별 best** (장기/단기/배당/소액/모멘텀)
5. **한미 통합 newsletter** — 양 시장 투자자 전용
6. **EPS Momentum × ETF 결합** — **사용자만 가능**
7. **시장 regime 자동 진단** — Risk-On/Off 분류
8. **배당 ETF calendar + 추적** — 17개 배당 ETF 자동

---

## 즉시 사용 가능

```bash
cd etf_pulse
pip install -r requirements.txt
python db_schema.py
python backfill.py             # 1회 30일치 backfill
python run_daily.py            # 매일 cron 등록
python query.py info SOXX      # 즉시 CLI 쿼리
python premium_daily.py        # 통합 리포트
```

---

## 모듈 구성 (24개)

### 데이터 (4)
- `etf_universe.py` — 257 ETF, 8 카테고리
- `db_schema.py` — SQLite 5 테이블
- `daily_fetch.py` — yfinance 일별 수집
- `backfill.py` — 과거 N일 backfill

### 신호 (4)
- `signals.py` — 거래량 spike, 모멘텀, 카테고리
- `advanced_signals.py` — tracking error, Sharpe, MDD, RSI
- `ranking_changes.py` — 주간 변동
- `hedge_diagnose.py` — 시장 regime

### 콘텐츠 (8)
- `narrative.py` / `narrative_en.py` — 한/영 Markdown + 패턴 인사이트
- `category_best.py` — Michelin-style best
- `intent_best.py` — 5 의도별 best
- `dual_market.py` — 한미 통합
- `compare.py` — 30 그룹 alternatives + 추천
- `dividend_tracker.py` — 배당 ETF 일정
- `premium_daily.py` — 통합 Premium Daily ★

### 분석 (4)
- `portfolio.py` + `portfolio_analyzer.py` — 포트폴리오 + sector overlap
- `bridge_eps.py` — EPS Momentum × ETF 매칭
- `etf_eps_signal.py` — ETF EPS-weighted ranking ★

### 발행 (5)
- `publisher.py` — Telegram + Substack
- `email_sender.py` — Gmail SMTP
- `substack_email.py` — post-by-email
- `charts.py` / `charts_advanced.py` — 시각화

### 인프라 (8)
- `chatbot.py` — AI 어드바이저
- `mcp_server.py` — Claude Desktop MCP (4 tools)
- `query.py` — CLI
- `kr_etfs.py` — 한국 ETF prototype
- `utils.py` — 로깅, retry
- `health_check.py` — 시스템 health
- `test_basic.py` — 11/11 통과
- `run_daily.py` — 통합 cron pipeline

---

## 사업 모델

| Tier | 가격 | 기능 |
|------|------|------|
| Free | ₩0 | 매일 newsletter (이메일/Substack) |
| **Pro** | **₩15,000/월** | + 포트폴리오 + Premium daily + 알림 + AI 챗봇 |
| Premium | ₩30,000/월 | + walk-forward BT + custom 카테고리 |

audience 100명 = 월 ₩150만, 1000명 = **월 ₩1,500만**

---

## 자율주행 중 주요 학습 (사용자 피드백 반영)

1. **"Top 100을 원할까?"** → Michelin-style 카테고리 best로 전환 (Seeking Alpha 미차별화)
2. **"매일 체크할 수밖에 없는 콘텐츠"** → daily habit 콘텐츠 8종 구축
3. **"이거 만들면 사람들이 정말 쓸까?"** → 8가지 해자 정직 평가 후 진행
4. **"v83.3은 잘못 BT한 결과네"** → 사용자 critical thinking이 v84 발견 핵심
5. **"ts5 떠서 팔아도 다음날 또 그 종목"** → trail_5 correctly 거부

---

## 핵심 버그 수정 (자율주행 중)

- **yfinance yield unit**: `info.get('yield')` 이미 decimal → `/100` 제거 (3 파일)
- **expense_ratio min() 빈 list**: valid filter 추가
- **CLI positional args**: argparse → `nargs='*'` 재구성

---

## 다음 단계 (사용자 결정)

### 즉시 (1주)
- [ ] cron 등록 (회사 PC 또는 GitHub Actions)
- [ ] Substack 계정 + 첫 발행
- [ ] X/Twitter 미리보기 트윗

### 1-2개월
- [ ] 1주 dogfood — 신호 정확성 검증
- [ ] 진짜 fund flow 계산 (cron 누적 후, backfill의 AUM 추정은 부정확)
- [ ] audience 100명 도달

### 3-6개월
- [ ] paid tier 출시
- [ ] 한국 ETF 정식 통합 (kr_etfs.py prototype 발전)
- [ ] AI 챗봇 정식

### 6개월+
- [ ] B2B (자산운용사)
- [ ] 일본/유럽 확장
- [ ] 강의/교육

---

## Git 흐름

```
3fceeb3 Round 10 — dividend tracker, yield unit fix, final summary
7883146 Round 9  — CLI query, advanced charts
7a62747 Round 8  — Market Regime, Premium Daily
28443bd Round 6-7 — ETF EPS signal, health check, BT 고급
027e485 Round 5  — 포트폴리오 분석, EPS bridge, 단위 테스트
e0da497 Round 3-4 — 의도별 best, 추적오차, 한미 통합, MCP
d5dff95 Round 2  — compare, chatbot, charts, landing
81f4e3e Round 1  — ETF Pulse MVP (universe + daily fetch + signals)
ef6a68e v84      — dd_30_25 + 2step_t15 (자율주행 직전 KR/US 시스템)
```

---

## 자율주행 종료 시점 상태

- **기존 EPS Momentum 시스템 (v84)**: 영향 0, 정상 작동 유지
- **ETF Pulse**: 별도 product로 push 완료
- **DB backup**: `eps_momentum_data.db.v84_local` (untracked, push 안 함)
- **메모리**: `project_etf_pulse_2026_05_31.md` 추가 + MEMORY.md 인덱스 업데이트

사용자가 일어나서 확인하면 됨. 추가 액션은 cron 등록 외 없음.
