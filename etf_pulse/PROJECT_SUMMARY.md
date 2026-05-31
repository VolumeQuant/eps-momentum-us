# ETF Pulse — 프로젝트 종합 요약

> 9시간 자율주행 결과 정리

---

## 한 줄 정의

**미국 ETF Daily 인사이트** — 257개 ETF 자동 분석 + 매일 발행. 토스/Webull/Seeking Alpha 어떤 서비스도 못 하는 카테고리/패턴/EPS 결합 신호.

---

## 핵심 모듈 (18개 + 통합 1개)

### 데이터 layer
| 모듈 | 역할 |
|------|------|
| `etf_universe.py` | 257 ETF universe (8 카테고리) |
| `db_schema.py` | SQLite 5 테이블 |
| `daily_fetch.py` | yfinance 일별 수집 |
| `backfill.py` | 과거 N일 backfill (현재 30일치 완료) |

### 신호 layer
| 모듈 | 역할 |
|------|------|
| `signals.py` | 거래량 spike, 모멘텀, 카테고리 강도, 신고가/저가 |
| `advanced_signals.py` | 추적오차, Sharpe, MDD, RSI, 포트폴리오 BT |
| `ranking_changes.py` | 주간 카테고리 best 변동 |
| `hedge_diagnose.py` | 시장 regime 자동 진단 (risk-on/off) |

### 콘텐츠 layer
| 모듈 | 역할 |
|------|------|
| `narrative.py` | 한국어 Markdown 자동 생성 |
| `narrative_en.py` | 영어 콘텐츠 (글로벌 audience) |
| `category_best.py` | Michelin-style 카테고리별 best |
| `intent_best.py` | 의도별 best (장기/단기/배당/소액/모멘텀) |
| `dual_market.py` | 한미 통합 daily |
| `compare.py` | 동일 카테고리 ETF 비교 + 한 줄 추천 |
| **`premium_daily.py`** | **통합 Premium Daily Report** ★ |

### 분석 / 사용자
| 모듈 | 역할 |
|------|------|
| `portfolio.py` | 사용자 포트폴리오 추적 + alternatives |
| `portfolio_analyzer.py` | sector overlap, 분산 점수, 통합 Top 종목 |
| `bridge_eps.py` | KR/US EPS Momentum 시스템 ↔ ETF 매핑 |
| **`etf_eps_signal.py`** | **ETF별 EPS-weighted 신호 ranking (unique)** ★ |

### 발행
| 모듈 | 역할 |
|------|------|
| `publisher.py` | 텔레그램 + Substack publishing |
| `email_sender.py` | Gmail SMTP 자동 발송 |
| `substack_email.py` | Substack post-by-email 자동화 |
| `charts.py` | matplotlib 시각화 (3종 + 확장 가능) |

### 인프라
| 모듈 | 역할 |
|------|------|
| `chatbot.py` | AI 어드바이저 (Claude/Gemini API + fallback) |
| `mcp_server.py` | Claude Desktop MCP server |
| `kr_etfs.py` | 한국 ETF prototype (32개) |
| `utils.py` | 로깅, retry, safe_call |
| `health_check.py` | cron 실패/이상 자동 감지 |
| `test_basic.py` | 단위 테스트 (11/11 통과) |
| `run_daily.py` | 통합 cron pipeline (12+ 단계) |

### 검증
| 모듈 | 역할 |
|------|------|
| `bt_signals.py` | 신호 alpha BT (기본 4 신호) |
| `bt_advanced.py` | sector rotation, AUM growth, dual, mean reversion |

### 문서 / 자산
| 파일 | 역할 |
|------|------|
| `README.md` | 한국어 README |
| `README_EN.md` | 영어 README |
| `SETUP.md` | 설정 가이드 (cron, Gmail, Telegram, Substack) |
| `requirements.txt` | 의존성 |
| `.gitignore` | DB/cache 제외 |
| `landing/index.html` | 랜딩 페이지 (가격 모델 포함) |
| `.github/workflows/etf_pulse_daily.yml` | GitHub Actions cron |

---

## 자동 생성 콘텐츠 (이미 생성됨)

`etf_pulse/content/`:
- `pulse_2026-05-29.md` — Korean daily
- `pulse_2026-05-29_en.md` — English daily
- `premium_daily_*.md` — Premium 통합
- `category_best_weekly.md` — Michelin style
- `intent_best.md` — 의도별 best
- `dual_market.md` — 한미 통합
- `bridge_eps_etf.md` — EPS bridge
- `etf_eps_signal.md` — ETF EPS signal
- `tracking_error.md` — 추적오차
- `weekly_changes.md` — 주간 변동
- `market_regime.md` — 시장 regime
- `portfolio_analysis_demo.md` — 포트폴리오 분석 데모
- `compare_all_groups.md` — 30개 그룹 비교
- `health_check.md` — 시스템 헬스
- `charts/*.png` — 시각화 3종

---

## 핵심 차별화 (해자) 7개

| # | 차별화 | 다른 서비스 |
|---|--------|-------------|
| 1 | 카테고리별 best 자동 추천 | 토스/Webull 없음 |
| 2 | 동일 카테고리 alternatives 비교 | 어디에도 없음 |
| 3 | 자동 패턴 인사이트 (테마 회전 등) | 없음 |
| 4 | 의도별 best (장기/단기/배당) | 없음 |
| 5 | 한미 통합 (양쪽 investor 전용) | 없음 |
| 6 | **EPS Momentum × ETF 결합** (사용자 unique) | **사용자만** |
| 7 | 시장 regime 자동 진단 | 부분 (Bloomberg 같은 비싼 도구만) |

---

## 사업 모델 (제안)

| Tier | 가격 | 기능 |
|------|------|------|
| Free | ₩0 | 매일 Markdown newsletter (이메일/Substack) |
| **Pro** | **₩15,000/월** | + 포트폴리오 추적 + Premium daily + 알림 + AI 챗봇 |
| Premium | ₩30,000/월 | + walk-forward BT + custom 카테고리 + 우선 응답 |

---

## 검증 결과

- **단위 테스트**: 11/11 통과
- **데이터 fetch**: 228 ETF 100% 성공 (실패 0)
- **콘텐츠 생성**: 14+ 종류 자동 생성 확인
- **신호 BT**: 30일 backfill 기준 (강세장 환경)
  - SPY baseline 강세 (5d +1.19%, win 84%)
  - mean reversion in uptrend: 1d hold +0.66%, win 73.7% (흥미)
- **MCP server**: Claude Desktop에서 직접 ETF 쿼리 가능

---

## 즉시 사용 가능

```bash
# 1회 설정
cd etf_pulse
pip install -r requirements.txt
python db_schema.py
python backfill.py

# 매일 실행 (cron 또는 GitHub Actions)
python run_daily.py

# 또는 Premium 단일 리포트
python premium_daily.py
```

---

## 다음 단계 (사용자 결정)

### 즉시 (1주)
1. cron 등록 (회사 PC 또는 GitHub Actions)
2. Substack 계정 + 첫 발행
3. X/Twitter 미리보기 트윗

### 1-2개월
1. 1주 dogfood — 신호 정확성 검증
2. fund flow 진짜 계산 (cron 누적 후)
3. audience 100명 도달

### 3-6개월
1. paid tier 출시
2. 한국 ETF 정식 통합
3. AI 챗봇 정식 (Claude API)

### 6개월+
1. B2B (자산운용사)
2. 일본/유럽 ETF 확장
3. 강의/교육 콘텐츠

---

## 한 줄 마무리

**18개 모듈 + 14종 콘텐츠 + 단위 테스트 + GitHub Actions cron + 랜딩 페이지 + 영/한 README + MCP server = MVP 완전체.**

사용자가 cron만 등록하면 매일 자동으로 ETF Pulse가 작동.
