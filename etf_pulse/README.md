# ETF Pulse 🌅

**미국 ETF Daily 인사이트** — 매일 시장 마감 후 자동 분석 + 콘텐츠 생성

---

## 무엇이 다른가? (해자)

| 기능 | 토스/MTS | Webull | Seeking Alpha | **ETF Pulse** |
|------|----------|--------|---------------|---------------|
| 카테고리별 신호 정리 | ✗ | ✗ | 부분 | ✓ ★ |
| 거래량 spike + 자동 narrative | ✗ | ✗ | ✗ | ✓ ★ |
| 동일 카테고리 alternatives | ✗ | ✗ | ✗ | ✓ ★ |
| 자동 인사이트 (테마 회전 감지) | ✗ | ✗ | ✗ | ✓ ★ |
| 포트폴리오 daily 펄스 | 부분 | 부분 | ✗ | ✓ ★ |
| Daily 자동 발행 (텔레그램/Substack) | ✗ | ✗ | ✗ | ✓ ★ |

---

## 구조

```
etf_pulse/
├── etf_universe.py     # 228 ETF universe (7 카테고리)
├── db_schema.py        # SQLite 스키마 (5 테이블)
├── daily_fetch.py      # yfinance 일별 수집 (가격/거래량/AUM/holdings)
├── backfill.py         # 과거 N일 backfill
├── signals.py          # 신호 추출 (spike/momentum/카테고리 강도 등)
├── narrative.py        # 신호 → Markdown 콘텐츠 자동 생성
├── publisher.py        # 텔레그램/Substack 발행
├── portfolio.py        # 사용자 포트폴리오 추적 (paid 기능)
├── bt_signals.py       # 신호 alpha BT 검증
├── run_daily.py        # 통합 cron 진입점
├── etf_pulse.db        # SQLite DB
└── content/            # 자동 생성된 콘텐츠 (Markdown)
```

---

## 사용법

### 1. 초기 설정 (1회)
```bash
python etf_pulse/db_schema.py    # DB 생성
python etf_pulse/backfill.py     # 30일 history backfill (선택)
```

### 2. 매일 cron (한국 시간 06:00 권장)
```bash
python etf_pulse/run_daily.py
```
또는 단계별:
```bash
python etf_pulse/daily_fetch.py   # 데이터 수집
python etf_pulse/narrative.py     # 콘텐츠 생성
python etf_pulse/publisher.py     # 발행
```

### 3. 포트폴리오 추적 (개인 사용)
```python
from etf_pulse.portfolio import add_holding, get_portfolio_pulse, gen_pulse_message

# 보유 등록
add_holding('user1', 'VOO', 100, entry_price=550, entry_date='2026-04-01')

# 일별 펄스
pulse = get_portfolio_pulse('user1')
print(gen_pulse_message(pulse))
```

### 4. 신호 BT 검증
```bash
python etf_pulse/bt_signals.py
```

---

## 데이터 소스

- **yfinance** (단일 source, 90% 커버):
  - 가격, 거래량, AUM, Top Holdings, 뉴스, 펀더멘털
- **추후 추가 가능**: ETF.com fund flows, iShares JSON, SPDR Excel

---

## 신호 종류

### 1. 거래량 spike (vs 30일 평균)
- 1.5x+ 폭증 ETF → 자금 이동/이벤트 신호

### 2. 카테고리 강도
- 7개 카테고리 (core_us, international, sectors, themes, bonds, commodity_hedge, income_lev)
- 평균 수익률로 강세/약세 자동 분류

### 3. 5일 모멘텀
- 5거래일 수익률 Top 5

### 4. 30일 신고가/신저가
- 큰 ETF (AUM $500M+) 신고가 = 강세 지속 신호

### 5. fund flow (cron 누적 후)
- AUM 일별 변동 - 가격수익률 효과 = 순유입 추정

### 6. 자동 패턴 인사이트
- 같은 테마 동시 폭증 (예: 사이버보안 ETF 2개)
- ARK 시리즈 동시 신고가
- 채권 거래량 폭증 (안전자산 신호)
- 원자재 신저가 클러스터

---

## 콘텐츠 예시 (자동 생성)

```markdown
# 🌅 ETF Pulse — 2026-05-29

## 🎯 어제 시장 분위기
**강세 테마**: 테마 (평균 +0.46%)
**약세 테마**: 원자재/헷지 (평균 -0.42%)

## 📈 어제 수익률 Top 5
- **MSTU (MSTR 2X 레버리지)**: +9.66%
- **CIBR (사이버보안)**: +6.41%
...

## 🧠 자동 인사이트
- **사이버보안** ETF 2개 수익률 동시 상승
- **ARK 시리즈 4개 동시 신고가** → 성장주 강세
- **원자재 ETF 4개 30일 신저가** → 원자재 사이클 약세 지속
```

전체 예시: `content/pulse_2026-05-29.md`

---

## BT 검증 (30일 데이터)

| 신호 | 1일 보유 | 3일 보유 | 5일 보유 |
|------|----------|----------|----------|
| SPY baseline | +0.22% | +0.71% | +1.19% |
| 거래량 spike Top 5 | -0.18% | -0.16% | +0.01% |
| 5일 모멘텀 Top 5 | +0.68% | +0.54% | -0.27% |
| 카테고리 회전 | +0.24% | +0.69% | +0.88% |

**결론**: 30일 표본은 SPY baseline이 강세장 환경에서 압도적. 신호들은 콘텐츠 가치 위주 (information value). 1년+ 누적 후 정확 BT 필요.

---

## 다음 단계 (Roadmap)

### Phase 1 ✅ MVP 완료
- 228 ETF universe + 데이터 수집
- 신호 추출 + Markdown 자동 생성
- 포트폴리오 추적 prototype
- BT 검증 framework

### Phase 2 (1-2개월)
- 매일 자동 cron 등록 (1주 dogfood)
- 텔레그램 봇 안정화 (token 재발급)
- Substack 계정 + 무료 발행 (audience 모음)
- X(Twitter) 미리보기 트윗

### Phase 3 (3-6개월)
- 포트폴리오 paid tier (사용자 등록 + 알림)
- AI 챗봇 어드바이저 (자연어 질문)
- 신규 ETF 자동 큐레이션
- 한국 ETF 추가 (KRX)

### Phase 4 (6개월+)
- B2B (자산운용사, RIA)
- 확장: 일본/유럽 ETF
- 교육 콘텐츠 / 강의

---

## 가격 모델 (시점 도래 시)

| Tier | 가격 | 기능 |
|------|------|------|
| Free | 0 | Daily Newsletter (이메일/Substack) |
| **Pro** | **$10-15/월** | 포트폴리오 추적 + 알림 + 10년 BT + AI 챗봇 |
| Premium | $30/월 | Walk-forward BT + 신호 알림 + custom 카테고리 |

---

## 신뢰성 단서

- ✓ yfinance 단일 소스 (안정적)
- ✓ 자동 인사이트는 객관 데이터 기반 (편향 X)
- ✓ "투자 추천 아님" 명시 (자문업 회피)
- ⚠️ 신호 alpha는 1년 누적 후 검증
- ⚠️ AUM lag, holdings 빈도는 dogfood 후 확정
