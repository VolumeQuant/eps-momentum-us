# US 자동매매 모듈 구현 계획

> 작성일: 2026-04-07
> 상태: 계획 단계 (구현 전)
> 원칙: **daily_runner.py 및 기존 프로덕션 코드 절대 수정 금지**

---

## 1. 현재 상태

- 한국투자증권 계좌 보유 (해외주식 거래 가능)
- v71.2 전략 확정, 프로덕션 가동 중
- 매일 KST 06:15 GitHub Actions에서 실행
  - EPS 수집 → 스코어링 → 랭킹 → DB 저장 → 텔레그램 발송 → git push
- 시그널은 `eps_momentum_data.db`에 전량 저장됨
- 사용자가 텔레그램 확인 후 **수동으로** 주문 넣는 구조

---

## 2. 목표

텔레그램 시그널을 보고 수동 주문하는 과정을 **완전 자동화**한다.
- DB에서 시그널 읽기 → 한투 API로 주문 실행 → 체결 리포트 텔레그램 발송
- 기존 프로덕션 코드는 **한 줄도 수정하지 않는다**
- 별도 모듈 + 별도 워크플로우로 완전 분리

---

## 3. 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│ GitHub Actions 스케줄                                    │
│                                                         │
│ KST 06:15  daily-screening.yml (기존, 수정 없음)         │
│            ├── daily_runner.py 실행                      │
│            ├── DB 저장 (ntm_screening, portfolio_log)    │
│            ├── 텔레그램 발송 (Signal, AI Risk, Watchlist) │
│            └── git push (DB 포함)                        │
│                                                         │
│ KST 10:05  kis-day-trading.yml (🆕 별도 워크플로우)      │
│            ├── git pull (최신 DB 받기)                   │
│            ├── auto_trade_us.py 실행                     │
│            │   ├── DB에서 시그널 추출 (읽기 전용)         │
│            │   ├── 한투 인증 + 잔고 조회                  │
│            │   ├── 포지션 조정 계산                       │
│            │   ├── 매도 실행 (데이마켓 지정가)            │
│            │   ├── 매수 실행 (데이마켓 지정가)            │
│            │   ├── 체결 확인 (폴링)                      │
│            │   └── 텔레그램 체결 리포트                   │
│            └── (git push 없음 — DB 수정하지 않음)        │
│                                                         │
│ KST 17:00  미체결 처리 (선택, 별도 스케줄)               │
│            ├── 미체결 주문 확인                           │
│            ├── 취소 또는 정규장 예약 전환                  │
│            └── 텔레그램 알림                              │
└─────────────────────────────────────────────────────────┘
```

### 왜 KST 10:05인가?
- daily_runner.py는 KST 06:15에 실행 → DB에 시그널 저장
- 한투 **데이마켓**(주간거래)은 KST 10:00~17:30 운영 (ATS 경유)
- 데이마켓 오픈 직후 주문하면 한국 낮 시간에 미국주식 즉시 체결 가능
- 데이마켓은 **지정가만** 가능 (시장가/MOO/MOC 불가)

### 왜 DB 읽기만 하는가?
- daily_runner.py가 매일 DB에 완전한 시그널을 저장
- `ntm_screening.part2_rank` = 3일 가중 순위 (매수 기준)
- `ntm_screening.seg1~seg4` = EPS 추세 (퇴출 기준)
- `portfolio_log` = 포트폴리오 진입가/수익률 (손절 기준)
- 별도 시그널 파일 불필요 → daily_runner.py 수정 불필요

---

## 4. 모듈 구조

```
eps-momentum-us/
  ├── daily_runner.py              # 기존 프로덕션 (절대 수정 금지)
  ├── eps_momentum_data.db         # 기존 (시그널 소스, 읽기 전용)
  ├── config.json                  # 기존 (수정 금지)
  │
  ├── kis_auth.py                  # 🆕 한투 인증 모듈 (~80줄)
  ├── kis_api.py                   # 🆕 한투 API 래퍼 (~250줄)
  ├── auto_trade_us.py             # 🆕 매매 로직 메인 (~400줄)
  ├── trade_report.py              # 🆕 체결 리포트 텔레그램 (~100줄)
  │
  ├── .github/workflows/
  │   ├── daily-screening.yml      # 기존 (수정 금지)
  │   └── kis-day-trading.yml      # 🆕 데이마켓 자동매매
  │
  └── .gitignore                   # kis_config.yaml 추가
```

---

## 5. DB 시그널 추출 (핵심)

### 5.1 DB 스키마 (ntm_screening 테이블)

시그널 추출에 필요한 핵심 컬럼:

| 컬럼 | 타입 | 용도 |
|------|------|------|
| `date` | TEXT | 거래일 (YYYY-MM-DD) |
| `ticker` | TEXT | 종목코드 |
| `part2_rank` | INTEGER | **3일 가중 순위** (1~30, NULL=탈락) — 매수/매도 핵심 |
| `composite_rank` | INTEGER | 당일 conviction 순위 — 참조용 |
| `adj_gap` | REAL | 저평가도 (음수=저평가) |
| `seg1` | REAL | EPS 추세 7일 (%) |
| `seg2` | REAL | EPS 추세 30일 (%) |
| `seg3` | REAL | EPS 추세 60일 (%) |
| `seg4` | REAL | EPS 추세 90일 (%) |
| `price` | REAL | 현재가 |
| `rev_up30` | INTEGER | 30일 상향 수 |
| `rev_down30` | INTEGER | 30일 하향 수 |
| `num_analysts` | INTEGER | 애널리스트 수 |
| `rev_growth` | REAL | 매출성장률 |
| `operating_margin` | REAL | 영업이익률 |

### 5.2 portfolio_log 테이블

| 컬럼 | 타입 | 용도 |
|------|------|------|
| `date` | TEXT | 거래일 |
| `ticker` | TEXT | 종목코드 |
| `action` | TEXT | 'enter', 'hold', 'exit' |
| `price` | REAL | 당일 가격 |
| `weight` | REAL | 비중 (%) |
| `entry_date` | TEXT | 진입일 |
| `entry_price` | REAL | 진입가 |
| `exit_price` | REAL | 퇴출가 (exit 시) |
| `return_pct` | REAL | 수익률 (exit 시) |

### 5.3 매수 시그널 추출 쿼리

```sql
-- Step 1: 최근 3거래일 확인
WITH recent_3 AS (
    SELECT DISTINCT date FROM ntm_screening
    WHERE part2_rank IS NOT NULL
    ORDER BY date DESC LIMIT 3
),
-- Step 2: 3일 연속 Top 30 = ✅ 검증 완료
verified AS (
    SELECT ticker FROM ntm_screening
    WHERE date IN (SELECT date FROM recent_3)
      AND part2_rank IS NOT NULL
    GROUP BY ticker
    HAVING COUNT(DISTINCT date) = 3
),
-- Step 3: 오늘 데이터
today AS (
    SELECT * FROM ntm_screening
    WHERE date = (SELECT MAX(date) FROM recent_3)
)
-- Step 4: 매수 조건 필터
SELECT
    t.ticker,
    t.part2_rank,
    t.price,
    t.adj_gap,
    MIN(t.seg1, t.seg2, t.seg3, t.seg4) AS min_seg,
    t.rev_up30,
    t.num_analysts
FROM today t
WHERE t.ticker IN (SELECT ticker FROM verified)
  AND t.part2_rank <= 3                              -- Top 3
  AND MIN(t.seg1, t.seg2, t.seg3, t.seg4) >= 0      -- min_seg ≥ 0%
ORDER BY t.part2_rank;
```

**매수 조건 요약:**
1. `part2_rank ≤ 3` (3일 가중 순위 상위 3)
2. 3일 연속 Top 30 진입 (✅ 검증)
3. `min(seg1, seg2, seg3, seg4) ≥ 0%` (EPS 추세 건강)
4. 이미 보유 중인 종목은 스킵
5. 슬롯 여유 있어야 (최대 5종목)

### 5.4 매도 시그널 추출 쿼리

```sql
-- 보유 종목의 퇴출 조건 확인
SELECT
    ticker,
    part2_rank,
    MIN(seg1, seg2, seg3, seg4) AS min_seg,
    adj_gap
FROM ntm_screening
WHERE date = (SELECT MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL)
  AND ticker IN ({현재_보유_종목})
  AND (
    part2_rank > 15                                  -- 순위 이탈
    OR part2_rank IS NULL                            -- 필터 탈락
    OR MIN(seg1, seg2, seg3, seg4) < -2              -- 추세 둔화
  );
```

```sql
-- 손절 확인 (-10%)
SELECT
    ticker,
    entry_price,
    price,
    ROUND((price - entry_price) / entry_price * 100, 1) AS return_pct
FROM portfolio_log
WHERE date = (SELECT MAX(date) FROM portfolio_log)
  AND action IN ('enter', 'hold')
  AND (price - entry_price) / entry_price < -0.10;
```

**매도 조건 요약:**
1. `part2_rank > 15` → 순위 이탈
2. `part2_rank IS NULL` → 필터 탈락 (MA120, 매출, 마진 등)
3. `min_seg < -2%` → 추세 둔화
4. 진입가 대비 `-10%` → 손절

### 5.5 거래소 코드 매핑

DB에 거래소 정보가 없으므로 별도 매핑 필요:

```python
def resolve_exchange(ticker: str) -> str:
    """티커 → 거래소코드 (NASD/NYSE/AMEX)
    1차: yfinance .info['exchange'] 캐시
    2차: NASD → NYSE → AMEX 순차 시도
    """
```

캐시 전략: `exchange_cache.json` 파일로 로컬 캐시 유지

---

## 6. Phase 1: 기반 구축 (1일)

### 6.1 kis_auth.py — 인증 모듈 (~80줄)

```python
"""한국투자증권 OAuth2 인증 모듈"""

def load_credentials() -> dict | None:
    """인증정보 로드
    우선순위: 환경변수(GitHub Actions) > kis_config.yaml(로컬)
    
    환경변수:
      KIS_APP_KEY, KIS_APP_SECRET
      KIS_ACCOUNT_NO (8자리)
      KIS_MODE (paper/production, 기본 paper)
    
    로컬 파일 (~/.kis/config.yaml):
      app_key, app_secret, account_no, mode
    """

def get_token(credentials: dict) -> str:
    """OAuth2 Bearer 토큰 발급
    - POST /oauth2/tokenP
    - 24시간 유효, 발급 후 파일 캐시
    - 1분 1회 발급 제한 → 캐시 필수
    """

def get_base_url(mode: str) -> str:
    """API 베이스 URL
    - paper: https://openapivps.koreainvestment.com:29443
    - production: https://openapi.koreainvestment.com:9443
    """

def get_headers(token: str, tr_id: str) -> dict:
    """API 공통 헤더
    - authorization: Bearer {token}
    - appkey, appsecret
    - tr_id: 거래 유형 코드
    - custtype: P (개인)
    """
```

### 6.2 kis_api.py — API 래퍼 (~250줄)

```python
"""한국투자증권 해외주식 API 래퍼 (데이마켓 전용)"""

def get_balance(token, account) -> dict:
    """해외주식 잔고 조회
    - API: /uapi/overseas-stock/v1/trading/inquire-balance
    - Returns: {
        'holdings': {ticker: {'qty': int, 'avg_price': float, 'current_price': float, 'pnl_pct': float}},
        'cash': float,        # USD 예수금
        'total_value': float  # 총 평가금액
      }
    - 거래소별 조회: NASD, NYSE, AMEX 각각 호출 후 합산
    """

def get_buyable_amount(token, account, ticker, price, exchange) -> int:
    """매수 가능 수량 조회
    - API: /uapi/overseas-stock/v1/trading/inquire-psamount
    - TR_ID: TTTS3007R
    """

def buy(token, account, ticker, qty, price, exchange) -> dict:
    """데이마켓 매수 주문
    - API: /uapi/overseas-stock/v1/trading/daytime-order
    - TR_ID: TTTS6036U (실전) / VTTS6036U (모의)
    - ord_dvsn: "00" (지정가)
    - Returns: {'order_no': str, 'status': str}
    """

def sell(token, account, ticker, qty, price, exchange) -> dict:
    """데이마켓 매도 주문
    - API: /uapi/overseas-stock/v1/trading/daytime-order
    - TR_ID: TTTS6037U (실전) / VTTS6037U (모의)
    """

def get_pending_orders(token, account) -> list:
    """미체결 주문 조회
    - API: /uapi/overseas-stock/v1/trading/inquire-nccs
    - Returns: [{'order_no': str, 'ticker': str, 'side': str, 'qty': int, 'price': float}]
    """

def cancel_order(token, account, order_no, exchange) -> dict:
    """미체결 주문 취소
    - API: /uapi/overseas-stock/v1/trading/daytime-order-rvsecncl
    - rvse_cncl_dvsn_cd: "02" (취소)
    """

def get_current_price(token, ticker, exchange) -> float:
    """현재가 조회 (주문가격 결정용)
    - API: /uapi/overseas-stock/v1/quotations/price
    """

def resolve_exchange(ticker: str) -> str:
    """티커 → 거래소코드 매핑
    1. exchange_cache.json 확인
    2. yfinance .info['exchange'] 조회
    3. 실패 시 NASD → NYSE → AMEX 순차 시도
    4. 결과 캐시 저장
    """
```

**주문가격 결정 로직:**
- 매수: `get_current_price()` × 1.005 (0.5% 위) → 체결 확률 높임
- 매도: `get_current_price()` × 0.995 (0.5% 아래) → 빠른 체결
- 데이마켓은 지정가만 → 시장가 효과를 위해 약간의 버퍼

---

## 7. Phase 2: 매매 로직 (2일)

### 7.1 auto_trade_us.py — 핵심 매매 엔진 (~400줄)

```python
"""EPS Momentum US 자동매매 엔진
daily_runner.py의 DB 출력을 읽어 한투 데이마켓에서 자동 주문 실행

실행: python auto_trade_us.py
스케줄: GitHub Actions, KST 10:05 (데이마켓 오픈 직후)
"""

DB_PATH = 'eps_momentum_data.db'
MAX_SLOTS = 5          # 최대 보유 종목 수
ENTRY_RANK = 3         # 진입 기준 순위
EXIT_RANK = 15         # 퇴출 기준 순위
STOP_LOSS = -0.10      # 손절 기준 (-10%)
MIN_SEG_EXIT = -2.0    # 추세둔화 퇴출 기준

def get_buy_signals(db_path: str) -> list[dict]:
    """DB에서 매수 시그널 추출
    조건: part2_rank ≤ 3 + 3일 검증(✅) + min_seg ≥ 0%
    Returns: [{'ticker': str, 'rank': int, 'price': float, 'adj_gap': float}]
    """

def get_sell_signals(db_path: str, holdings: dict) -> list[dict]:
    """DB에서 매도 시그널 추출
    조건: part2_rank > 15 OR NULL OR min_seg < -2% OR 손절 -10%
    Returns: [{'ticker': str, 'reason': str, 'rank': int}]
    """

def reconcile_positions(holdings, buys, sells, total_value) -> tuple[list, list]:
    """포지션 조정 계산
    
    1. sell_orders = 보유 중 & sells에 해당하는 종목
    2. keep_orders = 보유 중 & sells에 없는 종목
    3. available_slots = MAX_SLOTS - len(keep_orders)
    4. slot_size = total_value / MAX_SLOTS
    5. buy_orders = buys 중 미보유 & available_slots 이내
       - qty = int(slot_size / current_price)
       - 이미 보유 중이면 스킵 (중복 매수 방지)
    6. 총 매수금액 ≤ cash + 매도예상금액 검증
    
    Returns: (sell_orders, buy_orders)
    """

def execute_sells(sell_orders: list) -> list[dict]:
    """매도 주문 실행 (매수보다 먼저)
    - 데이마켓 지정가: 현재가 × 0.995
    - 각 주문 후 0.5초 대기 (rate limit)
    Returns: [{'ticker', 'qty', 'price', 'order_no', 'status'}]
    """

def execute_buys(buy_orders: list, cash_available: float) -> list[dict]:
    """매수 주문 실행
    - 데이마켓 지정가: 현재가 × 1.005
    - 잔여 현금 확인 후 주문
    - 각 주문 후 0.5초 대기 (rate limit)
    Returns: [{'ticker', 'qty', 'price', 'order_no', 'status'}]
    """

def check_fills(order_nos: list, timeout_sec: int = 120) -> list[dict]:
    """체결 확인 (폴링)
    - 10초 간격으로 미체결 조회
    - timeout 후 미체결 잔량 리턴
    Returns: [{'order_no', 'ticker', 'filled_qty', 'unfilled_qty', 'avg_price'}]
    """

def main():
    """메인 실행 흐름"""
    # 0. kill switch 확인
    if os.environ.get('KIS_KILL') == '1':
        log("Kill switch 활성화 — 매매 중단")
        return

    # 1. 인증
    creds = kis_auth.load_credentials()
    if not creds:
        log("KIS 인증정보 없음 — 매매 스킵")
        return
    token = kis_auth.get_token(creds)

    # 2. DB 시그널 추출
    buys = get_buy_signals(DB_PATH)
    log(f"매수 시그널: {[b['ticker'] for b in buys]}")

    # 3. 잔고 조회
    balance = kis_api.get_balance(token, creds['account'])
    holdings = balance['holdings']
    log(f"보유: {list(holdings.keys())}, 현금: ${balance['cash']:,.0f}")

    # 4. 매도 시그널 (보유 종목 기준)
    sells = get_sell_signals(DB_PATH, holdings)
    log(f"매도 시그널: {[s['ticker'] for s in sells]}")

    # 5. 포지션 조정
    sell_orders, buy_orders = reconcile_positions(
        holdings, buys, sells, balance['total_value']
    )

    # 6. 안전장치 확인
    #    - portfolio_mode == 'stop' → 매수 차단
    #    - 단일 주문 ≤ 총자산 30%
    #    - 1일 최대 매매 한도

    # 7. 매도 실행 (먼저)
    sell_results = execute_sells(sell_orders)

    # 8. 매수 실행
    buy_results = execute_buys(buy_orders, balance['cash'])

    # 9. 체결 확인
    all_orders = sell_results + buy_results
    fills = check_fills([o['order_no'] for o in all_orders])

    # 10. 텔레그램 리포트
    report = trade_report.create_report(
        sell_results, buy_results, fills, balance, creds['mode']
    )
    trade_report.send_telegram(report)
```

### 7.2 포지션 조정 예시

```
현재 보유: MU(15주), TTMI(8주), FIX(5주)
DB 시그널: 매수=[MU, TTMI, FTAI], 매도=FIX(순위이탈)
계좌 총액: $50,000

1. 매도: FIX 5주
2. 유지: MU(15주), TTMI(8주) → 2슬롯 사용
3. 매수 가능 슬롯: 5 - 2 = 3
4. MU, TTMI는 이미 보유 → 스킵
5. FTAI만 신규 매수 → slot_size = $50,000 / 5 = $10,000
6. FTAI 현재가 $85 → qty = int(10,000 / 85) = 117주
7. 매수: FTAI 117주 × $85.43 (현재가+0.5%)
```

---

## 8. Phase 3: 리포트 + 워크플로우 (1일)

### 8.1 trade_report.py — 체결 리포트 (~100줄)

텔레그램 메시지 포맷:

```
🤖 KIS 자동매매 US · 2026.4.7(월) 10:15
모드: 모의투자 | 데이마켓

━━━━━━━━━━━━━━━
📤 매도 체결
━━━━━━━━━━━━━━━
FIX × 5주 @ $1,420.00 (순위이탈, +12.3%)

━━━━━━━━━━━━━━━
📥 매수 체결
━━━━━━━━━━━━━━━
FTAI × 117주 @ $85.43 (✅ 3위, $10,000)

━━━━━━━━━━━━━━━
⏳ 미체결
━━━━━━━━━━━━━━━
없음

━━━━━━━━━━━━━━━
📊 포트폴리오
━━━━━━━━━━━━━━━
보유: MU · TTMI · FTAI (3/5종목)
총평가: $52,300 | 예수금: $3,200
```

### 8.2 kis-day-trading.yml (신규 워크플로우)

```yaml
name: KIS Day Market Trading (US)

on:
  schedule:
    # UTC 01:05 = KST 10:05 (데이마켓 오픈 직후)
    # 월~금 실행
    - cron: '5 1 * * 1-5'
  workflow_dispatch:  # 수동 실행 가능

env:
  KIS_APP_KEY: ${{ secrets.KIS_APP_KEY }}
  KIS_APP_SECRET: ${{ secrets.KIS_APP_SECRET }}
  KIS_ACCOUNT_NO: ${{ secrets.KIS_ACCOUNT_NO }}
  KIS_MODE: ${{ secrets.KIS_MODE }}            # paper 또는 production
  TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
  TELEGRAM_PRIVATE_ID: ${{ secrets.TELEGRAM_PRIVATE_ID }}

jobs:
  trading:
    runs-on: ubuntu-latest
    steps:
    - name: Checkout (최신 DB 포함)
      uses: actions/checkout@v4
      with:
        fetch-depth: 1

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.11'
        cache: 'pip'

    - name: Install dependencies
      run: pip install pyyaml yfinance

    - name: Run auto trading
      run: python auto_trade_us.py
      env:
        TZ: 'America/New_York'
```

### 8.3 GitHub Secrets 설정 (필요)

| Secret 이름 | 값 | 용도 |
|-------------|-----|------|
| `KIS_APP_KEY` | 한투 AppKey | API 인증 |
| `KIS_APP_SECRET` | 한투 AppSecret | API 인증 |
| `KIS_ACCOUNT_NO` | 8자리 계좌번호 | 주문/조회 |
| `KIS_MODE` | `paper` | 모의/실전 전환 |

기존 Secrets (`TELEGRAM_BOT_TOKEN`, `TELEGRAM_PRIVATE_ID`)는 재사용.

---

## 9. Phase 4: 모의투자 테스트 (1~2주)

- `KIS_MODE=paper` (모의투자)
- 매일 KST 10:05 자동 실행
- 체크리스트:
  - [ ] 시그널 추출 정확성 (DB vs 텔레그램 메시지 일치)
  - [ ] 매수 주문 정상 체결
  - [ ] 매도 주문 정상 체결
  - [ ] 중복 매수 방지 작동
  - [ ] 슬롯 관리 정확 (최대 5종목)
  - [ ] 손절 -10% 작동
  - [ ] 체결 리포트 텔레그램 정상 수신
  - [ ] 에러 발생 시 텔레그램 알림
  - [ ] kill switch 작동
- 통계 수집: 체결률, 슬리피지(시그널가 vs 체결가), 체결 소요시간

---

## 10. Phase 5: 실전 전환

1. `KIS_MODE` Secret을 `paper` → `production` 변경
2. 초기 1주: 소액 (총자산 10~20%)
3. 안정 확인 후 비중 점진 확대
4. 모든 주문 텔레그램 알림 유지

---

## 11. 안전장치

| # | 안전장치 | 설명 |
|---|---------|------|
| 1 | **모의투자 우선** | 최소 2주 모의투자 후 실전 |
| 2 | **kill switch** | `KIS_KILL=1` 환경변수로 즉시 정지 |
| 3 | **단일 주문 한도** | 계좌 총액 30% 초과 주문 거부 |
| 4 | **1일 매매 한도** | 설정 가능한 일일 최대 매매 금액 |
| 5 | **portfolio_mode** | DB에서 risk status 확인, 'stop' 시 매수 차단 |
| 6 | **중복 방지** | 미체결 주문 확인 → 같은 종목 중복 주문 스킵 |
| 7 | **에러 알림** | 모든 에러 즉시 텔레그램 개인봇으로 발송 |
| 8 | **매매 로그** | 모든 주문/체결 기록 (파일 또는 DB) |
| 9 | **DB 미수정** | auto_trade가 eps_momentum_data.db를 절대 수정하지 않음 |

---

## 12. 데이마켓 vs 정규장 비교

| 항목 | 데이마켓 (주간거래) | 정규장 |
|------|-------------------|--------|
| KST 시간 | 10:00~17:30 | 23:30~06:00 |
| 주문 유형 | **지정가만** | 지정가, MOO, MOC, LOO, LOC |
| 거래소 | ATS (대체거래소) | NASD, NYSE, AMEX |
| 유동성 | 낮음 | 높음 |
| 체결 속도 | 느릴 수 있음 | 빠름 |
| 시그널 → 체결 | ~4시간 | ~17시간 (다음 정규장) |

**데이마켓 선택 이유:**
1. 시그널 생성(06:15) 후 가장 빠른 체결 가능 (~4시간)
2. 한국 업무 시간에 모니터링 가능
3. 데이마켓 미체결 시 정규장으로 전환 가능

---

## 13. 일정

| 단계 | 기간 | 내용 | 산출물 |
|------|------|------|--------|
| **Phase 1** | 1일 | API 래퍼 | `kis_auth.py`, `kis_api.py` |
| **Phase 2** | 2일 | 매매 로직 | `auto_trade_us.py` |
| **Phase 3** | 1일 | 리포트 + 워크플로우 | `trade_report.py`, `kis-day-trading.yml` |
| **Phase 4** | 1~2주 | 모의투자 테스트 | 체결 통계, 버그 수정 |
| **Phase 5** | - | 실전 전환 | KIS_MODE=production |

---

## 14. 참조 파일 (읽기 전용)

| 파일 | 용도 | 비고 |
|------|------|------|
| `daily_runner.py` | 시그널 생성 로직 참조 | **수정 금지** |
| `eps_momentum_data.db` | 시그널 소스 | 읽기 전용 |
| `config.json` | 텔레그램 설정 참조 | **수정 금지** |
| `.github/workflows/daily-screening.yml` | 스케줄 참조 | **수정 금지** |
| KIS `open-trading-api` GitHub | API 샘플코드 참조 | 외부 레포 |
