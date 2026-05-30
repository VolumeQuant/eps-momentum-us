# MA Filter Exploration — 2026-05-29 자율주행 결과 보고서

사용자 자율주행 요청: "현재 ma120, ma60 fallback 말고 더 나은 필터 있을지 인사이트 발굴"

## TL;DR (한 줄)

**73일 BT 한정으로 현재 production (MA120 + MA60 fallback)을 의미있게 개선할 수 있는 MA filter 변형 없음.** w_gap/conviction 정렬이 이미 약세 case 3 종목을 자연 필터링하므로 MA filter 변경의 영향이 Top 3 entry zone에 도달하지 못함.

## 실행한 BT (v83.3 params, slot 2, 100×3 paired)

| 변형 | 정의 | paired lift | wins | 판정 |
|------|------|-------------|------|------|
| **current** (baseline) | MA120 + MA60 fallback | — | — | production |
| ma60_or_ma120 | 둘 중 하나 통과 (case 2+3 모두) | **+0.00%p** | **0/100** | current와 100% 동일 |
| ma120_slope_up_10 | current + MA120 10d slope > 0 | -5.57%p | 0/100 | ✗✗ 명확 열세 |
| ma120_slope_up_20 | current + MA120 20d slope > 0 | -5.57%p | 0/100 | ✗✗ 명확 열세 |
| ma120_slope_up_10_relaxed | current + MA120 10d slope > -1% | +0.00%p | 0/100 | current와 100% 동일 |
| eps_escape | current + rev_up30≥5 강한 EPS는 case 3도 OK | +0.00%p | 0/100 | current와 100% 동일 |

## 핵심 진단: 왜 ma60_or_ma120 = current인가?

| 비교 | 공통 | current only | ma60_or_ma120 only |
|------|------|--------------|--------------------|
| Top 30 (eligible pool) | 2042 | 109 | 114 |
| **Top 3 (entry zone)** | **219** | **0** | **0** |

- Top 30 레벨에선 다른 종목 잡음 (case 3 종목 추가로 들어옴)
- **Top 3 entry zone에서 차이 0** — w_gap conviction 정렬이 case 3 종목을 자연스럽게 밀어냄
- **이유**: case 3 = 가격 < MA120 = 가격 약세 → adj_gap이 강세 종목 대비 약함 → conviction 순위에서 밀림

## MA60 Fallback의 진짜 가치 (사전 분석)

- DB 8.6% rows가 MA120 NULL + MA60 OK (7,677 rows)
- MA60 fallback으로 part2_rank Top 10 진입한 핵심 종목:
  - **SNDK 4일, MU 4일, TTMI 4일, TER 4일, STX 4일, LITE 4일, COHR 4일**
  - 모두 v83 알파 핵심 종목들 ([[project_v83_2026_05_24]] 메모리의 SNDK +93%, MU +103% buy-the-dip)
- **결론**: MA60 fallback 자체는 절대 못 뺀다 (신규 상장 buy-the-dip 알파의 핵심)

## Slope filter는 왜 열세인가?

- MA120 slope > 0 강제 시 -5.57%p 손실
- 원인: 일부 알파 종목이 MA120 slope < 0 상태에서 강한 EPS revision 신호 보유 (예: 5/28 LLY)
- slope > -1% (약한 하락 허용)로 풀면 → current와 동일 (slope -1% ~ 0%인 종목이 part2_rank에 없음)
- **결론**: slope filter는 지나치게 엄격하거나 무효함 → 추가 가치 없음

## EPS escape (case 3 진입 허용)는 왜 무효인가?

- rev_up30 ≥ 5 강한 EPS 종목 중 case 3 해당 종목 → conviction 순위에서 결국 Top 3 못 들어옴
- ma60_or_ma120과 같은 원리 — Top 30 풀만 바꾸고 entry zone은 동일
- **결론**: EPS escape도 무효

## 73일 BT의 한계 (시장 환경 인식)

이 결과는 다음 환경 한정:
- **강세장 (S&P 2026-02-12 ~ 05-28 +~8%)**
- **slot 2 v83.3 production params**
- MA60 > MA120 다수 종목 (강세장 특성)

다른 환경에서는 결과 다를 수 있음:
- **베어마켓 (S&P -10%+)**: MA120이 천천히 하락 → 구조적 약세 종목 계속 통과시킴 → current 취약. MA60 strict가 보호 효과 있을 수 있음.
- **횡보장 (±5%)**: MA60 strict는 whipsaw 폭발 (회전 60+ → 거래비용 -10%p). current 우월.

## 권고

1. **단기 (즉시)**: 현행 MA120 + MA60 fallback 유지. 73일 BT 환경 한정으로는 개선 가능한 변형 없음.

2. **중기 (regime monitoring)**: SPY 60일 수익률 < -10% 트리거 시 MA60 strict 일시 전환 검토 (전문가 의견 [[project_v83_2026_05_24]] 참조). 필터 자체 변경이 아닌 regime switch로 다루기.

3. **장기 (BT 확장)**: 2025년 약세장 데이터 확보 후 MA filter 변형 재검증. 현재 73일은 강세장 한정 표본.

4. **개선 대신 단순화 검토**: ma60_or_ma120 = current 100% 동일 결과 → **이론적으로 production을 ma60_or_ma120로 바꿔도 BT 결과 동일**. 단, 메시지/리스크 매니저 관점에선 더 관대해 보여서 권고 X. 현행 유지.

## 다음 검증 후보 (사용자 결정 필요)

본 BT에서 다루지 못한 영역:
- **MA period grid (MA90, MA150, MA180, MA200) + MA60 fallback** — 새 MA period 계산 필요 (yfinance 추가 fetch). MA120 = 약 6개월, MA200 = 약 10개월. 기간 trade-off 미검증.
- **MA filter 완전 제거 + 다른 trend filter (예: 종가 > N일 high의 X%)**
- **Bear regime DB 확보 후 재검증** — 2025 1~6월 데이터 (S&P -15% 구간)

## 생성된 파일
- `research/bt_ma_alternatives.py` — 5변형 BT 스크립트
- `research/bt_ma_alternatives.log` — 결과 로그
- `research/ma_filter_dbs/alt_*.db` — 변형별 DB

## 관련 메모리
- [[project_v83_2026_05_24]] — v83 + 슬롯 비중 정책
- [[project_blind_spots_audit_2026_05_20]] — "MA60 경고 (behavioral bias로 알파 갉아먹음)" 결정
- 이전 BT: `research/bt_ma_filter.log` (5변형 검증), `research/bt_ma_filter_no_mu.log` (MU 제외)
- 전문가 의견 (Agent quant analyst): bt_ma_diff.log 분석 후 받음
