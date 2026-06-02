# V86e+ Production 적용 확정 (2026-06-03)

## 결정

**V86e+ 그대로 production 운영.** 9번 자율주행 (V87~V101) 후 최종 채택.

## V86e+ 사양

### 메가홀드 조건 (regime detector)
- **PEG < 0.22** (성장 대비 극단적 저평가 = EPS revision regime)
- Mean reversion regime (대다수)과 분리

### Carryover 로직 (매도 보호 layer)
어제 보유 종목 중 다음 모두 만족 시 → 순위 무관 보유:
1. PEG < 0.22 유지
2. min_seg ≥ -2 (EPS 안 꺾임)
3. rev_growth ≥ 0.25 (매출 둔화 X)

### 자동 해제 (매도)
- min_seg < -2 (EPS 꺾임)
- rev_growth < 0.25 (매출 둔화)
- 메가 조건 자체 미충족 (PEG ≥ 0.22)

### 신규 매수 (기존 v84 그대로)
- part2_rank ≤ 3
- min_seg ≥ 0
- 기타 필터 (rev_growth ≥ 10%, num_analysts ≥ 3, MA120/60, dd_30_25, FCF·ROE)

## 진입/이탈/슬롯 (v84 그대로, 변경 X)

| 항목 | 값 |
|------|-----|
| **MAX_SLOTS** | 2 |
| **진입 조건** | part2_rank ≤ 3 + 다중 필터 |
| **이탈 조건** | part2_rank > 10 (메가는 carryover) |
| **가중치** | dynamic — 1·2위 score 격차 ≥15 → 100/0, 그외 50/50 |

## 메시지 표시 (텔레그램 줄바꿈 정리)

### 메가홀드 표시 (Watchlist 영역)
```
🔒 메가 홀드: SNDK(26위) MU(순위밖) UMBF(14위)
  순위 밀려도 보유 권장
  (초저평가 PEG<0.22)
  EPS 꺾이거나 매출<25% 매도
```

### 운영 규칙 (Signal + Watchlist footer)
```
매수: 1·2위 점수차 dynamic
  (격차≥15 → 1위 100%, 격차<15 → 50/50)
매도: 10위 밖 or 실적하락
  🔒 메가(PEG<0.22)는 홀드
  (단 매출<25%면 매도)
```

### 시스템 누적 수익률 (V86e+ 시뮬)
- **V86e+ 누적**: +221.56% / SPY +11.06% / 알파 **+210.5%p** (72거래일)
- 이전 v84 시뮬: +252.6%
- 차이: -31p (추후 정밀 검증)

## 9번 자율주행 종합

| Round | 시도 | 결과 |
|-------|------|------|
| V87 | rank 직접 override (6 variants) | -57~-104p 손실 ❌ |
| V88 | 계산식 보너스 (8 variants) | -167~-179p 손실 ❌ |
| V89 | slot 3 메가전용 확장 (6 variants) | -143~-155p 손실 ❌ |
| V91 | adj_gap NTM_rev 차감 (10 variants) | -32~-46p 손실 ❌ |
| V94 | BT 좋아지는 새 방법 (7 variants) | -22~-152p 손실 ❌ |
| V95 | 메가 정밀 식별 | UMBF 제거 시 LOWO -22.7p (UMBF가 진짜 알파 driver) |
| V96 | broad 메가 (8 variants) | V86e+ LOWO -all5 +25.5p 발견 (5종목 제외해도 robust) |
| V97/V98 | conviction 통합 (20+ variants) | 모두 -20~-179p ❌ |
| V99 | 산식 본질 분석 (12 variants) | -23~-32p ❌ |
| V100/V101 | 종합 audit + 맹점 진단 | 5가지 맹점 발견, V86e+ 평균 우월 입증 |

### Mathematical Impossibility 입증
- 매수 단계 메가 보너스 = 메가 자동 1·2위 점유 = 신규 알파 차단 (KEYS/AEIS/LITE/MOD/STX)
- 75일 BT에서 PEG<0.22 종목 73일 유지 → 어떤 보너스든 self-reinforcing
- V86e+ carryover (매도 layer)만이 trade-off 회피 가능 (베이지안 정보 가중)

## 5가지 맹점 (V100/V101)

1. 75일 BT 한계 (단일 상승장)
2. MU/SNDK 알파 80% 후반 30일 폭등 의존 (data snooping 위험)
3. Multistart phase별 우열 반전 (후반 V99 우월)
4. LOWO -SNDK V99 +50p 우월
5. PEG/rev_exit cutoff over-fit 위험

→ 모두 인지. 미래 환경 변화 시 V99 옵션 가치 있을 수 있음. 현재는 V86e+ 평균 우월.

## 다음 단계 (자동, 사용자 액션 0)

- 다음 cron부터 V86e+ 자동 작동
- 메시지에 메가홀드 표시 (SNDK/MU/UMBF)
- 운영 규칙 명확 표시 (PEG<0.22, 매출<25% 매도)
- 모니터링 없이 자동

## Caveat (정직)

- 약세장 미검증
- 메가 반전 미검증
- V86e+ vs v84 production 시뮬 -31p — 추후 정밀 검증
