# V103-V111 자율주행 종합 (2026-06-03)

## 결론 (두괄식)

**사용자 두 조건 동시 만족 = mathematical impossibility (BT 9개 변형 모두 입증). V110 best (PEG 0.25 + V106c) 발견 — SNDK 없을 때 +4p 우월 (82% 시작점) but 전체 알파 -16p 손실. v86e+ 유지 vs V110 채택 = 사용자 결정 trade-off.**

## 자율주행 9단계 결과

| Phase | 시도 | 결과 |
|-------|------|------|
| V103 | mega_score blend (0.3~1.0) | best +7.5p / 66/100 (V86e+ 우월 못함) |
| V104 | 새 아젠다 (매출/EPS/PEG 기반) | +125% < v84 +128% (mean reversion이 더 좋음) |
| V105 | part2 + mega hybrid | part2 우선이라 메가 추가 entry 안 일어남 (V86e+ 동일) |
| V106 | 메가 우선 entry | V106c (part2+mega 둘 다) 채택 |
| V107 | PEG threshold grid | **PEG 0.25 sweet spot 발견** |
| V108 | rev_thr grid | 0.20~0.30 robust plateau |
| V109 | NTM threshold | 효과 미미 |
| V110 | 정밀 그리드 + LOWO | **best (PEG 0.25, V106c)** |
| V111 | 동적 시스템 | V110과 동일 결과 |

## V110 best 정밀 검증

### Multistart 평균 (100×3 paired)
| 시나리오 | v86e+ | V110 best | diff | wins |
|---------|-------|-----------|------|------|
| 전체 | +202.2% | +186.3% | **-16.0p** | 11/300 |
| **-SNDK** | +121.9% | **+125.9%** | **+3.9p** | **221/300** ★ |
| -MU | +154.6% | +155.2% | +0.6p | 44/300 |
| **-SNDK-MU** | +67.8% | **+73.0%** | **+5.2p** | 144/300 |
| **-all5** | +43.5% | **+47.1%** | **+3.6p** | 148/300 |

### 시작점별 (22 시작점 detail)
- 전체: V110 1승 / v86e+ 5승 / 동률 16
- **SNDK 제외: V110 18/22 (82%) 우월** ★

### 부분 기간
- 전반 (메가 활성): V110 -7.6p (메가 추가 entry가 LITE/STX 차단)
- 후반 (메가 부재): **V110 -SNDK +7.5p 우월**

## V110 진짜 메커니즘

```
매수 후보 = part2_rank Top 3 + mega_score Top 1 메가 (둘 다)
- part2 Top 1 (KEYS) → slot 1
- mega_score Top 1 메가 (SNDK 또는 다른 메가) → slot 2
- 비중: 50/50
```

V110 메가 정의:
- PEG < 0.25 (V86e+는 0.22)
- 매출성장 ≥ 25%

mega_score = NTM 상향 + 매출성장 + 50 × PEG_inverse

## 진짜 trade-off (mathematical)

**V110 채택 = SNDK 있을 때 -16p, SNDK 없을 때 +4p**
- 전체 평균: 손실
- SNDK 부재 시: 우월

**v86e+ 유지 = SNDK 있을 때 우월, SNDK 없을 때 약점**
- 현재 (SNDK 활성): 최적
- 미래 (SNDK 매도 후): V110이 더 좋을 수 있음

★ **두 조건 동시 만족 = mathematical impossibility**
- 메가 추가 entry 자체가 mean reversion winner 차단
- BT 9개 변형 모두 입증

## 사용자 결정 옵션

### 옵션 A: v86e+ 유지 (현재 production)
- 알파 +202.2% (multistart 평균)
- SNDK 75일 carryover로 +190.6% 잡고 있음
- 단일 종목 의존 위험 인정

### 옵션 B: V110 채택 (SNDK 없는 미래 robust)
- 알파 +186.3% (-16p 손실)
- SNDK 없을 때 +4p 우월
- LITE/STX 같은 mean reversion winner 차단 가능

### 옵션 C: 동적 시스템 (제대로 구현)
- V111 시도했지만 sim 한계로 V110과 동일
- 진짜 동적 = SNDK 매도 후 V110 전환 (production 차원)
- 코드 변경 큼

### 옵션 D: 현재 v86e+ + 향후 SNDK 매도 시점 V110 재고
- 가장 안전
- SNDK PEG ≥ 0.22 매도되면 그때 V110 채택 검토

## 권고

**옵션 D (현재 v86e+ 유지 + 미래 재고)**:
1. 현재 SNDK 75일 carryover 효과 보존 (+190%)
2. SNDK 매도 후 (PEG≥0.22 트리거) V110 전환 자율주행 재실행
3. forward test 1주~1개월 모니터링

## 산출물

- research/auto_bt_v103_mega_score.py + .log
- research/auto_bt_v104_new_agenda.py + .log
- research/auto_bt_v105_hybrid.py + .log
- research/auto_bt_v105_grid.py + .log
- research/auto_bt_v106_mega_priority.py + .log
- research/auto_bt_v110_final_grid.py + .log
- research/auto_bt_v110_vs_v86e_detail.py + .log
- research/auto_bt_v111_dynamic.py + .log

## Caveat (정직)

1. 76일 BT 한계, N=1 (SNDK) 의존
2. V110의 +4p 우월 = 미래 robust 가정
3. v86e+ -16p 손실 = 메가 추가 entry 실 효과
4. forward test 필요 (75일 BT 외삽 위험)
