# Investment Clock 별도 트랙 — 셋업 보고서

**일시**: 2026-06-01
**상태**: 데이터 수집 완료, framework 설계 완료, BT 구현 다음 세션으로

## 1. 목적

EPS Momentum 시스템(v84)과 **별도**로 운영되는 사이클-선행 매매 트랙. Buffett 13F 패턴 (다음 사이클 winner 미리 매수) 정량 검증.

**핵심 차이:**
| | EPS Momentum (L0) | Investment Clock (L4 신규) |
|---|---|---|
| 신호 | 분석가 EPS revision (reactive) | macro 사이클 단계 (predictive) |
| 매수 시점 | EPS 상향 후 | EPS 약하지만 사이클 turn 임박 |
| Horizon | 1~6개월 | 18~36개월 |
| 대상 | 개별주 (NVDA, AEIS 등) | sector ETF (XLF, XHB 등) |
| Sizing | 시스템 자금 풀 | 총자산의 5% |

## 2. 데이터 수집 완료

**파일**: `research/sector_etf_8y.parquet` (4,024일 × 15 컬럼)

| 컬럼 | 의미 | 시작일 |
|---|---|---|
| XLF | 금융 sector | 2018-05-30 |
| XLE | 에너지 | 2018-05-30 |
| XLI | 산업재 | 2018-05-30 |
| XLP | 필수소비 | 2018-05-30 |
| XLV | 헬스케어 | 2018-05-30 |
| XLU | 유틸 | 2018-05-30 |
| XLY | 임의소비 | 2018-05-30 |
| XLB | 소재 | 2018-05-30 |
| XLK | 기술 | 2018-05-30 |
| XLRE | 부동산 | 2018-05-30 |
| XLC | 통신 | 2018-06-19 |
| SPY | 벤치마크 | 2018-05-30 |
| TLT | 장기채 | 2018-05-30 |
| GLD | 금 | 2018-05-30 |
| ^VIX | 변동성 | 2018-05-29 |

→ **8년 데이터, 약세장 2회 포함 (2020 COVID, 2022 인플레)**. 사이클 전환 다수.

## 3. 추가 필요 데이터 (다음 세션에서)

| 데이터 | source | 빈도 |
|---|---|---|
| ISM Manufacturing PMI | FRED CSV (https://fred.stlouisfed.org/graph/fredgraph.csv?id=NAPM) | 월 |
| Conference Board LEI | Conference Board press release | 월 |
| OECD CLI US | FRED USALOLITONOSTSAM | 월 |
| Core PCE YoY | FRED PCEPILFE | 월 |
| 10Y-2Y Spread | FRED T10Y2Y | 일 |
| Sahm Rule | FRED SAHMCURRENT | 월 |

## 4. BT framework 설계

### Phase 1: 사이클 단계 분류 함수

```python
def classify_cycle_stage(date, signals):
    """
    Returns: '초봄', '중봄', '늦봄', '초여름', '중여름', '늦여름',
             '초가을', '중가을', '늦가을', '초겨울', '중겨울', '늦겨울'
    """
    pmi = signals.get('pmi')
    lei_6mo = signals.get('lei_6mo')
    sahm = signals.get('sahm')
    core_pce = signals.get('core_pce')
    fed_dir = signals.get('fed_direction')  # 'hike', 'pause', 'cut'
    curve = signals.get('curve_inverted')

    # 12단계 분류 (앞서 정의한 신호 매트릭스 적용)
    # ...
```

### Phase 2: Sector 매핑 (단계별 winner)

```python
STAGE_TO_SECTORS = {
    '초봄': ['XLF', 'XLY', 'XLRE'],     # 회복 베팅
    '중봄': ['XLI', 'XLB', 'XLF'],
    '늦봄': ['XLK', 'XLY', 'XLI'],
    '초여름': ['XLE', 'XLB', 'XLI'],
    '중여름': ['XLE', 'XLF', 'XLB'],
    '늦여름': ['XLE', 'XLV', 'GLD'],     # 방어 시작
    '초가을': ['XLP', 'XLV', 'XLU', 'GLD'],
    '중가을': ['XLU', 'XLV', 'TLT', 'GLD'],
    '늦가을': ['TLT', 'GLD', 'cash'],
    '초겨울': ['TLT', 'XLP', 'XLV'],
    '중겨울': ['TLT', 'cash', 'GLD'],
    '늦겨울': ['XLF', 'XLY', 'XLRE'],   # 봄 winner 선행
}
```

### Phase 3: 선행 매수 룰 (Buffett 패턴)

```python
def get_target_sectors(today_stage):
    """현재 단계의 sector + 다음 단계 sector 50:50"""
    next_stage = next_in_sequence(today_stage)
    return STAGE_TO_SECTORS[today_stage] + STAGE_TO_SECTORS[next_stage]
```

### Phase 4: BT 룰

- 자본: $100k (5%/년 yield 가정)
- 매월 1영업일 = 사이클 재판정 + 리밸런싱
- 목표: 사이클 단계당 3~5개 sector 균등 매수
- 평가: 7년 CAGR, MDD, Calmar, Sharpe vs SPY buy-hold

## 5. 다음 자율주행 작업 순서

1. **ISM PMI / LEI 데이터 fetch** (FRED CSV)
2. **사이클 단계 분류 함수 구현** + 8년 history에 적용
3. **단계별 sector return EDA**: 각 단계에서 어떤 sector가 best였는지 확인
4. **Investment Clock BT**: 단계 → 매수 → 리밸런싱 → 누적 성과
5. **Buffett 13F follow BT 비교**: 분기별 Berkshire 매수 → sector ETF로 follow
6. **vs SPY buy-hold 비교**: alpha 측정

## 6. 예상 산출 (다음 세션)

| 분석 | 기대 결과 |
|---|---|
| 7년 Investment Clock BT | Sharpe ↑ vs SPY (위험 대비 우월) 가능. 절대 return은 SPY 비슷 (액티브 cost) |
| Buffett 13F follow | Lead time 검증 (1-3년 선행) + sector ETF로 단순화 가능한지 |
| 2020 COVID + 2022 약세장 sector 성과 | 사이클 framework의 약세장 가치 검증 |

## 7. EPS Momentum과의 관계 — 명확히 분리

| 관점 | L0 (EPS Momentum) | L4 (Investment Clock) |
|---|---|---|
| 운영 자금 | 시스템 자금 (전체의 56%) | 별도 5% |
| 자동화 정도 | 100% 자동 (cron) | 월 1회 사용자 판정 |
| 결정 메커니즘 | 분석가 EPS 신호 | 매크로 신호 + 본인 판단 |
| Horizon | 1~6개월 | 18~36개월 |
| 검증 | 72일 BT 풍부 | 다음 세션 BT 예정 |

**충돌 X — 두 알파는 다른 시간 차원에서 작동.** 동시 운영 가능.

## 8. 메모리 저장

이 트랙 신호 + 데이터 위치 저장:

- `research/sector_etf_8y.parquet` — 1차 데이터
- `research/INVESTMENT_CLOCK_TRACK_SETUP.md` — 본 문서
- 메모리: `project_investment_clock_track.md` (신규)
