# Session Handoff: 전략 개선 논의

> **v1**: 2026-02-06 직장 PC — 구조적 문제 발견, score_321 개선 논의
> **v2**: 2026-02-06 집 PC — NTM EPS 전환 결정, DB/Score/랭킹 전면 재설계
> **v3**: 2026-02-06 집 PC — 풀 유니버스 시뮬레이션, 이상치 처리, Part 2 재설계
> **v4**: 2026-02-06 집 PC — 텔레그램 포맷 확정, 발송 채널 분리, 업종 매핑
> **v5**: 2026-02-07 집 PC — 모바일 UI 리디자인, 고객 친화적 말투
> **v6**: 2026-02-07 집 PC — 트래픽 라이트, ⚠️ 경고, Part 2 EPS>0 필터, 코드 정리
> **v7**: 2026-02-07 집 PC — 4단계 신호등, Score 정렬, 가중평균, MD 정리
> **v8**: 2026-02-07 집 PC — Gemini 2.5 Flash AI 리스크 검증 도입
> **v9**: 2026-02-08 집 PC — 6단계 트래픽 라이트, AI 뉴스 스캐너, 턴어라운드 메시지 제거, Score > 10, Part 2 표시값 변경
> **v10**: 2026-02-08 집 PC — AI 리스크 스캐너 소거법 전환, 프롬프트 5회 반복 튜닝
> **v11**: 2026-02-08 집 PC — 방향 보정(adj_score) 도입, Part 2 필터 adj_score > 9
> **v12**: 2026-02-08 집 PC — 트래픽 라이트 8패턴 리디자인, Part 2 괴리율+의견 표시 추가
> **v13**: 2026-02-08 집 PC — 트래픽 라이트 12패턴 확장 (피크 위치 기반 + 진동 감지, "상향 가속" 12/30→최대 5/30)
> **v14**: 2026-02-08 집 PC — AI 뉴스 스캐너→AI 브리핑 전환 (검색은 코드가, 분석은 AI가)
> **v15**: 2026-02-09 집 PC — 6단계 신호등→5단계 날씨 아이콘 (☀️🌤️☁️🌧️⛈️), 포트폴리오 비중 단순화
> **v15**: 2026-02-09 집 PC — AI 브리핑 정량 리스크 스캐너 전환, UI/말투 개선
> **v16**: 2026-02-09 집 PC — 포트폴리오 추천 기능 통합, Part 2 UI 압축
> **v17**: 2026-02-09 집 PC — adj_score 기반 정렬 전환, 메시지 단계별 흐름 [1/4]~[4/4], UI 개선
> **v18**: 2026-02-09 집 PC — adj_gap 도입, 리스크 필터 정비(모순 제거+저커버리지), AI 브리핑 동기화
> **v19**: 2026-02-10 회사 PC — Safety & Trend Fusion: MA60+3일검증+Death List, Part 1 제거, 메시지 3개 축소, adj_gap≤0/$10 필터
> **v19.2**: 2026-02-10 집 PC — [1/2][2/2] 구조, 시장 컨텍스트, ⏳ 삭제, 날짜 biz_day 통일
> **v20**: 2026-02-11 회사 PC — Simple & Clear: Death List 제거, Top 30 통일, ⏳ 표시 전용 복원, 투자 가이드 재작성
> **v21**: 2026-02-12 집 PC — Composite Score: 매출성장률 30% 반영, 동일 비중, AI 프롬프트 구조화
> **v22**: 2026-02-12 집 PC — Revenue Required: 섹터 분산 제거, rev_growth 필수화, 업종 분포 통계
> **v23**: 2026-02-12 집 PC — HY Spread EDA: 30년 데이터 분석, Verdad 4분면 모델 채택, Method C 확정
> **v24**: 2026-02-12 집 PC — 현금 비중 권장: 기본 20% + 매크로 추가, Q1 해빙기 0%(풀 공격), 종목 5개 고정(분산 유지)
> **v25**: 2026-02-13 집 PC — VIX 심층 분석: 에이전트 2개 독립 토의 → Strategy C(3레이어 복합) 확정, 메시지 [1/3]→[1/4]+[2/4] 분리
> **v26**: 2026-02-13 집 PC — VIX Layer 2 구현: fetch_vix_data() + get_market_risk_status() + Concordance Check + 텔레그램 VIX 표시
> **v27**: 2026-02-13 집 PC — 시장 위험 지표(🏦신용시장+⚡변동성) + 사계절 라벨 + 애널리스트 DB 저장 + 데이터 보호 복원 + FRED 3회 재시도
> **v30**: 2026-02-13 집 PC — 신호등(🟢🔴) 도입, concordance 액션 고객 친화 개선, 비중 항상 20% 고정
> **v31**: 2026-02-18 집 PC — Balanced Review 8대 개선: 버퍼존(20진입/35유지), VIX 퍼센타일, 손절(L2+L3), 매출 필터 제거, Forward Test, 자본배분 가이드
> **v32**: 2026-02-19 집 PC — Risk Consistency + 품질 필터: 매출10% 복원, 애널리스트 품질 하드필터(저커버리지+하향>30%), rank 버그, 이탈 구분, 섹터/어닝 경고, 차등 비중, Top30 통일, UI 가독성, 3일 가중순위(T0×0.5+T1×0.3+T2×0.2)
> **Ackman Quality Screen**: 2026-02-19 집 PC — 별도 프로젝트 신규 생성. 빌 애크먼 8대 원칙 기반 S&P 500 품질 스크리너 (주간, 별도 텔레그램 봇)
> **v33**: 2026-02-19 집 PC — 재무 품질 데이터 축적: ntm_screening에 13개 컬럼 추가(rev_growth+FCF,ROE,D/E,마진 등), 916종목 전체 매일 수집, 10스레드 병렬화(3분→13초), 전략 변경 없음
> **v34**: 2026-02-19 집 PC — UX 대폭 개선: 읽는 법 각 메시지 상단 이동, 날씨 아이콘 설명, 아이콘 교체(🛡️→🚨🤖), [3/4] 어제 마감 집중, [4/4] Google Search Grounding으로 비즈니스 맥락 검색+비중요약 삭제+주의사항 정리+퍼널 간결화, 국내 프로젝트 동기화
> **v34.1**: 2026-02-20 집 PC — 읽는 법 📖 가이드로 통합, yfinance Rate Limit 해결, 사계절 2줄 분리, HY/VIX 볼드+콜론, Gemini 서두 자동 제거, 국내 동기화(오늘의 메시지 목차 삭제)
> **v34.2**: 2026-02-20 집 PC — [1/4] 다우존스 추가, [3/4] AI 지수 수치 반복 금지, 국내 가중치 개편(V45Q15G10M30, 공식 유지)
> **v35**: 2026-02-20 집 PC — 가중순위 기반 Top 30 선정: eligible 전체에서 T0×0.5+T1×0.3+T2×0.2로 Top 30 경계 결정, 과거 8일 DB 재계산
> **v35.1**: 2026-02-20 집 PC — composite_rank 분리: DB에 composite_rank 컬럼 추가, 가중순위는 항상 composite에서 계산 (누적 방지)
> **v35.2**: 2026-02-20 집 PC — 데이터 일관성 확보: rev_growth backfill + recalc_ranks composite_rank 저장 + 한국 프로젝트 교차 검증
> **v35.3**: 2026-02-20 집 PC — 어닝 일정 수정: .calendar Rate Limit → .info earningsTimestamp 활용 + 장후(16시 ET) 발표 +1일 보정
> **v35.4**: 2026-02-20 집 PC — 데이터 보호 캐시 경로 rev_growth 누락 수정: 재수집 스킵 시 part2_rank 0건 버그
> **v35.5**: 2026-02-21 집 PC — 종합 감사: 데이터 보호 모드 제거, today_str 단일화, exit 가격 버그 수정(어제종가→퇴출일종가), DB 백필 복구, 버전 v31 표기
> **v36**: 2026-02-21 집 PC — 순위 변동 원인 태그: get_rank_change_tags() 신규, [2/4] 순위 줄에 📈주가↑/💡저평가↑/📉모멘텀↓/📈모멘텀↑/🔄상대변동 태그 표시 + migrate_weighted_ranks.py ddof=1 통일
> **v36.1**: 2026-02-21 집 PC — 태그 비교 구간 수정: 3일 궤적 종목은 T0 vs T2 비교 (1일 delta로는 threshold 못 넘어 🔄상대변동 오분류 → 2일 누적 delta로 정확한 진단)
> **v36.2**: 2026-02-21 집 PC — 표시/DB 불일치 수정: create_candidates_message가 composite Top 30 표시 vs save_part2_ranks가 weighted Top 30 DB 저장 → 최대 8종목 불일치(유령 상태). today_tickers 전달로 통일
> **v36.3**: 2026-02-21 집 PC — 태그 방향 일치 + 지배적 팩터: gap 우선순위 제거, 순위 방향에 맞는 delta만 수집 → 정규화(|delta|/threshold) 최대 팩터 선택

---

## v36.4 — 2축 태그 + 동일비중 + σ정규화 (2026-02-21)

### 배경
- v36~36.3의 태그 시스템을 근본적으로 재설계
- 백테스트(9거래일)로 포트폴리오 파라미터 검증

### 태그 시스템 재설계
- **2축 독립 판정**: 가격(adj_gap)과 실적(adj_score)을 각각 독립적으로 판정
- **σ 기반 임계값**: 고정 임계값 → 일간 변동 표준편차(1.0σ) 기준
  - GAP_STD = 3.96 (adj_gap 일간 변동 σ, 7일 데이터)
  - SCORE_STD = 1.48 (adj_score 일간 변동 σ, 7일 데이터)
- **중복 표시**: 둘 다 임계값 넘으면 둘 다 표시 (예: 💪실적↑ 📉가격↓)
- **사실 전달, 해석 배제**: 보유자/매수검토자 모두 같은 사실로 각자 판단

### 태그 4종 (9종 조합)
| 태그 | 설명 | 조건 |
|---|---|---|
| 📈가격↑ | 가격이 올랐어요 | adj_gap Δ ≥ +3.96 (1σ) |
| 📉가격↓ | 가격이 내렸어요 | adj_gap Δ ≤ -3.96 (1σ) |
| 💪실적↑ | 실적이 좋아졌어요 | adj_score Δ ≥ +1.48 (1σ) |
| ⚠️실적↓ | 실적이 나빠졌어요 | adj_score Δ ≤ -1.48 (1σ) |

조합 매트릭스:
| 실적 \ 가격 | 가격↑ | 가격↓ | 변동없음 |
|---|---|---|---|
| 실적↑ | 💪실적↑ 📈가격↑ | 💪실적↑ 📉가격↓ | 💪실적↑ |
| 실적↓ | ⚠️실적↓ 📈가격↑ | ⚠️실적↓ 📉가격↓ | ⚠️실적↓ |
| 변동없음 | 📈가격↑ | 📉가격↓ | 태그 없음 |

### 제거된 것
- 🔄상대변동: 원인 불명이면 태그 없음 (단순함 원칙)
- 방향 일치 필터: 2축 독립이므로 불필요
- 지배적 팩터 선택: 둘 다 표시하므로 불필요
- 🆕 종목 태그: 신규 진입은 이미 🆕 표시

### 동일비중 포트폴리오
- **변경**: 차등 비중 [25,25,20,15,15] → 동일 비중 [20,20,20,20,20]
- **근거**: 9일 백테스트 결과
  - 동일 20%: +6.50% (효율 3.46)
  - 차등 25/25/20/15/15: +5.04% (효율 2.68)
  - 급경사 35/25/20/12/8: +4.43% (효율 1.95)
  - 1위(SNDK) 변동성이 커서 집중할수록 손실 증폭

### 백테스트 결과 요약 (9거래일 2/6~2/20)
- **composite 70/30 유지**: Gap-only(+0.03%) vs 70/30(+6.50%), rev_growth 비중↑일수록 좋지만 과적합 위험
- **Top 5 최적**: Top3(+1.3%) < Top5(+6.5%) > Top7(+1.3%) > Top10(+2.7%)
- **z-score 정규화 확인**: composite 계산 시 이미 z-score 적용 → 70/30 가중치는 의도대로 작동
- **실제 기여도**: 분산 기준 adj_gap 85% / rev_growth 15% (가중치² 효과)

### 비교 구간 유지
- 3일 궤적(r2 < PENALTY) → T0 vs T2 비교 (2일 누적 delta)
- 2일 궤적(r2 = PENALTY) → T0 vs T1 비교 (1일 delta)
- |Δrank| < 3이면 태그 없음

---

## v36~36.3 — 순위 변동 태그 초기 버전 (2026-02-21, v36.4로 대체)

### ddof 통일
- migrate_weighted_ranks.py: `np.std(gaps)` → `np.std(gaps, ddof=1)`
- daily_runner.py의 `pd.Series.std()` (ddof=1)와 일치시킴

### 표시/DB 불일치 수정 (v36.2)
- **문제**: `create_candidates_message()`가 composite Top 30, `save_part2_ranks()`가 가중순위 Top 30 → 최대 8종목 불일치
- **수정**: `create_candidates_message()`에 `today_tickers` 파라미터 추가

---

## v35.5 — 종합 감사 + exit 가격 수정 (2026-02-21)

### 배경
- 실투자용 시스템 종합 재점검 요청
- Feb 20 데이터가 stale (데이터 보호 모드가 Feb 19 가격 캐싱 → 910/912 동일가격)
- portfolio_log의 exit 가격이 퇴출일이 아닌 전일 종가 사용 — 수익률 부정확

### 변경 사항

| 항목 | Before | After |
|------|--------|-------|
| 데이터 보호 | 같은 날짜 재수집 방지 (캐시) | **제거** — 항상 새로 수집 |
| today_str | run_ntm_collection + main 중복 결정 | run_ntm_collection에서만 반환 |
| exit 가격 | `p['price']` (어제 종가) | ntm_screening에서 퇴출일 종가 조회 |
| 버전 문자열 | v19 | v31 |

### DB 복구
- Feb 20 stale 데이터 삭제 (ntm_screening + portfolio_log + ai_analysis)
- rev_growth 백필: Feb 19 → Feb 06~18 (858종목/일)
- composite_rank + part2_rank 재계산: migrate_weighted_ranks.py (6일분)
- portfolio_log entry_price=0 복구 + MU exit 가격 보정 ($399.78→$420.95, 0%→+5.3%)

### 변경 파일
- `daily_runner.py` — 데이터 보호 제거, today_str 단일화, exit 가격 수정, 버전 문자열
- `eps_momentum_data.db` — 백필 + 재계산 + exit 가격 보정

## v35.4 — 데이터 보호 캐시 경로 rev_growth 누락 수정 (2026-02-20)

### 배경
- 데이터 보호 (같은 마켓 날짜 재수집 방지)가 작동하면 DB에서 캐시 로드
- 캐시 경로의 SELECT문에 `rev_growth` 컬럼이 빠져있었음
- `results_df`에 rev_growth가 없으면 `get_part2_candidates()`에서 `has_rev=False` → composite score 대신 adj_gap 단독 정렬
- 이로 인해 매출 10% 필터가 미적용되고, composite 순위가 달라지면서 가중순위 T-1/T-2 참조 시 불일치 발생
- 결과: `save_part2_ranks()`에서 composite_rank UPDATE → part2_rank UPDATE 실행은 되지만, 이전 composite_rank가 없어 가중순위가 전부 PENALTY 50으로 계산됨

### 변경 사항

| 항목 | Before | After |
|------|--------|-------|
| 캐시 SELECT | rev_up30, rev_down30, num_analysts만 | **+ rev_growth** 추가 |
| row_dict | rev_growth 키 없음 | `'rev_growth': r[15]` 추가 |

### 변경 파일
- `daily_runner.py` — run_ntm_collection() 캐시 경로 SELECT + row_dict

---

## v35.3 — 어닝 일정 수정 (2026-02-20)

### 배경
- [3/4] AI 리스크 필터에서 "📅 어닝 주의: 해당 없음" — Top 30 전체 어닝 미감지
- NVDA(2/26), NEM(2/20) 등 2주 내 어닝 종목이 있는데도 0건
- 원인 1: `fetch_revenue_growth()`에서 861종목 `.info` 호출 후 Rate Limit → `.calendar` 30건 전부 실패 (except: pass가 에러 삼킴)
- 원인 2: NEM `.info` earningsTimestamp가 2/19 16:00 ET (장후) → `.date()` 하면 2/19 → `today_date(2/20) <= 2/19` 실패

### 변경 사항

| 항목 | Before | After |
|------|--------|-------|
| 어닝 날짜 소스 | `.calendar` 별도 호출 (30건) | `.info` `earningsTimestamp` 활용 (추가 0건) |
| 장후 보정 | 없음 | hour >= 16 ET → +1일 (시장 영향일 기준) |
| 전달 방식 | 각 함수에서 yf.Ticker().calendar 직접 호출 | `earnings_map` dict로 run_ai_analysis/run_portfolio_recommendation에 전달 |
| 결과 | 어닝 0종목 | **어닝 4종목** (NEM 2/20, NVDA 2/26, THO 3/3, DY 3/4) |

### 핵심 코드
```python
# fetch_revenue_growth()에서 .info 수집 시 어닝 날짜도 추출
ets = info.get('earningsTimestampEnd') or info.get('earningsTimestampStart') or info.get('earningsTimestamp')
dt_et = datetime.fromtimestamp(ets, tz=ZoneInfo('America/New_York'))
earn_date = dt_et.date()
if dt_et.hour >= 16:  # 장후 발표 → 다음 거래일
    earn_date += timedelta(days=1)
earnings_map[t] = earn_date
```

### 변경 파일
- `daily_runner.py` — fetch_revenue_growth() earnings_map 반환 + run_ai_analysis/run_portfolio_recommendation 파라미터 추가

---

## v35.2 — 데이터 일관성 확보 (2026-02-20)

### 배경 (v35.1 → v35.2)
- v35.1 배포 후 텔레그램에 🆕 18개, 이탈 17개 — 비정상적 대규모 턴오버
- **원인**: rev_growth가 2/6~2/18에 전부 NULL → rev 필터 미적용 → 2/19에만 적용되어 스코어링 공식 불일치
- recalc_ranks.py에서 composite_rank를 DB에 저장하지 않아 가중순위 계산에 차질

### 변경 사항

| 항목 | Before | After |
|------|--------|-------|
| rev_growth (과거) | 2/6~2/18 전부 NULL | 2/19 값으로 backfill (870/913) |
| recalc_ranks.py | part2_rank만 저장 | **composite_rank** 전체 eligible 저장 추가 |
| 턴오버 | 🆕 18, 이탈 17 | 🆕 1(ADI), 이탈 1(CIEN) — 정상 |

### rev_growth backfill 방법
1. 2/19 날짜의 rev_growth 값을 전체 과거 날짜에 복사 (연간 매출성장률이라 변동 미미)
2. 2/19에 없는 13개 티커를 yfinance에서 추가 수집
3. migrate_weighted_ranks.py 재실행 → 전체 8일 part2_rank/composite_rank 재계산
4. 결과: eligible 28~37개/일 (일관됨), 턴오버 정상화

### 재무지표 (market_cap, roe 등) 결정
- 과거 날짜: backfill 안 함 (시가총액 등은 매일 변하는 값 → 현재 값 소급 부적절)
- 앞으로: daily_runner가 매일 전체 종목 자동 저장 (2/19부터 정상 작동 확인)
- 순위 계산에 미사용 (adj_gap + rev_growth만 사용) → 영향 없음

### 한국 프로젝트 교차 검증 결과
- composite_rank 존재: 6개 JSON 전체 ✅
- 가중순위 정확성: 15개 독립 검증 일치 ✅
- 누적 방지: composite_rank(순수 점수)만 사용 ✅
- 턴오버: 2~6종목/일 정상 ✅
- ranking_manager.py + send_telegram_auto.py: composite_rank 사용 ✅

### 변경 파일
- `recalc_ranks.py` — composite_rank 저장 로직 추가
- `eps_momentum_data.db` — rev_growth backfill + composite_rank 재계산

---

## v35.1 — composite_rank 분리 (2026-02-20)

### 배경 (v35 → v35.1)
- v35에서 가중순위를 part2_rank에 저장 → 다음 날 그 가중순위를 T-1로 참조 → 또 가중 → **누적(cascading) 문제**
- 올바른 구조: DB에는 **composite_rank**(순수 점수 순위)를 별도 저장, 가중순위는 항상 composite에서 계산

### 변경 사항

| 항목 | v35 (잘못됨) | v35.1 (수정) |
|------|-------------|-------------|
| DB 저장 | part2_rank = 가중순위 | **composite_rank** = 순수 점수 순위 (신규 컬럼) |
| T-1/T-2 참조 | part2_rank (가중순위 → 누적!) | **composite_rank** (순수 → 누적 없음) |
| part2_rank | 가중순위 1~30 | 가중순위 Top 30 (변경 없음) |

### 가중순위 공식
```
weighted = composite_T0 × 0.5 + composite_T1 × 0.3 + composite_T2 × 0.2
composite = 당일 순수 점수 기반 순위 (adj_gap 70% + rev_growth 30%)
미등재 = PENALTY 50
```

### 핵심
- `composite_rank`: 모든 eligible 종목에 저장, 매일 독립적
- `part2_rank`: 가중순위 Top 30에만 저장 (3일 검증/이탈 비교용)
- 가중순위 계산 시 항상 `composite_rank` 참조 → 누적 방지

### 변경 파일
- `daily_runner.py` — composite_rank 컬럼 추가 + save_part2_ranks() + compute_weighted_ranks()
- `migrate_weighted_ranks.py` — 과거 8일 composite_rank 저장 + part2_rank 재계산

---

## v34.2 — 다우존스 추가 + AI 지수 축소 (2026-02-20)

### 배경
- [3/4] AI 시장 동향이 "S&P 500이 43포인트 하락하고 나스닥은 0.47% 빠졌고..." 식으로 지수 수치 나열
- 지수 수치는 이미 [1/4]에 표시됨 → [3/4]에서 중복
- 사용자: "차라리 1/4에 다우지수를 추가하는게 낫겠다"

### 변경 사항

| 항목 | Before | After |
|------|--------|-------|
| [1/4] 시장 지수 | S&P 500 + 나스닥 | S&P 500 + 나스닥 + **다우** |
| [3/4] AI 프롬프트 | "어제 시장 마감(지수 등락, 주요 원인)에 집중" | "어제 시장 핵심 이슈(원인, 테마)만. **지수 수치 반복 금지**" |

#### 변경 파일
- `daily_runner.py` — `get_market_context()`: `("^DJI", "다우")` 추가
- `daily_runner.py` — Gemini 프롬프트: 지수 수치 반복 금지 명시

#### 국내 프로젝트 동기화 (v18.5)
- 가중치: V45+Q25+G10+M20 → V45+Q15+G10+M30
- 모멘텀: `(12M-1M)/σ` 유지 (12M/σ 테스트 후 반전 보호 필요성 확인하여 복원)
- AI: 인사말 금지 + Google Search Grounding
- 전체 날짜(20260209~19) 새 가중치로 재계산

---

## v34.1 — 읽는 법 📖 가이드 통합 + UX 정리 (2026-02-20)

### 배경
- 읽는 법이 [1/4], [2/4] 각 메시지 상단에 반복되어 매일 받는 고객에게 피로감 유발
- yfinance Rate Limit: 916종목 .info 호출 후 시장 지수(^GSPC/^IXIC) 수집 실패 (Too Many Requests)
- [1/4] HY Spread/VIX가 아이콘만으로 구분 안 됨
- [4/4] Gemini가 "다음은 요청하신~" 서두를 자기 마음대로 추가

### 변경 사항

#### 읽는 법 통합
- [1/4] 시장 현황: 계절/신호등 읽는 법 (~9줄) 제거
- [2/4] 매수 후보: ✅⏳🆕/추세 범례 (~10줄) 제거
- 📖 투자 가이드: 하단에 ━━━ 구분선 안에 읽는 법 집약 (시장현황 + 매수후보 + 추세)
- 사계절 2줄 분리: `🌸봄 · ☀️여름` / `🍂가을 · ❄️겨울` (모바일 줄바꿈 대응)

#### yfinance Rate Limit 수정
- `get_market_context()` 호출을 `fetch_revenue_growth()` **이전**으로 이동
- 916종목 .info 대량 호출 전에 지수 데이터 먼저 수집 → Rate Limit 회피

#### [1/4] HY/VIX 구분 명확화
- `🏦 HY Spread 2.84%` → `🏦 <b>HY Spread</b>: 2.84%`
- `⚡ VIX 15.3` → `⚡ <b>VIX</b>: 15.3`
- 항목명 볼드 + 콜론으로 각 지표 명확 구분

#### [4/4] Gemini 서두 자동 제거
- 프롬프트에 서두/인사말/도입문 금지 명시
- 응답 후처리: `**1.` 앞 텍스트 자동 삭제 (Gemini가 그래도 붙일 경우 대비)

#### 포맷 정리
- [1/4] 날짜↔지수 사이 불필요 빈줄 제거
- 읽는 법 내 서브섹션 간 빈줄 추가 (시장현황↔매수후보↔추세)

#### 국내 프로젝트 동기화
- 읽는 법 📖 가이드 통합 (시장위험신호↔매수후보 줄바꿈)
- Gemini 서두 금지 + 자동 제거 (`gemini_analysis.py`)
- 📩 오늘의 메시지 목차 삭제 (투자 가이드에서 불필요)

---

## v34 — UX 대폭 개선 (2026-02-19)

### 배경
- 고객 입장에서 날씨 아이콘, 신호등, 계절 등 설명 없이 데이터만 나오는 문제
- 읽는 법이 데이터 뒤에 있어서 읽기 순서가 역전
- [3/4] 시장 동향이 "이번 주" 전체 요약 → 어제 마감이 더 유용
- [4/4] AI 선정 이유가 "위험 신호 없이 복합 순위가 높아 선정" 반복, 주의사항 레이아웃 어색

### 변경 사항

#### 📖 투자 가이드
- 시장 위험 읽는 법 → [1/4]로 이동
- 종목 추세 읽는 법 → [2/4]로 이동
- 가이드는 전략 소개(뭘/어떻게/얼마나/언제/얼마)만 남김

#### [1/4] 시장 현황
- 상단에 읽는 법 추가: 계절(🌸봄→☀️여름→🍂가을→❄️겨울) 먼저, 신호등(🟢🔴) 다음
- 데이터 블록 공백 추가 (제목/지표/결론 사이 빈 줄)

#### [2/4] 매수 후보
- 읽는 법 보강: ✅3일 연속/⏳2일/🆕신규 의미 명시
- 추세 아이콘 범례 + 가속/둔화 예시
- 주도 업종을 읽는 법과 구분선으로 분리

#### [3/4] AI 리스크 필터
- 시장 동향 프롬프트: "금주 이벤트" → "어제 마감 + 내일 일정" 집중
- "이번 주 요약 하지 마" 명시

#### [4/4] 최종 추천
- **Google Search Grounding 채택**: Gemini가 각 종목의 실적 성장 배경을 검색해서 비즈니스 맥락 한 줄 작성
  - 기존: "EPS 316.6% 급증, 전문가 8명 모두 긍정적이에요" (숫자 반복)
  - 개선: "AI 데이터센터 수요 확대로 GPU 매출 급증 중이에요" (사업적 이유)
- A/B 테스트 후 A(Search Grounding) 확정, B(코드 템플릿)는 fallback으로 유지
- safe dict에 rev_growth 누락 수정
- 비중 한눈에 보기 삭제 (종목별 이미 표시됨)
- 괴리율/Fwd PE를 AI 입력에서 제거 (부정 멘트 원인 차단)
- 주의사항: "안정적" 표시 제거, 실질 경고만 · 종목 리스트 뒤로 이동
- 퍼널: `916→Top30→✅3일검증→최종5종목` (가중순위 제거)
- 활용법 간결화

#### 아이콘 교체 (미국+국내)
- 시장 위험: 🛡️ → 🚨
- AI 리스크 필터: 🛡️ → 🤖

#### 국내 프로젝트 동기화
- 🛡️ → 🤖/🚨 아이콘 교체 (gemini_analysis, send_telegram_auto, test_ui_preview, README, SESSION_HANDOFF)
- 비중 한눈에 보기 삭제

### 임계값 통계 검증
- Top 30 분포: 🔥5.7% ☀️28.9% 🌤️45.6% ☁️18.8% 🌧️1.0% → 현재 임계값 적절
- 🔥 >20% · ☀️ 5~20% · 🌤️ 1~5% · ☁️ ±1% · 🌧️ <-1%

---

## v33 — 재무 품질 데이터 축적 (2026-02-19)

### 배경
- Ackman 8원칙 분석 결과, 정량 하드필터보다 원본 데이터 축적이 우선
- 백테스팅을 위해 전체 유니버스 매일 수집 (과거 데이터는 yfinance로 복원 불가)
- 기존 전략/메시지/포트폴리오 로직 일절 변경 없음

### 변경 사항
1. **DB 컬럼 13개 추가** (init_database ALTER TABLE)
   - rev_growth (기존에는 메모리에서만 사용, DB 미저장이었음)
   - market_cap, free_cashflow, roe, debt_to_equity
   - operating_margin, gross_margin, current_ratio
   - total_debt, total_cash, ev, ebitda, beta
2. **fetch_revenue_growth(df, today_str)** 병합 함수
   - 기존 `fetch_revenue_growth(df)` + `fetch_quality_fundamentals()` → 단일 함수
   - 전체 유니버스 (~916종목) yfinance .info 호출
   - rev_growth + 12개 재무 지표 DB UPDATE
   - **10스레드 병렬** (ThreadPoolExecutor): 3분 20초 → 13초
3. **main()** 에서 save_part2_ranks() 직전 호출 (today_str 인수 추가)

### 성능
- 병렬 수집: 862종목 × 10스레드 = ~13초 (순차 대비 93% 단축)
- 전체 워크플로우: 4분 27초 → **1분 18초** (원래 속도 유지)

### 주의
- debtToEquity는 yfinance가 percentage로 반환 (150 = 1.5x). 분석 시 ÷100 필요
- yfinance `.info`는 현재 스냅샷만 제공 → 과거 날짜 백필 불가, 매일 쌓아야 함
- rev_growth도 매일 변할 수 있음 (가이던스 수정, 실적 발표)

---

## v32 — Risk Consistency: 리스크 관리 ↔ 최종 추천 일관성 (2026-02-19)

### 배경
v31에서 매출 필터를 제거했더니 AA(매출 -1%), HSY(매출 +7%) 같은 사이클/기저효과 종목이
Top 30에 진입. save_part2_ranks(필터ON)와 display(필터OFF)의 불일치로 rank `-→-→-` 버그 발생.
또한 [3/4] AI 리스크 필터에서 경고한 종목이 [4/4] 최종 추천에 그대로 포함되는 모순 확인.

### 변경 8개

| # | 항목 | Before (v31) | After (v32) |
|---|------|-------------|-------------|
| 1 | 매출 필터 | 제거됨 (composite 가중치만) | **10% 하드필터 복원** — 사이클/기저효과 차단 |
| 2 | rank 표시 | 🆕 종목 `-→-→-` | **`-→-→{rank}`** — hist 전부 '-'이면 현재 순위 표시 |
| 3 | 이탈 사유 | 전부 `📉 매도 검토` | **✅ 목표 달성**(괴리+) vs **⚠️ 펀더멘탈 악화** 분리 |
| 4 | 하향 비율 | 절대 30%만 | **30% 절대 + ↓≥↑(2건↑) 상대** 이중 체크 |
| 5 | 섹터 경고 | 없음 | [4/4]에서 **테크 밸류체인 3종목+** 시 집중 리스크 경고 |
| 6 | 어닝 [4/4] | [3/4]에서만 경고 | [4/4] ⚠️ 주의사항 섹션에 **📅 어닝 인라인 표시** |
| 7 | 비중 | 균등 20% | **차등 25/25/20/15/15** (composite 상위 우대) |
| 8 | Top 30 통일 | 버퍼존 21~35 | **버퍼존 완전 제거** — save/display/query 모두 Top 30 |
| 9 | UI 가독성 | 이탈 구분선 없음, [4/4] 경고 뒤섞임 | 이탈 📉 헤더+구분선, [4/4] ⚠️ 주의사항 통합, 가이드 ④ 자연스럽게 |
| 10 | AI 날짜 | "어제 미국 시장" | Gemini에 **구체적 날짜(biz_str)** 전달 |
| 11 | 애널리스트 품질 | 경고만 | **하드필터**: 저커버리지(<3명) + 하향 과다(>30%) → 순위 제외 |
| 12 | 가중 순위 | composite 순 정렬 | **3일 가중 순위** `T0×0.5 + T1×0.3 + T2×0.2` — [2/4] 표시 + [4/4] 포트폴리오 선정 |

### 설계 원칙
- **"경고했으면 행동도 바꿔야"**: [3/4] 리스크 경고와 [4/4] 최종 추천의 일관성
- **어닝은 제외 안 함**: EPS 상향 중이면 어닝이 기회. 주의 표시만 (Method C 철학)
- **매출 10%**: "파괴적 혁신 기업을 싸게" — 매출 뒷받침 없는 EPS 상향은 사이클/회복
- **차등 비중**: 신호 강도에 비례. SNDK(+327%)와 MU(+124%)가 같은 비중이면 약한 종목이 수익률 희석
- **단순함**: 버퍼존 제거 → Top 30 하나의 경계, 유지 구간 혼란 제거
- **컨센서스 순도**: "여러 전문가가 동시에 올리면 더 강한 신호" — 반대 의견 >30%면 컨센서스 아님
- **일관성 = 신뢰**: 3일 가중 순위로 하루 급변에 의한 순위 급등락 완화 (Slow In 철학, 한국 프로젝트와 동일)

### 핵심 코드 변경
- `get_part2_candidates()`: rev_growth < 0.10 + num_analysts >= 3 + 하향 ≤ 30% 필터
- `save_part2_ranks()`: 버퍼존 제거 — Top 30만 연속 저장
- `create_candidates_message()`: rank 버그 수정, 이탈 📉 헤더+분류, 버퍼존 UI 제거, 가중 순위 정렬
- `run_portfolio_recommendation()`: 차등 비중, ⚠️ 주의사항 통합(시장/어닝/섹터), 가중 순위 기반 포트폴리오 선정
- `compute_weighted_ranks()`: 3일 가중 순위 계산 (T0×0.5 + T1×0.3 + T2×0.2, 미등재 패널티 50)
- AI 리스크 필터 + 포트폴리오 필터: 하향 비율 `↓≥↑ & ↓≥2` 추가
- Gemini 프롬프트: "어제" → `{biz_str}` 구체적 날짜
- DB 쿼리 4곳: `part2_rank <= 35` → `<= 30`
- `create_guide_message()`: ④ "3일 연속 검증 완료" → "3일 연속 상위권 유지 종목만 매수 후보로"

---

## v27 — 시장 위험 지표 + 애널리스트 데이터 수정 (2026-02-13)

### 변경 내용

| 항목 | Before (v26) | After (v27) |
|------|-------------|-------------|
| 섹션 제목 | 🟢 **신용시장** — 성장기 | 🛡️ **시장 위험 지표** — ☀️ 여름(성장국면) |
| 하위 카테고리 | 없음 (HY/VIX 평렬) | 🏦 **신용시장** + ⚡ **변동성** (구분선 분리) |
| HY 스프레드 | HY Spread(부도위험) | 🏦 신용시장 → HY Spread(부도위험) |
| VIX | 📊 VIX(변동성) | ⚡ 변동성 → VIX |
| 데이터 보호 | 제거됨 | **복원** — 같은 마켓 날짜 재수집 방지 (rev_up30 등 DB 저장 완료) |
| FRED 재시도 | 없음 | HY/VIX 모두 3회 재시도 (5초 간격) |
| 투자 비중 | 📊 투자 80% | 💰 투자 80% |
| 분면 라벨 | 회복기/성장기/과열기/침체기 | 🌸봄(회복국면)/☀️여름(성장국면)/🍂가을(과열국면)/❄️겨울(침체국면) |
| 해빙 신호 | 침체기→회복기 전환 | 겨울→봄 전환 |
| 애널리스트 데이터 | 캐시 경로에서 항상 0 | **DB 저장/로드** |

### 버그 수정: HY 504 시 시장 위험 지표 전체 소실

**문제**: FRED HY Spread API 504 타임아웃 → `hy_data=None` → `if hy_data:` 블록 안에 VIX+투자비중 포함 → 전부 소실.

**해결**:
1. `if hy_data:` → `if hy_data or vix_data:` — 둘 중 하나만 있어도 섹션 표시
2. HY/VIX 각각 독립적으로 None 체크 후 표시
3. `fetch_hy_quadrant()`에 3회 재시도 (5초 간격) 추가

### 버그 수정: 애널리스트 의견 분포

**문제**: 텔레그램 메시지에서 모든 종목이 `의견 ↑0↓0`, AI 리스크 필터에서 전원 `커버리지 애널리스트가 0명`.

**원인**: v26의 NTM 데이터 보호(같은 마켓 날짜 재수집 방지) 도입 시, DB에서 데이터를 로드하는 캐시 경로에서 `rev_up30`, `rev_down30`, `num_analysts`가 DB에 저장되지 않아 항상 0으로 하드코딩.

**해결**:
1. DB 마이그레이션에 `rev_up30`, `rev_down30`, `num_analysts` 3개 컬럼 추가
2. 신규 수집 시 `UPDATE ntm_screening SET ... rev_up30=?, rev_down30=?, num_analysts=?` 저장
3. 캐시 경로에서 `SELECT ... rev_up30, rev_down30, num_analysts` 로드

### 아이콘 설계 원칙

주도업종(📊)과 시장 위험 지표 섹션의 아이콘을 완전 분리:
- 🛡️ 시장 위험 지표 (방패 = 리스크 방어)
- 🏦 신용시장 (은행 = HY Spread 하위 카테고리)
- ⚡ 변동성 (번개 = VIX 하위 카테고리)
- 💰 투자 비중 (돈주머니 = 자산 배분)
- 사계절 아이콘: 🌸☀️🍂❄️ (트렌드 ☀️와 동일하지만 문맥 구분)

---

## v26 — VIX Layer 2 구현 (2026-02-13)

### 변경 내용

v25에서 분석/설계한 VIX 전략을 실제 코드로 구현.

| 항목 | Before (v25) | After (v26) |
|------|-------------|-------------|
| VIX | 분석 문서만 | **`fetch_vix_data()` + `get_market_risk_status()` 구현** |
| 현금비중 | HY 단일 레이어 | **HY + VIX 2레이어 + Concordance** |
| 텔레그램 | VIX 표시 없음 | **[1/4] 시장 현황에 VIX 블록 추가** |
| main() 플로우 | `hy_data = fetch_hy_quadrant()` | **`risk_status = get_market_risk_status()`** |

### 수정 파일
- `daily_runner.py` — `fetch_vix_data()`, `get_market_risk_status()` 신규 + `create_market_message()` 시그니처 변경 (`hy_data=` → `risk_status=`) + main() 플로우 변경

### 구현 상세

```
fetch_vix_data():
  FRED VIXCLS CSV (~400일) → 5일 slope (±0.5 threshold)
  레짐 7종: normal / elevated / high / crisis / crisis_relief / stabilizing / complacency
  cash_adjustment: -10% ~ +15%

get_market_risk_status():
  hy = fetch_hy_quadrant()
  vix = fetch_vix_data()
  Concordance: hy Q3/Q4='warn' × vix direction
    both_warn   → VIX 전액
    hy_only     → VIX 0%
    vix_only    → VIX 50%
    both_stable → VIX 그대로
  final_cash = max(0, min(70, base + vix_adj))
```

### 검증 결과 (2026-02-13)
```
FRED VIXCLS: 282 rows, 최신 2026-02-11 = 17.65
5일 전: 21.77 → slope -4.12 (falling) → normal (안정)
HY: 2.84% Q2 여름 → 현금 20%
Concordance: both_stable → VIX 가감 0%
최종 현금: 20%
```

---

## v25 — VIX 심층 분석 + 메시지 레이아웃 개선 (2026-02-13)

### 에이전트 토의 개요

두 에이전트(한국 전략 담당 + US 전략 담당)가 각각 독립적으로 VIX 35년(1990~2025) 데이터를 웹 리서치(한국 21건, US 31건)하여 분석. 약 5분씩 소요 후 동일한 결론에 도달.

### 핵심 발견 1: VIX 기초 EDA

- 평균 19.71, 중앙값 17.84 → 우편향 분포 (평소 낮고, 간헐적 극단 스파이크)
- 구간별 빈도: <15(~35%), 15~20(~30%), 20~30(~25%), 30~40(~8%), 40+(~2%)
- Mean-Reversion Half-life: 평상시 2~10일 (Ornstein-Uhlenbeck 모형)
- 스파이크가 클수록 회귀 속도 빠름, 최근 추세로 회귀 속도 과거보다 더 빨라짐
- 구조적 위기(2008, 2020)에서는 half-life가 수개월로 확장 → HY 교차 확인 필수

### 핵심 발견 2: VIX 레벨별 향후 수익률

Hartford Funds, WisdomTree, BlackRock 실증:
- VIX 30+ 후 6개월 85% 확률 상승, 12개월 90% 확률 상승, 평균 +23.4%
- VIX 40+ 후 3년 이내 예외 없이 시장 회복 + 추가 수익
- 역설: VIX < 15(저변동) 구간 수익률이 가장 낮음 — complacency(안주) 상태에서 충격 취약
- 최악의 날과 최고의 날은 군집(cluster) — 최고의 5일 놓치면 20년 수익률 절반 감소

### 핵심 발견 3: HY-VIX 상호보완성

상관계수 +0.71(일별), VIX가 HY 변동의 51%만 설명 → 49% 독립 정보.

| 지표 | 강점 | 약점 |
|------|------|------|
| **HY** | 구조적 신용 리스크 포착, 서서히 무너지는 시장 감지(2007) | 급락장 첫 1~3일 반응 지연 |
| **VIX** | 즉각적 공포 포착, 급락 초기 선행(2020.02) | 소음 多, 신용위기 서서히 확대 시 무반응 |

동시성 vs 엇갈림 매트릭스:
- **동시 경고** (HY 악화 + VIX 상승): 가장 위험 — 2008, 2020 초기
- **동시 안정** (HY 개선 + VIX 하락): 가장 안전 — 전형적 강세장
- **VIX만 경고**: 단기 쇼크, 빠른 회복 가능 (Flash Crash류)
- **HY만 경고**: 구조적 위험 누적, 가장 교활 (2007 하반기)

### 핵심 발견 4: VIX 40+ 역설

VIX 40+에서 현금을 더 늘리면 역효과:
1. 도달 시점에 이미 시장 상당 하락 → 최악의 타이밍에 손절
2. VIX 40+는 mean-reversion이 가장 강력한 구간
3. 최악/최고의 날이 군집 → 현금이면 최고의 반등을 놓침

단, **하락 전환이 조건**: VIX 40→45→60 계속 상승 중이면 아직 정점 아님. VIX 60→55→45→38 내려오기 시작해야 공포 정점 통과 신호.

### 추천 전략: Strategy C (3레이어 복합 모델)

두 에이전트 모두 동일하게 Strategy C 추천. 이유:

**Strategy A (절대값 단독) 기각 이유**: VIX는 하루에 수십 포인트 변동 → 잦은 whipsaw. complacency(VIX<12) 탐지 시 false positive 빈번.

**Strategy B (기울기 단독) 기각 이유**: Z-score 계산에 충분한 lookback 필요. 횡보장에서 노이즈 심함. HY 없이 단독 사용 시 2007년 같은 서서히 무너지는 시장 놓침.

**Strategy C 선택 이유**:
1. 기존 HY의 구조적 판단(방향타) 보존
2. VIX의 속도(속도계) 추가로 급락 초기 1~3일 선행 방어
3. Concordance Check로 false signal 감소
4. VIX 단독의 whipsaw 문제 완화

### 메시지 레이아웃 개선

| Before | After |
|--------|-------|
| [1/3] 시장+매수후보 (한 메시지) | [1/4] 시장현황 + [2/4] 매수후보 (분리) |
| 빈 줄로 영역 구분 | ─── 구분선으로 명확 구분 |
| 가이드 타이틀 뒤 빈 줄 | 빈 줄 제거 |
| AI 종목간 빈 줄 2개 | ─── 구분선 1개 |
| [2/4] 읽는법이 Top30과 붙어있음 | 💡 읽는 법 헤더 + 구분선으로 분리 |
| 📊 주도 업종: 데이터 한 줄 | 📊 주도 업종 (라벨) + 줄바꿈 후 데이터 |

### NTM 데이터 보호 (재수집 방지)

**문제**: 같은 마켓 날짜를 여러 번 실행하면 yfinance NTM 컨센서스 데이터가 수집 시점마다 달라져 순위 뒤집힘.
예: LITE 오전 3위(gap=-0.9) → 오후 12위(gap=+8.9) — ntm_7d가 7.88→11.05로 변동.

**해결**: `run_ntm_collection()`에서 DB에 이미 해당 마켓 날짜 데이터(adj_score NOT NULL 100건+)가 있으면 NTM 수집 루프 전체 스킵. DB 기존 값으로 DataFrame 구성 후 메시지 생성으로 바로 진행.

**강제 재수집**: `FORCE_RECOLLECT=true` 환경변수 설정 시 기존 데이터 무시하고 재수집.

### 참고 자료 (에이전트 수집)

- [FRED VIXCLS](https://fred.stlouisfed.org/series/VIXCLS), [Macrotrends VIX](https://www.macrotrends.net/2603/vix-volatility-index-historical-chart)
- [Macroption VIX-SPX Correlation](https://www.macroption.com/vix-spx-correlation/)
- [Hartford Funds - When Fear Runs High](https://www.hartfordfunds.com/practice-management/client-conversations/managing-volatility/when-fear-runs-high-time-to-buy.html)
- [WisdomTree VIX Spike Bullish](https://www.wisdomtree.com/investments/blog/2025/04/28/dont-minimize-the-importance-of-the-vix-spike-its-bullish)
- [Cassini Capital - Credit Spread and VIX](https://www.cassinicap.com/t-plus/credit-spread-and-vix/index.php)
- [SSRN - VIX-HY Relationship](https://papers.ssrn.com/sol3/Delivery.cfm/5213881.pdf?abstractid=5213881)
- [Six Figure Investing - VIX/VIX3M Ratio](https://www.sixfigureinvesting.com/2012/09/taming-inverse-volatility-with-a-simple-ratio/)

---

## Phase 1: 문제 발견 (직장 PC)

### 1-1. 시작점: "마이크로소프트는 왜 안 나오는 거야?"

텔레그램 메시지에서 MSFT를 한 번도 본 적 없다는 의문에서 시작.

### 1-2. Track 1 vs Track 2 필터 불일치

**Track 1 (텔레그램)**: 10단계 필터 → ~24개 통과
**Track 2 (DB)**: 2단계 필터 → ~265개 passed

MSFT는 EPS 모멘텀(score 9.5)은 좋지만, **Price < MA200**에서 Track 1 탈락.
→ "무엇을 보유할까"와 "언제 살까"가 같은 필터에 혼재된 구조적 문제.

### 1-3. score_321 공식의 문제

```python
# 기존 방식: binary 방향 + 임의 스케일링 + 90d 미사용
if current > d7:  score += 3    # 0.01% 올라도 +3, 10% 올라도 +3
score += eps_chg_60d / 5        # 임의 스케일링
# 90d는 아예 안 봄 → TTWO(90d -34%)를 못 잡음
```

---

## Phase 2: 근본적 재설계 (집 PC)

### 2-1. +1y 컬럼의 치명적 문제 발견

현재 시스템은 `trend.loc['+1y']`로 EPS를 가져오는데, **`+1y`가 가리키는 실제 연도가 종목마다 다름:**

| 종목 | +1y endDate | 0y endDate | 원인 |
|------|-----------|-----------|------|
| AMZN | **2027**-12-31 | 2026-12-31 | FY2025 발표 완료, 롤오버 |
| CRWV | **2026**-12-31 | 2025-12-31 | FY2025 미발표 |
| AAPL | **2027**-09-30 | 2026-09-30 | 9월 결산 |

**→ +1y끼리 비교하면 2026년과 2027년 EPS가 뒤섞인 엉터리 랭킹**

**확인 방법**: `stock._analysis._earnings_trend` 리스트의 각 항목에 `endDate` 필드 존재

### 2-2. 해결: NTM (Forward 12M) EPS 도입

**0y와 +1y를 endDate 기반 시간 가중치로 블렌딩:**

```python
# 각 시점(ref_date)마다 앞으로 12개월 윈도우를 계산
window_start = ref_date
window_end = ref_date + 365일

# 0y, +1y 각각의 겹치는 기간으로 가중치 산출
w0 = overlap(window, 0y_fiscal_year) / total_overlap
w1 = overlap(window, +1y_fiscal_year) / total_overlap

NTM_EPS = w0 × (0y EPS) + w1 × (+1y EPS)
```

**핵심: 5개 시점 각각 가중치를 재계산**
- NTM_current: 오늘 기준 가중치로 블렌딩
- NTM_7d: 7일 전 기준 가중치로 블렌딩
- NTM_30d: 30일 전 기준 가중치로 블렌딩
- NTM_60d: 60일 전 기준 가중치로 블렌딩
- NTM_90d: 90일 전 기준 가중치로 블렌딩

**검증 결과 (MSFT, 6월 결산):**
```
시점       0y(FY26)    w0      +1y(FY27)   w1      NTM EPS
current    17.202    39.6%  +  19.025    60.4%  =  18.304
7d ago     17.235    41.5%  +  18.997    58.5%  =  18.266
30d ago    15.678    47.8%  +  18.545    52.2%  =  17.175
60d ago    15.633    56.0%  +  18.531    44.0%  =  16.907
90d ago    15.661    64.3%  +  18.557    35.7%  =  16.696
```
- **NTM 모멘텀: +9.63%** (기존 +1y만: +2.52%) — 0y의 강한 상향이 반영됨

### 2-3. Score 공식 (NTM 기반, 내부 로직)

```python
seg1 = (NTM_current - NTM_7d)  / |NTM_7d|  × 100   # 최근 7일
seg2 = (NTM_7d - NTM_30d)     / |NTM_30d| × 100   # 7~30일 구간
seg3 = (NTM_30d - NTM_60d)    / |NTM_60d| × 100   # 30~60일 구간
seg4 = (NTM_60d - NTM_90d)    / |NTM_90d| × 100   # 60~90일 구간

score = seg1 + seg2 + seg3 + seg4
```

**4개 구간이 겹치지 않는 독립 구간으로 90일 전체를 커버:**
```
|----seg4----|----seg3----|----seg2----|--seg1--|
90d         60d         30d          7d      today
```

**참고**: v5까지는 내부용이었으나, v5 이후 Part 1의 정렬/표시 기준으로 변경됨. Part 2 필터(Score > 3)에도 사용.

### 2-4. 추세 표시 (트래픽 라이트)

seg 방향을 트래픽 라이트로 시각화. **순서: 과거→현재 (왼→오)**
```
추세(90d/60d/30d/7d)
    🟢  🟡  🔴  🟢
    seg4 seg3 seg2 seg1
```

**임계값 (v9 이후 6단계, 네모=강한 변동):**
- 🟩 폭발적 상승: > 20%
- 🟢 상승: 2~20%
- 🔵 양호: 0.5~2%
- 🟡 보합: 0~0.5%
- 🔴 하락: 0~-10%
- 🟥 급락: < -10%

**15개 말 설명 카테고리** (트래픽 라이트와 함께 표시):
| 패턴 | 말 설명 |
|------|---------|
| 🟢🟢🟢🟢 | 강세 지속 |
| 🟢🟢🟢🟡 | 소폭 감속 |
| 🟢🟢🟢🔴 | 최근 꺾임 |
| 🟢🟢🟡🟡 | 둔화 |
| 🔴🟢🟢🟢 | 반등 |
| 🔴🔴🟢🟢 | 가속 |
| 🟡🟡🟡🟡 | 거의 정체 |
| 🔴🔴🔴🔴 | 하락세 |
| 기타 (🟢 ≥ 3) | 소폭 개선 |
| 기타 (🔴 ≥ 3) | 최근 약세 |
| 기타 (상승세) | 회복 중, 변동 개선 등 |
| 기타 | 혼조, 등락 반복 등 |

**결정 근거 (v5):** 기존 ↑↓ 화살표는 방향만 표시. ▲△ 시도했으나 모바일에서 크기 다른 문제.
트래픽 라이트는 방향+속도를 동시에 표현하며 크기 일관됨. 말 설명 추가로 고객 이해도 향상.

---

## Phase 3: 시뮬레이션 & Part 2 재설계 (집 PC)

### 3-1. 유니버스 정리

- **현행 유지**: NASDAQ 100 + S&P 500 + S&P 400 MidCap = **915개**
- **FOX 제거**: FOX(Class B)는 eps_trend 데이터 없음, FOXA(Class A)가 커버 → `INDICES['SP500']`에서 제거 완료
- GOOG/GOOGL 둘 다 유지 (Class A/C 별도 상장)

### 3-2. 이상치 처리: |NTM EPS| < $1.00 분리

**문제 발견**: 1차 시뮬레이션에서 ALB 스코어 54,065 (NTM_90d ≈ $0.01 → 분모 폭발)

**검토한 대안들:**
| 방법 | 결과 | 문제점 |
|------|------|--------|
| 세그먼트 캡 (±200%) | ALB 800점 | 여전히 SNDK(322)보다 높음 — 부당 |
| 최소 분모 ($0.50) | ALB 521점 | 여전히 비정상적으로 높음 |
| Z-Score 정규화 | 분포 기반 | 1-2개 이상치가 전체 분포를 왜곡 |
| **|NTM| < $1.00 분리** ✅ | **깔끔하게 해결** | 없음 |

**최종 결정: $1.00 최소 EPS 기준**
- `abs(NTM_current) < $1.00` 또는 `abs(NTM_90d) < $1.00`이면 → **"턴어라운드" 카테고리**로 분리 (양 끝점만 체크, 중간 시점 이상치 방지)
- 메인 랭킹에서 제외, 별도 섹션으로 표시
- **근거**: NTM EPS가 $1 미만인 종목은 성장률 계산이 의미 없음 (0.01→0.02가 +100%)

### 3-3. 풀 유니버스 시뮬레이션 결과 (2026-02-06)

**기본 통계:**
```
전체 유니버스: 916개 (FOX 제거 전)
데이터 있음:   913개
데이터 없음:     0개
에러:            3개 (COKE, L, NEU — endDate 파싱 에러)
```

**$1 필터 적용 후:**
```
메인 랭킹:    861개 (|NTM| >= $1.00)
턴어라운드:    52개 (|NTM| < $1.00)
```

**Score 분포 (메인 861개):**
```
Score >  0:  596 (69%)
Score >  1:  506 (59%)
Score >  2:  389 (45%)
Score >  3:  310 (36%)
Score >  5:  177 (21%)
Score > 10:   74 (9%)
Score > 20:   31 (4%)
Min=-98.19, Max=322.15, Median=1.26
정배열: 236
```

### 3-4. Part 2: Forward P/E 변화율 (= 괴리율)

**기존 Part 2 (폐기):** MA200, RSI 기반 기술적 진입 타이밍
**새 Part 2:** EPS 개선이 아직 주가에 반영 안 된 종목 찾기

**핵심 지표: Forward P/E 90일 변화율 (고객 표시명: "괴리율")**
```python
Fwd_PE_now = Price_now / NTM_current
Fwd_PE_90d = Price_90d / NTM_90d
괴리율 = (Fwd_PE_now - Fwd_PE_90d) / Fwd_PE_90d × 100
```

**해석:**
- **괴리율 마이너스** = EPS 상향 > 주가 상승 → "아직 덜 반영됨" → **매수 기회**
- **괴리율 플러스** = 주가 상승 > EPS 상향 → "이미 선반영됨" → 추격 매수 위험

---

## Phase 4: 텔레그램 포맷 & 발송 채널 확정 ← NEW

### 4-1. Part 1 순위 기준 변경 (재변경: v5)

**v4 결정**: 90일 이익변화율 기준 정렬
**v5 재변경**: **Score 기준 정렬** (세그먼트 캡 ±100% 적용)

**v5 재변경 이유**: 시스템의 핵심이 4구간 Score인데 고객에게 다른 값을 보여주는 건 불일치.
Score가 높으면 꾸준하고 강하게 오르는 중이라는 의미를 읽는 법에서 설명.
세그먼트 캡(±100%)으로 이상치(ELS: 942→9.3) 방지.

### 4-2. 업종 분류

- yfinance의 `industry` 필드 사용 (130개 고유값)
- 한글 축약 매핑 테이블 1회 생성 (예: Semiconductors → 반도체, Software-Application → 응용SW)
- 매핑 안 된 건 영어 그대로 표시

### 4-3. 텔레그램 메시지 포맷 (v5~v6 모바일 최적화)

**설계 원칙:**
- 모바일 가로폭 우선 (테이블/고정폭 헤더 폐기)
- 3줄 레이아웃: rank+데이터 / 이름(티커) 업종 / 신호등+설명
- HTML bold/italic으로 시각적 구분, 종목 사이 빈 줄
- "읽는 법" 가이드를 데이터 목록 위에 배치
- 친절한 말투 + 수치 + 이해하기 쉬운 단어 ("~예요" 어체)

**Part 1: EPS 모멘텀 Top 30** (Score 순)
```
📈 EPS 모멘텀 Top 30
💡 읽는 법: Score, 4단계 신호등 설명

1위 · Score 225.6
Sandisk Corporation (SNDK) 반도체
🟢🟢🟢🟢 강세 지속
```

**Part 2: 매수 후보 Top 30** (Score > 3 필터, 괴리율 순)
```
💰 매수 후보 Top 30
💡 읽는 법: EPS/주가 가중평균, 신호등, ⚠️ 설명

1위 · EPS +5.2% · 주가 -12.3%
MicroStrategy (MSTR) 응용SW
🟢🟡🟢🔴 일시 조정
```

**턴어라운드 주목** (Top 10, Score 순)
```
⚡ 턴어라운드 주목
💡 읽는 법: 90일 전→현재 EPS 전망치, 예시 설명

1위 · EPS $0.24 → $2.61
Albemarle Corporation (ALB) 특수화학
🟢🟢🟢🟡 소폭 감속
```

### 4-4. 발송 채널

| 메시지 | 로컬 실행 | GitHub Actions |
|--------|----------|---------------|
| Part 1 (모멘텀 랭킹) | 개인봇 | **채널** |
| Part 2 (매수 후보) | 개인봇 | **채널** |
| 턴어라운드 | 개인봇 | **채널** |
| 시스템 로그 | 개인봇 | 개인봇 |

### 4-5. 실행 스케줄

- **GitHub Actions**: 매일 KST 07:30 (미국 장마감 ET 16:00 = KST 06:00, 데이터 안정화 후 1.5시간)
- **로컬**: 수동 실행 (개인봇에만 발송)

---

## 결정된 사항 ✅

1. **+1y → NTM EPS 전환**: endDate 기반 시간 가중 블렌딩
2. **Score = seg1+seg2+seg3+seg4**: 내부 DB 저장/필터링용
3. **Part 1 고객 표시는 Score**: 세그먼트 캡 ±100% 적용 (v5에서 변경)
4. **Part 1 정렬: Score 순** (v5에서 90일 이익변화율→Score로 재변경)
5. **Part 2 정렬: 괴리율 순** (Fwd P/E 90일 변화율)
6. **Part 2 필터: Score > 10** (상위 10% EPS 모멘텀, v9에서 3→10으로 상향)
7. **DB는 전 종목 저장**: 915개 전체 (나중에 순위 진입/이탈 추적 가능)
8. **패턴은 별도 저장 불필요**: 5개 NTM 값에서 재계산
9. **턴어라운드 분리**: `abs(current) < $1.00 OR abs(90d) < $1.00` → 양 끝점만 체크 (중간 시점 이상치 방지)
10. **FOX 제거**: eps_trend 없음, FOXA가 커버
11. **Fwd PE / 괴리율은 DB 미저장**: NTM 값 + Yahoo 주가에서 파생 가능
12. **업종: yfinance industry** → 한글 축약 매핑
13. **종목 표기: 이름(티커)** (v5에서 변경)
14. **추세: 6단계 트래픽 라이트 🟩🟢🔵🟡🔴🟥** — 네모=강한 변동, 순서 = 과거→현재, Score 기반 15개 말 설명 동반 (v9)
15. **Part 1: Top 30, GitHub Actions시 채널 발송** (로컬은 개인봇)
16. **Part 2: Top 30, GitHub Actions시 채널 발송, EPS 변화 > 0 필터 적용**
17. **턴어라운드: 내부 분리만, 메시지 발송 안 함** (v9에서 제거 — 대부분 저 베이스 왜곡, 진짜 턴어라운드 극소수)
18. **시스템 로그: 개인봇에만 발송** (DB 적재 컬럼명 포함)
19. **실행 스케줄: KST 07:30 GitHub Actions**
20. **에러 종목: skip** (로그만 남김)
21. **기존 eps_snapshots 테이블: 삭제**
22. **모바일 UI 리디자인 (v5)**: 테이블/고정폭 헤더 폐기, 한 줄/두 줄 compact 포맷, 고객 친화적 부제
23. **6단계 트래픽 라이트 (v5→v9)**: ↑↓→4단계→6단계. 네모(🟩🟥)=강한 변동, 동그라미=일반
24. **⚠️ 경고 시스템 (v6)**: Part 2에서 |주가변화|/|이익변화| > 5일 때 표시, 하드 필터 아닌 소프트 경고
25. **Part 2 EPS>0 필터 (v6)**: 이익 변화율 음수 종목은 매수 후보에서 제외
26. **읽는 법 헤더 배치 (v6)**: 모든 메시지에서 데이터 목록 위에 해석 가이드 배치
27. **Part 1 채널 발송 (v6)**: GitHub Actions 실행 시 Part 1도 채널로 발송
28. **세그먼트 캡 ±100% (v5)**: Score 이상치 방지 (ELS: 942→9.3)
29. **Part 2 EPS/주가 90일 변화율 (v9)**: 직관적 실제값 표시 (가중평균에서 변경)
30. **"이익" → "EPS 전망치" (v5)**: NTM이므로 전망치임을 명확히. 첫 등장시 "(향후 12개월 주당순이익 예상)" 부연
31. **친절한 말투 (v5)**: "~예요/~해요" 어체 통일, 구체적 예시와 수치 포함
32. **Score → "EPS 점수" (v7)**: 고객 표시명 변경, 상승 폭+지속성 종합 설명
33. **턴어라운드 Score > 3 필터 (v7)**: 고정 Top 10 → 품질 필터 후 전체 표시
34. **발송 순서 (v9)**: Part 1→Part 2→AI 리스크 체크→시스템 로그 (4종)
35. **Part 2 핵심 문구 (v7)**: "오늘의 핵심 리포트예요" 인트로 추가
36. **3줄 레이아웃 변경 (v7)**: 순위+종목명 / 섹터+데이터 / 신호등 (종목명 우선)
37. **AI 소거법 전환 (v10)**: 뉴스 스캐너→리스크 스캐너, 호재 제거, 🚫/📅/✅ 3단 구조
38. **AI 상시 리스크 금지 (v10)**: "경쟁 심화" 같은 업종 일반론 차단, 최근 실제 뉴스만
39. **AI anti-hallucination (v10)**: Google 검색 결과에 없으면 ✅로 분류, temperature 0.2
40. **AI 구분선 후처리 (v10)**: 🚫 섹션 종목 간 ────── 삽입 (코드, Gemini 비의존)
41. **AI 재무 리스크 항목 (v10)**: 신용등급 하향, 유동성 위기, 파산 우려 뉴스 보고

---

## 미결 사항 ❓

(모두 해결됨 — 코드 마이그레이션 완료, 업종 매핑 130개 구현 완료)

---

## 폐기 대상 (기존 시스템)

| 항목 | 이유 |
|------|------|
| score_321 | NTM Score로 대체 |
| quality_score (100점) | NTM Score로 대체 |
| Kill Switch (7d -1%) | seg1이 자연 감점 |
| 이상치 필터 (60d > 200%) | $1 기준으로 턴어라운드 분리 |
| passed_screen 플래그 | score 랭킹으로 대체 |
| 기존 eps_snapshots 테이블 | ntm_screening으로 대체, **삭제** |
| MA200/RSI/action 로직 | Part 2에서 괴리율로 대체 |
| get_action_label() | 폐기 |
| is_actionable() | 폐기 |

---

## DB 스키마 (새)

```sql
CREATE TABLE ntm_screening (
    date        TEXT,     -- 스크리닝 날짜
    ticker      TEXT,     -- 종목
    rank        INTEGER,  -- 그날의 순위 (Score 기준, 턴어라운드는 0)
    score       REAL,     -- seg1+seg2+seg3+seg4 (내부 필터링용)
    ntm_current REAL,     -- NTM EPS 현재 추정치
    ntm_7d      REAL,     -- NTM EPS 7일 전 추정치
    ntm_30d     REAL,     -- NTM EPS 30일 전 추정치
    ntm_60d     REAL,     -- NTM EPS 60일 전 추정치
    ntm_90d     REAL,     -- NTM EPS 90일 전 추정치
    is_turnaround INTEGER DEFAULT 0,  -- |NTM| < $1.00 여부
    PRIMARY KEY (date, ticker)
);
```

**설계 근거:**
- 원본 5개 NTM 값 보존 → 나중에 공식 바꿔도 재계산 가능
- 전 종목 저장 → 순위 진입/이탈 추적, 3개월 후 수익률 상관관계 분석
- 패턴/괴리율은 5개 값에서 파생 → 별도 컬럼 불필요
- is_turnaround 플래그로 메인/턴어라운드 구분

---

## 기술 참고

### 데이터 접근 방법

```python
# NTM 계산에 필요한 데이터
stock = yf.Ticker(ticker)
eps_trend = stock.eps_trend                    # 5개 시점 × 4개 기간
raw_trend = stock._analysis._earnings_trend    # endDate 포함

# endDate 추출
for item in raw_trend:
    period = item['period']      # '0y', '+1y'
    end_date = item['endDate']   # '2026-12-31'

# 업종 정보
industry = stock.info.get('industry', 'N/A')   # ex: "Semiconductors"
```

### NTM 계산 핵심 코드

```python
# 각 snapshot별 시간 가중 NTM 계산
snapshots = {'current': 0, '7daysAgo': 7, '30daysAgo': 30, '60daysAgo': 60, '90daysAgo': 90}
for col, days_ago in snapshots.items():
    ref = today - timedelta(days=days_ago)
    we = ref + timedelta(days=365)
    o0d = max(0, (min(we, fy0_end) - max(ref, fy0_start)).days)
    o1d = max(0, (min(we, fy1_end) - max(ref, fy1_start)).days)
    total = o0d + o1d
    ntm[col] = (o0d/total) * eps_0y + (o1d/total) * eps_1y

# 턴어라운드 판별 (양 끝점만 체크)
is_turnaround = abs(ntm_current) < 1.0 or abs(ntm_90d) < 1.0

# Score는 Part 1 정렬 기준 (세그먼트 캡 ±100%)
SEG_CAP = 100
seg1 = clamp(-SEG_CAP, (ntm_cur - ntm_7d) / abs(ntm_7d) * 100, SEG_CAP)
score = seg1 + seg2 + seg3 + seg4

# 괴리율 (Part 2 정렬 기준, 가중평균 Fwd PE 변화)
weights = {7d: 0.4, 30d: 0.3, 60d: 0.2, 90d: 0.1}
fwd_pe_chg = weighted_avg(pe_change_per_period, weights)

# Part 2 표시: EPS/주가 가중평균 변화율
eps_chg_weighted = weighted_avg(eps_change_per_period, weights)
price_chg_weighted = weighted_avg(price_change_per_period, weights)
```

---

## Phase 7: AI 리스크 검증 도입 (v8)

### 7-1. LLM 선정

- Claude Max 구독($110/월)은 API 접근 불포함 → GitHub Actions 자동화 불가
- Gemini Pro 구독 보유, but Gemini 2.5 Pro 무료 티어 quota=0 (429 RESOURCE_EXHAUSTED)
- **Gemini 2.5 Flash**: 무료 티어 사용 가능, 1.5 Pro보다 성능 우수 → 채택

### 7-2. AI 역할 정의: "추천"이 아닌 "검증"

시스템이 이미 매수 후보를 선별했으므로, AI의 역할은 **리스크 검증/필터링**:
- "사도 되는 것" vs "사면 안 되는 것" 판별
- 시스템이 못 보는 6가지 영역 보완:
  ① EPS 품질 (매출 성장 vs 비용절감 vs 일회성)
  ② 악재 유무 (소송, 규제, 경쟁, 대주주 매도)
  ③ 밸류에이션 (동종업계 대비 Fwd PE)
  ④ 재무 건전성 (부채, FCF)
  ⑤ 실적 발표 타이밍
  ⑥ 내부자/기관 동향

### 7-3. 프롬프트 설계 (5회 반복)

- v1: "추천 5개" → 너무 단순
- v2: 6개 체크포인트 추가 → 섹터 분석 리포트가 되어버림
- v3: 사용자 "섹터분석을 하라는게 아니야" → 방향 수정
- v4: "추천" → "검증/필터링"으로 역할 전환
- v5 (최종): 시스템 설명 + 검증 관점 6개 + 출력 형식 3000자

핵심 결정:
- Part 2가 검증 대상, Part 1/Turnaround는 참고 데이터
- EPS%/주가% 수치는 시스템 내부 가중평균이므로 AI가 인용 금지
- Google Search Grounding으로 실시간 웹 검색 활성화

### 7-4. 메시지 체계 변경

v8에서 5종으로 확장 후, v9에서 턴어라운드 메시지 제거하여 4종으로 축소:
Part 1 → Part 2 → AI 리스크 체크 → 시스템 로그
AI 분석은 개인봇에만 전송 (로컬/GitHub Actions 모두)

### 7-5. 기술 구현

- SDK: `google-genai>=1.0.0` (NOT google-generativeai)
- Markdown→HTML 변환: `**bold**` → `<b>`, `*italic*` → `<i>`, `#` headers strip
- `run_ai_analysis()` 함수: daily_runner.py lines 681-793

### 시뮬레이션 스크립트 (개발 시 사용, 현재 프로젝트에 미포함)
개발 과정에서 ntm_simulation.py, ntm_sim.py, ntm_sim2.py, collect_industries.py 등을 사용.
프로덕션 코드(daily_runner.py + eps_momentum_system.py)에 모두 반영 완료되어 삭제됨.

---

## Phase 8: UX 개선 & 시스템 철학 정립 (v9)

### 8-1. 6단계 트래픽 라이트 (네모=강한 변동)

4단계에서 6단계로 확장. 도형의 모양(네모 vs 동그라미)으로 변동폭 크기 표현:
- 🟩 폭발적 상승 (>20%) — 네모
- 🟢 상승 (2~20%)
- 🔵 양호 (0.5~2%)
- 🟡 보합 (0~0.5%)
- 🔴 하락 (0~-10%)
- 🟥 급락 (<-10%) — 네모

**결정 근거**: Score는 총합(magnitude), 신호등은 패턴(direction) — 두 가지 보완적 차원.
기존 4단계는 방향만 표시했으나, 🟩🟥 추가로 magnitude 정보도 부분 반영.
데이터 분포: 🟩 출현률 0.5%, 🟥 출현률 1.2% — 희귀해서 의미 있음.

### 8-2. 턴어라운드 메시지 제거

**제거 이유**:
- ALB 같은 종목: NTM $0.24→$2.61이지만 진짜 턴어라운드가 아닌 저 베이스 EPS
- EPS 추정치 부호 변환(적자→흑자)이 실제 주가를 움직이는 경우는 극소수
- 내부 is_turnaround 플래그는 Score 왜곡 방지용으로 유지, 고객 메시지에서만 제거

### 8-3. AI 역할 변경: 분석가 → 뉴스 스캐너 → 리스크 스캐너

**v8**: 6가지 체크포인트로 분석 → 메시지 폭발, 부정확한 판단
**v9**: 팩트만 전달하는 뉴스 스캐너 → 실적 숫자 나열 문제
**v10**: 리스크 소거법으로 전환 (프롬프트 5회 반복 튜닝)

**v10 최종 설계 — 소거법**:
시스템이 이미 좋은 종목을 골랐으니, AI는 "숨은 위험"만 찾아서 걸러냄.
리스크 미발견 종목 = 진짜 매수 후보.

출력 구조:
- 📰 이번 주 시장: 매크로 이벤트 2-3줄
- 🚫 주의: 최근 1~2주 실제 리스크 뉴스만 (종목 간 구분선)
- 📅 어닝 주의: 2주 내 실적발표 임박 종목
- ✅ 리스크 미발견: 나머지 종목 나열

**프롬프트 튜닝 과정 (5회)**:
1. v9 뉴스 스캐너 → 실적 숫자 나열 문제 (SNDK 매출 상회, ASML 29% 증가 등)
2. 리스크+호재 분리 → 같은 종목에 리스크/호재 공존 → 판단 더 혼란
3. 섹터 그룹화 시도 → 정보량 늘었으나 여전히 혼란
4. 소거법 전환(리스크만) → 상시 리스크 일반론 생성 ("경쟁 심화", "가격 변동성")
5. anti-hallucination 강화 + 예시 기반 출력 형식 → 최종 안정화

**핵심 결정들**:
- 호재 보고 제거: Part 2가 이미 좋은 종목을 골랐음 → AI가 호재 반복은 무의미
- 상시 리스크 금지: "연료비 변동성" 같은 업종 상시 리스크 ≠ 최근 뉴스
- anti-hallucination: Google 검색 결과에 없으면 ✅로 분류
- temperature 0.7→0.2: hallucination 억제
- 구분선 후처리: Gemini 출력 후 🚫 섹션 bullet 사이에 ────── 삽입 (코드)

### 8-4. Part 2 Score 기준 상향 (3→10)

**문제**: Score > 3은 양수 종목의 50%(307개)를 통과 → 필터가 아닌 반쪽짜리 통과
- 41%가 90일 EPS 변화 5% 미만 (노이즈 수준)
- Part 1 30위(17.6)의 1/6 수준인 종목이 "매수 후보"에 올라옴

**결정**: Score > 10 (64종목, 상위 10%)
- 신흥강자도 EPS가 2주 내 10%+ 뛰면 통과 가능 (매일 실행이므로 놓치지 않음)

### 8-5. Part 2 표시값 변경

**기존**: 가중평균 EPS%/주가% → 내부 계산값이라 고객이 이해 불가
**변경**: 90일 대비 EPS 추정치 변화율 & 주가 변화율 (직관적 실제값)
- 정렬 기준(가중평균 Fwd PE 변화)은 유지, 표시만 변경

### 8-8. Part 2 괴리율 가중치 검토 — 최근가중 유지 결정

**현재**: 7d×40% + 30d×30% + 60d×20% + 90d×10% (최근가중)

3가지 방식 비교 검토:
| 방식 | 장점 | 단점 |
|------|------|------|
| **최근가중 (현행)** | 괴리 방향 구분 가능 | 7d 노이즈에 민감 |
| 동일가중 (25%씩) | 다시점 확인으로 안정적 | 좁혀지는/벌어지는 괴리 구분 불가 |
| 단순 90d | 직관적, 총 괴리 반영 | 단일 시점, 노이즈 취약 |

**핵심 발견 — 동일가중의 맹점:**
```
종목 X (시장이 따라잡는 중): 7d -5%, 30d -10%, 60d -15%, 90d -20%
종목 Y (지금 벌어지는 중):  7d -20%, 30d -15%, 60d -5%,  90d 0%
동일가중: 둘 다 -12.5% (구분 불가!)
최근가중: X=-10.0%, Y=-13.5% (Y가 더 높음 ✓)
```
종목 X는 PE가 회복 중(시장이 이미 따라잡는 중), 종목 Y는 지금 벌어지는 중(신선한 기회).
Part 2 취지("주가가 아직 못 따라간 종목")에는 Y가 더 맞고, 최근가중만 이를 구분함.

**결정**: 최근가중 유지. "지금 벌어지고 있는 괴리"에 높은 점수를 주는 것이 Part 2 아젠다에 부합.

### 8-9. Part 1 Score 가중치 검토 — 동일가중(현행) 유지 결정

**질문**: Part 2처럼 Part 1 Score에도 최근가중을 넣으면 가속 중인 종목이 더 높은 평가를 받지 않을까?

**핵심 발견 — 세그먼트 기간 비대칭으로 이미 최근가중 내재:**
```
seg1: 7d→오늘   (7일)  → Score 25% → 일당 3.6%
seg2: 30d→7d   (23일) → Score 25% → 일당 1.1%
seg3: 60d→30d  (30일) → Score 25% → 일당 0.83%
seg4: 90d→60d  (30일) → Score 25% → 일당 0.83%
```
seg1은 7일치인데 Score의 25%를 차지 → **일당 비중이 seg3/seg4의 4.3배**.
동일한 5% 변화라도 seg1(7일)에서 발생하면 seg3(30일)보다 4배 빠른 속도의 변화이며, Score에는 동일하게 반영됨.

**결정**: 현행 유지. 세그먼트 구조 자체가 이미 강한 최근가중을 내포하고 있어, 명시적 가중치를 추가하면 이중 가중이 됨.

### 8-6. ⚠️ 경고 간소화

경고 텍스트 줄 제거, ⚠️ 아이콘만 유지. AI 뉴스 스캐너가 해당 종목의 실제 뉴스를 찾아주므로 보완됨.

### 8-7. 프로젝트 정리

- `setup_scheduler.ps1` 삭제 (레거시, GitHub Actions가 대체)
- `create_turnaround_message()` 함수 삭제 (데드 코드)
- 시스템 로그 aligned_count: 🟢==4 체크 → 🔴🟥 미포함 체크로 수정

### 8-8. 방향 보정 (adj_score) 도입

**배경**: Score는 EPS 변화의 "크기"만 반영. 같은 Score라도 최근 가속 vs 최근 꺾임은 질적으로 다름.
고객은 순위 위주로 보기 때문에, 패턴 품질이 순위(Score)에 반영되어야 함.

**논의 과정**:
1. 카테고리 방식 (7개 패턴 × 고정 보너스) vs 연속 공식 비교
2. 카테고리 방식: 자의적 파라미터 14개+, 경계값 절벽 문제
3. 연속 공식: 파라미터 1개(÷30), 통계적 근거(1σ=12%), 매끄러운 보정

**최종 공식**:
```
recent = (seg1 + seg2) / 2
old = (seg3 + seg4) / 2
direction = recent - old
adj_score = score × (1 + clamp(direction / 30, -0.3, +0.3))
```
- ÷30 근거: direction 표준편차 3.67, 1σ → 12% 보정이 적절
- ±30% 캡으로 극단값 제한

**효과** (2026-02-08 데이터):
- Part 1 Top 30: 꺾임 종목 제거 (3.3%→0%), 가속 비율 증가 (50%→60%)
- 대표 예시: AR(direction -19.5, 꺾임) 19위→27위, ASML(+6.0, 가속) 35위→28위

### 8-9. Part 2 필터 기준: adj_score > 9

**논의**: 8, 9, 10 세 기준 비교.
- adj>10: 64종목 — 보수적
- adj>9: 71종목 — 10→9 추가 7종목 중 5개 가속 패턴, 꺾임 0개 (품질 유지)
- adj>8: 86종목 — 9→8 추가 15종목 중 꺾임 4개 (품질 하락, 꺾임 비율 5.6%→9.3%)

**결정**: adj_score > 9. 후보 풀을 넓히되 품질은 유지하는 최적 지점.
Top 30은 세 기준 모두 동일 (30위 adj_score=17.7로 기준과 무관).

---

## Phase 9: Part 2 매수 후보 품질 판별 강화 (v12)

### 9-1. 매출·재무 리스크 검토 — Layer 2 탐색과 포기

**문제**: Part 2가 EPS 모멘텀+가격 괴리로 30종목을 골라주지만, 매출 성장이나 재무 리스크 체크가 없어 어떤 종목을 살지 판단하기 어려움.

**탐색 과정**:
1. `_earnings_trend`에 `revenueEstimate` 데이터 발견 → NO 추가 API 호출 필요
2. 그러나 revenue estimate에는 historical snapshot(7d/30d/60d/90d 전) 없음 → 변화 추세 추적 불가
3. **핵심 발견**: Part 2 종목(adj_score > 9, EPS > 0)은 거의 전부 NTM Revenue Growth 양수 → **변별력 없음**
4. Revenue direction display 포기 → Revision Breadth(변별력 있음)로 전환

### 9-2. 트래픽 라이트 설명 리디자인 (15개 → 8패턴 + 강도 수식어)

**문제**: 기존 15개 카테고리가 과도하게 세분화. 비슷한 표현들이 고객을 혼란시킴.

**새 설계**: 8개 기본 패턴 + 🟩🟥 강도 수식어

기본 패턴 (base):
- **횡보**: flat 3개 이상
- **하락**: neg 3개 이상
- **전구간 상승**: neg 0, 최근≈과거
- **상향 가속**: neg 0, 최근 > 과거 + 2
- **상향 둔화**: neg 0, 과거 > 최근 + 2
- **반등**: 과거 neg 우세 → 최근 pos 우세, 최근avg > 과거avg
- **추세 전환**: 과거 pos 우세 → 최근 neg 우세, 과거avg > 최근avg
- **등락 반복**: 위 어디에도 해당 안 됨

강도 수식어:
- 🟩 + 🟥 → 급등락/급락 후 반등/급격한 전환 (base에 따라)
- 🟩 → 폭발적 상승/폭발적 가속/폭발적 반등
- 🟥 → 급락/급격한 전환/급락 후 반등/급등락

**Score = magnitude + direction (adj_score에 통합), lights = 시각적 패턴 표시** — 보완적 역할.

### 9-3. Part 2 라인 3 강화: 괴리율 표시

**문제**: EPS%, 주가% 변화율만 보여주면 순위 기준(괴리율)이 보이지 않음. Type A(EPS 크게 상승, 주가도 오르지만 덜) vs Type B(EPS 소폭 상승, 주가 급락)의 구분 필요.

**변경**:
```
EPS +45.2% / 주가 +12.1% · 괴리 -28.3
```
- 괴리 = Fwd PE 가중평균 변화. 마이너스 = EPS 대비 주가 미반영도 (순위 기준)
- 고객이 "왜 이 순서인가" 이해 가능

### 9-4. Part 2 라인 4 추가: 애널리스트 의견

**변경**: `의견 ↑N ↓N` (30일간 EPS 상향/하향 수정 애널리스트 수)

**설계 결정들**:
- 한글 라벨("전원 상향", "상향 우세") 대신 숫자 표기 → 트래픽 라이트 설명과 시각적 혼동 방지
- `epsRevisions.upLast30days`, `downLast30days` 사용 (이미 캐시된 `_earnings_trend`에서)
- 0y 기간 데이터 사용 (당기)
- 의견이 0↑0↓인 경우도 그대로 표시 (중립 = 무관심 = 사용자가 판단)

### 9-5. 읽는 법 헤더 업데이트

Part 2 읽는 법에 추가:
```
괴리 = EPS 대비 주가 미반영도 (순위 기준)
의견 ↑↓ = 30일간 EPS 상향/하향 애널리스트 수
```

### 9-6. 트래픽 라이트 12패턴 확장 (v13)

**문제**: v12의 8패턴에서 "상향 가속"이 Top 30 중 12개(40%)에 해당. 신호등 모양이 🟢🟢🟢🟢, 🔵🔵🟢🟢, 🟡🟢🟢🔵, 🟡🔵🔵🟢로 다른데 문구가 동일하여 혼란.
- **원인**: `recent_avg - old_avg > 2` 기준이 너무 느슨. 2점 평균만으로 4세그먼트 형태 구분 불가.

**해결**: neg_count==0 분기에서 **피크 위치 + 형태 분석**으로 5개 하위 패턴 분화:
- **전구간 상승**: 전체 평균 < 1.5 (미미한 변동)
- **꾸준한 상승**: spread/mean < 0.8 (균일한 분포)
- **최근 급상향**: seg1(최근)이 피크, 나머지 평균의 3배 초과
- **상향 가속**: seg1이 피크이나 점진적 증가 / seg2 피크이나 seg1 유지
- **중반 강세**: seg2가 피크, seg1이 피크의 60% 미만 (정점 지남)
- **상향 둔화**: seg3/seg4(초반)가 피크

**동률 처리**: `segs.index(max_seg)` → `max(range(4), key=lambda i: (segs[i], i))` — 동률 시 최근(오른쪽) 우선

**수식어 추가**:
- 🟩 + 최근 급상향 → "폭발적 급상향"
- 🟩 + 중반 강세 → "중반 급등"
- 🟩 + 상향 둔화 → "급등 후 둔화"
- 🟩 + 꾸준한 상승 → "폭발적 상승"

**진동 감지 추가**:
양수 영역 내 zigzag 패턴 (high-low-high-low) 감지:
- 인접 구간 차이 부호가 3번 교차 (`signs[0]*signs[1] < 0 and signs[1]*signs[2] < 0`)
- 최소 진폭 > 3 (미세 변동 제외)
- "상승 등락" (🟩 수식어 → "폭발적 등락")
- TLN [17.1→1.3→9.7→1.1], FIVE [11.2→1.2→9.2→1.6], MSTR [62.9→0.2→43.5→14.4] 감지

**결과**: 12개 "상향 가속" → 11개 카테고리로 분산 (최대 5개/카테고리, 17%)

### 9-7. 매출 성장 필터 검토 — 불필요, 도입 안 함

**질문**: EPS 전망치 상향이 진짜 회사 성장인지, 비용절감/자사주매입인지 구분하려면 매출 성장 필터가 필요하지 않은가?

**검증** (2026-02-08 데이터, adj_score > 9 종목 71개):
- YoY 매출 10%↑: 43개 (60.6%), 평균 adj_score **31.06**
- YoY 매출 0~10%: 19개 (26.8%), 평균 adj_score 21.29
- YoY 매출 역성장: 9개 (12.7%), 평균 adj_score **11.25** (하위권 집중)

**핵심 발견**:
- Top 30 내 매출 역성장 종목: **0개** (필터 없이도 자연 정렬)
- Top 10 중 10%↑ 성장: 9/10 (90%)
- 매출 역성장 9종목은 전부 adj_score 9~13 하위권 (ROIV, MRNA, TKO, BRK-B, SMCI 등)
- adj_score가 높을수록 매출 성장도 강한 양의 상관관계

**결정**: 매출 필터 도입 안 함.
- adj_score가 이미 매출 성장과 자연 상관 → 별도 필터 불필요
- 추가 시 916종목 × quarterly_income_stmt API 호출 → 수집 시간 대폭 증가
- 애널리스트 EPS 전망치 상향 자체가 대부분 매출 성장 전망에 기반 (자사주매입은 이미 모델에 반영)

---

*v1 작성: Claude Opus 4.6 | 2026-02-06 직장 PC*
*v2 업데이트: Claude Opus 4.6 | 2026-02-06 집 PC*
*v3 업데이트: Claude Opus 4.6 | 2026-02-06 집 PC — 시뮬레이션 결과 & Part 2 재설계*
*v4 업데이트: Claude Opus 4.6 | 2026-02-06 집 PC — 텔레그램 포맷 & 발송 채널 확정*
*v5 업데이트: Claude Opus 4.6 | 2026-02-07 집 PC — 모바일 UI 리디자인, 고객 친화적 말투*
*v6 업데이트: Claude Opus 4.6 | 2026-02-07 집 PC — 트래픽 라이트, ⚠️ 경고, EPS>0 필터, 코드 정리*
*v7 업데이트: Claude Opus 4.6 | 2026-02-07 집 PC — 4단계 신호등, Score 정렬, 가중평균, 친절한 말투, MD 정리*
*v8 업데이트: Claude Opus 4.6 | 2026-02-07 집 PC — Gemini 2.5 Flash AI 리스크 검증, Google Search Grounding*
*v9 업데이트: Claude Opus 4.6 | 2026-02-08 집 PC — 6단계 라이트, AI 뉴스 스캐너, 턴어라운드 제거, Score>10, Part 2 표시값*
*v10 업데이트: Claude Opus 4.6 | 2026-02-08 집 PC — AI 리스크 소거법, 프롬프트 5회 튜닝, 구분선 후처리, temperature 0.2*
*v11 업데이트: Claude Opus 4.6 | 2026-02-08 집 PC — 방향 보정(adj_score) 도입, adj_score > 9*
*v12 업데이트: Claude Opus 4.6 | 2026-02-08 집 PC — 트래픽 라이트 8패턴, Part 2 괴리율+의견 표시*
*v13 업데이트: Claude Opus 4.6 | 2026-02-08 집 PC — 트래픽 라이트 12패턴 (피크 위치 + 진동 감지, 상향 가속 과다 해결)*

---

## Phase 10: AI 뉴스 스캐너 → AI 브리핑 전환 (v14)

### 10-1. 뉴스 스캐너의 근본적 한계 발견

**문제**: v10 소거법으로 30종목 리스크 스캔 시도 → 실패 반복

**시도와 실패 기록**:
1. **구조화 티커 + 안티할루시네이션** → 30종목 중 리스크 2개만 발견, 📅 섹션에서 23개 가짜 어닝 날짜 할루시네이션
2. **섹터 기반 + 개별 Top 10** → 모든 섹터 "검색되지 않았습니다", 모든 종목 "해당 없음"
3. **뉴스 허용 범위 확대 (리스크→긍정/부정 모두)** + temperature 상향 → API 한도(20회/일) 소진

**근본 원인 발견**: Gemini + Google Search Grounding은 요청당 5-8개 검색 쿼리만 생성. 30종목 개별 검색은 구조적으로 불가능.

### 10-2. 설계 전환: "검색은 코드가, 분석은 AI가"

**핵심 인사이트**: AI에게 검색을 시키면 할루시네이션 + 불완전 결과. 코드가 팩트를 수집하고 AI는 해석만 하면 할루시네이션 구조적 불가.

**새 구조**:
| 섹션 | 데이터 소스 | AI 역할 |
|------|-----------|---------|
| 📰 시장 동향 | Google Search (1회, 광범위 쿼리) | 검색 결과 요약 |
| 📊 매수 후보 분석 | results_df (코드가 구성) | 데이터 해석/인사이트 |
| 📅 어닝 주의 | yfinance stock.calendar (코드가 조회) | 그대로 표시만 |

**제거된 것**: 🚫 개별 종목 리스크 스캔, ✅ 리스크 미발견 목록, 구분선 후처리

### 10-3. 어닝 일정: yfinance 직접 조회

**문제**: Gemini가 어닝 날짜를 라운드로빈으로 할루시네이션 (23개 가짜 날짜 생성)

**해결**: yfinance `stock.calendar.get('Earnings Date', [])` 직접 조회
```python
for ticker, _ in part2_stocks:
    stock = yf.Ticker(ticker)
    cal = stock.calendar
    earn_dates = cal.get('Earnings Date', [])
    for ed in earn_dates:
        if today_date <= ed <= two_weeks_date:
            earnings_tickers.append(f"{ticker} {ed.month}/{ed.day}")
```
- 결과: NEM 2/20, RGLD 2/19 정확히 조회됨
- 프롬프트에 `[어닝 일정 — 시스템 확인 완료]`로 전달, AI는 수정/추가 금지

### 10-4. 매수 후보 데이터: results_df 직접 구성

**문제**: msg_part2=None일 때 Gemini가 "데이터가 없다"고 응답

**해결**: msg_part2 텍스트 파싱 대신 results_df에서 직접 데이터 구성
```python
data_lines.append(
    f"{idx+1}. {t} ({ind}) {lights} {desc} · "
    f"점수 {asc:.1f} · EPS {eps_c:+.1f}% · 주가 {price_c:+.1f}% · "
    f"괴리 {pe_c:+.1f} · 의견 ↑{rup} ↓{rdn}{warn}"
)
```
- 함수 시그니처 변경: `run_ai_analysis(..., results_df=None)`

### 10-5. 청크 분할 버그 수정

**문제**: 텔레그램에 빈 메시지 48개 전송됨

**원인**: `split_point = remaining[:4000].rfind('\n')` → 첫 4000자에 개행 없으면 `split_point==-1` → 무한루프 + 빈 청크 생성

**수정**:
```python
if split_point <= 0:
    split_point = 4000
remaining = remaining[split_point:].strip()
chunks = [c for c in chunks if c.strip()]  # 빈 청크 제거
```

### 10-6. temperature 및 기타

- temperature: 0.2 → 0.3 (0.2는 빈 응답 발생, 0.3이 안정적)
- 빈 응답 시 1회 재시도 로직 추가
- 헤더: "AI 리스크 체크" → "AI 브리핑"
- 설명: "리스크를 소거법으로 스캔" → "매수 후보 데이터를 AI가 분석한 브리핑"

### 결정 사항 추가

42. **AI 브리핑 전환 (v14)**: 소거법→데이터 분석, "검색은 코드가, 분석은 AI가"
43. **어닝 yfinance 직접 조회 (v14)**: AI 할루시네이션 방지, stock.calendar 사용
44. **results_df 직접 전달 (v14)**: msg_part2 텍스트 파싱 대신 DataFrame에서 구조화 데이터 구성
45. **청크 분할 수정 (v14)**: split_point <= 0 방어, 빈 청크 필터링
46. **temperature 0.3 (v14)**: 0.2는 빈 응답 위험, 0.3이 데이터 분석에 적합

*v14 업데이트: Claude Opus 4.6 | 2026-02-08 집 PC — AI 뉴스 스캐너→AI 브리핑 (검색은 코드가, 분석은 AI가)*

---

## Phase 11: 6단계 신호등 → 5단계 날씨 아이콘 (v15)

### 11-1. 신호등의 UX 문제

**문제**: 모바일에서 🟢🔵🟡가 구분이 안 됨.
- 🟢(2~20%)과 🔵(0.5~2%): 작은 화면에서 초록/파랑 구분 어려움
- 🟡(0~0.5%): 신호등 맥락에서 "주의"로 오해되나 실제는 "보합"
- 🟩🟥(네모) vs 🟢🔴(동그라미) 형태 차이가 모바일에서 미묘
- 고객이 "읽는 법"을 매번 다시 봐야 하는 인지 부하

### 11-2. 날씨 아이콘 선정 근거

**핵심 장점**: 각 아이콘의 **형태(실루엣) 자체가 다르다**
- 신호등: 색상만 다르고 형태 동일 (동그라미)
- 날씨: ☀️🌤️☁️🌧️⛈️ — 5개 모두 완전히 다른 모양
- 설명 없이도 통함: 맑음=좋다, 폭풍=나쁘다

**6→5단계 축소 근거**: 🔵 양호(0.5~2%)가 가장 약한 고리
- 고객 관점에서 0.3%와 1.5%의 차이는 "둘 다 약간 오르는 중"
- 12개 패턴 설명이 이미 디테일한 해석을 제공 → 아이콘은 빠른 시각적 인상만

### 11-3. 임계값 결정: 데이터 기반

Top 30 세그먼트 120개 분포 분석으로 4가지 임계값 방안 비교:
```
제안A ±1%,±10%: ☀️30% 🌤️57% ☁️12% 🌧️1% ⛈️0% → 🌤️에 쏠림
제안B ±2%,±10%: ☀️30% 🌤️39% ☁️30% 🌧️1% ⛈️0% → 균형 최적 ✅
제안C ±3%,±15%: ☀️17% 🌤️46% ☁️37% 🌧️1% ⛈️0% → ☁️에 쏠림
제안D 0,±5%,±20%: ☀️12% 🌤️38% ☁️48% 🌧️3% ⛈️0% → ☁️에 쏠림
```

**제안B (±2%, ±10%) 채택**:
- ☀️(30%)과 ☁️(30%)가 대칭적 → 패턴 구분 선명
- 🌤️(39%)가 중심축 → "정상적으로 오르는 중"
- 현재 6단계 🟢가 57.5%로 과반 → B안은 최대 카테고리 39%로 분산

### 11-4. 최종 매핑

| 아이콘 | 범위 | 의미 | Top 30 비율 |
|--------|------|------|------------|
| ☀️ | >10% | 맑음 — 강한 상승 | 30% |
| 🌤️ | 2~10% | 구름조금 — 상승 | 39% |
| ☁️ | -2~2% | 흐림 — 보합 | 30% |
| 🌧️ | -10~-2% | 비 — 하락 | 1% |
| ⛈️ | <-10% | 폭풍 — 급락 | 0% |

예시: SNDK `🌤️🌤️☀️🌤️ 중반 급등` — 중반에 폭발, 전후 안정 상승

### 11-5. 패턴 설명 체계 유지

12개 기본 패턴 + 강도 수식어 구조는 동일하게 유지.
- `has_green_sq` → `☀️` 포함 여부로 변경
- `has_red_sq` → `⛈️` 포함 여부로 변경
- 수식어 로직(폭발적~/급~/중반 급등 등) 변경 없음

### 11-6. 포트폴리오 비중 단순화

**문제**: 기존 비중 배분이 min 10%, max 30%, 5% 단위, 잔여분 하위 종목 배분 → 불자연스러운 결과

**변경**: adj_score 단순 비례 (5% 단위 반올림, 합계 100% 보정)
```python
for i, s in enumerate(selected):
    raw = scores[i] / total_score * 100
    s['weight'] = round(raw / 5) * 5
diff = 100 - sum(s['weight'] for s in selected)
if diff != 0:
    selected[0]['weight'] += diff
```

### 결정 사항 추가

47. **6단계 신호등→5단계 날씨 (v15)**: ☀️(>10%) 🌤️(2~10%) ☁️(-2~2%) 🌧️(-10~-2%) ⛈️(<-10%)
48. **임계값 ±2%, ±10% (v15)**: Top 30 데이터 분포 분석 기반, B안 채택 (☀️30%/🌤️39%/☁️30%)
49. **포트폴리오 비중 단순화 (v15)**: 상한/하한 제거, adj_score 단순 비례

*v15 업데이트: Claude Opus 4.6 | 2026-02-09 집 PC — 날씨 아이콘 전환 (데이터 기반 임계값), 포트폴리오 비중 단순화*

---

## Phase 11: AI 브리핑 정량 리스크 스캐너 전환 (v15)

### 11-1. v14 AI 브리핑의 한계

**문제**: v14는 Part 2 데이터를 요약해주는 구조 → 고객이 이미 본 내용의 반복. 가독성 떨어지고 실질 가치 없음.
**고객 니즈**: Part 2 매수 후보의 숨은 리스크를 체크하여 매매 판단 보조.

### 11-2. 뉴스 기반 접근 시도 → 실패

yfinance `stock.news` → `content.title/summary/pubDate`로 종목별 뉴스 수집 시도:
- yfinance 뉴스는 큐레이션 피드로 종목별 리스크 신호가 약함 (저신호 고노이즈)
- Google Search Grounding 시장 동향도 부정확 (나스닥 상승을 하락이라 보고)
- 16/30 종목 위험 판정 → 과도한 false positive

### 11-3. 정량 리스크 스캐너 전환 (최종)

**핵심 아이디어** (사용자): "PART2에 나온 위험 신호들을 정리해서 AI한테 전달하면 AI가 그거 기반으로 사면 위험한 종목과 이유 말해주는거 어때?"

**6가지 정량 위험 플래그** (코드가 results_df에서 계산):

| 플래그 | 조건 | 이모지 |
|--------|------|--------|
| 의견 하향 (강) | `rev_down >= 3` | 🔻 |
| 의견 하향 (약) | `rev_down >= 1 and rev_down >= rev_up` | 📉 |
| 극단적 괴리 | EPS 가중↑ & 주가 가중↓, 비율 5배 초과 | ⚠️ |
| 주가 급락 | `price_chg < -20%` (90일) | 📉 |
| 모멘텀 감속 | `direction < -10` | ↘️ |
| 고평가 | `fwd_pe > 50` | 💰 |
| 어닝 임박 | yfinance `stock.calendar` 2주 이내 | 📅 |

**Gemini 역할**: 위험 플래그를 해석하여 고객에게 친근한 말투로 설명. 팩트에 없는 내용 생성 금지.

### 11-4. 출력 구조

```
━━━━━━━━━━━━━━━━━━━
      🤖 AI 브리핑
━━━━━━━━━━━━━━━━━━━

📰 시장 동향 (Google Search 2~3줄)

⚠️ 매수 주의 종목
종목명(티커)
리스크 설명 1~2줄 (~예요/~해요 체)
──────────────────
다음 종목...

📅 어닝 주의
종목명 날짜 · ...

✅ 위험 신호 없음
종목명(티커) · 종목명(티커) · ...
```

### 11-5. UI/말투 개선

- **[SEP] 마커 → 분리선**: Gemini에게 `[SEP]` 출력 지시 → 코드에서 `──────────────────`로 변환
- **여백 축소**: `re.sub(r'\n*\[SEP\]\n*', '\n──────────────────\n', ...)` — 빈 줄 제거
- **말투**: 프롬프트에 구체적 예시 제공 ("주가가 크게 빠졌어요", "조심하시는 게 좋겠어요")
- **종목명 볼드**: `**종목명(티커)**` → `<b>종목명(티커)</b>`

### 11-6. Gemini response.text None 대응

Google Search Grounding 사용 시 `response.text`가 None 반환하는 케이스 발견.
`extract_text()` 헬퍼로 `candidates[0].content.parts`에서 직접 추출하도록 수정.

### 11-7. 테스트 인프라

- `send_ai_briefing.py`: AI 브리핑만 테스트 발송 (개인봇)
- `.github/workflows/ai-briefing-only.yml`: 수동 트리거 워크플로우
- **교훈**: 포맷 변경은 기존 출력을 재가공하여 테스트. 매번 916종목 재수집 불필요.

### 결정 사항 추가

47. **AI 정량 리스크 스캐너 (v15)**: 뉴스→정량 신호 기반, 할루시네이션 구조적 차단
48. **[SEP] 종목 분리선 (v15)**: AI 출력에 마커, 코드에서 분리선 변환 (여백 최소화)
49. **친근한 말투 프롬프트 (v15)**: 구체적 예시 기반 (~예요/~해요), 추상적 지시 불충분
50. **extract_text 헬퍼 (v15)**: Gemini response.text None 대응, parts 직접 추출
51. **temperature 0.2 유지 (v15)**: 정량 데이터 해석이므로 낮은 temperature 적합
52. **✅ 섹션 티커만 5개씩 줄바꿈 (v15)**: 종목명(티커) 한줄 나열 → 티커만 5개씩 줄바꿈으로 가독성 개선

*v15 업데이트: Claude Opus 4.6 | 2026-02-09 집 PC — AI 정량 리스크 스캐너, UI/말투 개선, extract_text 안정화*

## Phase 12: 포트폴리오 추천 기능 (v16)

### 12-1. 목적

Part 1→Part 2→AI 브리핑 흐름에 최종 액션 아이템 추가.
916종목 → 30종목(Part 2) → 리스크 제거 → 추세 반영 → **최종 5종목 포트폴리오**.

### 12-2. 선정 로직

1. Part 2 필터 (adj_score > 9, fwd_pe_chg, eps_change_90d > 0) → 30종목
2. 리스크 플래그 6가지로 위험 종목 제거 → ✅ 종목만
3. **괴리율 × 추세 3단계 가중치** = `weighted_gap`
   - 좋음 (상향 가속, 꾸준한 상승, 폭발적 등): **x1.2**
   - 보통 (중반 강세, 반등, 등락 등): **x1.0**
   - 나쁨 (둔화, 전환, 하락, 횡보): **x0.8**
4. weighted_gap 내림차순 상위 5개 선정
5. 비중 배분: 괴리율 비례, 5% 단위, 최소 10%

### 12-3. 추세 가중치 설계 경위

- 처음: 복합점수 (괴리율 40% + 모멘텀 35% + 컨센서스 25%) → 과적합 위험
- "모멘텀 기반으로 싼 거 찾는 건데 왜 모멘텀을 또 더해?" → 모멘텀 제거
- 순수 괴리율 → "Part 2랑 똑같잖아" → 추세 가중치 도입
- 20개 세분화 가중치 → "임의로 정한 거라 불안" → 3단계(±20%) 대칭으로 단순화
- 3단계 vs 세분화 비교: TOP 5 종목 동일, 3~4위 순서만 다름 → 3단계 채택

### 12-4. 메시지 구조

```
━━━━━━━━━━━━━━━━━━━
      💼 추천 포트폴리오
━━━━━━━━━━━━━━━━━━━
📅 2026년 02월 09일

Part 2 매수 후보 중 위험 신호 종목을 제거하고,
EPS 추세 품질을 반영하여 선정했어요.

(Gemini 생성: 종목별 비중 + 선정 이유, [SEP] 구분)
```

### 12-5. 전체 메시지 흐름

Part 1 (모멘텀 순위) → Part 2 (괴리율 순) → AI 브리핑 (리스크) → **포트폴리오 (최종 추천)** → 시스템 로그

### 12-6. Part 2 UI 압축

종목당 4줄→3줄: 의견(↑N ↓N)을 변화율 줄에 합침.

### 결정 사항 추가

53. **포트폴리오 추천 (v16)**: Part 2 ✅ 종목 → 괴리율×추세가중 → 상위 5개
54. **추세 3단계 가중치 (v16)**: ±20% 대칭, 과적합 최소화 (1.2/1.0/0.8)
55. **Part 2 UI 압축 (v16)**: 종목당 4줄→3줄, 의견을 변화율 줄에 합침

56. **테스트 인프라 정리 (v16)**: ai-briefing-only.yml, send_ai_briefing.py 삭제 — 실전 전환
57. **전체 메시지 흐름 확정 (v16)**: Part1 → Part2 → AI브리핑 → 포트폴리오 → 시스템로그 (채널에는 로그 제외 4개)

*v16 업데이트: Claude Opus 4.6 | 2026-02-09 집 PC — 포트폴리오 추천 통합, 추세 3단계 가중치, Part 2 압축, 테스트 인프라 정리*

---

## Phase 13: adj_score 기반 전환 & 메시지 UX 개선 (v17)

### 13-1. 포트폴리오 랭킹: 괴리율×추세가중치 → adj_score

**문제**: v16의 `abs(fwd_pe_chg) × trend_weight(±20%)` 공식이 괴리율 극단값에 치우침.
- MMS(gap=-21.7, 상향 둔화, adj=12.4)가 LUV(gap=-8.9, 중반 급등, adj=69.2)보다 높은 순위
- ±20% 추세 가중치가 괴리율 범위(-2~-34) 대비 너무 약해서 사실상 무의미

**사용자 인사이트**: "EPS 모멘텀을 보려는 건데 왜 속도로만 보는 거야? 속도랑 방향 둘 다 만족시키는 걸 순위로 해야지"

**해결**: `adj_score`가 이미 속도(score) × 방향(direction) 반영
```
adj_score = score × (1 + clamp(direction/30, -0.3, +0.3))
```
- 새 공식이 필요 없음 — 기존 adj_score 그대로 사용
- 자의적 3단계 추세 가중치(GOOD/NORMAL/BAD) 완전 제거

**결과 비교** (2026-02-09 데이터):
| 순위 | 이전 (괴리율×추세) | 변경 후 (adj_score) |
|------|-------------------|-------------------|
| 1 | SNDK (40%) | SNDK (30%) |
| 2 | MMS (20%) | LUV (20%) |
| 3 | CPRI (15%) | AA (20%) |
| 4 | AA (15%) | FCX (15%) |
| 5 | APH (10%) | FIVE (15%) |

### 13-2. Part 2 정렬: 괴리율 → adj_score

Part 2, AI 분석, 포트폴리오 모두 `fwd_pe_chg` → `adj_score` 내림차순으로 통일.
- Part 2 설명 변경: "EPS 전망치는 좋아졌는데 주가가 아직 못 따라간 종목" → "EPS 모멘텀이 가장 강한 매수 후보"
- 모멘텀 점수 볼드 표시: `괴리 -33.9` → `<b>모멘텀 165.0</b>`
- 읽는법: `괴리 = EPS 대비 주가 미반영도` → `<b>모멘텀</b> = EPS 변화 속도+방향`

### 13-3. 포트폴리오 비중 cap 30%

**문제**: adj_score 비례 배분 시 SNDK(165.0)이 50%로 과집중.
**해결**: 단일 종목 최대 30%, 최소 10%, 5% 단위. 잔여분은 하위 종목부터 배분 (분산 효과).

### 13-4. 메시지 단계별 흐름 [1/4]~[4/4]

4개 메시지가 순차적 단계를 거쳐 최종 포트폴리오에 도달하는 흐름 연출:

```
[1/4] 📈 오늘(MM월DD일) EPS 모멘텀 리포트
  916종목 → Top 30
  👉 다음: 매수 후보 선정 [2/4]

[2/4] 💰 매수 후보 선정
  EPS 모멘텀이 가장 강한 매수 후보
  👉 다음: AI가 위험 신호를 점검해요 [3/4]

[3/4] 🤖 AI 위험 신호 점검
  매수 후보의 위험 신호를 AI가 점검
  👉 다음: 최종 포트폴리오 [4/4]

[4/4] 💼 최종 포트폴리오
  위험 신호 제거 → 최종 5종목
```

### 13-5. UI 세부 개선

- **Part 1 설명 압축**: 5줄 → 2줄 ("미국 916종목 중 애널리스트 EPS 전망치를 가장 많이 올린 기업 순위예요.")
- **Part 2 신호등 읽는법 중복 제거**: Part 1에만 신호등 설명, Part 2에서 제거
- **Part 2 의견 라인 분리**: 변화율·의견 합침(3줄) → 분리(4줄)
- **의견 → 애널리스트 의견**: Part 2, AI 브리핑, 포트폴리오 전체
- **포트폴리오 Gemini 프롬프트**: `**종목명(티커) · 비중 N%**` + 설명 다음줄로 분리
- **AI 브리핑 설명**: "매수 후보의 위험 신호를 AI가 점검했어요." 한 줄로 정리

### 13-6. 디버그 로그

포트폴리오 함수에 상세 로그 추가:
- 각 종목별 ✅/❌ 리스크 플래그 판정 결과
- safe 종목 adj_score 순위 전체 출력
- 선정 5종목 비중 표시

### 13-7. 임시 테스트 워크플로우

`.github/workflows/test-private-only.yml`: 개인봇 전용 전체 테스트.
- TELEGRAM_CHAT_ID 미설정 → 채널 전송 안 함
- 워크플로우 이름에 한글 사용 시 GitHub 인식 안 됨 → ASCII로 변경 (`Test Private Bot Only`)

### 결정 사항 추가

58. **포트폴리오 adj_score 랭킹 (v17)**: 괴리율×추세가중 → adj_score(속도×방향), 자의적 가중치 제거
59. **Part 2 adj_score순 정렬 (v17)**: 괴리율순 → adj_score순, AI분석/포트폴리오도 동일
60. **모멘텀 점수 표시 (v17)**: Part 2에서 괴리 → 모멘텀 점수 볼드 표시
61. **비중 cap 30% (v17)**: 단일 종목 최대 30%, 잔여분 하위부터 배분
62. **단계별 흐름 (v17)**: [1/4]~[4/4] 번호 + 다음 단계 예고 (👉)
63. **Part 1 설명 압축 (v17)**: 5줄 → 2줄
64. **신호등 읽는법 Part 1 전용 (v17)**: Part 2에서 중복 제거
65. **의견 → 애널리스트 의견 (v17)**: 전체 메시지 통일

*v17 업데이트: Claude Opus 4.6 | 2026-02-09 집 PC — adj_score 기반 전환, 단계별 흐름 [1/4]~[4/4], 비중 cap, UI 개선*

---

## 미결 사항: 신호등 체계 개편 (v17 이후)

### 배경

현행 6단계(🟩🟢🔵🟡🔴🟥)는 네모=강한 변동이라는 규칙이 직관적이지 않음.
처음 보는 사용자가 별도 설명 없이 바로 이해할 수 있는 체계 필요.

### 후보 A: 4단계 (🔥🟢🟡🔴)

| 신호 | 범위 | 의미 |
|------|------|------|
| 🔥 | >20% | 폭발적 상승 — 불꽃 = 강한 것의 보편적 상징 |
| 🟢 | 2~20% | 상승 |
| 🟡 | ±2% | 보합 |
| 🔴 | <-2% | 하락 |

**장점**: 극도로 단순, 신호등 3색 + 불꽃 1개로 직관적
**단점**: 🔵(0.5~2% "양호") 구간이 🟡에 흡수 → 약한 상승과 보합 구분 불가

### 후보 B: 5단계 (🔥🟢🔵🟡🔴)

🔵을 살려서 약한 상승(0.5~2%)을 보합(±0.5%)과 구분.

**장점**: 정보 손실 최소
**단점**: 5색은 여전히 설명이 필요할 수 있음

### 데이터 검증 (2026-02-09)

**전체 916종목 (3,640 구간)**: 🟡(±2%) = 56.1% → 대다수가 보합
**Part 2 Top 30 (120 구간)**: 🟡(±2%) = 9.2%, 🔥+🟢 = 87.5% → 상위 종목에서는 노란색 걱정 없음

사용자가 보는 메시지(Part 1/Part 2)는 상위 30종목이므로 🔥🟢가 대부분이고 🟡는 소수.

### 패턴 설명/수식어 영향

- 12개 기본 패턴: 원본 segment 값(>0.5%, <-0.5%)으로 판단 → **색상 무관, 변경 없음**
- 강도 수식어(폭발적~/급~): 원본 값(>20%, <-10%)으로 판단 → **색상 무관, 변경 없음**
- 코드 수정: `get_trend_lights()` 색상 매핑만 변경, `has_green_sq` → `has_fire`, `has_red_sq` → `any(s < -10)`

### 결정

**해결 (v15)** — 5단계 날씨 아이콘(☀️🌤️☁️🌧️⛈️) 적용, 임계값 ±2%/±10% (데이터 분석 기반).

---

## Phase 14: adj_gap 도입 — Part 2 괴리율 복원 + 방향 보정 (v18)

### 14-1. 문제: Part 1과 Part 2가 동일 기준 (adj_score)

**v17에서의 변경**: Part 2 정렬을 괴리율(fwd_pe_chg) → adj_score로 변경.
**문제점**: Part 1과 Part 2가 모두 adj_score로 정렬 → 두 리스트가 거의 동일한 종목 노출.
- Part 1: "EPS 모멘텀이 강한 종목" (adj_score 순)
- Part 2: "EPS 모멘텀이 강한 매수 후보" (adj_score 순) ← Part 1과 차별화 없음

**사용자 인사이트**: "adj_score 대비 가격 등락률에 대한 괴리율을 봐야 하는데..."
→ Part 2의 원래 목적은 "EPS 개선 대비 주가 미반영 종목"을 찾는 것.

### 14-2. 해결: adj_gap = fwd_pe_chg × (1 + clamp(direction/30, -0.3, +0.3))

**설계 원칙**: adj_score에서 쓰는 방향 보정을 괴리율에도 동일하게 적용.

```
fwd_pe_chg = 가중평균 Fwd PE 변화율 (7d×40%+30d×30%+60d×20%+90d×10%)
dir_factor = clamp(direction / 30, -0.3, +0.3)
adj_gap = fwd_pe_chg × (1 + dir_factor)
```

**해석**:
- fwd_pe_chg < 0: EPS 개선 > 주가 반응 → 저평가
- direction > 0 (가속): 저평가 강화 (더 음수) → 가속 중인데 저평가면 더 좋은 기회
- direction < 0 (감속): 저평가 약화 (덜 음수) → 감속 중이면 저평가가 정당화될 수 있음

**예시** (2026-02-09):
| 종목 | fwd_pe_chg | direction | adj_gap | 의미 |
|------|-----------|-----------|---------|------|
| PLTR | -39.5 | +8.0 | -52.8 | 급상향인데 주가 폭락 → 강한 저평가 |
| SNDK | -33.9 | +9.5 | -44.1 | 급상향인데 주가 미반영 |
| MMS | -19.0 | -2.6 | -20.6 | 감속 → 방향 보정 약함 |

### 14-3. 이중 랭킹 시스템 최종 구조

```
Part 1: adj_score 내림차순 (EPS 모멘텀 크기 + 방향)
  → "이 종목들의 EPS 전망이 가장 빠르게 좋아지고 있어요"

Part 2: adj_score > 9 필터 → adj_gap 오름차순 (더 음수 = 더 저평가)
  → "EPS 개선이 주가에 덜 반영된 종목이에요"
```

**차별화**: Part 1은 속도, Part 2는 괴리 (+ 방향 보정)

### 14-4. 포트폴리오 비중도 adj_gap 기반

- 리스크 필터 통과 종목(✅) 중 adj_gap 순 Top 5 선정
- 비중: abs(adj_gap) 비례, 5% 단위 반올림
- 더 저평가된 종목에 더 높은 비중 배분

### 14-5. 결과 비교 (2026-02-09)

| 순위 | v17 (adj_score) | v18 (adj_gap) |
|------|----------------|---------------|
| 1 | SNDK (45%) | SNDK (45%) |
| 2 | LITE (20%) | MMS (20%) |
| 3 | LUV (15%) | CPRI (15%) |
| 4 | WDC (10%) | APH (10%) |
| 5 | STX (10%) | LUV (10%) |

adj_score 기준에서는 EPS 모멘텀만 강한 반도체 집중 → adj_gap에서는 저평가 관점으로 업종 분산.

### 14-6. 리스크 필터 정비 — 시스템 철학과의 모순 제거

**문제**: 기존 리스크 필터가 adj_gap 시스템과 모순.

| 필터 | 문제 | 조치 |
|------|------|------|
| 급락 (price < -20%) | 주가 하락이 adj_gap을 만드는 원인. 기회를 위험으로 오분류 | **제거** |
| 괴리 (\|price/eps\| > 5) | adj_gap이 측정하는 것과 동일. 1등을 제거하는 구조 | **제거** |
| 감속 (direction < -10) | adj_score가 이미 direction 최대 30% 감점 반영. 이중 처벌 | **제거** |
| 하향 (rev_down ≥ 1, ≥ up) | 하향 1건으로 시스템 전체 신호 무시. 과도 | **제거** |
| 하향 (rev_down ≥ 3) | 다수 애널리스트 동시 하향 → EPS 전망 자체 신뢰 하락 | **유지** |
| 고평가 (PE > 50) | 50은 성장주에서 흔함. 100으로 상향 | **100으로 변경** |
| 어닝 (2주 이내) | 실적발표 전후 변동성 리스크 | **유지** |

**추가**: 저커버리지 (num_analysts < 3)
- `earningsEstimate.numberOfAnalysts` (0y) 값 수집
- 소수 애널리스트의 추정치 변경이 NTM EPS를 과도하게 흔들 수 있음
- 예시: MMS(애널리스트 2명), TLN(애널리스트 2명) → 포트폴리오 제외

**최종 리스크 필터** (AI 브리핑 & 포트폴리오 공통):
1. 하향 (rev_down ≥ 3)
2. 저커버리지 (num_analysts < 3)
3. 고평가 (fwd_pe > 100)
4. 어닝 (2주 이내 실적발표)

**원칙**: adj_gap이 "저평가 기회"를 찾는 시스템이므로, 리스크 필터는 "데이터 자체의 신뢰성"만 검증.

### 결정 사항 추가

66. **adj_gap 도입 (v18)**: fwd_pe_chg × (1 + direction 보정), Part 2 정렬 기준 복원
67. **Part 2 이중 필터 (v18)**: adj_score > 9 (입장), adj_gap 오름차순 (정렬)
68. **포트폴리오 adj_gap 기반 (v18)**: 비중 = abs(adj_gap) 비례, 더 저평가 = 더 높은 비중
69. **Part 2 표시 "괴리" (v18)**: `모멘텀 X` → `괴리 +X.X`, 읽는법 "EPS↑ vs 주가 반영도"
70. **리스크 필터 정비 (v18)**: 모순 4개 제거 + 저커버리지 추가, AI 브리핑 동기화
71. **리스크 철학 (v18)**: adj_gap=기회 찾기, 필터=데이터 신뢰성 검증 (기회를 위험으로 오분류하지 않음)

*v18 업데이트: Claude Opus 4.6 | 2026-02-09 집 PC — adj_gap 도입, 리스크 필터 정비(모순 제거+저커버리지), AI 브리핑 동기화*

---

## Phase 15: v19 Safety & Trend Fusion (2026-02-10)

### 15-1. 핵심 철학 변경

**문제**: v18은 당일 데이터만으로 매수 후보를 선정하여 하루짜리 노이즈에 취약하고, 기술적 안전장치(MA60)가 없었음.

**해결**: 3일 연속 검증 + MA60 필터 + adj_gap ≤ 0 필터를 추가하여 신뢰도를 높이고, 메시지를 4개→3개로 축소.

### 15-2. 메시지 구조 개편 (4개 → 3개)

| 항목 | v18 (이전) | v19 (변경) |
|------|-----------|-----------|
| 메시지 수 | 4개 ([1/4]~[4/4]) | 3개 ([1/3]~[3/3]) |
| Part 1 | EPS 모멘텀 Top 30 | **제거** (adj_score 랭킹은 참고용이었음) |
| Part 2 | [2/4] 매수 후보 | [1/3] 매수 후보 (핵심) |
| AI 브리핑 | [3/4] | [2/3] |
| 포트폴리오 | [4/4] | [3/3] |

**Part 1 제거 근거**: adj_gap(괴리율)이 실제 매수 신호이고 adj_score는 참고용이었음. 당일 순위를 보여줘야 한다면 adj_gap 기반 Part 2가 맞음.

### 15-3. 새 필터 3개 추가

| 필터 | 조건 | 근거 |
|------|------|------|
| MA60 | price > 60일 이동평균 | 하락 추세 종목 제외 (기술적 안전장치) |
| adj_gap ≤ 0 | adj_gap > 0이면 제외 | 주가가 EPS를 이미 초과 반영 → 기회 아님 |
| $10 | price ≥ $10 | 페니스톡 제외 (S&P/NASDAQ 유니버스에서는 거의 해당 없음) |

**기존 필터 유지**: adj_score > 9, eps_change_90d > 0, fwd_pe > 0

### 15-4. 3일 연속 검증 시스템

- Part 2 eligible 종목(필터 통과 전체, Top 30 제한 없이)에 `part2_rank` 부여 → DB 저장
- 최근 3개 DB date에서 모두 part2_rank가 있는 종목 = ✅ (검증)
- 오늘만 있는 종목 = 🆕 (신규 진입, 관찰)
- DB 3일 미만 (시스템 초기) → 전부 ✅ 처리 (cold start)

### 15-5. Death List (탈락 알림)

- 어제 Part 2에 있었지만 오늘 빠진 종목 자동 감지
- 사유 자동 판별: MA60↓, 괴리+, 점수↓, EPS↓, 순위밖, 데이터없음
- Part 2 메시지 하단에 `🚨 탈락 종목` 섹션으로 통합 (별도 메시지 아님)
- 탈락 종목 없으면 섹션 생략

### 15-6. DB 스키마 확장

기존 10개 컬럼에 5개 추가:

```sql
ALTER TABLE ntm_screening ADD COLUMN adj_score REAL;
ALTER TABLE ntm_screening ADD COLUMN adj_gap REAL;
ALTER TABLE ntm_screening ADD COLUMN price REAL;
ALTER TABLE ntm_screening ADD COLUMN ma60 REAL;
ALTER TABLE ntm_screening ADD COLUMN part2_rank INTEGER;  -- NULL = Part 2 미해당
```

- 기존 DB 자동 마이그레이션 (ALTER TABLE IF NOT EXISTS 패턴)
- part2_rank로 3일 교집합 쿼리 가능

### 15-7. 포트폴리오 변경

- **소스**: Part 2 전체 → ✅ (3일 검증) 종목만
- **리스크 필터**: 기존 유지 (하향/저커버리지/고평가/어닝)
- ✅ 종목 부족 시: 있는 만큼만 추천 (무리하게 채우지 않음)
- ✅ 종목 0개: "관망 권장" 메시지 발송

### 15-8. Cron 변경

- UTC 22:00 → UTC 22:15 (KST 07:00 → 07:15)
- 미국 장 마감 후 데이터 정착 여유 확보

### 15-9. 공통 필터 함수 추출

`get_part2_candidates(df, top_n)` — Part 2 필터가 3곳에 중복되던 것을 통합.
`save_part2_ranks()`, `get_3day_status()`, `get_death_list()` 신규 함수.

### 결정 사항 추가

72. **Part 1 제거 (v19)**: adj_score 랭킹은 참고용이었음, adj_gap 기반 Part 2가 실제 신호
73. **MA60 필터 (v19)**: 60일 이동평균 위 종목만 Part 2 진입, 하락 추세 제외
74. **adj_gap ≤ 0 필터 (v19)**: 주가가 EPS를 초과 반영한 종목 제외 (기회 아님)
75. **$10 필터 (v19)**: 페니스톡 제외 (실질적 영향 미미)
76. **3일 교집합 (v19)**: 3일 연속 Part 2 eligible = ✅ 검증, 포트폴리오는 ✅만 대상
77. **Death List 통합 (v19)**: 별도 메시지가 아닌 Part 2 하단 섹션으로 통합
78. **메시지 축소 (v19)**: 4개→3개, Part 1 제거
79. **Cron 변경 (v19)**: UTC 22:00→22:15
80. **지수 경고 (미결)**: SPY/QQQ MA 하향 시 진입 주의 권장 → 테스트 후 추가 예정

### 15-10. 영업일/휴장 처리

- 3일 교집합은 **DB에 있는 distinct date** 기준 → 주말/공휴일 자동 대응
- 예: 금요일(2/7)→월요일(2/10) 실행 시 DB에 2/6, 2/7, 2/10 3개 날짜 → 정상 동작
- 미국 공휴일(Presidents' Day 등)은 GitHub Actions가 실행되더라도 yfinance 데이터가 없어 자연스럽게 skip됨
- 별도 휴장 캘린더 관리 불필요

### 15-11. 데이터 소급(Backfill) 교훈

- **DB만으로 계산 가능**: adj_score (= score × 방향보정, seg1~4에서 도출)
- **yfinance 필요**: price, ma60 (가격 다운로드), adj_gap (fwd_pe 변화율 + 방향보정)
- **실행 시 실시간 조회**: rev_up30/rev_down30 (DB 미저장, epsRevisions API)
- 결론: 기존 DB 데이터만으로 v19 컬럼 100% 소급은 불가능, 일부만 가능

### 15-12. 초기 테스트 결과 (2026-02-10 로컬)

- 총 864종목 수집 (916 중 52 에러)
- Part 2 후보: 20개
- 3일 검증: ✅ 15개 / 🆕 5개
- Death List: 🚨 4개 (ASML 포함)
- 소요시간: ~469초
- 텔레그램: 로컬 config `telegram_enabled: false` → 미발송 → GitHub Actions 워크플로우로 테스트

### 15-13. Cold Start 자동 채널 전송 제어

- **문제**: 백필 데이터가 3일 검증/Death List 결과를 왜곡 → 백필 삭제
- **해결**: `is_cold_start()` 함수 — DB에 part2_rank 있는 날짜가 3일 미만이면 True
- **동작**: cold_start=True → 채널 전송 비활성화, 개인봇만 전송
- **자동 전환**: 3일 데이터 축적되면 자동으로 채널 전송 시작 (날짜 하드코딩 불필요)
- **워크플로우**: daily-screening.yml 하나로 통합 운영 (date check 제거)
- 결정 사항 추가:
  81. **백필 삭제 (v19)**: 백필 데이터가 ✅/🆕/🚨 판정을 왜곡하므로 제거
  82. **Cold Start 자동 제어 (v19)**: is_cold_start()로 DB 상태 기반 채널 전송 자동 전환

### 미결 사항

- **지수 경고 기능**: SPY/QQQ의 5일선/MA 상태에 따라 진입 주의 권장 메시지 추가
  - 사용자 요청: "먼저 테스트 돌리고 나서 추가하자"
  - 구현 시점: v19 텔레그램 테스트 완료 후

### 15-14. 메시지 UX 대개편 — 고객 친화적 리뉴얼

- **📖 투자 가이드**: 신규 메시지. 시스템 개요, 선정 과정, 보유/매도 기준 설명. 매일 첫 발송.
- **아이콘 체계 변경**: 💰→🔍(탐색), 🤖→🛡️(점검), 💼→🎯(실행). 스토리: 이해→탐색→점검→실행
- **[1/3] 🔍 매수 후보**: 도입부/읽는 법 고객친화 개선, 날씨 범례 추가, Death List→📉+매도 안내
- **[2/3] 🛡️ AI 점검**: "AI 브리핑"→"AI 점검", 안전 점검 느낌
- **[3/3] 🎯 최종 추천**: "추천 포트폴리오"→"최종 추천", 활용법(보유기간/매도/분산) 하단 추가
- **관망 메시지**: "무리한 진입보다 기다림이 나을 때도 있어요" 추가
- 결정 사항 추가:
  83. **투자 가이드 메시지 (v19)**: 매일 첫 발송, 처음 보는 고객도 이해 가능
  84. **아이콘 리뉴얼 (v19)**: 🔍🛡️🎯 스토리 라인, 날씨 아이콘 유지
  85. **활용법 가이드 (v19)**: 보유 2~4주, 탈락=매도검토, 비중대로 분산

### 15-15. 🆕 전용 포트폴리오 버그 수정

- **문제**: 3일 검증에서 ✅ 종목이 0개일 때 (전부 🆕), `verified_tickers`가 빈 set → `if verified_tickers:` 조건 실패 → 필터 미적용 → 🆕 종목 전체가 포트폴리오 추천됨
- **원인**: cold start에서는 `get_3day_status()`가 전부 ✅ 반환하므로 `verified_tickers`가 비지 않음. 빈 경우는 cold start가 아닌 **DB 3일 이상이지만 전부 🆕인 과도기** 상황
- **수정**: `if verified_tickers:` → `if status_map:` — status_map이 존재하면 항상 ✅ 필터 적용
- **결과**: 전부 🆕일 때 filtered.empty → 기존 관망 메시지("무리한 진입보다 기다림이 나을 때도 있어요") 반환
- 결정 사항:
  86. **🆕 관망 강제 (v19.1)**: ✅ 종목 없으면 추천 안 함 (cold start 제외)

### 15-16. "3일 순위보고 한 달 보유" 전략 근거

유저 질문: "애초에 3일 순위보고 한 달 가져가는게 맞을지"

**답변 — 전략적으로 합리적인 조합:**

1. **3일 검증의 역할 = 진입 시점 신뢰도**
   - EPS 전망치는 매일 변동 → 하루 나타났다 사라지는 종목(노이즈) 걸러냄
   - 3일 연속 Part 2 = 최소 3번의 독립적 데이터 수집에서 일관된 신호
   - "필터"이지 "보유 기간 결정자"가 아님

2. **보유 2~4주의 근거 = EPS 반영 사이클**
   - 애널리스트 EPS 상향 → 주가 반영까지 통상 2~6주 소요 (학술 연구: PEAD, Post-Earnings Announcement Drift)
   - adj_gap이 음수 = EPS 개선이 아직 주가에 미반영 → 반영까지 시간 필요
   - 너무 짧으면(1주) 반영 전에 매도, 너무 길면(3개월+) 새로운 EPS 변동에 노출

3. **탈락 = 매도 검토 (Death List)**
   - 보유 중인 종목이 Part 2에서 빠지면 → Death List 알림
   - 사유(MA60↓, 괴리+, 점수↓, EPS↓) 제공 → 기계적 보유 아닌 조건부 보유
   - 즉, 보유 기간은 "2~4주 OR 탈락 시점" 중 빠른 쪽

4. **결론**: 3일 검증(진입 신뢰도) + 2~4주 보유(EPS 반영 사이클) + 탈락 매도(조건부 퇴장)은 서로 보완적. 3일이 보유 기간을 결정하는 게 아니라, 진입과 퇴장의 역할이 분리되어 있음.

### 15-17. 백테스트 프레임워크 구축

- **파일**: `backtest.py` (신규)
- **목적**: 검증 일수(2/3/5/7) × 보유 기간(5/10/15/20일) × 퇴장 조건(고정/Death List) 매트릭스 비교
- **설계**:
  - DB `part2_rank IS NOT NULL`로 Part 2 진입 판별 (이미 6개 필터 통과한 종목)
  - `find_entry_signals(df, verify_days)`: N일 연속 Part 2 종목 → 진입 신호
  - `get_exit_prices()`: yfinance 일괄 다운로드 (캐싱)
  - `find_death_exit_date()`: Part 2 탈락일 = Death List 퇴장
  - 고정 보유 vs Death List 퇴장 두 가지 전략 비교
- **출력**: 평균 수익률 매트릭스 + 거래 수/승률/최대 손실
- **현재 상태**: DB 1일치 → 최소 30일 필요 → ~29 거래일 후 실행 가능
- 결정 사항:
  87. **백테스트 프레임워크 (v19.1)**: 데이터 축적 후 검증 일수/보유 기간 최적값 도출 예정

### 15-18. ⏳ 포트폴리오 제외 — ✅만 추천

- **변경 전**: ✅(3일) 풀 비중 + ⏳(2일) 절반 비중으로 포트폴리오 포함
- **변경 후**: ✅(3일)만 포트폴리오 포함, ⏳/🆕는 매수 후보에만 표시
- **이유**: 3일 검증이 "최소 신뢰 기준"인데 2일도 넣으면 기준의 의미가 희석됨. 절반 비중도 고객에게 직관적이지 않음. 심플하게 유지.
- **비중 로직 단순화**: ⏳ 절반 비중/보정 로직 삭제, 모든 ✅ 종목 동일 기준 (adj_gap 비례, 30% 캡, 5% 단위)
- 결정 사항:
  88. **⏳ 포트폴리오 제외 (v19.1)**: 포트폴리오는 ✅만, ⏳는 관찰용

*v19.1 업데이트: Claude Opus 4.6 | 2026-02-10 집 PC — 🆕 버그 수정, ⏳ 포트폴리오 제외, 전략 근거 문서화, 백테스트 프레임워크*

### 15-19. v19.2 UI 개편 — [1/2][2/2] 구조 + 시장 컨텍스트

**메시지 구조 변경** (3개 → 4개 → 3개):
- 📖 투자 가이드 (신규): 시스템 개요, 선정 과정, 보유/매도 기준
- [1/2] 🔍 매수 후보: Part 2 통합 (아이콘 💰→🔍)
- [2/2] 🛡️ AI 점검 + 🎯 최종 추천: AI 브리핑 + 포트폴리오 통합 (아이콘 🤖→🛡️, 💼→🎯)
- 시스템 로그 (개인봇)

**시장 컨텍스트 추가**:
- [1/2] 상단에 S&P 500 / 나스닥 전일 종가 및 등락률 표시
- `get_market_context()`: yfinance `^GSPC`, `^IXIC` 5일 히스토리에서 계산

**Death List 구조화**: `get_death_list()` — 어제 Part 2에서 탈락한 종목 + 사유 자동 판별
**보유 확인 섹션**: "✅ 보유 유지 가능" 섹션 추가 (목록에 있는 종목 = 보유 유지)
**Survivors 리스트**: 어제도 오늘도 Part 2에 있는 종목 표시

**⏳ 삭제**: 2일 검증 아이콘 제거 → ✅/🆕 2단계로 단순화
**날짜 통일**: 전 메시지 `biz_day` (미국 영업일) 기준으로 통일
**MAX_WEIGHT 30%**: 포트폴리오 단일 종목 비중 상한 30% 유지

- 결정 사항:
  89. **[1/2][2/2] 구조 (v19.2)**: AI+포트폴리오 통합, 채널 메시지 3개로 최적화
  90. **시장 컨텍스트 (v19.2)**: S&P 500/나스닥 전일 종가+등락률, yfinance 조회
  91. **⏳ 삭제 (v19.2)**: 2단계(✅/🆕) 단순화, 고객 혼란 방지
  92. **날짜 biz_day 통일 (v19.2)**: 주말 실행 시에도 금요일 날짜 표시

*v19.2 업데이트: Claude Opus 4.6 | 2026-02-10 집 PC — [1/2][2/2] 구조, 시장 컨텍스트, ⏳ 삭제, 날짜 통일*

---

## Phase 16: v20 Simple & Clear — 단순 명료하게 (2026-02-11)

### 16-1. 핵심 철학

**사용자 인사이트**: "성공하는 제품은 이름이 빅맥처럼 부르기 쉽고 구조도 단순 명료해야 해."

Death List, Survivors, 2일 연속 탈락 등 복잡한 퇴장 로직을 모두 제거하고, **"목록에 있으면 보유, 없으면 매도 검토"** 하나의 원칙으로 단순화.

### 16-2. ⏳ 2일 관찰 아이콘 복원

v19.2에서 삭제했던 ⏳를 **표시 전용**으로 복원:
- ✅ = 3일 연속 Top 30 (포트폴리오 대상)
- ⏳ = 2일 연속 Top 30 (표시만, 내일 검증 가능)
- 🆕 = 오늘 첫 진입 (표시만)

**차이점**: v19.1에서 ⏳는 절반 비중으로 포트폴리오 포함 → v20에서는 **순수 표시만** (포트폴리오 제외).

### 16-3. Top 30 경계 통일

모든 시스템이 동일한 **Top 30** 기준 사용:
| 항목 | 이전 | v20 |
|------|------|-----|
| `save_part2_ranks()` | 필터 통과 전체 저장 | **Top 30만** 저장 |
| `get_3day_status()` | `part2_rank IS NOT NULL` | `part2_rank <= 30` |
| `get_daily_changes()` | (없었음, Death List 사용) | 어제 Top 30 vs 오늘 Top 30 |
| 메시지 표시 | Top 30 | Top 30 (변경 없음) |

### 16-4. Death List → `get_daily_changes()` (단순 set 비교)

**이전** (`get_death_list()`):
- 어제 Part 2에서 탈락한 종목 감지
- 사유 판별 (MA60↓, 괴리+, 점수↓, EPS↓, 순위밖)
- 복잡한 로직 (전체 results_df 조회)

**변경** (`get_daily_changes()`):
```python
def get_daily_changes(today_tickers):
    yesterday_top30 = DB에서 어제 part2_rank <= 30 조회
    entered = today_set - yesterday_top30
    exited = yesterday_top30 - today_set
    return sorted(entered), sorted(exited)
```
- 단순 set 차집합 비교, 사유 판별 없음
- 이탈 종목은 [1/2] 메시지 하단에 `📉 어제 대비 이탈 N개` + 종목명만 표시
- Death List 섹션, Survivors 섹션 모두 제거

**사용자 결론**: "top30 자체가 생존자니까 생존자 탈락자 그딴거없이 그냥 top30을 매일 보여주는건 어때?"

### 16-5. Cold Start 변경: 전부 ✅ → 전부 🆕

- **이전**: DB 3일 미만 → 전부 ✅ 처리 (cold start 시 모든 종목이 포트폴리오 대상)
- **변경**: DB 2일 미만 → 전부 🆕  처리 (검증 안 된 종목은 🆕)
- **이유**: 검증 데이터 없이 ✅ 주는 것은 기만. 🆕로 시작하면 자연스럽게 데이터 축적 → 정상 작동

### 16-6. 투자 가이드 메시지 전면 재작성

**주요 변경점**:
- "증권사 애널리스트" → "월가 애널리스트" (더 있어보이게)
- "실적이 좋아질 거야" → "이익이 늘어날 거야" (EPS = 주당순이익, 영업이익 아님)
- ①~⑤ 5단계가 파이프라인/퍼널로 연결되게 재구성:
  ```
  ① 이익 전망이 오르는 종목을 찾고
  ② 주가 흐름이 건강한 종목만 남기고
  ③ 그중 주가가 덜 오른 순서로 Top 30 선별
  ④ 3일 연속 Top 30에 들면 검증 완료 ✅
  ⑤ AI 위험 점검 후 최종 5종목 추천
  ```
- "약 2~4주" → "최소 2주 보유 권장"
- 매도 기준: "목록에 있으면 보유, 없으면 매도 검토"
- "미국 916종목을 매일 5단계로 걸러요" 추가

### 16-7. [1/2] 매수 후보 메시지 개선

- 도입부: "실적 전망" → "이익 전망"
- 읽는 법: ✅/⏳/🆕 각각 의미 명확히 + Top 30 연결
- 의견 설명 추가: "의견 = 최근 30일 애널리스트 ↑상향 ↓하향 수"
- ⚠️ 설명 제거 (날씨 아이콘만 유지)
- 하단: Death List 블록 + Survivors 블록 → 단순 "📉 어제 대비 이탈 N개" + "목록에 있으면 보유, 없으면 매도 검토."

### 16-8. [2/2] AI 점검 + 포트폴리오 메시지

- `run_ai_analysis()`: `death_list` 매개변수 제거 (더 이상 사용 안 함)
- 포트폴리오 활용법: "탈락 알림(📉)이 오면" → "목록에서 빠지면"
- 보유 기간: "약 2~4주" → "최소 2주"

### 16-9. 대칭적 진입/퇴장 기준 정리

**진입**: 3일 연속 Top 30 → ✅ → 포트폴리오 대상
**퇴장**: Top 30에 없으면 매도 검토

**이전의 비대칭 문제**:
- 진입: Top 30 기준
- 퇴장: 전체 Part 2 필터 통과 기준 (더 넓은 범위)
- 결과: 진입은 엄격하고 퇴장은 느슨 → 고객 혼란

**v20 해결**: 진입도 퇴장도 모두 Top 30 기준. 단순하고 일관됨.

### 16-10. 메시지 UX 최종 개편 (v20 후반)

**메시지 3개 분리**: [1/2]+[2/2] → [1/3]+[2/3]+[3/3]
- `[1/3] 🔍 매수 후보`: adj_gap순 Top 30, ✅/⏳/🆕 마커, 📉 이탈 표시
- `[2/3] 🛡️ AI 리스크 필터`: "AI 점검" → "AI 리스크 필터" 명칭 변경, "매수 후보의 위험 요소를 AI가 걸러냈어요"
- `[3/3] 🎯 최종 추천`: 포트폴리오를 별도 메시지로 분리 (하이라이트 강조)

**AI 리스크 필터 개선**:
- 종목간 구분선 `──────────────────` → 빈 줄(`\n\n`)로 간소화
- `[SEP]` 변환: `re.sub(r'\n*\[SEP\]\n*', '\n\n', analysis_html)`
- 하단 다음 단계 안내: `👉 다음: 최종 추천 포트폴리오 [3/3]`

**포트폴리오 메시지 강화**:
- 퍼널 요약: `916종목 → Top 30 → ✅ 검증 → 최종 N종목`
- `📊 비중 한눈에 보기`: 한 줄 요약 (종목명(티커) N% · ...)
- 날짜 추가: `📅 YYYY년 MM월 DD일 (미국장 기준)`
- Gemini 프롬프트: 순위 번호 + 날씨 아이콘 포함 형식
- 관망 메시지도 `[3/3] 🎯 최종 추천` 형식 유지

**투자 가이드 풋터 간소화**:
- `📩 오늘의 메시지` 섹션 제거 (메시지 순서 안내 불필요)
- `⏱️ 보유` + `📉 매도` 섹션은 유지

**스토리 라인**: 📖이해 → 🔍탐색 → 🛡️필터링 → 🎯실행

### 결정 사항 추가

93. **빅맥 원칙 (v20)**: 단순 명료하게 — Death List/Survivors 제거, set 비교로 단순화
94. **⏳ 표시 전용 복원 (v20)**: 2일 연속 관찰용, 포트폴리오 제외 (v19.2에서 삭제 → v20에서 재도입)
95. **Top 30 경계 통일 (v20)**: 저장/검증/비교/표시 모두 Top 30
96. **get_daily_changes (v20)**: Death List → 단순 set 비교 (사유 판별 제거)
97. **Cold start 🆕 (v20)**: 검증 없으면 🆕 (이전: 전부 ✅)
98. **투자 가이드 재작성 (v20)**: 월가, 이익, 5단계 퍼널, Top 30 명시, 최소 2주
99. **매도 원칙 (v20)**: "목록에 있으면 보유, 없으면 매도 검토" — 하나의 규칙으로 통일
100. **진입/퇴장 대칭 (v20)**: 모두 Top 30 기준, 비대칭 제거
101. **메시지 3분리 (v20)**: [1/2]+[2/2] → [1/3]+[2/3]+[3/3], 포트폴리오 별도 메시지
102. **AI 리스크 필터 (v20)**: "AI 점검" → "AI 리스크 필터", 종목간 구분선 제거
103. **포트폴리오 퍼널 (v20)**: 916→Top30→✅→최종 N종목, 비중 한눈에 보기
104. **가이드 간소화 (v20)**: 📩 오늘의 메시지 섹션 제거, 보유/매도 기준은 유지
105. **애널리스트 데이터 max(0y,+1y) (v20)**: rev_up30, rev_down30, num_analysts를 0y만 → max(0y, +1y)로 변경. NTM이 0y+1y 블렌딩이므로 revision도 양쪽 반영 필요. 저커버리지 종목(CPT 등)에서 +1y 데이터가 보완.
106. **의견 하향 비율 기반 (v20)**: rev_down ≥ 3 (절대) → rev_down/(up+down) > 0.5 → **> 0.3 (30%)**. GM처럼 ↑17↓4인 대형주가 절대값으로 제외되던 문제 해소. 50%는 SMCI(↑13↓9=41%)를 놓쳐서 30%로 강화. "의견 하향 과반" → "의견 하향"으로 명칭 변경.
107. **어닝 임박 소프트 전환 (v20)**: 어닝 2주 내 → 포트폴리오 하드 제외에서 표시만으로 변경. EPS 상향 종목이 어닝에서 좋은 실적 나올 확률 높음.
108. **고평가(PE>100) 필터 제거 (v20)**: adj_gap≤0이 이미 밸류에이션 제어. PLTR 같은 고성장 EPS 모멘텀주가 PE 하나로 부당 제외되는 문제. 리스크 필터 철학 확립: 데이터 신뢰성만 걸러냄(저커버리지, 하향과반), 주가/밸류에이션은 건드리지 않음.
109. **과거 데이터 보충 (v20)**: DB 2/7→2/6(금) 날짜 수정, 2/8(주말) 삭제, 2/6·2/9에 yfinance 과거 종가+NTM으로 price/ma60/adj_score/adj_gap/part2_rank 계산. 소급 추정 아닌 실제 데이터 기반 정확한 계산. 결과: 2/6(22개), 2/9(24개), 2/10(22개) → 3일 part2_rank 확보, cold start 해제.

110. **마켓 날짜 자동 감지 (v20)**: `datetime.now()` 대신 SPY 최근 거래일 기준으로 `today_str` 결정. EST 자정 이후 테스트 실행 시 미래 날짜(2/11)로 데이터 저장되어 3일 검증 오염되던 문제 해결. `MARKET_DATE` 환경변수 오버라이드도 지원. test-private-only.yml에 `market_date` 입력 파라미터 추가.

111. **INSERT OR REPLACE → ON CONFLICT (v20)**: 같은 마켓 날짜로 재수집 시 `INSERT OR REPLACE`가 part2_rank를 NULL로 초기화하던 문제. `INSERT ... ON CONFLICT DO UPDATE`로 변경하여 지정 컬럼만 업데이트, part2_rank 보존.
112. **포트폴리오 섹터 분산 (v20)**: adj_gap 순 정렬 후 섹터당 1종목만 선정 (중복 섹터 스킵). 비중은 기존 adj_gap 비례 유지. 순위 기반 고정 비중(25/25/20/15/15%)은 시도 후 원복.
113. **워크플로우 git merge 방식 변경 (v20)**: `git pull --rebase` → `git pull --no-rebase -X ours`. binary DB 파일은 rebase 불가하여 충돌 발생 방지.

114. **이탈 종목 순위 변동 표시 (v20)**: 이탈 종목에 "어제 N위 → M위" 또는 "어제 N위 → 조건 미달" 표시. 사유 분석(이평선 이탈, 의견 하향 등)은 데이터 부족으로 폐기, 순위 변동만 표시. 전체 eligible 종목 계산하여 현재 순위 매핑.
115. **adj_gap ≤ 0 필터 제거 (v20)**: MU가 하루 +10% 급등으로 adj_gap 플러스 전환 → 즉시 탈락하는 문제. EPS 모멘텀은 여전히 rank 4인데 일간 변동성에 목록이 불안정. 필터 제거하고 adj_gap 정렬만 유지 → 비싸도 모멘텀 강한 종목은 남되 순위가 밀림.

*v20 업데이트: Claude Opus 4.6 | 2026-02-11 회사 PC — Simple & Clear: Death List 제거, Top 30 통일, 투자 가이드 재작성, 메시지 3분리, AI 리스크 필터, 포트폴리오 강화, 애널리스트 max(0y,+1y), 리스크 필터 철학 확립, 과거 데이터 보충, 마켓 날짜 자동 감지, ON CONFLICT 보존, 섹터 분산, 이탈 순위 변동 표시, adj_gap 필터 제거*

## Phase 17: v21 Composite Score — 매출 성장률 반영 (2026-02-12)

### 17-1. 핵심 철학

**사용자 인사이트**: "파괴적 혁신 기업을 싸게 살래"

adj_gap(EPS 대비 저평가)만으로는 COLM(매출 -2%), GM(매출 -5%) 같은 매출 역성장 기업이 상위에 올라오는 문제. KR quant 프로젝트의 타이밍 팩터(RSI+MA20) 검토 후 부적합 판단 → 매출 성장률을 복합 점수에 반영.

### 17-2. Composite Score 도입

```python
# z-score 정규화 후 복합 점수
z_gap = (adj_gap - mean) / std      # 부호 반전: 음수가 좋으므로 -z_gap
z_rev = (rev_growth - mean) / std   # 양수가 좋음

composite = (-z_gap) * 0.7 + z_rev * 0.3
```

**비율 결정 과정**: 100/0, 90/10, 80/20, 70/30, 60/40, 50/50 시뮬레이션.
- 80/20: 보수적이지만 혁신 반영 약함
- 70/30: SMCI 같은 잡주도 올라오지만 "파괴적 혁신" 철학에 부합
- 50/50: 과도하게 매출 성장만 추종

→ **70/30 채택** (사용자: "파괴적 혁신 기업을 싸게 살래")

### 17-3. fetch_revenue_growth() 신규 함수

- eligible 상위 50종목만 yfinance `revenueGrowth` 수집 (~12초)
- rev_growth가 없는 종목: composite 정렬 뒤에 adj_gap 순으로 붙임
- 유효 데이터 10개 미만이면 fallback (adj_gap만 정렬)

### 17-4. 과거 데이터 소급 재계산

4일치(2/6, 2/9, 2/10, 2/11) part2_rank를 composite score 기준으로 재계산.
→ 3일 검증(✅) 상태가 새 순위 기준에 맞게 갱신됨.

### 17-5. 포트폴리오 정렬 변경

**이전**: `get_part2_candidates()`로 composite 정렬 후, 포트폴리오 함수에서 `safe.sort(key=lambda x: x['adj_gap'])`으로 재정렬 → composite 순서 파괴
**변경**: 재정렬 제거, composite 순서 그대로 유지

### 17-6. 포트폴리오 동일 비중

**이전**: 순위 기반 비중 [30/25/20/15/5] — 5위 종목이 5%로 의미 없음
**변경**: **동일 비중** (5종목 = 각 20%)

**결정 근거**:
- 소수점 매매 수수료가 기존의 2배 → 비율 맞추기 부담
- 5종목이면 순위 차이가 크지 않아 동일 비중이 합리적
- 3종목이면 34/33/33, 4종목이면 25/25/25/25

### 17-7. AI 리스크 필터 프롬프트 구조화

**문제**: Gemini가 인사말만 반환하고 실제 분석 없음
**원인**: "위험 신호 없는 종목은 언급하지 마" 지시가 너무 강해서 전체 응답이 비어버림

**수정**:
- "인사말/서두/맺음말 금지. 아래 3개 섹션만 출력" 명시
- 필수 3개 섹션: 📰 시장 동향 / ⚠️ 매수 주의 / 📅 어닝 주의
- 주의 종목 없을 때: "✅ 모든 후보가 현재 양호해요." 한 줄 출력
- 응답 검증: `📰` 또는 `시장` 키워드 없으면 자동 재시도

### 17-8. 채널 전송 중단

`daily-screening.yml`에서 `TELEGRAM_CHAT_ID` 주석 처리 → 개인봇만 발송.

### 17-9. 매출 성장 10% 하드 필터

**철학**: "파괴적 혁신 기업을 싸게 살래" — 매출 성장률 10% 미만은 혁신이 부족한 기업.
- HSY(+7%, 제과), LUV(+7%, 항공), F(+9%, 완성차) 등 제외
- `get_part2_candidates()`에서 `valid = valid[valid['rev_growth'] >= 0.10]`
- `save_part2_ranks()`에 NULL 초기화 추가: 필터 변경 시 잔여 rank 방지
- 과거 4일치(2/6, 2/9, 2/10, 2/11) 재계산 완료 (`recalc_ranks.py`)

### 17-10. 메시지 포맷 최종

```
✅ 1. Western Digital(SNDK)
HW · ☀️☀️🔥☀️ 중반 급등
EPS +317% · 매출 +61%
의견 ↑8↓0 · 순위 1→1→1
──────────────────
```

- 4줄 레이아웃: 종목명(티커) / 업종·날씨 / EPS·매출 / 의견·순위이력
- 괴리 표시 제거 (composite 순위라 단독 표시 무의미)
- 순위 이력: `get_rank_history()` → 최근 3일 DB 조회 → `3→4→1`
- 이력 없으면 `-→-→N` 형태 (이전 순위 없었음을 표시)
- 이탈 종목: 종목명(티커) 형식 통일

### 결정 사항 추가

116. **매출 성장률 복합 순위 (v21)**: adj_gap 70% + rev_growth 30% composite score. z-score 정규화. "파괴적 혁신 기업을 싸게" 철학.
117. **70/30 비율 (v21)**: 시뮬레이션으로 결정. SMCI(+123%) 2위 진입, COLM(-2%) 11위 하락, GM(-5%) 14위 하락.
118. **동일 비중 (v21)**: 순위 기반 → 동일 비중(각 20%). 소수점 매매 수수료 부담 제거. 실전 편의성.
119. **AI 프롬프트 구조화 (v21)**: 3개 필수 섹션 명시 + 인사말 금지 + 응답 검증(재시도). 빈 응답 방지.
120. **채널 전송 중단 (v21)**: 개인봇만 발송. 채널은 추후 재개.
121. **매출 성장 10% 하드 필터 (v21)**: `rev_growth < 0.10` 제외. HSY(+7%), LUV(+7%) 등 저성장 기업 탈락. "혁신 없는 종목은 아무리 저평가여도 제외."
122. **save_part2_ranks NULL 초기화 (v21)**: 저장 전 `UPDATE SET part2_rank=NULL WHERE date=?`. 필터 변경 시 잔여 rank 방지.
123. **메시지 4줄 포맷 (v21)**: ① 종목명(티커) ② 업종 · 날씨 ③ EPS · 매출 ④ 의견 · 순위이력. 괴리 표시 제거(composite 순위라 단독 무의미).
124. **순위 이력 3일 표시 (v21)**: `get_rank_history()` → `3→4→1` 형태. 이력 없으면 `-→-→N` 형태.
125. **이탈 종목 종목명 표시 (v21)**: 티커만 → 종목명(티커) 형식 통일.

126. **섹터 분산 제거 (v22)**: industry 중복 스킵 로직 제거. composite 순서 그대로 상위 5종목 선정. yfinance `industry`가 NVDA(GPU)와 MU(메모리)를 같은 "반도체"로 묶어 사업이 다른 종목을 부당하게 제외하던 문제 해결. "파괴적 혁신 기업을 싸게 살래" 철학상 혁신이 반도체에 집중되면 반도체 3종목이 맞는 신호.
127. **rev_growth 필수화 (v22)**: 매출 데이터 없는 종목(NaN)은 Top 30에서 제외. 이전에는 rev_growth NaN이 10% 하드 필터를 우회하여 composite 뒤에 adj_gap순으로 붙는 허점 존재. WDC, TER, AEIS 등이 매출 미검증 상태로 순위에 올라오던 문제 해결.
128. **fetch_revenue_growth 전체 수집 (v22)**: 상위 50종목→eligible 전체(~61종목)로 확대. rev_growth 필수화로 Top 30을 채우려면 더 넓은 풀에서 매출 데이터 수집 필요. 수집 시간 ~12초→~15초 소폭 증가.
129. **업종 분포 통계 (v22)**: [1/3] 메시지에 `📊 주도 업종: 반도체 8 · 통신장비 3` 형태로 Top 30의 업종 분포 표시. 2종목 이상 업종만 표시. 주도 섹터 파악, 시장 폭(Market Breadth) 판단, 섹터 로테이션 감지 용도.

*v22 업데이트: Claude Opus 4.6 | 2026-02-12 집 PC — 섹터 분산 제거, rev_growth 필수화, 매출 수집 전체 확대, 업종 분포 통계*

---

## 미결: 현금 비중 관리 — 매크로 지표 기반 (v21 이후)

### 배경
종목 레벨(916→Top30→5종목)은 완성됐으나, 자산배분 레벨(퀀트 vs 현금 비중)이 없음. 시장 전체가 하락할 때 풀 투입하면 종목 선별이 아무리 좋아도 손실.

### 검토한 매크로 지표

| 지표 | 장점 | 단점 | 판정 |
|------|------|------|------|
| **ICE BofA HY Spread** | 신용시장 체온계, 체제 판단 최적 | 일일 변화로 단기 예측 불가 (mean reversion) | **메인** |
| **VIX** | 실시간 공포 체감 | 후행적, 스파이크성, 이미 빠진 후 반응 | **보조** |
| **CNN Fear & Greed** | 센티먼트 종합 | 과도한 단순화, 노이즈, 구성 요소를 직접 보는 게 나음 | **불필요** |

### 30년 데이터 EDA 결과 (2026-02-12 분석)

> 데이터: FRED BAMLH0A0HYM2 × S&P500/NASDAQ, 1997~2026 (7,321 영업일)
> 분석 스크립트: hy_analysis.py ~ hy_analysis4.py
> 상세 결과: memory/hy-spread-analysis.md

#### 핵심 발견 1: 동행 지표이지 선행 지표가 아님
- 동행 상관 -0.43 (강함), 선행 +0.07 (없음)
- HY 급등 다음날은 모든 구간에서 **반등** (mean reversion)
- 유일한 단기 경고: **4% 상향 돌파** (61건, 다음날 -0.39%, 하락확률 54%)

#### 핵심 발견 2: 해빙(하락) = 강력한 매수 신호
- 4~5%에서 ≤-20bp 축소 → 60일 **+6.22%**
- 피크 대비 -300~500bp 하락 → 60일 **+11.89%**
- 5% 하향 돌파 (올클리어) → 60일 **+4.17%**

#### 핵심 발견 3: Verdad 4분면 모델 (최적)
수준(10년 중위수 대비 넓/좁) × 방향(3개월 전 대비 상승/하락):

| 분면 | 조건 | 비중 | SP500 연율 | 양수확률 |
|------|------|------|-----------|---------|
| Q1 회복 | 넓+하락 | 18% | **+14.3%** | 86% |
| Q2 성장 | 좁+하락 | 44% | +9.4% | 84% |
| Q3 과열 | 좁+상승 | 20% | +5.1% | 72% |
| Q4 침체 | 넓+상승 | 18% | +9.9%* | 79% |

*Q4가 높은 건 위기 후 반등 포함. Q4 후기(61일+)는 60일 -2.38%.

Verdad 모델은 단순 수준 모델보다 분별력 **1.4배** 우수 (250일 최고-최저 차: 9.2%p vs 6.4%p).

현재 상태 (2026-02-11): HY 2.84%, 10년 중위수 3.79% → **Q2 성장 (좁+하락)**

#### 참고 논문
- Gilchrist & Zakrajsek (2012) AER: Excess Bond Premium이 진짜 예측력
- Lopez-Salido, Stein & Zakrajsek (2017) QJE: 스프레드 좁으면 2년 후 확대
- Neuberger Berman (2024): 하위 10%ile 도달 → 6건 중 5건 확대 (+152bp/397일)
- Verdad Capital: 수준×방향 4분면, 연 450bp 초과수익

### Method C 구현 완료 ✅ (v23~v24, 2026-02-12)

#### 함수: `fetch_hy_quadrant()` (daily_runner.py)
- FRED CSV 직접 다운로드 (API 키 불필요)
- 10년(2520영업일) 롤링 중위수 계산 (min 5년)
- Verdad 4분면 판정 + 해빙 신호 4가지 감지
- **분면 지속 일수 계산** → 현금 비중 + 핵심 행동 자동 산출 (v24)
- 실패 시 `None` 반환 → 기존 메시지 정상 출력 (graceful degradation)

#### Verdad 4분면 (매일 표시)

| 분면 | 조건 | 아이콘 | 메시지 표시 |
|------|------|--------|------------|
| Q1 회복기 | HY ≥ 중위수 + 3개월 전보다 하락 | 🟢 | `🟢 신용시장 — 회복기` + HY 해석 + `→ 적극 매수하세요.` |
| Q2 성장기 | HY < 중위수 + 3개월 전보다 하락 | 🟢 | `🟢 신용시장 — 성장기` + HY 해석 + `→ 평소대로 투자하세요.` |
| Q3 과열기 | HY < 중위수 + 3개월 전보다 상승 | 🟡 | `🟡 신용시장 — 과열기` + HY 해석 + `→ 신중하게 판단하세요.` |
| Q4 침체기 | HY ≥ 중위수 + 3개월 전보다 상승 | 🔴 | `🔴 신용시장 — 침체기` + HY 해석 + `→ 매수를 멈추고~현금 확보.` |

- **수준 기준**: HY spread vs 10년 롤링 중위수 (시대에 적응, 현재 ~3.76%)
- **방향 기준**: 현재 HY vs 63영업일(3개월) 전 HY
- 현재 상태: HY 2.84%, 중위수 3.76% → **Q2 성장기**

#### 해빙 신호 4가지 (조건 충족 시에만 추가 표시)

| # | 조건 | 표시 | 30년 근거 |
|---|------|------|----------|
| 1 | HY 4~5% 구간 + 일일 ≤-20bp | `💎 HY x%, 전일 대비 -Nbp 급락 — 반등 매수 기회에요!` | 60일 +6.22% |
| 2 | 전일 HY ≥5% → 오늘 <5% | `💎 HY x%로 5% 밑으로 내려왔어요 — 적극 매수 구간이에요!` | 60일 +4.17% |
| 3 | 60일 고점 대비 -300bp 이상 | `💎 60일 고점 대비 -Nbp 하락 — 바닥 신호, 적극 매수하세요!` | 60일 +11.89% |
| 4 | 전일 Q4 → 오늘 Q1 | `💎 침체기→회복기 전환 — 가장 좋은 매수 타이밍이에요!` | 250일 +12.69% |

#### 메시지 표시 위치
[1/3] 매수 후보 메시지 상단, S&P500/나스닥 지수 바로 아래:
```
─────────────────
🟢 S&P 500  6,068 (+0.47%)
🟡 나스닥  19,643 (-0.16%)
🟢 신용시장 — 성장기  ← 여기
HY Spread(부도위험) 2.84%
평균(3.76%)보다 낮아서 안정적이에요.
📊 투자 80% + 현금 20%
→ 평소대로 투자하세요.
```

#### 현금 비중 권장 (v24, 30년 EDA 기반)

종목 수는 항상 **5개 유지** (분산 유지). 종목당 비중 **고정 20%**.
현금비중만 별도 조절 (0~70%).

**기본 현금 20%** (교체 대기, 물타기, 급락 대비) + 매크로 리스크 추가.
Q1 해빙기만 0% — 30년 최고 수익 구간이므로 풀 공격.

| 분면 | 지속 기간 | 현금 비중 | 핵심 행동 | 30년 근거 |
|------|----------|----------|----------|----------|
| **Q1 회복기** | - | **0%** | **적극 매수하세요. 역사적으로 수익률이 가장 높은 구간이에요.** | 연율 +14.3%, 양수확률 86% |
| Q2 성장기 | - | 20% | 평소대로 투자하세요. | 연율 +9.4%, 양수확률 84% |
| Q3 과열기 | <60일 | 20% | 매수할 때 신중하게 판단하세요. | 60일 +1.84%, 아직 양수 |
| Q3 과열기 | ≥60일 | 30% | 신규 매수를 줄여가세요. | 60일 +0.39%, 거의 제로 |
| Q4 침체기 | 1~20일 | 30% | 신규 매수를 멈추고 관망하세요. | 60일 +3.37% (반등), 급매도 금지 |
| Q4 침체기 | 21~60일 | 50% | 보유 종목을 줄이고 현금을 늘리세요. | 60일 +0.54%, 모멘텀 약화 |
| Q4 침체기 | 61일+ | 70% | 현금을 최대한 확보하세요. | 60일 **-2.38%**, 본격 하락 |

**핵심 통찰**:
- Q4 초기 ≠ 패닉. 초기 반등(+3.37%) 있으므로 단계적 축소가 정답.
- Q1 0%는 "레버리지 대용". 평소 20% → 해빙기 0% = 상대적 1.25배 공격.
- 기본 20%는 교체 대기/물타기/급락 대비 운영 자금.

#### 케이스별 출력 예시

**Case 1: Q2 성장기 (평상시)**
```
🟢 신용시장 — 성장기
HY Spread(부도위험) 2.84%
평균(3.76%)보다 낮아서 안정적이에요.
📊 투자 80% + 현금 20%
→ 평소대로 투자하세요.
```

**Case 2: Q3 과열기 (초기)**
```
🟡 신용시장 — 과열기
HY Spread(부도위험) 3.50%
평균(3.76%) 이하지만 올라가는 중이에요.
📊 투자 80% + 현금 20%
→ 매수할 때 신중하게 판단하세요.
```

**Case 2b: Q3 과열기 60일+ (장기)**
```
🟡 신용시장 — 과열기
HY Spread(부도위험) 3.55%
평균(3.76%) 이하지만 올라가는 중이에요.
📊 투자 70% + 현금 30%
→ 신규 매수를 줄여가세요.
```

**Case 3a: Q4 침체기 (초기 1~20일)**
```
🔴 신용시장 — 침체기
HY Spread(부도위험) 4.80%
평균(3.80%)보다 높고 계속 올라가고 있어요.
📊 투자 70% + 현금 30%
→ 신규 매수를 멈추고 관망하세요.
```

**Case 3b: Q4 침체기 (중기 21~60일)**
```
🔴 신용시장 — 침체기
HY Spread(부도위험) 5.20%
평균(3.80%)보다 높고 계속 올라가고 있어요.
📊 투자 50% + 현금 50%
→ 보유 종목을 줄이고 현금을 늘리세요.
```

**Case 3c: Q4 침체기 (장기 61일+)**
```
🔴 신용시장 — 침체기
HY Spread(부도위험) 5.80%
평균(3.80%)보다 높고 계속 올라가고 있어요.
📊 투자 30% + 현금 70%
→ 현금을 최대한 확보하세요.
```

**Case 4: Q1 회복기 + 급축소**
```
🟢 신용시장 — 회복기
HY Spread(부도위험) 4.50%
평균(3.80%)보다 높지만 빠르게 내려오고 있어요.
📊 투자 100%
→ 적극 매수하세요. 역사적으로 수익률이 가장 높은 구간이에요.
💎 HY 4.50%, 전일 대비 -25bp 급락 — 반등 매수 기회에요!
```

**Case 5: 올클리어 (5% 하향 돌파)**
```
🟢 신용시장 — 회복기
HY Spread(부도위험) 4.95%
평균(3.80%)보다 높지만 빠르게 내려오고 있어요.
📊 투자 100%
→ 적극 매수하세요. 역사적으로 수익률이 가장 높은 구간이에요.
💎 HY 4.95%로 5% 밑으로 내려왔어요 — 적극 매수 구간이에요!
```

**Case 6: 강력 매수 (피크 대비 -300bp)**
```
🟢 신용시장 — 회복기
HY Spread(부도위험) 6.80%
평균(3.80%)보다 높지만 빠르게 내려오고 있어요.
📊 투자 100%
→ 적극 매수하세요. 역사적으로 수익률이 가장 높은 구간이에요.
💎 60일 고점 대비 -350bp 하락 — 바닥 신호, 적극 매수하세요!
```

**Case 7: 최고 매수 구간 (Q4→Q1 + 복합 신호)**
```
🟢 신용시장 — 회복기
HY Spread(부도위험) 4.60%
평균(3.80%)보다 높지만 빠르게 내려오고 있어요.
📊 투자 100%
→ 적극 매수하세요. 역사적으로 수익률이 가장 높은 구간이에요.
💎 HY 4.60%, 전일 대비 -30bp 급락 — 반등 매수 기회에요!
💎 침체기→회복기 전환 — 가장 좋은 매수 타이밍이에요!
```

#### 사용법: "오늘의 날씨 + 행동 가이드"

분면은 **시장 체제 배경 정보** + 현금 비중은 **행동 가이드**:
- Q2↔Q3 전환은 경계선 노이즈 → 현금 비중 변화 없음
- Q3 60일+: 신규 진입만 축소, 기존 보유 유지
- Q4 진입: 단계적 축소 (초기 반등 놓치지 않도록)
- Q4→Q1 + 해빙 신호: 현금 → 주식 복원 시점
- 보유 종목은 분면과 무관하게 기존 규칙(Top 30 이탈 시 매도 검토) 유지

#### 설계 철학

| 방향 | 가능한가? | 적용 |
|------|----------|------|
| HY 급등 → 내일 하락 예측 | **불가** (mean reversion) | 미적용 |
| HY 급등 → 체제 변화 감지 | 제한적 (4% 돌파만) | Q4 경고 |
| HY 급락(해빙) → 매수 기회 | **강력** (60일 +4~12%) | 💎 해빙 신호 4가지 |
| HY 수준 → 시장 체제 | **확실** (30년 검증) | 매일 분면 표시 |
| HY 지속 기간 → 리스크 수준 | **확실** (Q4 61일+ = -2.38%) | 현금 비중 단계적 조절 |

### 추가 분석 후보 (미실행)
1. 스프레드 변동성 (HY 일일변화의 20일 std)
2. HY vs VIX 괴리 (HY↑ VIX 안변 = 숨은 위험)
3. 가속도 (2차 미분) — 위기 안정화 조기 감지
4. 퍼센타일 랭크 (롤링) — 고정 임계치 대신 시대 적응
5. HY/IG 비율, CCC-BB 차이 — 진짜 신용위기 구분

---

## v28: 전략-코드 정합성 감사 (Strategy-Code Alignment Audit)

### 핵심 철학
"좋은 종목을 싸게 사되 현재 시장 위험을 판단해서 베타 위험을 회피하자. 반대로 베타의 기회도 확실히 잡으려고."

### 발견된 문제 & 수정

#### 1. final_action이 concordance 무시 (v27에서 수정)
- **문제**: `final_action = hy['action']` 고정 → VIX 보조지표 무의미
- **수정**: concordance(both_warn/hy_only/vix_only/both_stable) × quadrant 조합별 행동 권장 메시지

#### 2. 포트폴리오가 시장 위험 완전 무시 (🔴 Critical)
- **문제**: `run_portfolio_recommendation()`에 `risk_status` 미전달
  - [1/4]에서 "현금 40% 확보하세요" 하면서 [4/4]에서 "각 20% 균등 투자" → 고객 혼란
  - `invest_pct = 100 // n` 고정 → 시장 상태와 무관한 비중
- **수정**:
  - `risk_status` 파라미터 추가, `main()`에서 전달
  - `invest_pct = 100 - final_cash_pct` → 비중에 시장 위험 반영
  - 예: final_cash=40% → 5종목 각 12%, Q1+both_stable → 5종목 각 20%
  - 비중 한눈에 보기에 "현금 N%" 추가
  - "🛡️ 시장 위험 반영" 라인 표시
  - 활용법에 현금 비중 안내

#### 3. Gemini 포트폴리오 프롬프트에 시장 컨텍스트 없음 (🟡)
- **문제**: AI가 종목 선정 이유만 쓰고 시장 위험 언급 불가
- **수정**: `[시장 위험 상태]` 섹션 추가 (HY/VIX/concordance/final_action)

#### 4. HY 실패 시 VIX cash_adjustment 무시 (🟡)
- **문제**: `else` 블록에서 `final_cash = 20` 고정, VIX 조정 미적용
- **수정**: `vix_adj = vix['cash_adjustment']` 적용, `max(0, min(70, base_cash + vix_adj))`

### 검증 완료 (문제 없음)
- both_stable + Q4 조합 불가능 (Q4 → hy_dir='warn') → dead code, 방어적 코드로 유지
- 리스크 필터 일관성: AI 점검(line 1590)과 포트폴리오(line 1818) 동일 기준
- 관망 메시지: ✅ 종목 없으면 정상 출력

---

## v29: 고객 친화적 7대 개선 — 시장 위험↔포트폴리오 완전 연동

### 핵심: 고객이 메시지 보고 바로 행동할 수 있게

#### 1. 결론 멘트 concordance 종합 → v28에서 완료

#### 2. 현금 비중 → 종목 비중 자동 조절 (종목 수 5개 고정)
- 종목 선정(알파) = 항상 Top 5 고정, 비중 조절(베타) = 시장 위험 반영
- `weight = invest_pct // 5` (cash 0%→각20%, 20%→각16%, 40%→각12%, 70%→각6%)

#### 3. AI 브리핑에 시장 환경 전달
- `run_ai_analysis()`에 `risk_status` 전달
- Gemini 프롬프트에 `[현재 시장 환경]` 섹션 추가
- "지금 시장이 공격적 투자에 적합한지 방어적으로 가야 하는지 한마디 덧붙여줘"

#### 4. 투자 가이드에 현금 비중 안내
- `🛡️ 시장 위험은요?` 섹션 추가
- 봄~여름 = 적극 투자, 가을~겨울 = 현금 비중 UP

#### 5. 이탈 종목 사유 표시
- Part 2 필터 중 어떤 조건에서 탈락했는지 태그 표시
- `[MA60↓]` `[괴리+]` `[점수↓]` `[EPS↓]` `[순위↓]`
- 고객이 왜 빠졌는지 한눈에 파악

#### 6. Q1 봄 + 전지표 안정 → 💎 기회 강조
- [1/4] 시장 현황 + [4/4] 포트폴리오 양쪽에 표시
- "💎 역사적 매수 기회! 모든 지표가 매수를 가리켜요."

#### 7. 이탈 경보 시장 위험 차등
- Q4 겨울: 🚨 "즉시 매도하세요"
- both_warn: 🚨 "빠르게 매도하세요"
- Q3 가을: ⚠️ "매도를 적극 검토하세요"
- Q1/Q2: 📉 "매도를 검토하세요"

---

## Phase 19: 신호등 + Concordance 액션 개선 + 비중 고정 (v30)

### 19-1. 신호등(🟢🔴) 도입
**문제**: 한국 프로젝트는 `🟢🟢🟢 3/3 안정 — 확실한 신호`로 시장 위험을 한눈에 보여주는데, US 프로젝트는 텍스트 서술만 있어서 직관성 부족.

**해결**: [1/4] 시장 현황에 concordance 신호등 추가
- US는 2개 지표(HY, VIX): `🟢🟢 2/2 안정 — 확실한 신호`
- 신뢰도 라벨: 확실한 신호 / 엇갈린 신호 / 위험 신호
- 투자 가이드에도 신호등 읽는 법 설명 추가

### 19-2. Concordance final_action 고객 친화 개선
**문제**: 기존 concordance 메시지가 한국 프로젝트 대비 단순 (Q2+both_stable → "평소대로 투자하세요"만)

**해결**: 한국 `_synthesize_action()` 패턴 적용 — 계절 × 지표 조합별 구체적 안내
- Q1+전부안정: "모든 지표가 매수를 가리켜요. 적극 투자하세요!"
- Q2+전부안정: "모든 지표가 안정적이에요. 평소대로 투자하세요."
- Q2+VIX위험: "신용시장은 안정적이지만 VIX가 높아요. 신규 매수 시 신중하세요."
- Q4+전부위험: "모든 지표가 위험해요. 신규 매수를 멈추고 현금을 확보하세요."
- Q4+VIX안정: "신용시장이 악화 중이지만 변동성은 안정적이에요. 현금 비중을 유지하며 지켜보세요."

### 19-3. 포트폴리오 비중 항상 20% 고정
**문제**: `invest_pct(80) // 5 = 16%`로 비중이 계산되어 16%씩 표시 — 비효율적이고 혼란
**해결**: 종목 비중은 항상 20%씩 균등, 현금 비중은 별도 권고 문구로 분리
- 비중 한눈에 보기: `SNDK 20% · NVDA 20% · ...`
- 현금 권고: `🛡️ 시장 위험 권고: 현금 20% 보유 추천`

### 19-4. 가이드 UI 개선
- 신호등/계절 설명 구분: 빈 줄로 시각적 분리
- [1/4] VIX↔신호등 사이 구분선(─────) 추가
- 가이드 헤더: `🛡️ 시장 위험은요?` → `🌡️ 시장 위험 신호 읽는 법` (한국 프로젝트 통일)
- 지표 라벨 명시: `순서대로 🏦신용(HY) · ⚡변동성(VIX)`

### 19-5. 한국 프로젝트 가이드 개선
- 신호등 설명에 각 동그라미가 상징하는 지표 추가
- `순서대로 🏦신용(HY) · 🇰🇷한국(BBB-) · ⚡변동성(VIX)`
- 불필요한 `추천은 항상 5종목` 문구 제거

---

## v31 — Balanced Review 종합 개선 (2026-02-18)

8대 전략 개선 과제 검토 후 6개 구현, 2개 보류, 1개 현상 유지.

### 20-1. Buy/Hold 버퍼존 (Entry 20 / Exit 35)
**문제**: Top 30 경계에서 매일 진입/퇴출 반복 → 불필요한 매매, 고객 혼란
**해결**: 비대칭 진입/퇴출 임계값
- **진입**: composite Top 20 이내일 때만 새로 진입
- **유지**: 21~35위는 어제 리스트에 있었던 종목만 유지 (버퍼존)
- **퇴출**: Top 35 밖으로 떨어지면 이탈
- `save_part2_ranks()` 완전 재작성, 모든 `part2_rank <= 30` → `<= 35`
- 예상 효과: 40~60% 불필요 매매 감소

### 20-2. 매출 10% 하드 필터 제거
**문제**: rev_growth ≥ 10% 하드 필터가 30% 가중치인 매출에 거부권 부여 → 70% 가중치(adj_gap) 무력화
**사례**: LUV(7.4%), ALB, AA, FCX 등 강한 EPS 모멘텀 종목 46개 차단
**해결**: 하드 필터 제거, rev_growth NA → 0으로 fill (composite에서 매출 페널티만 적용)
- 기존 안전장치(인덱스 유니버스, price≥$10, analysts≥3)로 잡주 차단 충분
- 순환매 장세에서 전통 산업 종목 포착 가능

### 20-3. VIX 252일 퍼센타일 전환
**문제**: 고정 임계값(12/20/25/35)은 시장 레짐 변화에 적응 못함 (2017년 VIX 9~12, 2020년 20~80)
**해결**: 252일 rolling percentile로 자동 적응
- < 10th: 안일(complacency)
- 10~67th: 정상(normal)
- 67~80th: 경계/안정화 (slope에 따라 분기)
- 80~90th: 상승경보/높지만안정 (slope에 따라 분기)
- ≥ 90th: 위기/공포완화 (slope에 따라 분기)
- 모든 메시지/AI 프롬프트에 "(1년 중 Nth)" 퍼센타일 표시 추가
- `crisis_relief`(≥90th falling)도 concordance direction='warn' 유지

### 20-4. L3 시장 동결 (L2 미구현)
**L3**: concordance='both_warn'일 때 비검증(🆕⏳) 종목 포트폴리오 제외, 기존 ✅만 유지
**L2 어닝 반감**: 미구현 (사용자 확인 후 제외)

### 20-5. 현금 비중 % 제거 → 행동 등급(final_action)만 유지
**문제**: 현금 비중 %를 계산해도 포트폴리오(5종목×20%)에 실제 반영 안 됨. 숫자만 있고 행동 없는 상태.
**해결**:
- `get_market_risk_status()`에서 현금 계산 로직 전체 제거
- `final_cash_pct`, `invest_pct` 변수 완전 삭제
- [1/4] 시장 현황: `💰 투자 X% + 현금 Y%` 라인 삭제
- [4/4] 포트폴리오: `🛡️ 시장 위험 권고: 현금 X%` 삭제
- AI 프롬프트: 현금 % 참조 제거
- 가이드: "현금 비중 UP" → "매수 중단, 보유 점검" / "매수 줄이기, 보유 점검"
- dead code 정리: Q1 else(도달불가), Q4 중복 분기 제거

### 20-6. final_action: Q × VIX × q_days (14케이스, 30년 EDA 기반)

**핵심 통찰**: Q4가 오래 지속될수록 바닥에 가까움. Q4 후기(>60d)는 Q1 수준의 양수 수익률.
Q4→Q1 전환(250일 +8~12%)을 잡으려면 Q1 전환 전에 포지션 필요. "사전 포석" 개념.

| # | 계절 | q_days | VIX | 행동 | EDA 근거 |
|---|------|--------|-----|------|----------|
| 1 | Q1 | - | ok | 적극 투자하세요! | 연율+14.3%, 양수86% |
| 2 | Q1 | - | warn | 반등 기회일 수 있어요. 적극 투자! | VIX40+역설: 6개월 85%상승 |
| 3 | Q2 | - | ok | 평소대로 투자하세요. | 연율+9.4% |
| 4 | Q2 | - | warn | 신규 매수 시 신중하세요. | |
| 5 | Q3 | <60d | ok | 과열 초기. 신규 매수 시 신중. | 60일+1.84% |
| 6 | Q3 | <60d | warn | 과열 초기 + 변동성. 매수 멈추기. | |
| 7 | Q3 | ≥60d | ok | 과열 지속. 매수 줄이기. | 60일+0.39% |
| 8 | Q3 | ≥60d | warn | 과열 장기화. 점검 + 매수 중단. | |
| 9 | Q4 | ≤20d | ok | 급매도 금물, 관망. | 초기 약세, 반등가능 |
| 10 | Q4 | ≤20d | warn | 급매도 금물, 지켜보기. | VIX40+역설 |
| 11 | Q4 | 21~60d | ok | 매수 멈추고 관망. | 턴어라운드 시작 |
| 12 | Q4 | 21~60d | warn | 보유 줄여가기. | |
| 13 | Q4 | >60d | ok | **바닥권 접근. 분할 매수 고려.** | 60일+1.5~3.5%, Q1수준 |
| 14 | Q4 | >60d | warn | 바닥 가능. 관망, 회복 대기. | |

**v24 데이터 수정**: Q4 61+ = -2.38%는 구간 미분리 결과. 61~120일(+1.5~2.5%)과 121일+(+2.5~3.5%)을 분리하면 양수.

### 20-7. Forward Test 트래커
**목적**: 매일 포트폴리오 enter/hold/exit 기록 → 미래 성과 검증용 (t-stat ≥ 3.0)
- `portfolio_log` 테이블 신설 (date, ticker, action, price, weight, entry_date/price, exit_price, return_pct)
- `log_portfolio_trades()` 함수: 어제 포트폴리오 대비 변동 자동 판별
- 퇴출 시 수익률 자동 계산, 로그 출력

### 20-8. 자본 배분 가이드
**추가**: 가이드 메시지에 "전체 투자 자산의 20~30%만 이 전략에 적용, 나머지는 VTI 분산" 권고

### 20-9. 텔레그램 메시지 UI 개선

#### [1/4] 시장 현황 — 구분선 압축
**문제**: HY/VIX/신호등/주도업종이 각각 구분선(─)으로 분리되어 6개 구분선 → 서로 다른 영역처럼 보임
**해결**:
- HY + VIX + 신호등 + 액션을 **하나의 블록**으로 압축 (구분선 1개만)
- q_days 표시 추가: "🛡️ 시장 위험 — ☀️ 여름(성장국면) 45일째"
- 주도업종 코드 제거 → [2/4]로 이동
- 미사용 코드 정리 (Counter import, filtered 변수, import pandas)

#### [2/4] 매수 후보 — 주도업종 추가 + Death List q_days 반영
- **주도업종**: [1/4]에서 이동, "읽는 법" 아래 종목 리스트 전에 "📊 주도 업종: 업종1 N · 업종2 N" 표시
- **Death List 매도 경보**: q_days 반영
  - Q4 ≤20d: "침체 초기, 급매도는 금물"
  - Q4 21~60d: "침체 지속, 매도 검토"
  - Q4 >60d: "바닥권, 이탈 종목은 매도 검토하되 시장 반등에 대비"
  - (기존: Q4 일괄 "즉시 매도" → 바닥에서 파는 문제 수정)

#### 가이드 메시지 — 겨울 후기 = 매수 기회 반영
- 기존: "🍂가을~❄️겨울 = 매수 줄이기, 보유 점검" (1줄)
- 변경: "🍂가을 = 신중하게, 줄여가기 / ❄️겨울 초기 = 관망, 급매도 금물 / ❄️겨울 오래가면 = 바닥 접근, 매수 기회" (3줄)

### 20-10. fetch_hy_quadrant() 정리
- 미사용 `cash_pct` 제거 (현금 비중 % 시스템 완전 제거)
- `action` 필드: 14케이스 final_action의 fallback용으로 단순화 (Q별 1줄)

### 20-11. AI 프롬프트 q_days 반영
- 포트폴리오 프롬프트: "HY Spread: 2.84% (여름(성장국면), 45일째)"
- AI 리스크 프롬프트: "신용시장: HY Spread 2.84% · 여름(성장국면) (45일째)"

### 보류 항목
- **FMP 데이터 전환**: $59/월 비용 → 현재 yfinance로 충분, 향후 재검토
- **EPS 시그널 개선**: 현재 4-segment 구조 충분, 향후 Forward Test 결과 기반 재검토
- **섹터 캡**: 현재 업종 분산 없음 유지 (모멘텀 시스템에 학술적으로 적합)
