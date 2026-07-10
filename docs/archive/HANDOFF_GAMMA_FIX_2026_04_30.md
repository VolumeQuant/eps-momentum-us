# Gamma Fix (v80.3) — 2026-04-30 작업 핸드오프

**작성**: 2026-04-30 (회사PC)
**다음 작업자**: 집PC의 클로드코드 (자기 자신)
**상태**: v80.3 코드+DB+문서 적용 완료, 테스트 워크플로우 진행 중

---

## 🚨 현재 상태 (즉시 인지)

### ✅ 완료
1. **코드 변경**: γ (segment cap 발동 시 direction 무효화) 적용
   - `eps_momentum_system.py`: `calculate_ntm_score`에 cap-aware direction
   - `daily_runner.py`: line 665 + line 791 두 곳 (adj_gap 계산 위치)
2. **DB 재계산**: 모든 일자(54일) row를 γ 기준으로 재계산
   - 백업: `eps_momentum_data.db.bak_pre_gamma` (16MB)
   - 현재 DB: γ 적용 상태 (eps_test_gamma.db에서 복사)
   - baseline 96% Top 3 일치 (BT 검증)
3. **메인 워크플로우 enable**: `Daily EPS Momentum Screening` active
4. **문서 업데이트**: README v80.3 / SESSION_HANDOFF v80.3 / MEMORY.md
5. **커밋 푸시**: def3b4d (코드+DB) + be4a2e5 (문서)

### 🔄 진행 중
- **테스트 워크플로우 trigger**: Run ID `25156055906` (test-private-only.yml)
- 시작: 2026-04-30 08:43 UTC
- 예상 완료: ~7분 후
- 백그라운드 watching ID: b8n0xdcx3

### 📋 집PC 도착 후 첫 단계
```bash
cd "C:\dev\claude code\eps-momentum-us"
git pull
gh run view 25156055906 --json status,conclusion
# completed/success면 정상. 텔레그램 개인봇 메시지 확인 (4/30 데이터 v80.3 첫 적용)
```

---

## 📌 작업 배경 — 사용자 본질 우려

MU가 4/27 cr=1 → 4/28 cr=13으로 폭락. 그런데 NTM EPS는 거의 안 변함 ($86.23 → $86.37, +0.16%).

사용자: "**입력 안정인데 출력 흔들림 = 시스템 결함**"

이게 진짜 결함인지, 의도된 동작인지, 아니면 부작용인지 진단 필요.

---

## 🔍 진단 — 두 차원으로 분리

### 차원 A — Lookback shift는 시스템 의도 (95%, fix 불가)
- 3/18 MU 어닝(+33.2% 서프라이즈)이 NTM 컨센서스를 $52→$80로 폭등
- yfinance 30daysAgo snapshot은 **trading days 기준**
- 4/27이 본 30d ago = 3/16경 (어닝 **전**), ntm_30d=$52
- 4/28이 본 30d ago = 3/17경 (어닝 **후**), ntm_30d=$81
- 단 하루 차이로 lookback이 어닝 가로지름 → fwd_pe_chg 음→양 부호 반전 (-8.58 → +1.14)
- 이게 cr 1 → 13 폭락의 95% 설명
- **시스템이 의도한 측정** (어닝 효과 fwd_pe_chg에 반영하려는 디자인). fix하면 시스템 의미 사라짐.

### 차원 B — Segment cap 부작용 (5%, fix 가능)
- 4/27 segments: [+3.28, +59.35, +35.20, +7.12], direction=+10.15
- 4/28 segments: [+1.00, +5.47, **+100(cap)**, +5.91], direction=**-49.72** (부호 반전!)
- 한 segment가 cap에 걸리면서 group 평균(recent vs old)을 통째로 흔듦
- 결과: dir_factor -0.30 → adj_score 136 → 78 (-43% 폭락)
- **이건 의도와 무관한 노이즈**. 곱셈식 비대칭 부작용.

---

## 🛠 시도한 fix들

| 변형 | 설명 | 매매 BT 수익 | MDD | Sharpe | 결정 |
|------|------|--------------|-----|--------|------|
| baseline | 현재 production | +57.43% | -15.34% | 4.14 | 기준 |
| **γ** | cap 발동 시 dir_factor=0 | **+60.51%** | -15.84% | **4.20** | ⭐ **채택** |
| γ'' | cap segment 제외 후 partial direction | +59.23% | -15.34% | 4.17 | 차선 |
| δ | dir_factor를 adj_gap에서 제거 | +51.32% | -18.63% | 3.66 | ❌ 폐기 |
| ζ | DB calendar lookup으로 NTM 시계열 사용 | (시도 안 함) | - | - | ❌ 폐기 |

### 폐기 이유
- **δ**: dir_factor 제거가 진짜 알파 손상. 매매 BT -6.11%p 명확.
- **ζ (DB calendar lookup)**: 사용자 지적 — "그건 그 당시 추정치가 아니라 미래 추정치." 4/27이 한 달 전 보면 calendar로 3/27인데, 3/27 row의 ntm_current는 어닝 후 값. "4/27 시점에서 30일 전(3/16) 시장이 봤어야 하는 어닝 전 NTM"이 아니라 "어닝 발표 후 흡수된 NTM" 사용 → 시점 misalignment. yfinance trading 30d snapshot이 정확한 의미 가짐.

### γ vs γ'' 결정
- 매매 BT γ가 +1.29%p 더 좋음 (단 표본 노이즈 범위 내일 수 있음)
- γ 코드 단순 (한 줄), γ'' partial direction 처리 + edge case 많음
- 직관 명확: γ "cap 발동 = 노이즈 = 가속도 측정 신뢰 못함"
- γ' Sharpe 4.20 vs γ'' 4.17, 거의 동일하지만 γ 우세
- **데이터 우선 → γ 채택**

---

## 💡 사용자 직관 vs 시스템 동작

사용자 핵심 질문: "adj_score 78→112 회복했는데 왜 cr 14→15로 후순위?"

답: cr은 **adj_gap ascending sort**로 결정. adj_score는 표시용. cr 결정 식:
```
adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_q
```

MU 4/28: fwd_pe_chg=+1.14 (양수, 가격이 NTM보다 빠르게 오름). 이게 양수인 한 cr은 음수 adj_gap 종목들 뒤로 밀림.

baseline cr=14 자체가 사실 dir_factor의 곱셈 비대칭으로 인공적으로 한 칸 앞당겨진 결과:
- baseline: +1.14 × 0.7 × 1.15 = **+0.92** (양수가 작음 → 앞순위)
- γ: +1.14 × 1.0 × 1.15 = **+1.32** (양수 그대로 → 후순위)

**γ가 cr=15로 한 단계 후순위로 보내는 게 사실 더 정직한 동작**. baseline 14는 곱셈식 부작용으로 인공 보정된 것.

→ **fix 후에도 cr 폭락은 거의 그대로** (1단위 변화). 어닝 효과가 fwd_pe_chg에 직접 반영되는 건 차원 A (의도)라 못 고침. 4/30 이후 (어닝 + 30 trading days) 자연 정상화 예상.

---

## 📁 변경된 파일

### 코드
- `eps_momentum_system.py`: `calculate_ntm_score`의 direction 계산을 cap-aware로
- `daily_runner.py`:
  - line 665 (주 pipeline의 adj_gap 계산): min_seg cap 제외
  - line 791 (부 pipeline의 adj_gap 계산): min_seg cap 제외
- `backtest_v3.py`: conv_base 시그니처에 rev_growth 추가 (이전 결함 수정)

### 데이터
- `eps_momentum_data.db`: γ로 재계산 (54일 × ~1200종목)
- `eps_momentum_data.db.bak_pre_gamma`: γ 적용 전 백업 (16MB, gitignore 안 됨)

### 문서
- `README.md`: 헤더 v80.3, 헤드라인, 변경 이력
- `SESSION_HANDOFF.md`: v80.3 단락 추가
- `MEMORY.md`: v80.3 라인 추가
- `~/.claude/projects/...memory/feedback_db_first.md`: "DB 구조 먼저 살펴보기" 피드백
- `~/.claude/projects/...memory/feedback_clear_explanation.md`: "분석 결과 명료한 설명" 피드백

### BT 인프라 (research/)
- `bt_segment_fix.py`: γ/γ''/δ 변형 BT 인프라 (DB row 사용)
- `bt_pnl.py`: 매매 BT (Top 3 균등비중, T+1 종가 기준)
- `bt_segfix_db_only.py`: ζ 시도 (폐기됨, 참고용)
- `bt_segfix_verify.py`: BT 결과 검증 (verify 코드 자체에 yfinance 호출한 게 처음 헛돌이 원인)
- `bt_min_analysts.py`: 저커버리지 필터 BT (별건, v80.2 결정 근거)
- `bt_slot_replace.py`: 슬롯 대체 BT (v80.2 결정 근거)

### 메시지 인프라 (작업 결과 텔레그램 발송용)
- `scripts/send_handoff.py`: md 파일 → 텔레그램 발송
- `.github/workflows/send-handoff.yml`: workflow_dispatch 트리거
- `research/handoff_message.md`: 텔레그램 발송 메시지 (마지막 버전 = 초등학생 수준)

---

## 🧠 헛돌이 교훈 (집PC에서 같은 실수 안 하기)

1. **DB 구조 먼저 확인**. 이번엔 verify에서 yfinance 호출하다가 사용자 시간 낭비. ntm_screening 테이블에 매일 raw 데이터 다 있는데 외부 호출이 불필요. → `feedback_db_first.md` 메모리 추가됨.

2. **차원 분리**. 처음엔 segment 결함과 fwd_pe_chg lookback shift를 한 덩어리로 봤음. 사용자가 "어닝 효과는 의도, segment cap은 부작용" 짚어줘서 깨달음. fix는 부작용 차원만 가능.

3. **분석 결과 명료한 설명**. 사용자가 "쉽게 다시 설명해" 여러 번 요청. 압축된 표·약어보다 비유와 단계별 풀어쓰기가 의사소통에 더 효과적. → `feedback_clear_explanation.md` 메모리 추가됨.

4. **사용자 직관 vs 데이터**. γ vs γ'' 결정에서 사용자가 "안간힘 써서 넣은" 가속도 알파를 살리고 싶어했지만, 매매 BT는 γ(완전 차단)가 +1.29%p 우위. 데이터 우선.

5. **시점 정렬 (시계열 분석 일반)**. ζ 시도가 잘못된 이유 — DB calendar lookup으로 30d ago row 가져오면 그날 측정한 NTM은 "그 당시 시장이 봤던 NTM"인데, 어닝 발표일을 lookback에 쓰면 "어닝 후 NTM을 어닝 전 시점 lookback baseline으로 사용" = 미래 정보 leakage. yfinance snapshot이 정확한 시점 정렬 가짐.

---

## 🔍 미해결 / 향후 검토

### 단기 (4/30~5/2)
- [ ] 테스트 워크플로우 (Run 25156055906) 결과 확인. 4/30 데이터 v80.3 첫 적용. 텔레그램 메시지 확인.
- [ ] 메인 워크플로우 다음 실행 (KST 05:58, 평일) 정상 동작 확인.
- [ ] 4/30 cr 1~10 종목들이 baseline DB 결과와 어떻게 다른지 비교 (특히 cap 발동 케이스).

### 중기 (5월 첫째주)
- [ ] MU의 어닝 + 30 trading days 통과 시점(약 4/30~5/1) 이후 cr이 정상화되는지 모니터링.
- [ ] 다른 어닝 종목들에서 같은 lookback shift 패턴 발생 여부 추적.
- [ ] 3개월 누적 BT 가능 시점(데이터 90일 누적, 5/10 이후)에 더 큰 표본으로 γ 효과 재검증.

### 장기 (1~3개월)
- [ ] **시스템 디자인 자체 변경 (차원 A fix)**: cr이 가격 변동에 즉각 반응하는 게 시스템 디자인의 자연 결과. 사용자 직관(점수 좋음 = 순위 좋음)을 만족시키려면 cr 정렬 식에 score 정보 명시적 반영 필요. 예: `adj_gap = fwd_pe_chg × eps_q - α × score`. 큰 변경, BT 다시 필요.
- [ ] BT 인프라 정확도 향상: bt_segment_fix.py가 baseline 96% Top 3 일치. 100% 정확하게 하려면 conv_base 호출 시 rev_growth 정확히 전달 + 기타 detail. 현재는 "충분히 정확" 수준.

---

## 🎯 집PC 도착 후 우선순위

1. **`git pull`** — 최신 변경 (def3b4d, be4a2e5) 받음
2. **테스트 워크플로우 결과 확인**:
   ```bash
   gh run view 25156055906 --json status,conclusion
   gh run view 25156055906 --log 2>&1 | grep -E "디스플레이|w_gap|Signal|ERROR"
   ```
3. **개인봇 텔레그램** 확인 — 4/30 데이터 v80.3 첫 적용 메시지
4. **메인 워크플로우 다음 실행 모니터링**: 평일 KST 05:58 (UTC 20:58) cron
5. **이상 없으면 종료**. 이상 있으면 위 미해결 단기 리스트 참고.

---

## 📞 텔레그램 발송 인프라 활용

핸드오프 메시지를 텔레그램 개인봇으로 보내려면:
```bash
# research/handoff_message.md 수정 후
git add research/handoff_message.md
git commit -m "..."
git push
gh workflow run send-handoff.yml --ref master
```

(이번 세션에서 핸드오프 메시지 3번 보냄: 처음 → 짧게 → 초등학생 수준. 마지막 버전이 가장 알기 쉬움.)

---

## 💾 핵심 데이터 (참고)

### MU 4/27→4/28 비교 (γ 적용 후)
| 날짜 | cr | p2 | adj_gap | adj_score | direction |
|------|-----|-----|---------|-----------|-----------|
| 4/27 | 1 | 1 | -14.50 | 136.45 | +10.15 (cap 안 걸림) |
| 4/28 | 15 | 1 | +1.32 | 112.38 | 0 (cap 발동으로 무효화) |

### 매매 BT 결과 (재게)
- baseline +57.43% / MDD -15.34% / Sharpe 4.14
- **γ +60.51% / MDD -15.84% / Sharpe 4.20** (채택)
- γ'' +59.23% / MDD -15.34% / Sharpe 4.17
- δ +51.32% / MDD -18.63% / Sharpe 3.66 (폐기)

### 4/28 cr 1~10 (γ 적용)
1 LRCX (-7.87) / 2 ASML (-6.60) / 3 LITE (-4.71) / 4 FIVE (-2.03) / 5 LNG (-2.50) /
6 AGX (-1.88) / 7 FIX (-1.66) / 8 SNDK (-1.10) / 9 TPR (-1.27) / 10 HWM (-1.25)

MU = cr 15 (adj_gap +1.32)

---

## ⚠️ 안전 원칙 (반드시 준수)

1. **production DB 직접 수정 금지** — 변경 시 항상 백업 + BT 검증 후
2. **메인 워크플로우 disable 시 신중하게 enable**
3. **DB 재계산은 BT 인프라 통과한 결과만 적용**
4. **사용자 직관과 데이터가 충돌하면 데이터 우선이지만, 사용자 디자인 의도(예: "안간힘 써서 넣은 알파")는 가능한 한 살릴 방향 모색**
