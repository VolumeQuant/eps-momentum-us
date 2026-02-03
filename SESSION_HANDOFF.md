# SESSION_HANDOFF.md
## EPS Momentum System v6.3.2 - Quality & Value Scorecard

**Last Updated:** 2026-02-03 10:30
**Session:** v6.3 -> v6.3.2 업그레이드 완료

---

## 1. 작업 요약

### v6.3.2 핵심 변경사항

#### A. Quality Score - score_321 직접 활용

**문제점 (v6.3.1):**
- 정배열 보너스(20점) + 모멘텀 보너스(10점) = 중복 계산
- 정배열 여부는 이미 score_321에 반영됨 (C>7d:+3, 7d>30d:+2, 30d>60d:+1)

**해결 (v6.3.2):**
```python
# 기존 (중복)
if is_aligned:
    score += 20  # 정배열 보너스
    score += min(10, momentum_score)

# v6.3.2 (직접 활용)
score += min(30, momentum_score)  # score_321 그대로 사용
```

**결과:**
| 종목 | M점수 | v6.3.1 Q | v6.3.2 Q | 변화 |
|------|-------|----------|----------|------|
| MU | 32.6 | 80 | 85 | +5 |
| AVGO | 11.7 | 85 | 67 | -18 |
| NEM | 11.7 | 80 | 62 | -18 |

---

### v6.3 전체 아키텍처

#### Quality Score (맛, 100점)
```
EPS 모멘텀:     30점 - min(30, score_321) 직접 활용
ROE 품질:       25점 - 30%+:25, 20%+:20, 10%+:15
EPS 성장률:     20점 - 20%+:20, 10%+:15, 5%+:10
추세(MA200위):  15점
거래량스파이크: 10점
```

#### Value Score (값, 100점)
```
PEG 평가:       35점 - <1.0:35, <1.5:28, <2.0:20
Forward PER:    25점 - <15:25, <25:20, <40:12
52주 고점대비:  25점 - -25%+:25, -15%+:20, -10%+:15
RSI 눌림목:     15점 - 30-45:15, 45-55:10
```

#### Actionable Score (실전점수)
```
실전점수 = (Quality × 0.5 + Value × 0.5) × Action Multiplier
```

#### Action Multiplier
| Action | Multiplier | 조건 |
|--------|------------|------|
| 🚀강력매수 | ×1.1 | RSI 70-84 + 신고가 + 거래량 |
| 적극매수/저점매수 | ×1.0 | 눌림목/과매도 |
| 매수적기 | ×0.9 | 건강한 추세 |
| 관망 (RSI🚀) | ×0.75 | RSI 70-84 (거래량 미동반) |
| 관망 | ×0.7 | 진입 애매 |
| 진입금지 | ×0.3 | RSI 85+ / 단기급등 |
| 추세이탈 | ×0.1 | MA200 하회 |

---

### RSI Momentum Strategy

**핵심 철학:**
> RSI 70 이상을 무조건 진입금지로 처리하지 않음.
> 신고가 돌파 + 거래량 동반 = Super Momentum

```python
if RSI >= 85:
    return "진입금지 (극과열)"      # ×0.3
elif 70 <= RSI < 85:
    if 신고가근처 and 거래량스파이크:
        return "🚀강력매수 (돌파)"  # ×1.1
    elif 신고가근처:
        return "관망 (RSI🚀고점)"   # ×0.75
    else:
        return "관망 (RSI🚀)"       # ×0.75
```

---

## 2. 실행 결과 (2026-02-03)

### v6.3.2 Top 10
| # | 종목 | M점수 | Q점수 | V점수 | 실전 | Action |
|---|------|-------|-------|-------|------|--------|
| 1 | NEM | 11.7 | 62 | 90 | 75.8 | 적극매수 |
| 2 | AVGO | 11.7 | 67 | 75 | 70.8 | 적극매수 |
| 3 | EXEL | 5.9 | 51 | 65 | 58.0 | 적극매수 |
| 4 | G | 9.1 | 49 | 60 | 54.5 | 저점매수 |
| 5 | MU | 32.6 | 85 | 60 | 54.4 | 관망(RSI🚀) |
| 6 | WMG | 6.5 | 52 | 55 | 53.2 | 적극매수 |
| 7 | LRCX | 13.3 | 73 | 45 | 53.2 | 매수적기 |
| 8 | DRI | 5.0 | 50 | 50 | 50.0 | 적극매수 |
| 9 | CRS | 10.3 | 55 | 42 | 48.6 | 적극매수 |
| 10 | CMC | 16.6 | 67 | 60 | 47.5 | 관망(RSI🚀) |

**해석:**
- MU: 모멘텀 32.6 최고지만 RSI 75 과열 → ×0.75 페널티 → 5위
- NEM: 모멘텀 11.7 + Value 90 (떨이세일) → 1위
- AVGO: 모멘텀 11.7 + Value 75 (저평가) → 2위

---

## 3. 수정된 파일

### eps_momentum_system.py
```python
def calculate_quality_score(..., momentum_score=None):
    # v6.3.2: score_321 직접 활용 (정배열 보너스 중복 제거)
    if momentum_score is not None and momentum_score > 0:
        score += min(30, momentum_score)
```

### daily_runner.py
- v6.3 스코어카드 텔레그램 포맷
- RSI Momentum Strategy (get_action_label)
- 거래량 스파이크/실적 D-Day/Fake Bottom 감지

---

## 4. score_321 계산 로직 (참고)

```python
def calculate_momentum_score_v3(current, d7, d30, d60):
    score = 0

    # 가중치 기반 (최근일수록 높은 점수)
    if current > d7:   score += 3  # 최신
    if d7 > d30:       score += 2
    if d30 > d60:      score += 1

    # 역전 페널티
    if d7 < d30:       score -= 1
    if d30 < d60:      score -= 1

    # 60일 변화율 보너스
    score += eps_chg_60d / 5

    # 정배열 보너스
    if current > d7 > d30 > d60:
        score += 3  # 완전 정배열
    elif current > d7 > d30:
        score += 1  # 부분 정배열

    return score
```

**예시:**
- 완전 정배열 + 60일 변화율 20% = 3+2+1+4+3 = **13점**
- 부분 정배열 + 60일 변화율 100% = 3+2+0+20+1 = **26점**

---

## 5. 버전 히스토리

### v6.3.2 (2026-02-03 10:30)
- Quality Score: score_321 직접 활용 (정배열 중복 제거)

### v6.3.1 (2026-02-03 10:20)
- Quality Score: 정배열 + 모멘텀 강도 조합 (중복 문제 발견)

### v6.3 (2026-02-03 08:15)
- Quality & Value Scorecard 분리
- RSI Momentum Strategy
- 거래량 스파이크 / 실적 D-Day / Fake Bottom

### v6.2 (2026-02-03)
- Action Multiplier 도입

### v6.1 (2026-02-03)
- 가격위치 점수 (52주 고점 대비)

### v6.0 (2026-02-02)
- Forward PER, ROE 지표 추가
- 3-Layer Filtering

---

## 6. 다음 작업

### 선택적 개선
- [ ] 백테스트: v6.2 vs v6.3.2 수익률 비교
- [ ] Quality/Value 가중치 튜닝 (현재 50:50)
- [ ] 거래량 스파이크 임계값 조정 (현재 1.5x)

---

## 7. 설정 파일

**config.json:**
```json
{
  "telegram_enabled": true,
  "git_enabled": true,
  "min_score": 4.0,
  "indices": ["NASDAQ_100", "SP500", "SP400_MidCap"]
}
```

---

*작성: Claude Opus 4.5 | 2026-02-03*
