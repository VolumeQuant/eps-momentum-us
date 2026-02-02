# SESSION_HANDOFF.md
## EPS Momentum System v6.1 - Option A (가격위치 반영)

**Last Updated:** 2026-02-03 06:58
**Session:** v6.0 → v6.1 업그레이드 완료

---

## 1. 작업 요약

### v6.1 핵심 변경사항 (Option A)

**문제 인식 (v6.0):**
- Hybrid Score가 Momentum과 98% 상관관계
- Value 컴포넌트(100/PER)가 전체 점수의 19.4%만 기여
- 결과적으로 "S등급 비싼 사과"만 선별됨 (대부분 52주 고점 근처)

**해결책 - Option A:**
```
기존 (v6.0): Hybrid = Momentum × 0.7 + Value × 0.3
신규 (v6.1): Hybrid = Momentum × 0.5 + Value × 0.2 + Position × 0.3
```

**Position Score 공식:**
```python
Position Score = 100 - (현재가 / 52주고점 × 100)
# 점수 범위: 0~50
```
- 고점 근처 (99%): 1점 (비쌈)
- 조정 (-20%): 20점 (적당)
- 큰 조정 (-35%): 35점 (쌈)

**철학 변화:**
> "S등급 비싸게 사기" → "A등급 싸게 사기"

---

## 2. 수정된 파일

### eps_momentum_system.py

**신규 함수 추가:**
```python
def calculate_price_position_score(price, high_52w):
    """52주 고점 대비 가격 위치 점수 계산"""
    if price is None or high_52w is None or high_52w <= 0:
        return None
    position_pct = (price / high_52w) * 100
    score = 100 - position_pct
    score = max(0, min(50, score))  # 0~50 범위 제한
    return round(score, 2)
```

**수정된 함수:**
```python
def calculate_hybrid_score(momentum_score, forward_per, price_position_score=None,
                           weight_momentum=0.5, weight_value=0.2, weight_position=0.3):
    """하이브리드 점수 계산 (v6.1 - Option A)"""
    momentum_component = momentum_score * weight_momentum
    value_score = 100 / forward_per if forward_per and forward_per > 0 else 0
    value_component = value_score * weight_value
    position_component = price_position_score * weight_position if price_position_score else 0
    return round(momentum_component + value_component + position_component, 2)
```

### daily_runner.py

1. **Import 추가:** `calculate_price_position_score`
2. **run_screening():**
   - 52주 고점에서 `high_52w` 계산
   - `price_position_score` 계산
   - `calculate_hybrid_score(score_321, fwd_per, price_position_score)` 호출
3. **run_data_collection():** 동일하게 v6.1 공식 적용
4. **텔레그램 메시지:** v6.1 공식 설명 추가
5. **리포트:** Formula 표시 추가
6. **버전 표기:** v6.0 → v6.1 전체 업데이트

---

## 3. 실행 결과 (2026-02-03)

### Track 1 스크리닝
| 항목 | 값 |
|------|-----|
| 총 스캔 | 917개 |
| 통과 | 49개 (v6.0: 52개 → v6.1: 49개) |
| Kill Switch | 114개 |
| ROE 10% 미만 | 57개 |
| 평균 Forward PER | 23.4 |
| 평균 ROE | 29.1% |

### Track 2 데이터 수집
| 항목 | 값 |
|------|-----|
| 수집 | 917개 |
| 오류 | 0개 |
| 소요시간 | 약 2시간 |

### 순위 변화 (v6.0 → v6.1)

| 순위 | v6.0 | Hybrid | v6.1 | Hybrid | Position |
|------|------|--------|------|--------|----------|
| 1 | MU | 25.9 | MU | 19.7 | 4.6 (고점) |
| 2 | CMC | 14.4 | **MRVL** | 13.7 | **35.7** |
| 3 | F | 10.7 | **G** | 12.8 | **21.5** |
| 4 | NEM | 10.4 | **AVGO** | 12.8 | **20.2** |
| 5 | FIVE | 10.2 | NEM | 12.3 | 16.7 |
| 7 | - | - | CMC | 10.2 | 0.3 (고점) |

**해석:**
- MRVL: 모멘텀 낮지만 큰 조정(-35.7%)으로 #2 등극
- CMC: 고점 근처(-0.3%)로 #2 → #7 하락
- 조정받은 종목들이 상위 랭크로 이동

### 포트폴리오 변동
- **편입:** EXEL (1개)
- **편출:** CBRE, HALO, IDXX, TLN (4개)

---

## 4. 시스템 동작 확인

### 텔레그램 메시지
- User Briefing: 전송 완료 (1개 메시지)
- Admin Log: 전송 완료 (1개 메시지)

### Git
- 자동 commit/push 오류 발생 (수동 실행 필요)
```
[ERROR] Git 오류: Command '['git', 'add', '-A']' returned non-zero exit status 128.
```

---

## 5. 다음 작업

### 필수
- [x] SESSION_HANDOFF.md 업데이트
- [ ] README.md 업데이트 (v6.1 공식 설명)
- [ ] Git commit/push (수동)

### 선택적 개선
- [ ] Position Score 가중치 튜닝 (현재 0.3)
- [ ] Position Score 상한 조정 (현재 50)
- [ ] 백테스트로 v6.0 vs v6.1 성과 비교

---

## 6. 설정 파일 정보

**config.json 위치:** `C:\dev\claude-code\eps-momentum-us\config.json`

주요 설정:
- `telegram_enabled`: true
- `git_enabled`: true
- `min_score`: 4.0
- `indices`: ["NASDAQ_100", "SP500", "SP400_MidCap"]

---

## 7. 디렉토리 구조

```
eps-momentum-us/
├── daily_runner.py          # 메인 실행 파일 (v6.1)
├── eps_momentum_system.py   # 핵심 로직 (v6.1)
├── config.json              # 설정
├── eps_momentum_data.db     # SQLite DB
├── eps_data/
│   └── screening_YYYY-MM-DD.csv
├── reports/
│   ├── report_YYYY-MM-DD.md
│   └── report_YYYY-MM-DD.html
└── logs/
    └── daily_YYYYMMDD.log
```

---

## 8. 트러블슈팅

### Git 오류
```
[ERROR] Git 오류: Command '['git', 'add', '-A']' returned non-zero exit status 128.
```
- 원인: 별도 git 프로세스가 락을 잡고 있거나 권한 문제
- 해결: 수동으로 `git add -A && git commit && git push` 실행

### 텔레그램 HTTP 400
- 원인: HTML 특수문자 (`<`, `>`) 미이스케이프
- 해결: 한국어 텍스트로 대체 (예: `<` → "미만")

---

## 9. 이전 버전 히스토리

### v6.0 (2026-02-02)
- Forward PER, ROE 지표 추가
- 3-Layer Filtering: Momentum → Quality (ROE > 10%) → Safety (PER < 60)
- Hybrid Ranking: Score = (Momentum * 0.7) + ((100/PER) * 0.3)
- 텔레그램 User/Admin 분리

### v5.4 (이전)
- 시장 국면 3단계 진단 시스템 (RED/YELLOW/GREEN + VIX)

---

*작성: Claude Opus 4.5 | 2026-02-03*
