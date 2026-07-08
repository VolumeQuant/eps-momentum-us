# PIT(Point-In-Time) 추정치 아카이빙 설계서 (2026-07-09)

★ 이 문서는 설계·검증 결과만 담는다. **production 코드(daily_runner.py)·production DB(eps_momentum_data.db)·워크플로우(.github/workflows/*)는 전혀 수정하지 않았다.** 초안 수집기는 `research/pit_archive_draft_2026_07_09.py` (독립 실행, 별도 출력 경로).

## 0. 결론 먼저

- **추가로 아카이빙할 가치가 있는 필드는 이미 매일 공짜로 받고 있다.** `stock.eps_trend`를 호출하면 yfinance는 내부적으로 `_analysis._earnings_trend` 하나(quoteSummary `earningsTrend` 모듈, HTTP 1회)를 채우는데, 여기에 `earnings_estimate`/`revenue_estimate`/`eps_revisions`/분기(`0q`/`+1q`) 데이터가 전부 같이 들어있다. daily_runner는 이 중 NTM 블렌딩에 쓴 값(0y/+1y의 current~90d)과 `rev_up30`/`rev_down30`/`num_analysts`(0y/+1y 중 max값)만 뽑아 쓰고 **나머지(약 90개 필드)는 그냥 버린다.**
- 그러므로 "PIT 아카이빙"의 **추가 API 비용은 0회/종목**(같은 호출 재사용) — 단, **애널리스트 등급(recommendations)만은 별도 HTTP 호출**이 필요(+1회/종목, 실측 아래).
- 저장 용량은 실측(30종목 2475B/종목 → 300종목 726B/종목, parquet 오버헤드가 표본 클수록 상각됨)으로 **전종목(~1300) 기준 하루 ~900KB~1MB, 연 ~230MB(recommendations 제외)** 로 추정. 이미 32MB인 production DB보다 빠르게 repo를 불릴 수 있어 **메인 repo에 매일 커밋하는 방식은 비권장** — 아래 3절 참조.

---

## 1. 필드 조사 — 뭘 이미 가지고 있고, 뭘 새로 archiving해야 하나

### 1-1. 현재 daily_runner.py가 실제로 쓰는 것 (이미 있음)

| 데이터 | 소스 | 현재 사용처 | DB 컬럼 |
|---|---|---|---|
| eps_trend 5스냅샷(current/7d/30d/60d/90d) × (0y,+1y) | `stock.eps_trend` → `calculate_ntm_eps()` | NTM 블렌딩(fiscal weight) | `ntm_current/7d/30d/60d/90d` |
| epsRevisions.upLast30days/downLast30days (0y,+1y 중 **max**) | `raw_trend` (`stock._analysis._earnings_trend`) | 합의강도 필터(rev_up30≥3) | `rev_up30`, `rev_down30` |
| earningsEstimate.numberOfAnalysts (0y,+1y 중 **max**) | 〃 | (표시용) | `num_analysts` |

→ **이 호출(`stock.eps_trend` 1회)이 raw_trend 전체를 이미 메모리에 들고 있다.** `daily_runner.py:602-621`의 `_prefetch_eps` 워커가 정확히 이 패턴(eps_trend 먼저 트리거 → `_analysis._earnings_trend` 캐시 히트로 raw 추출)을 쓰고 있어, 아카이버는 동일 패턴만 복붙하면 된다.

### 1-2. raw_trend에 있지만 현재 버려지는 것 (신규 아카이빙 대상, 비용 0)

`stock._analysis._earnings_trend`는 리스트[dict], 각 item(기간: `0q`,`+1q`,`0y`,`+1y`)마다:

```
{maxAge, period, endDate, growth,
 earningsEstimate {avg, low, high, yearAgoEps, numberOfAnalysts, growth},
 revenueEstimate  {avg, low, high, numberOfAnalysts, yearAgoRevenue, growth},
 epsTrend         {current, 7daysAgo, 30daysAgo, 60daysAgo, 90daysAgo},
 epsRevisions     {upLast7days, upLast30days, downLast7Days, downLast30days, downLast90days}}
```

실측(2026-07-09, AAPL 예):

```
0y: growth 17.35% | eps avg/low/high 8.76/8.29/9.04 analysts 41 |
    rev avg/low/high 478.2B/469.8B/485.3B analysts 38 |
    epsTrend cur/7d/30d/60d/90d 8.76/8.76/8.75/0.00/8.49 |
    epsRevisions up7=2 up30=4 down7=1 down30=0
```

**버려지는 것 = earningsEstimate(low/high/growth/yearAgoEps), revenueEstimate 전체, epsRevisions의 7일/90일 창, 그리고 0q/+1q(분기) 전체.** 현재는 0y/+1y의 up30/down30만 `max()`로 뭉개서 저장 — 기간별·창(window)별 세분화된 원본은 영구 소실 중.

### 1-3. 완전히 새로 호출해야 하는 것

| 데이터 | yfinance 속성 | 실측 HTTP 비용 | 비고 |
|---|---|---|---|
| 애널리스트 등급 분포(strongBuy~strongSell, 0m~-3m) | `stock.recommendations` | **+1회/종목**, 첫 호출 ~0.7s (이후 `.recommendations_summary`는 캐시, 0회) | Zacks식 "Agreement"의 대안/보완 axis |
| 개별 등급 변경 이력(회사·직전PT·신규PT·날짜) | `stock.upgrades_downgrades` | +1회/종목 (recommendations와 별개 모듈) | 이미 날짜가 찍힌 **누적 이력**이라 매일 스냅샷할 필요는 적음(한 번 받으면 과거분 이미 포함, 신규 행만 diff) |

### 1-4. Top 5 아카이빙 가치 필드 (요청사항)

1. **`epsRevisions` 전체(up7/up30/down7/down30/down90) × 4개 기간(0q/+1q/0y/+1y)** — 현재는 0y·+1y를 `max()`로 뭉개 **7일 창을 버림**. Agreement/모멘텀 축(사용자가 원하는 Zacks식 게이트·신호 재설계)의 원자료가 바로 이것. *지금 안 모으면 복구 불가능*(yfinance가 과거 값을 안 준다 — 오늘 것만 "현재 시점").
2. **`earningsEstimate.low/high` (analyst 추정치 분산, 4기간)** — `(high-low)/avg`로 애널리스트 불일치(Dispersion) 계산 가능. 현재 완전 미저장. 낮은 분산 = 높은 합의(agreement) 신호로, gap 게이트·PER 게이트 다음 후보 축.
3. **`revenueEstimate` 전체(avg/low/high/yearAgoRevenue/growth, 4기간)** — 매출 서프라이즈/매출성장 추정. `rev_growth`(DB 기존 컬럼, `fetch_revenue_growth`의 `.info` 기반 후행 실적)와는 **다른 것**(이건 forward 추정치) — 현재 시스템에 전혀 없는 axis.
4. **분기(`0q`/`+1q`) epsTrend 5스냅샷** — 현재는 연간(0y/+1y) 블렌딩만 NTM으로 쓰고 분기 원본은 버림. 분기 단위는 연간보다 반응이 빨라(다음 실적 발표가 가깝다) 더 민감한 revision-momentum 신호 후보.
5. **`endDate` (4기간 모두)** — 지금 당장의 표시값이 아니라 **재현성**을 위한 필드. fiscal year가 실적 발표로 롤오버되면 "0y"의 의미 자체가 바뀐다(예: 10월 결산 발표 후 0y가 다음 회계연도로 이동). endDate를 안 찍어두면, 6개월 뒤 "그날의 NTM 블렌딩"을 정확히 재현할 방법이 없다 — PIT의 정의상 이게 없으면 나머지 4개도 반쪽짜리가 된다.

(6위, opt-in: `recommendations` — 위 1~5는 API 비용 0인데 이것만 비용이 붙어 별도 등급 취급)

---

## 2. 저장 설계

### 2-1. 포맷: parquet, 일별 스냅샷 — 권장하되 세부는 조정

- **일별 불변 스냅샷**(`research/pit_archive/YYYY-MM-DD.parquet`) 자체는 권장 유지: PIT의 핵심은 "그날 그 시점 값이 절대 변하지 않는다"는 불변성인데, 파일을 날짜로 쪼개면 append-only가 파일시스템 레벨에서 강제된다(실수로 어제 값을 덮어쓸 수 없음).
- 다만 **1300종목×~250거래일/년 = 소파일 다수(연 250개 파일)** 문제가 있다. parquet은 표본이 작을수록 컬럼 통계/딕셔너리 오버헤드 비중이 커진다(실측: 30종목 2475B/종목 vs 300종목 726B/종목 — 아래 4절). 파일 개수 자체는 git/파일시스템엔 큰 문제 아니므로(수백 개 정도) **일별 유지를 1차 권장**, 필요시 분기/연 단위로 재압축하는 **월별 consolidation job**(별도, 이번 범위 밖)을 나중에 추가하는 걸 제안.

### 2-2. 스키마 (초안 코드에서 실제 생성)

```
date, ticker,
{0q,p1q,0y,p1y}_end_date,
{0q,p1q,0y,p1y}_growth,
{0q,p1q,0y,p1y}_eps_{avg,low,high,year_ago,n_analysts,growth},
{0q,p1q,0y,p1y}_rev_{avg,low,high,n_analysts,year_ago,growth},
{0q,p1q,0y,p1y}_epstrend_{cur,7d,30d,60d,90d},
{0q,p1q,0y,p1y}_rev_{up7,up30,down7,down30,down90}
```
= 98 컬럼(date/ticker + 4기간 × 24필드). recommendations 포함 시 +20컬럼(118).

'+' 기호는 컬럼명에 못 써서 `+1y→p1y`, `+1q→p1q`로 치환(`PERIOD_SUFFIX` 매핑, 코드 상단). `-1q`/`-1y`(과거분기, 일부 종목만 나타남)도 매핑에 포함해뒀으나 실측 표본엔 안 나타남 — 안 나타나는 period 코드는 조용히 스킵(스키마 안정성 우선, 미래 신규 코드가 와도 크래시 안 남).

**설계 원칙**: raw 필드만 저장, agreement/dispersion 같은 파생 지표는 저장하지 않음(다운스트림에서 계산). 이유: 파생 공식이 나중에 바뀌어도 원본 재계산 가능해야 진짜 PIT 아카이브(단일 진실 소스) 의미가 있음.

### 2-3. 용량 실측 및 추정 (4절 샘플 실행 결과 참조)

| 항목 | 값 |
|---|---|
| 컬럼 수 (recommendations 미포함) | 98 |
| 종목당 압축후 크기 (n=300 실측) | 726 B |
| 전종목(~1300) 추정 | ~922 KB/일 |
| 연간(252거래일) 추정 | **~227 MB/년** |
| recommendations 포함 시 컬럼/종목당크기 | 118컬럼 / 2904B(n=30, 소표본이라 과대추정) |

→ **1년치가 이미 production DB(현재 32MB, 매일 재커밋되며 계속 성장 중)보다 크다.** 메인 repo에 그대로 얹으면 `.git` 히스토리 폭증(현재 이미 1.4GB — 매일 DB 파일 전체를 재커밋하는 구조라서 매 커밋마다 새 blob이 쌓임) 문제를 가속시킨다.

### 2-4. 저장 위치 — repo 안 vs 밖

GitHub Actions 러너는 **매 실행마다 새 VM**(이전 실행의 로컬 디스크가 없음) — 따라서 "로컬에만 쌓기"는 cron에서 불가능하다. 뭔가에 **커밋하거나 외부로 push**해야 지속된다. 옵션:

| 옵션 | 장점 | 단점 | 판단 |
|---|---|---|---|
| A. 메인 repo에 커밋(`research/pit_archive/`, 현재 초안 위치) | 코드 변경 최소, 기존 `git add -A` 흐름 재사용 가능 | 메인 repo `.git` 폭증 가속(연 230MB+, 이미 1.4GB인 저장소에 매년 누적) | 시험/개발 단계만 (지금 상태) |
| **B. 별도 전용 repo** (예: `VolumeQuant/eps-momentum-pit-archive`) | 메인 repo 오염 0, 독립 라이프사이클(용량 문제 생기면 그 repo만 정리), 기존 git 워크플로우 그대로 재사용(별도 PAT 시크릿만 추가) | 크론 스텝에 별도 checkout+push 스텝 필요 | **권장(운영 전환 시)** |
| C. Git LFS (메인 repo 내) | repo 히스토리 자체엔 포인터만 남아 clone 크기는 유지 | GitHub 무료 LFS 할당량 1GB storage/1GB bandwidth·월 — 2~3년이면 초과, 유료 전환 필요 | 비권장(무료한도 작음) |
| D. 외부 오브젝트 스토리지(Hugging Face Datasets 등, git 아님) | git 완전 우회, HF datasets는 parquet 축적용으로 설계됨, 무료 | 새 자격증명/의존성(`huggingface_hub`) 추가 | 장기 후보(2번째 안) |

**권장: B(별도 repo)** — 이미 git 기반 cron 인프라(체크아웃→실행→커밋→푸시)가 검증돼 있어 재사용 비용이 가장 낮고, 메인 repo 히스토리 비대화 문제와 완전히 분리된다.

---

## 3. cron 통합 제안 (제안만 — 실제 수정 안 함)

판정일에 실제로 적용한다면 다음 위치에 diff:

1. **`daily_runner.py` `_prefetch_eps` 워커 (L602-621)**: `raw_trend = stock._analysis._earnings_trend` 라인 바로 뒤에 `_prefetched[ticker]['raw_trend_full'] = raw_trend` 형태로 이미 들고 있는 객체를 그대로 반환값에 얹기만 하면 됨(추가 API 호출 0). 즉 **daily_runner의 기존 워커를 조금만 확장**하면 별도 스크립트 없이도 같은 실행에서 공짜로 얻을 수 있다는 게 이번 조사의 핵심 발견 — 다만 이번 태스크 범위상 실제 수정은 하지 않음.
2. **신규 스텝**: `.github/workflows/daily-screening.yml`의 `Run EPS Momentum Screening` 스텝(L54-61) 이후에 `python research/pit_archive_draft_2026_07_09.py --full`(또는 daily_runner 확장판이 이미 만든 파일을 저장) 스텝 추가 위치 제안.
3. **저장소 분리 채택 시**: 별도 `actions/checkout` (다른 repo, 다른 토큰) + push 스텝을 daily-screening.yml 끝에 추가하는 위치 제안(현재 커밋/푸시 스텝 L82-87 패턴과 동일하게, 대상 repo만 다르게).
4. **`recommendations` 포함 여부**: 별도 env 플래그(`PIT_ARCHIVE_RECOMMENDATIONS=0/1`, 기본 0)로 킬스위치화 제안 — 비용이 붙는 부분만 opt-in.

---

## 4. 초안 코드 + 샘플 실행 결과

파일: `research/pit_archive_draft_2026_07_09.py` (production 코드/DB 미참조·미변경, 읽기전용으로만 DB에서 표본 티커 조회)

사용법:
```
python research/pit_archive_draft_2026_07_09.py --sample 30                        # 표본 30종목
python research/pit_archive_draft_2026_07_09.py --sample 30 --with-recommendations  # +등급 수집
python research/pit_archive_draft_2026_07_09.py --full                             # 전종목(확인 프롬프트)
```

### 실행 결과 (2026-07-09, DB 최신일 2026-07-07 상위종목 표본)

| 실행 | 종목수 | API호출 | 컬럼 | 소요시간 | 파일크기 | 종목당(압축후) |
|---|---|---|---|---|---|---|
| `--sample 30` | 30 | 30 (1.0/종목) | 98 | 5.2s | 74,240 B | 2,475 B |
| `--sample 300` | 300 | 300 (1.0/종목) | 98 | 65.5s | 217,923 B | **726 B** |
| `--sample 30 --with-recommendations` | 30 | 60 (2.0/종목) | 118 | 7.6s | 87,134 B | 2,904 B |

핵심 확인:
- **recommendations 없이는 API 호출이 정확히 1회/종목** — 즉 기존 daily_runner의 eps 수집(이미 1회/종목)과 100% 같은 비용. 별도 수집기를 만들 필요 없이 daily_runner 안에 raw_trend를 그대로 흘려보내기만 하면 됨(3절 참조).
- recommendations 포함 시 API 호출 2배(60/30종목), 종목당 소요시간도 0.173s→0.253s로 증가 — 전종목(~1300) 규모로 확장하면 전체 수집 시간이 유의하게 늘어나(현재 EPS 수집만 ~110~135초/1200~1280종목, 이미 rate-limit에 예민한 파이프라인) **레이트리밋 리스크를 키운다.** 그래서 opt-in/저빈도(주간) 권장.
- n=30→300 사이 종목당 압축 크기가 2475B→726B로 크게 줄어듦(parquet 오버헤드 상각) → 실제 운영 규모(1300종목)에서는 300종목 수치(726B)가 30종목 수치보다 신뢰도 높은 하한 추정치. 최종 용량 추정(2-3절)은 이 값 기준.

출력 파일(오늘 하루치 실제 스냅샷, 유지): `research/pit_archive/2026-07-09.parquet` (30종목, 98컬럼)

---

## 5. 한계 및 다음 단계

- **표본이 30~300종목**(전종목 ~1300 아님) — 사용자 요청에 "30종목 샘플로 실측" 명시돼 있어 그대로 따름. 전종목 실행 시 API 호출 시간·레이트리밋 거동은 추정치이며 daily_runner의 기존 배치(30개씩, 스레드 2, 배치간 1.5s 슬립) 패턴을 그대로 재사용하면 기존 파이프라인과 동일한 안정성을 기대할 수 있음(초안 코드가 정확히 그 패턴을 복제).
- **오늘(2026-07-09) 하루치만 존재** — PIT 아카이브는 "시간이 지나야 생기는 자산"이라는 문제의식 그대로, 지금 시작해도 검증 가능한 결과물(예: FY2/Agreement 축 신호 리서치)이 나오려면 최소 수개월 축적 필요. 매일 자동 축적 여부(cron 편입)는 사용자 판정 대기.
- **저장소 분리(B안) 자체는 미실행** — 별도 repo 생성·PAT 발급은 GitHub 조직 설정이 필요해 이번 설계 범위 밖. 승인 시 별도 작업으로 진행 제안.
- **recommendations는 opt-in 미검증 규모** — 30종목만 실측, 1300종목 전체 소요시간(추정 ~7~8분, 기존 EPS 수집의 ~4배)은 실제 cron에서 검증 필요.
