# KR EPS 리비전 소스 실현가능성 스파이크 (2026-07-09 자율주행)

사용자 요청: "fnguide wisefn, yf 등 전수조사해서 제대로 다시 분석해." 근본원인 = yfinance
KR 커버 불안정(73~210/일) + ntm_90d 1일 glitch. 대체 소스 실현가능성을 조사한다.

## 결론 (먼저)
- **WiseReport(FnGuide) ajax 엔드포인트 = 접근 가능·정상 파싱·대형주 컨센서스 안정 제공 → 유력 교체 대상.**
- pykrx(KRX 직접) = 이 환경에선 네트워크 차단(빈 응답). KRX는 애초에 *선행 컨센서스 없음*
  (후행 PER/EPS·시총만) → 리비전 신호엔 부적합. 유동성/시총 보조로만 유용.
- Naver Finance = WebFetch 차단.
- 최선책 2안 병기(아래).

## 실측: WiseReport ajax
엔드포인트: `comp.wisereport.co.kr/company/ajax/c1080001_data.aspx?cmp_cd={6자리}&frq=0&rpt=1&finGubun=MAIN`
- **삼성(005930)**: 14개 증권사별 추정 EPS(42,311~51,839, 실명 애널리스트), 2Q26 영업이익
  89.4조(+56% QoQ)·매출 171조, close 278,000원.
- **하이닉스(000660)**: 12+개 증권사 추정 EPS(313k~355k), 2026 영업이익 279조(YoY +461%),
  close 2,186,000원.
- ★**두 종목 close price가 우리 KR DB와 정확히 일치**(278,000·2,186,000) → **KR DB 가격/시총은
  이 timeline 실제값 확정**(이전 "10x 버그" 우려 완전 해소).
- ★삼성·하이닉스 펀더멘털 급등(OP +56% QoQ·+461% YoY) = **대규모 상향 리비전이 진짜** →
  glitch로 나온 삼성 rev90 64%가 오류였음을 외부 소스가 교차확인.

## 교체 설계 2안 (KR 시스템 레벨, production 미변경 — 제안만)
**A안: WiseReport 컨센서스 직접 인제스션**
- 매일 c1080001 ajax로 증권사별 추정 EPS 수집 → 평균/중앙값 = forward EPS 컨센서스.
- 장점: 한국 전문 소스·안정·증권사 실명(dispersion=SUE식 변동성 정규화 가능·리비전 상향/하향 카운트).
- 단점: (1)저작권 고지("무단 DB화 민형사 책임") — 개인 리서치/소량이면 관행상 회색지대나 상업배포 불가
  (2)ajax 파라미터·구조 변경 리스크 (3)rev90(90일 변화)은 *일별 스냅샷 축적* 필요(엔드포인트는 현재값).

**B안: 자체 아카이브 기반 point-in-time rev90 (rolling-column 탈피)**
- 현재 rev90 = yfinance가 주는 ntm_current & '90daysAgo' 컬럼 차 → **'90daysAgo'가 glitch의 원흉**.
- 대안: **우리가 매일 저장하는 ntm_current로** rev90(today)=ntm_current(today)/ntm_current(today−90일)−1.
  yfinance rolling 컬럼 안 씀 → glitch 원천 차단.
- 요건: ntm_current 90일 히스토리. **US=이미 충족(2/6~, 150일) → 지금도 전환 가능.**
  **KR=6/1~ 26일뿐 → ~2026-08-30 충족.** 그때까지는 glitch 가드가 방어.
- 장점: 소스 무관·저작권 무관·구현 단순(자체 DB만). 단점: KR은 대기 필요.

## 권고 (판정일 안건)
1. **단기(완료)**: ntm_90d glitch 가드 배포 — 소스 무관하게 방어.
2. **중기 US**: B안(자체 point-in-time rev90) US부터 전환 — 데이터 이미 충족, rolling-column 위험 제거.
3. **중기 KR**: 8월말 자체 히스토리 충족 시 B안 KR 적용 / 병행해 A안(WiseReport)으로 커버 확대·안정화
   검토(yfinance 73~210 불안정 해소). A안은 KR 시스템(eps_momentum_kr.py) 레벨 작업.
4. **수집 헬스가드**: KR/통합에 US식 `_validate_collection_health` 이식(69종목 같은 부실일 차단).

## 환경 한계 (정직)
- 이 개발 환경은 KRX·Naver 직접 접근 차단. WiseReport ajax만 WebFetch로 뚫림. 실제 소스 파이프라인
  구축·검증은 KR 시스템 런타임(회사 PC/Actions·한국 egress)에서 해야 함. 여기선 실현가능성만 확인.
