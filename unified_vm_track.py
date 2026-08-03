# -*- coding: utf-8 -*-
"""미국 VM 신호 — 실매매 본선 (2026-07-08 개시, 2026-07-31 전면 개편)

★현재 운영 스펙 (env로 전환, 기본값은 코드 상수)
  순위 기준 : adj_gap(괴리율) = 선행PER 압축폭 — '전망은 올랐는데 주가가 안 따라온' 순
              (구 rev90 = 90일 전망 상향폭. VM_STRATEGY=rev90으로 복귀 가능)
  시장      : 미국 단독 (VM_US_ONLY=1). KR 다리는 코드 보존 — 0으로 되살아남
  포트폴리오: N_TOP=5 각 20%, REBAL=5거래일 (앵커 = US 그리드 2026-07-02)
  게이트    : 거래대금 $1B · 선행PER<=30 · min_seg>=-2% · 안전필터 5종(동전주·애널3·
              rev90>0·영익5%·FCF&ROE) · 업종제외.  ★gap 게이트는 해제
              (고성장 요구조건이라 가치 순위와 충돌 — META·GOOGL 등 저변동 우량주를 34% 차단)
  발송      : KST 월~토 아침 (미국 수집 완료 이벤트 + 백업 크론). 개인봇, 채널은 env로 개시
  ★모든 BT는 exec_lag=1(신호 판정 다음날 체결) 기준 — lag=0 수치는 낙관 편향

사용: python unified_vm_track.py       (계산 + 원장 append + 메시지 발송)
      python unified_vm_track.py --nav (원장 리플레이 NAV만)
원장: data_cache/unified_vm_log.csv (append-only, us_date 기준 1회 반영)

연구 근거: research/_prod_faithful_bt_2026_07_31.py(정직 하네스)·_gate_sweep_lag1_*(게이트)
          ·_slots_sweep_*(슬롯)·_lookahead_audit_*(look-ahead 감사)·_robustness_lag1_*(견고성)
"""
import os, sys, json, csv, sqlite3
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
# 2026-07-10: KR 경로 머신별 자동탐색 — 회사PC=KR 프로덕션 직접(C:/dev/kr_eps_momentum, 18:10
#   schtask가 여기서 돎), 집PC=quant_py-main 클론(회사PC가 매일 ~16:40 커밋 → git pull 수신).
#   집PC의 C:/dev/kr_eps_momentum·C:/dev/data_cache는 07-09 정리 때 삭제됨(스크래치 복사본).
def _first_existing(env_key, cands):
    v = os.environ.get(env_key)
    if v:
        return v
    for p in cands:
        if os.path.exists(p):
            return p
    return cands[0]

KR_DB = _first_existing('KR_DB_PATH', [
    'C:/dev/kr_eps_momentum/eps_momentum_data_kr.db',                          # 회사PC 프로덕션
    'C:/dev/claude-code/quant_py-main/kr_eps_momentum/eps_momentum_data_kr.db',  # 집PC 클론
    'C:/dev/claude code/quant_py-main/kr_eps_momentum/eps_momentum_data_kr.db',
])
KR_FS_DIR = _first_existing('KR_FS_DIR', [
    'C:/dev/data_cache',                                # 회사PC
    'C:/dev/claude-code/quant_py-main/data_cache',      # 집PC 클론
    'C:/dev/claude code/quant_py-main/data_cache',
])
LOG = os.path.join(HERE, 'data_cache', 'unified_vm_log.csv')
# ★2026-07-09 프로덕션 재캘리브레이션 동기화: gap 2.5→1.5(전수검사 기준), top4→top5.
#   US 프로덕션과 패리티 유지가 이 트랙의 존재 이유(같은 게이트를 양국에). in_top4 컬럼명은 로그 연속성
#   위해 유지(의미 = topN 멤버십). ⚠️gap 1.5로 낮추며 KR에 US 업종제외 등가 필터 신설(정유 등 —
#   구 2.5에선 gap이 우연히 걸렀지만 1.5에선 S-Oil이 상위 진입, US 규칙이면 원자재/정유 제외 대상).
# ★슬롯 수 (2026-07-31 재스윕, exec_lag=1 · 이중상장 가드 적용 · 위상평균).
#   5 -> 6. 헤드라인·운뺀최악·MDD 세 지표 모두 6이 우위:
#     N   수익%   MDD%   Calmar  최악LOWO
#     2  +147.5  -26.3   24.41    10.82   ← 헤드라인 1위지만 LOWO에서 반토막(한 종목 50%)
#     3  +113.1  -22.7   19.04    10.22
#     4   +98.3  -20.5   17.26    12.12
#     5   +83.2  -19.7   14.28    11.21   ← 구 설정. 양옆(4·6)보다 낮은 중간값
#     6   +75.5  -16.6   14.83    12.46   ← 채택
#    10   +57.4  -12.7   13.56    13.09
#   ★N이 적을수록 많이 벌고 많이 빠지는 단조 관계. N=2/3의 높은 헤드라인은 SNDK(+120.8%)
#     한 종목이 50%를 차지해 만든 착시 — LOWO에서 무너져 기각(아침에 구 2슬롯 시스템이
#     +304% -> SNDK 제외 시 +93%로 붕괴한 것과 동일 구조).
#   매매 부담 실측: 리밸당 새로 사는 종목 2.3개(N5) -> 2.9개(N6), 연 230회 -> 288회.
#     슬롯 6개라고 매번 6종목을 사는 게 아니라 절반은 유지된다.
#   ⚠️차이는 노이즈 범위(14.28 vs 14.83, 5.5개월 표본). 방향이 세 지표에서 일관될 뿐.
#   research/_slots_sweep_2026_07_31.py
#   ★2026-07-31 최종: 6 -> 5로 되돌림 (사용자 결정).
#     6의 이득(Calmar 14.28->14.83, MDD -19.7->-16.6)은 5.5개월 표본에서 노이즈 범위.
#     반면 16.7%씩 배분·리밸당 2.9종목 교체는 실행 난이도를 올린다.
#     ★불편하면 룰을 안 지키게 되고, 그 이탈 위험이 노이즈 수준 Calmar 이득보다 크다.
#     노이즈 이득을 얻으려 실행 난이도를 올리는 건 손해 — 5종목 20%씩 유지.
#     (N=6으로 가려면 VM_N_TOP=6. 스윕 근거는 위 표 그대로 유효.)
N_TOP = int(os.environ.get('VM_N_TOP', '5'))
# 교체 주기(영업일). ★2026-07-31 스윕(production 정확 게이트, research/_prod_faithful_bt):
#   rev90은 주기 무관(R1 11.66 ~ R3 12.91 ~ R5 11.60 = 평평) — 분기 실적 기반이라 느리게 변함.
#   괴리율은 주기가 결정적(R1 29.03 / R2 27.94 / R3 21.66 / R5 16.97) — 매일 주가로 변하는
#   신호라 5일 기다리면 괴리가 이미 메워짐. 비용 40bp까지 넣으면 R2(17.52)가 R1(14.79)보다
#   강건(회전 절반: 월 17.5건 vs 24.9건) → gap 전략 채택 시 R2~R3 권장.
REBAL = int(os.environ.get('VM_REBAL', '5'))
PE_MAX, GAP_MIN = 30.0, 1.5
# ★2026-08-01 거래대금 게이트 재정의 (사용자 "거래대금은 슬리피지 안 나게 최소 기준으로만"):
#   알파 팩터가 아니라 체결 제약 조건이다. 성과 스윕으로 임계를 고르는 것은 범주 오류 —
#   실측 지형이 실제로 노이즈($100M 20.95 / 300M 17.36 / 500M 22.20 / 1B 17.16, 구조 없음).
#   스펙 = 공식: 필요 ADV = 포지션금액($10만) ÷ 참여율(1%) = $10M, 안전배수 30배
#   (수집오류·거래량 급감·계좌 성장·구독자 여유) → $300M. 계좌가 커지면 공식으로 재산출.
#   ★어떤 하한이든 "수익에 최적"이라는 주장은 금지 — 그 주장이 불가능하다는 것이 이 게이트의 정의.
#   구 $1B는 rev90 시절 '주도주 필터'라는 알파 명목의 유산(v117)이었고 필요치의 100배였다.
DV_MIN_MUSD = float(os.environ.get('VM_DV_MIN', '300'))
# ★2026-08-01 가치함정(신호 전제 부재) 게이트 — 사용자 확정: "전망은 제자리인데 주가만 빠져서
#   싸 보이는 종목"은 이 전략의 간판 전제("전망은 올랐는데 주가가 안 따라옴")를 위반한다.
#   컷 = rev30(30일 순상향) <= +2% AND 20일 주가 하락 '동시' 충족 — 전망이 오르는 중이거나
#   주가가 안 빠졌는데 싼 종목은 건드리지 않는다. PER30·TE통화가드와 같은 '유효영역 제약'
#   (알파 주장 아님 — 검증 에이전트가 성과 증거를 기각했고, 6/15~ 구간엔 이 컷이 오히려
#   성과를 깎았음을 실측·명기: REDESIGN_DEBATE_2026_08_01.md 추기 참조).
#   임계 앵커 = 사전 등록값만(과적합 차단): 2% = 2026-07-13 stale 정의(rev30<=2 '상향 동력
#   죽음')의 기존 경계 재사용, 0% = 부호 경계. 에폭 8/3(미래로만 — 거버넌스 ⓐ, 7/31 재안내 불변).
TRAP_GATE_ON = os.environ.get('VM_TRAP_GATE_DISABLE') != '1'
TRAP_REV30 = float(os.environ.get('VM_TRAP_REV30', '2.0'))
# 에폭: 최초 8/3(미래로만)이었으나 사용자 승인(08-01 밤 "니 말대로 하자")으로 7/31 소급 —
# top5(발송된 지시)는 소급 전후 동일(AMKR·CLS·MU·STX·SNDK)이라 거버넌스 ⓐ의 보호 대상
# (지시 모순)이 침해되지 않고, 대기조(정보)만 재편됨. 재발송으로 대기조도 당일 정정 완료.
TRAP_EPOCH = '2026-07-31'
# ★na≥6 게이트 (2026-08-01 밤, 사용자 승인): 애널 3~5명 종목은 추정치 계단 노이즈가
#   6명+의 2배(일간 |ΔNTM|>3% 8.4% vs 3.9%, 6~12명 고원 — 경계=실측 불연속, 성과로 안 고름).
#   dv $1B 시절엔 na≥3이 non-binding이었으나 $300M 전환으로 하중 기둥이 됨(하중 전이).
#   성과 영향(dv300·lag=1): 수익 −0.8%p·Cal −0.23 = 노이즈 수준(REDESIGN_DEBATE 추기 3).
#   레거시(rev90) 모드는 구값 3 유지. env VM_NA_MIN(기본 6).
NA_MIN = int(os.environ.get('VM_NA_MIN', '6'))
# ★2026-08-02 구시스템 안전장치 복원 2종 (사용자 확정 "당연히 2안으로" — REDESIGN_DEBATE·
#   QUALITY_CAMPAIGN 틱 17/12/19 근거. ru3·min_seg 복원과 같은 '복원' 범주, 신규 발명 아님):
#   ①dd_30_25 진입 유예(v84 정의 그대로): 30거래일 고점 대비 -25% 이상 하락 종목은 '신규 진입'만
#     건너뜀(기보유 불간섭 — 구시스템 게이트의 원래 의미). 7월 크래시 매수 9건 전패(평균 -15.6%)
#     실측 + v84 독립 검증 + 휴면보험형(전반 비용 -0.7%p). 자동 해제: 회복하거나 30세션 경과 시.
#   ②보유 유지 밴드(EXIT_RANK 원리, 구 12/10=1.2~1.4x 앵커): 진입은 top5, 보유는 순위
#     HOLD_BAND(7)까지 유지 — 경계 동전던지기 교체(실측 17%) 제거.
#   조합 실측(lag=1·위상평균): +86.8/-16.4/Cal 17.85 → +94.9/-12.7/Cal 26.19, 전반에도 우위
#     (+91.4 vs +88.4 = 7월 재포장 아님·캠페인 유일 통과), LOWO 3종 전승, 교체 2.91→2.59.
#   에폭 = us_date 2026-07-31(사용자 2안: 현 리밸부터 — AMKR 신규매수가 규칙 금지 유형이므로).
#   ⚠️신규 고객 정합(사용자 지적): 목표에 포함된 '보유 유지형' 종목(dd 걸린 기보유 MU·SNDK)은
#     메시지에 "처음 시작하는 분은 현금 대기" 자동 표시 — 표시 소멸 시 합류.
DD_ENTRY_ON = os.environ.get('VM_DD_ENTRY_DISABLE') != '1'
DD_THR = float(os.environ.get('VM_DD_THR', '-25'))
# ★2026-08-03 dd_30_25 OFF (사용자 재확인 "이거 도입 안 하고 트레일링스탑 15%로 관리하기로 한 것").
#   위 8/2 '사용자 확정 2안' 기록과 충돌 — 그 세션 트랜스크립트가 이 머신에 없어 검증 불가하며,
#   1차 근거(사용자 본인 확인)가 2차 기록(커밋 메시지)에 우선한다. 급락 관리는 시스템이 아니라
#   고객 측 트레일링 스탑 15% 안내가 담당(교체일 메시지의 TS 블록).
#   측정(research/_stop_revg_matrix_2026_08_03.py, 매출게이트 포함): 밴드7만(dd 없음)이 전 셀 최고 —
#   수익 +104.3%/Cal 23.17/LOWO 15.86 vs dd포함 +87.6/19.79. 비용은 스트레스 MDD −8.3→−12.3%p
#   (7월형 크래시 직후 매수를 다시 허용하는 대가 — 고객 낙폭은 TS15%가 −15%에서 절단).
#   ★에폭 = 2026-08-01 (거버넌스 ⓐ 엄수: 발송된 지시의 기준 소급 금지). 7/31분은 오늘 아침
#   고객 채널에 CLS·MU·STX·SNDK·KNX로 이미 나갔다 → 그 us_date는 구 기준(dd ON·밴드7)으로
#   동결해 재현하고, 새 기준은 다음 계산일(8/3~, 실제 반영은 8/7 교체)부터.
#   ⚠️내가 "미체결이니 소급해도 된다"고 예외를 만들려다 사용자에게 저지당함 — 미체결 여부는
#   시스템이 알 수 없고(계좌 모름), 이미 나간 지시를 바꾸는 것 자체가 신뢰 파괴다. 예외 금지.
#   복원 = VM_DD_OFF_EPOCH='9999-12-31'.
DD_OFF_EPOCH = os.environ.get('VM_DD_OFF_EPOCH', '2026-08-01')
_DD_LIVE = False   # compute()가 당일 us_date로 갱신
# ★2026-08-03 HOLD_BAND 7→5 (= 밴드 해제, 사용자 "기존 5종목 이런 거 없다니까? 새로 시작하는 건데?").
#   dd_30_25와 동일한 병 — 목표 목록이 '이전 보유'에 의존하면 오늘 합류한 고객이 받은 목록과
#   기존 고객의 목록이 영원히 달라진다(포트폴리오 분화). 방송 상품은 언제 들어오든 같은 한 장이어야 함.
#   비용은 명시: 밴드7이 성능은 더 좋았다(수익 +104.3 vs +95.1 / Cal 23.17 vs 21.47 / 회전 227 vs 271).
#   그 차이는 '보유 상태를 아는 계좌'에서만 실현 가능한 이득이라 방송에서는 못 받는 이득으로 판단.
#   개인 계좌 운용 시에는 밴드7이 우월 — VM_HOLD_BAND=7로 복원 가능.
#   ★에폭 = dd와 동일 2026-08-01 (7/31분은 밴드7로 이미 발송됨 → 그 us_date는 구 기준 동결).
HOLD_BAND = int(os.environ.get('VM_HOLD_BAND', '5'))
HOLD_BAND_LEGACY = 7
BAND_OFF_EPOCH = os.environ.get('VM_BAND_OFF_EPOCH', '2026-08-01')
GATE2_EPOCH = '2026-07-31'
# 에폭: 이 us_date부터 $300M 적용. 최초 8/3으로 뒀으나(토요일 발송과 월요일 재안내의 모순 방지)
# 사용자 지시(2026-08-01 "7/31 시장에 대해서도 적용해야지")로 7/31 소급 — 실제 체결은 월요일 밤이므로
# 월요일 아침 재안내가 새 기준 TOP5로 나가면 고객 기준 모순은 없다(토요일분은 예고로 대체됨).
# 같은 us_date 재발송이 '미체결 지시를 갱신'하는 처리는 _replay 참조.
DV_EPOCH = '2026-07-31'
# ★KR 하한 = 백분위 등가. 스펙은 하나("각 시장 거래대금 상위 ~10%", 2026-07-09 사용자 승인),
#   환율만 시장별 — 과거 $100M '특례' 폐지 사유(자의적 숫자)를 해소한 원칙.
# ★2026-07-13 재산출 $0.3B→$0.1B (사용자 "300M이 현실적인지 검증해라"): 구 $0.3B는
#   MA120 사전필터 아티팩트로 좁아진 유니버스 225개 기준 상위 10%였음. 7/10 수집 확대
#   (필터 OFF, 372종목) 후 실측: 상위 10% 경계 = $106M (fx 1499), $300M은 상위 2.4% =
#   자기 스펙 위반. US $1B = 상위 8.9%(123/1,378, 7/10)와 등가도 ~$110M. → $100M(상위
#   10.5%) 채택. 검증: 임계 300/150/106 통합 top10 완전 동일(오늘 신호 무영향), KR 후보
#   4→13종목(삼성생명 $149M·효성重 $131M 등 편입 — 통합 11위권, 대기 후보 존).
#   유니버스 정의가 또 바뀌면(예: .KQ 폴백로 코스닥 편입) 재산출할 것.
#   상세: research/KR_DV_PARITY_2026_07_09.md (+ 2026-07-13 추기), GATECHAIN_REVIEW_2026_07_12.md
KR_DV_MIN_MUSD = 100.0
KR_HOLDCO = {'402340.KS'}  # SK스퀘어(지주) — KR production 지주제외 준용
KR_IND_BLOCK = {'010950.KS', '096770.KS'}  # S-Oil·SK이노베이션(정유) — US COMMODITY(석유정제) 등가
# 병기 변형: 메모리 테마 캡2 (6월 그리드서 유일 유효 손잡이 — 급락창 1회라 채택 아닌 병기 관찰)
MEMORY_THEME = {'SNDK', 'MU', 'WDC', 'STX', '005930.KS', '000660.KS'}
_MEM_ALERT_ON = False  # 메모리 주의보 상태 (카드 라벨용, _compose_and_send에서 설정)
# ★2026-07-31 메시지 노출 제거 (사용자 결정 '안 따를 건데 왜 넣냐').
#   경보 자체는 진짜다 — 같은 기간·같은 구조로 랜덤 현금(300회) Calmar 0.75 vs 경보 1.22,
#   랜덤 분포 상위 3%(MDD 96%·Calmar 97% 우세). 타이밍에 실력 있음.
#   ★그러나 그 1.22는 '발동 시 실제로 메모리를 판다'는 전제에서만 나온다.
#   우리는 매매 적용을 하지 않기로 했으므로(정밀도 17%·검증구간 발동 1회) 표시만 남기면
#   이득은 0이고 혼란만 생긴다 — 시스템이 SNDK를 3위로 추천하면서 동시에 '주의' 라벨이 붙어
#   고객이 취할 합리적 행동이 없다. => 노출 제거.
#   참고 정직 수치: 안 씀 Calmar 1.15 / 따름 1.22 (구 보고 1.05->1.61은 look-ahead 허수).
#   되살리려면 MEM_ALERT_SHOW='1'. 계산 모듈(memory_cycle_alert.py)·연구는 그대로 보존.
MEM_ALERT_SHOW = os.environ.get('MEM_ALERT_SHOW', '') == '1'
THEME_CAP = 2

# ── 전략 스위치 (2026-07-31, 기본 OFF = 현행 동작 100% 동일) ────────────────────
# VM_STRATEGY='gap'  : 순위 기준을 rev90(90일 전망 상향폭) → adj_gap(EPS 대비 가격 괴리율,
#                      낮을수록 좋음)으로 교체 + gap/min_seg 게이트 해제.
#   근거: 미국 단독 5.5개월 BT(research/_calmar_summary_2026_07_31.py) Calmar 9.67→18.61
#   (수익 +87.9→+88.3 동일, MDD −31.4→−16.4). 검증 = 기간 5/5·LOWO 4/4·게이트 16조합
#   MDD 16/16·유니버스편향 0(rev90을 adj_gap 보유 유니버스로 제한해도 수치 불변).
#   ★gap/min_seg를 반드시 함께 해제해야 함 — 지표만 바꾸면 Calmar가 오히려 악화.
#   ⚠️2026-07-31 정정: 초기 설명 "adj_gap이 min_seg를 이미 내포 → 중복"은 틀렸음.
#     실측 상관 adj_gap↔min_seg −0.015 / adj_gap↔gap −0.026 = 사실상 무상관(중복 아님).
#     ★진짜 이유 = 두 게이트 모두 '모멘텀 성격의 요구조건'이라 가치 순위와 충돌:
#       gap>=1.5  "선행EPS가 후행 1.5배 이상"(고성장 요구) — top5 슬롯의 34% 차단
#       min_seg>=0 "4구간 전부 전망 안 꺾임"(상향 지속 요구) — 8% 차단
#     잘리는 종목 = META·HD·ACN·GOOGL·ORCL 등 성숙 우량주(일간 변동성 2.54%),
#     대체 투입 = MU·AVGO·MCHP 등 고성장주(5.73% = 2.3배).
#     → 자르면 수익은 오르나(5일 +3.58% vs +2.13%) 낙폭이 더 커짐(MDD −21.1 vs −17.2).
#     즉 '모멘텀 전략의 부품을 가치 전략에 달아둔 것'을 떼어낸 변경.
#     production 게이트 실측(R5·위상평균): gapON/segON 8.97 → gapOFF/segON 15.52
#       → gapON/segOFF 14.80 → 둘다OFF 16.97 (LOWO 5/5 둘다OFF 우세).
#   ⚠️min_seg는 원래 '전망 꺾이는 종목 배제' 안전규칙 — 강세장이라 8%밖에 안 걸렸을 뿐.
#     하락장에서 가치함정 사고 시 되살릴 1순위(아래 _GAP_MODE 조건 제거하면 복원).
#     research/_gate_why_2026_07_31.py
#   한계: 5.5개월 단일 강세장(진짜 약세장 미검증), 상승 구간(~5/15·6/15·7/15)은 rev90 우위,
#   회전율 2.6배(리밸당 0.90→2.31종목, 20bp 반영 후에도 Calmar 16.14>9.15 유지).
# VM_US_ONLY='1'     : KR 다리 제외(미국 종목만). 통합 BT는 KR DB가 40일·유니버스 3회 급변
#   (73→390→535)이라 측정 불가 → KR 백분위 보정을 adj_gap으로 검증할 방법이 없음.
#   미국만이면 그 미검증 의존이 사라져 위 5.5개월 BT가 곧 실제 나갈 물건이 됨.
#   (KR 노출은 별도 KR 시스템 48%가 담당 — 이 트랙에서 빼도 노출 소멸 아님 +
#    CLAUDE.md 통합 도입 시 명시한 대가 '메모리 테마 이중집중' 해소)
# 롤백 = env 제거. 원장은 strategy 컬럼으로 자기기술(과거 행 소급 재작성 없음).
# 이중상장(동일 기업 복수 클래스) — 같은 회사에 2슬롯(=40%) 나가는 것 방지. 상위 1개만 남김.
#  2026-07-31 발견: gap 모드 첫 시행에서 GOOGL·GOOG가 4·5위 동시 진입(BT 23회 리밸에선 0회라
#  기존 결과 오염은 없으나, 라이브 재발 시 분산 파괴 → 지표와 무관한 구조 가드로 상시 적용).
DUAL_CLASS = {'GOOG': 'GOOGL', 'FOXA': 'FOX', 'NWSA': 'NWS', 'UAA': 'UA',
              'BRK-B': 'BRK-A', 'LEN-B': 'LEN', 'HEI-A': 'HEI', 'BF-B': 'BF-A'}


def _dedup_dual_class(merged):
    seen, out, dropped = set(), [], []
    for d in merged:
        key = DUAL_CLASS.get(d['ticker'], d['ticker'])
        if key in seen:
            dropped.append(d['ticker']); continue
        seen.add(key); out.append(d)
    return out, dropped


# ★min_seg 임계 (2026-07-31 재스윕, exec_lag=1 정직 기준).
#   전망 4구간 중 최악이 이 값 미만이면 제외 = '실적 전망이 꺾이는 종목' 배제 안전규칙.
#   스윕(괴리율 순위·N5·R5·위상평균): 해제 13.00 / -5% 14.07 / -2% 14.43 / 0% 12.97
#   ★가장 엄격한 0%가 네 값 중 최악 — 강세장에서도 한 구간만 살짝 꺾인 정상주를 과다 배제.
#   -2% 채택 근거: LOWO 5/5 전승(14.43/11.22/10.99/7.45/7.69 vs 현행 12.97/10.72/9.50/6.94/7.09),
#     랜덤 진입일 150회 수익 승률 65%(+1.51%p), MDD 중립(+0.16%p), -5%와 인접 고원.
#   ⚠️정직하게: 이 표본(강세장 5.5개월)에서 이 게이트의 '보호' 효과는 입증되지 않았다
#     (스트레스 구간 MDD가 해제·-5%·-2%·0% 전부 -12.1~-12.2로 동일).
#     -2%를 쓰는 근거는 수익이고, 게이트를 아예 없애지 않는 근거는 '검증 못 한 지속형 하락장 대비'다.
#     괴리율은 주가가 빠진 종목을 우선 담으므로, 실적까지 꺾이는 종목(가치함정)을 거르는 장치는 남긴다.
#   ⚠️PE<=20도 헤드라인 14.83으로 후보였으나 LOWO에서 붕괴(9.70/9.42/4.78/4.45)해 기각 — PE는 30 유지.
#   ⚠️거래대금은 값별로 12.0/21.5/15.4/13.0/16.0/14.5로 들쭉날쭉(고원 없음) → 손대지 않음.
#   research/_gate_sweep_lag1_2026_07_31.py
MIN_SEG_THR = float(os.environ.get('VM_MIN_SEG_THR', '-2.0'))
# 최근 30일 전망 상향 애널 최소 인원 (0 = 해제). 근거는 us_candidates 게이트 주석 참조.
REV_UP30_MIN = int(os.environ.get('VM_REV_UP30_MIN', '1'))  # ★2026-08-02 3→1 (사용자 '수치로 증명되면 바로 적용'): 하나-빼기 실측에서 ≥3이 유일한 실측 마이너스(Cal 17.85→20.70). ≥1=≥0과 동일 성능(20.68)이면서 실패모드1(상향 0명 NXPI형) 방어는 유지 — 해로운 부분(3명 요구)만 제거
# ★매출성장 게이트 (2026-08-03 복원, 사용자 "시스템 시작일부터 공들여 넣었던 필터를 왜 뺐어").
#   구시스템 하드필터(rev_growth<10% 제외, v44~)가 7/5 VM 트랙 신축 때 이식 누락됐던 것 —
#   NXT(매출 −5% 역성장인데 EPS 전망 +17%)가 top10에 들어오며 발각. 실패모드 정당성:
#   매출 없이 EPS 전망만 오르는 건 마진 스토리 = 괴리율 신호의 유효영역 밖(PER30 캡과 같은 범주).
#   측정(research/_stop_revg_matrix_2026_08_03.py, exec_lag=1·위상평균): 스탑 4방식 전부에서 개선
#   (현행 체인 Cal 16.09→19.79, LOWO 8.77→10.23, 스탑없음 15.56→21.47 — 4/4 문맥 무관 유익).
#   missing=pass(MU처럼 데이터 공백인 정당 종목 학살 방지 — SNDK gap 교훈과 동일 규약).
REVG_MIN = float(os.environ.get('VM_REVG_MIN', '0.10'))  # 0이면 해제
VM_STRATEGY = os.environ.get('VM_STRATEGY', 'rev90')
VM_US_ONLY = os.environ.get('VM_US_ONLY', '0') == '1'
_GAP_MODE = (VM_STRATEGY == 'gap')


def _adj_gap_map(conn, date):
    """해당 일자 {ticker: adj_gap}. adj_gap = fwd_pe_chg×(1+dir)×eps_quality (낮을수록 좋음)."""
    return {tk: float(v) for tk, v in conn.execute(
        'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (date,))}


def _score(d):
    """순위 점수 — 항상 '클수록 좋음'으로 통일 (하류 정렬 로직 무변경)."""
    if _GAP_MODE:
        a = d.get('adj_gap')
        return None if a is None else -a
    return d['rev90']


def _seg(a, b):
    # 인접 창 변화율(%). 한쪽이라도 0/None/음수(수집 글리치·결측)면 '변화 없음'(0)으로 처리 —
    # 야후가 특정 필드를 순간 0으로 뱉는 사고를 '이익 -100% 붕괴'로 오인해 정상 종목을
    # min_seg 게이트에서 부당 탈락시키던 버그 방지 (2026-07-14 하이닉스 ntm_60d=0 사고).
    if not a or not b or a <= 0 or b <= 0:
        return 0.0
    return (a - b) / abs(b) * 100


def _carry_forward_windows(conn, today):
    """오늘 0/None으로 글리치된 EPS 창을 종목별 '직전 유효값'으로 대체하기 위한 맵.
    야후 순간 수집 실패(2026-07-14 KR 34·US 12종목 관측)가 재발해도 정상 종목이
    min_seg/rev90에서 탈락하지 않도록 — 하이닉스 n60=0 부당탈락 재발 방지.
    반환: {ticker: {col: 오늘 이전 최신 유효값}}."""
    cols = ('ntm_current', 'ntm_7d', 'ntm_30d', 'ntm_60d', 'ntm_90d')
    m = {}
    for row in conn.execute(
            'SELECT ticker,' + ','.join(cols) + ' FROM ntm_screening WHERE date < ? ORDER BY date',
            (today,)):
        d = m.setdefault(row[0], {})
        for i, col in enumerate(cols):
            v = row[1 + i]
            if v and v > 0:
                d[col] = v
    return m


def _cf(v, tk, col, cf):
    """v가 0/None 글리치면 carry-forward 맵의 직전 유효값으로 대체(없으면 원값 유지)."""
    if v and v > 0:
        return v
    return cf.get(tk, {}).get(col, v)


def _fx_usdkrw():
    try:
        import yfinance as yf
        v = yf.Ticker('KRW=X').fast_info.last_price
        if v and 900 < v < 2500:
            return float(v)
    except Exception:
        pass
    return 1380.0


def _kr_ttm_eps(t6, shares):
    """DART fs_dart 분기 지배순이익(억원) TTM ÷ 주식수. conviction_fusion_tracker.ttm_eps 로직 준용."""
    import pandas as pd
    p = f'{KR_FS_DIR}/fs_dart_{t6}.parquet'
    if not os.path.exists(p) or not (shares and shares > 0):
        return None
    try:
        # ★2026-07-13: engine 하드코딩 금지 4번째 지점 통일 — 집PC pyarrow가 fs_dart를 못 읽어
        #   (Repetition level histogram mismatch) 로컬 실행 gap 0/48 전멸하던 원인. HY-OAS 7/5 수리 동일 패턴.
        from daily_runner import _read_parquet_robust
        fs = _read_parquet_robust(p)
        fs['rcept_dt'] = pd.to_datetime(fs['rcept_dt'], errors='coerce')
        for acct in ('지배주주당기순이익', '당기순이익'):
            q = fs[(fs['공시구분'] == 'q') & (fs['계정'] == acct) & (fs['rcept_dt'].notna())].sort_values('rcept_dt')
            v = q['값'].astype(float).values
            if len(v) >= 4:
                return (v[-4:].sum() * 1e8) / shares
    except Exception:
        return None
    return None


def us_candidates():
    import daily_runner as dr
    TC = json.load(open(os.path.join(HERE, 'ticker_info_cache.json'), encoding='utf-8'))
    BAD = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
    BAD_TK = set(dr.COMMODITY_TICKERS)

    def ind_ok(tk):
        if tk in BAD_TK:
            return False
        v = TC.get(tk)
        ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
        return not (isinstance(ind, str) and ind in BAD)

    # 2026-07-09: 전수검사 전환 — full 캐시(1,445종목) 우선, 없으면 구 sparse 폴백 (프로덕션 _vm_trailing_eps 패리티)
    _te_full = os.path.join(HERE, 'data_cache', 'trailing_eps_ttm_full.json')
    _te_path = _te_full if os.path.exists(_te_full) else os.path.join(HERE, 'data_cache', 'trailing_eps_ttm.json')
    TE = json.load(open(_te_path, encoding='utf-8'))
    conn = sqlite3.connect(os.path.join(HERE, 'eps_momentum_data.db'))
    c = conn.cursor()
    last = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    # 안전필터 패리티 (2026-07-09 production A군): OM/FCF/ROE는 회전수집이라 60일 carry-forward
    fund = {}
    for tk, om, fcf, roe in c.execute(
            "SELECT ticker, operating_margin, free_cashflow, roe FROM ntm_screening "
            "WHERE date<=? AND date>=date(?, '-60 day') ORDER BY date", (last, last)):
        e = fund.setdefault(tk, [None, None, None])
        if om is not None: e[0] = om
        if fcf is not None: e[1] = fcf
        if roe is not None: e[2] = roe
    cf = _carry_forward_windows(conn, last)  # 글리치 0값 → 직전 유효값 대체 (재발 방지)
    AGM = _adj_gap_map(conn, last)           # 괴리율(gap 모드 순위 기준)
    # ★2026-08-01 관찰컬럼용 이력 (매매 개입 0, REDESIGN_DEBATE_2026_08_01.md):
    #   px_chg20 = 20거래일 주가 변화% (약형 괴리 '주가 하락산' 판별)
    #   crash5   = 5거래일 −18%+ 급락 & 추정 유지(−2% 이내) 플래그 (크래시셀 라이브 forward 추적)
    #   07-13 stale/below_ma120 컬럼과 같은 선례 — 게이트 아님, 판정일 판단용 기록.
    _hd = [r[0] for r in conn.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE date<=? ORDER BY date DESC LIMIT 31', (last,))]
    _d20 = _hd[20] if len(_hd) >= 21 else None
    _d5 = _hd[5] if len(_hd) >= 6 else None
    # dd_30_25용 30세션 고점 (오늘 포함 30개 날짜의 최고가 — v84 high30 정의)
    HI30 = {}
    for _dd_ in _hd[:30]:
        for _t, _p in conn.execute('SELECT ticker, price FROM ntm_screening WHERE date=? AND price IS NOT NULL', (_dd_,)):
            if _p and _p > HI30.get(_t, 0):
                HI30[_t] = _p
    PX20 = {t: p for t, p in conn.execute(
        'SELECT ticker, price FROM ntm_screening WHERE date=? AND price IS NOT NULL', (_d20,))} if _d20 else {}
    PX5, NC5 = {}, {}
    if _d5:
        for _t, _p, _n in conn.execute('SELECT ticker, price, ntm_current FROM ntm_screening WHERE date=?', (_d5,)):
            if _p: PX5[_t] = _p
            if _n: NC5[_t] = _n
    out = []
    for tk, p, nc, n7, n30, n60, n90, dv, na, m120, ru30, rg in c.execute(
            'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,'
            'num_analysts,ma120,rev_up30,rev_growth FROM ntm_screening '
            'WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)):
        n7, n30 = _cf(n7, tk, 'ntm_7d', cf), _cf(n30, tk, 'ntm_30d', cf)
        n60, n90 = _cf(n60, tk, 'ntm_60d', cf), _cf(n90, tk, 'ntm_90d', cf)
        if not ind_ok(tk):
            continue
        _dvmin = DV_MIN_MUSD if last >= DV_EPOCH else 1000.0
        if dv is None or dv < _dvmin:
            continue
        # ★min_seg = 항상 적용. 2026-07-31 오전 gap과 함께 해제했다가 같은 날 복원.
        #   해제 근거가 부실했음: 직접 관측 9건(top5의 8%)뿐이고, 그 8%는 강세장이라 꺾이는
        #   종목이 거의 없어서지 비용이 없어서가 아님 — 보험금 안 나가는 구간에서 보험료를 재고
        #   '싸다'고 한 셈. ★특히 괴리율은 '주가가 빠진 종목'을 우선 담는 지표이고 min_seg는
        #   '빠지면서 실적도 꺾이는 종목'(가치함정)을 막는 유일한 장치라, 동시에 하면 가장
        #   위험한 조합이 된다. 그 구간(지속형 하락장)은 표본에 없다. 사후 복원은 대비가 아니므로 선복원.
        # gap(선행EPS>=후행 1.5배)은 해제 유지 — 그건 '성장률 요구'라 가치함정을 못 막고,
        #   저변동 우량주(META·HD·GOOGL, 변동성 2.54% vs 대체 5.73%)를 34% 잘라내는 부작용이 크다.
        if min(_seg(nc, n7), _seg(n7, n30), _seg(n30, n60), _seg(n60, n90)) < MIN_SEG_THR:
            continue
        if nc <= 0 or (n90 or 0) <= 0.1:
            continue
        if p / nc > PE_MAX:
            continue
        rec = TE.get(tk)
        te = rec[-1][1] if rec else None
        # ★2026-08-01 TE 통화 단위 가드: TSM의 TTM EPS가 현지통화(NT$371.9)로 저장돼
        #   있는데 NTM은 ADR 달러($19.6) → gap 0.05, 카드에 "작년의 0.1배" 발송 사고.
        #   판별 = 묵시 트레일링PER(주가÷TTM) < 3배는 통화 불일치/데이터 오류로 간주
        #   (실제 그런 초저PER 대형주는 존재하지 않음. TSM: 404÷371.9 = 1.1배).
        #   해당 TE는 무효 처리 → gap=None(missing=pass 규약과 동일, '배' 표시도 자동 생략).
        #   ⚠️같은 오류가 rev90 시절엔 gap>=1.5 게이트로 TSM류 ADR을 부당 배제하고 있었음.
        if te and te > 0 and p / te < 3.0:
            te = None
        g = (nc / te) if (te and te > 0) else None
        if not _GAP_MODE and g is not None and g < GAP_MIN:
            continue
        # A군 안전필터 (production _vm_pick 패리티): 동전주·저커버·마진<5%·FCF/ROE 동시음수·rev90>0
        # ★rev_up30 게이트 (2026-07-31 복원). 최근 30일 전망을 올린 애널이 REV_UP30_MIN명 미만이면 제외.
        #   계기: 사용자 "NXPI는 정보부족이면서 뭘 5등을 추천해?" — 실측 TOP5 중 NXPI만 상향 0명
        #   (VRT 2·STX 5·SNDK 6·GOOGL 4). 기존 체인은 '애널 커버 수'만 보고 '최근 상향 활동'은 안 봤다.
        #   구시스템 v80.8에 있던 필터('단일 분석가 의존 종목 차단', WELL 사례)를 되살린 것.
        #   스윕(exec_lag=1·괴리율·N5·R5·위상평균, research/_revup30_gate_2026_07_31.py):
        #     미적용 Cal 14.28 / >=1 14.28 / >=2 16.99 / >=3 17.21 / >=5 17.09  (2~5가 고원)
        #     ★>=1은 무의미하고 >=2부터 계단 — 2·3·5가 평평해 뾰족한 봉우리가 아님.
        #     LOWO 5/5 전승(예: ex-MU 11.21 -> 14.87), 랜덤 진입일 MDD 승률 79%.
        #     MDD -19.7% -> -15.8%, 수익 -2.2%p. 후보 풀 58 -> 36종목.
        #   ★3 채택 이유: 헤드라인 최고 + 고원 중심 + 구시스템이 독립적으로 도달한 값과 일치.
        #   env VM_REV_UP30_MIN(0이면 해제).
        if (ru30 or 0) < REV_UP30_MIN:
            continue
        _na_min = NA_MIN if (_GAP_MODE and last >= TRAP_EPOCH) else 3
        if p < 10 or (na or 0) < _na_min or _seg(nc, n90) <= 0:
            continue
        om, fcf, roe = fund.get(tk, (None, None, None))
        if om is not None and om < 0.05:
            continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0:
            continue
        # ★매출성장 게이트 (상단 REVG_MIN 주석 참조). missing=pass.
        if rg is not None and rg < REVG_MIN:
            continue
        # rev30/below_ma120/px_chg20/crash5/na/up30 = 관찰 전용 원장 컬럼 (매매 개입 0)
        _p20, _p5, _n5 = PX20.get(tk), PX5.get(tk), NC5.get(tk)
        # 가치함정 게이트 (상단 TRAP_* 주석 참조): 괴리가 '주가 하락만'으로 만들어진 후보 컷.
        if (_GAP_MODE and TRAP_GATE_ON and last >= TRAP_EPOCH
                and _seg(nc, n30) <= TRAP_REV30 and _p20 is not None and p < _p20):
            continue
        out.append(dict(ticker=tk, market='US', rev90=_seg(nc, n90), fwd_per=p / nc,
                        gap=g, dv_musd=dv, price=p, rev30=_seg(nc, n30),
                        adj_gap=AGM.get(tk),
                        below_ma120=(int(p < m120) if m120 else None),
                        px_chg20=((p / _p20 - 1) * 100 if _p20 else None),
                        crash5=(int((p / _p5 - 1) <= -0.18 and (nc / _n5 - 1) >= -0.02)
                                if (_p5 and _n5 and _n5 > 0) else None),
                        na=na, up30=(ru30 or 0),
                        dd30=((p / HI30[tk] - 1) * 100 if HI30.get(tk) else None)))
    conn.close()
    if _GAP_MODE:  # 괴리율 결측 종목은 순위 산정 불가 → 후보 제외 (US 결측률 4.9%)
        out = [d for d in out if d.get('adj_gap') is not None]
    return last, out


def kr_candidates(fx):
    """반환: (last_date, 후보리스트, health). health = 수집/게이트 건강성 (2026-07-10 감사수리:
    ①7/9 GH Actions 샘플 실행에서 fs_dart parquet 부재 → gap 전원 None → missing=pass로
    KR 가치게이트가 조용히 전멸했던 사고 감지 ②KR yf 수집 붕괴(210→73) 감시)."""
    conn = sqlite3.connect(KR_DB)
    c = conn.cursor()
    last = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    rows = c.execute(
        'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,market_cap,num_analysts,ma120 '
        'FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)).fetchall()
    cf = _carry_forward_windows(conn, last)  # 글리치 0값 → 직전 유효값 대체 (재발 방지)
    conn.close()
    health = {'today_n': sum(1 for r in rows if r[2] and r[2] > 0 and (r[6] or 0) > 100),
              'gap_reach': 0, 'gap_computed': 0, 'warnings': []}
    if not os.path.isdir(KR_FS_DIR):
        health['warnings'].append(f'KR 재무 폴더 없음({KR_FS_DIR}) — 가치게이트(gap) 전면 미작동')
    # 안전필터 패리티 (KR도 동일): OM/FCF/ROE carry-forward
    kconn = sqlite3.connect(KR_DB)
    kfund = {}
    for tk_, om_, fcf_, roe_ in kconn.execute(
            'SELECT ticker, operating_margin, free_cashflow, roe FROM ntm_screening ORDER BY date'):
        e = kfund.setdefault(tk_, [None, None, None])
        if om_ is not None: e[0] = om_
        if fcf_ is not None: e[1] = fcf_
        if roe_ is not None: e[2] = roe_
    KAGM = _adj_gap_map(kconn, last)  # 괴리율(gap 모드 순위 기준)
    kconn.close()
    pre = []
    for tk, p, nc, n7, n30, n60, n90, mc, na, m120 in rows:
        n7, n30 = _cf(n7, tk, 'ntm_7d', cf), _cf(n30, tk, 'ntm_30d', cf)
        n60, n90 = _cf(n60, tk, 'ntm_60d', cf), _cf(n90, tk, 'ntm_90d', cf)
        if tk in KR_HOLDCO or tk in KR_IND_BLOCK:
            continue
        if (na or 0) < 5:
            continue
        if min(_seg(nc, n7), _seg(n7, n30), _seg(n30, n60), _seg(n60, n90)) < MIN_SEG_THR:
            continue
        # 저분모 가드 100원 (2026-07-10 감사수리: 구 0.1은 USD용 임계를 원화에 그대로 써 무가드)
        if nc <= 0 or (n90 or 0) <= 100:
            continue
        if p / nc > PE_MAX:
            continue
        if _seg(nc, n90) <= 0:
            continue
        om, fcf, roe = kfund.get(tk, (None, None, None))
        if om is not None and om < 0.05:
            continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0:
            continue
        shares = (mc / p) if (mc and p) else None
        te = _kr_ttm_eps(tk.split('.')[0], shares)
        g = (nc / te) if (te and te > 0) else None
        health['gap_reach'] += 1
        if g is not None:
            health['gap_computed'] += 1
        if not _GAP_MODE and g is not None and g < GAP_MIN:
            continue
        pre.append(dict(ticker=tk, market='KR', rev90=_seg(nc, n90), fwd_per=p / nc,
                        gap=g, dv_musd=None, price=p, mc=mc, rev30=_seg(nc, n30),
                        adj_gap=KAGM.get(tk),
                        below_ma120=(int(p < m120) if m120 else None)))
    if health['gap_reach'] >= 3 and health['gap_computed'] == 0:
        health['warnings'].append(
            f"KR 가치게이트(gap) 계산 0/{health['gap_reach']}건 — 재무 데이터 접근 실패 의심, "
            'missing=pass 규칙으로 KR 전원이 무검사 통과 중')
    # 거래대금 게이트: yf 30일 평균 거래대금 → USD. 실패 시 시총>=13조 프록시.
    if pre:
        try:
            import yfinance as yf
            hist = yf.download([d['ticker'] for d in pre], period='2mo', threads=2,
                               progress=False, auto_adjust=False)
            for d in pre:
                try:
                    cl = hist['Close'][d['ticker']].dropna()
                    vo = hist['Volume'][d['ticker']].dropna()
                    dvk = (cl * vo).tail(30).mean()
                    d['dv_musd'] = float(dvk) / fx / 1e6
                except Exception:
                    d['dv_musd'] = None
        except Exception:
            pass
        if all(d['dv_musd'] is None for d in pre):
            health['warnings'].append('KR 거래대금(yf) 전건 조회 실패 — 시총 13조 프록시로만 필터 중')
    out = []
    for d in pre:
        # ★KR 유동성 하한 = KR_DV_MIN_MUSD($0.3B, 2026-07-09 사용자 승인 '각 시장 상위 ~10%'
        #   백분위 등가 — KR_DV_PARITY_2026_07_09.md). (구 주석의 $100M은 폐기된 특례 —
        #   2026-07-10 감사에서 주석 부패 정정.)
        if d['dv_musd'] is not None:
            if d['dv_musd'] < KR_DV_MIN_MUSD:
                continue
        elif (d.get('mc') or 0) < 13e12:  # dv 조회 실패 시 시총 13조 프록시($1B/일 체급)
            continue
        d.pop('mc', None)
        out.append(d)
    if _GAP_MODE:  # 괴리율 결측 종목은 순위 산정 불가 → 후보 제외
        out = [d for d in out if d.get('adj_gap') is not None]
    return last, out, health


def _universe_rev90(db, dv_min=None, n90_floor=0.1, window_days=30):
    """해당 시장 투자가능 유니버스의 rev90 분포 (백분위·z 환산용).

    ★2026-07-10 감사수리 — 분모 = '당일 스냅샷'이 아니라 **최근 window_days 내 관측된
    종목별 최신 유효 행(트레일링 유니온)**. 당일 분모는 KR yf 수집 붕괴(유효 6/1 184 →
    7/9 69, 탈락종목이 체계적으로 차가움: 탈락 중앙값 rev90 +11 vs 생존 +17)로 뜨거운
    생존자만 남아 KR 백분위를 ~5%p 하향 왜곡했음(삼성 84.1%ile vs 온전분모 89.1%ile —
    top5 경계 여유와 같은 스케일). 트레일링 유니온은 attrition에 강건.
    dv_min(백만$) 지정 시 종목별 최신 행의 dollar_volume_30d로 필터(US $1B 유동주 패리티).
    KR은 dv 컬럼 부재라 전체(수집 자체가 애널 커버 엘리트, CROSS_MARKET_NORM_2026_07_09.md).
    n90_floor: 저분모 rev90 폭발 가드 — 구현이 0.1을 양국 동일 적용해 원화(KR)엔 사실상
    무가드였음 → KR 호출부는 100(원)을 넘길 것.
    """
    c = sqlite3.connect(db)
    dt = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    has_dv = any(r[1] == 'dollar_volume_30d' for r in c.execute("PRAGMA table_info(ntm_screening)"))
    dv_col = ', dollar_volume_30d' if has_dv else ', NULL'
    rows = c.execute(
        f'SELECT ticker, ntm_current, ntm_90d{dv_col}, adj_gap FROM ntm_screening '
        'WHERE date>=date(?, ?) AND ntm_current>0 AND ntm_90d>? ORDER BY date',
        (dt, f'-{int(window_days)} day', n90_floor)).fetchall()
    c.close()
    latest = {}
    for tk, nc, n90, dv, ag in rows:  # ORDER BY date라 뒤 행이 최신 — 종목별 최신 행만 남김
        latest[tk] = (nc, n90, dv, ag)
    vals = []
    for nc, n90, dv, ag in latest.values():
        if dv_min is not None and has_dv and (dv is None or dv < dv_min):
            continue
        if _GAP_MODE:  # gap 모드 분모도 같은 척도(괴리율)로 — 지표·분모 불일치 방지
            if ag is None:
                continue
            vals.append(-float(ag))
        else:
            vals.append((nc - n90) / abs(n90) * 100)
    return vals


def _dist_med_mad(vals):
    import statistics as _s
    med = _s.median(vals)
    mad = _s.median([abs(v - med) for v in vals]) or 1e-9
    return med, mad


def compute():
    fx = _fx_usdkrw()
    us_date, us = us_candidates()
    if VM_US_ONLY:
        # KR 다리 제외 — 통합+gap은 KR DB 40일·유니버스 3회 급변으로 검증 불가(2026-07-31).
        kr_date, kr, kr_health = None, [], {'today_n': None, 'warnings': []}
    else:
        kr_date, kr, kr_health = kr_candidates(fx)
    merged = sorted(us + kr, key=lambda d: -(_score(d) or -9e9))
    meta = {'norm': 'pct', 'strategy': VM_STRATEGY, 'us_only': VM_US_ONLY,
            'warnings': list(kr_health.get('warnings') or []),
            'kr_today_n': kr_health.get('today_n'), 'base_n': {},
            # 가치함정 게이트 활성 여부 — 메시지 소개문이 실제 필터와 어긋나지 않게(에폭 전
            # 재안내일에 "거른다"고 말해놓고 대기조에 FN류가 보이는 자기모순 방지).
            'trap_active': bool(_GAP_MODE and TRAP_GATE_ON and us_date >= TRAP_EPOCH)}
    # ★2026-08-02 유동성 국면 가드 (사용자 "시장 유동성이 변해 dv 기준이 안 맞으면?"):
    #   dv는 절대값(체결 산수)이라 시장 유동성이 마르면 후보 수 감소로 나타난다 — 그걸 보이게.
    #   평시 게이트 통과 100~170개, 30 미만이면 top5 품질 저하 신호 → 데이터 품질 경고(매매 개입 0).
    if _GAP_MODE and len(us) < 30:
        meta['warnings'].append(
            f'게이트 통과 후보 {len(us)}개뿐(평시 100~170) — 시장 유동성 위축 또는 수집 이상. '
            f'거래대금 기준(${DV_MIN_MUSD:.0f}M) 재점검 필요')
    if _GAP_MODE and not VM_US_ONLY:
        meta['warnings'].append(
            '괴리율(gap) 전략 + KR 편입 = 미검증 조합 — KR 백분위 보정이 rev90 기준으로만 '
            '검증됨(괴리율은 한·미 중앙값 +0.7 vs −20.4로 격차가 커 보정 의존도 급증)')
    # 백분위 결합 = 본선 (2026-07-09 사용자 승인, CROSS_MARKET_NORM 연구): rev90 절대값이
    # 아니라 자기 시장 '유동성 유니버스' 내 백분위로 환산해 결합 — KR 리비전 인플레 보정.
    # (실측: 횡단면 중앙값 US +4.3% vs KR +17.0%, MAD 3.4 vs 13.5. LG이노텍 +50.6%=KR
    #  82%ile vs HPE +49.2%=US 94%ile → 절대값이 아니라 백분위로 재야 HPE 우위.)
    # denominator = 각 시장 애널 커버 전체(무필터), 30일 트레일링 유니온.
    # ★2026-07-10 2차 감사수리(사용자 "둘 다 거래대금 조건을 걸거나 둘 다 안 걸어야"):
    #   구 스펙은 US만 $1B 필터(121) / KR 무필터 = 잣대 둘 — 이 비대칭이 5위(삼성 vs FLEX)를
    #   결정하고 있었음. 대칭 대안 중 '둘 다 필터'는 KR이 20종목(등수 1칸=5%p)이라 통계 불능 →
    #   '둘 다 무필터' 채택. 근거: ①정규화의 근거 측정(US +4.4 vs KR +10.1 중앙값)부터 무필터
    #   전체끼리 잰 것(증거-구현 일관성) ②유동성은 후보 게이트(US $1B/KR $0.3B)가 이미 담당 —
    #   자(분모)에 또 섞으면 개념 이중적용 ③top4(SNDK·MU·HPE·하이닉스)는 잣대 무관 확고,
    #   5위는 razor-thin(판정일 재확인 항목). 상세: research/AUDIT_FIXES_2026_07_10.md 2차.
    try:
        uus = _universe_rev90(os.path.join(HERE, 'eps_momentum_data.db'))
        ukr = _universe_rev90(KR_DB, n90_floor=100.0) if not VM_US_ONLY else []
        meta['base_n'] = {'US': len(uus), 'KR': len(ukr)}
        if not VM_US_ONLY and len(ukr) < 30:
            meta['warnings'].append(f'KR 백분위 분모 {len(ukr)}종목뿐 — 순위 신뢰 낮음')
        kt = kr_health.get('today_n') or 0
        if kt and (kt < 60 or kt < 0.6 * len(ukr)):
            meta['warnings'].append(
                f'KR 수집 부실: 오늘 {kt}종목 (최근 30일 관측 {len(ukr)}종목) — KR 순위 참고만')
        mus, dus = _dist_med_mad(uus)
        mkr, dkr = _dist_med_mad(ukr) if ukr else (0.0, 1e-9)  # US_ONLY 시 KR 분모 없음
        for d in merged:
            base, med, mad = (uus, mus, dus) if d['market'] == 'US' else (ukr, mkr, dkr)
            sc = _score(d)
            d['pct'] = sum(1 for v in base if v < sc) / len(base) * 100
            d['rz'] = (sc - med) / mad * 0.6745  # robust-z 병기 관찰
        merged.sort(key=lambda d: (-d['pct'], -(_score(d) or -9e9)))
        merged, _dropped = _dedup_dual_class(merged)
        if _dropped:
            print('[이중상장 중복 제거] %s' % ', '.join(_dropped))
    except Exception as e:
        # 2026-07-10 감사수리: 조용한 폴백 금지 — 본선 결합 방식이 바뀌면 메시지에 명시
        meta['norm'] = 'abs_fallback'
        meta['warnings'].append(f'백분위 환산 실패 → 절대 상향폭 순위로 임시 결합됨: {e}')
        print(f'[!!] 백분위 환산 실패 — 절대 rev90 결합으로 폴백: {e}')
    return us_date, kr_date, fx, merged, meta


def _git_sha():
    """실행 코드 버전 — 메시지·로그에 박아 '낡은 코드로 발송' 사고를 사후 식별 가능하게 (감사수리 3)."""
    try:
        import subprocess
        return subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True,
                              text=True, cwd=HERE, timeout=10).stdout.strip()
    except Exception:
        return ''


def _us_grid():
    """리밸 시계의 단일 기준 = US 거래일 그리드(앵커 2026-07-02, R5).
    2026-07-10 감사수리: 표시(is_rebal)는 이 그리드, NAV 리플레이는 '로그 실행일 인덱스 i%5'로
    서로 다른 시계였음(지시한 매매와 표시한 누적 성과가 다른 날 리밸) → 전부 이 그리드로 통일."""
    c = sqlite3.connect(os.path.join(HERE, 'eps_momentum_data.db'))
    usd = [x[0] for x in c.execute(
        "SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL AND date>='2026-07-02' ORDER BY date")]
    c.close()
    return usd


def _ledger_blocks(rows):
    """일자별 마지막 완전 블록(rank==1 시작) — 같은 날 중복 실행(수동+schtask) dedup.
    구 cmd_nav는 dedup 없이 전 행을 써 중복 시 보유 비중이 왜곡됐음(감사수리 4)."""
    days = sorted({r['run_date'] for r in rows})
    blocks = {}
    for d in days:
        day = [r for r in rows if r['run_date'] == d]
        st = [i for i, r in enumerate(day) if r.get('rank') == '1']
        blocks[d] = day[st[-1]:] if st else day
    return days, blocks


def _replay(rows):
    """원장 리플레이 — 단일 리밸 시계(US 그리드)로 보유·NAV 재구성. 로컬통화 수익률(FX 미반영) 근사.
    반환: {'nav', 'days', 'state': {day: {'is_rebal', 'held_before', 'held_after'}}, 'ew_last'}
    held_before = 그날 리밸 직전 보유(교체 diff의 올바른 기준), held_after = 리밸 반영 후.
    ★국면 오버레이 (2026-07-10 사용자 승인): US 메인의 검증된 방어 신호(S&P200일선
    15일확인 OR VIX36 OR HY-OAS, daily_runner._regime_defense_series 재사용)를 날짜별
    주식비중으로 반영 — 방어일 수익 = ret×weight(0.0=현금). 지시(전량 현금)와 NAV 정합."""
    days, blocks = _ledger_blocks(rows)
    usd = _us_grid()
    # 날짜별 국면 주식비중 (실패 시 전부 1.0 = 기존과 동일)
    ew = {}
    try:
        import daily_runner as dr
        uds = sorted({blocks[d][0].get('us_date') for d in days if blocks.get(d)} - {None})
        if uds:
            ew, _ = dr._regime_defense_series(uds)
    except Exception as _e:
        print(f'[국면 시리즈 스킵(전부 주식100% 가정): {_e}]')
    nav, hold, ppx = 1.0, [], {}
    state = {}
    ew_last = 1.0
    pend = None   # ★2026-07-31 실행지연: 신호는 미국장 D일 종가로 계산되고 고객은
                  #   KST D+1 아침에 받아 그날 밤 미국장에서 체결한다. 즉 D일 종가에
                  #   즉시 매수한 것으로 계산하면 불가능한 이득이 섞인다(감사 2026-07-31).
                  #   실측 영향: 표시 누적 -7.6% -> -8.7%.
    prev_ud = None   # ★2026-07-31: 같은 미국 거래일을 두 번 발송(토=예고/월=매매직전 재안내)해도
                     #   장부는 1회만 반영. 안 그러면 토요일 블록이 '교체 완료'로 보유를 갱신해
                     #   정작 매매하는 월요일에 "오늘 할 일 없음"이 떠 지시가 사라짐.
    for i, d in enumerate(days):
        day = blocks[d]
        px = {}
        for r in day:
            try:
                if r.get('price'):
                    px[r['ticker']] = float(r['price'])
            except (TypeError, ValueError):
                pass
        ud = day[0].get('us_date') if day else None
        if ud is not None and ud == prev_ud:
            # 동일 us_date 재발송 — 새 가격도 새 리밸도 없음. 직전 상태를 그대로 물려줘
            # 메시지가 같은 교체 지시를 반복하게 한다(장부·NAV 불변).
            # ★단, 아직 체결 전(pend)인 지시가 있으면 최신 블록의 TOP5로 목표를 갱신한다 —
            #   기준 변경 소급(2026-08-01 dv 에폭 7/31) 시 고객이 실제 체결하는 지시는
            #   마지막 발송분이므로 장부도 그걸 따라야 지시==NAV 정합이 유지된다.
            if pend is not None:
                _renew = [r['ticker'] for r in day if r.get('in_top4') == '1']
                if _renew:
                    pend = (pend[0], _renew)
            state[d] = dict(state[days[i - 1]])
            continue
        w = float(ew.get(ud, 1.0))
        if hold:
            rr = [px[t] / ppx[t] - 1 for t in hold if t in px and t in ppx and ppx[t] > 0]
            if rr:
                nav *= 1 + (sum(rr) / len(rr)) * w
        if pend is not None and pend[0] <= i:   # 지연 체결 (==가 아니라 <=: 신호일과 체결일 사이에
            hold = pend[1]; pend = None         # 재발송일이 끼면 그 날이 인덱스를 소비해 ==를 영영
                                                # 못 만나 교체가 장부에 반영 안 되던 잠복 버그 수리)
        gi = usd.index(ud) if ud in usd else None
        is_rb = (i == 0) or (gi is not None and gi % REBAL == 0)  # 첫 로그일 = 페이퍼 개시(초기 편입)
        held_before = list(hold)
        if is_rb:
            _new = [r['ticker'] for r in day if r.get('in_top4') == '1']
            if i == 0:
                hold = _new          # 페이퍼 개시일은 지연 없음(기준점)
            else:
                pend = (i + 1, _new)
        state[d] = {'is_rebal': is_rb, 'held_before': held_before, 'held_after': list(hold)}
        ppx.update(px)
        ew_last = w
        prev_ud = ud
    return {'nav': nav, 'days': days, 'state': state, 'ew_last': ew_last}


def _dd_blocked(d):
    """dd_30_25 진입 유예 대상 여부 (급락 소화 중 — 신규 진입만 차단).
    ★2026-08-03부터 기본 OFF(_DD_LIVE, 상단 DD_OFF_EPOCH 주석) — 7/31 이전 us_date 재현에만 잔존."""
    return _DD_LIVE and DD_ENTRY_ON and d.get('dd30') is not None and d['dd30'] <= DD_THR


def _sent_target(us_date):
    """이미 발송된 us_date의 목표를 원장에서 그대로 되읽는다 (없으면 None).
    ★2026-08-03 B1 수리: 재발송(토요일 지시 → 월요일 재안내)이 그 사이 바뀐 규칙으로
    재계산되어 **같은 교체일에 서로 다른 TOP5가 두 번 나가는** 사고가 실제로 발생했다
    (7/31: 토 AMKR·CLS·MU·STX·SNDK → 월 CLS·MU·STX·SNDK·KNX). 에폭 상수로 막으려 했으나
    그 분기가 호출되지 않는 死코드로 남아 있었음이 QC 감사에서 드러났다(내 검증은 목표
    출력만 보고 호출 경로를 확인하지 않았다).
    → 상수가 아니라 **사실**로 막는다: 원장은 발송 시에만 append되므로 '고객이 실제로 받은
    지시'의 유일한 소스다. 그 us_date가 원장에 있으면 어떤 규칙이 바뀌었든 그대로 재생한다."""
    try:
        if not os.path.exists(LOG):
            return None
        rows = [r for r in csv.DictReader(open(LOG, encoding='utf-8'))
                if r.get('us_date') == us_date]
        if not rows:
            return None
        _d, blocks = _ledger_blocks(rows)          # 같은 날 중복 실행 dedup
        last = blocks[sorted(blocks)[-1]]
        tg = [r['ticker'] for r in last if str(r.get('in_top4')) == '1']
        return tg or None
    except Exception as e:
        print(f'[원장 목표 조회 실패 → 새로 계산: {e}]')
        return None


def _select_target(merged, held=None, us_date=None):
    """목표 = 그날 순위 top N. 단 이미 발송된 us_date면 원장 기록을 그대로 재생(_sent_target).
    ★보유 개념 없음 (사용자 "보유종목에 대해 왈가왈부하지 마라. 너는 교체날에 top5를
    알려주는 역할만 해") — dd_30_25 진입유예·HOLD_BAND는 '이전 보유'를 전제해 방송 상품과
    양립 불가라 폐기됐다."""
    if us_date:
        prev = _sent_target(us_date)
        if prev:
            cur = [d['ticker'] for d in merged[:N_TOP]]
            if set(prev) != set(cur):
                print(f'[발송 지시 동결] {us_date}는 이미 발송됨 → 원장 목표 재생 {prev} '
                      f'(현 규칙 재계산값 {cur}는 미적용)')
            return prev
    return [d['ticker'] for d in merged[:N_TOP]]


def _select_target_legacy(merged, held, us_date=None):
    """목표 5종목 선택 (2026-08-02 진입/이탈 분리 — 상단 DD_/HOLD_BAND 주석 참조).
    보유 종목은 순위<=HOLD_BAND까지 유지, 빈 슬롯은 순위순 충원하되 dd_30_25 종목은 건너뜀.
    held가 None(보유 상태 불명)이거나 에폭 전이면 구 동작(순위 top N) 폴백."""
    if not (_GAP_MODE and held is not None) or (us_date and us_date < GATE2_EPOCH):
        return [d['ticker'] for d in merged[:N_TOP]]
    rank = {d['ticker']: i + 1 for i, d in enumerate(merged)}
    _band = HOLD_BAND if (us_date and us_date >= BAND_OFF_EPOCH) else HOLD_BAND_LEGACY
    sel = [d['ticker'] for d in merged
           if d['ticker'] in set(held) and rank[d['ticker']] <= _band][:N_TOP]
    for d in merged:
        if len(sel) >= N_TOP:
            break
        if d['ticker'] in sel or _dd_blocked(d):
            continue
        sel.append(d['ticker'])
    return sel


def _held_from_ledger():
    """원장 리플레이 기준 현재 보유. 조회 불능이면 None(선택은 구 동작 폴백).
    ★2026-08-02 맹점 수리: 원장이 존재하는데 리플레이가 보유를 못 세우면(행은 파싱되나
    days가 빔 = 손상 의심, 7/23 원장 유실 전례) 빈 리스트를 반환하던 것을 None으로 —
    빈 리스트는 '보유 없음'으로 해석돼 기보유 종목이 dd 게이트에 신규로 걸리며
    전량 매도 지시가 오발된다(실검증: held=[] 시 MU·SNDK가 목표에서 소실)."""
    try:
        if not os.path.exists(LOG):
            return []                      # 원장 자체가 없는 초기 상태 = 진짜 보유 없음
        rows = list(csv.DictReader(open(LOG, encoding='utf-8')))
        rp = _replay(rows)
        if not rp['days']:
            print('[경고] 원장 행은 있으나 리플레이 불능(손상 의심) → 게이트 미적용 폴백')
            return None
        return rp['state'][rp['days'][-1]]['held_after']
    except Exception as e:
        print(f'[보유 상태 조회 실패 → 순위 top{N_TOP} 폴백: {e}]')
        return None


def _capped_top(merged):
    """테마캡2 변형 top4 (메모리 테마 최대 2종목)."""
    hold = []; mem = 0
    for d in merged:
        if d['ticker'] in MEMORY_THEME:
            if mem >= THEME_CAP:
                continue
            mem += 1
        hold.append(d['ticker'])
        if len(hold) >= N_TOP:
            break
    return hold


def _wrap(text, width=32):
    """텔레그램 표시폭(한글 2칸) 기준 단어 줄바꿈."""
    def w(t):
        return sum(2 if ord(c) > 0x2E7F else 1 for c in t)
    out, cur = [], ''
    for word in str(text).split():
        cand = (cur + ' ' + word).strip()
        if cur and w(cand) > width:
            out.append(cur)
            cur = word
        else:
            cur = cand
    if cur:
        out.append(cur)
    return out


def _gemini_key():
    """Gemini 키 — env → repo config.json → C:/dev/config.py (텔레그램 토큰 폴백과 동일 패턴).
    2026-07-10: 회사PC 로컬 실행에 env가 없어 AI 섹션이 조용히 빠지던 구멍 봉합."""
    key = os.environ.get('GEMINI_API_KEY', '')
    if not key:
        try:
            import json as _j
            key = _j.load(open(os.path.join(HERE, 'config.json'), encoding='utf-8')).get('gemini_api_key', '')
        except Exception:
            pass
    if not key:
        try:
            sys.path.insert(0, r'C:\dev')
            import config as _c
            key = getattr(_c, 'GEMINI_API_KEY', '')
        except Exception:
            pass
    return key


# ★2026-07-31: 아침(KST 08시) 발송 + 미국단독 전환 → [한국 증시] 섹션 제거.
#   ①한국장은 09시 개장이라 08시 발송 시점엔 '어제 장' 얘기밖에 못 쓴다(낡은 정보).
#   ②포트폴리오가 미국 전용이라 코스피 마감·특징업종은 행동으로 이어지지 않는다.
#   [반도체·메모리]는 유지 — 삼성·SK하이닉스는 우리가 담는 미국 메모리주의 업황 지표라 유효.
#   원/달러도 유지(원화 투자자의 달러자산 평가에 직결). 한국 복귀 시 자동 원복.
def _session_words(kst_now):
    """발송 요일에 맞는 '직전 장'·'다음 장' 표현. 아침 발송(월~토) 기준.

    ★2026-07-31 사용자 지적: 토요일 아침에 '[오늘 밤 체크]'는 거짓 — 토요일 밤엔 미국
      정규장이 없다. 같은 이유로 월요일 아침의 '지난밤 마감'도 틀리다(직전 장은 금요일).
      미국 정규장은 월~금(KST 화~토 새벽 마감) → 요일별로 표현을 갈라준다.
    반환: (직전장 표현, 다음장 라벨, 다음장 설명)
    """
    wd = kst_now.weekday()          # 0=월 … 5=토, 6=일
    prev = '지난 금요일 미국장 마감' if wd == 0 else '지난밤 미국장 마감'
    if wd < 5:                      # 월~금 아침 → 그날 밤 미국장 있음
        return prev, '[오늘 밤 체크]', '오늘 밤 열리는 미국 정규장'
    return prev, '[다음 장 체크]', '다음 미국 정규장(월요일 밤 개장)'


def _mkt_labels(kst_now):
    """AI 시황 단락 라벨 — 시장 범위(US단독/통합) + 요일에 따라 달라진다."""
    _, nxt, _d = _session_words(kst_now)
    base = ['[미국 증시]', '[반도체·메모리]']
    if not VM_US_ONLY:
        base.append('[한국 증시]')
    return tuple(base + [nxt])


def _ai_market_brief(idx_facts=None, _now=None):
    """AI 시황 — 4단락 문단형 (2026-07-10 전면 개편: 구 5문장 단문은 '기계 같다' 피드백).
    idx_facts: yf 실측 지수 문자열 리스트 — 프롬프트에 ground truth로 주입해 stale 숫자
    발송 차단(폴백 lite가 검색 없이 2024년 지수를 답한 사례 실관측). 키 없으면 None."""
    from datetime import datetime as _dt2
    _now = _now or _dt2.now()
    key = _gemini_key()
    if not key:
        return None
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=key, http_options={'timeout': 120_000})
        tool = types.Tool(google_search=types.GoogleSearch())
        facts = ''
        if idx_facts:
            facts = ('★오늘 실측 지수(이 숫자를 그대로 써라, 다른 출처의 지수 숫자 금지): '
                     + ' / '.join(idx_facts) + '\n')
        _who = '미국 주식에 투자하는' if VM_US_ONLY else '한국+미국 주식을 함께 투자하는'
        _labels = _mkt_labels(_now)
        _prev, _nxt_lb, _nxt_desc = _session_words(_now)
        prompt = (f'지금 한국시간 아침이다. 구글 검색으로 사실 확인 후, {_who} '
                  '사람을 위한 오늘의 시황 브리핑을 한국어 문어체(~습니다)로 써라. '
                  f'아래 {len(_labels)}개 단락을 대괄호 라벨 그대로 시작하고, 단락 사이에 빈 줄 1개. '
                  '★각 단락 2문장 이내·80자 이내(길면 안 읽힌다). 구체 숫자 1~2개만. 마크다운 헤더(#) 금지. '
                  '과장·투자권유·미확인 루머(상장설·인수설) 금지, 확인된 사실만.\n' + facts +
                  f'[미국 증시] {_prev} — 지수 등락과 원인, 주도 섹터와 종목.\n'
                  '[반도체·메모리] HBM·D램·낸드 가격과 수급, 주요 기업 뉴스. '
                  '삼성전자·SK하이닉스 동향도 업황 지표로 포함.\n'
                  + ('[한국 증시] 오늘 코스피·코스닥 마감과 특징 업종, 원/달러 환율.\n'
                     if '[한국 증시]' in _labels else '')
                  + f'{_nxt_lb} {_nxt_desc}에서 볼 미국 경제지표·연준 발언·'
                    '미국 상장기업 실적 발표 일정과 관전 포인트.'
                    '★한국 기업 실적·한국 일정은 이 단락에 넣지 마라(미국장 기준).')
        # 모델 폴백 체인 (2026-07-10 실측): 2.5-flash 무료 20회/일 — KR 16:00 시스템과
        # 키 공유라 저녁엔 쿼터 소진 잦음(당일 실발생) → flash-lite 폴백(무료 한도 큼).
        # lite는 형식 이탈이 잦아 4개 라벨 검증 후 통과분만 채택, 실패 시 1회 더.
        for model in ('gemini-2.5-flash', 'gemini-2.5-flash-lite', 'gemini-2.5-flash-lite'):
            try:
                resp = client.models.generate_content(
                    model=model, contents=prompt,
                    config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
                txt = (resp.text or '').strip()
                if txt and all(lb in txt for lb in _labels):
                    if model != 'gemini-2.5-flash':
                        print(f'[AI 시황: {model} 폴백 사용]')
                    return txt
                if txt:
                    print(f'[AI 시황 {model}: 라벨 형식 미달 → 재시도]')
            except Exception as _e:
                print(f'[AI 시황 {model} 실패: {str(_e)[:120]}]')
        return None
    except Exception as e:
        print(f'[AI 시황 스킵: {e}]')
        return None


_NAME_SEED = {
    'SNDK': '샌디스크', 'MU': '마이크론', 'HPE': 'HPE', 'DELL': '델', 'FLEX': '플렉스',
    'MCHP': '마이크로칩', 'AVGO': '브로드컴', 'TSM': 'TSMC', 'WDC': '웨스턴디지털',
    'STX': '씨게이트', 'NVDA': '엔비디아', 'AMD': 'AMD', 'SMCI': '슈퍼마이크로',
    'CRDO': '크레도', 'AAL': '아메리칸항공', 'ADI': '아날로그디바이스',
    'AMAT': '어플라이드', 'LNG': '셰니어에너지', 'NOW': '서비스나우', 'MRK': '머크',
    'META': '메타', 'INTU': '인튜이트', 'ORCL': '오라클', 'KLAC': 'KLA',
    'LRCX': '램리서치', 'ON': '온세미', 'NOK': '노키아', 'CLS': '셀레스티카',
    'ANET': '아리스타', 'CIEN': '시에나', 'COHR': '코히런트', 'LITE': '루멘텀',
    'APH': '암페놀', 'CRM': '세일즈포스', 'ADBE': '어도비', 'CSCO': '시스코',
    'SNPS': '시놉시스', 'NXPI': 'NXP반도체', 'APP': '앱러빈', 'GOOG': '알파벳',
    'IBM': 'IBM', 'MSFT': '마이크로소프트', 'JNJ': '존슨앤드존슨', 'MA': '마스터카드',
    # ★2026-08-03: 영문 법인명 그대로 나가던 종목들(Vertiv Holdings, LLC / Amkor Technology)
    #   — 한국 독자 기준으로 읽히는 통용 표기로 통일. 확인 안 되는 티커는 추가하지 않는다.
    'AMKR': '앰코테크놀로지', 'VRT': '버티브', 'KNX': '나이트스위프트',
    'TXN': '텍사스인스트루먼트', 'BKR': '베이커휴즈', 'RDDT': '레딧', 'INTC': '인텔',
    'SANM': '산미나', 'FN': '패브리넷', 'NXT': '넥스트래커', 'BE': '블룸에너지',
    'TTMI': 'TTM테크놀로지스', 'AMZN': '아마존', 'AAPL': '애플', 'TSLA': '테슬라',
    '000660.KS': 'SK하이닉스', '005930.KS': '삼성전자', '011070.KS': 'LG이노텍',
    '009150.KS': '삼성전기', '402340.KS': 'SK스퀘어', '066570.KS': 'LG전자',
    '051910.KS': 'LG화학', '006400.KS': '삼성SDI', 'SOFI': '소파이',
}


def _display_name(tk):
    """종목명 표시 — 시드맵 → 캐시(ticker_names.json) → yf shortName(1회 후 캐시)."""
    if tk in _NAME_SEED:
        return _NAME_SEED[tk]
    import json as _j
    cp = os.path.join(HERE, 'data_cache', 'ticker_names.json')
    try:
        cache = _j.load(open(cp, encoding='utf-8'))
    except Exception:
        cache = {}
    if tk in cache:
        return cache[tk]
    try:
        import yfinance as yf
        nm = (yf.Ticker(tk).info or {}).get('shortName') or tk
        # 법인 접미사 제거 — 'Vertiv Holdings, LLC' 같은 등기명이 카드 헤더에 그대로 나가던 문제.
        for _sfx in (', Inc.', ' Inc.', ', LLC', ' LLC', ', Ltd.', ' Ltd.', ' plc', ' PLC',
                     ' Corporation', ' Corp.', ' Company', ' Co.', ' Holdings', ' Holding'):
            if nm.endswith(_sfx):
                nm = nm[:-len(_sfx)]
        nm = nm.strip().rstrip(',').strip()
        cache[tk] = nm
        _j.dump(cache, open(cp, 'w', encoding='utf-8'), ensure_ascii=False)
        return nm
    except Exception:
        return tk


def _industry_tag(d):
    """'(미 · 반도체)' 형식 업종 태그 — US=ticker_info_cache, KR=고정 맵."""
    KR_IND = {'000660.KS': '메모리 반도체', '005930.KS': '전자', '011070.KS': '전자부품',
              '066570.KS': '가전·전장', '051910.KS': '화학·배터리', '006400.KS': '배터리',
              '009150.KS': '전자부품'}
    ind = ''
    if d['market'] == 'KR':
        ind = KR_IND.get(d['ticker'], '')
    else:
        try:
            global _TC_CACHE
            if '_TC_CACHE' not in globals():
                import json as _j
                _TC_CACHE = _j.load(open(os.path.join(HERE, 'ticker_info_cache.json'), encoding='utf-8'))
            v = _TC_CACHE.get(d['ticker'])
            ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v) or ''
        except Exception:
            ind = ''
    return ind or ''


def _kr_card(ticker, dv_musd=None):
    """KR 종목 카드 — yf(분석가·시총) + 거래대금. 실패 항목은 생략."""
    parts1, parts2 = [], []
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        na = info.get('numberOfAnalystOpinions')
        mc = info.get('marketCap')
        if na:
            parts1.append('분석가 %d명' % na)
        if mc:
            parts2.append('시총 %.0f조원' % (mc / 1e12))
    except Exception:
        pass
    if dv_musd:
        parts2.append('거래 $%.1fB/일' % (dv_musd / 1e3))
    out = []
    if parts1:
        out.append(' · '.join(parts1))
    if parts2:
        out.append(' · '.join(parts2))
    return out


def _us_cards(tickers):
    """US 종목 건강성 카드 (US DB carry-forward 최신값). {tk: [줄,...]}"""
    out = {}
    try:
        conn = sqlite3.connect(os.path.join(HERE, 'eps_momentum_data.db'))
        for tk in tickers:
            r = conn.execute(
                "SELECT num_analysts, rev_up30, rev_down30, rev_growth, market_cap, "
                "dollar_volume_30d, roe, free_cashflow, operating_margin FROM ntm_screening "
                "WHERE ticker=? AND date>=date((SELECT MAX(date) FROM ntm_screening), '-60 day') "
                "ORDER BY date", (tk,)).fetchall()
            f = [None] * 9
            for row in r:
                for k, v in enumerate(row):
                    if v is not None:
                        f[k] = v
            na, up, dn, rg, mc, dv, roe, fcf, om = f
            l1, l2 = [], []
            if na:
                l1.append('분석가 %d명(↑%d/↓%d)' % (na, up or 0, dn or 0))
            if rg is not None:
                l1.append('매출 %+.0f%%' % (rg * 100))
            if mc:
                l2.append('시총 %.1f조달러' % (mc / 1e12) if mc >= 1e12 else '시총 $%.0fB' % (mc / 1e9))
            if dv:
                l2.append('거래 $%.1fB/일' % (dv / 1e3))
            if om is not None:
                l2.append('마진 %.0f%%' % (om * 100))
            out[tk] = [' · '.join(x) for x in (l1, l2) if x]
        conn.close()
    except Exception as e:
        print('[US 카드 스킵: %s]' % e)
    return out


def _brief_dict(x):
    """브리핑을 {biz, why, risk} dict로 강제 — 문자열이 와도 카드 렌더가 깨지지 않게.
    형식: '회사소개 | 왜 핫한지 || 리스크'. ★2026-07-10 첫 라이브 버그: Gemini가 '||' 대신
    단일 '|' 3분할로 답해 리스크가 '| ...' 채로 왜 섹션에 붙어 발송됨 → 단일 파이프
    3분할도 수용(마지막 조각=리스크) + 조각별 파이프 잔재 제거."""
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip():
        # ★2026-07-31: 마크다운 제거. Gemini가 **볼드**를 섞어 보내 문장 끝에 '**'가
        #   그대로 발송되고 있었음(10개 카드 전부). HTML 메시지라 마크다운은 의미 없음.
        import re as _re
        x = _re.sub(r'\*{1,3}', '', x).strip()
        # ★2026-08-03 B5 수리 (QC 감사): AI 산문이 HTML 이스케이프 없이 삽입되고 있었다.
        #   Gemini가 'PER<10 & 마진>30%' 같은 문장을 한 번만 써도 텔레그램 HTML 파싱이 실패하고,
        #   _send_long의 구제 경로가 parse_mode 없이 재발송하므로 <b> 태그가 리터럴로 노출된
        #   메시지가 통째로 나간다(주입 테스트로 재현됨). <b> 래핑은 코드가 나중에 붙이므로
        #   여기서 이스케이프해도 안전하다.
        import html as _html
        x = _html.escape(x, quote=False)
        if '||' in x:
            head, risk = x.split('||', 1)
        else:
            parts = x.split('|')
            if len(parts) >= 3:
                head, risk = '|'.join(parts[:-1]), parts[-1]
            elif len(parts) == 2:
                head, risk = parts[0], parts[1]
            else:
                head, risk = x, ''
        if '|' in head:
            biz, why = head.split('|', 1)
        else:
            biz, why = head, ''
        biz, why, risk = biz.strip(' |'), why.strip(' |'), risk.strip(' |')
        # ★2026-07-31 폴백: 구분자를 아예 안 쓰고 산문으로 답하는 경우(실관측 — 10/10 카드가
        #   biz 한 덩어리로 뭉쳐 '왜 지금'·'위험 요인' 섹션이 통째로 사라졌음).
        #   프롬프트가 요구한 구성이 (a)소개 (b)이유 (c)리스크 순서라 문장 단위로 복원한다.
        if not why and not risk:
            sents = [s.strip() for s in _re.split(r'(?<=다\.)\s+', biz) if s.strip()]
            if len(sents) >= 3:
                _R = ('다만', '그러나', '하지만', '리스크', '위험', '우려', '단,')
                if sents[-1].startswith(_R) or any(k in sents[-1] for k in _R):
                    biz, why, risk = sents[0], ' '.join(sents[1:-1]), sents[-1]
                else:
                    biz, why = sents[0], ' '.join(sents[1:])
            elif len(sents) == 2:
                biz, why = sents[0], sents[1]
        return {'biz': biz, 'why': why, 'risk': risk}
    return {}


def _ai_stock_briefs(entries):
    """종목 브리핑 1콜 — 1~5위 상세, 6~10위 두 문장. {ticker: dict} (실패시 빈 dict).
    2026-07-10 개편: 6~20위→6~10위, 소개/이유 분리('|'), 존댓말 문어체,
    top5 파싱 커버리지 검증 후 재시도(1위 브리핑 누락 발송 재발 방지)."""
    key = _gemini_key()
    if not key or not entries:
        return {}
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=key, http_options={'timeout': 150_000})
        tool = types.Tool(google_search=types.GoogleSearch())
        # 2026-07-10 사용자 결정: 6~10위도 1~5위와 동일한 풀 설명 (차별 없음)
        dl = []
        for d in entries[:10]:
            dl.append('%s(%s, %s): 90일 이익전망 %+.0f%%, 선행PER %.0f'
                      % (d['ticker'], _display_name(d['ticker']), _industry_tag(d) or '업종미상', d['rev90'], d['fwd_per']))
        prompt = ('한국+미국 주식 퀀트 시스템의 오늘 순위 10종목이다. 각 종목을 처음 듣는 일반 '
                  '투자자에게 설명하듯 한국어 존댓말 문어체(~습니다)로 써라. '
                  '★모든 종목은 지금 상장되어 활발히 거래 중이다. 상장폐지·인수 소멸 서술 절대 금지, '
                  '미확인 루머(상장 추진설·인수설 등) 금지, 반드시 2026년 최신 정보를 검색해 확인하라'
                  '(예: SNDK는 2025년 웨스턴디지털에서 분사 재상장한 샌디스크). '
                  '★분량 엄수 — 텔레그램 카드라 길면 안 읽힌다. 종목당 구성: '
                  '(a)무슨 사업으로 돈 버는 회사인지 **1문장 40자 이내**(제품·고객이 그려지게) '
                  '(b)왜 지금 이익전망이 급상향되는지 **1문장 50자 이내** — 최근 실적·수주·제품가격 중 '
                  '가장 결정적인 것 하나만, 구체 숫자 1개 포함(검색 확인) '
                  '(c)리스크 **1문장 40자 이내**(막연한 일반론 금지, 이 회사 고유의 위험). '
                  '수식어·배경설명·나열 금지. 과장 없이 사실만. '
                  '★형식(종목당 정확히 한 줄): "TICKER: 회사소개 문장 | 상향 이유 문장들 || 리스크 문장들" '
                  '(소개와 이유 사이 |, 리스크 앞 || 필수).\n' + '\n'.join(dl))

        def _parse(text):
            out = {}
            for d in entries[:10]:
                tk = d['ticker']
                base = tk.split('.')[0]
                for ln in text.splitlines():
                    t = ln.strip().lstrip('-*• ')
                    if t.upper().startswith(tk.upper() + ':') or t.upper().startswith(base.upper() + ':'):
                        out[tk] = _brief_dict(t.split(':', 1)[1].strip())
                        break
            return out

        out = {}
        need = {d['ticker'] for d in entries[:10]}
        _topn = {d['ticker'] for d in entries[:N_TOP]}

        # ★2026-08-01 2차 수리: '존재'와 '완결' 다음은 '실질'이다 — 8/1 실발송에서 SNDK의
        #   왜지금/위험 섹션이 "검색 결과에서 확인되지 않습니다. 제공하기 어렵습니다"라는
        #   변명문으로 나감(비어 있지 않아 완결성 검사 통과). 정보 부재 메타 문구는 결측 취급.
        _EXCUSE = ('확인되지 않', '찾을 수 없', '검색 결과에', '제공하기 어렵',
                   '알 수 없습니다', '정보가 부족', '확인이 어렵', '알려진 바 없')

        def _substantive(txt):
            return bool(txt) and not any(p in txt for p in _EXCUSE)

        def _incomplete(cur):
            """★2026-07-31: 구 커버리지 검사는 '티커가 응답에 있는가'만 봤다.
            Gemini가 소개 한 문장만 뱉어도 통과돼 '왜 지금 뜨거운가요'·'위험 요인'
            섹션이 통째로 빈 카드가 발송됐다(사용자 관측: NXPI). → 완결성으로 판정.
            상위 N(매수 대상)은 3요소 전부, 대기 후보는 소개+이유까지 요구."""
            bad = set()
            for tk in need:
                v = cur.get(tk) or {}
                if not (_substantive(v.get('biz')) and _substantive(v.get('why'))):
                    bad.add(tk)
                elif tk in _topn and not _substantive(v.get('risk')):
                    bad.add(tk)
            return bad
        # 모델 폴백 체인 (2026-07-10 실측): flash 무료 20회/일(KR 16:00 시스템과 키 공유,
        # 당일 소진 실발생) → flash-lite 폴백. lite는 형식 이탈이 잦아 flash 우선 2회.
        # ★2026-08-03 B3 수리 (QC 감사): lite 폴백이 **사실이 틀린 카드**를 통과시켰다 —
        #   실측 3회 렌더 중 1회에서 CLS를 '항공우주·방산', VRT를 '고압 직류 송전'으로 서술
        #   (실제: 데이터센터 EMS / 데이터센터 전력·냉각). 존댓말 규칙도 붕괴.
        #   기존 검사(_incomplete/_substantive)는 '비어 있지 않고 변명문이 아님'만 보므로
        #   이런 오서술을 못 잡는다 = 사실 검증 게이트 0.
        #   → lite 결과는 산문으로 승격하지 않는다. 쿼터 소진 시엔 '숫자만 + 실패 고지'가
        #     정답이다(틀린 설명 발송 < 설명 없음). flash 재시도를 4회로 늘려 흡수한다.
        #   ⚠️같은 이유로 테스트 렌더에 실키 사용 금지(쿼터를 태우면 이 경로로 떨어진다).
        attempts = [('gemini-2.5-flash', 1), ('gemini-2.5-flash', 2),
                    ('gemini-2.5-flash', 3), ('gemini-2.5-flash', 4)]
        for _i, (model, _n) in enumerate(attempts, 1):
            try:
                resp = client.models.generate_content(
                    model=model, contents=prompt,
                    config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
                cand = _parse(resp.text or '')
                # 더 완결한 응답으로만 덮어쓴다(부분 응답이 완전 응답을 밀어내지 않게).
                # 실질 필드만 계수 — 변명문 3개짜리가 진짜 2개짜리를 밀어내지 않게.
                for k, v in cand.items():
                    old = out.get(k) or {}
                    if sum(1 for f in ('biz', 'why', 'risk') if _substantive(v.get(f))) > \
                       sum(1 for f in ('biz', 'why', 'risk') if _substantive(old.get(f))):
                        out[k] = v
                bad = _incomplete(out)
                if not bad:
                    if model != 'gemini-2.5-flash':
                        print(f'[브리핑: {model} 폴백 사용]')
                    break
                print('[브리핑 시도 %d(%s): 미완결 %d종목(%s) → 재시도]'
                      % (_i, model, len(bad), ','.join(sorted(bad)[:5])))
            except Exception as _e:
                print('[브리핑 시도 %d(%s) 실패: %s]' % (_i, model, str(_e)[:120]))
                import time as _t
                _t.sleep(10)
        # 최종 위생: 재시도 소진 후에도 남은 변명문 필드는 지워서 카드가 해당 섹션을
        # 아예 생략하게 한다(변명문 발송 < 섹션 생략 — 8/1 SNDK 사고의 최후 방어선).
        for v in out.values():
            for f in ('biz', 'why', 'risk'):
                if v.get(f) and not _substantive(v.get(f)):
                    v[f] = ''
        # ★2026-08-03 B4 수리: 같은 날 같은 프롬프트인데 실행마다 매출성장률이 달랐다
        #   (AMKR: +26% / +27% / '15% 성장 예상' — DB 실측 +26%). 재발송 상품이라 구독자가
        #   두 버전을 나란히 본다. → 산문 속 '매출 N% 증가'가 DB rev_growth와 5%p 넘게
        #   어긋나면 그 문장을 폐기한다(코드 강제, 프롬프트 부탁 아님).
        import re as _re
        _rg = {d['ticker']: d.get('rev_growth') for d in entries[:10]
               if d.get('rev_growth') is not None}
        for tk, v in out.items():
            g = _rg.get(tk)
            if g is None or not v.get('why'):
                continue
            for mm in _re.finditer(r'매출[^.]{0,20}?(\d+(?:\.\d+)?)\s*%', v['why']):
                if abs(float(mm.group(1)) - g * 100) > 5.0:
                    print('[브리핑 값 불일치 → 문장 폐기] %s: 산문 %s%% vs DB %.0f%%'
                          % (tk, mm.group(1), g * 100))
                    v['why'] = ''
                    break
        return out
    except Exception as e:
        print('[종목 브리핑 스킵: %s]' % e)
        return {}


def _market_page():
    """두 번째 메시지: 시장 지수(KR 마감+US 전일/선물) + AI 시황."""
    lines = ['━━━━━━━━━━━━━━━', '  🤖 <b>AI 시장 분석</b>', '━━━━━━━━━━━━━━━']
    try:
        import yfinance as yf
        idx = []
        for sym, nm in [('^KS11', '코스피'), ('^KQ11', '코스닥'), ('^GSPC', 'S&P'), ('^IXIC', '나스닥')]:
            try:
                fi = yf.Ticker(sym).fast_info
                px, pv = fi.last_price, fi.previous_close
                if px and pv:
                    idx.append('%s %s(%+.1f%%)' % (nm, format(px, ',.0f'), (px / pv - 1) * 100))
            except Exception:
                pass
        if idx:
            lines += ['', '📊 <b>시장 지수</b>']
            for k in range(0, len(idx), 2):
                lines.append(' · '.join(idx[k:k + 2]))
    except Exception as e:
        print('[지수 스킵: %s]' % e)
    brief = _ai_market_brief()
    if brief:
        lines += ['', '📰 <b>시장 동향</b>']
        import re as _re
        for sent in _re.split(r'(?<=[.다])\s+', brief):
            for wl in _wrap(sent.strip(), 90):
                if wl:
                    lines.append(wl)
    return '\n'.join(lines) if len(lines) > 3 else None


def _send_long(token, chat_id, msg, label=''):
    """4096자 제한 분할 발송 (줄 경계).

    2026-07-31: 응답 미확인 = 조용한 실패 경로였음. HTML 파싱 오류·chat_id 오류·봇 차단 등으로
      텔레그램이 ok:false를 반환해도 그냥 넘어가 "발송했다"고 착각하게 만들었음
      (이 레포 반복 패턴: bat pull 무음실패·브레드스 무음소멸·무효토큰 무음 미발송).
      → 응답 검증 + 실패 시 예외. HTML 파싱 실패는 평문 재시도로 구제(서식 손실 < 메시지 유실).
    """
    import requests as _rq
    chunks, cur = [], ''
    for ln in msg.split('\n'):
        if len(cur) + len(ln) + 1 > 3500:
            chunks.append(cur)
            cur = ln
        else:
            cur = (cur + '\n' + ln) if cur else ln
    if cur:
        chunks.append(cur)
    for n, ch in enumerate(chunks, 1):
        r = _rq.post('https://api.telegram.org/bot%s/sendMessage' % token,
                     data={'chat_id': chat_id, 'text': ch, 'parse_mode': 'HTML'},
                     timeout=20).json()
        if not r.get('ok'):
            desc = r.get('description', '')
            r2 = _rq.post('https://api.telegram.org/bot%s/sendMessage' % token,
                          data={'chat_id': chat_id, 'text': ch}, timeout=20).json()
            if not r2.get('ok'):
                raise RuntimeError('텔레그램 발송 실패%s (%d/%d): %s / 평문재시도: %s'
                                   % ((' [%s]' % label) if label else '', n, len(chunks),
                                      desc, r2.get('description', '')))
            print('[!] 발송 %s(%d/%d) HTML 파싱 실패 → 평문 발송: %s' % (label, n, len(chunks), desc))
    print('  ↳ 발송 확인%s %d/%d 청크' % ((' ' + label) if label else '', len(chunks), len(chunks)))


def cmd_run():
    us_date, kr_date, fx, merged, meta = compute()
    run_date = datetime.now().strftime('%Y-%m-%d')
    capped = _capped_top(merged)
    held = _held_from_ledger()
    global _DD_LIVE
    _DD_LIVE = bool(us_date and us_date < DD_OFF_EPOCH)   # dd_30_25는 7/31 이전 재현에만
    target = _select_target(merged, held, us_date)
    meta['target'], meta['held'] = target, (held or [])
    if target != [d['ticker'] for d in merged[:N_TOP]]:
        print(f'[목표≠순위 top{N_TOP}] {target} — 발송 지시 동결분 재생')
    print(f'=== 통합 VM top{N_TOP} (US {us_date} / KR {kr_date}, USDKRW {fx:.0f}, '
          f'code {_git_sha() or "?"}) ===')
    if meta.get('base_n'):
        print(f"백분위 분모(30일 유니온): US {meta['base_n'].get('US')} / KR {meta['base_n'].get('KR')} "
              f"(KR 당일 수집 {meta.get('kr_today_n')})")
    for wmsg in meta.get('warnings', []):
        print(f'[경고] {wmsg}')
    for i, d in enumerate(merged[:10], 1):
        mark = ' ★목표' if d['ticker'] in target else ''
        if _dd_blocked(d):
            mark += ' [급락유예]'
        if d['ticker'] in capped and d['ticker'] not in target:
            mark += ' (캡2픽)'
        gap_s = f"{d['gap']:.1f}" if d['gap'] else 'pass'
        _m = ('괴리 %+7.1f' % d['adj_gap']) if (_GAP_MODE and d.get('adj_gap') is not None)             else ('rev90 %+7.1f%%' % d['rev90'])
        print(f"{i:2}. [{d['market']}] {d['ticker']:10} {_m}  "
              f"fwdPER {d['fwd_per']:5.1f}  gap {gap_s:>5}  dv ${(d['dv_musd'] or 0):,.0f}M{mark}")
    # ★2026-07-31 관찰 변형 정리 (미국단독 전환으로 일부가 수학적으로 무의미해짐)
    #   절대결합·robust-z는 원래 '한국과 미국을 어떻게 합칠까'(2026-07-09 백분위 승격)의 비교군이다.
    #   시장이 하나뿐이면 백분위·절대값·robust-z가 모두 같은 값의 단조변환이라 순서가 항상 동일 —
    #   실측 확인: 본선/절대결합/robust-z 셋 다 VRT·STX·SNDK·GOOGL·NXPI로 일치.
    #   => 미국단독에선 계산·출력을 끄고 원장 컬럼은 본선값으로 채운다(컬럼 삭제 시 과거 행과
    #      어긋나므로 스키마는 유지). 한국 복귀 시 자동으로 되살아난다.
    #   테마캡2(메모리 최대 2종목)는 메모리가 2개 이상 들어오면 다시 의미가 생기므로 유지.
    print('테마캡2 변형 top%d:' % N_TOP, capped)
    main_top = [d['ticker'] for d in merged[:N_TOP]]
    if VM_US_ONLY:
        abs_top = rz_top = main_top
    else:
        abs_top = [x['ticker'] for x in sorted(merged, key=lambda z: -(_score(z) or -9e9))[:N_TOP]]
        print('절대결합 변형 top%d(관찰):' % N_TOP, abs_top)
        rz_top = [x['ticker'] for x in sorted(merged, key=lambda z: -z.get('rz', 0))[:N_TOP]]
        print('robust-z 변형 top%d(관찰):' % N_TOP, rz_top)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    if os.environ.get('UNIFIED_NO_LOG') == '1':
        # 2026-07-10 감사수리 2: 샘플/일회성 실행이 append-only 공식 원장을 오염시킨 사고
        # (7/9 블록 = GH Actions 샘플, KR gap 전원 공란) 재발 방지 — 원장 기록은 옵트아웃 가능.
        print('[UNIFIED_NO_LOG=1] 원장 기록 생략')
        return merged, meta
    # rev30/stale/below_ma120 = 관찰 전용 (2026-07-13, GATECHAIN_REVIEW log-stale-flag-observe,
    # 매매 개입 0): stale=rev90>=20 & rev30<=2 (106일 중 1건뿐이던 케이스의 forward 라이브 추적),
    # below_ma120=픽 시점 가격<120일선 (rev90 랭킹의 사실상 추세필터 역할 라이브 검증).
    # ★2026-08-01 관찰컬럼 4종 추가(px_chg20/crash5/na/up30 — REDESIGN_DEBATE_2026_08_01.md):
    #   신규 컬럼은 반드시 끝에 붙인다 — 마이그레이션이 끝-패딩이라 중간 삽입 시 구행 정렬 깨짐.
    COLS = ['run_date', 'us_date', 'kr_date', 'rank', 'market', 'ticker',
            'rev90', 'fwd_per', 'gap', 'dv_musd', 'price', 'in_top4', 'in_top4_cap2',
            'pct', 'in_top5_abs', 'rz', 'in_top5_rz', 'pct_base_n',
            'rev30', 'stale', 'below_ma120', 'adj_gap', 'strategy',
            'px_chg20', 'crash5', 'na', 'up30']
    if os.path.exists(LOG):  # 구헤더(13/14컬럼) → 신헤더 마이그레이션, 과거 행은 공란 패딩
        lines = open(LOG, encoding='utf-8').read().splitlines()
        hdr = lines[0].split(',')
        if len(hdr) < len(COLS):
            with open(LOG, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow(COLS)
                for ln in lines[1:]:
                    cells = next(csv.reader([ln]))
                    w.writerow((cells + [''] * len(COLS))[:len(COLS)])
    new = not os.path.exists(LOG)
    with open(LOG, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if new:
            w.writerow(COLS)
        for i, d in enumerate(merged[:20], 1):
            # in_top4 = 목표(선택) 멤버십 — 2026-08-02부터 순위 1~N이 아니라 선택 결과
            # (보유 밴드·급락 유예 반영). _replay가 이 플래그로 보유를 재구성하므로 정합.
            w.writerow([run_date, us_date, kr_date, i, d['market'], d['ticker'],
                        round(d['rev90'], 2), round(d['fwd_per'], 2),
                        round(d['gap'], 3) if d['gap'] else '',
                        round(d['dv_musd'], 1) if d['dv_musd'] else '', d['price'],
                        int(d['ticker'] in target), int(d['ticker'] in capped),
                        round(d.get('pct', 0), 2), int(d['ticker'] in abs_top),
                        round(d.get('rz', 0), 2), int(d['ticker'] in rz_top),
                        meta.get('base_n', {}).get(d['market'], ''),
                        round(d['rev30'], 2) if d.get('rev30') is not None else '',
                        int(d['rev90'] >= 20 and d['rev30'] <= 2) if d.get('rev30') is not None else '',
                        d['below_ma120'] if d.get('below_ma120') is not None else '',
                        round(d['adj_gap'], 3) if d.get('adj_gap') is not None else '',
                        VM_STRATEGY + ('/us' if VM_US_ONLY else ''),
                        round(d['px_chg20'], 2) if d.get('px_chg20') is not None else '',
                        d['crash5'] if d.get('crash5') is not None else '',
                        d.get('na') or '', d.get('up30') if d.get('up30') is not None else ''])
    print(f'로그 append: {LOG}')
    return merged, meta


def cmd_nav():
    """로그 리플레이 NAV — 공용 _replay 사용 (2026-07-10 감사수리 4: 구현이 '로그일 i%5'
    자체 시계 + 중복블록 dedup 없음으로 표시(is_rebal)와 어긋났음 → US 그리드 단일화)."""
    if not os.path.exists(LOG):
        print('로그 없음')
        return
    import csv as _csv
    rows = list(_csv.DictReader(open(LOG, encoding='utf-8')))
    rp = _replay(rows)
    days = rp['days']
    for d in days:
        s = rp['state'][d]
        if s['is_rebal']:
            print(f"{d} REBAL → {s['held_after']}")
    print(f"통합 트랙 NAV: {(rp['nav'] - 1) * 100:+.2f}% ({days[0]} ~ {days[-1]}, {len(days)}일)")



# ═══ 메시지 렌더링 (2026-07-09 UX 전문가 스펙: 행동→회사→근거3+앵커→위험→규모 고정 카드) ═══
PER_ANCHOR = {'US': '미국 평균 22배', 'KR': '한국 평균 11배'}



def _card_facts(d, cards_map):
    """cards_map 원시줄에서 팩트 추출 — {analysts, rev_growth, mcap, dv, margin}."""
    f = {}
    for c in (cards_map.get(d['ticker']) or []):
        for part in [x.strip() for x in c.split('·')]:
            if part.startswith('분석가'):
                f['analysts'] = part.replace('분석가 ', '')
            elif part.startswith('매출'):
                f['rev_growth'] = part
            elif part.startswith('시총'):
                f['mcap'] = part
            elif part.startswith('거래'):
                f['dv'] = part.replace('거래 ', '')
            elif part.startswith('마진'):
                f['margin'] = part
    return f


def _name_tk(ticker):
    """매수 목록용 표기 — ★2026-08-03 (사용자 "티커나 영문명은 넣지도 않네").
    한글명만 있으면 증권사 앱에서 검색이 안 된다. 주문에 필요한 티커를 항상 병기."""
    base = ticker.replace('.KS', '').replace('.KQ', '')
    nm = _display_name(ticker)
    return nm if nm == base else f'{nm} ({base})'


def _stock_card(rank, d, brief, cards_map, first=False):
    """종목 카드 — ★2026-08-03 (2차) 아이콘 제거·타이포 위계 (사용자 "길이 줄이랬더니
    아이콘을 덕지덕지 붙여놨네"). 카드마다 🔥📊⚠️가 똑같이 반복되면 그 아이콘은 정보가
    0이고 소음만 된다 — 다섯 장에 15개가 찍혀 있었다.

    구조를 만드는 수단을 이모지 → 활자(굵기·들여쓰기·괘선)로 교체:
      · 카드 구분선 = 얇은 괘선(─), 메시지 제목선 = 굵은 괘선(━) → 계층이 선 굵기로 보인다.
      · 헤더  = <b>순위 이름</b> + 티커 · 업종 (굵기가 곧 카드 시작 표시)
      · 산문  = 왼쪽 정렬 2문단(무슨 회사 / 왜 지금) — 아이콘 없이 그냥 읽힌다.
      · 숫자  = 3칸 들여쓴 블록. '들여쓰기 = 기계가 계산한 근거'라는 신호이고,
                순위 근거 두 수치만 <b>로 굵혀 무엇으로 뽑았는지가 한눈에 보이게.
      · 위험  = 같은 블록 안, 라벨만 굵게. 좋은 소식으로 오독될 유일한 줄이라 라벨을 남김.
    46칸(한글 23자) 넘는 산문은 _wrap으로 미리 접어 모바일 강제 줄바꿈을 막는다.
    (1차 간결화 때 지운 것 — 라벨 4줄·희소성 백분위·시총/거래대금·'N배'·용어 해설 — 은
     그대로 두고 되살리지 않는다.)"""
    nm = _display_name(d['ticker'])
    tk = d['ticker'].replace('.KS', '').replace('.KQ', '')
    sect = _industry_tag(d)
    head = f"<b>{rank}위 {nm}</b>  {tk}" + (f" · {sect}" if sect else '')
    if not VM_US_ONLY:
        head = ('🇰🇷 ' if d['market'] == 'KR' else '🇺🇸 ') + head
    L = ['──────────────', head]
    b = _brief_dict(brief)
    if b.get('biz'):
        L += _wrap(b['biz'].strip(), 44)
    if b.get('why'):
        L += _wrap(b['why'].strip(), 44)
    if _MEM_ALERT_ON and MEM_ALERT_SHOW and d['ticker'] in MEMORY_THEME:
        L.append('※ 메모리 업황 주의보 해당 종목')
    if _GAP_MODE and d.get('adj_gap') is not None:
        L.append(f"   전망 <b>+{d['rev90']:.0f}%</b> 상향 · 주가 "
                 f"<b>{-d['adj_gap']:.0f}%</b> 덜 따라옴")
    else:
        L.append(f"   이익전망 3개월 <b>+{d['rev90']:.0f}%</b> 상향")
    fx = _card_facts(d, cards_map)
    tail = []
    if fx.get('analysts'):
        tail.append('애널 ' + fx['analysts'].replace('(↑', '(30일 ↑').replace('/↓', ' ↓'))
    tail.append(f"선행PER {d['fwd_per']:.0f}배")
    L.append('   ' + ' · '.join(tail))
    if b.get('risk'):
        rw = _wrap(b['risk'].strip(), 38)
        L.append('   <b>위험</b> ' + rw[0])
        L += ['        ' + x for x in rw[1:]]
    L.append('')
    return L


def _split_sents(text):
    import re
    out = []
    for sent in re.split(r'(?<=[.다])\s+', (text or '').strip()):
        s = sent.strip()
        if s:
            out.append(s)
    return out




def _kr_regime():
    """KR 국면 — ★표시 전용 (2026-07-10 사용자 승인: OR 결합 대신 우선 병기 표시).
    KR production(7.4년 검증, KOSPI MA20/80 크로스+5일 확인)이 매일 16:00에 계산·커밋하는
    권위 상태 state/regime_state.json을 읽음 — 자체 재계산(yf ^KS11)도 동일 결론(boost)을
    냈으나, 판정은 재현이 아니라 원 시스템의 산출을 직접 읽는 게 정합적(파라미터 드리프트
    0, 6/18 사고 교훈: 같은 사실의 이중 계산은 어긋날 때 조용히 갈라짐).
    ※2026-07-10 정정: 처음 'yf 지수 데이터 쓰레기'로 기각했으나 사용자 교정 — 일별 ±5~10%
    출렁임은 실제 장세였음(7/9 7,292 교차확인, 7월 초 -14% 급락 = KR SL 재도입 사건과 일치).
    매매 반영(시장별 스코프: KR 방어 시 KR 종목만 현금)은 지수 프록시 BT 후 별도 결정.
    반환 {'mode','pending_days','asof'} 또는 None(실패)."""
    import json as _j
    from datetime import datetime as _d
    cands = [os.environ.get('KR_STATE_PATH', ''),
             os.path.join(os.path.dirname(KR_FS_DIR), 'state', 'regime_state.json'),
             r'C:\dev\state\regime_state.json']
    for p in cands:
        if not p or not os.path.exists(p):
            continue
        try:
            r = _j.load(open(p, encoding='utf-8'))
            mode = r.get('mode', 'boost')
            streak_mode = r.get('streak_mode', mode)
            pending = int(r.get('streak', 0)) if streak_mode != mode else 0
            asof = r.get('last_date', '')
            asof_s = f'{asof[4:6]}/{asof[6:8]}' if len(asof) == 8 else asof
            stale = False
            try:
                stale = (_d.now() - _d.strptime(asof, '%Y%m%d')).days > 5
            except ValueError:
                pass
            return {'mode': mode, 'pending_days': pending, 'asof': asof_s, 'stale': stale}
        except Exception as e:
            print(f'[KR 국면 파일 파싱 실패({p}): {e}]')
    print('[KR 국면 표시 스킵: regime_state.json 없음]')
    return None


def _trade_when(kst_now):
    """매매 가능 시점 문구. 아침 발송 기준 — 그날 밤 미국장이 열리는지로 분기.
    토(5)·일(6) 아침엔 그날 밤 미국 정규장이 없어 '오늘 밤'이 거짓이 됨(2026-07-31 사용자 지적)."""
    return '오늘 밤 미국장' if kst_now.weekday() < 5 else '다음 미국장(월요일 밤)'


def _next_msg_day(us_latest, next_in):
    """다음 교체 지시 메시지의 KST 날짜 근사 — us_latest에서 미국 거래일 next_in개 진행 후
    그 다음 KST 평일(주말이면 월요일). 미국 휴장일 미반영이라 '예정' 라벨과 함께 쓸 것."""
    from datetime import datetime, timedelta
    try:
        cur = datetime.strptime(us_latest, '%Y-%m-%d')
    except Exception:
        return None
    cnt = 0
    while cnt < next_in:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            cnt += 1
    # ★2026-07-31: 아침 발송 전환 — 미국장 D일 마감분은 KST D+1 아침에 계산·발송된다.
    #   미국 거래일은 월~금이므로 발송일은 자연히 화~토. 구 로직은 저녁(월~금) 발송 기준이라
    #   주말을 건너뛰어 금요일장 신호를 '월요일'로 표기했다(실제로는 토요일 아침 도착).
    return cur + timedelta(days=1)


def _earnings_lines(tickers):
    """보유종목 14일 내 실적발표 일정 (yf calendar, 실패 종목은 조용히 생략)."""
    out = []
    try:
        import yfinance as yf
        from datetime import datetime, timedelta
        lim = (datetime.now() + timedelta(days=14)).date()
        today = datetime.now().date()
        for tk in tickers:
            try:
                cal = yf.Ticker(tk).calendar or {}
                eds = cal.get('Earnings Date') or []
                for ed in eds[:1]:
                    d = ed.date() if hasattr(ed, 'date') and callable(getattr(ed, 'date')) else ed
                    if today <= d <= lim:
                        out.append(f"· {_display_name(tk)}: {d.month}/{d.day} 실적 발표 예정")
            except Exception:
                continue
    except Exception:
        pass
    return out


def _credit_vol_lines():
    """신용(HY-OAS)·변동성(VIX) 상태 — 시스템 방어 임계 대비 현재 위치."""
    out = []
    try:
        import daily_runner as dr
        hy = dr._compute_hy_oas_defense()
        if hy:
            oas, trough, fired = hy
            st = '🔴 경계 (방어 신호)' if fired else '🟢 안정'
            out.append(f"회사채 금리차(HY) {oas:.2f}%p {st}")
            out.append(f"  6개월 저점 대비 +{max(oas - trough, 0):.2f}%p (경보선 +1.0%p)")
    except Exception as e:
        print(f'[HY 스킵: {e}]')
    try:
        import yfinance as yf
        fi = yf.Ticker('^VIX').fast_info
        v = fi.last_price
        if v:
            st = '🔴 공포 구간' if v > 36 else ('🟡 다소 높음' if v > 25 else '🟢 안정')
            out.append(f"변동성지수(VIX) {v:.1f} {st} (방어선 36)")
    except Exception:
        pass
    return out


def _compose_and_send(merged, meta=None):
    import csv as _csv
    from datetime import datetime as _dt
    meta = meta or {}
    rows = list(_csv.DictReader(open(LOG, encoding='utf-8'))) if os.path.exists(LOG) else []
    # 2026-07-10 감사수리 4: 시계 단일화 — diff·NAV·is_rebal 전부 _replay(US 그리드) 기준.
    #   (구현은 diff=로그일 idx-5, NAV=로그일 i%5, is_rebal=US그리드로 3개 시계가 달랐음)
    rp = _replay(rows) if rows else {'nav': 1.0, 'days': [], 'state': {}}
    all_days = rp['days']
    today = all_days[-1] if all_days else _dt.now().strftime('%Y-%m-%d')
    _, blocks = _ledger_blocks(rows) if rows else (None, {})
    trows = blocks.get(today, [])
    st_today = rp['state'].get(today, {})
    is_rebal = st_today.get('is_rebal', False)
    usd = _us_grid()
    us_latest = trows[0]['us_date'] if trows else None
    gi = usd.index(us_latest) if us_latest in usd else len(usd) - 1
    next_in = REBAL - (gi % REBAL)
    # ★2026-08-02 진입/이탈 분리: 표시도 '목표 5종목'(선택 결과) 기준 — 순위 1~5와 다를 수 있음.
    #   meta['target'] 부재 시(구 호출 경로) 순위 top N 폴백.
    _tgt = (meta or {}).get('target') or [d['ticker'] for d in merged[:N_TOP]]
    top5 = [d for d in merged if d['ticker'] in set(_tgt)][:N_TOP]
    bench = [d for d in merged if d['ticker'] not in set(_tgt)][:10 - N_TOP]
    m10 = top5 + bench
    briefs = _ai_stock_briefs(m10)
    cards = _us_cards([d['ticker'] for d in m10 if d['market'] == 'US'])
    for d in m10:
        if d['market'] == 'KR' and d['ticker'] not in cards:
            kc = _kr_card(d['ticker'], d.get('dv_musd'))
            if kc:
                cards[d['ticker']] = kc
    # 교체 diff = 오늘 top5 vs 리밸 직전 보유(held_before) — 원장 리플레이와 같은 기준
    diff = None
    if is_rebal:
        prev_set = set(st_today.get('held_before') or [])
        cur_set = {d['ticker'] for d in top5}
        buys = sorted(cur_set - prev_set)
        sells = sorted(prev_set - cur_set)
        diff = (buys, sells)
    nav = rp['nav']
    # 신호등
    try:
        from memory_cycle_alert import build_message
        amsg, fired = build_message()
        globals()['_MEM_ALERT_ON'] = bool(fired)
    except Exception as _ae:
        amsg, fired = f'🚦 신호등 계산 실패: {_ae}', False
    # ── 국면 오버레이 (2026-07-10 사용자 승인): US 메인의 검증된 방어 신호 재사용 ──
    # S&P<200일선(15일 확인) OR VIX>36(2일) OR HY-OAS 신용경보 → defense(주식 0%).
    # 26~30년 검증(dotcom/GFC/COVID/2022 포착). 실패 시 boost 가정(기존과 동일).
    try:
        import daily_runner as _dr
        reg = _dr.get_market_regime() or {}
    except Exception as _e:
        print(f'[국면 조회 스킵(강세 가정): {_e}]')
        reg = {}
    regime = reg.get('regime', 'boost')
    reentry = (regime != 'defense') and (rp.get('ew_last', 1.0) == 0.0)  # 방어→강세 복귀 첫날
    # 미국단독 모드에선 KR state 파일을 읽을 이유가 없다(표시도 안 하고 매매에도 무관).
    krr = None if VM_US_ONLY else _kr_regime()

    # ── 메시지 1: 상단 공통 + TOP5 카드 (2026-07-10 전면 개편) ──
    kdt = _dt.now()
    wd = '월화수목금토일'[kdt.weekday()]
    alert_head = (amsg or '').split('\n')[0]  # 신호등 상태 한 줄 — 상단 노출 (상세는 메시지3)
    _title = ('미국 TOP%d 신호' % N_TOP) if VM_US_ONLY else ('한국+미국 TOP%d 신호' % N_TOP)
    # ★2026-08-03 아이콘 정리: 제목의 📬는 매 발송 고정이라 정보가 0 → 제거.
    #   국면 신호등(🟢🟠🛑)만 남긴다 — 세 값이 갈리는 상태 표시라 색 자체가 정보다.
    m1 = [f'<b>{_title}</b> · {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━']
    if regime == 'defense':
        m1.append('국면 🛑 방어 — 주식 0% (전량 현금)')
    elif regime == 'half_defense':
        m1.append('국면 🟠 부분 방어 — 주식 50%')
    else:
        m1.append('국면 🟢 강세 — 주식 100%')
        _db = reg.get('days_below') or 0
        if _db:
            m1 += _wrap(f'약세 신호 누적 {_db}일 — 15일 확인 중이라 '
                        '아직 매매 변화는 없습니다.', 44)
    # KR 국면은 평시 생략, 이상 신호(약세/전환 진행)일 때만 상단 노출 (상세는 메시지3)
    if (not VM_US_ONLY) and krr and (krr['mode'] == 'defense' or krr['pending_days'] > 0):
        if krr['mode'] == 'defense':
            m1.append('🇰🇷 한국 국면: 🛑 약세 (참고 — 한국 종목 주의)')
        else:
            m1.append(f"🇰🇷 한국 약세 전환 진행 {krr['pending_days']}/5일 (참고)")
    if alert_head and MEM_ALERT_SHOW:
        m1.append(alert_head)
    m1.append('')
    if meta.get('warnings'):
        m1.append('⚠️ <b>데이터 품질 주의</b>')
        for wmsg in meta['warnings']:
            m1 += _wrap('· ' + wmsg, 44)
        m1.append('')
    # ★2026-07-31 강등: 매매 명령 → 주의 표시.
    #   10년 실측(research/_memalert_precision_2026_07_31.py): 발동 29회 중 실제 −20% 급락으로
    #   이어진 건 5회 = 정밀도 17%(헛방어 24회, 다수가 6~15일 단발). 재현율은 71%로 높아
    #   '위기엔 항상 켜지지만 켜졌다고 위기는 아닌' 신호 → 명령으로 쓰면 10년간 29회 왕복매매.
    #   게다가 시스템이 SNDK를 3위로 추천하는데 경보가 "메모리 전량 매도"를 지시해 자기모순이었음.
    #   정보 가치는 유지(ON 구간 메모리 연율 −4.3% vs OFF +81.4%) → 해당 종목에 ⚠️ 라벨만.
    if fired and MEM_ALERT_SHOW:
        _mem = [d['ticker'] for d in top5 if d['ticker'] in MEMORY_THEME]
        import memory_cycle_alert as _mca
        m1 += ['⚠️ <b>메모리 업황 주의보</b>',
               f'메모리 대표 {len(_mca.CLUSTER)}종 중 {_mca.K_FIRE}종 이상이 하락',
               '추세입니다. 과거 큰 하락은 모두 이',
               '신호가 먼저 떴지만, 신호가 떴다고',
               '항상 하락하지는 않습니다.']
        if _mem:
            m1.append('아래 TOP%d 중 해당 종목: ' % N_TOP + ', '.join(_display_name(t) for t in _mem))
        m1 += ['매매는 평소대로 신호를 따르시고,', '이 종목들은 변동성이 클 수 있다는', '점만 감안하세요.', '']
    nxt = _next_msg_day(us_latest, next_in) if us_latest else None
    nxt_s = f"{nxt.month}/{nxt.day}({'월화수목금토일'[nxt.weekday()]}) 아침" if nxt else f"{next_in}거래일 후"
    if regime == 'defense':
        m1 += ['<b>오늘 할 일 — 전량 현금</b>',
               '시장 전체가 약세 국면으로 판정됐습니다',
               '(S&P500 200일선 이탈 15일 확인 또는',
               ' 공포지수·신용시장 경보).',
               '보유 종목을 전부 팔고 현금으로',
               '보관하세요. 이미 파셨다면 그대로 유지.',
               '강세 복귀 알림이 올 때까지',
               '신규 매수는 하지 않습니다.']
    elif reentry:
        m1 += ['<b>오늘 할 일 — 강세 복귀, 재진입</b>',
               '방어 국면이 해제됐습니다.',
               f'아래 {N_TOP}종목을 각 {100/N_TOP:.0f}%씩',
               f'{_trade_when(kdt)}에 매수하세요.']
        if not VM_US_ONLY:
            m1 += ['(미국 종목 = 오늘 밤 개장,', ' 한국 종목 = 내일 아침 개장)']
        for _i, t in enumerate([d['ticker'] for d in top5], 1):
            m1.append(f'  {_i}. {_name_tk(t)}')
    elif is_rebal:
        # ★2026-08-01 (사용자 지시 "교체하세요 할 때는 현재 TOP5만 보여줘야지 기존 종목
        #   들먹이지 마라"): 구 방식은 원장 보유와의 diff('팔기: 플렉스·HPE')로 지시했는데,
        #   그 '팔기' 종목명은 시스템 원장의 가정이지 구독자의 실제 보유가 아니다 —
        #   어제 '보유 단정 금지' 원칙(a55c173)을 세우고도 교체일 분기엔 남아 있었다.
        #   → 목표 상태(오늘 TOP5)만 제시. 각자 자기 계좌를 그 목표에 맞추면 되므로
        #   시스템이 모르는 정보(누가 뭘 들고 있나)에 기대지 않는다.
        # ★2026-08-02 (사용자 확정 "차별하지 마라, 무조건 하나의 방향"): 목표 리스트는 보유자·
        #   신규자 구분 없이 전원 동일 지시, 고객 행동 분기(⚠️·현금대기) 없음.
        #   (구 dd_30_25 급락 유예는 2026-08-03 OFF — "보유/신규 구분"은 시스템이 모르는
        #    고객 보유를 아는 척하는 구조라는 사용자 지적. 급락 리스크는 TS15%가 고객 측에서 절단.)
        # ★2026-08-03: 🔁·🛡️ 제거. 굵은 제목줄 + 들여쓴 번호 목록이면 '할 일'이라는 게
        #   그 자체로 읽힌다. 비중은 전 종목 동일(20%)이라 줄마다 반복하지 않고 제목에 1회만.
        m1.append(f'<b>오늘 할 일 — 아래 {N_TOP}종목으로 맞추세요</b>')
        m1.append(f'(각 {100/N_TOP:.0f}%씩)')
        for _i, _d in enumerate(top5, 1):
            m1.append(f'  {_i}. {_name_tk(_d["ticker"])}')
        # ★2026-08-03 필수정보 체크리스트 F5/F7 보강 (사용자 "구멍 3개 다 메워"):
        #   F5 = 목록 밖 종목 처리 규칙. '같아지도록'만으로는 처음 보는 사람이 못 읽는다.
        #        단 보유 단정 금지 원칙상 '무엇을 파세요'가 아니라 규칙 문장으로 쓴다.
        #   F7 = 다음 점검일. 평일 분기엔 있는데 정작 매수 직후인 교체일에 빠져 있었다.
        m1 += ['',
               f'{_trade_when(kdt)} 개장 때 이 목록과',
               '같아지도록 매매하시면 됩니다.',
               '(목록에 없는 미국 주식은 정리하고,',
               f' 없는 종목은 각 {100/N_TOP:.0f}%씩 매수하시면 됩니다)',
               '',
               '<b>트레일링 스탑 15%</b>를 함께 걸어두세요.',
               '산 뒤 최고가 대비 15% 빠지면 자동으로',
               '팔리는 예약 주문입니다.',
               '(증권사 앱 → 트레일링 스탑 → 감시폭 15%)',
               '',
               f'다음 교체 점검: <b>{nxt_s}</b>',
               '그때까지는 그대로 두시면 됩니다.']
    else:
        # ★2026-07-31 (2차 수정): 보유 종목을 메시지에 쓰지 않는다.
        #   시스템은 '자기가 무엇을 추천했는지'만 알 뿐 사용자의 실제 계좌를 모른다.
        #   원장 기록을 '지금 보유'로 단정하면 사용자가 한 번이라도 다르게 매매한 순간
        #   계속 거짓이 된다(실제 발생: 원장은 STX 보유로 기록했으나 사용자는 매수 안 함).
        #   → 교체일에만 매매를 지시하고, 그 외의 날은 순위만 보여준다.
        # ★2026-08-03 F6 보강(사용자 지시 "평일에도 스탑 안내 남겨"): 오늘 처음 받은 사람은
        #   평일에도 이 목록을 보고 매수한다 — 그 사람에게 스탑 규칙이 닿아야 한다.
        m1 += ['<b>오늘 할 일 — 없음</b>',
               f'다음 교체 점검: <b>{nxt_s}</b>', '',
               f'아래는 현재 목표 {N_TOP}종목입니다.',
               '교체는 점검일에만 하니 오늘은',
               '그대로 두세요.',
               '',
               f'새로 사실 때는 각 {100/N_TOP:.0f}%씩,',
               '<b>트레일링 스탑 15%</b>를 함께 걸어두세요.']
    # ★2026-08-03 간결화 (사용자 "메시지가 너무 길다"): 서비스 소개 13줄 + 용어 4줄 →
    #   소개 4줄. 규칙(무엇을 고르나·언제 교체하나)과 정직 고지(모의·비용 전)만 남기고
    #   게이트 나열·시장 눈금 설명·용어 사전은 삭제 — 매일 반복되는 고정 문구라 정보가 아니다.
    _scope = '미국 상장사 1,400곳' if VM_US_ONLY else '한국+미국 상장사 1,600곳'
    m1 += ['']
    if _GAP_MODE:
        m1 += [f'<b>{_scope}</b> 중 최근 3개월',
               '이익 전망이 오른 회사에서,',
               f'주가가 가장 덜 따라온 {N_TOP}곳.',
               f'교체는 {REBAL}거래일에 한 번.']
    else:
        m1 += [f'<b>{_scope}</b> 중 이익 전망이 가장',
               f'가파르게 오르는 {N_TOP}곳.',
               f'교체는 {REBAL}거래일에 한 번.']
    # 누적 성과 — 한 줄이 46칸을 넘어 접히던 자리라 두 줄로 나눔(아이콘 제거).
    m1 += ['', f"<b>전략 누적 {(nav - 1) * 100:+.1f}%</b>"]
    if all_days:
        m1.append(f"  {all_days[0][5:].replace('-', '/')}~ 모의운용 · 비용 반영 전")
    if not any(briefs.get(d['ticker']) for d in top5):
        m1 += ['', '※ 오늘은 AI 종목 설명 생성에 실패해',
               '숫자 지표만 표시됩니다. 다음 발송에서',
               '자동 복구됩니다.']
    if regime == 'defense':
        m1 += ['', '아래 순위는 <b>관찰용</b>입니다.',
               '방어 국면에는 매수하지 않습니다.']
    for i, d in enumerate(top5, 1):
        m1 += _stock_card(i, d, briefs.get(d['ticker']), cards, first=(i == 1))
    # ── 메시지 2: 대기 후보 6~10위 — 1~5위와 동일한 풀카드 (2026-07-10 사용자 "차별하지 마") ──
    m2 = None
    if len(m10) > N_TOP:
        # ★2026-08-03 간결화: 대기 후보는 '지금 안 사는 종목'이라 풀카드가 불필요했다.
        #   한 줄(순위·이름·업종 + 순위 근거 숫자)로 축약 — 구 10줄/종목 → 2줄/종목.
        # ★2026-08-03: 📋 제거 + TOP5 카드와 같은 활자 규칙(굵은 헤더 / 3칸 들여쓴 숫자줄)로
        #   통일 — 같은 성격의 정보는 같은 모양이어야 눈이 배우는 규칙이 하나로 유지된다.
        m2 = [f'<b>대기 후보</b> · {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━',
              '<b>지금 사는 종목이 아닙니다.</b>',
              f'목표 {N_TOP}종목에서 빠지면 여기서',
              '올라옵니다.', '']
        for j, d in enumerate(m10[N_TOP:], N_TOP + 1):
            _nm = _display_name(d['ticker'])
            _tk = d['ticker'].replace('.KS', '').replace('.KQ', '')
            _sc = _industry_tag(d)
            m2.append(f"<b>{j}위 {_nm}</b>  {_tk}" + (f" · {_sc}" if _sc else ''))
            if _GAP_MODE and d.get('adj_gap') is not None:
                m2.append(f"   전망 +{d['rev90']:.0f}% · 주가 {-d['adj_gap']:.0f}% 덜 따라옴"
                          f" · PER {d['fwd_per']:.0f}배")
            else:
                m2.append(f"   이익전망 +{d['rev90']:.0f}% · 선행PER {d['fwd_per']:.0f}배")
    # ── 메시지 3: AI 시장 분석 (2026-07-10 개편: 단락형 시황+신용·변동성+TOP5 실적 일정) ──
    # ★2026-08-03: 섹션 아이콘(📊🧭🏦📰📅🤖) 전부 제거 — 굵은 소제목 + 앞 빈 줄이면
    #   구획은 충분히 보인다. 상태 신호등(🟢🟡🔴)만 남긴다(값에 따라 바뀌므로 정보).
    m3 = [f'<b>AI 시장 분석</b> · {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━']
    idx_lines = []
    try:
        import yfinance as yf
        _syms = [('^GSPC', 'S&P500'), ('^IXIC', '나스닥'), ('^SOX', '반도체지수')]
        if not VM_US_ONLY:
            _syms += [('^KS11', '코스피'), ('^KQ11', '코스닥')]
        _syms += [('KRW=X', '원/달러')]   # 원화 투자자의 달러자산 평가 — 미국단독에서도 유지
        for sym, nm in _syms:
            try:
                fi = yf.Ticker(sym).fast_info
                px, pv = fi.last_price, fi.previous_close
                if px and pv:
                    idx_lines.append(f"{nm} {px:,.0f} ({(px / pv - 1) * 100:+.1f}%)")
            except Exception:
                pass
        if idx_lines:
            m3 += ['', '<b>주요 지수</b>'] + ['  ' + x for x in idx_lines]
    except Exception:
        pass
    if reg:
        st = {'defense': '🛑 방어 (주식 0%, 현금)',
              'half_defense': '🟠 부분 방어 (주식 50%)'}.get(regime, '🟢 강세 (주식 100%)')
        # 미국단독 모드에선 나라가 하나뿐이라 국기도 정보가 아니다 → 통합 모드에서만 표시.
        m3 += ['', '<b>시장 국면</b>', ('미국 ' if VM_US_ONLY else '🇺🇸 미국: ') + st]
        if reg.get('spx') and reg.get('ma200'):
            pos = '위' if reg['spx'] > reg['ma200'] else '아래'
            m3.append(f"  S&P500 {reg['spx']:,.0f} — 200일선({reg['ma200']:,.0f}) {pos}")
        # ★2026-07-31: 미국단독 모드에선 한국 국면 표시 제거 (메시지1은 이미 제외였으나
        #   메시지3이 누락돼 있었음). 한국 종목을 담지 않으므로 코스피 국면은 행동으로
        #   이어지지 않는다 — 매매 판단은 미국 신호(200일선·VIX·신용) 하나로 끝난다.
        #   한국 복귀 시 자동 원복.
        if krr and not VM_US_ONLY:
            kst = '🛑 약세' if krr['mode'] == 'defense' else '🟢 강세'
            m3.append(f"🇰🇷 한국: {kst} (참고 표시 · {krr['asof']} 기준"
                      + (' ⚠️스테일' if krr.get('stale') else '') + ')')
            m3.append('  코스피 20일선 vs 80일선 + 5일 확인 판정')
            if krr['pending_days'] > 0:
                nm = '약세' if krr['mode'] == 'boost' else '강세'
                m3.append(f"  ⚠️ {nm} 전환 진행 {krr['pending_days']}/5일")
        m3.append('약세 확정 시 전량 현금 안내가 나갑니다.')
    cv = _credit_vol_lines()
    if cv:
        m3 += ['', '<b>신용·변동성</b>'] + ['  ' + x for x in cv]
    brief_mkt = _ai_market_brief(idx_facts=idx_lines, _now=kdt)   # 요일 분기용 KST 시각 명시
    if brief_mkt:
        # ★2026-08-03: AI가 붙여 오는 [미국 증시]류 라벨을 굵은 소제목으로 승격시키므로,
        #   그 위에 '시장 동향' 굵은 제목을 또 얹으면 같은 굵기 제목이 2단으로 겹친다
        #   → 라벨이 하나라도 있으면 감싸는 제목 없이 다른 섹션들과 같은 층으로 편다.
        if '[' not in brief_mkt:
            m3 += ['', '<b>시장 동향</b>']
        # ★2026-08-01 강화: 8/1 실발송에서 Gemini가 전문을 한 응답에 두 번 뱉었고,
        #   라벨 하나가 문단 끝에 붙어 나와("...예상됩니다.[미국 증시]") 줄 단위 라벨
        #   dedup(7/31)이 못 잡았다 — 라벨만 지워지고 본문 3문단이 통째로 반복 발송됨.
        #   방어 2겹: ①문단 중간에 붙은 라벨 앞에 개행 강제 ②본문 자체를 문단 단위로
        #   dedup(같은 내용 재등장 시 스킵). AI 생성물은 프롬프트 부탁이 아니라 코드로 강제.
        import re as _re2
        _txt = _re2.sub(r'(?<!\n)\[(?=(미국 증시|반도체·메모리|한국 증시|오늘 밤 체크|다음 장 체크)\])',
                        '\n[', brief_mkt.replace('\r', ''))
        # ★2026-08-03: ①대괄호 라벨([미국 증시])을 다른 소제목과 같은 굵은 글씨로 통일
        #   — 메시지 안에 서로 다른 제목 표기가 두 종류 있을 이유가 없다.
        #   ②AI 본문은 한 문단이 통째로 한 줄이라 모바일에서 임의 지점에 접혔다 → _wrap으로
        #   46칸에 맞춰 미리 접는다(다른 모든 줄과 같은 폭 규칙 적용).
        _seen_lb = set(); _seen_body = set()
        for para in _txt.split('\n'):
            p = para.strip()
            if not p:
                continue
            if p.startswith('[') and p.endswith(']'):
                if p in _seen_lb:
                    continue
                _seen_lb.add(p)
                if m3 and m3[-1] != '':
                    m3.append('')
                m3.append('<b>' + p[1:-1].strip() + '</b>')
                continue
            _key = p[:60]
            if _key in _seen_body:
                continue
            _seen_body.add(_key)
            m3 += _wrap(p, 44)
        if m3[-1] == '':
            m3.pop()
    # ★2026-07-31: 구 라벨 '보유종목 일정'은 시스템이 모르는 사실(사용자 실제 보유)을
    #   단정하는 표현이었다. 표시하는 대상(오늘 순위 TOP N)에 맞춰 라벨을 정직하게 교정.
    el = _earnings_lines([d['ticker'] for d in top5])
    if el:
        m3 += ['', f'<b>TOP{N_TOP} 실적 발표 일정</b> (14일 내)'] + el
    if MEM_ALERT_SHOW:            # 메모리 감시등 상세 (기본 미노출, 2026-07-31)
        m3 += ['', amsg]
    _sha = _git_sha()
    if _sha:
        m3 += ['', f'<i>sys {_sha} · {today}</i>']  # 코드버전 — 낡은 코드 발송 식별용 (감사수리 3)
    # ── 발송 ──
    print('\n' + '\n'.join(m1).replace('<b>', '').replace('</b>', ''))
    if os.environ.get('UNIFIED_DRY_RUN') == '1':
        print('\n[UNIFIED_DRY_RUN=1] 발송 생략 — 메시지 2·3 미리보기:')
        if m2:
            print('\n'.join(m2).replace('<b>', '').replace('</b>', ''))
        print('\n'.join(m3).replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', ''))
        return
    _tk = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    _pid = os.environ.get('TELEGRAM_PRIVATE_ID', '')
    if not (_tk and _pid):
        # 폴백 1 = C:\dev\config.py (회사PC — 검증된 프로덕션 토큰. 집PC 것은 07-09 정리 때 삭제됨)
        try:
            sys.path.insert(0, r'C:\dev')
            from config import TELEGRAM_BOT_TOKEN as _tk, TELEGRAM_PRIVATE_ID as _pid
        except ImportError:
            # 폴백 2 = repo config.json (telegram_chat_id = 개인 유저 ID(양수) — daily_runner 패턴 준용.
            #   ⚠️집PC config.json 토큰은 2026-07-10 현재 401(폐기됨) — 유효 토큰으로 갱신 필요)
            import json as _j
            _cfg = _j.load(open(os.path.join(HERE, 'config.json'), encoding='utf-8'))
            _tk = _cfg['telegram_bot_token']
            _pid = _cfg.get('telegram_private_id') or _cfg['telegram_chat_id']
    _r = __import__('requests').get('https://api.telegram.org/bot%s/getMe' % _tk, timeout=15)
    if not _r.json().get('ok'):
        raise RuntimeError('텔레그램 토큰 무효(401) — 발송 불가. config 토큰을 갱신하세요.')
    _send_long(_tk, _pid, '\n'.join(m1))
    if m2:
        _send_long(_tk, _pid, '\n'.join(m2))
    _send_long(_tk, _pid, '\n'.join(m3))
    # ── 채널 발송 (2026-07-10 사용자 결정: US 채널 상품 = 아침 US 단독 → 저녁 통합으로 대체) ──
    # UNIFIED_CHANNEL_ID가 설정된 경우에만 발송(미설정 = 개인봇 단독 = 현행 유지).
    # 켜기: env UNIFIED_CHANNEL_ID(+봇이 다르면 UNIFIED_CHANNEL_BOT_TOKEN) 또는
    #       C:/dev/config.py에 UNIFIED_CHANNEL_ID(_BOT_TOKEN) 추가. 봇은 채널 관리자여야 함.
    # 안전판: 백분위 폴백(norm != 'pct') 등 본선 방식이 깨진 날은 채널 차단(개인봇만).
    ch_id = os.environ.get('UNIFIED_CHANNEL_ID', '')
    ch_tk = os.environ.get('UNIFIED_CHANNEL_BOT_TOKEN', '')
    if not ch_id:
        try:
            sys.path.insert(0, r'C:\dev')
            import config as _c2
            ch_id = str(getattr(_c2, 'UNIFIED_CHANNEL_ID', '') or '')
            ch_tk = ch_tk or str(getattr(_c2, 'UNIFIED_CHANNEL_BOT_TOKEN', '') or '')
        except ImportError:
            pass
    if ch_id:
        if (meta or {}).get('norm', 'pct') != 'pct':
            print('[채널 발송 차단: 본선(백분위) 폴백 상태 — 개인봇만 발송]')
            _send_long(_tk, _pid, '⚠️ 오늘 통합 신호는 백분위 계산 폴백 상태라 채널 발송을 건너뛰었습니다.')
        else:
            ch_tk = ch_tk or _tk
            _rc = __import__('requests').get('https://api.telegram.org/bot%s/getMe' % ch_tk, timeout=15)
            if _rc.json().get('ok'):
                _send_long(ch_tk, ch_id, '\n'.join(m1))
                if m2:
                    _send_long(ch_tk, ch_id, '\n'.join(m2))
                _send_long(ch_tk, ch_id, '\n'.join(m3))
                print(f'[채널 발송 완료: {ch_id[:6]}…]')
            else:
                print('[채널 봇 토큰 무효 — 채널 발송 실패, 개인봇은 발송됨]')


if __name__ == '__main__':
    sys.path.insert(0, HERE)
    # ★로컬 러너 원격 킬스위치 (2026-07-10): 실행 주체가 GH Actions로 이관됨(unified-signal.yml).
    # 회사PC schtask(run_unified_track.bat)는 실행 전 git pull을 하므로, 이 깃발 파일이
    # 저장소에 있으면 로컬(비-Actions) 실행은 스스로 종료 — 이중 발송·이중 원장 차단.
    # 로컬 실행을 되살리려면 LOCAL_RUNNER_OFF 파일 삭제. (schtask 자체는 여유 있을 때 삭제)
    if os.path.exists(os.path.join(HERE, 'LOCAL_RUNNER_OFF')) and not os.environ.get('GITHUB_ACTIONS'):
        print('[로컬 러너 OFF] 통합 신호는 GitHub Actions(18:15 KST)가 발송합니다 — 이 실행은 종료.')
        sys.exit(0)
    if '--nav' in sys.argv:
        cmd_nav()
    else:
        _merged_for_msg, _meta_for_msg = cmd_run()
        # 통합(US+KR) 신호 3종 발송 — 본선 (2026-07-09 사용자 확정)
        try:
            _compose_and_send(_merged_for_msg, _meta_for_msg)
        except Exception as _e:
            import traceback
            traceback.print_exc()
            print(f'[!!] 통합신호 발송 실패 — 메시지가 나가지 않았습니다: {_e}')
