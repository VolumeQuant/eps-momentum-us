# -*- coding: utf-8 -*-
"""시행착오 전수 카탈로그 + 패턴 EDA: 왜 실패하는가의 메커니즘을 정량 집계 → 새 방향.
layer: 선택(stock selection)/비중(weight)/방어(defense regime)/구조(param)/유니버스/데이터
verdict: WIN / reject / blocked / current(현행유지)
mech: 실패 메커니즘 태그
data: 검증에 쓴 데이터(88d=단일강세장 88일 종목 / 26y=26년 프록시 / FRED=차단 / KR)
"""
import sys
from collections import Counter
sys.stdout.reconfigure(encoding='utf-8')

# (이름, layer, verdict, mechanism, data)
T = [
    # ── 방어/국면 레이어 (대부분 26년 프록시 검증) ──
    ("인버스 ETF", "방어", "reject", "decay+whipsaw", "26y"),
    ("MA200 버퍼밴드", "방어", "reject", "overfit(단일임계)", "26y"),
    ("VIX-norm 재진입", "방어", "reject", "micro-gain", "26y"),
    ("VIX 텀구조(VIX/VIX3M)", "방어", "reject", "LOWO실패(V-crash의존)", "26y"),
    ("vol-scaling", "방어", "reject", "LOWO실패(V-crash의존)", "26y"),
    ("tiered 50%방어", "방어", "reject", "worse(둘다악화)", "26y"),
    ("IEF 방어자산", "방어", "reject", "regime의존(2022금리)", "26y"),
    ("HY스프레드 레벨", "방어", "reject", "redundant(MA200/VIX중복)", "26y"),
    ("VIX>25", "방어", "reject", "oversensitive", "26y"),
    ("수익률곡선 dis-inversion", "방어", "reject", "lagging(2000갭 못막음)", "26y"),
    ("방어/경기 섹터로테이션", "방어", "reject", "worse(MDD악화)", "26y"),
    ("Growth-Trend-Timing", "방어", "reject", "catastrophic", "26y"),
    ("절대모멘텀(12m)", "방어", "reject", "worse", "26y"),
    ("SKEW-VIX 다이버전스", "방어", "reject", "no-MDD-help", "26y"),
    ("VVIX", "방어", "reject", "overlap+2007only", "26y"),
    ("copper/gold", "방어", "reject", "coincident(선행아님)", "26y"),
    ("★섹터 브레드스", "방어", "WIN", "—(robust)", "26y"),
    ("브레드스 복귀 3일", "방어", "reject", "whipsaw(15일이 옳음)", "26y"),
    ("브레드스 C1 50%스케일", "방어", "WIN", "—(binary보다 robust)", "26y"),
    ("HY-OAS 변화율", "방어", "blocked", "데이터차단(FRED)", "FRED"),
    ("NFCI 금융컨디션", "방어", "blocked", "데이터차단(FRED)", "FRED"),
    ("EBP 초과채권프리미엄", "방어", "blocked", "데이터차단(FRED)", "FRED"),
    ("근월선도스프레드(NTFS)", "방어", "blocked", "데이터차단(FRED)", "FRED"),
    # ── 선택/비중 레이어 (전부 88일 종목 데이터) ──
    ("gap 점수주입(가산)", "선택", "reject", "winner밀어냄", "88d"),
    ("gap 점수주입(곱셈)", "선택", "reject", "winner밀어냄", "88d"),
    ("gap sleeve momentum필터", "선택", "reject", "트랩분리불가(CIEN)", "88d"),
    ("등수 50/50 블렌드", "선택", "reject", "winner탈락/트랩", "88d"),
    ("자본 블렌드 70/30", "비중", "reject", "corr0.83(분산효과0)", "88d"),
    ("순수 교집합", "선택", "reject", "식은winner탈락", "88d"),
    ("교집합 min-K", "선택", "reject", "희석/트랩복귀", "88d"),
    ("2슬롯 이중확인 오버레이", "선택", "reject", "LOWO실패(-40p)", "88d"),
    ("월간 융합", "선택", "reject", "winner순간놓침", "88d"),
    ("daily 융합 슬롯3", "구조", "reject", "config artifact", "88d"),
    ("gap 가속도 EXIT(트랩탐지)", "선택", "reject", "트랩분리불가(CIEN)", "88d"),
    ("gap-factor 타이밍", "선택", "reject", "mean-reversion", "88d"),
    ("gap-breadth 레짐", "선택", "reject", "변동0(dead)", "88d"),
    ("KR 확신가중 이식", "비중", "reject", "corr0.83(KR≠US)", "88d"),
    ("top종목 2x/3x 집중", "비중", "reject", "LOWO실패(SNDK몰빵)", "88d"),
    ("레버리지 2x/3x", "비중", "reject", "tail/mechanical착시", "88d"),
    ("gap 진입필터(압도적)", "선택", "reject", "winner탈락(-50p)", "88d"),
    ("forward<15 슬리브", "선택", "reject", "LOWO실패(=SNDK,-183p)", "88d"),
    ("gap sleeve K7 단독", "선택", "weak-option", "corr0.83(분산효과작음)", "88d"),
    # ── 구조/파라미터 (88일) ──
    ("단일슬롯 S1", "구조", "reject", "MDD악화+집중", "88d"),
    ("이탈선 H12", "구조", "current", "promising-not-proven", "88d"),
    ("$1B 순위단계 필터", "구조", "reject", "winner놓침(-72p)", "88d"),
    ("슬롯3", "구조", "reject", "수익붕괴(-90p)", "88d"),
    ("PE_HOLD 25/40", "구조", "current", "노이즈/비단조", "88d"),
]

print(f"총 시행: {len(T)}건\n")

print("=== [1] verdict 분포 ===")
for v, n in Counter(x[2] for x in T).most_common():
    print(f"  {v:12} {n:2}건")

print("\n=== [2] layer × verdict 교차 (★핵심 패턴) ===")
layers = ["방어", "선택", "비중", "구조", "유니버스"]
verds = ["WIN", "current", "weak-option", "reject", "blocked"]
print(f'{"layer":>8}' + "".join(f"{v:>11}" for v in verds))
for L in layers:
    row = {v: sum(1 for x in T if x[1] == L and x[2] == v) for v in verds}
    if sum(row.values()):
        print(f'{L:>8}' + "".join(f"{row[v]:>11}" for v in verds))

print("\n=== [3] data(검증표본) × WIN/robust 여부 ===")
for dat in ["26y", "88d", "FRED", "KR"]:
    sub = [x for x in T if x[4] == dat]
    wins = sum(1 for x in sub if x[2] == "WIN")
    print(f"  {dat:5}: {len(sub):2}건 중 robust WIN {wins}건  ({'장기프록시' if dat=='26y' else '단일강세장88일' if dat=='88d' else dat})")

print("\n=== [4] 실패 메커니즘 빈도 (reject만) ===")
for m, n in Counter(x[3] for x in T if x[2] == "reject").most_common():
    print(f"  {n}x  {m}")

print("\n=== [5] 'SNDK/MU 단일winner 의존' or 'corr/이미캡처/트랩' 태그 빈도 ===")
single = sum(1 for x in T if any(k in x[3] for k in ["SNDK", "winner", "LOWO", "트랩", "corr", "redundant"]))
print(f"  단일winner/상관/트랩/중복 계열 실패: {single}건 (= 좁은유니버스+집중 본질)")
print(f"  overfit/config/micro/표본 계열: {sum(1 for x in T if any(k in x[3] for k in ['overfit','config','micro','mean-rev','변동0']))}건")
print(f"  데이터차단(미검증): {sum(1 for x in T if x[2]=='blocked')}건")
