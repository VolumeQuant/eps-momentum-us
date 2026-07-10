# -*- coding: utf-8 -*-
"""US/KR rev90 분포 정밀 EDA + 정규화 잣대 후보 전부 비교."""
import os, sys, math, statistics
sys.stdout.reconfigure(encoding='utf-8')
BASE = r'C:/dev/claude code/eps-momentum-us/.claude/worktrees/gap-backsolve-findings'
sys.path.insert(0, BASE)
os.chdir(BASE)
import unified_vm_track as u

uus = sorted(u._universe_rev90(os.path.join(BASE, 'eps_momentum_data.db')))
ukr = sorted(u._universe_rev90(os.path.join(BASE, 'research', 'kr_db_snapshot_2026_07_09.db'), n90_floor=100.0))

def q(a, p):
    i = (len(a) - 1) * p
    lo, hi = int(i), min(int(i) + 1, len(a) - 1)
    return a[lo] + (a[hi] - a[lo]) * (i - lo)

def moments(a):
    n = len(a)
    m = sum(a) / n
    sd = (sum((x - m) ** 2 for x in a) / n) ** 0.5
    skew = sum((x - m) ** 3 for x in a) / n / sd ** 3
    return m, sd, skew

print('=== 1. 분위수 사다리 (rev90 %) ===')
print(f"{'':8}{'N':>6}{'p5':>8}{'p10':>8}{'p25':>8}{'p50':>8}{'p75':>8}{'p90':>8}{'p95':>8}{'p99':>8}{'max':>9}")
for nm, a in (('US', uus), ('KR', ukr)):
    print(f"{nm:8}{len(a):>6}" + ''.join(f'{q(a, p):>8.1f}' for p in (.05, .10, .25, .50, .75, .90, .95, .99)) + f'{a[-1]:>9.0f}')

print()
print('=== 2. 위치·척도·모양 ===')
for nm, a in (('US', uus), ('KR', ukr)):
    med = statistics.median(a)
    mad = statistics.median([abs(x - med) for x in a])
    m, sd, sk = moments(a)
    print(f'{nm}: 중앙값 {med:+.1f}  MAD {mad:.1f}  평균 {m:+.1f}  표준편차 {sd:.1f}  왜도 {sk:+.1f}  음수비중 {sum(1 for x in a if x < 0)/len(a)*100:.0f}%')

print()
print('=== 3. 모양 비교 — robust 표준화 후 분위수 (모양 같으면 두 줄이 비슷해야) ===')
print(f"{'':8}{'p10':>7}{'p25':>7}{'p75':>7}{'p90':>7}{'p95':>7}{'p99':>7}")
for nm, a in (('US', uus), ('KR', ukr)):
    med = statistics.median(a)
    mad = statistics.median([abs(x - med) for x in a])
    print(f'{nm:8}' + ''.join(f'{(q(a, p) - med) / mad:>7.2f}' for p in (.10, .25, .75, .90, .95, .99)))

print()
print('=== 4. 분위수 대응 (QQ) — "KR의 상위 X% 값 ÷ US의 상위 X% 값" 배율이 일정한가 ===')
print('   일정하면 순수 스케일 차이(robust-z로 충분), 위치별로 다르면 모양 차이(백분위 필요)')
for p in (.50, .75, .90, .95, .99):
    r = q(ukr, p) / q(uus, p) if q(uus, p) > 0 else float('nan')
    print(f'   p{int(p*100)}: KR {q(ukr,p):+7.1f} / US {q(uus,p):+7.1f} = {r:.2f}x')

print()
print('=== 5. 잣대 후보 전부 → 오늘 후보들에 적용 ===')
cands = [
    ('SNDK', 'US', 165.2), ('MU', 'US', 67.9), ('HPE', 'US', 49.2), ('FLEX', 'US', 41.8),
    ('MCHP', 'US', 27.4),
    ('하이닉스', 'KR', 82.73), ('삼성', 'KR', 63.82), ('이노텍', 'KR', 50.65),
]

def pct(a, x):
    return sum(1 for v in a if v < x) / len(a) * 100

def rz(a, x):
    med = statistics.median(a)
    mad = statistics.median([abs(v - med) for v in a])
    return (x - med) / mad * 0.6745

def logz(a, x):
    la = [math.log1p(max(v, -99) / 100) for v in a]
    m, sd, _ = moments(la)
    return (math.log1p(x / 100) - m) / sd

methods = {'백분위(본선)': pct, 'robust-z': rz, 'log-z(왜도교정)': logz}
for mname, fn in methods.items():
    scored = sorted(((fn(uus if m == 'US' else ukr, r), r, t) for t, m, r in cands),
                    key=lambda x: (-x[0], -x[1]))
    print(f'{mname:16} top5={[t for _, _, t in scored[:5]]}')
    print('   ' + '  '.join(f'{t}:{s:.2f}' for s, _, t in scored))

print()
print('=== 6. 경계 통계 신뢰도 — 삼성 백분위의 표본 오차 (N=202 이항 근사) ===')
p_s = pct(ukr, 63.82) / 100
se = (p_s * (1 - p_s) / len(ukr)) ** 0.5 * 100
print(f'삼성 {p_s*100:.1f}%ile ± {se:.1f}%p (1σ) | FLEX와 갭 {pct(uus, 41.8) - p_s*100:.1f}%p = {(pct(uus, 41.8) - p_s*100)/se:.1f}σ')
