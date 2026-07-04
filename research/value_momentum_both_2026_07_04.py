# -*- coding: utf-8 -*-
"""사용자 스펙: ①가치 게이트(싸다=fwd_PER<=상한) 먼저 → ②그 안에서 모멘텀(전망 상향폭) 좋은순 top-N.
'모멘텀만 좋고 비싼 것'(NVDA류) 배제. SNDK/MU(싸고+전망오름) 포함되나. 단일기준=coherent.
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
FULL = {}
for tk, d, p2, px, nc, dv, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,part2_rank,price,ntm_current,dollar_volume_30d,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(p2=p2, px=px, nc=nc, dv=dv, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def revision(v):   # 90일 전망 상향폭 = 모멘텀 세기
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

def pick(d, N, pe_max):
    """가치게이트(fwd_PER<=pe_max) + 전망오름(min_seg>=0) + $1B → 그 안에서 모멘텀(상향폭) 높은순 top-N."""
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if (v['dv'] or 0) < 1000: continue      # $1B 유동성
        if ms(v) < 0: continue                   # 전망 안 꺾임
        fpe = v['px'] / v['nc']
        if fpe > pe_max: continue                # ★가치 게이트: 싸야 함
        cand.append((tk, -revision(v)))          # 모멘텀(상향폭) 높은순
    cand.sort(key=lambda x: x[1])
    return [t for t, _ in cand[:N]]

def run(N, R, pe_max, start=2):
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if (i - start) % R == 0:
            hold = pick(d, N, pe_max)
    return (nav - 1) * 100, mdd * 100, set(hold)

print('=== 가치게이트(fwd_PER<=X) → 모멘텀순 top5, 주1회 ===')
print('  (현행 모멘텀-only carryover = +210%/MDD-27, 비일관)')
for pe in [15, 20, 25, 30]:
    r = run(5, 5, pe)
    print(f'  fwd_PER<={pe}: {r[0]:>+6.0f}%  MDD{r[1]:>+5.0f}  | 오늘보유 {sorted(r[2])}')
print()
print('=== SNDK/MU 오늘 포함되나 (가치게이트별) ===')
for pe in [15, 20, 30]:
    top10 = pick(ad[-1], 10, pe)
    sk = 'O' if 'SNDK' in top10 else 'X'; mu = 'O' if 'MU' in top10 else 'X'
    print(f'  fwd_PER<={pe}: SNDK={sk} MU={mu} | top10={top10}')
print()
print('=== coherence (fwd_PER<=20, top5 주1회) 시작일 무관? ===')
for s in [2, 20, 40]:
    print(f'  시작 {ad[s]}: {sorted(run(5,5,20,s)[2])}')
