# -*- coding: utf-8 -*-
"""재설계 v2 검증 심화 — 가치우선 top-N의 robust 최적 config 확정.
①pe_max 인접성(plateau vs spike) ②N 스윕 ③리밸주기 ④rankby ⑤walk-forward ⑥LOWO ⑦회전."""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
adset = set(ad); raw = {}
for tk, d, px, nc, dv, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,price,ntm_current,dollar_volume_30d,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0 ORDER BY date'):
    raw.setdefault(tk, {})[d] = dict(px=px, nc=nc, dv=dv, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
DVF = {}
for tk, dd in raw.items():
    last = None
    for d in sorted(dd):
        if dd[d]['dv'] is not None: last = dd[d]['dv']
        DVF.setdefault(tk, {})[d] = last
FULL = {}
for tk, dd in raw.items():
    for d, v in dd.items():
        if d in adset: FULL.setdefault(d, {})[tk] = v
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}
def ms(v):
    o = [((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0) for a, b in
         [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]]
    return min(o)
def mom(v): return (v['nc'] / v['n90'] - 1) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0
def pick(d, N, pe_max, rankby):
    q = []
    for tk, v in FULL.get(d, {}).items():
        if (DVF.get(tk, {}).get(d) or 0) < 1000: continue
        if ms(v) < 0: continue
        fpe = v['px'] / v['nc']
        if fpe > pe_max: continue
        q.append((tk, fpe, mom(v)))
    if rankby == 'value': q.sort(key=lambda x: x[1])
    elif rankby == 'mom': q.sort(key=lambda x: -x[2])
    else:
        bv = {t: i for i, (t, _, _) in enumerate(sorted(q, key=lambda x: x[1]))}
        bm = {t: i for i, (t, _, _) in enumerate(sorted(q, key=lambda x: -x[2]))}
        q.sort(key=lambda x: bv[x[0]] + bm[x[0]])
    return [t for t, _, _ in q[:N]]
def run(N, R, pe_max, rankby='blend', start=2, end=None, ban=()):
    end = end or len(ad); hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; turn = 0; navs = []
    for i in range(start, end):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1); navs.append(nav)
        if (i - start) % R == 0:
            tgt = [t for t in pick(d, N, pe_max, rankby) if t not in ban]
            turn += len([t for t in tgt if t not in hold]); hold = tgt
    return (nav - 1) * 100, mdd * 100, turn

print('=== ① pe_max 인접성 (N5, 주1, blend) — plateau인가 spike인가 ===')
for pe in [12, 13, 14, 15, 16, 18, 20, 25, 30]:
    r = run(5, 5, pe); print(f'  PER<={pe:>2}: {r[0]:>+6.0f}%  MDD{r[1]:>+5.0f}  {r[2]:>3}회')
print('\n=== ② N 스윕 (주1, blend) ===')
for pe in [15, 20]:
    for N in [3, 5, 8, 10]:
        r = run(N, 5, pe); print(f'  PER<={pe} top{N:>2}: {r[0]:>+6.0f}%  MDD{r[1]:>+5.0f}  {r[2]:>3}회')
print('\n=== ③ 리밸주기 (N5, blend) ===')
for pe in [15, 20]:
    for R in [1, 5, 10, 20]:
        r = run(5, R, pe); print(f'  PER<={pe} R={R:>2}일: {r[0]:>+6.0f}%  MDD{r[1]:>+5.0f}  {r[2]:>3}회')
print('\n=== ④ rankby (N5 주1) ===')
for pe in [15, 20]:
    for rb in ['mom', 'value', 'blend']:
        r = run(5, 5, pe, rb); print(f'  PER<={pe} {rb:>6}: {r[0]:>+6.0f}%  MDD{r[1]:>+5.0f}')
print('\n=== ⑤ walk-forward (전반 vs 후반, PER<=15/20 N5 주1) ===')
mid = 2 + (len(ad) - 2) // 2
for pe in [15, 20]:
    a = run(5, 5, pe, 'blend', 2, mid); b = run(5, 5, pe, 'blend', mid, len(ad))
    print(f'  PER<={pe}: 전반 {a[0]:+.0f}%(MDD{a[1]:.0f}) | 후반 {b[0]:+.0f}%(MDD{b[1]:.0f})  ← 둘다 양수면 robust')
print('\n=== ⑥ LOWO (PER<=15/20 N5 주1 blend) ===')
for pe in [15, 20]:
    f = run(5, 5, pe)[0]; l2 = run(5, 5, pe, 'blend', 2, None, {'MU', 'SNDK'})[0]
    l4 = run(5, 5, pe, 'blend', 2, None, {'MU', 'SNDK', 'NVDA', 'C'})[0]
    print(f'  PER<={pe}: full {f:+.0f}% | -MU·SNDK {l2:+.0f}% | -4대 {l4:+.0f}%')
