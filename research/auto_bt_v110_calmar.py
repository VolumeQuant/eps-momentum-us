# -*- coding: utf-8 -*-
"""V110a/b/c calmar 비교 (수익/MDD)"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
data_all = {}
for d in dates:
    data_all[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?', (d,)).fetchall():
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]; fpe = (r[3]/nc) if (r[3] and nc>0) else None
        peg = (fpe/(rg*100)) if (fpe and rg and rg>0) else None
        ntm_rev = (nc/n90-1)*100 if (nc and n90 and n90>0) else 0
        rg_pct = rg*100 if rg else 0
        peg_inv = 1/peg if peg and peg > 0 else 0
        data_all[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                               min_seg=min(segs) if segs else 0, high30=r[10],
                               peg=peg, rev_growth=rg, ntm_rev=ntm_rev, rg_pct=rg_pct, peg_inv=peg_inv)
pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p

def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data_all[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True

def is_mega(info):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= 0.25: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True

def mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)


def sim_with_mdd(variant, exclude=(), start=0):
    """수익률 + MDD 둘 다 반환"""
    held = {}; prev = None; val = 1.0
    peak = 1.0; mdd = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret)
            peak = max(peak, val)
            dd_now = (val/peak - 1) * 100
            mdd = min(mdd, dd_now)
        dd = data_all[d]
        for tk in list(held):
            info = dd.get(tk)
            if info is None: continue
            if info.get('min_seg', 0) < -2: del held[tk]; continue
            if is_mega(info):
                if info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]
                continue
            p2 = info.get('p2')
            if p2 is None or p2 > 10:
                del held[tk]
        if len(held) < 2:
            p2_cands, mega_cands = [], []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is not None and p2 <= 3:
                    p2_cands.append((p2, info['score'], tk))
                if is_mega(info):
                    mega_cands.append((-mega_score(info), info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])
            pick_p2 = next((c for c in p2_cands if c[2] not in held), None)
            pick_mega = next((c for c in mega_cands if c[2] not in held), None)
            if len(held) == 0:
                if pick_p2 and pick_mega and pick_p2[2] != pick_mega[2]:
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
                elif pick_p2:
                    if variant == 'v110b':
                        held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                        pick_p2_2 = next((c for c in p2_cands if c[2] != pick_p2[2] and c[2] not in held), None)
                        if pick_p2_2:
                            held[pick_p2_2[2]] = (d, dd[pick_p2_2[2]]['price'], 0.5)
                        else:
                            held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 1.0)
                    elif variant == 'v110c':
                        held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                    else:  # v110a
                        held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 1.0)
            elif len(held) == 1:
                if pick_mega and pick_mega[2] not in held:
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
                elif variant == 'v110b' and pick_p2 and pick_p2[2] not in held:
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(variant, exclude=()):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim_with_mdd(variant, exclude=exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


print('=' * 100)
print('V110a/b/c — calmar (수익/|MDD|) 비교')
print('=' * 100)

exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')), ('-all5', ('SNDK', 'MU', 'BE', 'LITE', 'TTMI'))]

print(f'{"variant":<10}{"수익":>11}{"MDD":>11}{"calmar":>11}{"양수":>10}')
print('-' * 60)
for v in ['v110a', 'v110b', 'v110c']:
    cums, mdds = run(v, ())
    avg_ret = statistics.mean(cums)
    avg_mdd = statistics.mean(mdds)
    calmar = avg_ret / abs(avg_mdd) if avg_mdd != 0 else 0
    pos = sum(1 for c in cums if c > 0)
    print(f'{v:<10}{avg_ret:>+10.1f}%{avg_mdd:>+10.1f}%{calmar:>10.2f}  {pos}/{len(cums)}')

print()
print('시나리오별 calmar:')
print(f'{"scenario":<12}' + ''.join(f'{v:>11}' for v in ['v110a', 'v110b', 'v110c']))
for n, ex in exclusions:
    line = f'{n:<12}'
    for v in ['v110a', 'v110b', 'v110c']:
        cums, mdds = run(v, ex)
        avg_ret = statistics.mean(cums)
        avg_mdd = statistics.mean(mdds)
        calmar = avg_ret / abs(avg_mdd) if avg_mdd != 0 else 0
        line += f'{calmar:>10.2f}  '
    print(line)

# worst MDD
print()
print('worst MDD (300 시뮬 중 최악):')
for v in ['v110a', 'v110b', 'v110c']:
    cums, mdds = run(v, ())
    worst_mdd = min(mdds)
    print(f'  {v}: worst MDD {worst_mdd:.1f}%')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
