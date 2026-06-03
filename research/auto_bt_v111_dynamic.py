# -*- coding: utf-8 -*-
"""V111 동적 시스템 — 메가 활성 시 v86e+, 부재 시 V110

조건:
- 현재 시뮬 시점에 메가 시그니처 있는 종목 (시스템이 보유 중) → v86e+ logic (carryover만)
- 메가 없으면 → V110 logic (mega_score entry 보강)

이게 진짜 두 조건 동시 만족 가능?
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
data = {}
for d in dates:
    data[d] = {}
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
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, ntm_rev=ntm_rev, rg_pct=rg_pct, peg_inv=peg_inv)

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p

def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True

def is_mega_strict(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr

def is_mega_v110(info, peg_thr=0.25):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True

def get_mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50*info.get('peg_inv', 0)


def sim(variant, exclude=(), start=0):
    """variant: v86e+, v110, v111_dynamic"""
    held = {}; prev = None; val = 1.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret)
        dd = data[d]

        # 매도 (variant 동일)
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                mega_check = is_mega_v110(info) if variant in ('v110', 'v111_dynamic') else is_mega_strict(info)
                if mega_check and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                mega_check = is_mega_v110(info) if variant in ('v110', 'v111_dynamic') else is_mega_strict(info)
                if mega_check: continue
                del held[tk]

        # 매수
        if len(held) < 2:
            # v111_dynamic: 현재 보유 메가가 있으면 v86e+, 없으면 v110
            if variant == 'v111_dynamic':
                has_mega = any(is_mega_v110(dd.get(tk)) for tk in held)
                use_v110_entry = not has_mega
            elif variant == 'v110':
                use_v110_entry = True
            else:
                use_v110_entry = False

            mega_cands = []; p2_cands = []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is not None and p2 <= 3:
                    p2_cands.append((p2, info['score'], tk))
                if use_v110_entry and is_mega_v110(info):
                    mega_cands.append((-get_mega_score(info), info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])

            if use_v110_entry:
                seen = set(); pick = []
                if p2_cands: pick.append(p2_cands[0]); seen.add(p2_cands[0][2])
                if mega_cands and mega_cands[0][2] not in seen:
                    pick.append(mega_cands[0])
                pick = pick[:2-len(held)]
                if len(held) == 0 and len(pick) >= 2:
                    for si, (_, _, tk) in enumerate(pick[:2]):
                        held[tk] = (d, dd[tk]['price'], 0.5)
                else:
                    for _, _, tk in pick:
                        held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
            else:
                pick = p2_cands[:2-len(held)]
                if len(held) == 0 and len(pick) >= 2:
                    s1, s2 = pick[0][1], pick[1][1]
                    w = [1.0, 0.0] if (s1-s2) >= 15 else [0.5, 0.5]
                    for si, (_, _, tk) in enumerate(pick[:2]):
                        if w[si] > 0: held[tk] = (d, dd[tk]['price'], w[si])
                else:
                    for _, _, tk in pick:
                        held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return (val-1)*100


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))

def run_avg(variant, ex=()):
    cums = []
    for ch in seeds:
        for s in ch:
            cums.append(sim(variant, exclude=ex, start=s))
    return statistics.mean(cums), cums


print('=' * 100)
print('V111 동적 시스템 — 메가 활성 시 v86e+, 부재 시 V110')
print('=' * 100)

exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')),
              ('-SNDK-MU-LITE', ('SNDK', 'MU', 'LITE')),
              ('-all5', ('SNDK', 'MU', 'BE', 'LITE', 'TTMI'))]

print(f'{"variant":<14}' + ''.join(f'{n:>11}' for n, _ in exclusions))
print('-' * 90)
results = {}
for v in ['v86e+', 'v110', 'v111_dynamic']:
    avgs = {n: run_avg(v, ex)[0] for n, ex in exclusions}
    results[v] = avgs
    print(f'{v:<14}' + ''.join(f'{avgs[n]:>+10.1f}%' for n, _ in exclusions))

print(f'\n[paired diff vs v86e+]')
for v in ['v110', 'v111_dynamic']:
    print(f'\n  {v} vs v86e+:')
    for n, ex in exclusions:
        _, cums_v = run_avg(v, ex)
        _, cums_86 = run_avg('v86e+', ex)
        diffs = [a-b for a, b in zip(cums_v, cums_86)]
        avg_d = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
        print(f'    {n}: {avg_d:+.1f}p ({wins}/{len(diffs)})')

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
