# -*- coding: utf-8 -*-
"""V90 자율주행 — PEG × rev_growth cutoff 정밀 grid (V86e 환경)

전문가 권고:
- PEG cutoff 0.20은 phase grid에서 sweep 안 됨
- rev_growth<0.25는 fit risk → 0.15가 더 safe

V86e 환경에서 정밀 grid:
  PEG: 0.10 / 0.12 / 0.15 / 0.18 / 0.20 / 0.22 / 0.25 / 0.30  (8개)
  rev_exit: 0.00 / 0.10 / 0.15 / 0.20 / 0.25 / 0.30  (6개)
  = 48 cells

각 cell paired BT + LOWO -MU-SNDK + 부분기간.
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
N_SEEDS = 100
SAMPLES = 3
MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data = {}
for d in dates:
    data[d] = {}
    for r in cur.execute(
        '''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?''',
        (d,)):
        tk = r[0]
        nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a - b) / abs(b) * 100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg)

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i - 1, i - 2):
        if j < 0:
            return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30:
            return False
    return True


def sim(use_mega, peg_thr=0.20, rev_exit=0.25, exclude=(), start=0):
    held = {}
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i - 1]
            ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk)
                pn = pf[d].get(tk, pp)
                if pp and pn:
                    ret += w * (pn / pp - 1)
            val *= (1 + ret)
            peak = max(peak, val)
            mdd = max(mdd, (peak - val) / peak)
        dd = data[d]
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                if use_mega and rev_exit > 0 and info.get('rev_growth') is not None and info['rev_growth'] < rev_exit:
                    del held[tk]
                    continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                is_mega = (use_mega and info is not None and info.get('peg') is not None
                           and info['peg'] < peg_thr)
                if is_mega:
                    continue
                del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > 3:
                    continue
                if tk in held or tk in exclude:
                    continue
                if info.get('min_seg') is not None and info['min_seg'] < 0:
                    continue
                if not info['price']:
                    continue
                if not verified(tk, i):
                    continue
                if info.get('high30') and info['price'] and info['price'] / info['high30'] - 1 < -0.25:
                    continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2 - len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(use_mega, peg_thr=0.20, rev_exit=0.25, exclude=()):
    cums = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(use_mega, peg_thr=peg_thr, rev_exit=rev_exit, exclude=exclude, start=s)
            cums.append(r['cum'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, savg


print('=' * 100)
print('V90 자율주행: PEG × rev_growth cutoff 48 cells 정밀 grid (V86e 환경)')
print('=' * 100)

_, base_savg = run(False)
print(f'\nbaseline avg: {sum(base_savg)/len(base_savg):+.1f}%')

PEG_GRID = [0.10, 0.12, 0.15, 0.18, 0.20, 0.22, 0.25, 0.30]
REV_GRID = [0.00, 0.10, 0.15, 0.20, 0.25, 0.30]

# Lift matrix
print(f'\n[Lift matrix vs baseline (paired, /100 wins)]')
print(f'{"PEG\\rev":<10}' + ''.join(f'{r:>10.2f}' for r in REV_GRID))
print('-' * (10 + 10 * len(REV_GRID)))

results = {}
for peg in PEG_GRID:
    row_str = f'{peg:<10.2f}'
    for re in REV_GRID:
        _, savg = run(True, peg_thr=peg, rev_exit=re)
        lifts = [b - a for a, b in zip(base_savg, savg)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        results[(peg, re)] = (avg_lift, wins)
        row_str += f'{avg_lift:>+9.1f}p'
    print(row_str)
    sys.stdout.flush()

# Wins matrix
print(f'\n[Wins matrix]')
print(f'{"PEG\\rev":<10}' + ''.join(f'{r:>10.2f}' for r in REV_GRID))
print('-' * (10 + 10 * len(REV_GRID)))
for peg in PEG_GRID:
    row_str = f'{peg:<10.2f}'
    for re in REV_GRID:
        _, wins = results[(peg, re)]
        row_str += f'{wins:>7}/100'
    print(row_str)

# Top cells
print('\n[Top 10 cells (lift desc, wins>=95)]')
sorted_cells = sorted([(k, v) for k, v in results.items() if v[1] >= 95], key=lambda x: -x[1][0])
print(f'{"rank":<6}{"PEG":>8}{"rev":>8}{"lift":>10}{"wins":>10}')
for i, ((peg, re), (lift, wins)) in enumerate(sorted_cells[:10], 1):
    print(f'{i:<6}{peg:>8.2f}{re:>8.2f}{lift:>+9.1f}p{wins:>7}/100')

# LOWO -MU-SNDK for top cells
print('\n[Top 10 cells LOWO -MU-SNDK]')
print(f'{"rank":<6}{"PEG":>8}{"rev":>8}{"lift":>10}{"LOWO -MU-SNDK":>20}')
for i, ((peg, re), (lift, wins)) in enumerate(sorted_cells[:10], 1):
    _, b_ex = run(False, exclude=('MU', 'SNDK'))
    _, n_ex = run(True, peg_thr=peg, rev_exit=re, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    lift_lowo = sum(lifts_lowo) / len(lifts_lowo)
    wins_lowo = sum(1 for l in lifts_lowo if l > 0)
    print(f'{i:<6}{peg:>8.2f}{re:>8.2f}{lift:>+9.1f}p{lift_lowo:>+10.1f}p({wins_lowo:>3})')

# Robustness gate: lift > 90% of max AND wins >= 95 AND LOWO -MU-SNDK >= 0
print('\n[Robust plateau — lift ≥ 90% of max AND wins ≥ 95 AND LOWO -MU-SNDK ≥ 0]')
max_lift = sorted_cells[0][1][0] if sorted_cells else 0
print(f'max lift: {max_lift:+.1f}p, 90% 기준: {0.9*max_lift:+.1f}p')

robust_cells = []
for (peg, re), (lift, wins) in results.items():
    if lift < 0.9 * max_lift or wins < 95:
        continue
    _, b_ex = run(False, exclude=('MU', 'SNDK'))
    _, n_ex = run(True, peg_thr=peg, rev_exit=re, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    lift_lowo = sum(lifts_lowo) / len(lifts_lowo)
    wins_lowo = sum(1 for l in lifts_lowo if l > 0)
    if lift_lowo >= 0 and wins_lowo >= 80:
        robust_cells.append(((peg, re), lift, wins, lift_lowo, wins_lowo))

print(f'\nrobust cells: {len(robust_cells)}')
print(f'{"PEG":>6}{"rev":>6}{"lift":>10}{"wins":>10}{"LOWO":>10}{"LOWO wins":>11}')
for (peg, re), lift, wins, lift_lowo, wins_lowo in robust_cells:
    print(f'{peg:>6.2f}{re:>6.2f}{lift:>+9.1f}p{wins:>7}/100{lift_lowo:>+9.1f}p{wins_lowo:>7}/100')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
