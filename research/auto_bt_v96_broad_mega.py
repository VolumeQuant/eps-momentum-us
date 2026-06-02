# -*- coding: utf-8 -*-
"""V96 — Broad 메가 시그니처 (BE/LITE/TTMI 포함)

사용자 발견: SNDK/MU 외에 BE/LITE/TTMI도 +56~75% winner.
- BE: PEG 2.26 (메가홀드 X), NTM +64%, rev_g 36%
- LITE: PEG 0.84 (메가홀드 X), NTM +83%, rev_g 66%
- TTMI: PEG 1.47 (메가홀드 X), NTM +22%, rev_g 19%

V86e+ (PEG<0.22)는 SNDK/MU만 잡음. BE/LITE/TTMI는 일반 logic으로 매도 위험.

Broad 시그니처 후보:
  B1: PEG<0.22 OR (NTM≥50 AND rev_g≥40)   — SNDK/MU/BE/LITE 잡음
  B2: PEG<0.50 (broad PEG)
  B3: PEG<1.0 (very broad)
  B4: NTM≥50 only
  B5: NTM≥30 only
  B6: PEG<0.22 OR (NTM≥80 AND PEG<1.0)   — 가속 강한 mid-PEG
  B7: PEG<0.22 OR (NTM≥50 AND rev_g≥30 AND PEG<1.5)  — 가장 broad robust

각 paired BT + LOWO -SNDK -MU -BE -LITE -TTMI.
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
        ntm_rev_pct = (nc/n90 - 1) * 100 if n90 and n90 > 0 else 0
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, ntm_rev=ntm_rev_pct)

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


def is_mega(info, variant):
    if info is None: return False
    peg = info.get('peg')
    ntm = info.get('ntm_rev', 0)
    rg = info.get('rev_growth')

    if variant == 'V86e+':
        return peg is not None and peg < 0.22
    elif variant == 'B1':  # PEG<0.22 OR (NTM≥50 AND rev_g≥40)
        if peg is not None and peg < 0.22:
            return True
        if ntm >= 50 and rg is not None and rg >= 0.40:
            return True
        return False
    elif variant == 'B2':  # PEG<0.50
        return peg is not None and peg < 0.50
    elif variant == 'B3':  # PEG<1.0
        return peg is not None and peg < 1.0
    elif variant == 'B4':  # NTM≥50 only
        return ntm >= 50
    elif variant == 'B5':  # NTM≥30 only
        return ntm >= 30
    elif variant == 'B6':  # PEG<0.22 OR (NTM≥80 AND PEG<1.0)
        if peg is not None and peg < 0.22:
            return True
        if ntm >= 80 and peg is not None and peg < 1.0:
            return True
        return False
    elif variant == 'B7':  # PEG<0.22 OR (NTM≥50 AND rev_g≥30 AND PEG<1.5)
        if peg is not None and peg < 0.22:
            return True
        if ntm >= 50 and rg is not None and rg >= 0.30 and peg is not None and peg < 1.5:
            return True
        return False
    return False


def sim(variant='V86e+', exclude=(), start=0):
    held = {}
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
    n_buys = 0
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
                if info.get('min_seg', 0) < -2:
                    del held[tk]; continue
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info, variant):
                    continue
                del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > 3:
                    continue
                if tk in held or tk in exclude:
                    continue
                if info.get('min_seg', 0) < 0:
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
                        held[tk] = (d, dd[tk]['price'], w[si]); n_buys += 1
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0); n_buys += 1
        prev = dict(held)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100, n_buys=n_buys)


# baseline no mega
def sim_nomega(exclude=(), start=0):
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i - 1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn / pp - 1)
            val *= (1 + ret); peak = max(peak, val); mdd = max(mdd, (peak - val) / peak)
        dd = data[d]
        for tk in list(held):
            info = dd.get(tk)
            if info is not None and info.get('min_seg', 0) < -2: del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10: del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > 3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price'] / info['high30'] - 1 < -0.25: continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2 - len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0: held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick: held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return dict(cum=(val - 1) * 100)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(range(len(elig)), SAMPLES))


def run_base(exclude=()):
    savg = []
    for ch in seeds:
        sr = []
        for s in ch: sr.append(sim_nomega(exclude=exclude, start=s)['cum'])
        savg.append(sum(sr)/len(sr))
    return savg


def run(variant, exclude=()):
    cums = []; buys = []; savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
            cums.append(r['cum']); buys.append(r['n_buys']); sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums, savg, buys


print('=' * 100)
print('V96: Broad 메가 시그니처 — SNDK/MU/BE/LITE/TTMI 모두 포함 시도')
print('=' * 100)

base_savg = run_base()
print(f'\nbaseline avg: {sum(base_savg)/len(base_savg):+.1f}%')
base_ex_mss = run_base(exclude=('MU', 'SNDK'))
base_ex_all = run_base(exclude=('MU', 'SNDK', 'BE', 'LITE', 'TTMI'))

VARIANTS = [
    ('V86e+', 'PEG<0.22 (현재)'),
    ('B1', 'PEG<0.22 OR (NTM≥50 AND rev_g≥40)'),
    ('B2', 'PEG<0.50 (broad PEG)'),
    ('B3', 'PEG<1.0 (very broad)'),
    ('B4', 'NTM≥50 only'),
    ('B5', 'NTM≥30 only'),
    ('B6', 'PEG<0.22 OR (NTM≥80 AND PEG<1.0)'),
    ('B7', 'PEG<0.22 OR (NTM≥50 AND rev_g≥30 AND PEG<1.5)'),
]

print(f'\n{"v":<7}{"desc":<48}{"lift":>10}{"wins":>10}{"buys":>8}{"LOWO -MS":>14}{"LOWO -all5":>14}')
print('-' * 110)

for vid, desc in VARIANTS:
    cums, savg, buys = run(vid)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    avg_buys = statistics.mean(buys)
    # LOWO -MU-SNDK
    _, n_ex, _ = run(vid, exclude=('MU', 'SNDK'))
    lifts_ms = [y - x for x, y in zip(base_ex_mss, n_ex)]
    al_ms = sum(lifts_ms)/len(lifts_ms); w_ms = sum(1 for l in lifts_ms if l > 0)
    # LOWO -MU-SNDK-BE-LITE-TTMI
    _, n_ex2, _ = run(vid, exclude=('MU', 'SNDK', 'BE', 'LITE', 'TTMI'))
    lifts_all = [y - x for x, y in zip(base_ex_all, n_ex2)]
    al_all = sum(lifts_all)/len(lifts_all); w_all = sum(1 for l in lifts_all if l > 0)
    mk = ' ★' if vid == 'V86e+' else '  '
    print(f'{mk}{vid:<5}{desc:<48}{avg:>+8.1f}p{wins:>7}/100{avg_buys:>7.1f}{al_ms:>+8.1f}p({w_ms:>2}){al_all:>+8.1f}p({w_all:>2})')
    sys.stdout.flush()

# V86e+ vs broad direct
print('\n[V86e+ vs broad variants direct paired]')
_, m1, _ = run('V86e+')
for vid in ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7']:
    _, n, _ = run(vid)
    lifts = [y - x for x, y in zip(m1, n)]
    al = sum(lifts)/len(lifts); w = sum(1 for l in lifts if l > 0)
    print(f'  {vid} - V86e+: {al:+.1f}p ({w}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
