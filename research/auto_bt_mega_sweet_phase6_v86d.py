# -*- coding: utf-8 -*-
"""메가홀드 sweet spot — Phase 6: V86d 최종 sweet spot 확정 BT

Phase 5에서 발견: rev_exit 0.20 = +86.0p (rev_exit 0.15 +82.5p보다 +3.5p).
이번에 검증: V86d = PEG<0.20 + min_seg<-2 + rev_growth<20% 매도

100×3 paired + LOWO + 인접성 + 부분기간 + earnings cycle stratification.
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
        ntm_rev = (nc / n90 - 1) * 100 if n90 and n90 > 0 else None
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           ntm_rev=ntm_rev, peg=peg, rev_growth=rg)

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


def sim(variant='baseline', exclude=(), start=0, ntm_thr=60, peg_thr=0.20, rev_exit=0.20):
    held = {}
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
    n_mega_extends = 0  # 메가홀드 발동 횟수
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
                if variant in ('V86c', 'V86d') and info.get('rev_growth') is not None and info['rev_growth'] < rev_exit:
                    del held[tk]
                    continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                is_mega = False
                if variant == 'V86':
                    is_mega = (info is not None and info.get('ntm_rev') is not None
                               and info.get('peg') is not None
                               and info['ntm_rev'] >= ntm_thr and info['peg'] < peg_thr)
                elif variant in ('V86b', 'V86c', 'V86d'):
                    is_mega = (info is not None and info.get('peg') is not None
                               and info['peg'] < peg_thr)
                if is_mega:
                    n_mega_extends += 1
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
    return dict(cum=(val - 1) * 100, mdd=mdd * 100, n_mega=n_mega_extends)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(variant, exclude=(), peg_thr=0.20, rev_exit=0.20):
    cums = []
    mdds = []
    savg = []
    n_megas = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s, peg_thr=peg_thr, rev_exit=rev_exit)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            n_megas.append(r['n_mega'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg, n_megas


print('=' * 100)
print('Phase 6: V86d 최종 sweet spot 확정 BT')
print('  V86d: PEG<0.20 메가홀드 + min_seg<-2 매도 + rev_growth<20% 매도')
print('=' * 100)

_, _, base_savg, _ = run('baseline')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

# 직접 비교: V86 / V86b / V86c (rev 15%) / V86d (rev 20%)
VARIANTS = [
    ('V86', '회사PC (NTM≥60 AND PEG<0.20)', 0.20, 0.15),
    ('V86b', 'PEG<0.20 only', 0.20, 0.15),
    ('V86c', 'PEG<0.20 + rev_growth<15% 매도', 0.20, 0.15),
    ('V86d', 'PEG<0.20 + rev_growth<20% 매도 ⭐', 0.20, 0.20),
]

print(f'\n{"variant":<6}{"desc":<48}{"avg":>9}{"med":>9}{"MDD":>9}{"메가홀드":>9}{"lift":>9}{"wins":>9}')
print('-' * 105)

results = {}
for vid, desc, pt, re in VARIANTS:
    cums, mdds, savg, n_megas = run(vid, peg_thr=pt, rev_exit=re)
    avg = statistics.mean(cums)
    med = statistics.median(cums)
    mdd_med = statistics.median(mdds)
    n_mega_avg = statistics.mean(n_megas)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    results[vid] = (avg, mdd_med, avg_lift, wins, savg)
    ls = f'{avg_lift:>+7.1f}p{wins:>6}/100'
    print(f'  {vid:<4}{desc:<48}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{n_mega_avg:>8.1f}{ls}')
    sys.stdout.flush()

# Pairwise direct
print('\n[Pairwise paired comparison]')
for a, b in [('V86', 'V86d'), ('V86c', 'V86d'), ('V86b', 'V86d')]:
    _, _, sa, _ = run(a, peg_thr=0.20, rev_exit=0.15 if a == 'V86c' else 0.20)
    _, _, sb, _ = run(b, peg_thr=0.20, rev_exit=0.20)
    lifts = [y - x for x, y in zip(sa, sb)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    print(f'  {b} - {a}: avg lift {avg_lift:+.1f}p, wins {wins}/100')

# LOWO 종합
print('\n[LOWO 견고성 (전체/4 시나리오)]')
print(f'{"variant":<6}{"전체":>11}{"-MU":>13}{"-SNDK":>13}{"-MU-SNDK":>14}{"-MU-SNDK-MCHP":>16}')
print('-' * 78)

for vid, _, pt, re in VARIANTS:
    row = f'{vid:<6}'
    for exn, ex in [('전체', ()), ('-MU', ('MU',)), ('-SNDK', ('SNDK',)),
                     ('-MU-SNDK', ('MU', 'SNDK')), ('-MU-SNDK-MCHP', ('MU', 'SNDK', 'MCHP'))]:
        _, _, b, _ = run('baseline', exclude=ex)
        _, _, n, _ = run(vid, exclude=ex, peg_thr=pt, rev_exit=re)
        lifts = [y - x for x, y in zip(b, n)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        if exn == '전체':
            row += f'{avg_lift:>+8.1f}p({wins:>2})'
        else:
            row += f'{avg_lift:>+9.1f}p({wins:>2})'
    print(row)
    sys.stdout.flush()

# V86d 인접성 정밀 (PEG 0.18~0.22, rev_exit 0.15~0.25)
print('\n[V86d 인접성 정밀]')
print(f'{"PEG":>6}{"rev_exit":>10}{"lift":>10}{"wins":>10}')
print('-' * 36)
best = (None, None, -999)
for peg_t in [0.18, 0.20, 0.22, 0.25]:
    for re_t in [0.10, 0.15, 0.20, 0.25, 0.30]:
        _, _, savg, _ = run('V86d', peg_thr=peg_t, rev_exit=re_t)
        lifts = [b - a for a, b in zip(base_savg, savg)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        if avg_lift > best[2] and wins >= 95:
            best = (peg_t, re_t, avg_lift)
        print(f'{peg_t:>6.2f}{re_t:>10.2f}{avg_lift:>+9.1f}p{wins:>7}/100')

print(f'\n인접성 best: PEG={best[0]} rev_exit={best[1]} lift={best[2]:+.1f}p')

# 부분기간 stratification
n_dates = len(dates)
front_starts = [ch for ch in seeds if all(s < n_dates // 2 for s in ch)]
back_starts = [ch for ch in seeds if all(s >= n_dates // 2 - 30 for s in ch)]
print(f'\n[부분기간 — 전반 {len(front_starts)} / 후반 {len(back_starts)}]')

def run_filtered(variant, sf, exclude=(), peg_thr=0.20, rev_exit=0.20):
    savg = []
    for ch in sf:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s, peg_thr=peg_thr, rev_exit=rev_exit)
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return savg

print(f'{"variant":<6}{"전반 lift":>11}{"전반 wins":>11}{"후반 lift":>11}{"후반 wins":>11}')
print('-' * 55)
for vid, _, pt, re in VARIANTS:
    row = f'{vid:<6}'
    if front_starts:
        bf = run_filtered('baseline', front_starts)
        nf = run_filtered(vid, front_starts, peg_thr=pt, rev_exit=re)
        lifts = [y - x for x, y in zip(bf, nf)]
        avg_lift = sum(lifts) / len(lifts) if lifts else 0
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+8.1f}p{wins:>8}/{len(lifts)}'
    else:
        row += f'{"N/A":>11}{"N/A":>11}'
    if back_starts:
        bb = run_filtered('baseline', back_starts)
        nb = run_filtered(vid, back_starts, peg_thr=pt, rev_exit=re)
        lifts = [y - x for x, y in zip(bb, nb)]
        avg_lift = sum(lifts) / len(lifts) if lifts else 0
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+8.1f}p{wins:>8}/{len(lifts)}'
    else:
        row += f'{"N/A":>11}{"N/A":>11}'
    print(row)

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
