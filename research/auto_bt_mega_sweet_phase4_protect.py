# -*- coding: utf-8 -*-
"""메가홀드 sweet spot — Phase 4: 보호 강도 (rank cutoff)

현재 v86: 메가홀드 종목은 rank 무한 (순위 밖이어도 보호).
이번에 검증: rank 어디까지 보호하는 게 sweet spot인가?

variants:
  F0: baseline
  F1: 메가 무한 보호 (현재)
  F2: 메가 rank≤15까지만 보호
  F3: 메가 rank≤20까지만 보호
  F4: 메가 rank≤30까지만 보호
  F5: 메가 rank≤50까지만 보호

기준 시그니처: PEG<0.20 (Phase 2 sweet spot)
exit: min_seg<-2 + rev_growth<0.15 (Phase 3 best E5)
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


def is_mega(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.20


def sim(variant='F0', exclude=(), start=0):
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
            # 공통 매도 트리거: min_seg<-2 + rev_growth<0.15 (Phase 3 best E5)
            if info is not None:
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.15:
                    del held[tk]
                    continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                # 메가홀드 적용
                if variant == 'F0':
                    del held[tk]
                elif variant == 'F1' and is_mega(info):
                    continue
                elif variant == 'F2' and is_mega(info) and p2 is not None and p2 <= 15:
                    continue
                elif variant == 'F3' and is_mega(info) and p2 is not None and p2 <= 20:
                    continue
                elif variant == 'F4' and is_mega(info) and p2 is not None and p2 <= 30:
                    continue
                elif variant == 'F5' and is_mega(info) and p2 is not None and p2 <= 50:
                    continue
                else:
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


def run(variant, exclude=()):
    cums = []
    mdds = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg


print('=' * 100)
print('Phase 4: 메가홀드 보호 강도 (rank cutoff)')
print('  기준 시그니처: PEG<0.20')
print('  공통 exit: min_seg<-2 OR rev_growth<0.15 (Phase 3 best)')
print('=' * 100)

VARIANTS = [
    ('F0', 'baseline (메가 없음)'),
    ('F1', '메가 무한 보호 (현재)'),
    ('F2', '메가 rank≤15 보호'),
    ('F3', '메가 rank≤20 보호'),
    ('F4', '메가 rank≤30 보호'),
    ('F5', '메가 rank≤50 보호'),
]

_, _, base_savg = run('F0')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

print(f'\n{"variant":<6}{"desc":<32}{"avg":>9}{"med":>9}{"MDD":>9}{"lift":>9}{"wins":>9}')
print('-' * 80)

results = {}
for vid, desc in VARIANTS:
    cums, mdds, savg = run(vid)
    avg = statistics.mean(cums)
    med = statistics.median(cums)
    mdd_med = statistics.median(mdds)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    results[vid] = (avg, mdd_med, avg_lift, wins)
    mk = ' ★' if vid == 'F0' else '  '
    ls = '' if vid == 'F0' else f'{avg_lift:>+7.1f}p{wins:>6}/100'
    print(f'{mk}{vid:<4}{desc:<32}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{ls}')
    sys.stdout.flush()

# LOWO for viable
viable = [vid for vid, (_, _, lift, wins) in results.items() if lift > 0 and wins >= 80 and vid != 'F0']
print(f'\n[LOWO 견고성 (viable: {len(viable)})]')
print(f'{"variant":<6}{"전체":>11}{"-MU":>13}{"-SNDK":>13}{"-MU-SNDK":>13}')
print('-' * 60)

for vid in viable:
    row = f'{vid:<6}'
    _, _, b_all = run('F0')
    _, _, n_all = run(vid)
    lift_all = sum(y - x for x, y in zip(b_all, n_all)) / len(b_all)
    wins_all = sum(1 for l in [y - x for x, y in zip(b_all, n_all)] if l > 0)
    row += f'{lift_all:>+8.1f}p({wins_all:>2})'
    for exn, ex in [('-MU', ('MU',)), ('-SNDK', ('SNDK',)), ('-MU-SNDK', ('MU', 'SNDK'))]:
        _, _, b = run('F0', exclude=ex)
        _, _, n = run(vid, exclude=ex)
        lifts = [y - x for x, y in zip(b, n)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+9.1f}p({wins:>2})'
    print(row)
    sys.stdout.flush()

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
