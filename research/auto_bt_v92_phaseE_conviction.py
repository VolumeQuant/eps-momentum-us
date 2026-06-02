# -*- coding: utf-8 -*-
"""V92 Phase E — conviction asymmetry 실험 (사용자 의견 반영)

사용자 의견: "1번 conviction 비대칭은 안좋은 종목 거르려고 의도. 하지만 실험해봐."

전문가가 발견한 문제: SNDK NTM +147% → conviction 2.77배 → adj_gap +17 × 2.77 = +47.
                  슈퍼사이클 종목에 over-penalty.

실험 (시뮬 approximation):
  E1: 메가 종목 (PEG<0.22) adj_gap > 0이면 → cap 0 (over-penalty 제거)
  E2: 메가 + adj_gap > 0 → -1 강제 (1위 후보)
  E3: 메가 + adj_gap > 0 → -3 강제 (강한 매수 신호)
  E4: 모든 종목 adj_gap > 0이면 cap 5 (보편 asymmetry 완화)

each paired BT + LOWO + 부분기간.
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
        '''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth,adj_gap FROM ntm_screening WHERE date=?''',
        (d,)):
        tk = r[0]
        nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a - b) / abs(b) * 100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        data[d][tk] = dict(p2_orig=r[1], cr_orig=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0,
                           mean_seg=sum(segs)/len(segs) if segs else 0,
                           high30=r[10], peg=peg, rev_growth=rg,
                           adj_gap_orig=r[12])

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i - 1, i - 2):
        if j < 0:
            return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr_orig') is None or x['cr_orig'] > 30:
            return False
    return True


def is_mega(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


def get_modified_rank(d, variant):
    """variant 별 adj_gap 수정 후 part2_rank 재계산."""
    if variant == 'E0':
        return None
    dd = data[d]
    cands = []
    for tk, info in dd.items():
        if info.get('adj_gap_orig') is None:
            continue
        ag = info['adj_gap_orig']
        mega = is_mega(info)

        if variant == 'E1' and mega and ag > 0:
            ag = 0  # cap
        elif variant == 'E2' and mega and ag > 0:
            ag = -1
        elif variant == 'E3' and mega and ag > 0:
            ag = -3
        elif variant == 'E4' and ag > 0:
            ag = min(ag, 5)  # 모든 종목 + 5 cap

        cands.append((tk, ag))
    cands.sort(key=lambda x: x[1])
    return {tk: i + 1 for i, (tk, _) in enumerate(cands)}


def sim(variant='E0', exclude=(), start=0, cache=None):
    if cache is None:
        cache = {}
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

        if variant != 'E0':
            if cache.get(d) is None:
                cache[d] = get_modified_rank(d, variant)
            rank_map = cache[d]
        else:
            rank_map = None

        def get_p2(tk, info):
            if rank_map is not None:
                return rank_map.get(tk)
            return info.get('p2_orig') if info else None

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]
                    continue
            p2 = get_p2(tk, info) if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info):
                    continue  # V86e+ carryover 유지
                del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                p2 = get_p2(tk, info)
                if p2 is None or p2 > 3:
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
                cands.append((p2, info['score'], tk))
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
    cache = {}
    cums = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s, cache=cache)
            cums.append(r['cum'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, savg


print('=' * 100)
print('V92 Phase E — conviction asymmetry 실험 (사용자 의견)')
print('=' * 100)

# baseline (E0 = V86e+ 그대로)
_, base_savg = run('E0')
base_avg = sum(base_savg)/len(base_savg)
print(f'\nV86e+ (E0, baseline) avg: {base_avg:+.1f}%')

print(f'\n{"variant":<6}{"desc":<48}{"lift vs V86e+":>15}{"wins":>10}{"LOWO -MU-SNDK":>20}')
print('-' * 100)

VARIANTS = [
    ('E1', '메가 + adj_gap>0 → cap 0'),
    ('E2', '메가 + adj_gap>0 → -1 (1위 후보)'),
    ('E3', '메가 + adj_gap>0 → -3 (강한 매수)'),
    ('E4', '모든 종목 adj_gap>0 → cap 5 (보편 완화)'),
]

for vid, desc in VARIANTS:
    cums, savg = run(vid)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    _, b_ex = run('E0', exclude=('MU', 'SNDK'))
    _, n_ex = run(vid, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    print(f'  {vid:<4}{desc:<48}{avg:>+13.1f}p{wins:>7}/100{al_lowo:>+10.1f}p({w_lowo:>3}/100)')
    sys.stdout.flush()

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
