# -*- coding: utf-8 -*-
"""V98 — PEG를 conviction에 매출성장률 방식으로 녹여내기

사용자 명확: "매출성장률·커버리지가 산식에 녹여진 방식 따라"

기존 conviction 산식 (line 1728-1760):
  conviction = max(ratio, eps_floor) + rev_bonus
    ratio = rev_up30 / num_analysts (커버리지 + 합의)
    eps_floor = NTM 변화율
    rev_bonus = min(min(rev_growth, 0.5) × 0.6, 0.3) (smooth, cap 0.3)
  adj_gap_new = adj_gap × (1 + conviction)

PEG 통합 방식 (매출성장률과 동일 패턴):
  conviction_new = max(ratio, eps_floor) + rev_bonus + peg_bonus
  peg_bonus = smooth, cap 0.3 (rev_bonus와 동일 cap)

문제: conviction은 adj_gap 곱셈. SNDK adj_gap +17 + conviction 증가 → 더 over-penalty.

대안:
  A. peg_bonus symmetric conviction (기존 방식 + PEG bonus)
  B. peg_bonus asymmetric (adj_gap < 0일 때만 증폭)
  C. PEG bonus를 adj_gap 직접 가산 (-bonus, 음수 방향)
  D. PEG factor를 eps_floor에 통합 (eps_floor + peg_floor)
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
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, adj_gap_orig=r[12])

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


def peg_bonus_calc(peg, cap=0.3, peg_thr=0.22):
    """smooth PEG bonus (rev_bonus와 동일 구조)
    PEG 0 → cap, PEG peg_thr → 0
    """
    if peg is None or peg >= peg_thr:
        return 0
    # smooth linear: PEG 0 → cap, PEG threshold → 0
    return min(cap, cap * (peg_thr - peg) / peg_thr)


def modified_adj_gap_v98(info, variant, peg_thr=0.22, cap=0.3, scale=1.0):
    """variant 별 adj_gap 변형"""
    if info is None or info.get('adj_gap_orig') is None:
        return None
    ag = info['adj_gap_orig']
    peg = info.get('peg')

    pb = peg_bonus_calc(peg, cap, peg_thr)

    if variant == 'A':  # symmetric: adj_gap × (1 + peg_bonus)
        # 양수면 over-penalty 위험
        return ag * (1 + pb)
    elif variant == 'B':  # asymmetric: 음수일 때만 증폭
        if ag < 0:
            return ag * (1 + pb)
        return ag  # 양수면 그대로 (over-penalty 안함)
    elif variant == 'C':  # 가산: adj_gap - (peg_bonus × scale)
        # PEG 보너스만큼 adj_gap 더 음수로 (매수 신호 강화)
        return ag - pb * scale
    elif variant == 'D':  # asymmetric + 가산: 음수면 증폭, 양수면 cap 또는 가산
        if ag < 0:
            return ag * (1 + pb)
        else:
            # 양수도 PEG 보너스 가산 (방향 음수로)
            return ag - pb * scale
    return ag


def get_rank_map(d, variant, peg_thr, cap, scale):
    dd = data[d]
    cands = []
    for tk, info in dd.items():
        new_ag = modified_adj_gap_v98(info, variant, peg_thr, cap, scale)
        if new_ag is None:
            continue
        cands.append((tk, new_ag))
    cands.sort(key=lambda x: x[1])
    return {tk: i + 1 for i, (tk, _) in enumerate(cands)}


def is_mega_carryover(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


def sim(variant, peg_thr=0.22, cap=0.3, scale=1.0, mega_carry=True, exclude=(), start=0, cache=None):
    if cache is None:
        cache = {}
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
        if variant != 'baseline':
            ck = (variant, peg_thr, cap, scale, d)
            if cache.get(ck) is None:
                cache[ck] = get_rank_map(d, variant, peg_thr, cap, scale)
            rank_map = cache[ck]
        else:
            rank_map = None

        def get_p2(tk, info):
            if rank_map is not None:
                return rank_map.get(tk)
            return info.get('p2_orig') if info else None

        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if mega_carry and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = get_p2(tk, info) if info else None
            if info is None or p2 is None or p2 > 10:
                if mega_carry and is_mega_carryover(info):
                    continue
                del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                p2 = get_p2(tk, info)
                if p2 is None or p2 > 3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price'] / info['high30'] - 1 < -0.25: continue
                cands.append((p2, info['score'], tk))
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
    return dict(cum=(val - 1) * 100, mdd=mdd * 100)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(range(len(elig)), SAMPLES))

cache_g = {}

def run(variant, peg_thr=0.22, cap=0.3, scale=1.0, mega_carry=True, exclude=()):
    cums = []; savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, peg_thr, cap, scale, mega_carry, exclude=exclude, start=s, cache=cache_g)
            cums.append(r['cum']); sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums, savg


print('=' * 100)
print('V98: PEG conviction 통합 (매출성장률·커버리지 방식)')
print('=' * 100)

_, base_savg = run('baseline', mega_carry=False)
print(f'\nbaseline avg: {sum(base_savg)/len(base_savg):+.1f}%')
_, v86e_savg = run('baseline', mega_carry=True)
v86e_lift = sum(b-a for a, b in zip(base_savg, v86e_savg))/len(base_savg)
print(f'V86e+ lift: {v86e_lift:+.1f}p\n')

VARIANTS = [
    ('A', 'symmetric: ag × (1+peg_bonus), cap=0.3', 0.3, 1.0),
    ('A', 'symmetric: ag × (1+peg_bonus), cap=0.5', 0.5, 1.0),
    ('A', 'symmetric: ag × (1+peg_bonus), cap=1.0', 1.0, 1.0),
    ('B', 'asymmetric: ag<0면만 ag × (1+bonus), cap=0.3', 0.3, 1.0),
    ('B', 'asymmetric: cap=0.5', 0.5, 1.0),
    ('B', 'asymmetric: cap=1.0', 1.0, 1.0),
    ('B', 'asymmetric: cap=2.0', 2.0, 1.0),
    ('C', '가산: ag - peg_bonus×scale, scale=1', 0.3, 1.0),
    ('C', '가산: scale=5', 0.3, 5.0),
    ('C', '가산: scale=10', 0.3, 10.0),
    ('C', '가산: scale=20', 0.3, 20.0),
    ('D', 'asymmetric+가산: 음수면 증폭/양수면 가산, scale=5', 0.3, 5.0),
    ('D', 'asymmetric+가산: scale=10', 0.3, 10.0),
    ('D', 'asymmetric+가산: scale=20', 0.3, 20.0),
]

print(f'{"v":<4}{"desc":<55}{"lift":>10}{"vs V86e+":>12}{"wins":>9}{"LOWO -MS":>14}')
print('-' * 110)

base_ex = run('baseline', mega_carry=False, exclude=('MU', 'SNDK'))[1]

best = None
for vid, desc, cap, scale in VARIANTS:
    cums, savg = run(vid, peg_thr=0.22, cap=cap, scale=scale)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    lifts_v86e = [n - v for v, n in zip(v86e_savg, savg)]
    al_v86e = sum(lifts_v86e)/len(lifts_v86e)
    _, n_ex = run(vid, peg_thr=0.22, cap=cap, scale=scale, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(base_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    if al_v86e > 0 and (best is None or al_v86e > best[1]):
        best = (desc, al_v86e, avg)
    print(f'  {vid:<2}{desc:<55}{avg:>+8.1f}p{al_v86e:>+10.1f}p{wins:>6}/100{al_lowo:>+10.1f}p({w_lowo:>2})')
    sys.stdout.flush()

print(f'\nbest vs V86e+: {best}' if best else '\nbest: V86e+ 우월 변형 없음 (모두 음수)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
