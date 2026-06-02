# -*- coding: utf-8 -*-
"""V99 — 시스템 산식 본질 분석 기반 PEG 통합 (추론 기반)

이전 V87~V98 시도 = brute force grid search. 시스템 산식 본질 이해 X.

V99 = 산식 layer 깊이 분석 + 매출성장률·커버리지 통합 패턴 추론 기반:

V99a: PEG percentile (universe 상대값) — 시장 환경 변화 robust
V99b: direction 항에 PEG 통합 — 미사용 layer 활용
V99c: min_seg + PEG modifier (entry filter 완화) — 매출 필터 패턴
V99d: multi-objective ranking (w_gap_rank + peg_rank 가중)

각 추론 근거:
- V99a: PEG 절대값 0.22는 sensitive. percentile은 universe-aware
- V99b: direction은 score 방향 보정. PEG도 valuation direction. 자연
- V99c: rev_growth ≥ 10% level filter처럼 PEG도 filter modifier
- V99d: 시스템 본질 = "약한 신호 차단" 단일 차원 (v80.8). PEG는 다른 차원 → 두 차원 결합 필요
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
peg_by_date = defaultdict(list)
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
        if peg is not None and peg > 0:
            peg_by_date[d].append((tk, peg))

# precompute PEG percentile per date
peg_percentile = {}
for d, lst in peg_by_date.items():
    lst.sort(key=lambda x: x[1])
    n = len(lst)
    pct_map = {}
    for i, (tk, peg) in enumerate(lst):
        pct_map[tk] = (i + 1) / n  # bottom 5% = pct ≤ 0.05
    peg_percentile[d] = pct_map

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


def is_mega_carryover(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


# V99a: percentile based
def is_mega_pct(tk, d, pct_thr=0.05):
    pm = peg_percentile.get(d, {})
    pct = pm.get(tk, 1.0)
    return pct <= pct_thr


# V99d: multi-objective ranking
def get_rank_v99d(d, alpha):
    """ranking = α × w_gap_rank + (1-α) × peg_rank"""
    dd = data[d]
    pm = peg_percentile.get(d, {})
    # w_gap rank = part2_rank_orig
    # peg rank = percentile rank (낮을수록 1위)
    cands = []
    for tk, info in dd.items():
        p2 = info.get('p2_orig')
        if p2 is None: continue
        pct = pm.get(tk, 1.0)
        peg_rank = pct * 500  # 0~1 → 0~500 (rank scale)
        combined = alpha * p2 + (1 - alpha) * peg_rank
        cands.append((tk, combined))
    cands.sort(key=lambda x: x[1])
    return {tk: i + 1 for i, (tk, _) in enumerate(cands)}


# V99b: direction에 PEG 통합 → adj_gap 영향
def modified_adj_gap_v99b(info, alpha):
    """adj_gap_new = adj_gap_orig × (1 + α × peg_dir)
    peg_dir = clamp((0.5 - PEG)/0.5 × 0.3, ±0.3)  # direction range와 동일
    """
    if info is None or info.get('adj_gap_orig') is None:
        return None
    ag = info['adj_gap_orig']
    peg = info.get('peg')
    if peg is None:
        return ag
    peg_dir = max(-0.3, min(0.3, (0.5 - peg) / 0.5 * 0.3))
    # ag가 음수면 양수 보너스로 더 음수, ag가 양수면 양수로 cap
    # asymmetric: 음수일 때만 증폭
    if ag < 0:
        return ag * (1 + alpha * peg_dir)
    return ag


def get_rank_v99b(d, alpha):
    dd = data[d]
    cands = []
    for tk, info in dd.items():
        ag = modified_adj_gap_v99b(info, alpha)
        if ag is None: continue
        cands.append((tk, ag))
    cands.sort(key=lambda x: x[1])
    return {tk: i + 1 for i, (tk, _) in enumerate(cands)}


def sim(variant, param=0.05, mega_carry=True, exclude=(), start=0, cache=None):
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

        if variant == 'V99b':
            ck = (variant, param, d)
            if cache.get(ck) is None:
                cache[ck] = get_rank_v99b(d, param)
            rank_map = cache[ck]
        elif variant == 'V99d':
            ck = (variant, param, d)
            if cache.get(ck) is None:
                cache[ck] = get_rank_v99d(d, param)
            rank_map = cache[ck]
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
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if mega_carry and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = get_p2(tk, info) if info else None
            if info is None or p2 is None or p2 > 10:
                # V99a: percentile 기반 carryover
                if variant == 'V99a' and is_mega_pct(tk, d, param):
                    continue
                elif mega_carry and is_mega_carryover(info):
                    continue
                del held[tk]

        # 매수
        if len(held) < 2:
            # V99c: min_seg + PEG modifier
            cands = []
            for tk, info in dd.items():
                p2 = get_p2(tk, info)
                if p2 is None or p2 > 3:
                    # V99c: 메가는 진입 풀 확장 (rank ≤ 5)
                    if variant == 'V99c' and is_mega_carryover(info) and p2 is not None and p2 <= 5:
                        pass  # 풀에 포함
                    else:
                        continue
                if tk in held or tk in exclude: continue
                # min_seg filter
                ms = info.get('min_seg', 0)
                if variant == 'V99c' and is_mega_carryover(info):
                    # 메가는 min_seg threshold 완화
                    peg = info.get('peg', 0.5)
                    peg_modifier = 0.5 * max(0, (0.5 - peg))
                    if ms + peg_modifier < 0: continue
                else:
                    if ms < 0: continue
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

def run(variant, param=0.05, mega_carry=True, exclude=()):
    cums = []; savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, param, mega_carry, exclude=exclude, start=s, cache=cache_g)
            cums.append(r['cum']); sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums, savg


print('=' * 100)
print('V99 — 시스템 산식 본질 분석 기반 PEG 통합 (추론 기반)')
print('=' * 100)

_, base_savg = run('baseline', mega_carry=False)
print(f'\nbaseline avg: {sum(base_savg)/len(base_savg):+.1f}%')
_, v86e_savg = run('baseline', mega_carry=True)
v86e_lift = sum(b-a for a, b in zip(base_savg, v86e_savg))/len(base_savg)
print(f'V86e+ lift: {v86e_lift:+.1f}p\n')

VARIANTS = [
    # V99a: PEG percentile (universe 상대값)
    ('V99a', 'PEG percentile bottom 1% carryover', 0.01),
    ('V99a', 'PEG percentile bottom 3% carryover', 0.03),
    ('V99a', 'PEG percentile bottom 5% carryover', 0.05),
    ('V99a', 'PEG percentile bottom 10% carryover', 0.10),
    # V99b: direction에 PEG 통합 (asymmetric)
    ('V99b', 'direction PEG α=0.5 (asymmetric)', 0.5),
    ('V99b', 'direction PEG α=1.0', 1.0),
    ('V99b', 'direction PEG α=2.0', 2.0),
    # V99c: min_seg + PEG modifier (entry filter 완화)
    ('V99c', 'min_seg + PEG modifier (entry 완화)', 0),
    # V99d: multi-objective ranking
    ('V99d', 'multi-rank α=0.9 (w_gap 90%, PEG 10%)', 0.9),
    ('V99d', 'multi-rank α=0.7 (70%/30%)', 0.7),
    ('V99d', 'multi-rank α=0.5 (50%/50%)', 0.5),
    ('V99d', 'multi-rank α=0.3 (30%/70%)', 0.3),
]

print(f'{"v":<7}{"desc":<48}{"lift":>10}{"vs V86e+":>12}{"wins":>9}{"LOWO -MS":>14}')
print('-' * 105)

base_ex = run('baseline', mega_carry=False, exclude=('MU', 'SNDK'))[1]

for vid, desc, param in VARIANTS:
    cums, savg = run(vid, param)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    lifts_v86e = [n - v for v, n in zip(v86e_savg, savg)]
    al_v86e = sum(lifts_v86e)/len(lifts_v86e)
    _, n_ex = run(vid, param, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(base_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    print(f'  {vid:<5}{desc:<48}{avg:>+8.1f}p{al_v86e:>+10.1f}p{wins:>6}/100{al_lowo:>+10.1f}p({w_lowo:>2})')
    sys.stdout.flush()

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
