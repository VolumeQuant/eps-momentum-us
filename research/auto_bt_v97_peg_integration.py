# -*- coding: utf-8 -*-
"""V97 — PEG 조건을 시스템 계산식 자체에 통합 (사용자 직접 요청)

사용자: "PEG<0.22 조건을 시스템 계산식에 넣으면 안 돼?"

V91 (NTM_rev 차감)은 실패. 다른 통합 방식 시도:

A. adj_gap × peg_factor (메가는 신호 약화, 일반은 그대로)
B. adj_gap + γ × (PEG - 0.5) (PEG additive penalty)
C. eps_quality × peg_boost (메가만 eps_q 증폭)
D. adj_gap + γ × log(PEG) (PEG log 항)
E. composite rank 자체에 PEG 항 추가
F. adj_gap × max(PEG, 0.22) / 0.5 (smooth PEG factor)
"""
import sys, sqlite3, random, statistics, time, math
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


def is_mega_carryover(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.22


def modified_adj_gap(info, variant, param):
    """variant 별 adj_gap 수정 후 반환"""
    if info is None or info.get('adj_gap_orig') is None:
        return None
    ag = info['adj_gap_orig']
    peg = info.get('peg')

    if variant == 'A':  # adj_gap × peg_factor (메가 신호 약화)
        if peg is not None and peg < 0.22:
            return ag * param  # param e.g. 0.3
        return ag
    elif variant == 'B':  # adj_gap + γ × (PEG - 0.5) (PEG additive)
        if peg is not None:
            return ag + param * (peg - 0.5)
        return ag
    elif variant == 'C':  # eps_quality boost 효과: 메가 adj_gap × 1.5
        if peg is not None and peg < 0.22:
            return ag * param  # param e.g. 1.5
        return ag
    elif variant == 'D':  # adj_gap + γ × log(PEG)
        if peg is not None and peg > 0:
            return ag + param * math.log(peg)
        return ag
    elif variant == 'E':  # adj_gap × smooth peg factor (보편 PEG-aware)
        if peg is not None:
            # PEG 0.04 → 0.08, PEG 1.0 → 2.0
            factor = peg / 0.5
            return ag * factor
        return ag
    elif variant == 'F':  # 메가 adj_gap > 0 cap 0 (over-penalty 제거만)
        if peg is not None and peg < 0.22 and ag > 0:
            return 0
        return ag
    return ag


def get_rank_map(d, variant, param):
    """variant 별 part2_rank 재산정"""
    dd = data[d]
    cands = []
    for tk, info in dd.items():
        new_ag = modified_adj_gap(info, variant, param)
        if new_ag is None:
            continue
        cands.append((tk, new_ag))
    cands.sort(key=lambda x: x[1])
    return {tk: i + 1 for i, (tk, _) in enumerate(cands)}


def sim(variant, param, mega_carry=True, exclude=(), start=0, cache=None):
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
            dp = dates[i - 1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn / pp - 1)
            val *= (1 + ret); peak = max(peak, val); mdd = max(mdd, (peak - val) / peak)
        dd = data[d]

        if variant != 'baseline':
            if cache.get(d) is None:
                cache[d] = get_rank_map(d, variant, param)
            rank_map = cache[d]
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


def run(variant, param, mega_carry=True, exclude=()):
    cache = {}
    cums = []; savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, param, mega_carry, exclude=exclude, start=s, cache=cache)
            cums.append(r['cum']); sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums, savg


print('=' * 100)
print('V97: PEG 조건 시스템 계산식 통합 (사용자 직접 요청)')
print('=' * 100)

# baseline = no mega
_, base_savg = run('baseline', 0, mega_carry=False)
print(f'\nbaseline avg: {sum(base_savg)/len(base_savg):+.1f}%')

# V86e+ current
_, v86e_savg = run('baseline', 0, mega_carry=True)
v86e_lift = sum(b-a for a, b in zip(base_savg, v86e_savg))/len(base_savg)
print(f'V86e+ lift: {v86e_lift:+.1f}p\n')

VARIANTS = [
    ('A', '메가 adj_gap × 0.3 (신호 약화)', 0.3),
    ('A', '메가 adj_gap × 0.5', 0.5),
    ('A', '메가 adj_gap × 0.0 (무효화)', 0.0),
    ('A', '메가 adj_gap × -0.5 (반대 부호)', -0.5),
    ('A', '메가 adj_gap × -1.0 (완전 반대)', -1.0),
    ('B', 'adj_gap + 1.0 × (PEG-0.5)', 1.0),
    ('B', 'adj_gap + 5.0 × (PEG-0.5)', 5.0),
    ('B', 'adj_gap + 10.0 × (PEG-0.5)', 10.0),
    ('B', 'adj_gap + 20.0 × (PEG-0.5)', 20.0),
    ('C', '메가 adj_gap × 1.5 (boost)', 1.5),
    ('C', '메가 adj_gap × 2.0 (강 boost)', 2.0),
    ('D', 'adj_gap + 1.0 × log(PEG)', 1.0),
    ('D', 'adj_gap + 3.0 × log(PEG)', 3.0),
    ('D', 'adj_gap + 5.0 × log(PEG)', 5.0),
    ('E', 'adj_gap × (PEG/0.5) — universal', 0),
    ('F', '메가 adj_gap > 0 → cap 0 (over-penalty 제거)', 0),
]

print(f'{"v":<4}{"desc":<48}{"lift":>10}{"vs V86e+":>12}{"wins":>9}{"LOWO -MS":>14}')
print('-' * 100)

base_ex = run('baseline', 0, mega_carry=False, exclude=('MU', 'SNDK'))[1]

for vid, desc, param in VARIANTS:
    cums, savg = run(vid, param)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    # vs V86e+ direct
    lifts_v86e = [n - v for v, n in zip(v86e_savg, savg)]
    al_v86e = sum(lifts_v86e)/len(lifts_v86e)
    # LOWO
    _, n_ex = run(vid, param, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(base_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    print(f'  {vid:<2}{desc:<48}{avg:>+8.1f}p{al_v86e:>+10.1f}p{wins:>6}/100{al_lowo:>+10.1f}p({w_lowo:>2})')
    sys.stdout.flush()

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
