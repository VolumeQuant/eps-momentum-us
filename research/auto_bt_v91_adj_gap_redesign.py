# -*- coding: utf-8 -*-
"""V91 자율주행 — 시스템 본질 재설계 후보 C (adj_gap에 NTM_rev 통합)

전문가 권고 (V87-V89 후속 deep analysis):
"후보 C: adj_gap_new = (fwd_pe_chg - α × NTM_revision_rate) × eps_quality
유일하게 본질적으로 새로운 정의. BT 가치 있음."

목표: SNDK adj_gap +17.385 (비쌈) 같은 신호를 EPS 폭발 종목에서 자연 음수로.

시도 grid:
  α ∈ {0.5, 1.0, 2.0, 5.0, 10.0} — NTM_rev 차감 강도
  carryover {ON (V86e), OFF}
  = 10 cells

각 paired BT + LOWO + 부분기간.
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
        ntm_rev_pct = (nc/n90 - 1) * 100 if n90 and n90 > 0 else 0
        data[d][tk] = dict(p2_orig=r[1], cr_orig=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, adj_gap_orig=r[12],
                           ntm_rev_pct=ntm_rev_pct)

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


def is_mega(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.22


def adjusted_rank(d, alpha):
    """후보 C: adj_gap_new = adj_gap_orig - α × NTM_rev_decimal × eps_quality_approx
    NTM_rev_pct는 percentage, /100 = decimal
    """
    dd = data[d]
    cands = []
    for tk, info in dd.items():
        if info.get('adj_gap_orig') is None:
            continue
        ag = info['adj_gap_orig']
        ntm_rev = info.get('ntm_rev_pct') or 0
        # 후보 C: NTM_rev 차감
        ag_new = ag - alpha * (ntm_rev / 100)
        cands.append((tk, ag_new))
    cands.sort(key=lambda x: x[1])
    return {tk: i + 1 for i, (tk, _) in enumerate(cands)}


def sim(use_mega=False, alpha=0, exclude=(), start=0, new_rank_cache=None):
    if new_rank_cache is None:
        new_rank_cache = {}
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

        # ranking 계산 (alpha > 0이면 후보 C 적용)
        if alpha > 0:
            if new_rank_cache.get(d) is None:
                new_rank_cache[d] = adjusted_rank(d, alpha)
            rank_map = new_rank_cache[d]
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
                if use_mega and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]
                    continue
            p2 = get_p2(tk, info) if info else None
            if info is None or p2 is None or p2 > 10:
                if use_mega and is_mega(info):
                    continue
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


def run(use_mega, alpha, exclude=()):
    cache = {}
    cums = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(use_mega, alpha, exclude=exclude, start=s, new_rank_cache=cache)
            cums.append(r['cum'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, savg


print('=' * 100)
print('V91 자율주행: 후보 C — adj_gap에 NTM_rev 차감')
print('=' * 100)

# baseline (no mega, no alpha)
_, base_savg = run(False, 0)
print(f'\nbaseline avg: {sum(base_savg)/len(base_savg):+.1f}%')

# V86e+ (current production = mega ON, alpha=0)
_, v86e = run(True, 0)
print(f'V86e+ (current) avg: {sum(v86e)/len(v86e):+.1f}%')

print(f'\n[grid: α × carryover]')
print(f'{"α":>6}{"carry":>8}{"avg lift":>11}{"wins":>10}{"LOWO -MU-SNDK":>20}')
print('-' * 55)

VARIANTS = []
for alpha in [0.5, 1.0, 2.0, 5.0, 10.0]:
    for carry in [True, False]:
        VARIANTS.append((alpha, carry))

# baseline (mega OFF, alpha=0)도 비교 baseline
for alpha, carry in VARIANTS:
    cums, savg = run(carry, alpha)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    # LOWO
    _, b_ex = run(False, 0, exclude=('MU', 'SNDK'))
    _, n_ex = run(carry, alpha, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    lowo_lift = sum(lifts_lowo) / len(lifts_lowo)
    lowo_wins = sum(1 for l in lifts_lowo if l > 0)
    carry_s = 'V86e' if carry else 'OFF'
    print(f'{alpha:>6.1f}{carry_s:>8}{avg_lift:>+10.1f}p{wins:>7}/100{lowo_lift:>+10.1f}p({lowo_wins:>3}/100)')
    sys.stdout.flush()

# V86e+ vs V91 직접 비교 (carry ON)
print('\n[V86e+ (alpha=0) vs V91 (alpha 다양) direct paired]')
for alpha in [0.5, 1.0, 2.0, 5.0, 10.0]:
    _, n = run(True, alpha)
    lifts = [y - x for x, y in zip(v86e, n)]
    al = sum(lifts) / len(lifts)
    w = sum(1 for l in lifts if l > 0)
    print(f'  V91(α={alpha}, V86e ON) - V86e+: {al:+.1f}p ({w}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
