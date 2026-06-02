# -*- coding: utf-8 -*-
"""V87 자율주행 — 메가홀드 score/rank 보너스 방식 BT

사용자 지적: V86e는 "보유 결정 layer"라 예외 처리. 일관성 X.
사용자 안: 메가 종목 score/rank에 보너스 줘서 자연스럽게 보유.

전략 변형:
  baseline: 메가홀드 없음 (V86e/v86 모두 아님)
  V86e    : 보유 결정 layer (carryover, 기준치)
  V87a    : 메가 part2_rank 강제 1 (가장 공격적)
  V87b    : 메가 part2_rank 강제 2
  V87c    : 메가 part2_rank min(orig, 3)  → 진입 대상
  V87d    : 메가 part2_rank min(orig, 5)
  V87e    : 메가 part2_rank min(orig, 10) → 매도 보호 동등
  V87f    : 메가 part2_rank min(orig, 2)  → 슬롯 점유 보장
  V87g    : composite + rank 둘 다 min(orig, 3)

각각 100×3 paired BT + LOWO + 부분기간 + 신규 매수 빈도 분석.
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
        data[d][tk] = dict(p2_orig=r[1], cr_orig=r[2], price=r[3], score=r[4] or 0,
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
        if not x or x.get('cr_orig') is None or x['cr_orig'] > 30:
            return False
    return True


def is_mega(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.20


def apply_rank_bonus(info, variant):
    """메가 종목의 part2_rank를 변형. 메가 아니면 원본."""
    p2 = info.get('p2_orig')
    if not is_mega(info):
        return p2
    if p2 is None:
        # 메가지만 part2_rank 없음 (eligible 탈락) — V87 변형으로도 매수 신호 불가
        return None
    if variant == 'V87a':  # 강제 1
        return 1
    elif variant == 'V87b':  # 강제 2
        return 2
    elif variant == 'V87c':  # min(orig, 3)
        return min(p2, 3)
    elif variant == 'V87d':  # min(orig, 5)
        return min(p2, 5)
    elif variant == 'V87e':  # min(orig, 10) — 매도 보호와 동등 효과
        return min(p2, 10)
    elif variant == 'V87f':  # min(orig, 2)
        return min(p2, 2)
    return p2


def sim(variant='baseline', exclude=(), start=0):
    """variant 별 시뮬.
    baseline: 메가홀드 없음 (rank>10 매도, carryover X)
    V86e: 보유 결정 layer (carryover, rev_growth<0.25 매도)
    V87a~f: rank 보너스 (carryover X)
    """
    held = {}  # tk -> (entry_date, entry_price, weight)
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

        # 매도 결정
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                # V86e: rev_growth<0.25 매도
                if variant == 'V86e' and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]
                    continue

            # rank 결정: variant 별
            if variant.startswith('V87'):
                p2 = apply_rank_bonus(info, variant) if info else None
            else:
                p2 = info.get('p2_orig') if info else None

            if info is None or p2 is None or p2 > 10:
                # V86e: 메가홀드 carryover
                if variant == 'V86e' and is_mega(info):
                    continue
                del held[tk]

        # 매수 결정
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                # rank: variant 별
                if variant.startswith('V87'):
                    p2 = apply_rank_bonus(info, variant)
                else:
                    p2 = info.get('p2_orig')
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
                        n_buys += 1
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
                    n_buys += 1
        prev = dict(held)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100, n_buys=n_buys)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(variant, exclude=()):
    cums = []
    mdds = []
    buys = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            buys.append(r['n_buys'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg, buys


print('=' * 100)
print('V87 자율주행: 메가홀드 score/rank 보너스 BT')
print('=' * 100)

VARIANTS = [
    ('baseline', '메가홀드 없음'),
    ('V86e', '현재 — 보유 결정 layer (carryover)'),
    ('V87a', 'rank 강제 1'),
    ('V87b', 'rank 강제 2'),
    ('V87f', 'rank min(orig, 2)'),
    ('V87c', 'rank min(orig, 3)'),
    ('V87d', 'rank min(orig, 5)'),
    ('V87e', 'rank min(orig, 10)'),
]

_, _, base_savg, _ = run('baseline')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

print(f'\n{"variant":<10}{"desc":<38}{"avg":>9}{"med":>9}{"MDD":>9}{"매수회수":>10}{"lift":>9}{"wins":>9}')
print('-' * 105)

results = {}
for vid, desc in VARIANTS:
    cums, mdds, savg, buys = run(vid)
    avg = statistics.mean(cums)
    med = statistics.median(cums)
    mdd_med = statistics.median(mdds)
    avg_buys = statistics.mean(buys)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    results[vid] = (avg, mdd_med, avg_lift, wins, avg_buys)
    mk = ' ★' if vid == 'baseline' else '  '
    ls = '' if vid == 'baseline' else f'{avg_lift:>+7.1f}p{wins:>6}/100'
    print(f'{mk}{vid:<8}{desc:<38}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{avg_buys:>9.1f}{ls}')
    sys.stdout.flush()

# LOWO -MU-SNDK 견고성
print('\n[LOWO 견고성 -MU-SNDK]')
print(f'{"variant":<10}{"전체":>11}{"-MU":>13}{"-SNDK":>13}{"-MU-SNDK":>14}')
print('-' * 65)

for vid, _ in VARIANTS:
    if vid == 'baseline':
        continue
    row = f'{vid:<10}'
    for exn, ex in [('전체', ()), ('-MU', ('MU',)), ('-SNDK', ('SNDK',)), ('-MU-SNDK', ('MU', 'SNDK'))]:
        _, _, b, _ = run('baseline', exclude=ex)
        _, _, n, _ = run(vid, exclude=ex)
        lifts = [y - x for x, y in zip(b, n)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        if exn == '전체':
            row += f'{avg_lift:>+8.1f}p({wins:>2})'
        else:
            row += f'{avg_lift:>+9.1f}p({wins:>2})'
    print(row)
    sys.stdout.flush()

# 부분기간 stratification
n_dates = len(dates)
front_starts = [ch for ch in seeds if all(s < n_dates // 2 for s in ch)]
back_starts = [ch for ch in seeds if all(s >= n_dates // 2 - 30 for s in ch)]
print(f'\n[부분기간 — 전반 {len(front_starts)} / 후반 {len(back_starts)}]')

def run_filtered(variant, sf, exclude=()):
    savg = []
    for ch in sf:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return savg

print(f'{"variant":<10}{"전반 lift":>11}{"전반 wins":>11}{"후반 lift":>11}{"후반 wins":>11}')
print('-' * 60)
for vid, _ in VARIANTS:
    if vid == 'baseline':
        continue
    row = f'{vid:<10}'
    bf = run_filtered('baseline', front_starts)
    nf = run_filtered(vid, front_starts)
    lifts = [y - x for x, y in zip(bf, nf)]
    al = sum(lifts) / len(lifts) if lifts else 0
    wf = sum(1 for l in lifts if l > 0)
    row += f'{al:>+8.1f}p{wf:>8}/{len(lifts)}'
    bb = run_filtered('baseline', back_starts)
    nb = run_filtered(vid, back_starts)
    lifts = [y - x for x, y in zip(bb, nb)]
    al = sum(lifts) / len(lifts) if lifts else 0
    wb = sum(1 for l in lifts if l > 0)
    row += f'{al:>+8.1f}p{wb:>8}/{len(lifts)}'
    print(row)

# V86e vs V87 best 직접 비교
print('\n[V86e vs V87 variants 직접 paired]')
_, _, v86e, _ = run('V86e')
for vid in ['V87a', 'V87b', 'V87c', 'V87d', 'V87e', 'V87f']:
    _, _, n, _ = run(vid)
    lifts = [y - x for x, y in zip(v86e, n)]
    al = sum(lifts) / len(lifts)
    w = sum(1 for l in lifts if l > 0)
    print(f'  {vid} - V86e: {al:+.1f}p ({w}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
