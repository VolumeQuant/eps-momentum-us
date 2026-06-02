# -*- coding: utf-8 -*-
"""메가홀드 sweet spot — Phase 1: NTM × PEG 7×7 = 49 cells 정밀 그리드

기준 v86: NTM≥60 AND PEG<0.2 → +81.5p / 100/100 (회사 PC가 4 cells만 검증).
이번에 검증할 그리드:
  NTM 임계값: 40 / 50 / 60 / 80 / 100 / 120 / 150
  PEG 임계값: 0.10 / 0.15 / 0.20 / 0.25 / 0.30 / 0.40 / 0.50

목표: sweet spot의 upper / lower bound 정확히 mapping.
방법: 각 cell paired BT 100×3, baseline 대비 lift/wins.

paired 정의: 같은 (seed, start triplet)에서 baseline vs override → 한 쌍 비교, lift>0이면 win.
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
                           ntm_rev=ntm_rev, peg=peg)

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


def is_mega(info, ntm_thr, peg_thr):
    if info is None:
        return False
    if info.get('ntm_rev') is None or info.get('peg') is None:
        return False
    return info['ntm_rev'] >= ntm_thr and info['peg'] < peg_thr


def sim(ntm_thr=None, peg_thr=None, exclude=(), start=0):
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
            ep = held[tk][1]
            p2 = info.get('p2') if info else None
            if info is not None and info.get('min_seg') is not None and info['min_seg'] < -2:
                del held[tk]
                continue
            if info is None or p2 is None or p2 > 10:
                if ntm_thr is not None and is_mega(info, ntm_thr, peg_thr):
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


def run(ntm, peg, exclude=()):
    cums = []
    mdds = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(ntm, peg, exclude=exclude, start=s)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg


print('=' * 100)
print('Phase 1: 메가홀드 NTM × PEG 7×7 = 49 cells 정밀 그리드')
print('=' * 100)

_, _, base_savg = run(None, None)
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

NTM_GRID = [40, 50, 60, 80, 100, 120, 150]
PEG_GRID = [0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]

# Header
print(f'\n{"NTM\\PEG":<10}' + ''.join(f'{p:>10.2f}' for p in PEG_GRID))
print('-' * (10 + 10 * len(PEG_GRID)))

# 결과 매트릭스: (lift, wins) 저장
results = {}
for ntm in NTM_GRID:
    row_str = f'{ntm:<10}'
    for peg in PEG_GRID:
        _, _, savg = run(ntm, peg)
        lifts = [b - a for a, b in zip(base_savg, savg)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        results[(ntm, peg)] = (avg_lift, wins)
        # 표시: lift만 (wins는 별도 표)
        row_str += f'{avg_lift:>+9.1f}p'
    print(row_str)
    sys.stdout.flush()

# wins matrix
print('\n[Wins matrix (paired vs baseline, /100)]')
print(f'{"NTM\\PEG":<10}' + ''.join(f'{p:>10.2f}' for p in PEG_GRID))
print('-' * (10 + 10 * len(PEG_GRID)))
for ntm in NTM_GRID:
    row_str = f'{ntm:<10}'
    for peg in PEG_GRID:
        _, wins = results[(ntm, peg)]
        row_str += f'{wins:>9}/100'
    print(row_str)

# Sweet spot 자동 탐지
print('\n[Top 10 cells (lift desc)]')
sorted_cells = sorted(results.items(), key=lambda x: -x[1][0])
print(f'{"rank":<6}{"NTM":>6}{"PEG":>8}{"lift":>10}{"wins":>10}')
for i, ((ntm, peg), (lift, wins)) in enumerate(sorted_cells[:10], 1):
    print(f'{i:<6}{ntm:>6}{peg:>8.2f}{lift:>+9.1f}p{wins:>7}/100')

# 평탄 고원 탐지: lift ≥ 90% of max AND wins ≥ 90
max_lift = sorted_cells[0][1][0]
plateau = [(k, v) for k, v in results.items() if v[0] >= 0.9 * max_lift and v[1] >= 90]
print(f'\n[Plateau (lift ≥ 90% of max={max_lift:.1f}p AND wins ≥ 90): {len(plateau)} cells]')
for (ntm, peg), (lift, wins) in plateau:
    print(f'  NTM={ntm:>3} PEG={peg:.2f}: {lift:+.1f}p ({wins}/100)')

# Robustness gate: 평탄 고원에서 LOWO -MU-SNDK 무해 (≥0) 확인
print('\n[Plateau cells LOWO -MU-SNDK robustness check]')
for (ntm, peg), (lift, wins) in plateau:
    _, _, b = run(None, None, exclude=('MU', 'SNDK'))
    _, _, n = run(ntm, peg, exclude=('MU', 'SNDK'))
    lifts = [y - x for x, y in zip(b, n)]
    lowo_lift = sum(lifts) / len(lifts)
    lowo_wins = sum(1 for l in lifts if l > 0)
    flag = '✓' if lowo_lift >= -1 else '✗'
    print(f'  {flag} NTM={ntm:>3} PEG={peg:.2f}: 전체+{lift:.1f}p / -MU-SNDK {lowo_lift:+.1f}p ({lowo_wins}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
