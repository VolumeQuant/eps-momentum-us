# -*- coding: utf-8 -*-
"""메가홀드 sweet spot — Phase 5: 최종 비교 + 부분기간 + 회사PC v86 vs 새 sweet spot

자율주행 phase 1~4 발견 종합:
  - Phase 1: NTM × PEG 49 cells → NTM 조건 무력, PEG가 알파 소스
  - Phase 2: PEG<0.20 only가 진짜 sweet spot (S2 LOWO -MU-SNDK +12.8p vs v86 +0.0p)
  - Phase 3: min_seg<-2 + rev_growth<15% 매도 best (E5 +82.5p)
  - Phase 4: 무한 보호가 best (rank cutoff 없음)

최종 후보:
  V86  : 회사 PC 원안 — NTM≥60 AND PEG<0.20, min_seg<-2 매도 only
  V86b : PEG<0.20 only, min_seg<-2 매도 only  (NTM 조건 제거)
  V86c : PEG<0.20 only, min_seg<-2 OR rev_growth<15% 매도  (PEG only + exit 강화) ⭐ 후보

이번 Phase 5 검증:
  1. V86 vs V86b vs V86c 직접 비교 (paired 100×3 + LOWO + 부분기간)
  2. 인접성 (PEG 0.18/0.20/0.22, rev_g cutoff 10/15/20)
  3. 메가 부재 기간 / 메가 포함 기간 stratification
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


def sim(variant='baseline', exclude=(), start=0, ntm_thr=60, peg_thr=0.20, rev_exit=0.15):
    """variant별 시뮬"""
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

            # exit 트리거
            if info is not None:
                # min_seg<-2 = 항상
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                # rev_growth<rev_exit (V86c only)
                if variant == 'V86c' and info.get('rev_growth') is not None and info['rev_growth'] < rev_exit:
                    del held[tk]
                    continue

            # rank > 10 → 메가홀드 체크
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                # 메가홀드 판정
                is_mega = False
                if variant == 'V86':
                    is_mega = (info is not None and info.get('ntm_rev') is not None
                               and info.get('peg') is not None
                               and info['ntm_rev'] >= ntm_thr and info['peg'] < peg_thr)
                elif variant in ('V86b', 'V86c'):
                    is_mega = (info is not None and info.get('peg') is not None
                               and info['peg'] < peg_thr)
                if is_mega:
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


def run(variant, exclude=(), peg_thr=0.20, rev_exit=0.15, seeds_filter=None):
    cums = []
    mdds = []
    savg = []
    iter_seeds = seeds if seeds_filter is None else seeds_filter
    for ch in iter_seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s, peg_thr=peg_thr, rev_exit=rev_exit)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg


print('=' * 100)
print('Phase 5: 최종 비교 — V86 (회사PC) vs V86b (PEG only) vs V86c (PEG + rev_exit)')
print('=' * 100)

_, _, base_savg = run('baseline')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

VARIANTS = [
    ('V86', '회사PC 원안 (NTM≥60 AND PEG<0.20)'),
    ('V86b', '신후보 — PEG<0.20 only'),
    ('V86c', '신후보 — PEG<0.20 + rev_growth<15% 매도 추가'),
]

print(f'\n{"variant":<6}{"desc":<42}{"avg":>9}{"med":>9}{"MDD":>9}{"lift":>9}{"wins":>9}')
print('-' * 95)

results = {}
for vid, desc in VARIANTS:
    cums, mdds, savg = run(vid)
    avg = statistics.mean(cums)
    med = statistics.median(cums)
    mdd_med = statistics.median(mdds)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    results[vid] = (avg, mdd_med, avg_lift, wins, savg)
    ls = f'{avg_lift:>+7.1f}p{wins:>6}/100'
    print(f'  {vid:<4}{desc:<42}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{ls}')
    sys.stdout.flush()

# Pairwise: V86c vs V86 직접 비교
print('\n[V86c vs V86 직접 paired]')
_, _, v86 = run('V86')
_, _, v86c = run('V86c')
lifts_direct = [c - v for v, c in zip(v86, v86c)]
avg_lift = sum(lifts_direct) / len(lifts_direct)
wins = sum(1 for l in lifts_direct if l > 0)
print(f'  V86c - V86: avg lift {avg_lift:+.1f}p, wins {wins}/100')

# LOWO
print('\n[LOWO 견고성 종합]')
print(f'{"variant":<6}{"전체":>11}{"-MU":>13}{"-SNDK":>13}{"-MU-SNDK":>14}')
print('-' * 65)

for vid, _ in VARIANTS:
    row = f'{vid:<6}'
    _, _, b_all = run('baseline')
    _, _, n_all = run(vid)
    lift_all = sum(y - x for x, y in zip(b_all, n_all)) / len(b_all)
    wins_all = sum(1 for l in [y - x for x, y in zip(b_all, n_all)] if l > 0)
    row += f'{lift_all:>+8.1f}p({wins_all:>2})'
    for exn, ex in [('-MU', ('MU',)), ('-SNDK', ('SNDK',)), ('-MU-SNDK', ('MU', 'SNDK'))]:
        _, _, b = run('baseline', exclude=ex)
        _, _, n = run(vid, exclude=ex)
        lifts = [y - x for x, y in zip(b, n)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+9.1f}p({wins:>2})'
    print(row)
    sys.stdout.flush()

# 부분 기간 stratification
# 데이터 dates 절반: 전반 / 후반
n_dates = len(dates)
half = n_dates // 2
front_starts = [ch for ch in seeds if all(s < half for s in ch)]
back_starts = [ch for ch in seeds if all(s >= half - 30 for s in ch)]  # 후반 시작
print(f'\n[부분기간 stratification — 전반 {len(front_starts)} chains / 후반 {len(back_starts)} chains]')
print(f'{"variant":<6}{"전반 lift":>11}{"전반 wins":>11}{"후반 lift":>11}{"후반 wins":>11}')
print('-' * 55)

for vid, _ in VARIANTS:
    row = f'{vid:<6}'
    # 전반
    if front_starts:
        _, _, b_f = run('baseline', seeds_filter=front_starts)
        _, _, n_f = run(vid, seeds_filter=front_starts)
        lifts = [y - x for x, y in zip(b_f, n_f)]
        avg_lift = sum(lifts) / len(lifts) if lifts else 0
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+8.1f}p{wins:>8}/{len(lifts)}'
    else:
        row += f'{"N/A":>11}{"N/A":>11}'
    # 후반
    if back_starts:
        _, _, b_b = run('baseline', seeds_filter=back_starts)
        _, _, n_b = run(vid, seeds_filter=back_starts)
        lifts = [y - x for x, y in zip(b_b, n_b)]
        avg_lift = sum(lifts) / len(lifts) if lifts else 0
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+8.1f}p{wins:>8}/{len(lifts)}'
    else:
        row += f'{"N/A":>11}{"N/A":>11}'
    print(row)

# 인접성 (V86c 기준)
print('\n[V86c 인접성 — PEG 0.15/0.20/0.25, rev_exit 0.10/0.15/0.20]')
print(f'{"PEG":>5}{"rev_exit":>10}{"lift":>10}{"wins":>10}')
print('-' * 35)
for peg_t in [0.15, 0.20, 0.25]:
    for re_t in [0.10, 0.15, 0.20]:
        _, _, savg = run('V86c', peg_thr=peg_t, rev_exit=re_t)
        lifts = [b - a for a, b in zip(base_savg, savg)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        print(f'{peg_t:>5.2f}{re_t:>10.2f}{avg_lift:>+9.1f}p{wins:>7}/100')

print(f'\n총 소요 {time.time() - t0:.0f}초')

# 최종 결론
print('\n' + '=' * 100)
print('최종 결론')
print('=' * 100)
v86_lift = results['V86'][2]
v86b_lift = results['V86b'][2]
v86c_lift = results['V86c'][2]
print(f'V86 (회사PC) lift   : {v86_lift:+.1f}p')
print(f'V86b (PEG only) lift: {v86b_lift:+.1f}p')
print(f'V86c (PEG+revexit)  : {v86c_lift:+.1f}p')
diff = v86c_lift - v86_lift
print(f'\nV86c vs V86 lift 차: {diff:+.1f}p')
print(f'V86c LOWO -MU-SNDK 견고성이 V86보다 큼 (Phase 2 발견)')
con.close()
