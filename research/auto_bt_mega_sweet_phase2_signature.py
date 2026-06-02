# -*- coding: utf-8 -*-
"""메가홀드 sweet spot — Phase 2: 대체 시그니처 비교

회사 PC v86: NTM≥60 AND PEG<0.2 (composite signature).
이번에 검증: NTM only / PEG only / 결합 / 다른 차원 결합.
목표: 어떤 시그니처가 진짜 알파 소스인지 분해.

variants:
  S0: baseline (no override)
  S1: NTM only (≥60)
  S2: PEG only (<0.20)
  S3: NTM ≥60 AND PEG <0.20 (v86 원안)
  S4: NTM ≥100 AND PEG <0.20 (Phase 1 sweet spot 보수)
  S5: NTM ≥60 AND rev_growth ≥30%
  S6: PEG ≥3-cell mean of recent 30d (relative)
  S7: NTM ≥60 AND rev_growth ≥50% (more selective)
  S8: ntm_rev × rev_growth product score (composite)
  S9: rev_growth only ≥50%
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


def make_check(variant):
    """variant_name → callable(info) -> bool"""
    if variant == 'S0':
        return lambda info: False
    elif variant == 'S1':  # NTM only ≥60
        return lambda info: info and info.get('ntm_rev') is not None and info['ntm_rev'] >= 60
    elif variant == 'S2':  # PEG only <0.20
        return lambda info: info and info.get('peg') is not None and info['peg'] < 0.20
    elif variant == 'S3':  # NTM≥60 AND PEG<0.20 (v86 원안)
        return lambda info: (info and info.get('ntm_rev') is not None and info.get('peg') is not None
                              and info['ntm_rev'] >= 60 and info['peg'] < 0.20)
    elif variant == 'S4':  # NTM≥100 AND PEG<0.20 (보수)
        return lambda info: (info and info.get('ntm_rev') is not None and info.get('peg') is not None
                              and info['ntm_rev'] >= 100 and info['peg'] < 0.20)
    elif variant == 'S5':  # NTM≥60 AND rev_growth ≥0.30
        return lambda info: (info and info.get('ntm_rev') is not None and info.get('rev_growth') is not None
                              and info['ntm_rev'] >= 60 and info['rev_growth'] >= 0.30)
    elif variant == 'S6':  # NTM ≥60 AND PEG<0.15 (더 엄격 PEG)
        return lambda info: (info and info.get('ntm_rev') is not None and info.get('peg') is not None
                              and info['ntm_rev'] >= 60 and info['peg'] < 0.15)
    elif variant == 'S7':  # NTM≥60 AND rev_growth ≥0.50
        return lambda info: (info and info.get('ntm_rev') is not None and info.get('rev_growth') is not None
                              and info['ntm_rev'] >= 60 and info['rev_growth'] >= 0.50)
    elif variant == 'S8':  # ntm_rev × rev_growth ≥ 30 (composite)
        def chk(info):
            if not info or info.get('ntm_rev') is None or info.get('rev_growth') is None:
                return False
            return info['ntm_rev'] * info['rev_growth'] >= 30
        return chk
    elif variant == 'S9':  # rev_growth ≥0.50 only
        return lambda info: info and info.get('rev_growth') is not None and info['rev_growth'] >= 0.50
    elif variant == 'S10':  # NTM≥60 AND PEG<0.20 AND rev_growth>0.20 (triple guard)
        return lambda info: (info and info.get('ntm_rev') is not None and info.get('peg') is not None
                              and info.get('rev_growth') is not None
                              and info['ntm_rev'] >= 60 and info['peg'] < 0.20 and info['rev_growth'] >= 0.20)
    elif variant == 'S11':  # NTM≥100 AND PEG<0.25 AND rev_growth>0.30 (most selective)
        return lambda info: (info and info.get('ntm_rev') is not None and info.get('peg') is not None
                              and info.get('rev_growth') is not None
                              and info['ntm_rev'] >= 100 and info['peg'] < 0.25 and info['rev_growth'] >= 0.30)
    return lambda info: False


def sim(check_fn, exclude=(), start=0):
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
                if check_fn(info):
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


def run(variant, exclude=()):
    check_fn = make_check(variant)
    cums = []
    mdds = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(check_fn, exclude=exclude, start=s)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg


print('=' * 100)
print('Phase 2: 메가홀드 대체 시그니처 비교')
print('=' * 100)

VARIANTS = [
    ('S0', 'baseline'),
    ('S1', 'NTM≥60 only'),
    ('S2', 'PEG<0.20 only'),
    ('S3', 'NTM≥60 AND PEG<0.20 (v86)'),
    ('S4', 'NTM≥100 AND PEG<0.20 (보수)'),
    ('S5', 'NTM≥60 AND rev_g≥30%'),
    ('S6', 'NTM≥60 AND PEG<0.15 (엄격 PEG)'),
    ('S7', 'NTM≥60 AND rev_g≥50%'),
    ('S8', 'ntm_rev × rev_g ≥ 30'),
    ('S9', 'rev_g≥50% only'),
    ('S10', 'NTM≥60 AND PEG<0.20 AND rev_g≥20% (triple)'),
    ('S11', 'NTM≥100 AND PEG<0.25 AND rev_g≥30% (most selective)'),
]

_, _, base_savg = run('S0')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

print(f'\n{"variant":<6}{"desc":<46}{"avg":>9}{"med":>9}{"MDD중앙":>9}{"lift":>9}{"wins":>9}')
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
    results[vid] = (avg, mdd_med, avg_lift, wins)
    mk = ' ★' if vid == 'S0' else '  '
    ls = '' if vid == 'S0' else f'{avg_lift:>+7.1f}p{wins:>6}/100'
    print(f'{mk}{vid:<4}{desc:<46}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{ls}')
    sys.stdout.flush()

# LOWO -MU-SNDK for each viable variant (lift>0 & wins>=80)
viable = [vid for vid, (_, _, lift, wins) in results.items() if lift > 0 and wins >= 80 and vid != 'S0']
print(f'\n[LOWO -MU-SNDK robustness (viable variants: {len(viable)})]')
print(f'{"variant":<6}{"전체 lift":>11}{"-MU lift":>12}{"-SNDK lift":>13}{"-MU-SNDK":>13}')
print('-' * 60)

for vid in viable:
    row = f'{vid:<6}'
    _, _, b_all = run('S0')
    _, _, n_all = run(vid)
    lift_all = sum(y - x for x, y in zip(b_all, n_all)) / len(b_all)
    row += f'{lift_all:>+10.1f}p'
    for exn, ex in [('-MU', ('MU',)), ('-SNDK', ('SNDK',)), ('-MU-SNDK', ('MU', 'SNDK'))]:
        _, _, b = run('S0', exclude=ex)
        _, _, n = run(vid, exclude=ex)
        lifts = [y - x for x, y in zip(b, n)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+9.1f}p ({wins:>2})'
    print(row)
    sys.stdout.flush()

# Adjacency: NTM 임계값 ±20% 안정성 검증 (S3 vs adjacent)
print('\n[S3 (v86 원안) 인접 그리드 안정성 — NTM 50/60/70, PEG 0.15/0.20/0.25]')
print(f'{"NTM":>5}{"PEG":>8}{"lift":>10}{"wins":>10}')
print('-' * 35)

for ntm in [50, 60, 70]:
    for peg in [0.15, 0.20, 0.25]:
        # 동적 variant: closure
        def make_chk(n, p):
            return lambda info: (info and info.get('ntm_rev') is not None and info.get('peg') is not None
                                  and info['ntm_rev'] >= n and info['peg'] < p)
        # sim 호출
        cums, mdds, savg = [], [], []
        check_fn = make_chk(ntm, peg)
        for ch in seeds:
            sr = []
            for s in ch:
                r = sim(check_fn, start=s)
                cums.append(r['cum'])
                mdds.append(r['mdd'])
                sr.append(r['cum'])
            savg.append(sum(sr) / len(sr))
        lifts = [b - a for a, b in zip(base_savg, savg)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        print(f'{ntm:>5}{peg:>8.2f}{avg_lift:>+9.1f}p{wins:>7}/100')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
