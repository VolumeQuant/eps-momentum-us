# -*- coding: utf-8 -*-
"""V110 final grid — V107 best (V106c + PEG 0.25) 인접 정밀 + LOWO 검증

V107 발견: V106c + PEG 0.25 = 종합 +539.8% (v86e+ +546.6과 거의 동등)
- -SNDK +125.9 (v86e+ +121.9, +4p 우월)
- -SNDK-MU +72.5 (v86e+ +67.8, +4.7p 우월)

이 best candidate 정밀 그리드:
- PEG 0.23, 0.24, 0.25, 0.26, 0.27, 0.28 (인접)
- rev_thr 0.20, 0.25, 0.30 (인접)
- 슬롯 분배 변형 (50/50, 60/40, 40/60)
- LOWO -all5 (MU/SNDK/BE/LITE/TTMI 제외)
- multistart percentile
- 부분 기간 (전반/후반)
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

data = {}
for d in dates:
    data[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?', (d,)).fetchall():
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]; fpe = (r[3]/nc) if (r[3] and nc>0) else None
        peg = (fpe/(rg*100)) if (fpe and rg and rg>0) else None
        ntm_rev = (nc/n90-1)*100 if (nc and n90 and n90>0) else 0
        rg_pct = rg*100 if rg else 0
        peg_inv = 1/peg if peg and peg > 0 else 0
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, ntm_rev=ntm_rev, rg_pct=rg_pct, peg_inv=peg_inv)

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True


def is_mega(info, peg_thr=0.25, rev_thr=0.25):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < rev_thr: return False
    return True


def get_mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50*info.get('peg_inv', 0)


def sim(peg_thr=0.25, rev_thr=0.25, w_p2=0.5, w_mega=0.5, exclude=(), start=0):
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val); mdd = max(mdd, (peak-val)/peak)
        dd = data[d]

        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if is_mega(info, peg_thr, rev_thr) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info, peg_thr, rev_thr): continue
                del held[tk]

        if len(held) < 2:
            mega_cands = []; p2_cands = []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is not None and p2 <= 3:
                    p2_cands.append((p2, info['score'], tk))
                if is_mega(info, peg_thr, rev_thr):
                    mega_cands.append((-get_mega_score(info), info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])

            seen = set()
            pick = []
            if p2_cands: pick.append(p2_cands[0]); seen.add(p2_cands[0][2])
            if mega_cands and mega_cands[0][2] not in seen:
                pick.append(mega_cands[0])
            pick = pick[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                ws = [w_p2, w_mega] if abs(w_p2 + w_mega - 1.0) < 0.01 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    held[tk] = (d, dd[tk]['price'], ws[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return (val-1)*100, mdd*100


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run_avg(peg_thr=0.25, rev_thr=0.25, w_p2=0.5, w_mega=0.5, ex=()):
    cums = []
    for ch in seeds:
        for s in ch:
            r, _ = sim(peg_thr, rev_thr, w_p2, w_mega, exclude=ex, start=s)
            cums.append(r)
    return statistics.mean(cums), cums


def sim_v86e(exclude=(), start=0):
    held = {}; prev = None; val = 1.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret)
        dd = data[d]
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if (info.get('peg') is not None and info['peg'] < 0.22) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if info is not None and info.get('peg') is not None and info['peg'] < 0.22: continue
                del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > 3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1-s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0: held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return (val-1)*100


def v86e_avg(ex=()):
    cums = []
    for ch in seeds:
        for s in ch:
            cums.append(sim_v86e(exclude=ex, start=s))
    return statistics.mean(cums), cums


print('=' * 100)
print('V110 final grid — V107 best 정밀 + LOWO')
print('=' * 100)

exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')),
              ('-SNDK-MU-LITE', ('SNDK', 'MU', 'LITE')),
              ('-all5', ('SNDK', 'MU', 'BE', 'LITE', 'TTMI'))]

baseline = {}
for n, ex in exclusions:
    avg, _ = v86e_avg(ex)
    baseline[n] = avg

print(f'\n[v86e+ baseline]')
for n in [n for n, _ in exclusions]:
    print(f'  {n}: {baseline[n]:+.1f}%')

print(f'\n[V110 PEG 정밀 그리드 (V107 best=PEG 0.25, rev 0.25)]')
print(f'{"peg":<8}{"rev":<8}' + ''.join(f'{n:>10}' for n, _ in exclusions))
print('-' * 95)
best_cfg = None
best_diff = -999
for pt in [0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.30]:
    for rt in [0.20, 0.25, 0.30]:
        avgs = {n: run_avg(pt, rt, ex=ex)[0] for n, ex in exclusions}
        # 사용자 두 조건 동시 만족 score: 전체 + (-SNDK lift) + (-SNDK-MU lift) + LOWO -all5
        score = avgs['전체'] + 2*(avgs['-SNDK'] - baseline['-SNDK']) + 2*(avgs['-SNDK-MU'] - baseline['-SNDK-MU']) + 1*(avgs['-all5'] - baseline['-all5'])
        if score > best_diff:
            best_diff = score; best_cfg = (pt, rt)
        print(f'{pt:<8.2f}{rt:<8.2f}' + ''.join(f'{avgs[n]:>+9.1f}%' for n, _ in exclusions))

print(f'\n★ best config: PEG={best_cfg[0]}, rev={best_cfg[1]}, score={best_diff:.1f}')

# best vs v86e+ paired
print(f'\n[best vs v86e+ paired diff]')
pt, rt = best_cfg
for n, ex in exclusions:
    _, cums_best = run_avg(pt, rt, ex=ex)
    _, cums_v86 = v86e_avg(ex)
    diffs = [a-b for a, b in zip(cums_best, cums_v86)]
    avg_d = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {n}: best - v86e+ = {avg_d:+.1f}p ({wins}/{len(diffs)})')

# 부분기간
print(f'\n[부분기간 — 전반 (start<n/2) / 후반 (start>=n/2)]')
n_d = len(dates)
front_starts = [ch for ch in seeds if all(s < n_d//2 for s in ch)]
back_starts = [ch for ch in seeds if all(s >= n_d//2 - 5 for s in ch)]
def run_filtered(sf, ex=()):
    cums_best = []; cums_v86 = []
    for ch in sf:
        for s in ch:
            r, _ = sim(best_cfg[0], best_cfg[1], exclude=ex, start=s)
            cums_best.append(r)
            cums_v86.append(sim_v86e(exclude=ex, start=s))
    if not cums_best: return None, None
    return statistics.mean(cums_best), statistics.mean(cums_v86)

print(f'전반: {len(front_starts)} chains / 후반: {len(back_starts)} chains')
for n, ex in [('전체', ()), ('-SNDK', ('SNDK',))]:
    fb, fv = run_filtered(front_starts, ex)
    bb, bv = run_filtered(back_starts, ex)
    if fb is not None:
        print(f'  {n} 전반: best {fb:+.1f}% / v86e+ {fv:+.1f}% (diff {fb-fv:+.1f}p)')
    if bb is not None:
        print(f'  {n} 후반: best {bb:+.1f}% / v86e+ {bv:+.1f}% (diff {bb-bv:+.1f}p)')

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
