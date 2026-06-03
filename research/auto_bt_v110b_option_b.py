# -*- coding: utf-8 -*-
"""V110b 옵션 A vs B 비교

옵션 A (현재 production): 메가 entry도 cr Top 30 + 3일 검증
옵션 B: 메가 entry는 cr 제한 풀기 (메가 시그니처만)
  - SNDK $1,716 같은 폭등 종목도 신규 매수 가능
  - 위험: fomo
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10
PEG_THR = 0.18

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
data_all = {}
for d in dates:
    data_all[d] = {}
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
        data_all[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                               min_seg=min(segs) if segs else 0, high30=r[10],
                               peg=peg, rev_growth=rg, ntm_rev=ntm_rev, rg_pct=rg_pct, peg_inv=peg_inv)
pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data_all[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True


def is_mega(info):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= PEG_THR: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True


def mega_score_fn(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)


def sim_with_mdd(variant, exclude=(), start=0):
    """variant: 'A' (cr Top 30 제한) or 'B' (메가 cr 무관)"""
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val)
            mdd = min(mdd, (val/peak - 1) * 100)
        dd = data_all[d]
        # 매도 (V113 carryover)
        for tk in list(held):
            info = dd.get(tk)
            if info is None: continue
            if info.get('min_seg', 0) < -2: del held[tk]; continue
            if is_mega(info):
                if info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]
                continue
            p2 = info.get('p2')
            if p2 is None or p2 > 10:
                del held[tk]
        # 매수
        if len(held) < 2:
            p2_cands, mega_cands = [], []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                # part2 후보 (cr Top 30 + 3일 검증 — 둘 다)
                p2 = info.get('p2')
                if p2 is not None and p2 <= 3 and verified(tk, i):
                    p2_cands.append((p2, info['score'], tk))
                # 메가 후보 — variant 분기
                if is_mega(info):
                    if variant == 'A':
                        if verified(tk, i):  # cr Top 30 제한
                            mega_cands.append((-mega_score_fn(info), info['score'], tk))
                    else:  # B
                        # cr 제한 없음 — 메가 시그니처만
                        mega_cands.append((-mega_score_fn(info), info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])
            pick_p2 = next((c for c in p2_cands if c[2] not in held), None)
            pick_mega = next((c for c in mega_cands if c[2] not in held), None)
            if len(held) == 0:
                if pick_p2 and pick_mega and pick_p2[2] != pick_mega[2]:
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
                elif pick_p2:
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 1.0)
                elif pick_mega:
                    # part2 없고 메가만 있는 경우 (옵션 B에서만 가능)
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 1.0)
            elif len(held) == 1:
                if pick_mega and pick_mega[2] not in held:
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(variant, exclude=()):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim_with_mdd(variant, exclude=exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


print('=' * 100)
print('V110b 옵션 A vs B (메가 cr 제한 풀기) — calmar 비교')
print('=' * 100)

# Full period
print('\n[1] Full period (start=0)')
for v in ['A', 'B']:
    c, m = sim_with_mdd(v, start=0)
    print(f'  옵션 {v}: cum {c:+.1f}% / MDD {m:.1f}% / calmar {c/abs(m) if m else 0:.2f}')

# Multistart
print('\n[2] Multistart 100×3 평균')
exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')), ('-all5', ('SNDK', 'MU', 'BE', 'LITE', 'TTMI'))]
print(f'{"scenario":<12}{"옵션A 수익":>14}{"옵션A MDD":>12}{"A calmar":>11}{"옵션B 수익":>14}{"옵션B MDD":>12}{"B calmar":>11}')
print('-' * 100)
results = {}
for n, ex in exclusions:
    cA, mA = run('A', ex)
    cB, mB = run('B', ex)
    results[n] = (cA, mA, cB, mB)
    avg_cA, avg_mA = statistics.mean(cA), statistics.mean(mA)
    avg_cB, avg_mB = statistics.mean(cB), statistics.mean(mB)
    calA = avg_cA/abs(avg_mA) if avg_mA else 0
    calB = avg_cB/abs(avg_mB) if avg_mB else 0
    print(f'{n:<12}{avg_cA:>+13.1f}%{avg_mA:>+11.1f}%{calA:>11.2f}{avg_cB:>+13.1f}%{avg_mB:>+11.1f}%{calB:>11.2f}')

# paired diff
print('\n[3] paired diff (옵션 B - 옵션 A)')
for n, ex in exclusions:
    cA, _, cB, _ = results[n]
    diffs = [a-b for a, b in zip(cB, cA)]
    avg = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {n}: {avg:+.1f}p ({wins}/{len(diffs)} 옵션B 우월)')

# worst MDD
print('\n[4] worst MDD (300 시뮬 중 최악)')
for v in ['A', 'B']:
    _, mdds = run(v, ())
    print(f'  옵션 {v}: worst MDD {min(mdds):.1f}%')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
