# -*- coding: utf-8 -*-
"""V110 슬롯 2 처리 옵션 비교 (메가 부재기)

V110a: 슬롯 2 비움 (슬롯 1 단독 100%) — V110 원본
V110b: 슬롯 2를 part2 Top 2로 채움 (V110 + V113 하이브리드)
V110c: 슬롯 2 비움 + 슬롯 1만 50% (현금 50%)

공통: 메가 활성 시 슬롯 2 = mega_score Top 1, 50/50.
공통: 메가 PEG < 0.25 + rev_growth ≥ 25%.
공통: v113 carryover patch (info None 시 메가면 carryover).
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


def is_mega(info, peg_thr=0.25):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True


def mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)


def sim(variant, exclude=(), start=0):
    """variant: v110a (메가없으면 slot1 단독 100%),
              v110b (메가없으면 part2 top2로 채움),
              v110c (메가없으면 slot1 50%+현금50%)"""
    held = {}; prev = None; val = 1.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret)
        dd = data_all[d]

        # 매도 (V113 carryover)
        for tk in list(held):
            info = dd.get(tk)
            if info is None: continue  # V113: 데이터 없으면 carryover
            if info.get('min_seg', 0) < -2: del held[tk]; continue
            if is_mega(info):
                if info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]
                continue  # 메가 유지
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
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is not None and p2 <= 3:
                    p2_cands.append((p2, info['score'], tk))
                if is_mega(info):
                    mega_cands.append((-mega_score(info), info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])

            seen = set(s_tk for s_tk in held)
            pick_p2 = next((c for c in p2_cands if c[2] not in seen), None)
            pick_mega = next((c for c in mega_cands if c[2] not in seen), None)

            if len(held) == 0:
                if pick_p2 and pick_mega and pick_p2[2] != pick_mega[2]:
                    # 둘 다 50/50
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
                elif pick_p2:
                    # 메가 없음 / 같은 종목 → variant 분기
                    if variant == 'v110b':
                        # part2 Top 2로 채움
                        held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                        seen.add(pick_p2[2])
                        pick_p2_2 = next((c for c in p2_cands if c[2] not in seen), None)
                        if pick_p2_2:
                            held[pick_p2_2[2]] = (d, dd[pick_p2_2[2]]['price'], 0.5)
                        else:
                            # slot1을 100%로 boost (part2 2위 없으면)
                            held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 1.0)
                    elif variant == 'v110c':
                        # slot 1만 50% (현금 50%)
                        held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                    else:  # v110a
                        # slot 1 단독 100%
                        held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 1.0)
            elif len(held) == 1:
                # 1슬롯 비어있음 — variant에 따라 채움
                if pick_mega and pick_mega[2] not in held:
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
                elif pick_p2 and pick_p2[2] not in held:
                    if variant == 'v110b':
                        held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
        prev = dict(held)
    return (val-1)*100


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(variant, exclude=()):
    return [sim(variant, exclude=exclude, start=s) for ch in seeds for s in ch]


print('=' * 100)
print('V110 슬롯 2 옵션 비교 (50/50 고정, PEG<0.25, V113 carryover)')
print('=' * 100)

print('\n[1] Full period (start=0)')
for v in ['v110a', 'v110b', 'v110c']:
    print(f'  {v}: cum {sim(v, start=0):+.1f}%')

print('\n[2] Multistart 100×3 평균')
exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')), ('-all5', ('SNDK', 'MU', 'BE', 'LITE', 'TTMI'))]
print(f'{"variant":<10}' + ''.join(f'{n:>11}' for n, _ in exclusions))
print('-' * 80)
data_runs = {}
for v in ['v110a', 'v110b', 'v110c']:
    runs = {n: run(v, ex) for n, ex in exclusions}
    data_runs[v] = runs
    print(f'{v:<10}' + ''.join(f'{statistics.mean(runs[n]):>+10.1f}%' for n, _ in exclusions))

print('\n[3] 안정성 (전체 시나리오)')
for v in ['v110a', 'v110b', 'v110c']:
    cums = data_runs[v]['전체']
    avg = statistics.mean(cums); sd = statistics.stdev(cums)
    sharpe = avg/sd if sd > 0 else 0
    worst = min(cums); best = max(cums); pos = sum(1 for c in cums if c > 0)
    print(f'  {v}: avg={avg:+.1f}% / stdev={sd:.1f} / sharpe={sharpe:.2f} / 양수 {pos}/{len(cums)}')

print('\n[4] paired diff (V110b vs V110a)')
for n, ex in exclusions:
    diffs = [a-b for a, b in zip(data_runs['v110b'][n], data_runs['v110a'][n])]
    avg = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {n}: {avg:+.1f}p ({wins}/{len(diffs)})')

print('\n[5] paired diff (V110c vs V110a)')
for n, ex in exclusions:
    diffs = [a-b for a, b in zip(data_runs['v110c'][n], data_runs['v110a'][n])]
    avg = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {n}: {avg:+.1f}p ({wins}/{len(diffs)})')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
