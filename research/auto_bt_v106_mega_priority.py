# -*- coding: utf-8 -*-
"""V106 — 메가 우선 entry 변형 + PEG threshold + 슬롯 확장

V105 발견: part2 우선이라 메가 추가 entry 안 일어남
새 시도:
- V106a: slot 1 = mega Top 1, slot 2 = part2 Top 1 (mega 우선)
- V106b: slot 1 = part2 Top 1, slot 2 = mega Top 1 (mean reversion 우선)
- V107: PEG threshold 그리드 (0.10/0.15/0.20/0.22/0.25/0.30)
- V108: 슬롯 3 = 메가 전용
- V109: 메가 시그니처 변형 (rev_g 임계값, NTM 임계값)
- V110: 동적 ranking (메가 부재 시 메가 우선, 있을 때 carryover만)
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


def is_mega(info, peg_thr=0.22, rev_thr=0.25, ntm_thr=0):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < rev_thr: return False
    if ntm_thr > 0 and info.get('ntm_rev', 0) < ntm_thr: return False
    return True


def get_mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50*info.get('peg_inv', 0)


def sim(variant, peg_thr=0.22, rev_thr=0.25, ntm_thr=0, exclude=(), start=0):
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

        # 매도 — carryover
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if is_mega(info, peg_thr, rev_thr, ntm_thr) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info, peg_thr, rev_thr, ntm_thr): continue
                del held[tk]

        # 매수
        if len(held) < 2:
            # 메가 후보
            mega_cands = []
            p2_cands = []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is not None and p2 <= 3:
                    p2_cands.append((p2, info['score'], tk))
                if is_mega(info, peg_thr, rev_thr, ntm_thr):
                    ms = get_mega_score(info)
                    mega_cands.append((ms, info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: -x[0])

            pick = []
            if variant == 'V106a':  # mega 우선 (slot 1 = mega, slot 2 = part2)
                if mega_cands: pick.append(mega_cands[0])
                if p2_cands: pick.append(p2_cands[0])
            elif variant == 'V106b':  # part2 우선 (slot 1 = part2, slot 2 = mega)
                if p2_cands: pick.append(p2_cands[0])
                if mega_cands and (not p2_cands or mega_cands[0][2] != p2_cands[0][2]):
                    pick.append(mega_cands[0])
            elif variant == 'V106c':  # mega + part2 둘 다 (top 1씩)
                seen = set()
                if p2_cands:
                    pick.append(p2_cands[0]); seen.add(p2_cands[0][2])
                if mega_cands and mega_cands[0][2] not in seen:
                    pick.append(mega_cands[0])

            pick = pick[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                w = [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return (val-1)*100


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run_avg(variant, peg_thr=0.22, rev_thr=0.25, ntm_thr=0, ex=()):
    cums = []
    for ch in seeds:
        for s in ch:
            cums.append(sim(variant, peg_thr, rev_thr, ntm_thr, exclude=ex, start=s))
    return statistics.mean(cums), cums


# baseline (v86e+)
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
                if is_mega(info) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info): continue
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
    return statistics.mean(cums)


print('=' * 100)
print('V106/V107 — 메가 우선 entry + PEG threshold grid')
print('=' * 100)

exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)), ('-SNDK-MU', ('SNDK', 'MU'))]
baseline = {n: v86e_avg(ex) for n, ex in exclusions}
print('\n[v86e+ baseline]')
for n in baseline: print(f'  {n}: {baseline[n]:+.1f}%')

print('\n[V106 — entry priority]')
print(f'{"variant":<10}{"전체":>10}{"-SNDK":>10}{"-MU":>10}{"-SNDK-MU":>11}{"종합":>10}')
print('-' * 70)
for v in ['V106a', 'V106b', 'V106c']:
    avgs = {n: run_avg(v, ex=ex)[0] for n, ex in exclusions}
    total = sum(avgs.values())
    print(f'{v:<10}' + ''.join(f'{avgs[n]:>+9.1f}%' for n, _ in exclusions) + f'{total:>+9.1f}%')

print('\n[V107 — PEG threshold + V106c (mega+part2 둘 다 entry)]')
print(f'{"peg_thr":<10}{"전체":>10}{"-SNDK":>10}{"-MU":>10}{"-SNDK-MU":>11}{"종합":>10}')
print('-' * 70)
for pt in [0.10, 0.15, 0.20, 0.22, 0.25, 0.30, 0.50]:
    avgs = {n: run_avg('V106c', peg_thr=pt, ex=ex)[0] for n, ex in exclusions}
    total = sum(avgs.values())
    print(f'{pt:<10.2f}' + ''.join(f'{avgs[n]:>+9.1f}%' for n, _ in exclusions) + f'{total:>+9.1f}%')

print('\n[V108 — rev_thr grid + V106c]')
print(f'{"rev_thr":<10}{"전체":>10}{"-SNDK":>10}{"-MU":>10}{"-SNDK-MU":>11}{"종합":>10}')
print('-' * 70)
for rt in [0.10, 0.15, 0.20, 0.25, 0.30, 0.50, 1.0]:
    avgs = {n: run_avg('V106c', rev_thr=rt, ex=ex)[0] for n, ex in exclusions}
    total = sum(avgs.values())
    print(f'{rt:<10.2f}' + ''.join(f'{avgs[n]:>+9.1f}%' for n, _ in exclusions) + f'{total:>+9.1f}%')

print('\n[V109 — NTM threshold + V106c]')
print(f'{"ntm_thr":<10}{"전체":>10}{"-SNDK":>10}{"-MU":>10}{"-SNDK-MU":>11}{"종합":>10}')
print('-' * 70)
for nt in [0, 30, 60, 100]:
    avgs = {n: run_avg('V106c', ntm_thr=nt, ex=ex)[0] for n, ex in exclusions}
    total = sum(avgs.values())
    print(f'{nt:<10}' + ''.join(f'{avgs[n]:>+9.1f}%' for n, _ in exclusions) + f'{total:>+9.1f}%')

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
