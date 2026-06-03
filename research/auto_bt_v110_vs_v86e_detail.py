# -*- coding: utf-8 -*-
"""V110 vs v86e+ 정밀 비교"""
import sys, sqlite3, random, statistics
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10
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

def is_mega_v86(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.22

def is_mega_v110(info):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= 0.25: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True

def get_mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50*info.get('peg_inv', 0)

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
                if is_mega_v86(info) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega_v86(info): continue
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

def sim_v110(exclude=(), start=0):
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
                if is_mega_v110(info) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega_v110(info): continue
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
                if is_mega_v110(info):
                    mega_cands.append((-get_mega_score(info), info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])
            seen = set(); pick = []
            if p2_cands: pick.append(p2_cands[0]); seen.add(p2_cands[0][2])
            if mega_cands and mega_cands[0][2] not in seen:
                pick.append(mega_cands[0])
            pick = pick[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                for si, (_, _, tk) in enumerate(pick[:2]):
                    held[tk] = (d, dd[tk]['price'], 0.5)
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return (val-1)*100


# 시작점별 비교
print('=== 시작점별 V110 vs v86e+ 비교 ===')
print(f'{"start":>6}{"days":>6}{"v86e+":>10}{"V110":>10}{"diff":>10}{"-SNDK v86e+":>14}{"-SNDK V110":>14}{"-SNDK diff":>13}')
print('-' * 90)
v110_wins = 0
v86_wins = 0
v110_wins_sndk = 0
n_total = 0
for s in range(2, len(dates) - MIN_HOLD, 3):
    n_total += 1
    r86 = sim_v86e(start=s)
    r110 = sim_v110(start=s)
    r86_sndk = sim_v86e(exclude=('SNDK',), start=s)
    r110_sndk = sim_v110(exclude=('SNDK',), start=s)
    diff = r110 - r86
    diff_sndk = r110_sndk - r86_sndk
    if diff > 0: v110_wins += 1
    if diff < 0: v86_wins += 1
    if diff_sndk > 0: v110_wins_sndk += 1
    print(f'{s:>6}{len(dates)-s:>6}{r86:>+9.1f}%{r110:>+9.1f}%{diff:>+9.1f}p{r86_sndk:>+13.1f}%{r110_sndk:>+13.1f}%{diff_sndk:>+12.1f}p')

print(f'\n총 {n_total} 시작점')
print(f'전체: V110 우월 {v110_wins} / v86e+ 우월 {v86_wins}')
print(f'SNDK 제외 시: V110 우월 {v110_wins_sndk}/{n_total}')

con.close()
