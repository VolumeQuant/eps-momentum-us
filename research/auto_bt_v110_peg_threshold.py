# -*- coding: utf-8 -*-
"""V110 PEG threshold 비교 BT (0.18 / 0.20 / 0.22 / 0.25 / 0.30)"""
import sys, sqlite3, random, statistics
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

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

def is_mega(info, peg_thr):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True

def mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)


def sim_with_mdd(peg_thr, exclude=(), start=0):
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
        for tk in list(held):
            info = dd.get(tk)
            if info is None: continue
            if info.get('min_seg', 0) < -2: del held[tk]; continue
            if is_mega(info, peg_thr):
                if info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]
                continue
            p2 = info.get('p2')
            if p2 is None or p2 > 10:
                del held[tk]
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
                if is_mega(info, peg_thr):
                    mega_cands.append((-mega_score(info), info['score'], tk))
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
            elif len(held) == 1:
                if pick_mega and pick_mega[2] not in held:
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))

print('=' * 80)
print('V110 PEG threshold 비교 (50/50 + V110a logic)')
print('=' * 80)
print(f'{"PEG":>8}{"수익":>12}{"MDD":>10}{"calmar":>10}{"worst MDD":>12}')
print('-' * 60)
for peg_thr in [0.18, 0.20, 0.22, 0.25, 0.30]:
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim_with_mdd(peg_thr, start=s)
            cums.append(c); mdds.append(m)
    avg_ret = statistics.mean(cums); avg_mdd = statistics.mean(mdds)
    worst = min(mdds)
    calmar = avg_ret / abs(avg_mdd) if avg_mdd != 0 else 0
    print(f'{peg_thr:>8.2f}{avg_ret:>+11.1f}%{avg_mdd:>+9.1f}%{calmar:>10.2f}{worst:>+11.1f}%')

con.close()
