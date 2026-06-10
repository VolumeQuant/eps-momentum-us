# -*- coding: utf-8 -*-
"""V117 거래량 필터 — Top N 정밀 sweep

Top 3 / 5 / 10 / 15 / 20 / 30 + $1B+ 비교
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10
WHIPSAW_GAP = -0.10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]
data_all = {}
for d in dates:
    data_all[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth,dollar_volume_30d FROM ntm_screening WHERE date=?', (d,)).fetchall():
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]; dv = r[12]
        data_all[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                               min_seg=min(segs) if segs else 0, high30=r[10], rev_growth=rg, dv=dv)
pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p

def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data_all[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True

def above_ma12(tk, i):
    if i < 6: return True
    prices = []
    for j in range(max(0, i-11), i+1):
        p = pf[dates[j]].get(tk)
        if p: prices.append(p)
    if len(prices) < 6: return True
    ma12 = sum(prices) / len(prices)
    cur_p = pf[dates[i]].get(tk)
    return cur_p > ma12 if cur_p else True

def today_gap(tk, i):
    if i < 1: return 0
    cur_p = pf[dates[i]].get(tk); prev_p = pf[dates[i-1]].get(tk)
    if not cur_p or not prev_p: return 0
    return cur_p / prev_p - 1


def sim(top_n, vol_filter=True, exclude=(), start=0):
    """top_n: part2_rank cutoff. vol_filter: $1B+ 적용 여부."""
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w, _) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val)
            mdd = min(mdd, (val/peak - 1) * 100)
        dd = data_all[d]
        for tk in list(held):
            info = dd.get(tk); ed, ep, w, grace = held[tk]
            if info and info.get('min_seg', 0) < -2: del held[tk]; continue
            if info is None: continue
            p2 = info.get('p2')
            if (p2 is None or p2 > 10) and not above_ma12(tk, i):
                gap = today_gap(tk, i)
                if gap <= WHIPSAW_GAP and not grace:
                    held[tk] = (ed, ep, w, True); continue
                del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is None or p2 > top_n: continue
                if vol_filter:
                    dv = info.get('dv') or 0
                    if dv < 1000: continue
                cands.append((p2, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                for _, _, tk in pick[:2]: held[tk] = (d, dd[tk]['price'], 0.5, False)
            elif len(held) == 0 and len(pick) == 1: held[pick[0][2]] = (d, dd[pick[0][2]]['price'], 1.0, False)
            else:
                for _, _, tk in pick: held[tk] = (d, dd[tk]['price'], 0.5, False)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(top_n, vol_filter=True, exclude=()):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim(top_n, vol_filter, exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


print('=' * 100)
print('Top N sweep (v117 거래량 $1B+ 적용)')
print('=' * 100)
print(f'{"variant":<22}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}{"vs Top3":>11}')
print('-' * 85)

# baseline (no vol filter, top 3)
bc, bm = run(3, vol_filter=False)
b_avg = statistics.mean(bc); b_mdd = statistics.mean(bm)
b_cal = b_avg / abs(b_mdd) if b_mdd else 0
print(f'{"Top 3 (no vol)":<22}{b_avg:>+9.1f}%{b_mdd:>+9.1f}%{b_cal:>8.2f}{sum(1 for c in bc if c > 0):>6}/300 {sim(3, False, start=0)[0]:>+8.1f}%')

# Top N + $1B+
results = {}
for top_n in [3, 5, 7, 10, 15, 20, 30]:
    cums, mdds = run(top_n, vol_filter=True)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg / abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    full_c, _ = sim(top_n, True, start=0)
    results[top_n] = (cums, avg, mdd, cal, pos)
    diff_3 = avg - results[3][1] if top_n != 3 else 0
    diff_str = f'{diff_3:+.1f}p' if top_n != 3 else 'baseline'
    print(f'Top {top_n} + $1B+{"":<10}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{pos:>6}/300 {full_c:>+8.1f}%{diff_str:>11}')

# paired diff Top 3 vs Top N
print()
print('paired diff (vs Top 3):')
top3_cums = results[3][0]
for top_n in [5, 7, 10, 15, 20, 30]:
    diffs = [a-b for a, b in zip(results[top_n][0], top3_cums)]
    avg_d = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  Top {top_n}: avg {avg_d:+.1f}p, wins {wins}/{len(diffs)}')

# LOWO
print()
print('=' * 100)
print('LOWO (단일/다중 winner 제외)')
print('=' * 100)
print(f'{"제외":<18}', end='')
for top_n in [3, 5, 10, 20]:
    print(f'{"Top "+str(top_n):>13}', end='')
print()
for excl in [(), ('SNDK',), ('MU',), ('SNDK','MU')]:
    name = '(없음)' if not excl else '-' + '/'.join(excl)
    print(f'{name:<18}', end='')
    for top_n in [3, 5, 10, 20]:
        cums = run(top_n, True, excl)[0]
        print(f'{statistics.mean(cums):>+12.1f}%', end='')
    print()

# Walk-forward
print()
print('=' * 100)
print('Walk-forward (5 블록)')
print('=' * 100)
n_blocks = 5
block_size = (len(dates) - MIN_HOLD) // n_blocks
print(f'{"블록":<8}{"시작일":<14}', end='')
for top_n in [3, 5, 10, 20]:
    print(f'{"Top "+str(top_n):>13}', end='')
print()
for b in range(n_blocks):
    start_i = 2 + b * block_size
    if start_i >= len(dates) - MIN_HOLD: break
    print(f'{b+1:<8}{dates[start_i]:<14}', end='')
    for top_n in [3, 5, 10, 20]:
        c, _ = sim(top_n, True, start=start_i)
        print(f'{c:>+12.1f}%', end='')
    print()

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
