# -*- coding: utf-8 -*-
"""V117 거래량 필터 — Top 3 vs Top 30 비교

옵션 A: part2_rank <= 3 + dv >= $1B (BT 기존, 엄격)
옵션 B: part2_rank <= 30 + dv >= $1B (production 현재, lenient)

비교: random/multi, LOWO, walk-forward
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


def sim(variant, exclude=(), start=0):
    """variant: 'baseline', 'A_top3', 'B_top30'"""
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
            if info and info.get('min_seg', 0) < -2:
                del held[tk]; continue
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
                if p2 is None: continue
                # variant 차이: rank cutoff
                if variant == 'A_top3':
                    if p2 > 3: continue
                elif variant == 'B_top30':
                    if p2 > 30: continue  # 사실상 cr Top 30 안 = candidates
                else:  # baseline (no volume filter)
                    if p2 > 3: continue
                # 거래량 필터 (baseline은 X)
                if variant != 'baseline':
                    dv = info.get('dv') or 0
                    if dv < 1000: continue
                cands.append((p2, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                for _, _, tk in pick[:2]:
                    held[tk] = (d, dd[tk]['price'], 0.5, False)
            elif len(held) == 0 and len(pick) == 1:
                tk = pick[0][2]
                held[tk] = (d, dd[tk]['price'], 1.0, False)
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5, False)
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
            c, m = sim(variant, exclude=exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


def report(name, variant):
    cums, mdds = run(variant)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg / abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    full_c, full_m = sim(variant, start=0)
    print(f'{name:<28}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{pos:>6}/300 {full_c:>+8.1f}%')
    return cums, mdds, avg


print('=' * 95)
print('1) Random multistart (100×3 paired)')
print('=' * 95)
print(f'{"variant":<28}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}')
print('-' * 80)
base_c, base_m, base_avg = report('baseline (no vol)', 'baseline')
a_c, a_m, a_avg = report('A: Top 3 + $1B+', 'A_top3')
b_c, b_m, b_avg = report('B: Top 30 + $1B+', 'B_top30')

# paired diff
print()
print('paired diff (vs baseline):')
for name, cums in [('A: Top 3', a_c), ('B: Top 30', b_c)]:
    diffs = [a-b for a, b in zip(cums, base_c)]
    avg_d = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {name}: avg {avg_d:+.1f}p, wins {wins}/{len(diffs)}')

# paired diff A vs B
diffs_ab = [a-b for a, b in zip(a_c, b_c)]
avg_ab = statistics.mean(diffs_ab); wins_ab = sum(1 for d in diffs_ab if d > 0)
print(f'  A vs B: avg {avg_ab:+.1f}p, A wins {wins_ab}/{len(diffs_ab)}')

print()
print('=' * 95)
print('2) LOWO (단일 winner 제외)')
print('=' * 95)
print(f'{"제외":<18}{"baseline":>12}{"A: Top 3":>14}{"B: Top 30":>14}')
print('-' * 60)
for excl in [(), ('SNDK',), ('MU',), ('SNDK', 'MU')]:
    name = '(없음)' if not excl else '-' + '/'.join(excl)
    bc = run('baseline', excl)[0]; ac = run('A_top3', excl)[0]; bbc = run('B_top30', excl)[0]
    print(f'{name:<18}{statistics.mean(bc):>+10.1f}%{statistics.mean(ac):>+12.1f}%{statistics.mean(bbc):>+12.1f}%')

print()
print('=' * 95)
print('3) Walk-forward (5 블록)')
print('=' * 95)
n_blocks = 5
block_size = (len(dates) - MIN_HOLD) // n_blocks
print(f'{"블록":<8}{"시작일":<14}{"baseline":>12}{"A: Top 3":>14}{"B: Top 30":>14}')
for b in range(n_blocks):
    start_i = 2 + b * block_size
    if start_i >= len(dates) - MIN_HOLD:
        break
    bc, _ = sim('baseline', start=start_i)
    ac, _ = sim('A_top3', start=start_i)
    bbc, _ = sim('B_top30', start=start_i)
    print(f'{b+1:<8}{dates[start_i]:<14}{bc:>+10.1f}%{ac:>+12.1f}%{bbc:>+12.1f}%')

print()
print('=' * 95)
print('4) 인접 안정성 ($500M, $700M, $1B, $1.5B, $2B) — option B (Top 30)')
print('=' * 95)
def sim_thr(thr, start=0):
    """B variant with custom threshold"""
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
                if tk in held: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is None or p2 > 30: continue
                dv = info.get('dv') or 0
                if dv < thr: continue
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

print(f'{"threshold":<14}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>9}')
for thr in [500, 700, 1000, 1500, 2000]:
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim_thr(thr, start=s)
            cums.append(c); mdds.append(m)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg / abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    print(f'${thr}M+{"":<8}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{pos:>6}/300')

print(f'\n총 소요: {time.time()-t0:.0f}초')
con.close()
