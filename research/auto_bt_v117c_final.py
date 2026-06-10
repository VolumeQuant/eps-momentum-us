# -*- coding: utf-8 -*-
"""V117c 최종 검증 — Top 3 + $1B+ vs Top 3 (no vol) 정밀 BT

DB.part2_rank 정렬 기준 (v117c production 정합).
random/multistart/paired/LOWO/walk-forward/인접 안정성 전수 검증.
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
        data_all[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                               min_seg=min(segs) if segs else 0, high30=r[10], rev_growth=r[11], dv=r[12])
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
    prices = [pf[dates[j]].get(tk) for j in range(max(0, i-11), i+1) if pf[dates[j]].get(tk)]
    if len(prices) < 6: return True
    return (pf[dates[i]].get(tk) or 0) > sum(prices)/len(prices)


def today_gap(tk, i):
    if i < 1: return 0
    cur_p = pf[dates[i]].get(tk); prev_p = pf[dates[i-1]].get(tk)
    if not cur_p or not prev_p: return 0
    return cur_p / prev_p - 1


def sim(vol_filter, exclude=(), start=0):
    """vol_filter: 0 (no filter) or 1000 ($1B+). Top 3 한정 공통."""
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
                if today_gap(tk, i) <= WHIPSAW_GAP and not grace:
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
                if p2 is None or p2 > 3: continue
                if vol_filter > 0:
                    dv = info.get('dv') or 0
                    if dv < vol_filter: continue
                cands.append((p2, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                for _, _, tk in pick[:2]: held[tk] = (d, dd[tk]['price'], 0.5, False)
            elif len(held) == 0 and len(pick) == 1:
                held[pick[0][2]] = (d, dd[pick[0][2]]['price'], 1.0, False)
            else:
                for _, _, tk in pick: held[tk] = (d, dd[tk]['price'], 0.5, False)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))

def run(vol_filter, exclude=()):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim(vol_filter, exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds

print('=' * 90)
print('v117c 최종 검증: Top 3 + $1B+ vs Top 3 (no vol filter)')
print('=' * 90)
print(f'기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)')
print()

# 1. Random multistart 100×3
print('[1] Random multistart (100×3 paired)')
print(f'{"variant":<22}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}')
print('-' * 75)
bc, bm = run(0)
b_avg = statistics.mean(bc); b_mdd = statistics.mean(bm)
b_cal = b_avg/abs(b_mdd) if b_mdd else 0
b_pos = sum(1 for c in bc if c > 0)
full_b, _ = sim(0, start=0)
print(f'Top 3 (no vol)         {b_avg:>+9.1f}%{b_mdd:>+9.1f}%{b_cal:>8.2f}{b_pos:>6}/300 {full_b:>+8.1f}%')

vc, vm = run(1000)
v_avg = statistics.mean(vc); v_mdd = statistics.mean(vm)
v_cal = v_avg/abs(v_mdd) if v_mdd else 0
v_pos = sum(1 for c in vc if c > 0)
full_v, _ = sim(1000, start=0)
print(f'Top 3 + $1B+           {v_avg:>+9.1f}%{v_mdd:>+9.1f}%{v_cal:>8.2f}{v_pos:>6}/300 {full_v:>+8.1f}%')

diffs = [v-b for v, b in zip(vc, bc)]
print(f'\n  $1B+ 효과: 평균 {statistics.mean(diffs):+.1f}p, wins {sum(1 for d in diffs if d > 0)}/{len(diffs)}, ties {sum(1 for d in diffs if d == 0)}/{len(diffs)}')

# 2. LOWO
print()
print('[2] LOWO (winner 제외)')
print(f'{"제외":<18}{"no vol":>12}{"$1B+":>11}{"diff":>10}{"양수($1B)":>12}')
print('-' * 65)
for excl in [(), ('SNDK',), ('MU',), ('STX',), ('LITE',), ('SNDK','MU'), ('SNDK','MU','STX')]:
    name = '(없음)' if not excl else '-' + '/'.join(excl)
    bc_e, _ = run(0, excl); vc_e, _ = run(1000, excl)
    b = statistics.mean(bc_e); v = statistics.mean(vc_e)
    pos = sum(1 for c in vc_e if c > 0)
    print(f'{name:<18}{b:>+10.1f}%{v:>+10.1f}%{v-b:>+9.1f}p{pos:>8}/300')

# 3. Walk-forward
print()
print('[3] Walk-forward (5 블록)')
n_blocks = 5
block_size = (len(dates) - MIN_HOLD) // n_blocks
print(f'{"블록":<8}{"시작일":<14}{"no vol":>12}{"$1B+":>11}{"diff":>10}')
v_wins = 0
for b in range(n_blocks):
    si = 2 + b * block_size
    if si >= len(dates) - MIN_HOLD: break
    bc_w, _ = sim(0, start=si); vc_w, _ = sim(1000, start=si)
    diff = vc_w - bc_w
    if diff > 0: v_wins += 1
    print(f'{b+1:<8}{dates[si]:<14}{bc_w:>+10.1f}%{vc_w:>+10.1f}%{diff:>+9.1f}p')
print(f'  $1B+ wins: {v_wins}/{n_blocks}')

# 4. 인접 안정성
print()
print('[4] 인접 안정성 (Top 3 + threshold sweep)')
print(f'{"threshold":<14}{"수익":>10}{"MDD":>9}{"calmar":>8}{"vs no vol":>12}')
for thr in [500, 700, 1000, 1200, 1500, 2000]:
    cums, mdds = run(thr)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg/abs(mdd) if mdd else 0
    print(f'${thr}M+{"":<8}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{avg-b_avg:>+11.1f}p')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
