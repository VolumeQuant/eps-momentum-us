# -*- coding: utf-8 -*-
"""V112 — eligible 무관 메가 carryover

V86e+/V110 limitation 발견:
- MU 05-08부터 composite_rank 17~67 (Part 2 풀 제외)
- 메가 시그니처 (PEG 0.04, 매출 196%) 유지 but carryover X
- 05-28 매도 (+0%)

V112 새 logic:
- carryover 조건: prev_held + 메가 시그니처 (PEG<0.25 + 매출≥25% + min_seg≥-2 + rev_g≥0.25)
- composite_rank/Part 2 풀 무관
- 매도 트리거: PEG≥0.25 / 매출<25% / min_seg<-2 / 가격 데이터 X

비교:
- v86e+: Part 2 풀 안 종목만 carryover
- v112: 메가 시그니처 유지 시 carryover (Part 2 풀 무관)
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

import time
t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

# 모든 종목 데이터 (eligible 무관)
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


def is_mega_strict(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


def is_mega_v112(info, peg_thr=0.25):
    """eligible 무관 메가 시그니처"""
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    if info.get('min_seg', 0) < -2: return False
    return True


def sim(variant, exclude=(), start=0):
    """variant: v86e+, v112"""
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

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is None:
                # 데이터 없으면 매도 (수집 실패 등)
                del held[tk]; continue
            if info.get('min_seg', 0) < -2:
                del held[tk]; continue

            if variant == 'v112':
                # V112: 메가 시그니처 유지 시 holding (composite_rank 무관)
                if is_mega_v112(info):
                    if info.get('rev_growth') and info['rev_growth'] < 0.25:
                        del held[tk]; continue
                    continue  # holding 유지
                else:
                    # 메가 시그니처 해제 — Part 2 logic
                    p2 = info.get('p2')
                    if p2 is None or p2 > 10:
                        del held[tk]
            else:  # v86e+
                if is_mega_strict(info) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
                p2 = info.get('p2')
                if p2 is None or p2 > 10:
                    if is_mega_strict(info): continue
                    del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info.get('p2') is None or info['p2'] > 3: continue
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


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(variant, exclude=()):
    cums = []
    for ch in seeds:
        for s in ch:
            cums.append(sim(variant, exclude=exclude, start=s))
    return cums


print('=' * 100)
print('V112 — eligible 무관 메가 carryover (MU 매도 fix)')
print('=' * 100)

# 1. start=0 full BT
print('\n[1] Full period (start=0)')
for v in ['v86e+', 'v112']:
    r = sim(v, start=0)
    print(f'  {v}: cum {r:+.1f}%')

# 2. Multistart
print('\n[2] Multistart 100×3')
exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')),
              ('-all5', ('SNDK', 'MU', 'BE', 'LITE', 'TTMI'))]
print(f'{"variant":<10}' + ''.join(f'{n:>11}' for n, _ in exclusions))
print('-' * 80)
for v in ['v86e+', 'v112']:
    avgs = {n: statistics.mean(run(v, ex)) for n, ex in exclusions}
    print(f'{v:<10}' + ''.join(f'{avgs[n]:>+10.1f}%' for n, _ in exclusions))

# 3. paired diff
print('\n[3] V112 vs v86e+ paired diff')
for n, ex in exclusions:
    cums_v86 = run('v86e+', ex)
    cums_v112 = run('v112', ex)
    diffs = [a-b for a, b in zip(cums_v112, cums_v86)]
    avg = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {n}: {avg:+.1f}p ({wins}/{len(diffs)})')

# 4. V112 매매 내역 (start=0)
print('\n[4] V112 매매 trace (start=0)')
held = {}; prev = None; val = 1.0; trades = []
for i in range(2, len(dates)):
    d = dates[i]
    if prev and i > 2:
        dp = dates[i-1]; ret = 0
        for tk, (ed, ep, w) in prev.items():
            pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
            if pp and pn: ret += w * (pn/pp - 1)
        val *= (1+ret)
    dd = data_all[d]
    for tk in list(held):
        info = dd.get(tk); ed, ep, w = held[tk]
        sell_reason = None
        if info is None:
            sell_reason = 'no_data'
        elif info.get('min_seg', 0) < -2:
            sell_reason = 'min_seg<-2'
        elif is_mega_v112(info):
            if info.get('rev_growth') and info['rev_growth'] < 0.25:
                sell_reason = 'mega rev<25%'
            # 메가 유지 — holding
        else:
            p2 = info.get('p2')
            if p2 is None or p2 > 10:
                sell_reason = f'mega 해제 + p2={p2}'
        if sell_reason:
            cp = (info.get('price') if info else None) or pf[d].get(tk, ep)
            trades.append((d, 'SELL', tk, cp, (cp/ep-1)*100, sell_reason))
            del held[tk]
    if len(held) < 2:
        cands = []
        for tk, info in dd.items():
            if info.get('p2') is None or info['p2'] > 3: continue
            if tk in held: continue
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
                if w[si] > 0:
                    held[tk] = (d, dd[tk]['price'], w[si])
                    trades.append((d, 'BUY', tk, dd[tk]['price'], 0, 'rank<=3'))
        else:
            for _, _, tk in pick:
                w_val = 0.5 if len(held) >= 1 else 1.0
                held[tk] = (d, dd[tk]['price'], w_val)
                trades.append((d, 'BUY', tk, dd[tk]['price'], 0, 'rank<=3'))
    prev = dict(held)

print(f'\n총 {len(trades)}건')
print(f'{"#":>3} {"일자":<12} {"a":<5} {"종목":<8} {"가격":>10} {"수익":>10}  사유')
for idx, t in enumerate(trades, 1):
    d, a, tk, p, ret, r = t
    ret_s = f'{ret:+.1f}%' if a == 'SELL' else '-'
    print(f'{idx:>3} {d:<12} {a:<5} {tk:<8} ${p:>8.2f} {ret_s:>9}  {r}')

print(f'\n현재 보유:')
last_p = pf[dates[-1]]
for tk, (ed, ep, w) in held.items():
    cp = last_p.get(tk, ep); ret = (cp/ep-1)*100
    print(f'  {tk} {int(w*100)}% : {ed} ${ep:.2f} → ${cp:.2f} ({ret:+.1f}%)')

print(f'\nV112 누적: {(val-1)*100:+.1f}%')
print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
