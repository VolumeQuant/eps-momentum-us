# -*- coding: utf-8 -*-
"""V117 거래량 필터 — LOWO + walk-forward 검증

LOWO (Leave-One-Winner-Out):
- 슈퍼 winner (SNDK/MU/STX/LITE/TTMI) 한 종목씩 매수 제외
- 알파 유지면 = 단일 종목 의존 아님 (broad)

Walk-forward:
- 80일 → 5분할 (각 16일 블록)
- 4분할 사용 (앞쪽 블록부터 → 뒤쪽으로 진행)
- 각 블록 시작점에서 sim
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


def sim(volume_filter, exclude=(), start=0, end=None):
    """volume_filter: 0 (baseline) or 1000 ($1B+)"""
    if end is None:
        end = len(dates)
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
    for i in range(start, end):
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
                if p2 is None or p2 > 3: continue
                if volume_filter > 0:
                    dv = info.get('dv') or 0
                    if dv < volume_filter: continue
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


def run(volume_filter, exclude=()):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim(volume_filter, exclude=exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


# 1. LOWO 검증
print('=' * 95)
print('1) LOWO (Leave-One-Winner-Out) 검증 — 슈퍼 winner 제외 시 알파 유지?')
print('=' * 95)
print(f'{"제외 종목":<20}{"baseline":>12}{"V117 $1B+":>14}{"diff":>10}{"양수":>11}')
print('-' * 75)
for exclusion in [(), ('SNDK',), ('MU',), ('STX',), ('LITE',), ('TTMI',),
                  ('SNDK', 'MU'), ('SNDK', 'MU', 'STX')]:
    name = '(없음)' if not exclusion else '-' + '/'.join(exclusion)
    bc, bm = run(0, exclusion)
    vc, vm = run(1000, exclusion)
    b_avg = statistics.mean(bc); v_avg = statistics.mean(vc)
    pos_v = sum(1 for c in vc if c > 0)
    diff = v_avg - b_avg
    print(f'{name:<20}{b_avg:>+10.1f}%{v_avg:>+12.1f}%{diff:>+9.1f}p{pos_v:>7}/300')

# 2. Walk-forward 검증
print()
print('=' * 95)
print('2) Walk-forward 검증 — 80일 5분할 → 각 블록 시작점에서 시뮬')
print('=' * 95)
n_blocks = 5
block_size = (len(dates) - MIN_HOLD) // n_blocks
print(f'각 블록 크기: ~{block_size}일')
print(f'{"블록":<8}{"시작일":<14}{"baseline":>12}{"V117 $1B+":>14}{"diff":>10}')
print('-' * 65)
total_b = 0; total_v = 0; v_wins = 0
for b in range(n_blocks):
    start_i = 2 + b * block_size
    if start_i >= len(dates) - MIN_HOLD:
        break
    start_d = dates[start_i]
    bc, _ = sim(0, start=start_i)
    vc, _ = sim(1000, start=start_i)
    diff = vc - bc
    total_b += bc; total_v += vc
    if diff > 0: v_wins += 1
    print(f'{b+1:<8}{start_d:<14}{bc:>+10.1f}%{vc:>+12.1f}%{diff:>+9.1f}p')
print('-' * 65)
print(f'합계 평균: baseline {total_b/n_blocks:+.1f}% / V117 {total_v/n_blocks:+.1f}% (V117 wins {v_wins}/{n_blocks})')

print(f'\n총 소요: {time.time()-t0:.0f}초')
con.close()
