# -*- coding: utf-8 -*-
"""옵션 A2 — soft volume penalty/score blend BT

variants:
- A1 (hard filter, baseline): 거래량 <$1B 차단 (v117 현재 production)
- A2a: rank penalty — 거래량 미달 종목 순위 +N (N: 5, 10, 20)
- A2b: score blend — combined = α × part2_pct + (1-α) × vol_pct
- A2c: log boost — score × (1 + log10(vol/1B))
- A2d: tiered penalty — $1B+ 정상, $500M~1B +5, <$500M hard

전부 v114 (MA12 추세홀드 + 휩쏘) 위에서 BT.
"""
import sys, sqlite3, random, statistics, time, math
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10
WHIPSAW_GAP = -0.10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

# 모든 데이터 로드 (DB의 dollar_volume_30d 사용 — backfill 완료)
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


def get_buy_rank(tk, info, variant, alpha=0.7, penalty_N=10):
    """variant에 따라 매수 순위 score 결정. 낮을수록 우선."""
    p2 = info.get('p2', 999)
    dv = info.get('dv', 0) or 0
    if variant == 'A1':  # hard filter
        if dv < 1000:
            return 9999  # 차단
        return p2
    elif variant == 'A2a':  # rank penalty
        if dv < 1000:
            return p2 + penalty_N
        return p2
    elif variant == 'A2b':  # score blend (낮을수록 좋음)
        # part2_rank: 1위 best, 30위 worst (정규화 0~1, 1위=0, 30위=1)
        p2_norm = (p2 - 1) / 29 if p2 <= 30 else 1
        # volume: 큰 게 best (0~1, $10B=0, $0=1)
        vol_norm = max(0, 1 - math.log10(max(dv, 1) / 100) / 2)  # $100M=1, $10B=0
        return alpha * p2_norm + (1 - alpha) * vol_norm
    elif variant == 'A2c':  # log boost
        # score = p2 - log boost (낮을수록 우선)
        boost = math.log10(max(dv, 1) / 1000)  # $1B=0, $10B=+1
        return p2 - boost * 2  # boost 2배 weight
    elif variant == 'A2d':  # tiered
        if dv >= 1000:
            return p2
        elif dv >= 500:
            return p2 + 5
        else:
            return 9999  # 차단
    else:
        return p2


def sim_v114(variant, alpha=0.7, penalty_N=10, start=0):
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
        # 매도
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
        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if tk in held: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is None or p2 > 3: continue
                rank_score = get_buy_rank(tk, info, variant, alpha, penalty_N)
                if rank_score >= 9999: continue  # 차단
                cands.append((rank_score, info['score'], tk))
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

def run(variant, **kwargs):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim_v114(variant, start=s, **kwargs)
            cums.append(c); mdds.append(m)
    return cums, mdds


def report(name, variant, **kwargs):
    cums, mdds = run(variant, **kwargs)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg / abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    full_c, full_m = sim_v114(variant, start=0, **kwargs)
    print(f'{name:<24}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{pos:>6}/300 {full_c:>+8.1f}%')
    return avg, cums

print('\n' + '=' * 95)
print('옵션 A2 — soft volume penalty BT')
print('=' * 95)
print(f'{"variant":<24}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}')
print('-' * 80)

# baseline (no filter)
report('baseline (no filter)', 'none')
# A1 (v117 current)
report('A1 hard filter $1B+', 'A1')
# A2a rank penalty
report('A2a penalty +5', 'A2a', penalty_N=5)
report('A2a penalty +10', 'A2a', penalty_N=10)
report('A2a penalty +20', 'A2a', penalty_N=20)
# A2b blend
report('A2b blend a=0.5', 'A2b', alpha=0.5)
report('A2b blend a=0.7', 'A2b', alpha=0.7)
report('A2b blend a=0.8', 'A2b', alpha=0.8)
report('A2b blend a=0.9', 'A2b', alpha=0.9)
# A2c log boost
report('A2c log boost', 'A2c')
# A2d tiered
report('A2d tiered', 'A2d')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
