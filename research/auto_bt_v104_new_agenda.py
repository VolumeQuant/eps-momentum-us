# -*- coding: utf-8 -*-
"""V104 — 새 시스템 아젠다 BT

사용자 결단: 시스템 아젠다 변경
- 기존: 가격 vs PE 변화율 괴리 (mean reversion)
- 신규: 강력한 매출/EPS/PEG 종목 중 저평가된 종목 선택

비교 BT:
- v84_pure: mean reversion only (메가 carryover X)
- v86e+: mean reversion + 메가 carryover (현재)
- v104_mega: mega_score만 (매출+NTM+PEG_inv) — 새 아젠다
- v104_blend: blend 0.5 (절반)

전체 검증:
1. full period (start=0, 76일)
2. multistart (100 seeds × 3 samples)
3. percentile 분포
4. paired diff
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100
SAMPLES = 3
MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
print(f'전체 거래일: {len(dates)} ({dates[0]} ~ {dates[-1]})')

data = {}
for d in dates:
    data[d] = {}
    rows = cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?', (d,)).fetchall()
    for r in rows:
        tk = r[0]
        nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]
        fpe = (r[3]/nc) if (r[3] and nc>0) else None
        peg = (fpe/(rg*100)) if (fpe and rg and rg>0) else None
        ntm_rev_pct = (nc/n90-1)*100 if (nc and n90 and n90>0) else 0
        rg_pct = rg*100 if rg else 0
        peg_inv = 1/peg if peg and peg > 0 else 0
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, ntm_rev=ntm_rev_pct,
                           rg_pct=rg_pct, peg_inv=peg_inv)

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True


def is_mega(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


def sim(variant, start=0, exclude=()):
    held = {}
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
    n_buys = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]
            ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk)
                pn = pf[d].get(tk, pp)
                if pp and pn:
                    ret += w * (pn/pp - 1)
            val *= (1+ret)
            peak = max(peak, val)
            mdd = max(mdd, (peak-val)/peak)
        dd = data[d]

        # rank 결정
        if variant.startswith('v104'):
            blend = 0.0 if variant == 'v104_mega' else 0.5
            cands_p2 = []
            cands_mega = []
            for tk, info in dd.items():
                p2 = info.get('p2')
                if p2 is None: continue
                cands_p2.append((tk, p2))
                ms = info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)
                cands_mega.append((tk, ms))
            p2_rank = {tk: r for tk, r in cands_p2}
            cands_mega.sort(key=lambda x: -x[1])
            mega_rank = {tk: ii+1 for ii, (tk, _) in enumerate(cands_mega)}
            combined = {tk: blend*p2_rank[tk] + (1-blend)*mega_rank.get(tk, 999) for tk in p2_rank}
            cs = sorted(combined.items(), key=lambda x: x[1])
            rank_map = {tk: ii+1 for ii, (tk, _) in enumerate(cs)}
        else:
            rank_map = None

        def get_rank(tk, info):
            if rank_map: return rank_map.get(tk)
            return info.get('p2') if info else None

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2:
                    del held[tk]; continue
                if variant == 'v86e+' and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            r = get_rank(tk, info) if info else None
            if info is None or r is None or r > 10:
                if variant == 'v86e+' and is_mega(info):
                    continue
                del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                r = get_rank(tk, info)
                if r is None or r > 3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25:
                    continue
                cands.append((r, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1-s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        held[tk] = (d, dd[tk]['price'], w[si])
                        n_buys += 1
            else:
                for _, _, tk in pick:
                    w = 0.5 if len(held) >= 1 else 1.0
                    held[tk] = (d, dd[tk]['price'], w)
                    n_buys += 1
        prev = dict(held)
    return dict(cum=(val-1)*100, mdd=mdd*100, n_buys=n_buys)


# 1. Full period BT
print('\n[1] Full period BT (start=0, 76일 전체)')
print('variant       cum       mdd       매수')
print('-' * 50)
results_full = {}
for v in ['v84_pure', 'v86e+', 'v104_mega', 'v104_blend']:
    r = sim(v, start=0)
    results_full[v] = r
    print(f'{v:<14} {r["cum"]:+8.1f}% {r["mdd"]:7.1f}% {r["n_buys"]:>6}')

# 2. Multistart 100×3
print('\n[2] Multistart (100 seeds × 3 samples = 300 시뮬)')
elig = list(range(2, len(dates) - MIN_HOLD))
print(f'시작점 풀: {len(elig)}개')
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(elig, SAMPLES))

results_ms = {'v84_pure': [], 'v86e+': [], 'v104_mega': [], 'v104_blend': []}
for v in results_ms:
    for ch in seeds:
        for s in ch:
            r = sim(v, start=s)
            results_ms[v].append(r['cum'])

print('\nvariant       평균       중앙       25%        75%        최소       최대')
print('-' * 80)
for v, vals in results_ms.items():
    s = sorted(vals)
    n = len(s)
    avg = statistics.mean(vals)
    p25 = s[n//4]; med = statistics.median(vals); p75 = s[3*n//4]
    print(f'{v:<14} {avg:+8.1f}% {med:+8.1f}% {p25:+8.1f}% {p75:+8.1f}% {min(vals):+8.1f}% {max(vals):+8.1f}%')

# 3. Paired diff
print('\n[3] Paired diff')
print('비교 (paired)        avg lift   wins/300')
print('-' * 50)
v86 = results_ms['v86e+']
v84 = results_ms['v84_pure']
for ref_name, ref_vals in [('v86e+', v86), ('v84_pure', v84)]:
    for v in ['v84_pure', 'v86e+', 'v104_mega', 'v104_blend']:
        if v == ref_name: continue
        diffs = [a - b for a, b in zip(results_ms[v], ref_vals)]
        avg = statistics.mean(diffs)
        wins = sum(1 for d in diffs if d > 0)
        print(f'{v} vs {ref_name:<10} {avg:+7.1f}p {wins:>6}/300')

# 4. v86e+ vs v84 (메가 carryover 효과)
print('\n[4] 메가 carryover 진짜 효과 (v86e+ vs v84)')
diffs = [a - b for a, b in zip(results_ms['v86e+'], v84)]
print(f'  v86e+ - v84: avg {statistics.mean(diffs):+.1f}p, wins {sum(1 for d in diffs if d > 0)}/300')

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
