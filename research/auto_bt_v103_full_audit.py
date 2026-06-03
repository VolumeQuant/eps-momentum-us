# -*- coding: utf-8 -*-
"""V103 full audit — start=0 전 기간 + multistart + percentile + KEYS 매수 시점 정밀

사용자 비판:
- "KEYS, VIRT 같은 종목 주가를 봐봐 수익을 주기는 했어?"
- "시스템 시작일부터 6/2까지 전부 모든날짜 대상으로 bt한거 맞아?"
- "랜덤 진입 테스트, 멀티진입테스트 이런거 다했어?"

확인:
1. full period (start=0 to end) BT
2. multistart 64개 시작점 평균
3. percentile (25/50/75/min/max)
4. KEYS/VIRT/FAF 매수 시점 진짜 수익
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
print(f'전체 거래일: {len(dates)} ({dates[0]} ~ {dates[-1]})')

data = {}
for d in dates:
    data[d] = {}
    for r in cur.execute('''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?''', (d,)):
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]; fpe = (r[3]/nc) if (r[3] and nc>0) else None
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
    """
    variant:
      'baseline': mean reversion only (V86e+ 전)
      'v86e+': V86e+ carryover (현재 production)
      'v103_07': mega_score blend 0.7
      'v103_05': mega_score blend 0.5
    """
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0.0
    buys = []; sells = []
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val); mdd = max(mdd, (peak-val)/peak)
        dd = data[d]

        # rank 결정
        if variant == 'v103_07' or variant == 'v103_05':
            blend = 0.7 if variant == 'v103_07' else 0.5
            cands_p2 = []; cands_mega = []
            for tk, info in dd.items():
                p2 = info.get('p2')
                if p2 is None: continue
                cands_p2.append((tk, p2))
                ms = info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)
                cands_mega.append((tk, ms))
            p2_rank = {tk: r for tk, r in cands_p2}
            cands_mega.sort(key=lambda x: -x[1])
            mega_rank = {tk: i+1 for i, (tk, _) in enumerate(cands_mega)}
            combined = {tk: blend*p2_rank[tk] + (1-blend)*mega_rank.get(tk, 999) for tk in p2_rank}
            cs = sorted(combined.items(), key=lambda x: x[1])
            rank_map = {tk: i+1 for i, (tk, _) in enumerate(cs)}
        else:
            rank_map = None

        def get_rank(tk, info):
            if rank_map: return rank_map.get(tk)
            return info.get('p2') if info else None

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; sells.append((d, tk, 'min_seg')); continue
                if variant == 'v86e+' and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; sells.append((d, tk, 'rev_g')); continue
            r = get_rank(tk, info) if info else None
            if info is None or r is None or r > 10:
                if variant == 'v86e+' and is_mega(info): continue  # carryover
                del held[tk]; sells.append((d, tk, 'rank'))

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
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                cands.append((r, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1-s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        held[tk] = (d, dd[tk]['price'], w[si])
                        buys.append((d, tk, w[si]))
            else:
                for _, _, tk in pick:
                    w = 0.5 if len(held) >= 1 else 1.0
                    held[tk] = (d, dd[tk]['price'], w)
                    buys.append((d, tk, w))
        prev = dict(held)
    return dict(cum=(val-1)*100, mdd=mdd*100, buys=buys, sells=sells)


print('\n[1] Full period BT (start=0, 76일 전체)')
print(f'{"variant":<12}{"cum":>10}{"mdd":>10}{"매수":>8}{"매도":>8}')
print('-' * 50)
for v in ['baseline', 'v86e+', 'v103_07', 'v103_05']:
    r = sim(v, start=0)
    print(f'{v:<12}{r[\"cum\"]:>+9.1f}%{r[\"mdd\"]:>9.1f}%{len(r[\"buys\"]):>8}{len(r[\"sells\"]):>8}')

# 매수 종목 trace (v86e+ full)
print('\n[2] V86e+ full BT (start=0) 매수 종목 + 보유 trace')
r86 = sim('v86e+', start=0)
print(f'전체 매수: {len(r86[\"buys\"])}회')
buy_tickers = {}
for d, tk, w in r86['buys']:
    if tk not in buy_tickers:
        buy_tickers[tk] = []
    buy_tickers[tk].append((d, w))
for tk in sorted(buy_tickers):
    entries = buy_tickers[tk]
    print(f'  {tk}: {len(entries)}회 진입 ({entries[0][0]}~)')

# 매도 종목 trace
print(f'\\n전체 매도: {len(r86[\"sells\"])}회')
sell_tickers = {}
for d, tk, reason in r86['sells']:
    if tk not in sell_tickers:
        sell_tickers[tk] = []
    sell_tickers[tk].append((d, reason))
for tk in sorted(sell_tickers):
    entries = sell_tickers[tk]
    reasons = [r for _, r in entries]
    print(f'  {tk}: {len(entries)}회 매도, 사유: {set(reasons)}')

print('\n[3] Multistart percentile (100 seeds × 3 samples = 300 시뮬)')
elig = list(range(2, len(dates) - MIN_HOLD))
print(f'시작점 풀: {len(elig)}개 (start=2 ~ start={len(dates)-MIN_HOLD-1})')
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))

results = {'baseline': [], 'v86e+': [], 'v103_07': [], 'v103_05': []}
for v in results:
    for ch in seeds:
        for s in ch:
            r = sim(v, start=s)
            results[v].append(r['cum'])

print(f'\\n{\"variant\":<12}{\"평균\":>10}{\"중앙\":>10}{\"25%\":>10}{\"75%\":>10}{\"최소\":>10}{\"최대\":>10}')
print('-' * 70)
for v, vals in results.items():
    s = sorted(vals)
    n = len(s)
    avg = statistics.mean(vals)
    p25 = s[n//4]; med = statistics.median(vals); p75 = s[3*n//4]
    print(f'{v:<12}{avg:>+9.1f}%{med:>+9.1f}%{p25:>+9.1f}%{p75:>+9.1f}%{min(vals):>+9.1f}%{max(vals):>+9.1f}%')

# paired diff (v86e+ vs others)
print(f'\\n[4] Paired diff (vs v86e+)')
v86 = results['v86e+']
for v in ['baseline', 'v103_07', 'v103_05']:
    diffs = [a - b for a, b in zip(results[v], v86)]
    avg = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {v} - v86e+: avg {avg:+.1f}p, wins {wins}/300')

print(f'\\n총 소요 {time.time()-t0:.0f}초')
con.close()
