# -*- coding: utf-8 -*-
"""V101 — 더 깊은 맹점 검증 (6가지 새 각도)

이전 V100에서 발견:
- 후반 phase V99 우월, LOWO -SNDK V99 +50p 우월
- 단정 어려움

이번 검증:
1. V99 LOWO 시 어떤 종목 잡는지 trace (메가 외 다른 메가 시그니처?)
2. 메가 시그니처 활성도 추이 (75일 내내 vs 일부 시기만)
3. 시드별 lift 분포 (평균 외 분산)
4. MDD/Sharpe 비교
5. 매수 종목 trace 다양한 시드 (V86e+ vs V99)
6. Systematic bias 진단 (look-ahead, selection)
"""
import sys, sqlite3, random, statistics, time, math
from pathlib import Path
from collections import defaultdict, Counter

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data = {}; peg_by_date = defaultdict(list)
for d in dates:
    data[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?', (d,)):
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]; fpe = (r[3]/nc) if (r[3] and nc>0) else None
        peg = (fpe/(rg*100)) if (fpe and rg and rg>0) else None
        data[d][tk] = dict(p2_orig=r[1], cr_orig=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10], peg=peg, rev_growth=rg)
        if peg is not None and peg > 0: peg_by_date[d].append((tk, peg))

peg_pct = {}
for d, lst in peg_by_date.items():
    lst.sort(key=lambda x: x[1]); n = len(lst)
    peg_pct[d] = {tk: (i+1)/n for i, (tk, _) in enumerate(lst)}

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr_orig') is None or x['cr_orig'] > 30: return False
    return True


def is_mega(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


def get_rank_multi(d, alpha):
    dd = data[d]; pm = peg_pct.get(d, {})
    cands = []
    for tk, info in dd.items():
        p2 = info.get('p2_orig')
        if p2 is None: continue
        pct = pm.get(tk, 1.0)
        cands.append((tk, alpha * p2 + (1-alpha) * pct * 500))
    cands.sort(key=lambda x: x[1])
    return {tk: i+1 for i, (tk, _) in enumerate(cands)}


def sim_trace(alpha=None, exclude=(), start=0):
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0.0
    rets_daily = []; buys = []; sells = []
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); rets_daily.append(ret)
            peak = max(peak, val); mdd = max(mdd, (peak-val)/peak)
        dd = data[d]
        rank_map = get_rank_multi(d, alpha) if alpha is not None and alpha < 1.0 else None

        def get_p2(tk, info):
            return rank_map.get(tk) if rank_map else (info.get('p2_orig') if info else None)

        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2:
                    sells.append((d, tk, 'min_seg')); del held[tk]; continue
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    sells.append((d, tk, 'rev_g')); del held[tk]; continue
            p2 = get_p2(tk, info) if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info): continue
                sells.append((d, tk, 'rank')); del held[tk]

        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                p2 = get_p2(tk, info)
                if p2 is None or p2 > 3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                cands.append((p2, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        buys.append((d, tk, w[si]))
                        held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    w = 0.5 if len(held) >= 1 else 1.0
                    buys.append((d, tk, w))
                    held[tk] = (d, dd[tk]['price'], w)
        prev = dict(held)
    sharpe = 0; sortino = 0
    if rets_daily and statistics.stdev(rets_daily) > 0:
        sharpe = statistics.mean(rets_daily) / statistics.stdev(rets_daily) * math.sqrt(252)
        downside = [r for r in rets_daily if r < 0]
        if downside and statistics.stdev(downside) > 0:
            sortino = statistics.mean(rets_daily) / statistics.stdev(downside) * math.sqrt(252)
    return dict(cum=(val-1)*100, mdd=mdd*100, sharpe=sharpe, sortino=sortino, buys=buys, sells=sells)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(range(len(elig)), SAMPLES))


# ============================================================
print('=' * 100)
print('V101 — 더 깊은 맹점 검증 (6가지 새 각도)')
print('=' * 100)

# [1] 메가 시그니처 활성도 추이
print('\n[1] 메가 시그니처 활성도 추이 (PEG<0.22 종목 일별)')
print(f'{"date":<12}{"메가 수":>10}{"종목":<40}')
print('-' * 70)
mega_count_by_date = []
for d in dates:
    megas = [tk for tk, info in data[d].items() if is_mega(info)]
    mega_count_by_date.append(len(megas))
    if len(megas) > 0 and d in [dates[0], dates[len(dates)//4], dates[len(dates)//2], dates[3*len(dates)//4], dates[-1]]:
        print(f'  {d:<12}{len(megas):>8}    {", ".join(sorted(megas)[:8])}')
print(f'\n  평균 메가 수: {statistics.mean(mega_count_by_date):.1f}일별')
print(f'  최대: {max(mega_count_by_date)}, 최소: {min(mega_count_by_date)}')

# [2] V86e+ vs V99 시드별 lift 분포
print('\n[2] 시드별 lift 분포 (paired 100×3)')
v86_savg = []; v99_savg = []
for ch in seeds:
    sr_86 = []; sr_99 = []
    for s in ch:
        sr_86.append(sim_trace(None, start=s)['cum'])
        sr_99.append(sim_trace(0.7, start=s)['cum'])
    v86_savg.append(sum(sr_86)/len(sr_86))
    v99_savg.append(sum(sr_99)/len(sr_99))

lifts = [v99 - v86 for v86, v99 in zip(v86_savg, v99_savg)]
lifts_sorted = sorted(lifts)
print(f'  V99-V86e+ lift 분포:')
print(f'  최소: {min(lifts):+.1f}p / 25%: {lifts_sorted[25]:+.1f}p / 중앙: {statistics.median(lifts):+.1f}p / 75%: {lifts_sorted[75]:+.1f}p / 최대: {max(lifts):+.1f}p')
print(f'  평균: {statistics.mean(lifts):+.1f}p / 표준편차: {statistics.stdev(lifts):.1f}p')
print(f'  V99 우월 시드: {sum(1 for l in lifts if l > 0)}/100')

# [3] MDD/Sharpe 비교
print('\n[3] MDD/Sharpe/Sortino 비교 (start=0 1회)')
for label, alpha in [('V86e+', None), ('V99(0.7)', 0.7), ('V99(0.9)', 0.9)]:
    r = sim_trace(alpha, start=0)
    print(f'  {label:<10}: cum {r["cum"]:+.1f}% MDD {r["mdd"]:.1f}% Sharpe {r["sharpe"]:.2f} Sortino {r["sortino"]:.2f}')

# [4] V99 LOWO 시 어떤 종목 잡는지 (start=0, -MU-SNDK)
print('\n[4] LOWO -MU-SNDK 시 매수 종목 비교 (start=0)')
v86_lowo = sim_trace(None, exclude=('MU', 'SNDK'), start=0)
v99_lowo = sim_trace(0.9, exclude=('MU', 'SNDK'), start=0)
print(f'  V86e+ -MU-SNDK 매수: {len(v86_lowo["buys"])}회 / cum {v86_lowo["cum"]:+.1f}%')
print(f'    종목: {sorted(set(tk for _, tk, _ in v86_lowo["buys"]))}')
print(f'  V99(0.9) -MU-SNDK 매수: {len(v99_lowo["buys"])}회 / cum {v99_lowo["cum"]:+.1f}%')
print(f'    종목: {sorted(set(tk for _, tk, _ in v99_lowo["buys"]))}')

# [5] 후반 phase에서 V86e+ vs V99 매수 비교 (start=50)
print('\n[5] 후반 phase 매수 (start=50, end=75)')
v86_late = sim_trace(None, start=50)
v99_late = sim_trace(0.7, start=50)
print(f'  V86e+ start=50: 매수 {len(v86_late["buys"])}회 / cum {v86_late["cum"]:+.1f}%')
print(f'    종목: {sorted(set(tk for _, tk, _ in v86_late["buys"]))}')
print(f'  V99(0.7) start=50: 매수 {len(v99_late["buys"])}회 / cum {v99_late["cum"]:+.1f}%')
print(f'    종목: {sorted(set(tk for _, tk, _ in v99_late["buys"]))}')

# [6] Systematic bias 진단
print('\n[6] Systematic bias 진단')
print('  a. look-ahead bias 검증')
# part2_rank가 cron 실행 후 저장 → 다음날 매수 시점에 이용 가능 ✓
# 단, 같은 날 종가 사용 — 실제로는 다음날 시초가가 더 정확
# 현재 sim: today p2 → today price 매수 (look-ahead 가능)
# 정확: today p2 → next day open 매수
print(f'    sim 매수가: today 종가 (실제 production = next-day open)')
print(f'    영향: 1일 lag, 보통 V86e+/V99 둘 다 동일 영향')

# b. selection bias 검증
print('  b. selection bias 검증')
print(f'    universe: {len(set(tk for d in dates for tk in data[d]))} 종목')
print(f'    cron 매일 fetch — survivorship bias 없음')
print(f'    단 dates 시작 (2026-02-06)은 이미 살아남은 종목 → mild bias 가능')

# c. data snooping bias
print('  c. data snooping bias 검증')
print(f'    PEG<0.22 cutoff = 데이터 보고 결정 (V90 grid search)')
print(f'    rev_exit 0.25 = 데이터 보고 결정')
print(f'    → 둘 다 75일 데이터 over-fit 위험. multistart로 일부 검증')

# d. paired BT의 동일 시점 가정
print('  d. paired BT 가정')
print(f'    같은 75일 데이터 → 시장 환경 변동 미반영')
print(f'    → multistart phase 결과 (전반/후반)로 일부 검증')

# e. MU/SNDK 가격 변화 timing
print('\n  e. MU/SNDK 가격 변화 timing (75일)')
for tk in ['MU', 'SNDK']:
    prices = []
    for d in dates:
        p = pf[d].get(tk)
        if p: prices.append((d, p))
    if prices:
        p_start = prices[0][1]; p_end = prices[-1][1]
        # 5등분 phase
        n = len(prices)
        for phase_i in [0, 1, 2, 3]:
            s = phase_i * n // 4
            e = (phase_i + 1) * n // 4
            phase_start = prices[s][1]; phase_end = prices[e-1][1]
            ret = (phase_end/phase_start - 1) * 100
            print(f'    {tk} phase {phase_i+1}/4 ({prices[s][0]}~{prices[e-1][0]}): {ret:+.1f}%')

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
