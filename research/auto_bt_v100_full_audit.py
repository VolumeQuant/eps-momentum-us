# -*- coding: utf-8 -*-
"""V100 — 종합 검증 (랜덤스타트, 멀티스타트, 인접안정성, 맹점 진단)

이전 BT 발견 맹점 1: dynamic weight (2step_t15) 적용 누락
이전 BT 발견 맹점 2: V99 결과가 sim 단순화로 distortion

이번 V100: 진짜 production-equivalent sim
1. dynamic weight (1·2위 score 차이 ≥15 → 100/0, else 50/50)
2. random 100×3 paired
3. 인접안정성 (PEG, rev_exit, α 동시 sweep)
4. multistart (전반/후반/중간)
5. 맹점 진단 (매수회수, 메가 보유일, 신규 차단 정량)
"""
import sys, sqlite3, random, statistics, time
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
    """multi-rank = α × w_gap_rank + (1-α) × peg_rank"""
    dd = data[d]; pm = peg_pct.get(d, {})
    cands = []
    for tk, info in dd.items():
        p2 = info.get('p2_orig')
        if p2 is None: continue
        pct = pm.get(tk, 1.0)
        combined = alpha * p2 + (1-alpha) * pct * 500
        cands.append((tk, combined))
    cands.sort(key=lambda x: x[1])
    return {tk: i+1 for i, (tk, _) in enumerate(cands)}


def sim(alpha=None, peg_thr=0.22, rev_exit=0.25, exclude=(), start=0):
    """정확한 production-equivalent sim (dynamic weight 포함)"""
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0.0
    n_buys = 0; mu_days = 0; sndk_days = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val); mdd = max(mdd, (peak-val)/peak)
        dd = data[d]
        if 'MU' in held: mu_days += 1
        if 'SNDK' in held: sndk_days += 1

        rank_map = get_rank_multi(d, alpha) if alpha is not None and alpha < 1.0 else None

        def get_p2(tk, info):
            return rank_map.get(tk) if rank_map else (info.get('p2_orig') if info else None)

        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if info.get('rev_growth') is not None and info['rev_growth'] < rev_exit:
                    del held[tk]; continue
            p2 = get_p2(tk, info) if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info, peg_thr): continue
                del held[tk]

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
            # ★ dynamic weight 적용
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        held[tk] = (d, dd[tk]['price'], w[si]); n_buys += 1
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0); n_buys += 1
        prev = dict(held)
    return dict(cum=(val-1)*100, mdd=mdd*100, n_buys=n_buys, mu_days=mu_days, sndk_days=sndk_days)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(alpha, peg_thr=0.22, rev_exit=0.25, exclude=()):
    cums = []; buys = []; mu_d = []; sndk_d = []; savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(alpha, peg_thr, rev_exit, exclude=exclude, start=s)
            cums.append(r['cum']); buys.append(r['n_buys'])
            mu_d.append(r['mu_days']); sndk_d.append(r['sndk_days'])
            sr.append(r['cum'])
        savg.append(sum(sr)/len(sr))
    return cums, savg, buys, mu_d, sndk_d


print('=' * 100)
print('V100 — 종합 검증 (정확한 production sim + dynamic weight)')
print('=' * 100)

# V86e+ baseline (α=None)
print('\n[1] V86e+ baseline (정확한 sim)')
_, v86_savg, v86_buys, v86_mu, v86_sndk = run(None)
v86_avg = sum(s for s in v86_savg)/len(v86_savg) + 100  # baseline 127.6 + lift
print(f'  baseline (no mega 비교용은 별도): savg avg lift {sum(v86_savg)/len(v86_savg):+.1f}% (=실제 누적)')
print(f'  매수 회수 평균: {statistics.mean(v86_buys):.1f}')
print(f'  MU 보유일 평균: {statistics.mean(v86_mu):.1f}')
print(f'  SNDK 보유일 평균: {statistics.mean(v86_sndk):.1f}')

# baseline (no mega): exclude로는 못함. 별도 sim 필요. 일단 V86e+를 reference로

print('\n[2] α grid sweep (PEG=0.22, rev_exit=0.25, dynamic weight)')
print(f'{"α":<8}{"avg":>10}{"vs V86e+":>12}{"wins":>8}{"buys":>8}{"MU일":>7}{"SNDK일":>8}')
print('-' * 65)
for alpha in [None, 0.99, 0.95, 0.9, 0.8, 0.7, 0.5]:
    cums, savg, buys, mu_d, sndk_d = run(alpha)
    avg_lift = sum(s for s in savg)/len(savg)
    diff = [b - a for a, b in zip(v86_savg, savg)]
    al = sum(diff)/len(diff); wins = sum(1 for l in diff if l > 0)
    a_s = 'V86e+' if alpha is None else f'{alpha:.2f}'
    print(f'  {a_s:<8}{avg_lift:>+8.1f}p{al:>+10.1f}p{wins:>5}/100{statistics.mean(buys):>7.1f}{statistics.mean(mu_d):>6.0f}{statistics.mean(sndk_d):>7.0f}')

print('\n[3] 인접안정성: PEG cutoff + α + rev_exit grid')
print(f'{"PEG":<6}{"α":<8}{"rev_exit":<10}{"avg":>10}{"vs V86e+":>12}{"wins":>8}')
print('-' * 60)
for peg in [0.20, 0.22, 0.25]:
    for alpha in [None, 0.7, 0.9]:
        for re in [0.15, 0.25]:
            _, savg, _, _, _ = run(alpha, peg_thr=peg, rev_exit=re)
            avg = sum(s for s in savg)/len(savg)
            diff = [b - a for a, b in zip(v86_savg, savg)]
            al = sum(diff)/len(diff); wins = sum(1 for l in diff if l > 0)
            a_s = 'V86e+' if alpha is None else f'{alpha:.2f}'
            print(f'  {peg:<6.2f}{a_s:<8}{re:<10.2f}{avg:>+8.1f}p{al:>+10.1f}p{wins:>5}/100')

print('\n[4] Multistart — 전반/후반/중간 시작')
n_d = len(dates)
phases = [
    ('전반 (0~25일)', [i for i in range(min(25, n_d-MIN_HOLD))]),
    ('중간 (25~50일)', [i for i in range(25, min(50, n_d-MIN_HOLD))]),
    ('후반 (50~)', [i for i in range(50, n_d-MIN_HOLD)]),
]
print(f'{"variant":<10}{"phase":<20}{"avg":>10}{"vs V86e+":>12}')
print('-' * 55)
for phase_name, starts in phases:
    # V86e+
    v86_results = []
    for s in starts:
        v86_results.append(sim(None, start=s)['cum'])
    # α=0.7
    v99_results = []
    for s in starts:
        v99_results.append(sim(0.7, start=s)['cum'])
    v86_avg = sum(v86_results)/len(v86_results) if v86_results else 0
    v99_avg = sum(v99_results)/len(v99_results) if v99_results else 0
    print(f'  {"V86e+":<10}{phase_name:<20}{v86_avg:>+8.1f}p{"-":>10}')
    print(f'  {"V99(0.7)":<10}{phase_name:<20}{v99_avg:>+8.1f}p{v99_avg-v86_avg:>+10.1f}p')

print('\n[5] LOWO 정확한 검증 (각 variant)')
print(f'{"variant":<10}{"전체 lift":>12}{"-MU":>12}{"-SNDK":>12}{"-MU-SNDK":>14}')
print('-' * 65)
for alpha in [None, 0.7, 0.9]:
    a_s = 'V86e+' if alpha is None else f'α={alpha}'
    row = f'  {a_s:<10}'
    for exn, ex in [('전체', ()), ('-MU', ('MU',)), ('-SNDK', ('SNDK',)), ('-MU-SNDK', ('MU', 'SNDK'))]:
        _, savg, _, _, _ = run(alpha, exclude=ex)
        avg = sum(s for s in savg)/len(savg)
        row += f'{avg:>+10.1f}p '
    print(row)

print(f'\n총 소요 {time.time()-t0:.0f}초')
con.close()
