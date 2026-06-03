# -*- coding: utf-8 -*-
"""V105 grid 정밀 — 사용자 두 조건 동시 만족 최적

조건:
- SNDK 없이도 robust (-SNDK 시 v86e+보다 우월)
- SNDK 있을 때 안 놓침 (v86e+의 90%+ 잡음)

V105a 발견 후 grid 정밀:
- mega_score 가중치 (α NTM, β 매출, γ PEG_inv)
- entry cutoff (Top 3, Top 5)
- mega_score entry cutoff (Top 3, Top 5, Top 10)
- carryover 유지 (V86e+ 그대로)

검증:
- 전체, -SNDK, -MU, -SNDK-MU, -SNDK-MU-LITE
- 종합 (3합) 평가
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

data = {}
for d in dates:
    data[d] = {}
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
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, ntm_rev=ntm_rev, rg_pct=rg_pct, peg_inv=peg_inv)

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


def sim(p2_cutoff, mega_cutoff, alpha, beta, gamma, exclude=(), start=0):
    """V105 grid sim
    p2_cutoff: part2_rank Top N
    mega_cutoff: mega_score Top N (메가만)
    alpha, beta, gamma: mega_score 가중치
    """
    held = {}; prev = None; val = 1.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret)
        dd = data[d]

        # mega_score rank (메가만)
        mega_cands = [(tk, alpha*info.get('ntm_rev',0) + beta*info.get('rg_pct',0) + gamma*info.get('peg_inv',0))
                      for tk, info in dd.items() if is_mega(info)]
        mega_cands.sort(key=lambda x: -x[1])
        mega_rank = {tk: i+1 for i, (tk, _) in enumerate(mega_cands)}

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if is_mega(info) and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if is_mega(info): continue  # carryover
                del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                p2 = info.get('p2'); mr = mega_rank.get(tk, 999)
                is_top_p2 = p2 is not None and p2 <= p2_cutoff
                is_top_mega = is_mega(info) and mr <= mega_cutoff
                if not (is_top_p2 or is_top_mega): continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                r = p2 if (p2 is not None and p2 <= p2_cutoff) else (100 + mr)  # part2 우선
                cands.append((r, info['score'], tk))
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


def run_avg(p2_c, mega_c, a, b, g, ex=()):
    cums = []
    for ch in seeds:
        for s in ch:
            cums.append(sim(p2_c, mega_c, a, b, g, exclude=ex, start=s))
    return statistics.mean(cums)


print('=' * 100)
print('V105 grid 정밀 — 두 조건 동시 만족 best')
print('=' * 100)

# 그리드
configs = [
    # (label, p2_cutoff, mega_cutoff, alpha, beta, gamma)
    ('v105a (기본)', 3, 3, 1.0, 1.0, 50.0),
    ('v105_p3m1', 3, 1, 1.0, 1.0, 50.0),
    ('v105_p3m2', 3, 2, 1.0, 1.0, 50.0),
    ('v105_p3m5', 3, 5, 1.0, 1.0, 50.0),
    ('v105_p3m10', 3, 10, 1.0, 1.0, 50.0),
    ('v105_p5m3', 5, 3, 1.0, 1.0, 50.0),
    ('v105_p2m3', 2, 3, 1.0, 1.0, 50.0),
    # 가중치 변형
    ('v105_w_ntm', 3, 3, 2.0, 1.0, 50.0),  # NTM 강조
    ('v105_w_rev', 3, 3, 1.0, 2.0, 50.0),  # 매출 강조
    ('v105_w_peg', 3, 3, 1.0, 1.0, 100.0),  # PEG_inv 강조
    ('v105_w_peg30', 3, 3, 1.0, 1.0, 30.0),  # PEG 줄임
    ('v105_w_rev_only', 3, 3, 0.5, 1.5, 50.0),  # 매출 우선
    # 보수적
    ('v105_p5m5', 5, 5, 1.0, 1.0, 50.0),
    ('v105_p5m10', 5, 10, 1.0, 1.0, 50.0),
]

exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU'))]

print(f'\\n{"config":<22}{"전체":>10}{"-SNDK":>10}{"-MU":>10}{"-SNDK-MU":>11}{"종합":>10}')
print('-' * 80)

# baseline (v86e+ = p2 Top 3 + mega 0 = pure mean reversion + carryover)
# 실제 v86e+ 시뮬은 mega_cutoff=0 (메가 entry 없음)
def sim_v86e(exclude=(), start=0):
    return sim(3, 0, 1.0, 1.0, 50.0, exclude=exclude, start=start)

baselines = {}
for name, ex in exclusions:
    cums = []
    for ch in seeds:
        for s in ch:
            cums.append(sim_v86e(exclude=ex, start=s))
    baselines[name] = statistics.mean(cums)

print(f'{"v86e+ (baseline)":<22}' + ''.join(f'{baselines[n]:>+9.1f}%' for n, _ in exclusions) + f'{sum(baselines.values()):>+9.1f}%')

best_score = -999
best_config = None
results = {}
for label, p2_c, m_c, a, b, g in configs:
    avgs = {}
    for name, ex in exclusions:
        avgs[name] = run_avg(p2_c, m_c, a, b, g, ex=ex)
    total = sum(avgs.values())
    # 두 조건 가중 점수: 전체 알파 + (-SNDK 시 v86e+ 대비 lift) × 2
    cond_score = avgs['전체'] + 2 * (avgs['-SNDK'] - baselines['-SNDK'])
    results[label] = (avgs, total, cond_score)
    if cond_score > best_score:
        best_score = cond_score
        best_config = label
    mark = ' ★' if cond_score > 200 else ''
    print(f'{label:<22}' + ''.join(f'{avgs[n]:>+9.1f}%' for n, _ in exclusions) + f'{total:>+9.1f}%{mark}')

print(f'\\n[두 조건 가중 점수 = 전체 + 2 × (-SNDK lift)]')
ranked = sorted(results.items(), key=lambda x: -x[1][2])
for label, (avgs, total, cs) in ranked[:5]:
    lift_sndk = avgs['-SNDK'] - baselines['-SNDK']
    print(f'  {label:<22} 전체 {avgs["전체"]:+.1f}% / -SNDK {avgs["-SNDK"]:+.1f}% (lift {lift_sndk:+.1f}p) / score {cs:.1f}')

print(f'\\nbest config: {best_config}')
print(f'총 소요 {time.time()-t0:.0f}초')
con.close()
