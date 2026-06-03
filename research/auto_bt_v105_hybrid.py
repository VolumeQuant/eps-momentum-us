# -*- coding: utf-8 -*-
"""V105 — 하이브리드 전략 심층 자율주행

사용자 명령:
- SNDK/MU 없이도 꾸준히 수익
- SNDK/MU 있을 때도 안 놓침
- 두 조건 동시 만족

V104_mega 발견: -SNDK 시 +133.4% (v86e+ +117보다 +16p 우월)
V86e+ 발견: 전체 +211.8% (SNDK carryover로 압도) but SNDK 제외 시 +117%

후보 전략:
- V105a: v86e+ carryover + mega_score 매수 보강 (entry 풀 확장)
- V105b: 동적 ranking — 메가 있을 때 v86e+, 없을 때 v104
- V105c: 슬롯 2 = part2_rank Top 1 + mega_score Top 1 (ensemble)
- V105d: mega_score 매수 + carryover
- V105e: entry 풀 = part2_rank Top 5 + mega_score Top 5 (union)

검증:
- 전체 (SNDK/MU 포함) — v86e+ 만큼 잡아야
- SNDK 제외 — v104보다 우월해야
- SNDK+MU 제외 — robust해야
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


def get_mega_rank(d):
    """mega_score 기반 ranking (높을수록 강함)"""
    dd = data[d]
    cands = [(tk, info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0))
             for tk, info in dd.items() if info['p2'] is not None]
    cands.sort(key=lambda x: -x[1])
    return {tk: i+1 for i, (tk, _) in enumerate(cands)}


def sim(variant, exclude=(), start=0):
    held = {}; prev = None; val = 1.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1 + ret)
        dd = data[d]
        mr = get_mega_rank(d) if variant.startswith('v105') else None

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2: del held[tk]; continue
                if variant in ('v86e+', 'v105a', 'v105c', 'v105d', 'v105e') and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    # v86e+ rev_growth 매도 (메가만)
                    if is_mega(info): del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                # carryover 조건
                if variant in ('v86e+', 'v105a', 'v105c', 'v105d', 'v105e') and is_mega(info):
                    continue
                del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            if variant == 'v105e':  # entry 풀 = part2 Top 5 + mega_score Top 5
                for tk, info in dd.items():
                    p2 = info.get('p2'); mr_v = mr.get(tk, 999) if mr else 999
                    if mr_v is None: mr_v = 999
                    if (p2 is None or p2 > 5) and (mr_v > 5): continue
                    if tk in held or tk in exclude: continue
                    if info.get('min_seg', 0) < 0: continue
                    if not info['price']: continue
                    if not verified(tk, i): continue
                    if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                    # 합산 rank
                    combined = min(p2 or 999, mr_v)
                    cands.append((combined, info['score'], tk))
            elif variant == 'v105c':  # slot 1 = part2 Top, slot 2 = mega_score Top
                # part2 후보 1개, mega_score 후보 1개
                p2_cands = []
                ms_cands = []
                for tk, info in dd.items():
                    if tk in held or tk in exclude: continue
                    if info.get('min_seg', 0) < 0: continue
                    if not info['price']: continue
                    if not verified(tk, i): continue
                    if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                    p2 = info.get('p2'); mr_v = mr.get(tk, 999) if mr else 999
                    if mr_v is None: mr_v = 999
                    if p2 is not None and p2 <= 3:
                        p2_cands.append((p2, info['score'], tk))
                    if mr_v <= 3 and tk not in [t for _, _, t in p2_cands]:
                        ms_cands.append((mr_v, info['score'], tk))
                p2_cands.sort(key=lambda x: x[0])
                ms_cands.sort(key=lambda x: x[0])
                pick = []
                if p2_cands: pick.append(p2_cands[0])
                if ms_cands: pick.append(ms_cands[0])
                pick = pick[:2-len(held)]
                # 매수
                if len(held) == 0 and len(pick) >= 2:
                    for _, _, tk in pick: held[tk] = (d, dd[tk]['price'], 0.5)
                else:
                    for _, _, tk in pick: held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
                prev = dict(held); continue
            elif variant in ('v105a', 'v105d'):  # part2 Top 3 + mega_score 메가만 추가
                for tk, info in dd.items():
                    p2 = info.get('p2'); mr_v = mr.get(tk, 999) if mr else 999
                    if mr_v is None: mr_v = 999
                    is_top_p2 = p2 is not None and p2 <= 3
                    is_top_mega = is_mega(info) and mr_v <= 3
                    if not (is_top_p2 or is_top_mega): continue
                    if tk in held or tk in exclude: continue
                    if info.get('min_seg', 0) < 0: continue
                    if not info['price']: continue
                    if not verified(tk, i): continue
                    if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                    # 우선순위: part2_rank
                    r = p2 if (p2 is not None and p2 <= 3) else mr_v
                    cands.append((r, info['score'], tk))
            else:  # baseline (v84, v86e+, etc)
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
print('V105 하이브리드 — SNDK 없이 robust + SNDK 있을 때 잡음')
print('=' * 100)

exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')),
              ('-SNDK-MU-LITE', ('SNDK', 'MU', 'LITE'))]

variants = ['v84_pure', 'v86e+', 'v105a', 'v105c', 'v105d', 'v105e']
print(f'\\n{"exclude":<22}{"v84":>9}{"v86e+":>9}{"v105a":>9}{"v105c":>9}{"v105d":>9}{"v105e":>9}')
print('-' * 80)
all_avgs = {}
for name, ex in exclusions:
    avgs = {}
    for v in variants:
        cums = run(v, ex)
        avgs[v] = statistics.mean(cums)
    all_avgs[name] = avgs
    print(f'{name:<22}' + ''.join(f'{avgs[v]:>+7.1f}%' for v in variants))

# 두 조건 (전체 v86e+ 매칭 + SNDK 제외 시 우월) 동시 만족 평가
print(f'\\n[두 조건 동시 만족 평가]')
print(f'{"variant":<10}{"전체":>10}{"-SNDK":>10}{"-SNDK-MU":>12}{"종합 (3합)":>12}')
print('-' * 60)
for v in variants:
    s1 = all_avgs['전체'][v]
    s2 = all_avgs['-SNDK'][v]
    s3 = all_avgs['-SNDK-MU'][v]
    total = s1 + s2 + s3
    print(f'{v:<10}{s1:>+9.1f}%{s2:>+9.1f}%{s3:>+11.1f}%{total:>+11.1f}%')

print(f'\\n총 소요 {time.time()-t0:.0f}초')
con.close()
