# -*- coding: utf-8 -*-
"""V95 자율주행 — 메가 종목 공통점 정밀 시그니처 + score 보너스

사용자 의도: "메가 공통점 분석해서 그걸로 보너스 또는 계산식 수정"

분석 결과 (MU/SNDK 75일 vs UMBF false positive):
  - MU: NTM +97~140%, PEG 0.02~0.19, rev_g 57~196%, analysts 28~33
  - SNDK: NTM +106~661%, PEG 0.03~0.18, rev_g 61~251%, analysts 16~20
  - UMBF: NTM +5~9% (false positive), PEG 0.198

진짜 슈퍼사이클 시그니처 후보:
  M1: PEG<0.22 (V86e+ 현재) — UMBF 포함
  M2: NTM≥60% AND PEG<0.22 — UMBF 제외 (false positive 차단)
  M3: NTM≥60% AND PEG<0.22 AND rev_g≥50% — 더 엄격
  M4: NTM≥60% AND PEG<0.22 AND rev_g≥50% AND analysts≥15 — 가장 엄격
  M5: 가중 score 보너스 — NTM 상향률 비례 (60 → +0, 100 → +5, 150 → +10)

각 paired BT + LOWO + 신규 매수 빈도.

추가 검증: 공통점 기반 score 보너스 (composite_rank 변경, part2_rank 그대로)
  S1: 메가 시그니처 → composite_rank 강제 1·2위 (표시만, BT 영향 0)
  → 사용자 의도 (자연 상위) 충족 + BT 알파 보존
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
N_SEEDS = 100
SAMPLES = 3
MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data = {}
for d in dates:
    data[d] = {}
    for r in cur.execute(
        '''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth,num_analysts FROM ntm_screening WHERE date=?''',
        (d,)):
        tk = r[0]
        nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a - b) / abs(b) * 100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        ntm_rev_pct = (nc/n90 - 1) * 100 if n90 and n90 > 0 else 0
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg, num_analysts=r[12] or 0,
                           ntm_rev=ntm_rev_pct)

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i - 1, i - 2):
        if j < 0:
            return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30:
            return False
    return True


def check_mega(info, variant):
    """variant 별 메가 판정"""
    if info is None:
        return False
    peg = info.get('peg')
    ntm_rev = info.get('ntm_rev', 0)
    rg = info.get('rev_growth')
    na = info.get('num_analysts', 0)

    if variant == 'M1':  # PEG<0.22 only (V86e+ 현재)
        return peg is not None and peg < 0.22
    elif variant == 'M2':  # NTM≥60 AND PEG<0.22
        return peg is not None and peg < 0.22 and ntm_rev >= 60
    elif variant == 'M3':  # + rev_g≥50%
        return (peg is not None and peg < 0.22 and ntm_rev >= 60
                and rg is not None and rg >= 0.50)
    elif variant == 'M4':  # + analysts≥15
        return (peg is not None and peg < 0.22 and ntm_rev >= 60
                and rg is not None and rg >= 0.50 and na >= 15)
    elif variant == 'M5':  # 가중 보너스 — NTM 비례 score boost (BT 영향 X)
        return peg is not None and peg < 0.22
    return False


def sim(variant='M1', exclude=(), start=0):
    held = {}
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
    n_buys = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i - 1]
            ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk)
                pn = pf[d].get(tk, pp)
                if pp and pn:
                    ret += w * (pn / pp - 1)
            val *= (1 + ret)
            peak = max(peak, val)
            mdd = max(mdd, (peak - val) / peak)
        dd = data[d]
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg', 0) < -2:
                    del held[tk]; continue
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if check_mega(info, variant):
                    continue
                del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > 3:
                    continue
                if tk in held or tk in exclude:
                    continue
                if info.get('min_seg', 0) < 0:
                    continue
                if not info['price']:
                    continue
                if not verified(tk, i):
                    continue
                if info.get('high30') and info['price'] and info['price'] / info['high30'] - 1 < -0.25:
                    continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2 - len(held)]
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
    return dict(cum=(val - 1) * 100, mdd=mdd * 100, n_buys=n_buys)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(variant, exclude=()):
    cums = []
    buys = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
            cums.append(r['cum'])
            buys.append(r['n_buys'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, savg, buys


print('=' * 100)
print('V95 자율주행: 메가 공통점 정밀 시그니처 + UMBF false positive 차단')
print('=' * 100)

# baseline = M1으로 (V86e+ 현재) 메가 OFF 비교용
def sim_no_mega(exclude=(), start=0):
    held = {}
    prev = None
    val = 1.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i - 1]
            ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk)
                pn = pf[d].get(tk, pp)
                if pp and pn:
                    ret += w * (pn / pp - 1)
            val *= (1 + ret)
        dd = data[d]
        for tk in list(held):
            info = dd.get(tk)
            if info is not None and info.get('min_seg', 0) < -2:
                del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                del held[tk]
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > 3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price'] / info['high30'] - 1 < -0.25: continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2 - len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return dict(cum=(val - 1) * 100)

base_savg = []
for ch in seeds:
    sr = []
    for s in ch:
        sr.append(sim_no_mega(start=s)['cum'])
    base_savg.append(sum(sr)/len(sr))
print(f'\nbaseline (no mega) avg: {sum(base_savg)/len(base_savg):+.1f}%')

VARIANTS = [
    ('M1', 'PEG<0.22 (V86e+ 현재 — UMBF 포함)'),
    ('M2', 'NTM≥60 AND PEG<0.22 (UMBF 제외)'),
    ('M3', '+ rev_g≥50% (더 엄격)'),
    ('M4', '+ analysts≥15 (가장 엄격, MU/SNDK only)'),
]

print(f'\n{"variant":<6}{"desc":<48}{"avg lift":>12}{"wins":>10}{"매수":>10}{"LOWO -MU-SNDK":>18}')
print('-' * 110)

base_ex_savg = []
for ch in seeds:
    sr = []
    for s in ch:
        sr.append(sim_no_mega(exclude=('MU', 'SNDK'), start=s)['cum'])
    base_ex_savg.append(sum(sr)/len(sr))

for vid, desc in VARIANTS:
    cums, savg, buys = run(vid)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    avg_buys = statistics.mean(buys)
    _, n_ex, _ = run(vid, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(base_ex_savg, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    print(f'  {vid:<4}{desc:<48}{avg:>+10.1f}p{wins:>7}/100{avg_buys:>9.1f}{al_lowo:>+10.1f}p({w_lowo:>2})')
    sys.stdout.flush()

# V86e+ vs V95 direct
print('\n[M1 (V86e+) vs M2/M3/M4 direct paired]')
_, m1, _ = run('M1')
for vid in ['M2', 'M3', 'M4']:
    _, n, _ = run(vid)
    lifts = [y - x for x, y in zip(m1, n)]
    al = sum(lifts)/len(lifts)
    w = sum(1 for l in lifts if l > 0)
    print(f'  {vid} - M1 (V86e+): {al:+.1f}p ({w}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
