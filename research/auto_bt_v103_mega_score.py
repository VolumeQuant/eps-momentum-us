# -*- coding: utf-8 -*-
"""V103 — 새 mega_score 산식 BT (사용자 비판 반영)

사용자 비판: "왜 자꾸 성과 안 좋다고 금방 포기?"
사용자 제안: "매출성장률, EPS 성장률 기준 스코어 산식 짜면 되잖아"

새 mega_score 산식:
  mega_score = α × NTM_상향(%) + β × 매출성장(%) + γ × PEG_inverse

이게 메가 종목 (SNDK 1622, MU 1191, BE 281, LITE 211) 진짜 winner 식별 정확.
기존 시스템 (mean reversion) = KEYS 1위 but 사용자 말한 winner 못 잡음.

시도 그리드:
  W1: part2_rank 그대로 + mega_score Top N 진입 후보 추가
  W2: ranking 자체 = α × part2_rank + (1-α) × mega_score_rank
  W3: mega_score 단독 ranking

가중치 그리드:
  α: 0.0, 0.3, 0.5, 0.7, 1.0
  β: 매출 가중
  γ: PEG_inv 가중

paired 100×3 BT.
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
        # mega_score 계산
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


def compute_mega_score(info, alpha=1.0, beta=1.0, gamma=50.0):
    """새 mega_score = α×NTM + β×매출 + γ×PEG_inv"""
    if info is None: return 0
    return alpha * info.get('ntm_rev', 0) + beta * info.get('rg_pct', 0) + gamma * info.get('peg_inv', 0)


def get_combined_rank(d, alpha_blend, mega_alpha=1.0, mega_beta=1.0, mega_gamma=50.0):
    """ranking = blend × part2_rank + (1-blend) × mega_score_rank
    blend=1.0 = pure part2_rank (mean reversion)
    blend=0.0 = pure mega_score
    """
    dd = data[d]
    cands_p2 = []
    cands_mega = []
    for tk, info in dd.items():
        p2 = info.get('p2')
        if p2 is None: continue
        cands_p2.append((tk, p2))
        ms = compute_mega_score(info, mega_alpha, mega_beta, mega_gamma)
        cands_mega.append((tk, ms))
    # part2_rank 그대로
    p2_rank = {tk: r for tk, r in cands_p2}
    # mega_score 내림차순 → rank
    cands_mega.sort(key=lambda x: -x[1])
    mega_rank = {tk: i+1 for i, (tk, _) in enumerate(cands_mega)}
    # blend
    combined = {}
    for tk in p2_rank:
        ms_r = mega_rank.get(tk, 999)
        combined[tk] = alpha_blend * p2_rank[tk] + (1-alpha_blend) * ms_r
    cands_c = sorted(combined.items(), key=lambda x: x[1])
    return {tk: i+1 for i, (tk, _) in enumerate(cands_c)}


def sim(blend, mega_alpha=1.0, mega_beta=1.0, mega_gamma=50.0, exclude=(), start=0, cache=None):
    if cache is None: cache = {}
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val); mdd = max(mdd, (peak-val)/peak)
        dd = data[d]

        if blend < 1.0:
            ck = (blend, mega_alpha, mega_beta, mega_gamma, d)
            if cache.get(ck) is None:
                cache[ck] = get_combined_rank(d, blend, mega_alpha, mega_beta, mega_gamma)
            rank_map = cache[ck]
        else:
            rank_map = None

        def get_rank(tk, info):
            if rank_map is not None: return rank_map.get(tk)
            return info.get('p2') if info else None

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None and info.get('min_seg', 0) < -2:
                del held[tk]; continue
            r = get_rank(tk, info) if info else None
            if info is None or r is None or r > 10:
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
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
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
    return (val-1)*100, mdd*100


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(blend, mega_alpha=1.0, mega_beta=1.0, mega_gamma=50.0, exclude=()):
    cache = {}; cums = []; savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r, _ = sim(blend, mega_alpha, mega_beta, mega_gamma, exclude=exclude, start=s, cache=cache)
            cums.append(r); sr.append(r)
        savg.append(sum(sr)/len(sr))
    return cums, savg


print('=' * 100)
print('V103 — 새 mega_score 산식 BT (매출/EPS/PEG 기반)')
print('=' * 100)

# baseline = blend 1.0 (pure part2_rank)
_, base_savg = run(1.0)
print(f'\nbaseline (part2_rank only) avg: {sum(base_savg)/len(base_savg):+.1f}%')

# grid
print(f'\n[blend × mega_score 가중치 그리드]')
print(f'{"blend":<8}{"α (NTM)":<10}{"β (매출)":<10}{"γ (PEG_inv)":<12}{"avg lift":>10}{"wins":>10}')
print('-' * 75)

best = None
configs = [
    (0.9, 1.0, 1.0, 50.0),  # 살짝 mega
    (0.7, 1.0, 1.0, 50.0),  # 보통 mega
    (0.5, 1.0, 1.0, 50.0),  # 균등
    (0.3, 1.0, 1.0, 50.0),  # mega 우세
    (0.0, 1.0, 1.0, 50.0),  # mega only
    # 가중치 변형
    (0.5, 0.5, 1.0, 30.0),
    (0.5, 1.0, 0.5, 20.0),
    (0.5, 0.5, 1.5, 30.0),  # 매출 강조
    (0.5, 1.5, 0.5, 30.0),  # NTM 강조
    (0.5, 1.0, 1.0, 100.0), # PEG 강조
]

for blend, a, b, g in configs:
    _, savg = run(blend, a, b, g)
    lifts = [b_ - a_ for a_, b_ in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    print(f'{blend:<8.2f}{a:<10.1f}{b:<10.1f}{g:<12.1f}{avg:>+9.1f}p{wins:>7}/100')
    if best is None or avg > best[1]:
        best = ((blend, a, b, g), avg)

print(f'\n총 소요 {time.time()-t0:.0f}초')
print(f'\nbest config: {best}')

# LOWO check (best config)
if best and best[1] > 0:
    print(f'\n[LOWO -MU-SNDK robustness check (best config)]')
    cfg = best[0]
    _, base_ex = run(1.0, exclude=('MU', 'SNDK'))
    _, n_ex = run(*cfg, exclude=('MU', 'SNDK'))
    lifts_lowo = [b_ - a_ for a_, b_ in zip(base_ex, n_ex)]
    lowo = sum(lifts_lowo)/len(lifts_lowo)
    w = sum(1 for l in lifts_lowo if l > 0)
    print(f'  best config LOWO -MU-SNDK: {lowo:+.1f}p ({w}/100)')

con.close()
