# -*- coding: utf-8 -*-
"""V88 자율주행 — 메가홀드 adj_gap/score 보너스 (계산식 통합)

사용자 의도: "V86e PEG<0.20 조건을 실제 계산식에 적용 → 메가 자연 상위 진입"

V87 (rank 직접 override) 결과 — 다 V86e 대비 -57~-103p.
이유: 메가가 1·2위 무조건 차지 → 신규 알파 (KEYS, FAF 등) 차단.

V88 시도 — 보너스만 줘서 메가가 강할 땐 진짜 상위, 약할 땐 자연 ranking:
  V88a: adj_gap *= 1.3 (메가, 더 음수로 conviction 강화)
  V88b: adj_gap *= 1.5
  V88c: adj_gap *= 2.0
  V88d: adj_gap -= 0.05 (절대 음수 보너스)
  V88e: adj_gap -= 0.10
  V88f: 매도만: 메가 rank cutoff > 20 (자연 진입 + 매도 보호)
  V88g: 매도만: 메가 rank cutoff > 30
  V88h: 매도만: 메가 rank cutoff 무한 (= V86e와 등가)

각 paired BT + LOWO + 부분기간.

NOTE: simulator에서 adj_gap 재계산 후 part2_rank 재정렬 시뮬레이션.
       data['adj_gap']을 미리 계산 (현재 DB에 저장됨), 메가는 보너스 곱셈.
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
        '''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth,adj_gap,adj_score
           FROM ntm_screening WHERE date=?''', (d,)):
        tk = r[0]
        nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a - b) / abs(b) * 100)) if b and abs(b) > 0.01 else 0)
        ntm_rev = (nc / n90 - 1) * 100 if n90 and n90 > 0 else None
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        data[d][tk] = dict(p2_orig=r[1], cr_orig=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           ntm_rev=ntm_rev, peg=peg, rev_growth=rg,
                           adj_gap=r[12], adj_score=r[13])

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def is_mega(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.20


def verified(t, i):
    for j in (i, i - 1, i - 2):
        if j < 0:
            return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr_orig') is None or x['cr_orig'] > 30:
            return False
    return True


def adjusted_rank(d, variant):
    """variant 별 part2_rank 재산정.
    adj_gap_boost: 메가의 adj_gap을 boost factor 곱셈 후 전체 종목 재정렬.
                   원본 데이터에서 adj_gap NOT NULL 종목만 대상.
    """
    dd = data[d]
    if variant.startswith('V88a') or variant.startswith('V88b') or variant.startswith('V88c'):
        if variant == 'V88a':
            factor = 1.3
        elif variant == 'V88b':
            factor = 1.5
        elif variant == 'V88c':
            factor = 2.0
        else:
            factor = 1.0
        cands = []
        for tk, info in dd.items():
            if info.get('adj_gap') is None:
                continue
            ag = info['adj_gap']
            if is_mega(info):
                ag *= factor  # 음수가 더 음수로 → 매수 conviction 강화
            cands.append((tk, ag))
        cands.sort(key=lambda x: x[1])  # 오름차순 (가장 음수 = 1위)
        # 새 part2_rank 부여
        new_rank = {tk: i + 1 for i, (tk, _) in enumerate(cands)}
        return new_rank
    elif variant in ('V88d', 'V88e'):
        delta = -0.05 if variant == 'V88d' else -0.10
        cands = []
        for tk, info in dd.items():
            if info.get('adj_gap') is None:
                continue
            ag = info['adj_gap']
            if is_mega(info):
                ag += delta
            cands.append((tk, ag))
        cands.sort(key=lambda x: x[1])
        new_rank = {tk: i + 1 for i, (tk, _) in enumerate(cands)}
        return new_rank
    return None  # variant가 ranking 재산정 안 함


def get_rank(d, tk, variant, new_rank_cache):
    """ranking 재산정 여부에 따라 part2_rank 반환"""
    if variant in ('V88a', 'V88b', 'V88c', 'V88d', 'V88e'):
        if new_rank_cache.get(d) is None:
            new_rank_cache[d] = adjusted_rank(d, variant)
        nr = new_rank_cache[d]
        return nr.get(tk) if nr else None
    else:
        info = data[d].get(tk)
        return info.get('p2_orig') if info else None


def sim(variant='baseline', exclude=(), start=0, new_rank_cache=None):
    if new_rank_cache is None:
        new_rank_cache = {}
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

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                if variant == 'V86e' and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]
                    continue

            # rank 계산
            if variant in ('V88f', 'V88g', 'V88h'):
                p2 = info.get('p2_orig') if info else None
                # 메가 매도 보호: cutoff 변경
                if variant == 'V88f':
                    mega_cutoff = 20
                elif variant == 'V88g':
                    mega_cutoff = 30
                elif variant == 'V88h':
                    mega_cutoff = 9999
                else:
                    mega_cutoff = 10
                if info is None or p2 is None or p2 > 10:
                    if is_mega(info) and (p2 is None or p2 <= mega_cutoff):
                        continue  # 메가는 cutoff까지 보호
                    del held[tk]
            elif variant in ('V88a', 'V88b', 'V88c', 'V88d', 'V88e'):
                # 보너스 적용된 새 rank
                p2 = get_rank(d, tk, variant, new_rank_cache)
                if info is None or p2 is None or p2 > 10:
                    del held[tk]
            elif variant == 'V86e':
                p2 = info.get('p2_orig') if info else None
                if info is None or p2 is None or p2 > 10:
                    if is_mega(info):
                        continue
                    del held[tk]
            else:  # baseline
                p2 = info.get('p2_orig') if info else None
                if info is None or p2 is None or p2 > 10:
                    del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                # rank 계산
                if variant in ('V88a', 'V88b', 'V88c', 'V88d', 'V88e'):
                    p2 = get_rank(d, tk, variant, new_rank_cache)
                else:
                    p2 = info.get('p2_orig')
                if p2 is None or p2 > 3:
                    continue
                if tk in held or tk in exclude:
                    continue
                if info.get('min_seg') is not None and info['min_seg'] < 0:
                    continue
                if not info['price']:
                    continue
                if not verified(tk, i):
                    continue
                if info.get('high30') and info['price'] and info['price'] / info['high30'] - 1 < -0.25:
                    continue
                cands.append((p2, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2 - len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        held[tk] = (d, dd[tk]['price'], w[si])
                        n_buys += 1
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
                    n_buys += 1
        prev = dict(held)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100, n_buys=n_buys)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(variant, exclude=()):
    cache = {}  # rank 재계산 캐시 (variant별)
    cums = []
    mdds = []
    buys = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s, new_rank_cache=cache)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            buys.append(r['n_buys'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg, buys


print('=' * 100)
print('V88 자율주행: adj_gap 보너스 (계산식 통합) + 매도 보호 cutoff')
print('=' * 100)

VARIANTS = [
    ('baseline', '메가홀드 없음'),
    ('V86e', '현재 — 보유 결정 layer (carryover)'),
    ('V88a', 'adj_gap × 1.3 (메가)'),
    ('V88b', 'adj_gap × 1.5'),
    ('V88c', 'adj_gap × 2.0'),
    ('V88d', 'adj_gap - 0.05'),
    ('V88e', 'adj_gap - 0.10'),
    ('V88f', '매도만: 메가 rank cutoff > 20'),
    ('V88g', '매도만: 메가 rank cutoff > 30'),
    ('V88h', '매도만: 메가 rank cutoff 무한 (V86e 매도부만 etc)'),
]

_, _, base_savg, _ = run('baseline')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

print(f'\n{"variant":<10}{"desc":<46}{"avg":>9}{"med":>9}{"MDD":>9}{"매수회수":>10}{"lift":>9}{"wins":>9}')
print('-' * 110)

results = {}
for vid, desc in VARIANTS:
    cums, mdds, savg, buys = run(vid)
    avg = statistics.mean(cums)
    med = statistics.median(cums)
    mdd_med = statistics.median(mdds)
    avg_buys = statistics.mean(buys)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    results[vid] = (avg, mdd_med, avg_lift, wins, avg_buys)
    mk = ' ★' if vid == 'baseline' else '  '
    ls = '' if vid == 'baseline' else f'{avg_lift:>+7.1f}p{wins:>6}/100'
    print(f'{mk}{vid:<8}{desc:<46}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{avg_buys:>9.1f}{ls}')
    sys.stdout.flush()

# LOWO 종합
print('\n[LOWO 견고성]')
print(f'{"variant":<10}{"전체":>11}{"-MU":>13}{"-SNDK":>13}{"-MU-SNDK":>14}')
print('-' * 65)

for vid, _ in VARIANTS:
    if vid == 'baseline':
        continue
    row = f'{vid:<10}'
    for exn, ex in [('전체', ()), ('-MU', ('MU',)), ('-SNDK', ('SNDK',)), ('-MU-SNDK', ('MU', 'SNDK'))]:
        _, _, b, _ = run('baseline', exclude=ex)
        _, _, n, _ = run(vid, exclude=ex)
        lifts = [y - x for x, y in zip(b, n)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        if exn == '전체':
            row += f'{avg_lift:>+8.1f}p({wins:>2})'
        else:
            row += f'{avg_lift:>+9.1f}p({wins:>2})'
    print(row)
    sys.stdout.flush()

# 부분기간
n_dates = len(dates)
front_starts = [ch for ch in seeds if all(s < n_dates // 2 for s in ch)]
back_starts = [ch for ch in seeds if all(s >= n_dates // 2 - 30 for s in ch)]
print(f'\n[부분기간 — 전반 {len(front_starts)} / 후반 {len(back_starts)}]')

def run_filtered(variant, sf, exclude=()):
    cache = {}
    savg = []
    for ch in sf:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s, new_rank_cache=cache)
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return savg

print(f'{"variant":<10}{"전반 lift":>11}{"전반 wins":>11}{"후반 lift":>11}{"후반 wins":>11}')
print('-' * 60)
for vid, _ in VARIANTS:
    if vid == 'baseline':
        continue
    row = f'{vid:<10}'
    bf = run_filtered('baseline', front_starts)
    nf = run_filtered(vid, front_starts)
    lifts = [y - x for x, y in zip(bf, nf)]
    al = sum(lifts) / len(lifts) if lifts else 0
    wf = sum(1 for l in lifts if l > 0)
    row += f'{al:>+8.1f}p{wf:>8}/{len(lifts)}'
    bb = run_filtered('baseline', back_starts)
    nb = run_filtered(vid, back_starts)
    lifts = [y - x for x, y in zip(bb, nb)]
    al = sum(lifts) / len(lifts) if lifts else 0
    wb = sum(1 for l in lifts if l > 0)
    row += f'{al:>+8.1f}p{wb:>8}/{len(lifts)}'
    print(row)

# V86e vs V88 direct comparison
print('\n[V86e vs V88 variants 직접 paired]')
_, _, v86e, _ = run('V86e')
for vid in ['V88a', 'V88b', 'V88c', 'V88d', 'V88e', 'V88f', 'V88g', 'V88h']:
    _, _, n, _ = run(vid)
    lifts = [y - x for x, y in zip(v86e, n)]
    al = sum(lifts) / len(lifts)
    w = sum(1 for l in lifts if l > 0)
    print(f'  {vid} - V86e: {al:+.1f}p ({w}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
