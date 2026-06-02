# -*- coding: utf-8 -*-
"""V89 자율주행 — slot 3 메가 전용 확장

전문가 권고: V86e의 trade-off (메가 슬롯 점유로 신규 매수 감소)를 해소하려면
            slot 3 메가 전용 슬롯 추가. 메가 없으면 비움 (현금).

변형:
  baseline: slot 2, 메가홀드 없음
  V86e    : slot 2, V86e carryover (현재 master)
  V89a    : slot 3 (50/30/20) — 1·2위 신규, 3위 메가 전용 (없으면 빈슬롯)
  V89b    : slot 3 (50/25/25)
  V89c    : slot 3 (40/30/30)
  V89d    : slot 3 (60/30/10) — 메가 비중 낮춤
  V89e    : slot 3 (33/33/33) — 균등
  V89f    : 메가 있을 때만 slot 3 활성 (없으면 slot 2 그대로)

매도 트리거 (V86e 동일): min_seg<-2 OR rev_growth<0.25
메가 조건: PEG<0.20
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
        '''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?''',
        (d,)):
        tk = r[0]
        nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a - b) / abs(b) * 100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           peg=peg, rev_growth=rg)

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


def is_mega(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.20


# 가중치 매핑
WEIGHTS = {
    'V89a': (0.50, 0.30, 0.20),
    'V89b': (0.50, 0.25, 0.25),
    'V89c': (0.40, 0.30, 0.30),
    'V89d': (0.60, 0.30, 0.10),
    'V89e': (0.34, 0.33, 0.33),
}


def sim(variant='baseline', exclude=(), start=0):
    held = {}      # tk -> (entry_date, entry_price, weight, slot)
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
            for tk, (ed, ep, w, sl) in prev.items():
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
            ed, ep, w, sl = held[tk]
            if info is not None:
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]
                    continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                # V89: 메가 종목 + slot 3 (메가 전용)이면 보호. 다른 슬롯이면 매도.
                if variant.startswith('V89') and is_mega(info) and sl == 3:
                    continue
                # V86e: 메가 carryover (모든 슬롯)
                elif variant == 'V86e' and is_mega(info):
                    continue
                del held[tk]

        # 매수 — variant 별
        if variant.startswith('V89'):
            w_alloc = WEIGHTS[variant] if variant in WEIGHTS else WEIGHTS['V89a']
            # V89f: 메가 있을 때만 slot 3 활성
            if variant == 'V89f':
                # 현재 메가 후보 있는지 체크
                has_mega_cand = any(is_mega(info) for info in dd.values())
                MAX_SLOTS = 3 if has_mega_cand else 2
                w_alloc = (0.50, 0.30, 0.20) if MAX_SLOTS == 3 else (0.5, 0.5, 0.0)
            else:
                MAX_SLOTS = 3

            # slot 3 메가 전용 — 메가 종목만 후보
            if 3 not in [s for _, (_, _, _, s) in held.items()] and len(held) < MAX_SLOTS:
                # 슬롯 3 채우기 시도 (메가 종목, part2_rank 무관)
                mega_cands = []
                for tk, info in dd.items():
                    if not is_mega(info):
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
                    p2 = info.get('p2') or 999
                    mega_cands.append((p2, info['score'], tk))
                mega_cands.sort(key=lambda x: x[0])
                if mega_cands:
                    _, _, tk = mega_cands[0]
                    held[tk] = (d, dd[tk]['price'], w_alloc[2], 3)
                    n_buys += 1

            # slot 1·2: 신규 part2_rank ≤ 3
            current_slots = set(s for _, (_, _, _, s) in held.items())
            for slot_target in [1, 2]:
                if slot_target in current_slots:
                    continue
                if len(held) >= MAX_SLOTS:
                    break
                cands = []
                for tk, info in dd.items():
                    if info['p2'] is None or info['p2'] > 3:
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
                    cands.append((info['p2'], info['score'], tk))
                cands.sort(key=lambda x: x[0])
                if cands:
                    _, _, tk = cands[0]
                    held[tk] = (d, dd[tk]['price'], w_alloc[slot_target - 1], slot_target)
                    n_buys += 1
                    current_slots.add(slot_target)

            # 가중치 합 normalize (메가/신규 일부만 채워졌을 경우)
            total_w = sum(w for _, _, w, _ in held.values())
            if total_w > 0 and total_w != 1.0:
                # 가중치 재정규화 (각 slot 채워진 비율로)
                for tk in held:
                    ed, ep, w, sl = held[tk]
                    held[tk] = (ed, ep, w / total_w, sl)

        else:
            # baseline / V86e: slot 2
            MAX_SLOTS = 2
            if len(held) < MAX_SLOTS:
                cands = []
                for tk, info in dd.items():
                    if info['p2'] is None or info['p2'] > 3:
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
                    cands.append((info['p2'], info['score'], tk))
                cands.sort(key=lambda x: x[0])
                pick = cands[:2 - len(held)]
                if len(held) == 0 and len(pick) >= 2:
                    s1, s2 = pick[0][1], pick[1][1]
                    w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                    for si, (_, _, tk) in enumerate(pick[:2]):
                        if w[si] > 0:
                            held[tk] = (d, dd[tk]['price'], w[si], si + 1)
                            n_buys += 1
                else:
                    for _, _, tk in pick:
                        held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0, len(held) + 1)
                        n_buys += 1
        prev = dict(held)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100, n_buys=n_buys)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(variant, exclude=()):
    cums = []
    mdds = []
    buys = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            buys.append(r['n_buys'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg, buys


print('=' * 100)
print('V89 자율주행: slot 3 메가 전용 확장 BT')
print('=' * 100)

VARIANTS = [
    ('baseline', '메가홀드 없음, slot 2'),
    ('V86e', '현재 — V86e carryover, slot 2'),
    ('V89a', 'slot 3 메가전용 (50/30/20)'),
    ('V89b', 'slot 3 메가전용 (50/25/25)'),
    ('V89c', 'slot 3 메가전용 (40/30/30)'),
    ('V89d', 'slot 3 메가전용 (60/30/10)'),
    ('V89e', 'slot 3 메가전용 (균등 34/33/33)'),
    ('V89f', 'slot 3 메가 있을때만 활성 (50/30/20)'),
]

_, _, base_savg, _ = run('baseline')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

print(f'\n{"variant":<10}{"desc":<42}{"avg":>9}{"med":>9}{"MDD":>9}{"매수회수":>10}{"lift":>9}{"wins":>9}')
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
    print(f'{mk}{vid:<8}{desc:<42}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{avg_buys:>9.1f}{ls}')
    sys.stdout.flush()

# LOWO
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
        al = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        if exn == '전체':
            row += f'{al:>+8.1f}p({wins:>2})'
        else:
            row += f'{al:>+9.1f}p({wins:>2})'
    print(row)
    sys.stdout.flush()

# 부분기간
n_dates = len(dates)
front_starts = [ch for ch in seeds if all(s < n_dates // 2 for s in ch)]
back_starts = [ch for ch in seeds if all(s >= n_dates // 2 - 30 for s in ch)]
print(f'\n[부분기간 — 전반 {len(front_starts)} / 후반 {len(back_starts)}]')

def run_filtered(variant, sf, exclude=()):
    savg = []
    for ch in sf:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
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

# V86e vs V89 direct
print('\n[V86e vs V89 variants 직접 paired]')
_, _, v86e, _ = run('V86e')
for vid in ['V89a', 'V89b', 'V89c', 'V89d', 'V89e', 'V89f']:
    _, _, n, _ = run(vid)
    lifts = [y - x for x, y in zip(v86e, n)]
    al = sum(lifts) / len(lifts)
    w = sum(1 for l in lifts if l > 0)
    print(f'  {vid} - V86e: {al:+.1f}p ({w}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
