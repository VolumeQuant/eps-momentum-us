# -*- coding: utf-8 -*-
"""V94 자율주행 — BT 좋아지면서 메가 자연 1·2위 새 방법

사용자 요구: "BT 결과 좋아지도록 다른 방법 생각해봐"

이전 시도 실패 원인 (V87/V88/V89/V91):
- 메가 1·2위 차지 → 신규 차단 → 알파 손실
- V89 slot 3: 회전 폭발 (19.8회 vs 8.6) — slot 1·2 일반 logic 매도

새 시도:
  N1: slot 3 + entry rank ≤ 2 (slot 1·2 strict) — V89 회전 해결
  N2: 메가 strong only (PEG<0.10) part2_rank 강제 + 일반 메가는 carryover
  N3: 메가 동적 부스트 — seg1 > 0 AND seg1 > mean_seg×1.5 (가속 강한 메가만)
  N4: composite_rank 메가 보너스 (BT 영향 0, 표시만)
  N5: slot 3 + 메가 가중치 작게 (메가 20%, 신규 40%, 40%)
  N6: 메가 carryover + 신규 매수 시 메가 시그니처 종목 우선 (entry rank ≤ 3 + PEG<0.22 우선)
  N7: 동적 slot — 메가 있을때만 slot 3, 매도 strict (rank>5)
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
                           segs=segs, min_seg=min(segs) if segs else 0,
                           mean_seg=sum(segs)/len(segs) if segs else 0,
                           seg1=segs[0] if segs else 0,
                           high30=r[10], peg=peg, rev_growth=rg)

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


def is_mega(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


def is_strong_mega(info):
    """N3: 가속 강한 메가만 — PEG<0.22 AND seg1 > 0 AND seg1 > mean_seg × 1.5"""
    if not is_mega(info):
        return False
    s1 = info.get('seg1', 0)
    ms = info.get('mean_seg', 0)
    if s1 <= 0:
        return False
    if ms != 0 and s1 < ms * 1.5:
        return False
    return True


def sim_baseline_or_v86e(config, exclude=(), start=0):
    """baseline (mega_carry=False) 또는 V86e+ (mega_carry=True) 시뮬"""
    mega_carry = config.get('mega_carry', True)
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
                if mega_carry and info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if mega_carry and is_mega(info):
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


def sim_v94(variant, exclude=(), start=0):
    """V94 variants — slot 3 또는 dynamic"""
    held = {}  # tk -> (ed, ep, w, slot)  slot 3 = 메가 전용
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
                if info.get('min_seg', 0) < -2:
                    del held[tk]; continue
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.25:
                    del held[tk]; continue
            p2 = info.get('p2') if info else None

            # variant 별 매도 조건
            if variant == 'N1':  # slot 3 + strict entry. 매도: slot 1·2 rank>10, slot 3 메가 보호
                if info is None or p2 is None or p2 > 10:
                    if sl == 3 and is_mega(info):
                        continue
                    del held[tk]
            elif variant == 'N5':  # slot 3 + 메가 작게
                if info is None or p2 is None or p2 > 10:
                    if sl == 3 and is_mega(info):
                        continue
                    del held[tk]
            elif variant == 'N7':  # 동적 slot + 매도 strict rank>5
                cutoff = 5
                if info is None or p2 is None or p2 > cutoff:
                    if sl == 3 and is_mega(info):
                        continue
                    del held[tk]
            elif variant == 'N2':  # strong only mega ranks 1; 일반 mega carryover
                if info is None or p2 is None or p2 > 10:
                    if is_mega(info):  # 일반 메가 carryover
                        continue
                    del held[tk]
            elif variant == 'N3':  # 가속 강한 메가만 carryover (PEG<0.22 AND seg1>mean*1.5)
                if info is None or p2 is None or p2 > 10:
                    if is_strong_mega(info):
                        continue
                    del held[tk]
            elif variant == 'N6':  # 메가 carryover + 매수 메가 우선
                if info is None or p2 is None or p2 > 10:
                    if is_mega(info):
                        continue
                    del held[tk]

        # 매수
        MAX_SLOTS = 3 if variant in ('N1', 'N5', 'N7') else 2

        # variant N7: 메가 있을때만 slot 3
        if variant == 'N7':
            has_mega = any(is_mega(info) for info in dd.values())
            MAX_SLOTS = 3 if has_mega else 2

        # variant 별 매수 logic
        if variant in ('N1', 'N5', 'N7'):
            # slot 3 메가 전용
            current_slots = set(sl for _, (_, _, _, sl) in held.items())
            if 3 not in current_slots and MAX_SLOTS == 3:
                mega_cands = []
                for tk, info in dd.items():
                    if not is_mega(info):
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
                    p2_val = info.get('p2') or 999
                    mega_cands.append((p2_val, info['score'], tk))
                mega_cands.sort(key=lambda x: x[0])
                if mega_cands:
                    _, _, tk = mega_cands[0]
                    if variant == 'N1':
                        w_mega = 0.33
                    elif variant == 'N5':
                        w_mega = 0.20
                    else:  # N7
                        w_mega = 0.33
                    held[tk] = (d, dd[tk]['price'], w_mega, 3); n_buys += 1

            # slot 1·2: 일반 신규 (entry rank ≤ 2 for N1, ≤3 for N5/N7)
            entry_max = 2 if variant == 'N1' else 3
            current_slots = set(sl for _, (_, _, _, sl) in held.items())
            for slot_target in [1, 2]:
                if slot_target in current_slots:
                    continue
                if len([1 for _, (_, _, _, sl) in held.items() if sl in (1, 2)]) >= 2:
                    break
                cands = []
                for tk, info in dd.items():
                    if info['p2'] is None or info['p2'] > entry_max:
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
                if cands:
                    _, _, tk = cands[0]
                    if variant == 'N1':
                        w_new = 0.33
                    elif variant == 'N5':
                        w_new = 0.40
                    else:  # N7
                        w_new = 0.33
                    held[tk] = (d, dd[tk]['price'], w_new, slot_target); n_buys += 1
                    current_slots.add(slot_target)

            # weights normalize
            total_w = sum(w for _, _, w, _ in held.values())
            if total_w > 0 and abs(total_w - 1.0) > 0.01:
                for tk in held:
                    ed, ep, w, sl = held[tk]
                    held[tk] = (ed, ep, w / total_w, sl)
        elif variant == 'N6':  # 메가 우선 매수
            if len(held) < 2:
                # 메가 우선 후보 먼저
                mega_cands = []
                normal_cands = []
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
                    if is_mega(info):
                        mega_cands.append((info['p2'], info['score'], tk))
                    else:
                        normal_cands.append((info['p2'], info['score'], tk))
                mega_cands.sort(key=lambda x: x[0])
                normal_cands.sort(key=lambda x: x[0])
                pick = (mega_cands + normal_cands)[:2 - len(held)]
                if len(held) == 0 and len(pick) >= 2:
                    s1, s2 = pick[0][1], pick[1][1]
                    w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                    for si, (_, _, tk) in enumerate(pick[:2]):
                        if w[si] > 0:
                            held[tk] = (d, dd[tk]['price'], w[si], si + 1); n_buys += 1
                else:
                    for _, _, tk in pick:
                        held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0, len(held) + 1); n_buys += 1
        elif variant in ('N2', 'N3'):
            # slot 2, 매수는 일반 logic (V86e+ 동일)
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
                            held[tk] = (d, dd[tk]['price'], w[si], si + 1); n_buys += 1
                else:
                    for _, _, tk in pick:
                        held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0, len(held) + 1); n_buys += 1
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
    sim_fn = sim_baseline_or_v86e if variant in ('baseline', 'V86e+') else sim_v94
    for ch in seeds:
        sr = []
        for s in ch:
            if variant == 'baseline':
                r = sim_fn(dict(mega_carry=False), exclude=exclude, start=s)
            elif variant == 'V86e+':
                r = sim_fn(dict(mega_carry=True), exclude=exclude, start=s)
            else:
                r = sim_v94(variant, exclude=exclude, start=s)
            cums.append(r['cum'])
            buys.append(r['n_buys'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, savg, buys


print('=' * 100)
print('V94 자율주행: BT 좋아지면서 메가 자연 1·2위 새 방법')
print('=' * 100)

_, base_savg, _ = run('baseline')
print(f'\nbaseline avg: {sum(base_savg)/len(base_savg):+.1f}%')

_, v86e_savg, v86e_buys = run('V86e+')
print(f'V86e+ avg: {sum(v86e_savg)/len(v86e_savg):+.1f}%, 매수 {statistics.mean(v86e_buys):.1f}')

VARIANTS = [
    ('N1', 'slot 3 + entry rank≤2 strict'),
    ('N2', '메가 strong (PEG<0.10) priority + 일반 carryover'),
    ('N3', '가속 강한 메가만 carryover (seg1>mean×1.5)'),
    ('N5', 'slot 3 + 메가 20%/신규 40·40'),
    ('N6', '슬롯 2, 메가 매수 우선 (메가 후보 먼저)'),
    ('N7', '동적 slot — 메가 있을때만 + 매도 rank>5'),
]

print(f'\n{"variant":<6}{"desc":<50}{"avg lift":>12}{"wins":>10}{"매수":>10}{"LOWO -MU-SNDK":>18}')
print('-' * 110)

for vid, desc in VARIANTS:
    cums, savg, buys = run(vid)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    avg_buys = statistics.mean(buys)
    _, b_ex, _ = run('baseline', exclude=('MU', 'SNDK'))
    _, n_ex, _ = run(vid, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    print(f'  {vid:<4}{desc:<50}{avg:>+10.1f}p{wins:>7}/100{avg_buys:>9.1f}{al_lowo:>+10.1f}p({w_lowo:>2})')
    sys.stdout.flush()

# V86e+ vs V94 direct paired
print('\n[V86e+ vs V94 variants direct paired]')
for vid, _ in VARIANTS:
    _, n, _ = run(vid)
    lifts = [y - x for x, y in zip(v86e_savg, n)]
    al = sum(lifts)/len(lifts)
    w = sum(1 for l in lifts if l > 0)
    print(f'  {vid} - V86e+: {al:+.1f}p ({w}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
