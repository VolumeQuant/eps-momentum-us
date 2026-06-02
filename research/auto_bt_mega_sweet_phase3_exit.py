# -*- coding: utf-8 -*-
"""메가홀드 sweet spot — Phase 3: 해제 조건 검증

Phase 2 발견: PEG<0.20 only가 진짜 sweet spot (NTM 조건은 over-engineered).
이번에 검증: 해제 조건 (min_seg cutoff + 추가 트리거).

variants:
  E0: baseline (no override)
  E1: PEG<0.20, min_seg<-2 매도 (현재)
  E2: PEG<0.20, min_seg<-1 매도 (더 민감)
  E3: PEG<0.20, min_seg<-3 매도 (덜 민감)
  E4: PEG<0.20, min_seg<-5 매도 (매우 둔감)
  E5: PEG<0.20, min_seg<-2 매도 + rev_growth<0.15 매도 (매출성장 약화)
  E6: PEG<0.20, min_seg<-2 매도 + PEG≥0.30 매도 (가격 상승해서 PEG 식어짐)
  E7: PEG<0.20, min_seg<-2 매도 + 최대 보유 60거래일 cap
  E8: PEG<0.20, min_seg<-2 매도 + MA60 이탈 매도

각 paired 100×3, lift / wins / Calmar / 평균 보유기간.
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
        ntm_rev = (nc / n90 - 1) * 100 if n90 and n90 > 0 else None
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           min_seg=min(segs) if segs else 0, high30=r[10],
                           ntm_rev=ntm_rev, peg=peg, rev_growth=rg)

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


# PEG-only check (Phase 2 sweet spot)
def is_mega(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.20


def sim(exit_variant='E0', exclude=(), start=0):
    """exit_variant 별 해제 조건"""
    held = {}      # tk -> (entry_date, entry_price, weight, entry_idx)
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
    n_trades = 0
    total_hold_days = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i - 1]
            ret = 0
            for tk, (ed, ep, w, eidx) in prev.items():
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
            ed, ep, w, eidx = held[tk]

            # 매도 트리거 변형
            sell_reason = None

            # 공통: min_seg cutoff
            if exit_variant == 'E2':
                seg_thr = -1
            elif exit_variant == 'E3':
                seg_thr = -3
            elif exit_variant == 'E4':
                seg_thr = -5
            else:
                seg_thr = -2

            if info is not None and info.get('min_seg') is not None and info['min_seg'] < seg_thr:
                sell_reason = 'minseg'

            # 매출 성장 약화
            if exit_variant == 'E5' and sell_reason is None and info is not None:
                if info.get('rev_growth') is not None and info['rev_growth'] < 0.15:
                    sell_reason = 'rev_weak'

            # PEG 식어짐
            if exit_variant == 'E6' and sell_reason is None and info is not None:
                if info.get('peg') is not None and info['peg'] >= 0.30:
                    sell_reason = 'peg_high'

            # 보유 cap
            if exit_variant == 'E7' and sell_reason is None:
                if i - eidx >= 60:
                    sell_reason = 'cap60'

            # rank>10 → 메가 아니면 매도
            if sell_reason is None:
                p2 = info.get('p2') if info else None
                if info is None or p2 is None or p2 > 10:
                    if exit_variant != 'E0' and is_mega(info):
                        pass  # 메가홀드
                    else:
                        sell_reason = 'rank'

            if sell_reason:
                total_hold_days += (i - eidx)
                n_trades += 1
                del held[tk]

        if len(held) < 2:
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
                        held[tk] = (d, dd[tk]['price'], w[si], i)
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0, i)
        prev = {k: v for k, v in held.items()}
    avg_hold = total_hold_days / max(1, n_trades)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100, n_trades=n_trades, avg_hold=avg_hold)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(variant, exclude=()):
    cums = []
    mdds = []
    savg = []
    holds = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(variant, exclude=exclude, start=s)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            holds.append(r['avg_hold'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg, holds


print('=' * 100)
print('Phase 3: 메가홀드 해제 조건 sweet spot (기준: PEG<0.20 only)')
print('=' * 100)

VARIANTS = [
    ('E0', 'baseline (메가홀드 없음)'),
    ('E1', 'PEG<0.20 + min_seg<-2 (현재)'),
    ('E2', 'PEG<0.20 + min_seg<-1 (민감)'),
    ('E3', 'PEG<0.20 + min_seg<-3 (둔감)'),
    ('E4', 'PEG<0.20 + min_seg<-5 (매우둔감)'),
    ('E5', 'E1 + rev_growth<15% 매도'),
    ('E6', 'E1 + PEG≥0.30 매도 (식어짐)'),
    ('E7', 'E1 + 60거래일 cap'),
]

_, _, base_savg, _ = run('E0')
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

print(f'\n{"variant":<6}{"desc":<38}{"avg":>9}{"med":>9}{"MDD":>9}{"평균보유":>10}{"lift":>9}{"wins":>9}')
print('-' * 100)

results = {}
for vid, desc in VARIANTS:
    cums, mdds, savg, holds = run(vid)
    avg = statistics.mean(cums)
    med = statistics.median(cums)
    mdd_med = statistics.median(mdds)
    avg_hold = statistics.mean(holds)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    results[vid] = (avg, mdd_med, avg_lift, wins, avg_hold)
    mk = ' ★' if vid == 'E0' else '  '
    ls = '' if vid == 'E0' else f'{avg_lift:>+7.1f}p{wins:>6}/100'
    print(f'{mk}{vid:<4}{desc:<38}{avg:>+8.1f}%{med:>+8.1f}%{mdd_med:>8.1f}%{avg_hold:>9.1f}d{ls}')
    sys.stdout.flush()

# LOWO for viable variants
viable = [vid for vid, (_, _, lift, wins, _) in results.items() if lift > 0 and wins >= 80 and vid != 'E0']
print(f'\n[LOWO -MU-SNDK 견고성 (viable: {len(viable)})]')
print(f'{"variant":<6}{"전체":>11}{"-MU":>13}{"-SNDK":>13}{"-MU-SNDK":>13}')
print('-' * 60)

for vid in viable:
    row = f'{vid:<6}'
    _, _, b_all, _ = run('E0')
    _, _, n_all, _ = run(vid)
    lift_all = sum(y - x for x, y in zip(b_all, n_all)) / len(b_all)
    wins_all = sum(1 for l in [y - x for x, y in zip(b_all, n_all)] if l > 0)
    row += f'{lift_all:>+8.1f}p({wins_all:>2})'
    for exn, ex in [('-MU', ('MU',)), ('-SNDK', ('SNDK',)), ('-MU-SNDK', ('MU', 'SNDK'))]:
        _, _, b, _ = run('E0', exclude=ex)
        _, _, n, _ = run(vid, exclude=ex)
        lifts = [y - x for x, y in zip(b, n)]
        avg_lift = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        row += f'{avg_lift:>+9.1f}p({wins:>2})'
    print(row)
    sys.stdout.flush()

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
