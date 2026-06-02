# -*- coding: utf-8 -*-
"""메가홀드 sweet spot — Phase 7: rev_exit 0.25 LOWO 검증 + 최종 결정

Phase 6 발견:
  - V86c (rev_exit 0.15): +82.5p, LOWO -MU-SNDK +11.8p (99/100) ★
  - V86d (rev_exit 0.20): +86.0p, LOWO -MU-SNDK -1.3p (32/100) ⚠️
  - rev_exit 0.25: +92.5p (인접성에서) — LOWO 미확인
  - rev_exit 0.30: +92.5p plateau

이번에 검증:
  - V86e (rev_exit 0.25) LOWO + 전체 검증
  - rev_exit 0.20~0.30 grid 더 정밀
  - 최종 선정
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


def sim(use_mega, exclude=(), start=0, peg_thr=0.20, rev_exit=0.15):
    held = {}
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
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
                if info.get('min_seg') is not None and info['min_seg'] < -2:
                    del held[tk]
                    continue
                if use_mega and rev_exit > 0 and info.get('rev_growth') is not None and info['rev_growth'] < rev_exit:
                    del held[tk]
                    continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                is_mega = (use_mega and info is not None and info.get('peg') is not None
                           and info['peg'] < peg_thr)
                if is_mega:
                    continue
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
                        held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(use_mega, exclude=(), peg_thr=0.20, rev_exit=0.15):
    cums = []
    mdds = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(use_mega, exclude=exclude, start=s, peg_thr=peg_thr, rev_exit=rev_exit)
            cums.append(r['cum'])
            mdds.append(r['mdd'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, mdds, savg


print('=' * 100)
print('Phase 7: rev_exit cutoff 정밀 + LOWO 종합')
print('=' * 100)

_, _, base_savg = run(False)
base_avg = sum(base_savg) / len(base_savg)
print(f'\nbaseline avg: {base_avg:+.1f}%')

print(f'\n{"rev_exit":>9}{"avg lift":>11}{"wins":>10}'
      f'{"LOWO -MU":>11}{"-SNDK":>11}{"-MU-SNDK":>12}{"-MU-SNDK-MCHP":>15}')
print('-' * 80)

best = None
for re_t in [0.00, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35]:
    use_mega = True
    # 전체
    _, _, n_all = run(use_mega, peg_thr=0.20, rev_exit=re_t)
    lifts_all = [b - a for a, b in zip(base_savg, n_all)]
    avg_lift = sum(lifts_all) / len(lifts_all)
    wins = sum(1 for l in lifts_all if l > 0)
    row = f'{re_t:>9.2f}{avg_lift:>+10.1f}p{wins:>7}/100'

    # LOWO
    lowo_results = {}
    for exn, ex in [('-MU', ('MU',)), ('-SNDK', ('SNDK',)),
                     ('-MU-SNDK', ('MU', 'SNDK')), ('-MU-SNDK-MCHP', ('MU', 'SNDK', 'MCHP'))]:
        _, _, b_ex = run(False, exclude=ex)
        _, _, n_ex = run(use_mega, exclude=ex, peg_thr=0.20, rev_exit=re_t)
        lifts = [y - x for x, y in zip(b_ex, n_ex)]
        ll = sum(lifts) / len(lifts)
        lw = sum(1 for l in lifts if l > 0)
        lowo_results[exn] = (ll, lw)
        if exn != '-MU-SNDK-MCHP':
            row += f'{ll:>+9.1f}p({lw:>2})'
        else:
            row += f'{ll:>+12.1f}p({lw:>2})'

    print(row)
    sys.stdout.flush()

    # 견고성 판정: LOWO -MU-SNDK ≥ 0 AND wins ≥ 80
    lowo_ms = lowo_results['-MU-SNDK'][0]
    lowo_ms_wins = lowo_results['-MU-SNDK'][1]
    if avg_lift > 0 and wins >= 95 and lowo_ms >= 0 and lowo_ms_wins >= 80:
        if best is None or avg_lift > best[1]:
            best = (re_t, avg_lift, wins, lowo_ms, lowo_ms_wins)

# 최종 결정
print('\n' + '=' * 100)
print('최종 결정')
print('=' * 100)

if best:
    re_t, lift, wins, lowo_ms, lowo_ms_wins = best
    print(f'★ 견고성 통과 + 알파 최대: rev_exit = {re_t}')
    print(f'  전체 lift: {lift:+.1f}p ({wins}/100)')
    print(f'  LOWO -MU-SNDK: {lowo_ms:+.1f}p ({lowo_ms_wins}/100)')
else:
    print('견고성 통과 cutoff 없음. 보수: rev_exit 0.15 권고.')

# 두 가지 후보 정직 비교
print('\n[정직한 trade-off 비교]')
print('  안전(robust) 후보: rev_exit 0.15 — lift +82.5p, LOWO -MU-SNDK +11.8p')
print('  공격(alpha) 후보 : rev_exit 0.25 — lift +92.5p, LOWO ?')
print('  → LOWO 결과에 따라 결정')

# rev_exit=0 (메가홀드만, exit 없음 = V86b 등가)
print('\n[참고: rev_exit=0 (V86b 등가, exit 없음)]')
_, _, n0 = run(True, peg_thr=0.20, rev_exit=0)
lifts0 = [b - a for a, b in zip(base_savg, n0)]
print(f'  lift {sum(lifts0)/len(lifts0):+.1f}p ({sum(1 for l in lifts0 if l > 0)}/100)')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
