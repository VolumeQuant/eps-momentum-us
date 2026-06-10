# -*- coding: utf-8 -*-
"""V118 patch 후보 BT:
- A: V118 baseline (slot 2)
- B: V118 + 재진입 우선 (매도 후 7일 내 메가 part2 Top 5 재진입 시 우선)
- C: V118 + 슬롯 3 (slot1 part2 + slot2 mega + slot3 part2 Top 2)
- D: B + C 결합

재진입 우선: 매도 종목이 7일 내 part2 Top 5 재진입 + 메가 시그니처 → 우선 매수
슬롯 3: 1/3 비중씩, slot1 part2 Top 1, slot2 mega Top 1, slot3 part2 Top 2 (메가 제외)
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

data_all = {}
for d in dates:
    data_all[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth,dollar_volume_30d FROM ntm_screening WHERE date=?', (d,)).fetchall():
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]; dv = r[12]; p = r[3]
        peg = (p/nc)/(rg*100) if (p and nc and nc > 0 and rg and rg > 0) else None
        ntm_rev = (nc/n90-1)*100 if (nc and n90 and n90 > 0) else 0
        rg_pct = rg*100 if rg else 0
        peg_inv = 1/peg if peg and peg > 0 else 0
        data_all[d][tk] = dict(p2=r[1], cr=r[2], price=p, score=r[4] or 0,
                               min_seg=min(segs) if segs else 0, high30=r[10], rev_growth=rg,
                               dv=dv, peg=peg, ntm_rev=ntm_rev, rg_pct=rg_pct, peg_inv=peg_inv)
pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data_all[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True


def is_mega(info, peg_thr=0.18):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True


def mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)


def sim(variant, max_slots, exclude=(), start=0):
    """variants:
       A: V118 base (slot 2)
       B: V118 + reentry priority
       C: V118 + slot 3
       D: V118 + reentry + slot 3
    """
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
    recent_exits = {}  # ticker -> exit_idx
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val)
            mdd = min(mdd, (val/peak - 1) * 100)
        dd = data_all[d]
        # 매도
        for tk in list(held):
            info = dd.get(tk); ed, ep, w = held[tk]
            if info and info.get('min_seg', 0) < -2:
                recent_exits[tk] = i; del held[tk]; continue
            if info is None: continue
            mega_now = is_mega(info)
            if mega_now:
                if info.get('rev_growth') and info['rev_growth'] < 0.25:
                    recent_exits[tk] = i; del held[tk]
                continue
            p2 = info.get('p2')
            if p2 is None or p2 > 10:
                recent_exits[tk] = i; del held[tk]
        # 매수
        if len(held) < max_slots:
            p2_cands, mega_cands, p2_2nd_cands = [], [], []
            reentry_cands = []  # 재진입 우선 후보
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                if (info.get('dv') or 0) < 1000: continue
                p2 = info.get('p2')
                # 재진입 우선 (variants B, D)
                if variant in ('B', 'D') and tk in recent_exits and i - recent_exits[tk] <= 7:
                    if is_mega(info) and p2 is not None and p2 <= 5:
                        reentry_cands.append((p2, info['score'], tk))
                if p2 is not None and p2 <= 3:
                    p2_cands.append((p2, info['score'], tk))
                if p2 is not None and 1 < p2 <= 3 and not is_mega(info):
                    p2_2nd_cands.append((p2, info['score'], tk))
                if is_mega(info):
                    mega_cands.append((-mega_score(info), info['score'], tk))
            reentry_cands.sort(key=lambda x: x[0])
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])
            p2_2nd_cands.sort(key=lambda x: x[0])

            # 재진입 우선 (B, D)
            if variant in ('B', 'D') and reentry_cands and len(held) < max_slots:
                tk = reentry_cands[0][2]
                if tk not in held:
                    held[tk] = (d, dd[tk]['price'], 0)

            # slot 1: part2 Top 1
            if len(held) < max_slots and p2_cands:
                pick = next((c for c in p2_cands if c[2] not in held), None)
                if pick:
                    held[pick[2]] = (d, dd[pick[2]]['price'], 0)

            # slot 2: mega Top 1
            if len(held) < max_slots and mega_cands:
                pick = next((c for c in mega_cands if c[2] not in held), None)
                if pick:
                    held[pick[2]] = (d, dd[pick[2]]['price'], 0)

            # slot 3: part2 Top 2 (변형 C, D만)
            if variant in ('C', 'D') and len(held) < max_slots and p2_2nd_cands:
                pick = next((c for c in p2_2nd_cands if c[2] not in held), None)
                if pick:
                    held[pick[2]] = (d, dd[pick[2]]['price'], 0)

            # rebalance
            if held:
                n = len(held); w_each = 1.0 / n
                for tk in held:
                    ed, ep, _ = held[tk]
                    held[tk] = (ed, ep, w_each)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(variant, max_slots, exclude=()):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim(variant, max_slots, exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


def report(name, variant, max_slots):
    cums, mdds = run(variant, max_slots)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg/abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    full_c, _ = sim(variant, max_slots, start=0)
    print(f'{name:<30}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{pos:>6}/300 {full_c:>+8.1f}%')
    return cums


print('=' * 105)
print('V118 patch 후보 BT — 재진입 우선 + 슬롯 3개')
print('=' * 105)
print(f'{"variant":<30}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}')
print('-' * 95)
base_cums = report('A. V118 base (slot 2)', 'A', 2)
b_cums = report('B. V118 + reentry (slot 2)', 'B', 2)
c_cums = report('C. V118 + slot 3', 'C', 3)
d_cums = report('D. V118 + reentry + slot 3', 'D', 3)

# paired diff
print('\npaired diff (vs V118 base):')
for name, cums in [('B', b_cums), ('C', c_cums), ('D', d_cums)]:
    diffs = [v-b for v, b in zip(cums, base_cums)]
    avg = statistics.mean(diffs); wins = sum(1 for d in diffs if d > 0)
    print(f'  {name}: avg {avg:+.1f}p, wins {wins}/{len(diffs)}')

# LOWO
print('\nLOWO (variants 비교)')
print(f'{"제외":<20}{"A. base":>12}{"B. reentry":>14}{"C. slot 3":>13}{"D. both":>13}')
print('-' * 75)
for excl in [(), ('SNDK',), ('MU',), ('STX',), ('SNDK','MU'), ('SNDK','MU','STX')]:
    name = '(없음)' if not excl else '-' + '/'.join(excl)
    line = f'{name:<20}'
    for v, slots in [('A', 2), ('B', 2), ('C', 3), ('D', 3)]:
        cums = run(v, slots, exclude=excl)[0]
        line += f'{statistics.mean(cums):>+12.1f}%'
    print(line)

# Walk-forward
print('\nWalk-forward (5 블록)')
n_blocks = 5
block_size = (len(dates) - MIN_HOLD) // n_blocks
print(f'{"블록":<8}{"시작일":<14}{"A. base":>12}{"B. reentry":>14}{"C. slot 3":>13}{"D. both":>13}')
for b in range(n_blocks):
    si = 2 + b * block_size
    if si >= len(dates) - MIN_HOLD: break
    line = f'{b+1:<8}{dates[si]:<14}'
    for v, slots in [('A', 2), ('B', 2), ('C', 3), ('D', 3)]:
        c, _ = sim(v, slots, start=si)
        line += f'{c:>+12.1f}%'
    print(line)

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
