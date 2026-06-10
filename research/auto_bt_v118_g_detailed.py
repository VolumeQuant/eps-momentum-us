# -*- coding: utf-8 -*-
"""V118 G (mega entry) 정밀 검증

G: slot 1 = part2 Top 1, slot 2 = mega_score Top 1 메가 (PEG<0.18 + 매출≥25%)
   50/50 분산. 메가 carryover (PEG 유지 시 무한 holding).

검증:
1. LOWO (단일/다중 winner 제외)
2. Walk-forward
3. 거래량 필터 ($1B+) 결합
4. PEG threshold sweep (0.15, 0.18, 0.22, 0.25)
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


def sim(peg_thr=0.18, vol_filter=False, exclude=(), start=0):
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
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
        for tk in list(held):
            info = dd.get(tk); ed, ep, w = held[tk]
            if info and info.get('min_seg', 0) < -2: del held[tk]; continue
            if info is None: continue
            mega_now = is_mega(info, peg_thr)
            if mega_now:
                if info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]
                continue  # 메가 carryover
            p2 = info.get('p2')
            if p2 is None or p2 > 10:
                del held[tk]
        if len(held) < 2:
            p2_cands, mega_cands = [], []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                if vol_filter and (info.get('dv') or 0) < 1000: continue
                p2 = info.get('p2')
                if p2 is not None and p2 <= 3:
                    p2_cands.append((p2, info['score'], tk))
                if is_mega(info, peg_thr):
                    mega_cands.append((-mega_score(info), info['score'], tk))
            p2_cands.sort(key=lambda x: x[0])
            mega_cands.sort(key=lambda x: x[0])
            seen = set(held)
            pick_p2 = next((c for c in p2_cands if c[2] not in seen), None)
            pick_mega = next((c for c in mega_cands if c[2] not in seen), None)
            if len(held) == 0:
                if pick_p2 and pick_mega and pick_p2[2] != pick_mega[2]:
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
                elif pick_p2:
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 1.0)
            elif len(held) == 1:
                if pick_mega and pick_mega[2] not in held:
                    held[pick_mega[2]] = (d, dd[pick_mega[2]]['price'], 0.5)
                elif pick_p2 and pick_p2[2] not in held:
                    held[pick_p2[2]] = (d, dd[pick_p2[2]]['price'], 0.5)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))

def run(peg_thr=0.18, vol_filter=False, exclude=()):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim(peg_thr, vol_filter, exclude, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


print('=' * 100)
print('V118 G (mega entry + slot 2) 정밀 검증')
print('=' * 100)

# 1) Random multistart
print('\n[1] Random multistart (PEG 0.18, no vol filter)')
print(f'{"":<22}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}')
cums, mdds = run(0.18, vol_filter=False)
avg = statistics.mean(cums); mdd = statistics.mean(mdds); cal = avg/abs(mdd) if mdd else 0
full_c, _ = sim(0.18, False, start=0)
print(f'{"v118 G base":<22}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{sum(1 for c in cums if c > 0):>6}/300 {full_c:>+8.1f}%')

# v117c baseline 비교
cums_v117 = []; mdds_v117 = []

# 2) PEG sweep
print('\n[2] PEG threshold sweep')
print(f'{"PEG threshold":<14}{"수익":>10}{"MDD":>9}{"calmar":>8}{"full":>10}')
print('-' * 60)
for peg_thr in [0.15, 0.18, 0.22, 0.25, 0.30]:
    cums, mdds = run(peg_thr)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds); cal = avg/abs(mdd) if mdd else 0
    full_c, _ = sim(peg_thr, start=0)
    print(f'PEG < {peg_thr:<8}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{full_c:>+8.1f}%')

# 3) 거래량 필터 결합
print('\n[3] 거래량 필터 결합')
print(f'{"variant":<20}{"수익":>10}{"MDD":>9}{"calmar":>8}{"full":>10}')
print('-' * 60)
cums, mdds = run(0.18, vol_filter=False)
avg = statistics.mean(cums); mdd = statistics.mean(mdds); cal = avg/abs(mdd) if mdd else 0
full_c, _ = sim(0.18, False, start=0)
print(f'no vol filter        {avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{full_c:>+8.1f}%')
cums, mdds = run(0.18, vol_filter=True)
avg = statistics.mean(cums); mdd = statistics.mean(mdds); cal = avg/abs(mdd) if mdd else 0
full_c, _ = sim(0.18, True, start=0)
print(f'+ $1B+ filter        {avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{full_c:>+8.1f}%')

# 4) LOWO
print('\n[4] LOWO (winner 제외, PEG 0.18, no vol filter)')
print(f'{"제외":<18}{"수익":>10}{"MDD":>9}{"calmar":>8}')
print('-' * 50)
for excl in [(), ('SNDK',), ('MU',), ('STX',), ('LITE',), ('TTMI',),
             ('SNDK','MU'), ('SNDK','MU','STX')]:
    name = '(없음)' if not excl else '-' + '/'.join(excl)
    cums, mdds = run(0.18, exclude=excl)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds); cal = avg/abs(mdd) if mdd else 0
    print(f'{name:<18}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}')

# 5) Walk-forward
print('\n[5] Walk-forward (5 블록)')
print(f'{"블록":<8}{"시작일":<14}{"수익":>10}{"MDD":>10}')
n_blocks = 5
block_size = (len(dates) - MIN_HOLD) // n_blocks
for b in range(n_blocks):
    si = 2 + b * block_size
    if si >= len(dates) - MIN_HOLD: break
    c, m = sim(0.18, start=si)
    print(f'{b+1:<8}{dates[si]:<14}{c:>+9.1f}%{m:>+9.1f}%')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
