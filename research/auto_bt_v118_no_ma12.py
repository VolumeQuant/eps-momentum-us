# -*- coding: utf-8 -*-
"""V118 자율주행 — MA12 제거 + 메가 carryover + 슬롯 sweep + 재진입

사용자 명령:
- MA12 조건 제거
- SNDK 같은 메가 놓치는 문제 해결
- 전부 BT 연구

variants:
- v117c (현재): MA12 + 슬롯 2 + part2 Top 3 + $1B+
- A. no MA12, slot 2: 매도 = 순위>10 OR EPS꺾임
- B. no MA12, slot 3: 슬롯 확장
- C. no MA12, slot 4
- D. 메가 carryover (PEG<0.18), slot 2: 보유 메가 무한 holding
- E. 메가 carryover, slot 3
- F. 메가 carryover + 재진입 우선 (매도 후 5일 내 part2 Top 재진입 시 우선)
- G. 메가 entry boost (V110 style): slot 1 part2 + slot 2 mega Top
- H. 종합: 메가 carryover + slot 3 + 재진입
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10
WHIPSAW_GAP = -0.10

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


def above_ma12(tk, i):
    if i < 6: return True
    prices = [pf[dates[j]].get(tk) for j in range(max(0, i-11), i+1) if pf[dates[j]].get(tk)]
    if len(prices) < 6: return True
    return (pf[dates[i]].get(tk) or 0) > sum(prices)/len(prices)


def today_gap(tk, i):
    if i < 1: return 0
    cur_p = pf[dates[i]].get(tk); prev_p = pf[dates[i-1]].get(tk)
    if not cur_p or not prev_p: return 0
    return cur_p / prev_p - 1


def is_mega(info, peg_thr=0.18):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    return True


def mega_score(info):
    return info.get('ntm_rev', 0) + info.get('rg_pct', 0) + 50 * info.get('peg_inv', 0)


def sim(variant, max_slots, exclude=(), start=0):
    """variants:
       v117c: MA12 + slot N + Top 3 + $1B+
       no_ma12: MA12 제거 + slot N
       mega_carry: 메가 carryover (무한 holding) + slot N
       mega_entry: slot 1=part2, slot 2=mega
       reentry: 메가 carryover + 매도 후 7일 내 재진입 우선
    """
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
    recent_exits = {}  # ticker -> exit_day_index (for reentry priority)
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w, _) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val)
            mdd = min(mdd, (val/peak - 1) * 100)
        dd = data_all[d]
        # 매도
        for tk in list(held):
            info = dd.get(tk); ed, ep, w, grace = held[tk]
            # EPS꺾임 즉시매도 (공통)
            if info and info.get('min_seg', 0) < -2:
                recent_exits[tk] = i
                del held[tk]; continue
            if info is None: continue
            p2 = info.get('p2')
            mega_now = is_mega(info)

            if variant == 'v117c':
                # MA12 + 순위 + 휩쏘 보험
                if (p2 is None or p2 > 10) and not above_ma12(tk, i):
                    if today_gap(tk, i) <= WHIPSAW_GAP and not grace:
                        held[tk] = (ed, ep, w, True); continue
                    recent_exits[tk] = i
                    del held[tk]
            elif variant == 'no_ma12':
                # MA12 제거, 순위>10이면 즉시 매도
                if p2 is None or p2 > 10:
                    recent_exits[tk] = i
                    del held[tk]
            elif variant.startswith('mega_carry'):
                # 메가 carryover: 메가 시그니처 유지 시 holding (PEG/매출 + min_seg)
                if mega_now:
                    if info.get('rev_growth') < 0.25:
                        recent_exits[tk] = i
                        del held[tk]
                    continue  # 메가 holding
                else:
                    # 메가 아니면 순위 logic
                    if p2 is None or p2 > 10:
                        recent_exits[tk] = i
                        del held[tk]
            elif variant == 'mega_entry':
                # V110-style: 메가 carryover + 메가 슬롯 별도
                if mega_now:
                    if info.get('rev_growth') < 0.25:
                        recent_exits[tk] = i
                        del held[tk]
                    continue
                if p2 is None or p2 > 10:
                    recent_exits[tk] = i
                    del held[tk]
            elif variant == 'reentry':
                # 메가 carryover + 재진입 우선
                if mega_now:
                    if info.get('rev_growth') < 0.25:
                        recent_exits[tk] = i
                        del held[tk]
                    continue
                if p2 is None or p2 > 10:
                    recent_exits[tk] = i
                    del held[tk]

        # 매수
        if len(held) < max_slots:
            cands = []
            mega_cands = []
            for tk, info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is None or p2 > 3: continue
                if (info.get('dv') or 0) < 1000: continue
                # 재진입 우선 (recent_exits 7일 내 + 메가)
                priority = 0
                if variant == 'reentry':
                    if tk in recent_exits and i - recent_exits[tk] <= 7 and is_mega(info):
                        priority = -100  # 우선
                cands.append((priority, p2, info['score'], tk))
                if variant == 'mega_entry' and is_mega(info):
                    mega_cands.append((-mega_score(info), p2, info['score'], tk))
            cands.sort(key=lambda x: (x[0], x[1]))
            mega_cands.sort(key=lambda x: x[0])

            if variant == 'mega_entry':
                # slot 1: part2 Top 1 + slot 2: mega Top 1
                pick = []
                if cands:
                    pick.append(cands[0])
                if mega_cands and (not pick or mega_cands[0][3] != pick[0][3]):
                    pick.append(mega_cands[0])
                pick = pick[:max_slots-len(held)]
                if len(held) == 0 and len(pick) >= 2:
                    for c in pick[:2]: held[c[3]] = (d, dd[c[3]]['price'], 0.5, False)
                elif len(held) == 0 and len(pick) == 1:
                    held[pick[0][3]] = (d, dd[pick[0][3]]['price'], 1.0, False)
                else:
                    for c in pick:
                        if c[3] not in held:
                            w = 1.0 / max_slots if len(held) == 0 else 0.5
                            held[c[3]] = (d, dd[c[3]]['price'], w, False)
            else:
                pick = cands[:max_slots-len(held)]
                if len(held) == 0 and len(pick) > 1:
                    w_init = 1.0 / min(len(pick), max_slots)
                    for c in pick: held[c[3]] = (d, dd[c[3]]['price'], w_init, False)
                elif len(held) == 0 and len(pick) == 1:
                    held[pick[0][3]] = (d, dd[pick[0][3]]['price'], 1.0, False)
                else:
                    for c in pick:
                        w = 1.0 / max_slots
                        held[c[3]] = (d, dd[c[3]]['price'], w, False)
                # rebalance weights to sum to 1.0
                if held:
                    n = len(held); w_each = 1.0 / n
                    for tk in held:
                        ed, ep, w_old, gr = held[tk]
                        held[tk] = (ed, ep, w_each, gr)
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
    print(f'{name:<32}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{pos:>6}/300 {full_c:>+8.1f}%')
    return cums


print('=' * 105)
print('V118 자율주행: MA12 제거 + 메가 carryover + 슬롯 sweep + 재진입')
print('=' * 105)
print(f'{"variant":<32}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}')
print('-' * 95)

# 1) v117c baseline (현재 production)
report('v117c (MA12+slot2)', 'v117c', 2)

# 2-4) no MA12 + slot sweep
report('A. no MA12 + slot 2', 'no_ma12', 2)
report('B. no MA12 + slot 3', 'no_ma12', 3)
report('C. no MA12 + slot 4', 'no_ma12', 4)

# 5-7) 메가 carryover + slot sweep
report('D. mega carry + slot 2', 'mega_carry', 2)
report('E. mega carry + slot 3', 'mega_carry', 3)
report('F. mega carry + slot 4', 'mega_carry', 4)

# 8) 메가 entry (V110 style)
report('G. mega entry + slot 2', 'mega_entry', 2)

# 9) 재진입 우선
report('H. reentry + slot 2', 'reentry', 2)
report('I. reentry + slot 3', 'reentry', 3)

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
