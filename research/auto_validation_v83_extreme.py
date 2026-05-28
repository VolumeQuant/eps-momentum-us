# -*- coding: utf-8 -*-
"""
자율검증 v3 (확장): 90/10이 80/20보다 일관 우월하다는 발견을 검증
- Test A: 극단 비중 (90/10 ~ 100/0 vs S=1)
- Test B: 다중 winner 제거 동시 (단일 leave-out 한계 보완)
- Test C: 90/10 vs 80/20 random-start paired (직접)
- Test D: 3-way 시간 분할 (전/중/후)
- Test E: 슬롯 2 + 비중에서 2등 슬롯의 효용 (있을 때 vs 없을 때)
"""
import sqlite3, random, statistics, math, itertools

DB = "eps_momentum_data.db"

print("=" * 72)
print("Loading...")
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute(
    "SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
day = {}; price_map = {}
for d in dates:
    rows = cur.execute(
        "SELECT ticker, part2_rank, composite_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price "
        "FROM ntm_screening WHERE date=?", (d,)).fetchall()
    day_d = {}; price_d = {}
    for r in rows:
        t, p2, cr, n0, n7, n30, n60, n90, px = r
        def seg(a, b):
            if a is None or b is None or b == 0: return None
            return (a - b) / abs(b) * 100
        segs = [s for s in [seg(n0,n7), seg(n7,n30), seg(n30,n60), seg(n60,n90)] if s is not None]
        ms = min(segs) if segs else None
        if p2 is not None: day_d[t] = (p2, cr, ms)
        if px is not None: price_d[t] = px
    day[d] = day_d; price_map[d] = price_d
con.close()
print(f"Loaded {len(dates)} dates")


def verified_cr(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        info = day[dates[j]].get(t)
        if not info or info[1] is None or info[1] > 30: return False
    return True

def simulate(entry, exit_thr, slots, weights, exclude=None, start_idx=0, end_idx=None):
    exclude = exclude or set()
    end_idx = end_idx or len(dates)
    held = {}; prev_held = None
    value = 1.0; peak = 1.0; mdd = 0.0; dr = []
    for i in range(start_idx, end_idx):
        d = dates[i]
        if prev_held and i > start_idx:
            d_prev = dates[i-1]
            items = list(prev_held.keys())
            ranks = sorted(((t, day[d_prev].get(t,(999,None,None))[0] or 999) for t in items), key=lambda x: x[1])
            n = len(items); ret = 0
            if n > 0:
                w_slice = list(weights[:n]); wsum = sum(w_slice) or 1
                w_norm = [w/wsum for w in w_slice]
                for (t,_), w in zip(ranks, w_norm):
                    pp = price_map[d_prev].get(t); pn = price_map[d].get(t, pp)
                    if pp and pn: ret += w*(pn/pp - 1)
            value *= (1+ret); dr.append(ret)
            peak = max(peak, value); mdd = max(mdd, (peak-value)/peak)
        dd = day[d]
        for t in list(held):
            info = dd.get(t)
            if info is None or (info[0] and info[0]>exit_thr) or (info[2] is not None and info[2]<-2):
                del held[t]
        if len(held) < slots:
            cands = sorted([(info[0], t) for t,info in dd.items()
                            if info[0] and info[0]<=entry and t not in exclude
                            and (info[2] is None or info[2]>=0)], key=lambda x: x[0])
            for p2, t in cands:
                if len(held)>=slots: break
                if t in held or not verified_cr(t, i): continue
                held[t] = (d, price_map[d].get(t,0))
        prev_held = dict(held)
    return value, mdd, dr, held

def met(v, mdd, dr):
    cum = (v-1)*100
    if not dr or len(dr)<2: return cum, mdd*100, 0, 0
    mu = statistics.mean(dr); sd = statistics.stdev(dr) if len(dr)>1 else 1e-9
    sh = (mu*252)/(sd*math.sqrt(252)) if sd>0 else 0
    cal = (cum/100)/mdd if mdd>0 else 0
    return cum, mdd*100, sh, cal


# ============================================================
# Test A: 극단 비중
# ============================================================
print("\n" + "="*72); print("Test A: 극단 비중 — 80~100% 1등 집중"); print("="*72)
print(f"  {'config':<20}{'cum%':>8}{'MDD%':>7}{'Sh':>6}{'Cal':>7}{'trades':>8}")
extremes = [
    ("S=2  80/20",   (2, 10, 2, (0.8, 0.2))),
    ("S=2  85/15",   (2, 10, 2, (0.85, 0.15))),
    ("S=2  90/10",   (2, 10, 2, (0.9, 0.1))),
    ("S=2  95/5",    (2, 10, 2, (0.95, 0.05))),
    ("S=2  99/1",    (2, 10, 2, (0.99, 0.01))),
    ("S=1  E=1",     (1, 10, 1, (1.0,))),
    ("S=1  E=2",     (2, 10, 1, (1.0,))),
    ("S=1  E=3",     (3, 10, 1, (1.0,))),
]
test_a = {}
for label, args in extremes:
    v, mdd, dr, _ = simulate(*args)
    cum, mp, sh, cal = met(v, mdd, dr)
    n_tr = 0  # not tracked here
    print(f"  {label:<20}{cum:>+8.1f}{mp:>7.1f}{sh:>6.2f}{cal:>7.2f}")
    test_a[label] = (cum, mp, sh, cal)


# ============================================================
# Test B: 다중 winner 동시 제거
# ============================================================
print("\n" + "="*72); print("Test B: 다중 winner 동시 제거 — single-stock illusion 차단"); print("="*72)
top4 = ["MU", "SNDK", "BE", "AEIS"]
combos = [
    ("NONE", set()),
    ("MU only", {"MU"}),
    ("SNDK only", {"SNDK"}),
    ("MU+SNDK", {"MU","SNDK"}),
    ("MU+SNDK+BE", {"MU","SNDK","BE"}),
    ("MU+SNDK+BE+AEIS", set(top4)),
    ("top4+TER", set(top4) | {"TER"}),
]
configs_b = [("80/20",(0.8,0.2)), ("85/15",(0.85,0.15)), ("90/10",(0.9,0.1)), ("95/5",(0.95,0.05)), ("S=1",None)]
print(f"  {'exclude':<22}", end="")
for lab,_ in configs_b: print(f"{lab:>10}", end="")
print(f"{'winner':>10}")
for cname, excl in combos:
    row = f"  {cname:<22}"
    rets = {}
    for label, w in configs_b:
        if w is None:
            v, mdd, dr, _ = simulate(2, 10, 1, (1.0,), exclude=excl)
        else:
            v, mdd, dr, _ = simulate(2, 10, 2, w, exclude=excl)
        cum = (v-1)*100
        rets[label] = cum
        row += f"{cum:>+8.1f}%".rjust(10)
    winner = max(rets, key=rets.get)
    print(row + f"{winner:>10}")


# ============================================================
# Test C: 90/10 vs 80/20 paired random-start
# ============================================================
print("\n" + "="*72); print("Test C: 90/10 vs 80/20 paired random-start (50 starts)"); print("="*72)
random.seed(2026)
max_start = min(25, len(dates)-30)
n_starts = 50
starts = [random.randint(0, max_start-1) for _ in range(n_starts)]
diffs = []; wins90 = 0
for s in starts:
    v90, m90, dr90, _ = simulate(2, 10, 2, (0.9,0.1), start_idx=s)
    v80, m80, dr80, _ = simulate(2, 10, 2, (0.8,0.2), start_idx=s)
    d = (v90-1)*100 - (v80-1)*100
    diffs.append(d)
    if d > 0: wins90 += 1
print(f"  90/10 wins: {wins90}/{n_starts} ({wins90/n_starts*100:.0f}%)")
print(f"  mean lift (90/10 - 80/20): {statistics.mean(diffs):+.2f}%p")
print(f"  median: {statistics.median(diffs):+.2f}%p, min: {min(diffs):+.2f}%p, max: {max(diffs):+.2f}%p")
print(f"  std: {statistics.stdev(diffs):.2f}%p")


# ============================================================
# Test D: 3-way 시간 분할
# ============================================================
print("\n" + "="*72); print("Test D: 3-way 시간 분할 — 비중 ordering이 시간에 robust한가"); print("="*72)
n = len(dates); t1 = n//3; t2 = 2*n//3
windows = [("Q1", 0, t1), ("Q2", t1, t2), ("Q3", t2, n)]
for wname, s, e in windows:
    print(f"\n  {wname} ({dates[s]} ~ {dates[e-1]}, {e-s}일):")
    print(f"    {'ratio':>8}{'cum%':>8}{'MDD%':>7}{'Cal':>7}")
    cfgs = [("50/50",(0.5,0.5)),("70/30",(0.7,0.3)),("80/20",(0.8,0.2)),("90/10",(0.9,0.1)),("95/5",(0.95,0.05)),("S=1",None)]
    for lab, w in cfgs:
        if w is None:
            v, mdd, dr, _ = simulate(2, 10, 1, (1.0,), start_idx=s, end_idx=e)
        else:
            v, mdd, dr, _ = simulate(2, 10, 2, w, start_idx=s, end_idx=e)
        cum, mp, sh, cal = met(v, mdd, dr)
        print(f"    {lab:>8}{cum:>+8.1f}{mp:>7.1f}{cal:>7.2f}")


# ============================================================
# Test E: 2등 슬롯 효용 — 90/10 vs S=1 직접 비교
# ============================================================
print("\n" + "="*72); print("Test E: 2등 슬롯 효용 — 90/10이 S=1보다 정말 좋은 이유"); print("="*72)
# 같은 random-start에서 paired
random.seed(2026)
starts = [random.randint(0, max_start-1) for _ in range(50)]
diff_s1 = []; wins_s2 = 0
for s in starts:
    v_s2, _, _, _ = simulate(2, 10, 2, (0.9,0.1), start_idx=s)
    v_s1, _, _, _ = simulate(2, 10, 1, (1.0,), start_idx=s)
    d = (v_s2-1)*100 - (v_s1-1)*100
    diff_s1.append(d)
    if d > 0: wins_s2 += 1
print(f"  S=2(90/10) vs S=1: wins {wins_s2}/{len(starts)} ({wins_s2/len(starts)*100:.0f}%)")
print(f"  mean lift: {statistics.mean(diff_s1):+.2f}%p")
print(f"  → 2등 슬롯 10%는 cash drag 방지 + 다음 1등 후보 사전 보유 효과")


print("\n" + "="*72); print("DONE"); print("="*72)
