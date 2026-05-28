# -*- coding: utf-8 -*-
"""
자율검증 v2: 두 결정을 분리해 검증
- Test 1: 슬롯 수 (S=1, 2, 3) — 1등 몰빵 vs 2개 분산 vs 3개 분산
- Test 2: S=2 안에서 비중 비율 (50/50 ~ 90/10) — 80/20 정당화
- Test 3: (E, X) grid — 진입/이탈 임계값 최적
- Test 4: random-start paired BT (S=2 비중 비교)
- Test 5: leave-one-stock-out robustness
- Test 6: 인접 안정성 (best 근처 셀들의 CV)

읽기 전용 — DB write/push 없음.
"""
import sqlite3, random, statistics, math
from collections import defaultdict

DB = "eps_momentum_data.db"

# ============================================================
# Load
# ============================================================
print("=" * 72)
print("Loading data...")
con = sqlite3.connect(DB)
cur = con.cursor()
dates = [r[0] for r in cur.execute(
    "SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date"
)]
day = {}
price_map = {}
for d in dates:
    rows = cur.execute(
        "SELECT ticker, part2_rank, composite_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price "
        "FROM ntm_screening WHERE date=?", (d,)
    ).fetchall()
    day_d = {}
    price_d = {}
    for r in rows:
        t, p2, cr, n0, n7, n30, n60, n90, px = r
        def seg(a, b):
            if a is None or b is None or b == 0: return None
            return (a - b) / abs(b) * 100
        segs = [s for s in [seg(n0,n7), seg(n7,n30), seg(n30,n60), seg(n60,n90)] if s is not None]
        ms = min(segs) if segs else None
        if p2 is not None: day_d[t] = (p2, cr, ms)
        if px is not None: price_d[t] = px
    day[d] = day_d
    price_map[d] = price_d
con.close()
print(f"Loaded: {len(dates)} dates ({dates[0]} ~ {dates[-1]})")

# ============================================================
# Simulator
# ============================================================
def verified_cr(t, i):
    """cr Top 30 for i, i-1, i-2 (3-day ✅ approximation)."""
    for j in (i, i-1, i-2):
        if j < 0: return False
        info = day[dates[j]].get(t)
        if not info or info[1] is None or info[1] > 30: return False
    return True

def simulate(entry, exit_thr, slots, weights, exclude=None, start_idx=0):
    """
    weights: tuple/list of weights for each rank slot (length = slots).
             For slots=1: (1.0,). For slots=2: (w1, w2). For slots=3: (w1, w2, w3).
             Weights are matched in part2_rank order (lowest rank gets weights[0]).
    """
    exclude = exclude or set()
    held = {}
    prev_held = None
    value = 1.0
    peak = 1.0
    mdd = 0.0
    daily_rets = []
    n_trades = 0

    for i, d in enumerate(dates):
        if i < start_idx: continue

        if prev_held:
            d_prev = dates[i-1]
            items = list(prev_held.keys())
            # rank items by part2_rank as of d_prev (lower rank = "stronger")
            ranks = sorted(((t, day[d_prev].get(t, (999,None,None))[0] or 999) for t in items),
                           key=lambda x: x[1])
            n = len(items)
            ret = 0.0
            if n > 0:
                # use weights[:n] then normalize (handles cases where actual holdings < slots)
                w_slice = list(weights[:n])
                wsum = sum(w_slice) or 1.0
                w_norm = [w/wsum for w in w_slice]
                for (t, _), w in zip(ranks, w_norm):
                    pp = price_map[d_prev].get(t)
                    pn = price_map[d].get(t, pp)
                    if pp and pn: ret += w * (pn/pp - 1)
            value *= (1 + ret)
            daily_rets.append(ret)
            peak = max(peak, value)
            mdd = max(mdd, (peak - value) / peak)

        dd = day[d]
        for t in list(held):
            info = dd.get(t)
            if info is None:
                del held[t]; n_trades += 1; continue
            p2, cr, ms = info
            if (p2 is not None and p2 > exit_thr) or (ms is not None and ms < -2):
                del held[t]; n_trades += 1

        if len(held) < slots:
            cands = sorted([(info[0], t) for t, info in dd.items()
                            if info[0] is not None and info[0] <= entry
                            and t not in exclude
                            and (info[2] is None or info[2] >= 0)],
                           key=lambda x: x[0])
            for p2, t in cands:
                if len(held) >= slots: break
                if t in held: continue
                if not verified_cr(t, i): continue
                held[t] = (d, price_map[d].get(t, 0))
                n_trades += 1
        prev_held = dict(held)

    return {"value": value, "mdd": mdd, "daily_rets": daily_rets,
            "n_trades": n_trades, "final_held": dict(held)}


def metrics(r):
    value = r["value"]; mdd = r["mdd"]; dr = r["daily_rets"]
    cum = (value - 1) * 100
    if not dr or len(dr) < 2: return cum, mdd*100, 0.0, 0.0
    mean_d = statistics.mean(dr)
    std_d = statistics.stdev(dr) if len(dr) > 1 else 1e-9
    sharpe = (mean_d * 252) / (std_d * math.sqrt(252)) if std_d > 0 else 0.0
    calmar = (cum/100) / mdd if mdd > 0 else 0.0
    return cum, mdd*100, sharpe, calmar


# ============================================================
# Baseline
# ============================================================
print("\n" + "=" * 72)
print("BASELINE: current production (E=2, X=10, S=2, weights 80/20)")
print("=" * 72)
base = simulate(2, 10, 2, (0.8, 0.2))
cum, mdd_p, sh, cal = metrics(base)
print(f"cum {cum:+.1f}%  MDD {mdd_p:.1f}%  Sharpe {sh:.2f}  Calmar {cal:.2f}")
print(f"trades: {base['n_trades']}  final held: {list(base['final_held'].keys())}")


# ============================================================
# TEST 1: 슬롯 수 (S=1 vs 2 vs 3) — 1등 몰빵 vs 분산
# ============================================================
print("\n" + "=" * 72)
print("TEST 1: 슬롯 수 비교 (E=2, X=10) — 균등 비중")
print("=" * 72)
slot_configs = [
    ("S=1 (1등만)",     (1, (1.0,))),
    ("S=2 균등 50/50",  (2, (0.5, 0.5))),
    ("S=3 균등 1/3씩", (3, (1/3, 1/3, 1/3))),
]
print(f"{'config':<20}{'cum%':>8}{'MDD%':>8}{'Sharpe':>8}{'Calmar':>8}{'trades':>8}")
for label, (S, w) in slot_configs:
    E_eff = max(2, S)  # E should be >= S
    r = simulate(E_eff, 10, S, w)
    cum, mdd_p, sh, cal = metrics(r)
    print(f"{label:<20}{cum:>+8.1f}{mdd_p:>8.1f}{sh:>8.2f}{cal:>8.2f}{r['n_trades']:>8}")


# ============================================================
# TEST 2: S=2 안에서 비중 비율 grid
# ============================================================
print("\n" + "=" * 72)
print("TEST 2: S=2 비중 비율 (E=2, X=10) — 80/20 vs 70/30 vs ...")
print("=" * 72)
weight_grid = [(0.5,0.5), (0.55,0.45), (0.6,0.4), (0.65,0.35),
               (0.7,0.3), (0.75,0.25), (0.8,0.2), (0.85,0.15), (0.9,0.1)]
print(f"{'ratio':>7}{'cum%':>8}{'MDD%':>8}{'Sharpe':>8}{'Calmar':>8}")
phase2 = []
for wh, wl in weight_grid:
    r = simulate(2, 10, 2, (wh, wl))
    cum, mdd_p, sh, cal = metrics(r)
    phase2.append((wh, wl, cum, mdd_p, sh, cal))
    print(f"{int(wh*100)}/{int(wl*100):<2}".rjust(7) + f"{cum:>+8.1f}{mdd_p:>8.1f}{sh:>8.2f}{cal:>8.2f}")


# ============================================================
# TEST 3: (E, X) grid at S=2, 80/20
# ============================================================
print("\n" + "=" * 72)
print("TEST 3: (E, X) grid at S=2, 80/20 — 진입/이탈 임계값")
print("=" * 72)
grid_ex = []
for E in [1, 2, 3, 4, 5]:
    for X in [5, 8, 10, 12, 15, 20]:
        r = simulate(E, X, 2, (0.8, 0.2))
        cum, mdd_p, sh, cal = metrics(r)
        grid_ex.append({"E": E, "X": X, "cum": cum, "mdd": mdd_p, "sh": sh, "cal": cal})

grid_ex_sorted = sorted(grid_ex, key=lambda x: -x["cal"])
print("Top 10 by Calmar:")
print(f"  {'E':>2} {'X':>3}  {'cum%':>7} {'MDD%':>6} {'Sharpe':>6} {'Calmar':>6}")
for r in grid_ex_sorted[:10]:
    marker = " <-- current" if (r["E"], r["X"]) == (2, 10) else ""
    print(f"  {r['E']:>2} {r['X']:>3}  {r['cum']:>+7.1f} {r['mdd']:>6.1f} {r['sh']:>6.2f} {r['cal']:>6.2f}{marker}")
for idx, r in enumerate(grid_ex_sorted, 1):
    if (r["E"], r["X"]) == (2, 10):
        print(f"\nCurrent (E=2, X=10): rank {idx}/{len(grid_ex_sorted)}, Calmar {r['cal']:.2f}")
        best_ex = grid_ex_sorted[0]
        print(f"Best (E,X):  ({best_ex['E']}, {best_ex['X']}), Calmar {best_ex['cal']:.2f}")
        gap = best_ex['cal'] - r['cal']
        print(f"Gap: {gap:.2f}  (CLAUDE.md noise threshold: ±0.10)")
        break


# ============================================================
# TEST 4: Random-start paired BT (S=2 weight ratios)
# ============================================================
print("\n" + "=" * 72)
print("TEST 4: Random-start paired BT — S=2 비중 robustness")
print("=" * 72)
random.seed(42)
max_start = min(25, len(dates) - 35)
n_starts = min(30, max_start)
starts = random.sample(range(0, max_start), n_starts)
print(f"  {n_starts} random starts in [0, {max_start}], runway >= {len(dates)-max_start} days")

s2_configs = [
    ("50/50", (0.5, 0.5)),
    ("60/40", (0.6, 0.4)),
    ("70/30", (0.7, 0.3)),
    ("80/20", (0.8, 0.2)),
    ("90/10", (0.9, 0.1)),
]

per_start = {label: [] for label, _ in s2_configs}
per_start_mdd = {label: [] for label, _ in s2_configs}
for s in starts:
    for label, w in s2_configs:
        r = simulate(2, 10, 2, w, start_idx=s)
        per_start[label].append((r["value"] - 1) * 100)
        per_start_mdd[label].append(r["mdd"] * 100)

print(f"\n  {'config':>7}  {'mean':>8}{'median':>8}{'min':>8}{'max':>8}{'std':>8}{'meanMDD':>10}")
for label, _ in s2_configs:
    rets = per_start[label]
    mdds = per_start_mdd[label]
    print(f"  {label:>7}  {statistics.mean(rets):>+8.1f}{statistics.median(rets):>+8.1f}{min(rets):>+8.1f}{max(rets):>+8.1f}{statistics.stdev(rets):>8.1f}{statistics.mean(mdds):>10.1f}")

print(f"\n  Paired 80/20 vs others (wins / mean lift over {n_starts} starts):")
base = per_start["80/20"]
for label, _ in s2_configs:
    if label == "80/20": continue
    other = per_start[label]
    wins = sum(1 for a, b in zip(base, other) if a > b)
    lift = statistics.mean([a-b for a, b in zip(base, other)])
    print(f"    80/20 vs {label}: wins {wins}/{n_starts} ({wins/n_starts*100:.0f}%), mean lift {lift:+.1f}%p")


# ============================================================
# TEST 5: Leave-one-stock-out robustness
# ============================================================
print("\n" + "=" * 72)
print("TEST 5: Leave-one-stock-out — top winner 빼도 80/20 우위 유지되는가")
print("=" * 72)
top_winners = ["MU", "SNDK", "BE", "STX", "AEIS", "TER", "MOD", "LITE"]
hdr = f"  {'exclude':<10}"
for label, _ in s2_configs:
    hdr += f"{label:>9}"
print(hdr + f"{'winner':>10}")

ordering_preserved = 0
total_cases = 0
robust_80_70 = 0  # 80/20 > 70/30 횟수
for tic in [None] + top_winners:
    excl = {tic} if tic else set()
    row = f"  {(tic or 'NONE'):<10}"
    rets = {}
    for label, w in s2_configs:
        r = simulate(2, 10, 2, w, exclude=excl)
        cum = (r["value"] - 1) * 100
        rets[label] = cum
        row += f"{cum:>+8.1f}%"
    # 80/20이 70/30보다 우위인지
    winner = max(rets, key=rets.get)
    total_cases += 1
    if rets["80/20"] > rets["70/30"]:
        robust_80_70 += 1
    row += f"{winner:>10}"
    print(row)
print(f"\n  80/20 > 70/30 유지: {robust_80_70}/{total_cases}")


# ============================================================
# TEST 6: 인접 안정성 (best 셀 주변 3x3의 CV)
# ============================================================
print("\n" + "=" * 72)
print("TEST 6: 인접 안정성 — best (E,X) 주변 셀들의 Calmar CV")
print("=" * 72)
best = best_ex
E0, X0 = best["E"], best["X"]
neighbors = []
for dE in [-1, 0, 1]:
    for dX in [-2, 0, 2]:
        E, X = E0+dE, X0+dX
        if E < 1 or X < 4: continue
        r = simulate(E, X, 2, (0.8, 0.2))
        _, _, _, cal = metrics(r)
        neighbors.append((E, X, cal))
cals = [c for _, _, c in neighbors]
mean_c = statistics.mean(cals)
std_c = statistics.stdev(cals) if len(cals) > 1 else 0
cv = std_c / abs(mean_c) if mean_c != 0 else 0
print(f"  best ({E0},{X0}) 주변 {len(neighbors)}개 셀:")
for E, X, c in neighbors:
    mk = " <-- best" if (E,X)==(E0,X0) else ""
    print(f"    ({E:>2}, {X:>2}): Cal {c:.2f}{mk}")
print(f"  mean {mean_c:.2f}  std {std_c:.2f}  CV {cv:.3f}  (CLAUDE.md 기준: <0.10~0.30이면 안정)")


# ============================================================
# TEST 7: 시간 분할 안정성 (전반기 vs 후반기)
# ============================================================
print("\n" + "=" * 72)
print("TEST 7: 시간 분할 — 전반기/후반기에서 best 비중이 같은가")
print("=" * 72)
half = len(dates) // 2
print(f"  전체 {len(dates)}일을 {half}일/{len(dates)-half}일로 분할")

def sim_window(weights, start, end):
    """date index range [start, end) only."""
    held = {}
    prev_held = None
    value = 1.0; peak = 1.0; mdd = 0.0; dr = []
    for i in range(start, end):
        d = dates[i]
        if prev_held and i > start:
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
            if info is None or (info[0] and info[0]>10) or (info[2] is not None and info[2]<-2):
                del held[t]
        if len(held) < 2:
            cands = sorted([(info[0], t) for t,info in dd.items() if info[0] and info[0]<=2 and (info[2] is None or info[2]>=0)], key=lambda x: x[0])
            for p2, t in cands:
                if len(held)>=2: break
                if t in held or not verified_cr(t, i): continue
                held[t] = (d, price_map[d].get(t,0))
        prev_held = dict(held)
    return value, mdd, dr

for window_name, (start, end) in [("전반기", (0, half)), ("후반기", (half, len(dates)))]:
    print(f"\n  {window_name} ({dates[start]} ~ {dates[end-1]}):")
    print(f"    {'ratio':>7}{'cum%':>8}{'MDD%':>7}{'Cal':>7}")
    for label, w in s2_configs:
        v, mdd, dr = sim_window(w, start, end)
        cum = (v-1)*100; cal = (cum/100)/mdd if mdd>0 else 0
        print(f"    {label:>7}{cum:>+8.1f}{mdd*100:>7.1f}{cal:>7.2f}")


print("\n" + "=" * 72)
print("DONE")
print("=" * 72)
