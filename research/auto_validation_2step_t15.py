"""2step_t15_100_50 vs v83.3 (90/10) — v83.3 채택 시 사용한 검증 구조 그대로

v83.3 채택 BT 강도:
  - random-start 50회 paired: 50/50 wins (100%), 평균 +21.45%p
  - leave-one-stock-out (top winner 8명): 9/9 cases 90/10 우위
  - 다중 winner 동시 제거: 90/10 ordering 유지
  - 3-way 시간 분할: 2/3 우위
  - (E,X) cross-product

같은 simulator (매일 part2_rank 기준 rebalance) + dynamic weights로 2step_t15 검증.
"""
import sqlite3, random, statistics, math
import sys
sys.stdout.reconfigure(encoding='utf-8')

DB = "eps_momentum_data.db"
print("=" * 72); print("Loading...")
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute(
    "SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
day = {}; price_map = {}; wgap_score = {}  # date → (top1, top2, score2_normalized, gap)

# w_gap precompute (= score_100 1위/2위)
sys.path.insert(0, '.'); sys.path.insert(0, 'research')
import daily_runner as dr

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

    # score gap precompute
    top_tks = sorted([(p2, t) for t, (p2, _, _) in day_d.items() if p2 is not None], key=lambda x: x[0])[:5]
    if len(top_tks) >= 2:
        tickers = [t for _, t in top_tks]
        wmap = dr._compute_w_gap_map(cur, d, tickers)
        sorted_by_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top_w = wmap.get(sorted_by_w[0], 0)
        if top_w > 0:
            s2 = wmap.get(sorted_by_w[1], 0) / top_w * 100
            wgap_score[d] = (sorted_by_w[0], sorted_by_w[1], s2, 100 - s2)
        else:
            wgap_score[d] = (sorted_by_w[0], sorted_by_w[1] if len(sorted_by_w) > 1 else None, 100, 0)
    else:
        wgap_score[d] = (None, None, 100, 100)
con.close()
print(f"Loaded {len(dates)} dates")


def verified_cr(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        info = day[dates[j]].get(t)
        if not info or info[1] is None or info[1] > 30: return False
    return True


def weight_2step_t15(gap):
    if gap >= 15: return (1.0, 0.0)
    return (0.5, 0.5)


def simulate(entry, exit_thr, slots, weight_mode, exclude=None, start_idx=0, end_idx=None):
    """weight_mode: tuple (fixed) or 'dynamic_2step_t15'"""
    exclude = exclude or set()
    end_idx = end_idx or len(dates)
    held = {}; prev_held = None
    value = 1.0; peak = 1.0; mdd = 0.0; dr_list = []
    for i in range(start_idx, end_idx):
        d = dates[i]
        if prev_held and i > start_idx:
            d_prev = dates[i-1]
            items = list(prev_held.keys())
            ranks = sorted(((t, day[d_prev].get(t,(999,None,None))[0] or 999) for t in items), key=lambda x: x[1])
            n = len(items); ret = 0
            if n > 0:
                # weights 결정
                if weight_mode == 'dynamic_2step_t15':
                    _, _, _, gap = wgap_score.get(d_prev, (None, None, 100, 0))
                    weights = weight_2step_t15(gap)
                else:
                    weights = weight_mode
                w_slice = list(weights[:n]); wsum = sum(w_slice) or 1
                w_norm = [w/wsum for w in w_slice]
                for (t,_), w in zip(ranks, w_norm):
                    pp = price_map[d_prev].get(t); pn = price_map[d].get(t, pp)
                    if pp and pn: ret += w*(pn/pp - 1)
            value *= (1+ret); dr_list.append(ret)
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
    return value, mdd, dr_list, held


def met(v, mdd, dr_list):
    cum = (v-1)*100
    if not dr_list or len(dr_list)<2: return cum, mdd*100, 0, 0
    mu = statistics.mean(dr_list); sd = statistics.stdev(dr_list) if len(dr_list)>1 else 1e-9
    sh = (mu*252)/(sd*math.sqrt(252)) if sd>0 else 0
    cal = (cum/100)/mdd if mdd>0 else 0
    return cum, mdd*100, sh, cal


print("\n" + "="*72)
print("★ Test A: 정적 v83.3 (90/10) vs 2step_t15 — 전체 BT")
print("="*72)
print(f"  {'config':<28}{'cum%':>9}{'MDD%':>8}{'Sh':>6}{'Cal':>7}")
configs_a = [
    ("v83.3 (90/10)",     (0.9, 0.1)),
    ("v83.2 (80/20)",     (0.8, 0.2)),
    ("2step_t15_100_50",  'dynamic_2step_t15'),
    ("S=1 (1종목 only)",  (1.0,)),
]
for label, w in configs_a:
    if isinstance(w, tuple) and len(w) == 1:
        v, mdd, dr_list, _ = simulate(2, 10, 1, w)
    else:
        v, mdd, dr_list, _ = simulate(2, 10, 2, w)
    cum, mp, sh, cal = met(v, mdd, dr_list)
    print(f"  {label:<28}{cum:>+8.1f}%{mp:>7.1f}%{sh:>6.2f}{cal:>7.2f}")


print("\n" + "="*72)
print("★ Test B: 다중 winner 동시 제거 (v83.3 검증 Test B 동일)")
print("="*72)
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
print(f"  {'exclude':<22}{'90/10':>10}{'2step_t15':>12}{'lift':>9}{'winner':>10}")
print('  ' + '-' * 65)
for cname, excl in combos:
    v90, _, _, _ = simulate(2, 10, 2, (0.9, 0.1), exclude=excl)
    v2s, _, _, _ = simulate(2, 10, 2, 'dynamic_2step_t15', exclude=excl)
    c90 = (v90-1)*100; c2s = (v2s-1)*100
    lift = c2s - c90
    winner = '2step_t15' if c2s > c90 else '90/10'
    print(f"  {cname:<22}{c90:>+8.1f}% {c2s:>+9.1f}% {lift:>+7.1f}%p {winner:>10}")


print("\n" + "="*72)
print("★ Test C: 2step_t15 vs 90/10 random-start paired (50 starts) — v83.3 Test C 동일")
print("="*72)
random.seed(2026)
max_start = min(25, len(dates)-30)
n_starts = 50
starts = [random.randint(0, max_start-1) for _ in range(n_starts)]
diffs = []; wins_2s = 0
for s in starts:
    v90, _, _, _ = simulate(2, 10, 2, (0.9, 0.1), start_idx=s)
    v2s, _, _, _ = simulate(2, 10, 2, 'dynamic_2step_t15', start_idx=s)
    d = (v2s-1)*100 - (v90-1)*100
    diffs.append(d)
    if d > 0: wins_2s += 1
print(f"  2step_t15 wins: {wins_2s}/{n_starts} ({wins_2s/n_starts*100:.0f}%)")
print(f"  mean lift (2step_t15 - 90/10): {statistics.mean(diffs):+.2f}%p")
print(f"  median: {statistics.median(diffs):+.2f}%p")
print(f"  min: {min(diffs):+.2f}%p, max: {max(diffs):+.2f}%p")
print(f"  std: {statistics.stdev(diffs):.2f}%p")
print(f"  → v83.3 채택 시 90/10 vs 80/20은 50/50 wins (100%), +21.45%p였음")


print("\n" + "="*72)
print("★ Test D: 3-way 시간 분할 (v83.3 Test D 동일)")
print("="*72)
n = len(dates); t1 = n//3; t2 = 2*n//3
windows = [("Q1", 0, t1), ("Q2", t1, t2), ("Q3", t2, n)]
for wname, s, e in windows:
    print(f"\n  {wname} ({dates[s]} ~ {dates[e-1]}, {e-s}일):")
    print(f"    {'config':<22}{'cum%':>8}{'MDD%':>7}{'Cal':>7}")
    for lab, w in [("90/10 (v83.3)", (0.9,0.1)), ("2step_t15", 'dynamic_2step_t15'), ("80/20", (0.8,0.2)), ("S=1", (1.0,))]:
        if isinstance(w, tuple) and len(w) == 1:
            v, mdd, dr_list, _ = simulate(2, 10, 1, w, start_idx=s, end_idx=e)
        else:
            v, mdd, dr_list, _ = simulate(2, 10, 2, w, start_idx=s, end_idx=e)
        cum, mp, sh, cal = met(v, mdd, dr_list)
        print(f"    {lab:<22}{cum:>+8.1f}{mp:>7.1f}{cal:>7.2f}")


print("\n" + "="*72)
print("★ Test E: (E,X) cross-product")
print("="*72)
print(f"  {'entry':>5}{'exit':>5}{'90/10':>10}{'2step_t15':>12}{'lift':>9}")
for e in [2, 3]:
    for x in [8, 10, 12]:
        v90, _, _, _ = simulate(e, x, 2, (0.9, 0.1))
        v2s, _, _, _ = simulate(e, x, 2, 'dynamic_2step_t15')
        c90 = (v90-1)*100; c2s = (v2s-1)*100
        lift = c2s - c90
        print(f"  {e:>5}{x:>5}{c90:>+8.1f}% {c2s:>+9.1f}% {lift:>+7.1f}%p")


print("\n" + "="*72); print("DONE"); print("="*72)
