"""
최종 그리드 서치 — 캐시 최적화 버전
1. baseline 중간 결과 캐시 (한 번 계산)
2. Case 1 sets 미리 계산
3. 조합별로는 변하는 부분만 재계산
"""
import numpy as np
import sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
import time

from gridsearch_internal import load_all_data, is_case1, compute_w_gap_internal

t0 = time.time()
print("Loading...")
all_dates, p2_dates, raw, chg_data, _ = load_all_data()
print(f"Period: {p2_dates[0]}~{p2_dates[-1]} ({len(p2_dates)} days), load {time.time()-t0:.1f}s")

# ══════════════════════════════════════
# Phase 1: 캐시 구축 (한 번만)
# ══════════════════════════════════════
print("\n[Phase 1] 캐시 구축...")
t1 = time.time()

# 1-a. baseline daily scores (각 날짜의 z-score) + baseline w_gap
baseline_daily_scores = {}  # date → {ticker: score(30~100)}
baseline_conv_gaps = {}     # date → {ticker: conv_gap}
baseline_wgap = {}          # date → {ticker: w_gap(3일 가중)}

for d in p2_dates:
    day_raw = raw.get(d, {})
    tickers = [tk for tk in day_raw if day_raw[tk].get('comp_rank') is not None]

    # conv_gap 계산
    conv_gaps = {}
    for tk in tickers:
        v = day_raw[tk]
        ag = v['adj_gap'] or 0
        ratio = (v['rev_up30'] / v['num_analysts']) if v['num_analysts'] > 0 else 0
        eps_floor = min(abs((v['ntm_cur'] - v['ntm_90d']) / v['ntm_90d']), 1.0) \
            if v['ntm_90d'] and abs(v['ntm_90d']) > 0.01 else 0
        base_conv = max(ratio, eps_floor)
        rev_bonus = 0.3 if (v['rev_growth'] is not None and v['rev_growth'] >= 0.30) else 0
        conv_gaps[tk] = ag * (1 + base_conv + rev_bonus)
    baseline_conv_gaps[d] = conv_gaps

    # z-score
    vals = list(conv_gaps.values())
    if len(vals) >= 2:
        m, s = np.mean(vals), np.std(vals)
        if s > 0:
            scores = {tk: min(100.0, max(30.0, 65 + (-(v - m) / s) * 15))
                     for tk, v in conv_gaps.items()}
        else:
            scores = {tk: 65 for tk in conv_gaps}
    else:
        scores = {tk: 65 for tk in conv_gaps}
    baseline_daily_scores[d] = scores

# 3일 가중 → baseline w_gap
MISS = 30
for di, d in enumerate(p2_dates):
    tickers = list(baseline_daily_scores[d].keys())
    wgap = {}
    dates_3 = [p2_dates[max(0, di-2)], p2_dates[max(0, di-1)], p2_dates[di]]
    dates_3 = [dd for dd in dates_3 if dd in baseline_daily_scores]
    weights = [0.2, 0.3, 0.5][-len(dates_3):]
    if len(dates_3) == 2: weights = [0.4, 0.6]
    elif len(dates_3) == 1: weights = [1.0]

    all_tickers = set()
    for dd in dates_3:
        all_tickers.update(baseline_daily_scores[dd].keys())

    for tk in all_tickers:
        wg = 0
        for i, dd in enumerate(dates_3):
            sc = baseline_daily_scores[dd].get(tk, MISS)
            wg += sc * weights[i]
        wgap[tk] = wg
    baseline_wgap[d] = wgap

# 1-b. Case 1 sets 미리 계산
THRESHOLDS = {}
for period in ['7d', '30d', '60d']:
    if period == '7d':
        THRESHOLDS[period] = [(0.5,-0.5),(0.5,-1.0),(1.0,-1.0),(1.0,-1.5),(1.0,-2.0),
                               (1.5,-1.5),(1.5,-2.0),(2.0,-2.0),(2.0,-3.0),(3.0,-3.0)]
    elif period == '30d':
        THRESHOLDS[period] = [(1.0,-1.0),(2.0,-2.0),(3.0,-3.0),(4.0,-4.0),(5.0,-5.0)]
    else:
        THRESHOLDS[period] = [(3.0,-3.0),(5.0,-5.0),(8.0,-8.0)]
THRESHOLDS['blend'] = [(3.0,0),(5.0,0),(8.0,0),(10.0,0)]

case1_sets = {}  # (period, nt, pt) → date → set(tickers)
for period, thrs in THRESHOLDS.items():
    for nt, pt in thrs:
        key = (period, nt, pt)
        case1_sets[key] = {}
        for d in p2_dates:
            dc = chg_data.get(d, {})
            case1_sets[key][d] = {tk for tk in dc if is_case1(dc[tk], period, nt, pt)}

print(f"  캐시 구축: {time.time()-t1:.1f}s")
print(f"  baseline_wgap: {len(baseline_wgap)} dates")
print(f"  case1_sets: {len(case1_sets)} 조합")

# ══════════════════════════════════════
# Phase 2: 시뮬 (캐시 기반, 빠름)
# ══════════════════════════════════════

def simulate_cached(e, x, s, position, period, nt, pt, strength, ed):
    c1_key = (period, nt, pt) if period else None

    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    for di in range(len(p2_dates)):
        d = p2_dates[di]
        day_raw = raw.get(d, {})
        day_chg = chg_data.get(d, {})

        # w_gap 계산 (캐시 기반 보정)
        if position == 'none':
            wgap = baseline_wgap[d]
        elif position in ('P3_zscore', 'P4_wgap'):
            # P3: 각 날짜 score에 가산 → 3일 가중 재계산
            # P4: w_gap에 직접 가산
            wgap = {}
            c1_today = case1_sets[c1_key].get(d, set()) if c1_key else set()

            if position == 'P4_wgap':
                for tk, wg in baseline_wgap[d].items():
                    wgap[tk] = wg + (strength if tk in c1_today else 0)
            else:  # P3
                dates_3 = [p2_dates[max(0, di-2)], p2_dates[max(0, di-1)], p2_dates[di]]
                dates_3 = [dd for dd in dates_3 if dd in baseline_daily_scores]
                weights = [0.2, 0.3, 0.5][-len(dates_3):]
                if len(dates_3) == 2: weights = [0.4, 0.6]
                elif len(dates_3) == 1: weights = [1.0]

                all_tickers = set()
                for dd in dates_3:
                    all_tickers.update(baseline_daily_scores[dd].keys())

                for tk in all_tickers:
                    wg = 0
                    for i, dd in enumerate(dates_3):
                        sc = baseline_daily_scores[dd].get(tk, MISS)
                        c1_dd = case1_sets[c1_key].get(dd, set()) if c1_key else set()
                        if tk in c1_dd:
                            sc += strength
                        wg += sc * weights[i]
                    wgap[tk] = wg
        elif position in ('P1_adjgap', 'P2_conviction'):
            # conv_gap 보정 → z-score 재계산 → 3일 가중
            dates_3 = [p2_dates[max(0, di-2)], p2_dates[max(0, di-1)], p2_dates[di]]
            dates_3 = [dd for dd in dates_3 if dd in baseline_conv_gaps]
            weights = [0.2, 0.3, 0.5][-len(dates_3):]
            if len(dates_3) == 2: weights = [0.4, 0.6]
            elif len(dates_3) == 1: weights = [1.0]

            scores_3 = {}
            for dd in dates_3:
                c1_dd = case1_sets[c1_key].get(dd, set()) if c1_key else set()
                bl_cg = baseline_conv_gaps[dd]

                if not c1_dd:
                    scores_3[dd] = baseline_daily_scores[dd]
                    continue

                # Case 1 종목만 conv_gap 보정
                new_cg = {}
                for tk, cg in bl_cg.items():
                    if tk in c1_dd:
                        if position == 'P1_adjgap':
                            new_cg[tk] = cg * (1 + strength)
                        else:  # P2
                            v = raw.get(dd, {}).get(tk, {})
                            ag = v.get('adj_gap', 0) or 0
                            new_cg[tk] = ag * (1 + strength)  # conviction 대체
                    else:
                        new_cg[tk] = cg

                # z-score 재계산
                vals = list(new_cg.values())
                if len(vals) >= 2:
                    m, s_val = np.mean(vals), np.std(vals)
                    if s_val > 0:
                        scores_3[dd] = {tk: min(100.0, max(30.0, 65 + (-(v - m) / s_val) * 15))
                                       for tk, v in new_cg.items()}
                    else:
                        scores_3[dd] = {tk: 65 for tk in new_cg}
                else:
                    scores_3[dd] = {tk: 65 for tk in new_cg}

            # 3일 가중
            all_tickers = set()
            for dd in dates_3:
                all_tickers.update(scores_3.get(dd, {}).keys())
            wgap = {}
            for tk in all_tickers:
                wg = 0
                for i, dd in enumerate(dates_3):
                    sc = scores_3.get(dd, {}).get(tk, MISS)
                    wg += sc * weights[i]
                wgap[tk] = wg

        # rank
        tickers = [tk for tk in wgap]
        sorted_tk = sorted(tickers, key=lambda t: wgap.get(t, 0), reverse=True)
        rank_map = {tk: i+1 for i, tk in enumerate(sorted_tk)}

        # consecutive
        new_con = defaultdict(int)
        for tk in tickers:
            if rank_map.get(tk, 999) <= 30:
                new_con[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_con

        # 이탈
        c1_today = case1_sets[c1_key].get(d, set()) if c1_key else set()
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            ms = day_chg.get(tk, {}).get('min_seg', 0)
            eff_exit = x + (ed if tk in c1_today and ed > 0 else 0)
            if (rank is None or rank > eff_exit) or ms < -2:
                price = day_raw.get(tk, {}).get('price')
                if price:
                    trades.append((price - portfolio[tk]) / portfolio[tk] * 100)
                del portfolio[tk]

        # 진입
        vac = s - len(portfolio)
        if vac > 0:
            for tk in sorted_tk:
                if vac <= 0: break
                if tk in portfolio: continue
                if rank_map.get(tk, 999) > e: continue
                if consecutive.get(tk, 0) < 3: continue
                ms = day_chg.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = day_raw.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = price
                    vac -= 1

        # 일별 수익
        if portfolio and di > 0:
            prev = p2_dates[di-1]
            dr = 0
            for tk in portfolio:
                p_now = day_raw.get(tk, {}).get('price')
                p_prev = raw.get(prev, {}).get(tk, {}).get('price')
                if p_now and p_prev and p_prev > 0:
                    dr += (p_now - p_prev) / p_prev * 100
            dr /= len(portfolio)
            daily_returns.append(dr)

    # 잔여
    if portfolio:
        last = p2_dates[-1]
        for tk in list(portfolio.keys()):
            p = raw.get(last, {}).get(tk, {}).get('price')
            if p: trades.append((p - portfolio[tk]) / portfolio[tk] * 100)

    cum = 1.0; peak = 1.0; mdd = 0
    for dr in daily_returns:
        cum *= (1+dr/100); peak = max(peak, cum); mdd = min(mdd, (cum-peak)/peak*100)
    n = len(trades)
    wr = (sum(1 for t in trades if t > 0)/n*100) if n else 0
    da = np.array(daily_returns) if daily_returns else np.array([0])
    sharpe = (da.mean()/da.std()*np.sqrt(252)) if da.std() > 0 else 0
    return {'ret': round((cum-1)*100,2), 'mdd': round(mdd,2), 'n': n,
            'wr': round(wr,1), 'sharpe': round(sharpe,2)}


# ══════════════════════════════════════
# Phase 3: 표본 테스트
# ══════════════════════════════════════
print("\n[Phase 2] 표본 테스트...")
t2 = time.time()
bl = simulate_cached(5, 12, 3, 'none', None, 0, 0, 0, 0)
print(f"  Baseline E5/X12/S3: ret {bl['ret']:+.1f}%, Sharpe {bl['sharpe']:.2f} ({time.time()-t2:.2f}s)")
t3 = time.time()
p3 = simulate_cached(3, 8, 3, 'P3_zscore', '30d', 1.0, -1.0, 8, 0)
print(f"  P3 30d N1/P-1 +8: ret {p3['ret']:+.1f}%, Sharpe {p3['sharpe']:.2f} ({time.time()-t3:.2f}s)")
t4 = time.time()
p1 = simulate_cached(3, 8, 3, 'P1_adjgap', '7d', 0.5, -1.0, 0.7, 0)
print(f"  P1 7d N0.5/P-1 ×0.7: ret {p1['ret']:+.1f}%, Sharpe {p1['sharpe']:.2f} ({time.time()-t4:.2f}s)")

per_combo = (time.time()-t2) / 3
print(f"  조합당: {per_combo:.3f}s")

# ══════════════════════════════════════
# Phase 4: 본 그리드
# ══════════════════════════════════════
ENTRY = [2, 3, 4, 5, 7]
EXIT = [8, 10, 12, 15]
SLOTS = [3, 5]
EXIT_DEF = [0, 3, 5]

STRENGTHS = {
    'P1_adjgap': [0.1, 0.3, 0.5, 0.7, 1.0, 1.5, 2.0, 3.0],
    'P2_conviction': [0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 1.5],
    'P3_zscore': [2, 3, 5, 8, 10, 15, 20, 25, 30],
    'P4_wgap': [2, 3, 5, 8, 10, 15, 20],
}

# 조합 수 계산
exs = sum(1 for e in ENTRY for x in EXIT if x > e) * len(SLOTS)
total_est = exs  # baseline
for pos, sts in STRENGTHS.items():
    for period, thrs in THRESHOLDS.items():
        total_est += len(thrs) * len(sts) * exs * len(EXIT_DEF)
print(f"\n[Phase 3] 본 그리드: {total_est}조합, 예상 {total_est*per_combo:.0f}초 = {total_est*per_combo/60:.1f}분")

t5 = time.time()
results = []
total = 0

# baseline
for e in ENTRY:
    for x in EXIT:
        if x <= e: continue
        for s in SLOTS:
            r = simulate_cached(e, x, s, 'none', None, 0, 0, 0, 0)
            results.append({'pos':'none','period':'-','nt':0,'pt':0,'str':0,'ed':0,
                           'e':e,'x':x,'s':s,**r})
            total += 1

# 모든 조합
for pos, sts in STRENGTHS.items():
    for period, thrs in THRESHOLDS.items():
        for nt, pt in thrs:
            for st in sts:
                for ed in EXIT_DEF:
                    for e in ENTRY:
                        for x in EXIT:
                            if x <= e: continue
                            for s in SLOTS:
                                r = simulate_cached(e, x, s, pos, period, nt, pt, st, ed)
                                results.append({
                                    'pos':pos,'period':period,'nt':nt,'pt':pt,'str':st,'ed':ed,
                                    'e':e,'x':x,'s':s,**r
                                })
                                total += 1
                                if total % 5000 == 0:
                                    elapsed = time.time()-t5
                                    eta = elapsed/total*(total_est-total)
                                    print(f"  {total}/{total_est} ({elapsed:.0f}s, ETA {eta:.0f}s)")

elapsed = time.time()-t5
print(f"\n총 {total}조합, {elapsed:.1f}초 = {elapsed/60:.1f}분")

# ══════════════════════════════════════
# Phase 5: 결과
# ══════════════════════════════════════
bl_all = [r for r in results if r['pos']=='none']
bl_best = max(bl_all, key=lambda x: x['ret'])
bl_v72 = [r for r in bl_all if r['e']==5 and r['x']==12 and r['s']==3]
bonused = [r for r in results if r['pos']!='none']
bn_ret = max(bonused, key=lambda x: x['ret'])
bn_shp = max(bonused, key=lambda x: x['sharpe'])

print(f"\n{'='*120}")
print("결과")
print(f"{'='*120}")
if bl_v72:
    print(f"  현행 E5/X12/S3: ret {bl_v72[0]['ret']:+.1f}%, Sharpe {bl_v72[0]['sharpe']:.2f}")
print(f"  BL 최적: E{bl_best['e']}/X{bl_best['x']}/S{bl_best['s']}: ret {bl_best['ret']:+.1f}%, Sharpe {bl_best['sharpe']:.2f}")
print(f"  보너스 최고(ret): {bn_ret['pos']} {bn_ret['period']} N{bn_ret['nt']}/P{bn_ret['pt']} str={bn_ret['str']} "
      f"ED{bn_ret['ed']} E{bn_ret['e']}/X{bn_ret['x']}/S{bn_ret['s']}: ret {bn_ret['ret']:+.1f}%, Sharpe {bn_ret['sharpe']:.2f}")
print(f"  보너스 최고(Shp): {bn_shp['pos']} {bn_shp['period']} N{bn_shp['nt']}/P{bn_shp['pt']} str={bn_shp['str']} "
      f"ED{bn_shp['ed']} E{bn_shp['e']}/X{bn_shp['x']}/S{bn_shp['s']}: ret {bn_shp['ret']:+.1f}%, Sharpe {bn_shp['sharpe']:.2f}")

for label, key_fn in [("수익률", lambda x: -x['ret']), ("Sharpe", lambda x: -x['sharpe'])]:
    print(f"\n{'='*120}")
    print(f"Top 30 ({label})")
    print(f"{'='*120}")
    top = sorted(results, key=key_fn)[:30]
    print(f"{'#':<3}{'pos':>14}{'per':>6}{'E':>3}{'X':>4}{'S':>3}{'NT':>5}{'PX':>5}{'str':>5}{'ED':>3}"
          f"{'ret':>8}{'MDD':>8}{'Shp':>7}{'n':>4}{'WR':>6}")
    for i, r in enumerate(top, 1):
        print(f"{i:<3}{r['pos']:>14}{r['period']:>6}{r['e']:>3}{r['x']:>4}{r['s']:>3}"
              f"{r['nt']:>5}{r['pt']:>5}{r['str']:>5}{r['ed']:>3}"
              f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['n']:>4}{r['wr']:>5.0f}%")

# 위치별 / 기간별
for groupby, vals, label in [
    ('pos', ['none','P1_adjgap','P2_conviction','P3_zscore','P4_wgap'], '위치별'),
    ('period', ['-','7d','30d','60d','blend'], '기간별')]:
    print(f"\n{'='*120}")
    print(f"{label} 최적")
    print(f"{'='*120}")
    for v in vals:
        sub = [r for r in results if r[groupby]==v]
        if not sub: continue
        best = max(sub, key=lambda x: x['ret'])
        m = '★' if best['ret'] == max(r['ret'] for r in results) else ' '
        print(f"  {m} {v:>14}: E{best['e']}/X{best['x']}/S{best['s']} {best.get('period','')} "
              f"N{best['nt']}/P{best['pt']} str={best['str']} ED{best['ed']} → "
              f"ret {best['ret']:+.1f}%, MDD {best['mdd']:.1f}%, Sharpe {best['sharpe']:.2f}")

# 현행 E5/X12/S3 기준
print(f"\n{'='*120}")
print("현행 E5/X12/S3 기준 Top 10")
print(f"{'='*120}")
v72 = sorted([r for r in results if r['e']==5 and r['x']==12 and r['s']==3], key=lambda x: -x['ret'])[:10]
for i, r in enumerate(v72, 1):
    print(f"  {i}. {r['pos']:>14} {r['period']:>5} N{r['nt']}/P{r['pt']} str={r['str']} ED{r['ed']}: "
          f"ret {r['ret']:+.1f}%, Sharpe {r['sharpe']:.2f}")

improved = len([r for r in bonused if r['ret'] > bl_best['ret']])
print(f"\nBL 최적({bl_best['ret']:+.1f}%) 초과: {improved}/{len(bonused)} ({improved/len(bonused)*100:.1f}%)")

import pickle
with open('gridsearch_final_results.pkl','wb') as f:
    pickle.dump(results, f)
print(f"\n총 시간: {time.time()-t0:.1f}초")
