"""
Step 4: 방안 A2 (상한 200) 단독 BT
- 캐시 재사용 (DB/yfinance 재조회 0회)
- 변경: min(100, ...) → min(200, ...)
- 멀티스타트 33시작일
- vs baseline delta
- Step 3 인사이트: clamp 영향 제한적(3/46일) → A2 효과도 제한적일 수 있음
"""
import pickle
import numpy as np
import sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

with open('zscore_cache.pkl', 'rb') as f:
    c = pickle.load(f)

p2_dates = c['p2_dates']
raw = c['raw']
conv_gaps_by_date = c['conv_gaps_by_date']
zscore_stats_by_date = c['zscore_stats_by_date']
chg_data = c['chg_data']
all_prices = c['all_prices']

MISS = 30

def compute_wgap_variant(clamp_max=100, coeff=15, center=65):
    """z-score 변형 → 3일 가중 w_gap 재계산"""
    # 1. z-score 재계산
    zscore_var = {}
    for d in p2_dates:
        s = zscore_stats_by_date.get(d)
        if not s or s['std'] <= 0:
            zscore_var[d] = {tk: 65 for tk in conv_gaps_by_date.get(d, {})}
            continue
        m, std = s['mean'], s['std']
        zscore_var[d] = {
            tk: min(clamp_max, max(30.0, center + (-(v - m) / std) * coeff))
            for tk, v in conv_gaps_by_date.get(d, {}).items()
        }

    # 2. Case 1 보너스 (v78)
    from datetime import datetime, timedelta
    for d in p2_dates:
        if d not in zscore_var: continue
        target_30d = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        # 30일 전 가격 찾기
        px_30d = {}
        for dd in sorted(all_prices.keys()):
            if dd <= target_30d:
                px_30d = all_prices[dd]
        for tk in list(zscore_var[d].keys()):
            v = raw.get(d, {}).get(tk, {})
            nc, n30, pn = v.get('ntm_cur', 0), v.get('ntm_30d', 0), v.get('price', 0)
            ntm_chg = ((nc - n30) / n30 * 100) if n30 and abs(n30) > 0.01 and nc else 0
            p30 = px_30d.get(tk)
            px_chg = ((pn - p30) / p30 * 100) if p30 and p30 > 0 and pn and pn > 0 else 0
            if ntm_chg > 1.0 and px_chg < -1.0:
                zscore_var[d][tk] += 8

    # 3. 3일 가중
    wgap_var = {}
    for di, d in enumerate(p2_dates):
        dates_3 = [p2_dates[max(0, di-2)], p2_dates[max(0, di-1)], p2_dates[di]]
        dates_3 = [dd for dd in dates_3 if dd in zscore_var]
        weights = [0.2, 0.3, 0.5][-len(dates_3):]
        if len(dates_3) == 2: weights = [0.4, 0.6]
        elif len(dates_3) == 1: weights = [1.0]
        all_tk = set()
        for dd in dates_3: all_tk.update(zscore_var[dd].keys())
        wgap = {}
        for tk in all_tk:
            wg = sum(zscore_var[dd].get(tk, MISS) * weights[i] for i, dd in enumerate(dates_3))
            wgap[tk] = wg
        wgap_var[d] = wgap
    return wgap_var, zscore_var

def simulate(wgap_by_date, start_idx=0, entry=3, exit_r=8, slots=3):
    portfolio = {}; daily_returns = []; trades = []; consecutive = defaultdict(int)
    for di in range(start_idx, len(p2_dates)):
        d = p2_dates[di]
        wg = wgap_by_date.get(d, {})
        day_raw = raw.get(d, {}); day_chg = chg_data.get(d, {})
        sorted_tk = sorted(wg.keys(), key=lambda t: wg.get(t, 0), reverse=True)
        rank_map = {tk: i+1 for i, tk in enumerate(sorted_tk)}
        new_con = defaultdict(int)
        for tk in sorted_tk:
            if rank_map.get(tk, 999) <= 30: new_con[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_con
        for tk in list(portfolio.keys()):
            rk = rank_map.get(tk); ms = day_chg.get(tk, {}).get('min_seg', 0)
            price = day_raw.get(tk, {}).get('price')
            if (rk is None or rk > exit_r) or ms < -2:
                if price:
                    trades.append((price - portfolio[tk]['ep']) / portfolio[tk]['ep'] * 100)
                del portfolio[tk]
        vac = slots - len(portfolio)
        if vac > 0:
            for tk in sorted_tk:
                if vac <= 0: break
                if tk in portfolio: continue
                if rank_map.get(tk, 999) > entry: continue
                if consecutive.get(tk, 0) < 3: continue
                ms = day_chg.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = day_raw.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = {'ep': price}; vac -= 1
        if portfolio and di > 0:
            prev = p2_dates[di-1]; dr = 0
            for tk in portfolio:
                pn = day_raw.get(tk, {}).get('price')
                pp = raw.get(prev, {}).get(tk, {}).get('price')
                if pn and pp and pp > 0: dr += (pn - pp) / pp * 100
            dr /= len(portfolio); daily_returns.append(dr)
    if portfolio:
        last = p2_dates[-1]
        for tk in list(portfolio.keys()):
            p = raw.get(last, {}).get(tk, {}).get('price')
            if p: trades.append((p - portfolio[tk]['ep']) / portfolio[tk]['ep'] * 100)
    return daily_returns, trades

def calc_metrics(drs, trades):
    cum = 1.0; peak = 1.0; mdd = 0
    for dr in drs: cum *= (1+dr/100); peak = max(peak, cum); mdd = min(mdd, (cum-peak)/peak*100)
    ret = (cum-1)*100
    da = np.array(drs) if drs else np.array([0])
    sharpe = (da.mean()/da.std()*np.sqrt(252)) if da.std() > 0 else 0
    neg = da[da < 0]
    sortino = (da.mean()/(neg.std() if len(neg) > 1 else 1))*np.sqrt(252)
    n = len(trades); wr = (sum(1 for t in trades if t > 0)/n*100) if n else 0
    pf_w = sum(t for t in trades if t > 0)
    pf_l = abs(sum(t for t in trades if t < 0))
    pf = pf_w / pf_l if pf_l > 0 else 999
    return {'ret': ret, 'mdd': mdd, 'sharpe': sharpe, 'sortino': sortino, 'n': n, 'wr': wr, 'pf': pf}

# ═══ 실행 ═══
starts = list(range(0, min(33, len(p2_dates)-5)))

# Baseline (clamp 100)
bl_wgap = c['wgap_by_date']  # 캐시에서 그대로
bl_multi = []
for si in starts:
    drs, trades = simulate(bl_wgap, start_idx=si)
    bl_multi.append(calc_metrics(drs, trades))

# A2 (clamp 200)
a2_wgap, a2_zscore = compute_wgap_variant(clamp_max=200)
a2_multi = []
for si in starts:
    drs, trades = simulate(a2_wgap, start_idx=si)
    a2_multi.append(calc_metrics(drs, trades))

# A1 (clamp 무제한)
a1_wgap, a1_zscore = compute_wgap_variant(clamp_max=9999)
a1_multi = []
for si in starts:
    drs, trades = simulate(a1_wgap, start_idx=si)
    a1_multi.append(calc_metrics(drs, trades))

# B1 (계수 12)
b1_wgap, _ = compute_wgap_variant(clamp_max=100, coeff=12)
b1_multi = []
for si in starts:
    drs, trades = simulate(b1_wgap, start_idx=si)
    b1_multi.append(calc_metrics(drs, trades))

# B2 (계수 10)
b2_wgap, _ = compute_wgap_variant(clamp_max=100, coeff=10)
b2_multi = []
for si in starts:
    drs, trades = simulate(b2_wgap, start_idx=si)
    b2_multi.append(calc_metrics(drs, trades))

# ═══ 결과 ═══
def print_summary(label, multi):
    rets = np.array([m['ret'] for m in multi])
    mdds = np.array([m['mdd'] for m in multi])
    return (f"{label:<16} {rets.mean():+6.1f}% {np.median(rets):+6.1f}% {rets.std():5.1f}% "
            f"{rets.min():+6.1f}% {rets.max():+6.1f}% {mdds.mean():6.1f}% {mdds.min():6.1f}% "
            f"{np.mean([m['sharpe'] for m in multi]):5.2f} {np.mean([m['sortino'] for m in multi]):6.2f} "
            f"{rets.mean()/abs(mdds.min()):5.2f} {np.mean([m['n'] for m in multi]):5.1f} "
            f"{np.mean([m['wr'] for m in multi]):5.1f}%")

print(f"\n{'='*130}")
print("방안 비교 (멀티스타트 {len(starts)}시작일)")
print(f"{'='*130}")
hdr = f"{'변형':<16} {'평균':>6} {'중앙':>6} {'std':>5} {'min':>6} {'max':>6} {'MDD avg':>7} {'MDD wst':>7} {'Shp':>5} {'Sort':>6} {'위험조정':>6} {'거래':>5} {'승률':>5}"
print(hdr)
for label, multi in [('baseline(100)', bl_multi), ('A2(200)', a2_multi),
                      ('A1(무제한)', a1_multi), ('B1(계수12)', b1_multi), ('B2(계수10)', b2_multi)]:
    print(print_summary(label, multi))

# Delta 표
print(f"\n{'='*130}")
print("vs Baseline Delta")
print(f"{'='*130}")
bl_avg = np.mean([m['ret'] for m in bl_multi])
bl_mdd = np.min([m['mdd'] for m in bl_multi])
bl_shp = np.mean([m['sharpe'] for m in bl_multi])
for label, multi in [('A2(200)', a2_multi), ('A1(무제한)', a1_multi),
                      ('B1(계수12)', b1_multi), ('B2(계수10)', b2_multi)]:
    avg = np.mean([m['ret'] for m in multi])
    mdd_w = np.min([m['mdd'] for m in multi])
    shp = np.mean([m['sharpe'] for m in multi])
    print(f"  {label:<16}: ret {avg-bl_avg:+.1f}%p, MDD worst {mdd_w-bl_mdd:+.1f}%p, Sharpe {shp-bl_shp:+.2f}")

# Top 3 순위 변동 분석 (A2 vs baseline)
print(f"\n{'='*130}")
print("A2 vs Baseline: Top 3 순위 변동")
print(f"{'='*130}")
changed = 0
for d in p2_dates:
    bl_top3 = sorted(bl_wgap.get(d, {}).items(), key=lambda x: -x[1])[:3]
    a2_top3 = sorted(a2_wgap.get(d, {}).items(), key=lambda x: -x[1])[:3]
    bl_set = set(tk for tk, _ in bl_top3)
    a2_set = set(tk for tk, _ in a2_top3)
    if bl_set != a2_set:
        changed += 1
        added = a2_set - bl_set
        removed = bl_set - a2_set
        print(f"  {d}: -{','.join(removed)} +{','.join(added)}")
print(f"\n  Top 3 변동: {changed}/{len(p2_dates)}일")
