"""
Step 7: 방안 A1(상한 무제한) + 방안 C(missing 가중치 재정규화) 조합
- A1 단독은 이미 통과 (ret +5.6%p, MDD +1.5%p)
- C 단독 먼저 테스트 (single-variable) → 통과하면 A1+C 조합
- C: T-2 missing이면 가중치를 [T-1, T0]만으로 재정규화
"""
import pickle
import numpy as np
import sys
from collections import defaultdict
from datetime import datetime, timedelta
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

def compute_wgap(clamp_max=100, missing_mode='penalty'):
    """
    clamp_max: 100(baseline), 9999(A1)
    missing_mode: 'penalty'(현재, 30점), 'exclude'(C: 재정규화)
    """
    # z-score
    zscore_var = {}
    for d in p2_dates:
        s = zscore_stats_by_date.get(d)
        if not s or s['std'] <= 0:
            zscore_var[d] = {tk: 65 for tk in conv_gaps_by_date.get(d, {})}
            continue
        m, std = s['mean'], s['std']
        zscore_var[d] = {
            tk: min(clamp_max, max(30.0, 65 + (-(v - m) / std) * 15))
            for tk, v in conv_gaps_by_date.get(d, {}).items()
        }

    # Case 1 보너스 (v78)
    for d in p2_dates:
        if d not in zscore_var: continue
        target_30d = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        px_30d = {}
        for dd in sorted(all_prices.keys()):
            if dd <= target_30d: px_30d = all_prices[dd]
        for tk in list(zscore_var[d].keys()):
            v = raw.get(d, {}).get(tk, {})
            nc, n30, pn = v.get('ntm_cur', 0), v.get('ntm_30d', 0), v.get('price', 0)
            ntm_chg = ((nc - n30) / n30 * 100) if n30 and abs(n30) > 0.01 and nc else 0
            p30 = px_30d.get(tk)
            px_chg = ((pn - p30) / p30 * 100) if p30 and p30 > 0 and pn and pn > 0 else 0
            if ntm_chg > 1.0 and px_chg < -1.0:
                zscore_var[d][tk] += 8

    # 3일 가중
    wgap_var = {}
    for di, d in enumerate(p2_dates):
        dates_3 = [p2_dates[max(0, di-2)], p2_dates[max(0, di-1)], p2_dates[di]]
        dates_3 = [dd for dd in dates_3 if dd in zscore_var]

        all_tk = set()
        for dd in dates_3: all_tk.update(zscore_var[dd].keys())

        wgap = {}
        for tk in all_tk:
            if missing_mode == 'exclude':
                # C: missing 날 제외하고 present만으로 재정규화
                present = []
                for dd in dates_3:
                    sc = zscore_var[dd].get(tk)
                    if sc is not None:
                        present.append((dd, sc))
                if not present:
                    wgap[tk] = MISS
                    continue
                # 가중치 재정규화
                base_weights = [0.2, 0.3, 0.5][-len(dates_3):]
                if len(dates_3) == 2: base_weights = [0.4, 0.6]
                elif len(dates_3) == 1: base_weights = [1.0]
                present_weights = []
                for i, dd in enumerate(dates_3):
                    sc = zscore_var[dd].get(tk)
                    if sc is not None:
                        present_weights.append((base_weights[i], sc))
                total_w = sum(w for w, _ in present_weights)
                if total_w > 0:
                    wgap[tk] = sum(w/total_w * sc for w, sc in present_weights)
                else:
                    wgap[tk] = MISS
            else:
                # penalty: 기존 방식 (missing = 30)
                weights = [0.2, 0.3, 0.5][-len(dates_3):]
                if len(dates_3) == 2: weights = [0.4, 0.6]
                elif len(dates_3) == 1: weights = [1.0]
                wg = sum(zscore_var[dd].get(tk, MISS) * weights[i] for i, dd in enumerate(dates_3))
                wgap[tk] = wg
        wgap_var[d] = wgap
    return wgap_var

def simulate(wgap_by_date, start_idx=0, entry=3, exit_r=8, slots=3):
    portfolio = {}; daily_returns = []; trades = []; consecutive = defaultdict(int)
    for di in range(start_idx, len(p2_dates)):
        d = p2_dates[di]; wg = wgap_by_date.get(d, {})
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
                if price: trades.append((price - portfolio[tk]) / portfolio[tk] * 100)
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
                if price and price > 0: portfolio[tk] = price; vac -= 1
        if portfolio and di > 0:
            prev = p2_dates[di-1]; dr = 0
            for tk in portfolio:
                pn = day_raw.get(tk, {}).get('price'); pp = raw.get(prev, {}).get(tk, {}).get('price')
                if pn and pp and pp > 0: dr += (pn - pp) / pp * 100
            dr /= len(portfolio); daily_returns.append(dr)
    if portfolio:
        last = p2_dates[-1]
        for tk in list(portfolio.keys()):
            p = raw.get(last, {}).get(tk, {}).get('price')
            if p: trades.append((p - portfolio[tk]) / portfolio[tk] * 100)
    return daily_returns, trades

def metrics(drs, trades):
    cum = 1.0; peak = 1.0; mdd = 0
    for dr in drs: cum *= (1+dr/100); peak = max(peak, cum); mdd = min(mdd, (cum-peak)/peak*100)
    da = np.array(drs) if drs else np.array([0])
    sharpe = (da.mean()/da.std()*np.sqrt(252)) if da.std() > 0 else 0
    neg = da[da < 0]; sortino = (da.mean()/(neg.std() if len(neg)>1 else 1))*np.sqrt(252)
    n = len(trades); wr = (sum(1 for t in trades if t>0)/n*100) if n else 0
    return {'ret': (cum-1)*100, 'mdd': mdd, 'sharpe': sharpe, 'sortino': sortino, 'n': n, 'wr': wr}

starts = list(range(0, min(33, len(p2_dates)-5)))

variants = [
    ('baseline(100,penalty)', compute_wgap(100, 'penalty')),
    ('A1(무제한,penalty)', compute_wgap(9999, 'penalty')),
    ('C단독(100,exclude)', compute_wgap(100, 'exclude')),
    ('A1+C(무제한,exclude)', compute_wgap(9999, 'exclude')),
]

print(f"{'='*120}")
print(f"방안 비교 (멀티스타트 {len(starts)}시작일)")
print(f"{'='*120}")
print(f"{'변형':<24} {'평균':>6} {'중앙':>6} {'std':>5} {'min':>6} {'max':>6} {'MDD_w':>6} {'Shp':>5} {'Sort':>6} {'위험조정':>6} {'n':>4} {'WR':>5}")

bl_avg = None
for label, wgap in variants:
    multi = []
    for si in starts:
        drs, trades = simulate(wgap, start_idx=si)
        multi.append(metrics(drs, trades))
    rets = np.array([m['ret'] for m in multi])
    mdds = np.array([m['mdd'] for m in multi])
    avg = rets.mean()
    if bl_avg is None: bl_avg = avg
    print(f"{label:<24} {avg:+6.1f}% {np.median(rets):+6.1f}% {rets.std():5.1f}% "
          f"{rets.min():+6.1f}% {rets.max():+6.1f}% {mdds.min():+6.1f}% "
          f"{np.mean([m['sharpe'] for m in multi]):5.2f} {np.mean([m['sortino'] for m in multi]):6.2f} "
          f"{avg/abs(mdds.min()):5.2f} {np.mean([m['n'] for m in multi]):4.1f} "
          f"{np.mean([m['wr'] for m in multi]):5.1f}%")

# Delta
print(f"\n{'='*120}")
print("vs Baseline Delta")
print(f"{'='*120}")
for label, wgap in variants[1:]:
    multi = []
    for si in starts:
        drs, trades = simulate(wgap, start_idx=si)
        multi.append(metrics(drs, trades))
    rets = np.array([m['ret'] for m in multi])
    mdds = np.array([m['mdd'] for m in multi])
    bl_mdd_w = -16.7  # Step 3에서 확인
    print(f"  {label:<24}: ret {rets.mean()-bl_avg:+.1f}%p, MDD worst {mdds.min()-bl_mdd_w:+.1f}%p, "
          f"Sharpe {np.mean([m['sharpe'] for m in multi])-4.78:+.2f}")

# VNOM 패턴 영향: A1+C에서 VNOM 순위 확인
print(f"\n{'='*120}")
print("VNOM 패턴 영향 (4/15~16)")
print(f"{'='*120}")
for label, wgap in variants:
    for d in ['2026-04-15', '2026-04-16']:
        if d not in wgap: continue
        top5 = sorted(wgap[d].items(), key=lambda x: -x[1])[:5]
        vnom_rank = next((i+1 for i, (tk,_) in enumerate(top5) if tk == 'VNOM'), '>5')
        print(f"  {label:<24} {d}: VNOM rank={vnom_rank}, top3=[{', '.join(f'{tk}:{v:.1f}' for tk,v in top5[:3])}]")
