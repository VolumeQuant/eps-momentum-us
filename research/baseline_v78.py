"""
Step 3: Baseline v78 metric + clamp 영향 분석
- 멀티스타트 (시작일 변동): 평균/중앙값/std/min/max
- MDD: 평균/worst
- Sharpe/Sortino
- 위험조정 = 평균 / |worst MDD|
- 거래 수, 승률, PF
- ★ clamp 영향 분석: clamp 발생일 vs 미발생일 매매 결과
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
wgap_by_date = c['wgap_by_date']
chg_data = c['chg_data']
zscore_raw = c['zscore_raw_by_date']
zscore_clamped = c['zscore_by_date']

# 시뮬 함수 (v78: E3/X8/S3, min_seg, consecutive≥3)
def simulate(start_idx=0, entry=3, exit_r=8, slots=3):
    portfolio = {}
    daily_returns = []
    trades = []  # (ret, date, ticker, clamp_day)
    consecutive = defaultdict(int)

    for di in range(start_idx, len(p2_dates)):
        d = p2_dates[di]
        wg = wgap_by_date.get(d, {})
        day_raw = raw.get(d, {})
        day_chg = chg_data.get(d, {})

        sorted_tk = sorted(wg.keys(), key=lambda t: wg.get(t, 0), reverse=True)
        rank_map = {tk: i+1 for i, tk in enumerate(sorted_tk)}

        # consecutive
        new_con = defaultdict(int)
        for tk in sorted_tk:
            if rank_map.get(tk, 999) <= 30:
                new_con[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_con

        # clamp 발생 여부
        n_clamp = sum(1 for tk in zscore_raw.get(d, {}) if zscore_raw[d][tk] >= 100)

        # 이탈
        for tk in list(portfolio.keys()):
            rk = rank_map.get(tk)
            ms = day_chg.get(tk, {}).get('min_seg', 0)
            price = day_raw.get(tk, {}).get('price')
            if (rk is None or rk > exit_r) or ms < -2:
                if price:
                    ep = portfolio[tk]['ep']
                    ret = (price - ep) / ep * 100
                    trades.append({
                        'ret': ret, 'entry_date': portfolio[tk]['date'],
                        'exit_date': d, 'ticker': tk,
                        'clamp_at_entry': portfolio[tk].get('clamp', False),
                    })
                del portfolio[tk]

        # 진입
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
                    portfolio[tk] = {'ep': price, 'date': d, 'clamp': n_clamp >= 2}
                    vac -= 1

        # 일별 수익
        if portfolio and di > 0:
            prev = p2_dates[di-1]
            dr = 0
            for tk in portfolio:
                pn = day_raw.get(tk, {}).get('price')
                pp = raw.get(prev, {}).get(tk, {}).get('price')
                if pn and pp and pp > 0:
                    dr += (pn - pp) / pp * 100
            dr /= len(portfolio)
            daily_returns.append(dr)

    # 잔여
    if portfolio:
        last = p2_dates[-1]
        for tk in list(portfolio.keys()):
            p = raw.get(last, {}).get(tk, {}).get('price')
            if p:
                ep = portfolio[tk]['ep']
                trades.append({'ret': (p-ep)/ep*100, 'entry_date': portfolio[tk]['date'],
                              'exit_date': last, 'ticker': tk,
                              'clamp_at_entry': portfolio[tk].get('clamp', False)})

    return daily_returns, trades

# ═══ 멀티스타트 ═══
print("="*100)
print("Baseline v78 — 멀티스타트 (E3/X8/S3)")
print("="*100)

starts = list(range(0, min(33, len(p2_dates)-5)))
multi_results = []
all_trades = []

for si in starts:
    drs, trades = simulate(start_idx=si)
    cum = 1.0; peak = 1.0; mdd = 0
    for dr in drs:
        cum *= (1+dr/100); peak = max(peak, cum); mdd = min(mdd, (cum-peak)/peak*100)
    ret = (cum-1)*100
    da = np.array(drs) if drs else np.array([0])
    sharpe = (da.mean()/da.std()*np.sqrt(252)) if da.std() > 0 else 0
    neg = da[da < 0]
    sortino = (da.mean()/(neg.std() if len(neg) > 1 else 1))*np.sqrt(252)
    n = len(trades); wr = (sum(1 for t in trades if t['ret'] > 0)/n*100) if n else 0
    pf_win = sum(t['ret'] for t in trades if t['ret'] > 0)
    pf_loss = abs(sum(t['ret'] for t in trades if t['ret'] < 0))
    pf = pf_win / pf_loss if pf_loss > 0 else 999

    multi_results.append({'si': si, 'ret': ret, 'mdd': mdd, 'sharpe': sharpe,
                          'sortino': sortino, 'n': n, 'wr': wr, 'pf': pf})
    if si == 0:
        all_trades = trades

rets = np.array([r['ret'] for r in multi_results])
mdds = np.array([r['mdd'] for r in multi_results])

print(f"\n멀티스타트 {len(starts)}시작일:")
print(f"  수익 — 평균: {rets.mean():+.1f}%, 중앙값: {np.median(rets):+.1f}%, "
      f"std: {rets.std():.1f}%, min: {rets.min():+.1f}%, max: {rets.max():+.1f}%")
print(f"  MDD — 평균: {mdds.mean():.1f}%, worst: {mdds.min():.1f}%")
print(f"  Sharpe — 평균: {np.mean([r['sharpe'] for r in multi_results]):.2f}")
print(f"  Sortino — 평균: {np.mean([r['sortino'] for r in multi_results]):.2f}")
print(f"  위험조정 — 평균/|worst MDD|: {rets.mean()/abs(mdds.min()):.2f}")
print(f"  거래수 — 평균: {np.mean([r['n'] for r in multi_results]):.1f}")
print(f"  승률 — 평균: {np.mean([r['wr'] for r in multi_results]):.1f}%")
print(f"  PF — 평균: {np.mean([r['pf'] for r in multi_results]):.2f}")

# 전체 기간 (si=0)
full = multi_results[0]
print(f"\n전체 기간 (si=0):")
print(f"  ret: {full['ret']:+.1f}%, MDD: {full['mdd']:.1f}%, Sharpe: {full['sharpe']:.2f}, "
      f"Sortino: {full['sortino']:.2f}, n: {full['n']}, WR: {full['wr']:.0f}%, PF: {full['pf']:.2f}")

# ═══ Clamp 영향 분석 ═══
print(f"\n{'='*100}")
print("Clamp 영향 분석")
print("="*100)

# 1. 매일 clamp 2+ 발생 여부
clamp_days = set()
for d in p2_dates:
    n_c = sum(1 for tk in zscore_raw.get(d, {}) if zscore_raw[d][tk] >= 100)
    if n_c >= 2:
        clamp_days.add(d)
print(f"  2+ clamp 발생일: {len(clamp_days)}/{len(p2_dates)}일")
print(f"  날짜: {sorted(clamp_days)}")

# 2. clamp 발생일에 진입한 거래 vs 아닌 거래
clamp_trades = [t for t in all_trades if t['clamp_at_entry']]
normal_trades = [t for t in all_trades if not t['clamp_at_entry']]
print(f"\n  clamp일 진입 거래: {len(clamp_trades)}건")
if clamp_trades:
    cr = [t['ret'] for t in clamp_trades]
    print(f"    평균: {np.mean(cr):+.1f}%, 승률: {sum(1 for r in cr if r>0)/len(cr)*100:.0f}%")
print(f"  일반일 진입 거래: {len(normal_trades)}건")
if normal_trades:
    nr = [t['ret'] for t in normal_trades]
    print(f"    평균: {np.mean(nr):+.1f}%, 승률: {sum(1 for r in nr if r>0)/len(nr)*100:.0f}%")

# 3. 1위 clamp(MU/SNDK)로 인해 "변별 못 한" 날 → 실제로 잘못된 종목 매수?
print(f"\n  1위 z_raw > 100인 날의 Top 3:")
for d in p2_dates[-10:]:
    rs = zscore_raw.get(d, {})
    top3_raw = sorted(rs.items(), key=lambda x: -x[1])[:3]
    top3_clamp = sorted(zscore_clamped.get(d, {}).items(), key=lambda x: -x[1])[:3]
    n_c = sum(1 for _, v in top3_raw if v >= 100)
    if n_c > 0:
        print(f"    {d}: raw=[{', '.join(f'{tk}:{v:.0f}' for tk,v in top3_raw)}]"
              f" clamp=[{', '.join(f'{tk}:{v:.0f}' for tk,v in top3_clamp)}]"
              f" → {n_c}개 동점")

# 4. "clamp 안 했으면 순위 바뀌었을" 날
print(f"\n  clamp 해제 시 순위 변동 영향:")
changed_days = 0
for d in p2_dates:
    rs = zscore_raw.get(d, {})
    cl = zscore_clamped.get(d, {})
    top3_raw = [tk for tk, _ in sorted(rs.items(), key=lambda x: -x[1])[:3]]
    top3_clamp = [tk for tk, _ in sorted(cl.items(), key=lambda x: -x[1])[:3]]
    if top3_raw != top3_clamp:
        changed_days += 1
print(f"    Top 3 순위 변동: {changed_days}/{len(p2_dates)}일")

# Metric 표
print(f"\n{'='*100}")
print("Baseline v78 Metric 표")
print("="*100)
print(f"| 항목 | 값 |")
print(f"|---|---|")
print(f"| 수익 평균 (multistart) | {rets.mean():+.1f}% |")
print(f"| 수익 중앙값 | {np.median(rets):+.1f}% |")
print(f"| 수익 std | {rets.std():.1f}% |")
print(f"| 수익 min/max | {rets.min():+.1f}% / {rets.max():+.1f}% |")
print(f"| MDD 평균 | {mdds.mean():.1f}% |")
print(f"| MDD worst | {mdds.min():.1f}% |")
print(f"| Sharpe 평균 | {np.mean([r['sharpe'] for r in multi_results]):.2f} |")
print(f"| Sortino 평균 | {np.mean([r['sortino'] for r in multi_results]):.2f} |")
print(f"| 위험조정 (평균/|worst MDD|) | {rets.mean()/abs(mdds.min()):.2f} |")
print(f"| 거래수 평균 | {np.mean([r['n'] for r in multi_results]):.1f} |")
print(f"| 승률 평균 | {np.mean([r['wr'] for r in multi_results]):.1f}% |")
print(f"| PF 평균 | {np.mean([r['pf'] for r in multi_results]):.2f} |")
