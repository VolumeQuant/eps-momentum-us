"""
7d/30d 세밀 임계값 탐색 — 시스템 내부 적용
"충분한 표본 + 효과 있음" 동시 만족하는 sweet spot 찾기
"""
import numpy as np
import sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

from gridsearch_internal import load_all_data, compute_w_gap_internal, is_case1

print("Loading...")
all_dates, p2_dates, raw, chg_data, _ = load_all_data()
print(f"Period: {p2_dates[0]}~{p2_dates[-1]} ({len(p2_dates)} days)")

# 먼저 각 임계값의 해당 종목 수 매핑
print(f"\n{'='*100}")
print("7d/30d 임계값별 Case 1 발생 빈도")
print(f"{'='*100}")

for period in ['7d', '30d']:
    if period == '7d':
        ntm_range = [0.3, 0.5, 0.7, 1.0, 1.2, 1.5, 2.0, 3.0, 5.0]
        px_range = [0, -0.5, -1.0, -1.5, -2.0, -3.0, -5.0]
    else:
        ntm_range = [1.0, 2.0, 3.0, 4.0, 5.0, 7.0, 10.0]
        px_range = [0, -1.0, -2.0, -3.0, -4.0, -5.0, -7.0]

    print(f"\n  [{period}] 총 건수 / 발생일 / 고유종목")
    print(f"  {'NTM↓ PX→':>10}", end='')
    for pt in px_range:
        print(f"{'<'+str(pt)+'%':>10}", end='')
    print()

    for nt in ntm_range:
        print(f"  {'>'+str(nt)+'%':>10}", end='')
        for pt in px_range:
            total = 0
            days = 0
            tks = set()
            for d in p2_dates:
                dc = chg_data.get(d, {})
                c1 = [tk for tk in dc if is_case1(dc[tk], period, nt, pt)]
                total += len(c1)
                if c1: days += 1
                tks.update(c1)
            print(f"{total:>4}/{days:>2}/{len(tks):>2}", end='')
        print()

# 시뮬
def simulate_fast(p2_dates, raw, chg_data, all_dates, e, x, s,
                  position, period, nt, pt, strength):
    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    for di in range(len(p2_dates)):
        today = p2_dates[di]
        day_raw = raw.get(today, {})
        day_chg = chg_data.get(today, {})
        tickers = [tk for tk in day_raw if day_raw[tk].get('comp_rank') is not None]

        wgap = compute_w_gap_internal(raw, chg_data, all_dates, today, tickers,
                                       position, period, nt, pt, strength)
        sorted_tk = sorted(tickers, key=lambda t: wgap.get(t, 0), reverse=True)
        rank_map = {tk: i+1 for i, tk in enumerate(sorted_tk)}

        new_con = defaultdict(int)
        for tk in tickers:
            if rank_map.get(tk, 999) <= 30:
                new_con[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_con

        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            ms = day_chg.get(tk, {}).get('min_seg', 0)
            price = day_raw.get(tk, {}).get('price')
            if (rank is None or rank > x) or ms < -2:
                if price:
                    trades.append((price - portfolio[tk]) / portfolio[tk] * 100)
                del portfolio[tk]

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
    return {'ret': round((cum-1)*100, 2), 'mdd': round(mdd, 2), 'n': n,
            'wr': round(wr, 1), 'sharpe': round(sharpe, 2)}

# 세밀 그리드
print(f"\n{'='*100}")
print("세밀 그리드 (P1_adjgap + P3_zscore, E3/X8/S3 고정)")
print(f"{'='*100}")

baseline = simulate_fast(p2_dates, raw, chg_data, all_dates, 3, 8, 3, 'none', None, 0, 0, 0)
print(f"\nBaseline E3/X8/S3: ret {baseline['ret']:+.1f}%, Sharpe {baseline['sharpe']:.2f}")

bl_v72 = simulate_fast(p2_dates, raw, chg_data, all_dates, 5, 12, 3, 'none', None, 0, 0, 0)
print(f"현행 E5/X12/S3: ret {bl_v72['ret']:+.1f}%, Sharpe {bl_v72['sharpe']:.2f}")

results = []
total = 0

for period in ['7d', '30d']:
    if period == '7d':
        thrs = [(0.5, -0.5), (0.5, -1.0), (0.7, -1.0), (1.0, -1.0),
                (1.0, -1.5), (1.0, -2.0), (1.5, -1.0), (1.5, -1.5),
                (1.5, -2.0), (2.0, -2.0), (2.0, -3.0), (3.0, -3.0)]
    else:
        thrs = [(1.0, -1.0), (2.0, -2.0), (2.0, -3.0), (3.0, -2.0),
                (3.0, -3.0), (3.0, -4.0), (4.0, -3.0), (4.0, -4.0),
                (5.0, -3.0), (5.0, -5.0)]

    for pos in ['P1_adjgap', 'P3_zscore']:
        if pos == 'P1_adjgap':
            strengths = [0.1, 0.2, 0.3, 0.5, 0.7, 1.0, 1.5, 2.0, 3.0]
        else:
            strengths = [2, 3, 5, 8, 10, 15, 20, 25]

        for nt, pt in thrs:
            for st in strengths:
                for e, x, s in [(3, 8, 3), (5, 12, 3), (3, 10, 3), (5, 10, 3)]:
                    if x <= e: continue
                    r = simulate_fast(p2_dates, raw, chg_data, all_dates, e, x, s,
                                     pos, period, nt, pt, st)
                    results.append({
                        'pos': pos, 'period': period, 'nt': nt, 'pt': pt, 'str': st,
                        'e': e, 'x': x, 's': s, **r
                    })
                    total += 1
                    if total % 500 == 0:
                        print(f"  {total} combos...")

print(f"\n총 {total} 조합")

# Top 20 수익률
print(f"\n{'='*100}")
print("Top 20 (수익률) — baseline +49.4% 대비")
print(f"{'='*100}")
top20 = sorted(results, key=lambda x: -x['ret'])[:20]
print(f"{'#':<3}{'pos':>12}{'기간':>5}{'E':>3}{'X':>4}{'S':>3}{'NTM':>5}{'PX':>5}{'str':>5}"
      f"{'ret':>8}{'MDD':>8}{'Shp':>7}{'n':>4}{'WR':>6}{'Δret':>7}")
for i, r in enumerate(top20, 1):
    delta = r['ret'] - baseline['ret']
    print(f"{i:<3}{r['pos']:>12}{r['period']:>5}{r['e']:>3}{r['x']:>4}{r['s']:>3}"
          f"{r['nt']:>5}{r['pt']:>5}{r['str']:>5}"
          f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['n']:>4}{r['wr']:>5.0f}%{delta:>+5.1f}%")

# Sharpe Top 20
print(f"\n{'='*100}")
print("Top 20 (Sharpe)")
print(f"{'='*100}")
top20s = sorted(results, key=lambda x: -x['sharpe'])[:20]
for i, r in enumerate(top20s, 1):
    delta = r['ret'] - baseline['ret']
    print(f"{i:<3}{r['pos']:>12}{r['period']:>5}{r['e']:>3}{r['x']:>4}{r['s']:>3}"
          f"{r['nt']:>5}{r['pt']:>5}{r['str']:>5}"
          f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['n']:>4}{r['wr']:>5.0f}%{delta:>+5.1f}%")

# 기간 × 위치별 최고
print(f"\n{'='*100}")
print("기간 × 위치별 최고 (수익률)")
print(f"{'='*100}")
for period in ['7d', '30d']:
    for pos in ['P1_adjgap', 'P3_zscore']:
        sub = [r for r in results if r['period']==period and r['pos']==pos]
        if not sub: continue
        best = max(sub, key=lambda x: x['ret'])
        delta = best['ret'] - baseline['ret']
        print(f"  {period:>3} {pos:>12}: N{best['nt']}/P{best['pt']} str={best['str']} "
              f"E{best['e']}/X{best['x']}/S{best['s']} → {best['ret']:+.1f}% (Δ{delta:+.1f}%) Sharpe {best['sharpe']:.2f}")

# 현행 E5/X12/S3 기준 개선
print(f"\n{'='*100}")
print("현행 E5/X12/S3 기준 Top 10")
print(f"{'='*100}")
v72_sub = [r for r in results if r['e']==5 and r['x']==12 and r['s']==3]
v72_top = sorted(v72_sub, key=lambda x: -x['ret'])[:10]
for i, r in enumerate(v72_top, 1):
    delta = r['ret'] - bl_v72['ret']
    print(f"  {i}. {r['pos']:>12} {r['period']:>3} N{r['nt']}/P{r['pt']} str={r['str']}: "
          f"ret {r['ret']:+.1f}% (Δ{delta:+.1f}%) Sharpe {r['sharpe']:.2f}")

# Case 1 빈도 vs 효과 관계
print(f"\n{'='*100}")
print("Case 1 빈도 vs 개선 효과 (E3/X8/S3, P1_adjgap str=1.0)")
print(f"{'='*100}")
for period in ['7d', '30d']:
    thrs_check = [(0.5, -0.5), (1.0, -1.0), (1.5, -2.0), (2.0, -3.0), (3.0, -3.0)] if period == '7d' \
        else [(1.0, -1.0), (2.0, -2.0), (3.0, -3.0), (4.0, -4.0), (5.0, -5.0)]
    for nt, pt in thrs_check:
        # 빈도
        total_c1 = sum(len([tk for tk in chg_data.get(d, {}) if is_case1(chg_data[d][tk], period, nt, pt)]) for d in p2_dates)
        r = simulate_fast(p2_dates, raw, chg_data, all_dates, 3, 8, 3, 'P1_adjgap', period, nt, pt, 1.0)
        delta = r['ret'] - baseline['ret']
        print(f"  {period:>3} N{nt:>3}/P{pt:>4}: 빈도 {total_c1:>4}건 → ret {r['ret']:+.1f}% (Δ{delta:+.1f}%)")
