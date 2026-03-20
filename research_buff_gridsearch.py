"""그리드서치 버프 연구: 수익률과 상관관계 있는 지표 조합 탐색

수익률 상관관계 유의미 지표 (Part2 종목, 3일 수익률 기준):
  1. min_seg  r=+0.244 ***  (가장 강함)
  2. seg3     r=+0.216 ***
  3. score    r=+0.186 ***
  4. adj_score r=+0.157 ***
  5. eps_chg_weighted r=+0.135 ***
  6. seg1     r=+0.131 ***
  7. rev_growth r=+0.131 ***

방식:
  A. 승수 (multiplicative): w_gap × multiplier
  B. 가산 (additive): w_gap - bonus
  C. 나눗셈 (divisive): w_gap / (1 + factor)
  D. 복합 스코어: w_gap × α + indicator × β
  E. 다중 지표 결합
"""
import sqlite3
import sys
import itertools
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = 'eps_momentum_data.db'


def calc_min_seg_and_segs(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs), segs


def compute_w_gap(cursor, date_str, all_dates):
    di = all_dates.index(date_str)
    d0 = all_dates[di]
    d1 = all_dates[di - 1] if di >= 1 else None
    d2 = all_dates[di - 2] if di >= 2 else None
    gaps = {}
    for d in [d0, d1, d2]:
        if d:
            rows = cursor.execute(
                'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (d,)
            ).fetchall()
            gaps[d] = {r[0]: r[1] for r in rows}
    result = {}
    all_tickers = set()
    for d in [d0, d1, d2]:
        if d and d in gaps:
            all_tickers.update(gaps[d].keys())
    for tk in all_tickers:
        wg = gaps.get(d0, {}).get(tk, 0) * 0.5
        if d1:
            wg += gaps.get(d1, {}).get(tk, 0) * 0.3
        if d2:
            wg += gaps.get(d2, {}).get(tk, 0) * 0.2
        result[tk] = wg
    return result


def load_data():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()]

    all_prices = {}
    daily_data = {}
    for d in dates:
        rows = c.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)).fetchall()
        all_prices[d] = {r[0]: r[1] for r in rows}

        rows2 = c.execute('''
            SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, adj_score, score, rev_growth, eps_chg_weighted
            FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL
        ''', (d,)).fetchall()
        daily_data[d] = {}
        for r in rows2:
            ms, segs = calc_min_seg_and_segs(r[3], r[4], r[5], r[6], r[7])
            daily_data[d][r[0]] = {
                'price': r[1], 'adj_score': r[9] or 0, 'min_seg': ms,
                'segs': segs, 'score': r[10] or 0, 'rev_growth': r[11] or 0,
                'eps_chg_w': r[12] or 0,
            }

    # Precompute w_gap for all dates
    w_gap_cache = {}
    for d in dates:
        w_gap_cache[d] = compute_w_gap(c, d, dates)

    conn.close()
    return dates, all_prices, daily_data, w_gap_cache


def run_backtest(dates, all_prices, daily_data, w_gap_cache, sort_fn, start_idx, top_n=3, exit_n=15):
    """Run backtest with custom sort function.
    sort_fn(wg, info_dict) -> sort key (ascending = better)
    """
    portfolio = {}
    daily_returns = []

    for i in range(start_idx, len(dates)):
        date = dates[i]
        prev_date = dates[i - 1]
        data = daily_data[date]
        prices = all_prices[date]
        prev_prices = all_prices[prev_date]
        w_gap = w_gap_cache[date]

        eligible = []
        for tk in data:
            if data[tk]['min_seg'] >= -2:
                eligible.append((tk, w_gap.get(tk, 0)))
        eligible.sort(key=lambda x: x[1])
        wgap_rank = {tk: rank + 1 for rank, (tk, _) in enumerate(eligible)}

        # Custom sort
        scored = []
        for tk, wg in eligible:
            info = data[tk]
            key = sort_fn(wg, info)
            scored.append((tk, key, wg, info))
        scored.sort(key=lambda x: x[1])
        custom_rank = {tk: rank + 1 for rank, (tk, _, _, _) in enumerate(scored)}

        # Exit check (always use wgap_rank for exit)
        exits = []
        for tk in list(portfolio.keys()):
            cur_price = prices.get(tk)
            if cur_price is None:
                exits.append(tk)
                continue
            rank = wgap_rank.get(tk)
            ms_val = data.get(tk, {}).get('min_seg', 0)
            ret = (cur_price - portfolio[tk]) / portfolio[tk] * 100
            if (rank is None or rank > exit_n) or ms_val < -2 or ret <= -10:
                exits.append(tk)
        for tk in exits:
            if tk in portfolio:
                del portfolio[tk]

        # Entry (custom rank)
        slots = top_n - len(portfolio)
        if slots > 0:
            for tk, key, wg, info in scored[:30]:
                if tk in portfolio:
                    continue
                if custom_rank.get(tk, 999) > top_n:
                    continue
                if info['min_seg'] < 0:
                    continue
                cur_price = prices.get(tk)
                if cur_price:
                    portfolio[tk] = cur_price
                    slots -= 1
                    if slots <= 0:
                        break

        # Daily return
        if portfolio:
            day_ret = 0
            count = 0
            for tk in portfolio:
                cur = prices.get(tk)
                prev = prev_prices.get(tk)
                if cur and prev and prev > 0:
                    day_ret += (cur - prev) / prev * 100
                    count += 1
            daily_returns.append(day_ret / count if count > 0 else 0)
        else:
            daily_returns.append(0)

    cumulative = sum(daily_returns)
    peak = mdd = cum = 0
    for r in daily_returns:
        cum += r
        if cum > peak:
            peak = cum
        dd = cum - peak
        if dd < mdd:
            mdd = dd
    return cumulative, mdd


def multi_start_eval(dates, all_prices, daily_data, w_gap_cache, sort_fn, max_starts=16):
    """Evaluate across multiple start dates, return (avg, min, max, std, all_results)"""
    results = []
    for si in range(2, min(len(dates) - 5, 2 + max_starts)):
        cum, mdd = run_backtest(dates, all_prices, daily_data, w_gap_cache, sort_fn, si)
        results.append(cum)

    avg = sum(results) / len(results)
    mn = min(results)
    mx = max(results)
    variance = sum((r - avg) ** 2 for r in results) / len(results)
    std = variance ** 0.5
    return avg, mn, mx, std, results


def main():
    print("=" * 80)
    print("  그리드서치 버프 연구")
    print("  수익률 상관관계 유의미 지표 기반 순위 버프 최적화")
    print("=" * 80)

    print("\n[1/5] 데이터 로딩...")
    dates, all_prices, daily_data, w_gap_cache = load_data()
    print(f"  기간: {dates[0]} ~ {dates[-1]} ({len(dates)}거래일)")
    print(f"  백테스트 시작: {dates[2]}")

    # =========================================================
    # Phase 1: 단일 지표 그리드서치
    # =========================================================
    print("\n" + "=" * 80)
    print("[2/5] Phase 1: 단일 지표 그리드서치")
    print("=" * 80)

    results_phase1 = []

    # Baseline
    baseline_fn = lambda wg, info: wg
    baseline_avg, baseline_min, baseline_max, baseline_std, _ = multi_start_eval(
        dates, all_prices, daily_data, w_gap_cache, baseline_fn
    )
    results_phase1.append(('BASELINE(현행)', baseline_avg, baseline_min, baseline_max, baseline_std))
    print(f"\n  BASELINE: avg={baseline_avg:+.2f}% min={baseline_min:+.1f}% max={baseline_max:+.1f}%")

    # --- 1A. min_seg 승수 (multiplicative) ---
    print("\n  --- 1A. min_seg 승수 ---")
    for threshold in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        for mult in [1.1, 1.15, 1.2, 1.3, 1.5, 2.0]:
            name = f"ms≥{threshold}→x{mult}"
            fn = lambda wg, info, t=threshold, m=mult: wg * m if info['min_seg'] >= t else wg
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase1.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1B. min_seg 가산 (additive) ---
    print("\n  --- 1B. min_seg 가산 ---")
    for threshold in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        for bonus in [1, 2, 3, 5, 7, 10]:
            name = f"ms≥{threshold}→wg-{bonus}"
            fn = lambda wg, info, t=threshold, b=bonus: wg - b if info['min_seg'] >= t else wg
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase1.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1C. min_seg 연속 함수 ---
    print("\n  --- 1C. min_seg 연속 ---")
    for scale in [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]:
        name = f"wg-ms*{scale}(연속)"
        fn = lambda wg, info, s=scale: wg - max(0, info['min_seg']) * s
        avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
        diff = avg - baseline_avg
        results_phase1.append((name, avg, mn, mx, std))
        if diff > 0.3:
            print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1D. min_seg 나눗셈 ---
    print("\n  --- 1D. min_seg 나눗셈 ---")
    for denom in [2, 3, 5, 7, 10]:
        name = f"wg/(1+ms/{denom})"
        fn = lambda wg, info, d=denom: wg / (1 + max(0, info['min_seg']) / d)
        avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
        diff = avg - baseline_avg
        results_phase1.append((name, avg, mn, mx, std))
        if diff > 0.3:
            print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1E. seg3 그리드 ---
    print("\n  --- 1E. seg3 버프 ---")
    for threshold in [1, 2, 3, 4, 5]:
        for mult in [1.1, 1.2, 1.3, 1.5]:
            name = f"seg3≥{threshold}→x{mult}"
            fn = lambda wg, info, t=threshold, m=mult: wg * m if info['segs'][2] >= t else wg
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase1.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    for scale in [0.5, 1.0, 2.0, 3.0]:
        name = f"wg-seg3*{scale}(연속)"
        fn = lambda wg, info, s=scale: wg - max(0, info['segs'][2]) * s
        avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
        diff = avg - baseline_avg
        results_phase1.append((name, avg, mn, mx, std))
        if diff > 0.3:
            print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1F. adj_score 그리드 ---
    print("\n  --- 1F. adj_score 버프 ---")
    for threshold in [15, 20, 25, 30, 40, 50]:
        for mult in [1.1, 1.15, 1.2, 1.3, 1.5]:
            name = f"sc≥{threshold}→x{mult}"
            fn = lambda wg, info, t=threshold, m=mult: wg * m if info['adj_score'] >= t else wg
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase1.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    for scale in [0.05, 0.1, 0.2, 0.3, 0.5]:
        name = f"wg-sc*{scale}(연속)"
        fn = lambda wg, info, s=scale: wg - max(0, info['adj_score']) * s
        avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
        diff = avg - baseline_avg
        results_phase1.append((name, avg, mn, mx, std))
        if diff > 0.3:
            print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1G. score 그리드 ---
    print("\n  --- 1G. score 버프 ---")
    for threshold in [20, 30, 40, 50, 60]:
        for mult in [1.1, 1.2, 1.3, 1.5]:
            name = f"score≥{threshold}→x{mult}"
            fn = lambda wg, info, t=threshold, m=mult: wg * m if info['score'] >= t else wg
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase1.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1H. seg1 그리드 ---
    print("\n  --- 1H. seg1 버프 ---")
    for threshold in [1, 2, 3, 5]:
        for mult in [1.1, 1.2, 1.3, 1.5]:
            name = f"seg1≥{threshold}→x{mult}"
            fn = lambda wg, info, t=threshold, m=mult: wg * m if info['segs'][0] >= t else wg
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase1.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1I. rev_growth 그리드 ---
    print("\n  --- 1I. rev_growth 버프 ---")
    for threshold in [15, 20, 30, 40, 50]:
        for mult in [1.1, 1.2, 1.3, 1.5]:
            name = f"rg≥{threshold}→x{mult}"
            fn = lambda wg, info, t=threshold, m=mult: wg * m if info['rev_growth'] >= t else wg
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase1.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<25s} avg={avg:+.2f}% diff={diff:+.2f}% std={std:.2f}")

    # --- 1J. 2단계 임계값 (min_seg) ---
    print("\n  --- 1J. 2단계 min_seg ---")
    for t1 in [1.0, 1.5, 2.0]:
        for m1 in [1.1, 1.2, 1.3]:
            for t2 in [t1 + 0.5, t1 + 1.0, t1 + 1.5]:
                if t2 > 4.0:
                    continue
                for m2 in [m1 + 0.1, m1 + 0.2, m1 + 0.3]:
                    if m2 > 2.0:
                        continue
                    name = f"ms≥{t1}→x{m1},≥{t2}→x{m2:.1f}"
                    fn = lambda wg, info, _t1=t1, _m1=m1, _t2=t2, _m2=m2: (
                        wg * _m2 if info['min_seg'] >= _t2 else
                        wg * _m1 if info['min_seg'] >= _t1 else wg
                    )
                    avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                    diff = avg - baseline_avg
                    results_phase1.append((name, avg, mn, mx, std))
                    if diff > 0.3:
                        print(f"    {name:<35s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # Phase 1 Top 20
    print("\n" + "=" * 80)
    print("  Phase 1 Top 20 (단일 지표)")
    print("=" * 80)
    results_phase1.sort(key=lambda x: -x[1])
    for i, (name, avg, mn, mx, std) in enumerate(results_phase1[:20]):
        diff = avg - baseline_avg
        print(f"  {i+1:>2d}. {name:<35s} avg={avg:+.2f}% diff={diff:+.2f}% "
              f"min={mn:+.1f}% max={mx:+.1f}% std={std:.2f}")

    # =========================================================
    # Phase 2: 2-지표 조합 그리드서치
    # =========================================================
    print("\n" + "=" * 80)
    print("[3/5] Phase 2: 2-지표 조합 그리드서치")
    print("=" * 80)

    results_phase2 = []

    # Phase 1 Top 지표에서 최적 임계값 추출
    # min_seg 최적: threshold ≈ 1~2, mult ≈ 1.2~1.3
    # seg3 최적값 탐색 필요

    # min_seg + seg3
    print("\n  --- min_seg + seg3 ---")
    for ms_t in [1.0, 1.5, 2.0]:
        for ms_m in [1.2, 1.3, 1.5]:
            for s3_t in [1, 2, 3]:
                for s3_m in [1.1, 1.2, 1.3]:
                    name = f"ms≥{ms_t}x{ms_m}+s3≥{s3_t}x{s3_m}"
                    fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _st=s3_t, _sm=s3_m: (
                        wg * (_mm if info['min_seg'] >= _mt else 1.0)
                           * (_sm if info['segs'][2] >= _st else 1.0)
                    )
                    avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                    diff = avg - baseline_avg
                    results_phase2.append((name, avg, mn, mx, std))
                    if diff > 0.5:
                        print(f"    {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # min_seg + adj_score
    print("\n  --- min_seg + adj_score ---")
    for ms_t in [1.0, 1.5, 2.0]:
        for ms_m in [1.2, 1.3, 1.5]:
            for sc_t in [20, 30, 40]:
                for sc_m in [1.1, 1.15, 1.2, 1.3]:
                    name = f"ms≥{ms_t}x{ms_m}+sc≥{sc_t}x{sc_m}"
                    fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _st=sc_t, _sm=sc_m: (
                        wg * (_mm if info['min_seg'] >= _mt else 1.0)
                           * (_sm if info['adj_score'] >= _st else 1.0)
                    )
                    avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                    diff = avg - baseline_avg
                    results_phase2.append((name, avg, mn, mx, std))
                    if diff > 0.5:
                        print(f"    {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # min_seg + score
    print("\n  --- min_seg + score ---")
    for ms_t in [1.0, 1.5, 2.0]:
        for ms_m in [1.2, 1.3, 1.5]:
            for sc_t in [30, 40, 50, 60]:
                for sc_m in [1.1, 1.2, 1.3]:
                    name = f"ms≥{ms_t}x{ms_m}+scr≥{sc_t}x{sc_m}"
                    fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _st=sc_t, _sm=sc_m: (
                        wg * (_mm if info['min_seg'] >= _mt else 1.0)
                           * (_sm if info['score'] >= _st else 1.0)
                    )
                    avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                    diff = avg - baseline_avg
                    results_phase2.append((name, avg, mn, mx, std))
                    if diff > 0.5:
                        print(f"    {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # min_seg + seg1
    print("\n  --- min_seg + seg1 ---")
    for ms_t in [1.0, 1.5, 2.0]:
        for ms_m in [1.2, 1.3, 1.5]:
            for s1_t in [1, 2, 3]:
                for s1_m in [1.1, 1.2, 1.3]:
                    name = f"ms≥{ms_t}x{ms_m}+s1≥{s1_t}x{s1_m}"
                    fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _st=s1_t, _sm=s1_m: (
                        wg * (_mm if info['min_seg'] >= _mt else 1.0)
                           * (_sm if info['segs'][0] >= _st else 1.0)
                    )
                    avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                    diff = avg - baseline_avg
                    results_phase2.append((name, avg, mn, mx, std))
                    if diff > 0.5:
                        print(f"    {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # seg3 + adj_score
    print("\n  --- seg3 + adj_score ---")
    for s3_t in [1, 2, 3]:
        for s3_m in [1.1, 1.2, 1.3]:
            for sc_t in [20, 30, 40]:
                for sc_m in [1.1, 1.15, 1.2]:
                    name = f"s3≥{s3_t}x{s3_m}+sc≥{sc_t}x{sc_m}"
                    fn = lambda wg, info, _st=s3_t, _sm=s3_m, _sct=sc_t, _scm=sc_m: (
                        wg * (_sm if info['segs'][2] >= _st else 1.0)
                           * (_scm if info['adj_score'] >= _sct else 1.0)
                    )
                    avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                    diff = avg - baseline_avg
                    results_phase2.append((name, avg, mn, mx, std))
                    if diff > 0.5:
                        print(f"    {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # Phase 2 Top 20
    print("\n" + "=" * 80)
    print("  Phase 2 Top 20 (2-지표 조합)")
    print("=" * 80)
    results_phase2.sort(key=lambda x: -x[1])
    for i, (name, avg, mn, mx, std) in enumerate(results_phase2[:20]):
        diff = avg - baseline_avg
        print(f"  {i+1:>2d}. {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}% "
              f"min={mn:+.1f}% max={mx:+.1f}% std={std:.2f}")

    # =========================================================
    # Phase 3: 가산(additive) 방식 조합
    # =========================================================
    print("\n" + "=" * 80)
    print("[4/5] Phase 3: 가산(additive) + 연속 함수 조합")
    print("=" * 80)

    results_phase3 = []

    # min_seg 연속 + seg3 연속
    print("\n  --- 연속 함수 조합 ---")
    for ms_scale in [0.5, 1.0, 1.5, 2.0, 3.0]:
        for s3_scale in [0, 0.3, 0.5, 1.0, 1.5]:
            name = f"wg-ms*{ms_scale}-s3*{s3_scale}"
            fn = lambda wg, info, _ms=ms_scale, _s3=s3_scale: (
                wg - max(0, info['min_seg']) * _ms - max(0, info['segs'][2]) * _s3
            )
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase3.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<35s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # min_seg 연속 + adj_score 연속
    for ms_scale in [0.5, 1.0, 1.5, 2.0]:
        for sc_scale in [0.05, 0.1, 0.2, 0.3]:
            name = f"wg-ms*{ms_scale}-sc*{sc_scale}"
            fn = lambda wg, info, _ms=ms_scale, _sc=sc_scale: (
                wg - max(0, info['min_seg']) * _ms - max(0, info['adj_score']) * _sc
            )
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase3.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<35s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # 나눗셈 조합
    print("\n  --- 나눗셈 조합 ---")
    for ms_d in [2, 3, 5]:
        for s3_d in [3, 5, 10]:
            name = f"wg/(1+ms/{ms_d})/(1+s3/{s3_d})"
            fn = lambda wg, info, _md=ms_d, _sd=s3_d: (
                wg / (1 + max(0, info['min_seg']) / _md) / (1 + max(0, info['segs'][2]) / _sd)
            )
            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
            diff = avg - baseline_avg
            results_phase3.append((name, avg, mn, mx, std))
            if diff > 0.3:
                print(f"    {name:<35s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # 혼합: 임계값 + 연속
    print("\n  --- 혼합 (임계값 + 연속) ---")
    for ms_t in [1.0, 1.5, 2.0]:
        for ms_m in [1.2, 1.3]:
            for cont_ind in ['seg3', 'adj_score']:
                for scale in [0.3, 0.5, 1.0]:
                    name = f"ms≥{ms_t}x{ms_m}-{cont_ind}*{scale}"
                    if cont_ind == 'seg3':
                        fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _s=scale: (
                            wg * (_mm if info['min_seg'] >= _mt else 1.0)
                            - max(0, info['segs'][2]) * _s
                        )
                    else:
                        fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _s=scale: (
                            wg * (_mm if info['min_seg'] >= _mt else 1.0)
                            - max(0, info['adj_score']) * _s
                        )
                    avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                    diff = avg - baseline_avg
                    results_phase3.append((name, avg, mn, mx, std))
                    if diff > 0.5:
                        print(f"    {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # Phase 3 Top 20
    print("\n" + "=" * 80)
    print("  Phase 3 Top 20 (가산/연속/혼합)")
    print("=" * 80)
    results_phase3.sort(key=lambda x: -x[1])
    for i, (name, avg, mn, mx, std) in enumerate(results_phase3[:20]):
        diff = avg - baseline_avg
        print(f"  {i+1:>2d}. {name:<40s} avg={avg:+.2f}% diff={diff:+.2f}% "
              f"min={mn:+.1f}% max={mx:+.1f}% std={std:.2f}")

    # =========================================================
    # Phase 4: 3-지표 조합 (Top 조합 기반)
    # =========================================================
    print("\n" + "=" * 80)
    print("[5/5] Phase 4: 3-지표 조합 + 최종 비교")
    print("=" * 80)

    results_phase4 = []

    # min_seg + seg3 + adj_score
    print("\n  --- min_seg + seg3 + adj_score ---")
    for ms_t in [1.0, 1.5, 2.0]:
        for ms_m in [1.2, 1.3]:
            for s3_t in [1, 2, 3]:
                for s3_m in [1.1, 1.2]:
                    for sc_t in [20, 30, 40]:
                        for sc_m in [1.1, 1.15]:
                            name = f"ms≥{ms_t}x{ms_m}+s3≥{s3_t}x{s3_m}+sc≥{sc_t}x{sc_m}"
                            fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _st=s3_t, _sm=s3_m, _sct=sc_t, _scm=sc_m: (
                                wg * (_mm if info['min_seg'] >= _mt else 1.0)
                                   * (_sm if info['segs'][2] >= _st else 1.0)
                                   * (_scm if info['adj_score'] >= _sct else 1.0)
                            )
                            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                            diff = avg - baseline_avg
                            results_phase4.append((name, avg, mn, mx, std))
                            if diff > 0.8:
                                print(f"    {name:<55s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # min_seg + seg3 + seg1
    print("\n  --- min_seg + seg3 + seg1 ---")
    for ms_t in [1.0, 1.5, 2.0]:
        for ms_m in [1.2, 1.3]:
            for s3_t in [1, 2, 3]:
                for s3_m in [1.1, 1.2]:
                    for s1_t in [1, 2, 3]:
                        for s1_m in [1.1, 1.2]:
                            name = f"ms≥{ms_t}x{ms_m}+s3≥{s3_t}x{s3_m}+s1≥{s1_t}x{s1_m}"
                            fn = lambda wg, info, _mt=ms_t, _mm=ms_m, _st=s3_t, _sm=s3_m, _s1t=s1_t, _s1m=s1_m: (
                                wg * (_mm if info['min_seg'] >= _mt else 1.0)
                                   * (_sm if info['segs'][2] >= _st else 1.0)
                                   * (_s1m if info['segs'][0] >= _s1t else 1.0)
                            )
                            avg, mn, mx, std, _ = multi_start_eval(dates, all_prices, daily_data, w_gap_cache, fn)
                            diff = avg - baseline_avg
                            results_phase4.append((name, avg, mn, mx, std))
                            if diff > 0.8:
                                print(f"    {name:<55s} avg={avg:+.2f}% diff={diff:+.2f}%")

    # Phase 4 Top 20
    print("\n" + "=" * 80)
    print("  Phase 4 Top 20 (3-지표 조합)")
    print("=" * 80)
    results_phase4.sort(key=lambda x: -x[1])
    for i, (name, avg, mn, mx, std) in enumerate(results_phase4[:20]):
        diff = avg - baseline_avg
        print(f"  {i+1:>2d}. {name:<55s} avg={avg:+.2f}% diff={diff:+.2f}% "
              f"min={mn:+.1f}% max={mx:+.1f}% std={std:.2f}")

    # =========================================================
    # 최종 종합
    # =========================================================
    print("\n" + "=" * 80)
    print("  최종 종합 Top 30 (모든 Phase 통합)")
    print("=" * 80)

    all_results = results_phase1 + results_phase2 + results_phase3 + results_phase4
    all_results.sort(key=lambda x: -x[1])

    # 중복 제거 (같은 avg면 첫 번째만)
    seen_avg = set()
    unique_results = []
    for r in all_results:
        key = round(r[1], 4)
        if key not in seen_avg:
            seen_avg.add(key)
            unique_results.append(r)

    print(f"\n  총 테스트: {len(all_results)}개 조합")
    print(f"  BASELINE: avg={baseline_avg:+.2f}%\n")
    print(f"  {'순위':>4s} {'전략':<55s} {'평균':>8s} {'차이':>8s} {'최소':>8s} {'최대':>8s} {'표준편차':>8s}")
    print("  " + "-" * 95)

    for i, (name, avg, mn, mx, std) in enumerate(unique_results[:30]):
        diff = avg - baseline_avg
        print(f"  {i+1:>4d} {name:<55s} {avg:>+7.2f}% {diff:>+7.2f}% "
              f"{mn:>+7.1f}% {mx:>+7.1f}% {std:>7.2f}")

    # 상위 전략 상세 분석
    print("\n" + "=" * 80)
    print("  상위 5개 전략 — 시작일별 상세")
    print("=" * 80)

    top5_fns = []

    # Top 전략들을 다시 만들어서 상세 분석 (상위 결과에서 추출)
    # 간단히 Phase1+2+3+4에서 이미 계산한 Top 5 이름으로 재현
    for i, (name, avg, mn, mx, std) in enumerate(unique_results[:5]):
        print(f"\n  --- #{i+1}: {name} (avg={avg:+.2f}%) ---")

    print("\n\n=== 연구 완료 ===")
    print(f"  BASELINE: {baseline_avg:+.2f}%")
    if unique_results:
        best = unique_results[0]
        print(f"  BEST: {best[0]} → {best[1]:+.2f}% (diff={best[1]-baseline_avg:+.2f}%)")
        print(f"  개선폭: {(best[1]-baseline_avg)/abs(baseline_avg)*100:.1f}%")


if __name__ == '__main__':
    main()
