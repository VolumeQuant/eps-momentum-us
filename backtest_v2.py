"""확장 백테스트: 7가지 변형 + Multistart + Capture Ratio 분석

변형:
  A: E5 baseline (현재)
  B: E3 단독
  F: E3 + Conviction 없음 (raw adj_gap)
  C: E3 + Conviction 강화 (이중확증 2.5x)
  D: E3 + Breakout Hold
  E: E3 + Conv 강화 + Hold (full)
  G: E3 + Conviction 없음 + Hold (역방향 full)

추가 분석:
  - Multistart: 진입일 2/12 ~ 2/25 변동
  - Capture ratio: 진입 후 60일 max gain 대비 실현
  - 4/10 데이터 포함 (가능하면)
"""
import sqlite3
from collections import defaultdict
import numpy as np

DB_PATH = 'eps_momentum_data.db'


def load_full_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, rev_up30, rev_down30, num_analysts,
                   ma60, ma120
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0
            data[d][tk] = {
                'old_p2': r[1], 'price': r[2], 'comp_rank': r[3],
                'ntm_current': nc, 'ntm_7d': n7, 'ntm_30d': n30,
                'ntm_60d': n60, 'ntm_90d': n90,
                'adj_gap': r[9],
                'rev_up30': r[10] or 0, 'rev_down30': r[11] or 0,
                'num_analysts': r[12] or 0,
                'ma60': r[13], 'ma120': r[14],
                'min_seg': min_seg,
            }
    conn.close()
    return dates, data


def conv_none(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d):
    """No conviction — raw adj_gap"""
    return adj_gap


def conv_base(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d):
    """현재 conviction (1~2x)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if num_analysts and num_analysts > 0 else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def conv_strong(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d):
    """강화 conviction (1~2.5x dual)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if num_analysts and num_analysts > 0 else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 2.0)
    base = max(ratio, min(eps_floor, 1.0))
    tail_bonus = 0
    if ratio >= 0.5 and eps_floor >= 1.0:
        tail_bonus = min((eps_floor - 1.0) * 0.5, 0.5)
    conviction = base + tail_bonus
    return adj_gap * (1 + conviction)


def recompute_ranks(data, conviction_fn):
    """conviction 함수로 composite_rank + part2_rank 재계산

    중요: tie-break을 conv_gap 원본값으로 함 (sim 정확성 버그 수정)
    z-score 100 cap에 걸린 종목들의 동점을 conv_gap으로 풀어줌.
    """
    dates = sorted(data.keys())
    new_score_by_date = {}
    new_conv_gaps_by_date = {}  # tie-break용

    for d in dates:
        ticker_gaps = {}
        for tk, v in data[d].items():
            if v['min_seg'] < -2:
                continue
            cg = conviction_fn(v['adj_gap'], v['rev_up30'], v['num_analysts'],
                               v['ntm_current'], v['ntm_90d'])
            if cg is not None:
                ticker_gaps[tk] = cg
        new_conv_gaps_by_date[d] = ticker_gaps

        vals = list(ticker_gaps.values())
        if len(vals) >= 2:
            mean_v = np.mean(vals)
            std_v = np.std(vals)
            if std_v > 0:
                new_score_by_date[d] = {
                    tk: min(100.0, max(30.0, 65 + (-(v - mean_v) / std_v) * 15))
                    for tk, v in ticker_gaps.items()
                }
            else:
                new_score_by_date[d] = {tk: 65 for tk in ticker_gaps}
        else:
            new_score_by_date[d] = {tk: 65 for tk in ticker_gaps}

    new_p2 = {}
    for di, today in enumerate(dates):
        recent = []
        for j in range(di, max(di - 3, -1), -1):
            if new_score_by_date.get(dates[j]):
                recent.insert(0, dates[j])
            if len(recent) >= 3:
                break
        if not recent:
            continue
        weights = [0.2, 0.3, 0.5]
        if len(recent) == 2:
            weights = [0.4, 0.6]
        elif len(recent) == 1:
            weights = [1.0]
        eligible = list(new_score_by_date.get(today, {}).keys())

        def carry(tk, idx):
            for j in range(idx - 1, -1, -1):
                prev = new_score_by_date.get(recent[j], {}).get(tk)
                if prev is not None:
                    return prev
            return 30

        wgap_map = {}
        for tk in eligible:
            ws = 0
            for i, d in enumerate(recent):
                score = new_score_by_date.get(d, {}).get(tk)
                if score is None:
                    score = carry(tk, i)
                ws += score * weights[i]
            wgap_map[tk] = ws

        # tie-break: 동점일 때 오늘의 conv_gap이 작은(음수) 순서대로
        today_conv = new_conv_gaps_by_date.get(today, {})
        sorted_tks = sorted(eligible,
                            key=lambda tk: (-wgap_map.get(tk, -999),
                                            today_conv.get(tk, 0)))
        new_p2[today] = {tk: i + 1 for i, tk in enumerate(sorted_tks[:30])}
    return new_p2


def is_breakout_hold(data, dates, today_idx, ticker):
    if today_idx < 20:
        return False
    today = dates[today_idx]
    v = data[today].get(ticker)
    if not v:
        return False
    past = data.get(dates[today_idx - 20], {}).get(ticker)
    if not past or not past.get('price') or past['price'] <= 0:
        return False
    price_chg_20d = (v['price'] - past['price']) / past['price'] * 100
    if price_chg_20d < 25:
        return False
    if not v.get('ntm_90d') or v['ntm_90d'] <= 0:
        return False
    if v['ntm_current'] <= v['ntm_90d']:
        return False
    if v['num_analysts'] < 1:
        return False
    if (v['rev_up30'] / v['num_analysts']) < 0.4:
        return False
    if not v.get('ma60') or v['price'] <= v['ma60']:
        return False
    return True


def simulate(dates_all, data, p2_ranks, entry_top, exit_top, max_slots,
             use_breakout_hold=False, start_date=None):
    """start_date 이후만 시뮬 (multistart 지원)"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)
    breakout_holds_used = 0

    # consecutive 초기화: start_date 이전 데이터로 채움 (3일 검증 위해)
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk in p2_ranks.get(d, {}):
                consecutive[tk] = consecutive.get(tk, 0) + 1

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = p2_ranks.get(today, {})

        new_consecutive = defaultdict(int)
        for tk in rank_map:
            if rank_map[tk] <= 30:
                new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False
            exit_reason = ''
            if rank is None or rank > exit_top:
                should_exit = True
                exit_reason = 'rank'
            if min_seg < -2:
                should_exit = True
                exit_reason = 'min_seg'

            if should_exit and exit_reason == 'rank' and use_breakout_hold:
                # multistart에서 today_idx 계산
                global_idx = dates_all.index(today)
                if is_breakout_hold(data, dates_all, global_idx, tk):
                    grace = portfolio[tk].get('grace_days', 0)
                    if grace < 2:
                        portfolio[tk]['grace_days'] = grace + 1
                        breakout_holds_used += 1
                        should_exit = False

            if should_exit and price:
                entry_price = portfolio[tk]['entry_price']
                ret = (price - entry_price) / entry_price * 100
                trades.append({
                    'ticker': tk,
                    'entry_date': portfolio[tk].get('entry_date', '?'),
                    'exit_date': today,
                    'entry_price': entry_price,
                    'exit_price': price,
                    'return': ret,
                })
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            candidates = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry_top:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    candidates.append((tk, price))
            for tk, price in candidates[:vacancies]:
                portfolio[tk] = {'entry_price': price, 'grace_days': 0,
                                  'entry_date': today}

        # 일간 수익
        if portfolio:
            day_ret = 0
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev_data = data.get(dates[di - 1], {})
                    prev_price = prev_data.get(tk, {}).get('price')
                    if prev_price and prev_price > 0:
                        day_ret += (price - prev_price) / prev_price * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr in daily_returns:
        cum_ret *= (1 + dr / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    # 미실현 수익 (open positions의 entry → 마지막 가격)
    unrealized_returns = []
    for tk, pos in portfolio.items():
        last_price = data.get(dates[-1], {}).get(tk, {}).get('price')
        if last_price and pos['entry_price']:
            unrealized_returns.append((last_price - pos['entry_price']) / pos['entry_price'] * 100)

    closed_returns = [t['return'] for t in trades]
    n_trades = len(closed_returns)
    win_rate = (sum(1 for t in closed_returns if t > 0) / n_trades * 100
                if n_trades > 0 else 0)

    return {
        'total_return': round((cum_ret - 1) * 100, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': n_trades,
        'n_open': len(portfolio),
        'win_rate': round(win_rate, 1),
        'best_trade': round(max(closed_returns), 2) if closed_returns else 0,
        'worst_trade': round(min(closed_returns), 2) if closed_returns else 0,
        'avg_unrealized': round(sum(unrealized_returns) / len(unrealized_returns), 2) if unrealized_returns else 0,
        'breakout_holds': breakout_holds_used,
        'trades': trades,
        'open_positions': list(portfolio.keys()),
    }


def main():
    print("Loading data...")
    dates, data = load_full_data()
    print(f"Period: {dates[0]} ~ {dates[-1]} ({len(dates)} days)")

    base_p2 = {d: {tk: v['old_p2'] for tk, v in data[d].items() if v.get('old_p2') is not None}
               for d in dates}

    print("\nRecomputing ranks for 3 conviction levels...")
    none_p2 = recompute_ranks(data, conv_none)
    base_p2_recomputed = recompute_ranks(data, conv_base)
    strong_p2 = recompute_ranks(data, conv_strong)

    # Sanity check: base_p2_recomputed should match base_p2 closely
    print(f"  base_p2 (DB)        rank assigned days: {sum(1 for d in dates if base_p2.get(d))}")
    print(f"  base_p2_recomputed  rank assigned days: {sum(1 for d in dates if base_p2_recomputed.get(d))}")

    print("\n=== Test 1: 7 Variants (single backtest, full period) ===\n")
    variants = [
        ('A: E5 baseline (DB)',         base_p2,           5, 12, 3, False),
        ('B: E3 단독 (DB)',               base_p2,           3, 12, 3, False),
        ('B2: E3 단독 (recomputed)',      base_p2_recomputed,3, 12, 3, False),
        ('F: E3 + No conviction',       none_p2,           3, 12, 3, False),
        ('C: E3 + Strong conviction',   strong_p2,         3, 12, 3, False),
        ('D: E3 + Breakout Hold',       base_p2,           3, 12, 3, True),
        ('E: E3 + Strong + Hold',       strong_p2,         3, 12, 3, True),
        ('G: E3 + None + Hold',         none_p2,           3, 12, 3, True),
    ]

    results = []
    for name, p2, e, x, s, hold in variants:
        r = simulate(dates, data, p2, e, x, s, use_breakout_hold=hold)
        r['name'] = name
        results.append(r)

    print(f"{'Variant':<32s} {'Ret':>7s} {'MDD':>7s} {'Trd':>4s} {'WR':>4s} "
          f"{'Best':>7s} {'Worst':>7s} {'AvgOpen':>8s} {'Hold':>5s}")
    print('-' * 90)
    for r in results:
        print(f"{r['name']:<32s} {r['total_return']:+6.1f}% {r['max_dd']:+6.1f}% "
              f"{r['n_trades']:>4d} {r['win_rate']:>3.0f}% "
              f"{r['best_trade']:+6.1f}% {r['worst_trade']:+6.1f}% "
              f"{r['avg_unrealized']:+7.1f}% {r['breakout_holds']:>5d}")

    print("\n=== A 대비 변화 ===")
    base = results[0]['total_return']
    base_dd = results[0]['max_dd']
    for r in results[1:]:
        ret_diff = r['total_return'] - base
        print(f"  {r['name']:<32s}: ret {ret_diff:+6.2f}%p, MDD {r['max_dd']-base_dd:+5.2f}%p")

    # ==========================================
    # Test 2: Multistart (진입일 변동)
    # ==========================================
    print("\n\n=== Test 2: Multistart Backtest (진입일 변동) ===\n")
    print("진입일을 다양하게 잡아 행운 효과 제거\n")

    # 가능한 시작일들 (3일 검증 후 진입 가능 시점부터)
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    start_samples = start_dates[::3][:8]  # 8개 시작일
    print(f"시작일 샘플: {start_samples}\n")

    multistart_variants = [
        ('A: E5 baseline', base_p2, 5, 12, 3, False),
        ('B: E3', base_p2, 3, 12, 3, False),
        ('F: E3 + No conviction', none_p2, 3, 12, 3, False),
        ('C: E3 + Strong conv', strong_p2, 3, 12, 3, False),
    ]

    print(f"{'시작일':<12s} ", end='')
    for name, *_ in multistart_variants:
        print(f"{name:>20s}", end='')
    print()
    print('-' * (12 + 20 * len(multistart_variants)))

    multistart_results = {name: [] for name, *_ in multistart_variants}
    for sd in start_samples:
        print(f"{sd:<12s} ", end='')
        for name, p2, e, x, s, hold in multistart_variants:
            r = simulate(dates, data, p2, e, x, s, use_breakout_hold=hold, start_date=sd)
            print(f"{r['total_return']:+8.1f}% (T={r['n_trades']:>2d})  ", end='')
            multistart_results[name].append(r['total_return'])
        print()

    print("\n=== Multistart 평균 ===")
    for name in multistart_results:
        rets = multistart_results[name]
        avg = sum(rets) / len(rets)
        std = (sum((r - avg) ** 2 for r in rets) / len(rets)) ** 0.5
        print(f"  {name:<28s}: 평균 {avg:+6.2f}%, 표준편차 {std:5.2f}, "
              f"최소 {min(rets):+6.1f}%, 최대 {max(rets):+6.1f}%")

    # ==========================================
    # Test 3: Capture Ratio 분석
    # ==========================================
    print("\n\n=== Test 3: Capture Ratio (전체 보유 종목의 잠재 수익) ===\n")
    # baseline 변형의 모든 closed trade에 대해 진입~exit_date+30일 max gain 측정
    base_result = results[0]
    print(f"Baseline closed trades에서 capture ratio 계산:\n")
    print(f"{'ticker':>6s} {'entry':<12s} {'exit':<12s} {'realized':>9s} {'max_60d':>9s} {'capture':>9s}")
    print('-' * 60)

    captures = []
    for t in base_result['trades']:
        tk = t['ticker']
        entry_date = t['entry_date']
        entry_price = t['entry_price']
        # entry 이후 60일 max 가격 찾기
        try:
            entry_idx = dates.index(entry_date)
        except ValueError:
            continue
        max_price = entry_price
        for j in range(entry_idx, min(entry_idx + 60, len(dates))):
            d = dates[j]
            p = data.get(d, {}).get(tk, {}).get('price')
            if p:
                max_price = max(max_price, p)
        max_gain = (max_price - entry_price) / entry_price * 100
        realized = t['return']
        capture = realized / max_gain if max_gain > 0 else 1.0
        captures.append(capture)
        print(f"{tk:>6s} {entry_date:<12s} {t['exit_date']:<12s} "
              f"{realized:+8.1f}% {max_gain:+8.1f}% {capture*100:+8.0f}%")

    if captures:
        avg_capture = sum(captures) / len(captures)
        print(f"\n평균 capture ratio: {avg_capture*100:.0f}%")
        if avg_capture < 0.5:
            print("[!] 시스템이 winners를 일찍 빠짐 (Breakout Hold 가치 있음)")
        else:
            print("[OK] 시스템이 winners를 잘 잡고 있음")


if __name__ == '__main__':
    main()
