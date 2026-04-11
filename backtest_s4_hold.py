"""S4: Breakout Hold 효과 검증

핵심: DB part2_rank 위에 추가 이탈 유예 조건만 적용 → 100% 정확
변형: 4가지 조건 (엄격 → 매우 완화)
"""
import sqlite3
from collections import defaultdict
from backtest_s2_params import simulate as base_simulate, load_data


# Hold 조건 4종
HOLD_CONFIGS = {
    'strict':       {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4, 'check_ma60': True, 'max_grace': 2},
    'moderate':     {'lookback_days': 10, 'price_threshold': 15, 'rev_up_ratio': 0.3, 'check_ma60': True, 'max_grace': 2},
    'loose':        {'lookback_days': 5, 'price_threshold': 10, 'rev_up_ratio': 0.3, 'check_ma60': True, 'max_grace': 3},
    'very_loose':   {'lookback_days': 5, 'price_threshold': 5, 'rev_up_ratio': 0.2, 'check_ma60': False, 'max_grace': 3},
}


def load_data_full():
    """price/MA/EPS 정보 모두 포함된 data"""
    DB_PATH = 'eps_momentum_data.db'
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   rev_up30, num_analysts, ma60, ma120
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
                'p2': r[1], 'price': r[2], 'comp_rank': r[3],
                'ntm_current': nc, 'ntm_90d': n90,
                'rev_up30': r[9] or 0, 'num_analysts': r[10] or 0,
                'ma60': r[11], 'ma120': r[12], 'min_seg': min_seg,
            }
    conn.close()
    return dates, data


def check_breakout(data, dates, today_idx, ticker, params):
    """Breakout 조건 체크"""
    today = dates[today_idx]
    v = data[today].get(ticker)
    if not v:
        return False

    if today_idx < params['lookback_days']:
        return False
    past = data.get(dates[today_idx - params['lookback_days']], {}).get(ticker)
    if not past or not past.get('price') or past['price'] <= 0:
        return False
    price_chg = (v['price'] - past['price']) / past['price'] * 100
    if price_chg < params['price_threshold']:
        return False

    if not v.get('ntm_90d') or v['ntm_90d'] <= 0:
        return False
    if v['ntm_current'] <= v['ntm_90d']:
        return False

    if v['num_analysts'] < 1:
        return False
    if (v['rev_up30'] / v['num_analysts']) < params['rev_up_ratio']:
        return False

    if params['check_ma60']:
        if not v.get('ma60') or v['price'] <= v['ma60']:
            return False
    return True


def simulate_with_hold(dates_all, data, entry_top, exit_top, max_slots,
                        hold_params=None, start_date=None):
    """hold 조건 추가된 시뮬레이션"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)
    breakout_holds_used = 0
    breakout_hold_days = defaultdict(int)  # ticker별 hold 일수

    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}

        new_consecutive = defaultdict(int)
        for tk in rank_map:
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

            # Breakout hold check (rank 사유만)
            if should_exit and exit_reason == 'rank' and hold_params:
                global_idx = dates_all.index(today)
                if check_breakout(data, dates_all, global_idx, tk, hold_params):
                    grace = portfolio[tk].get('grace_days', 0)
                    if grace < hold_params['max_grace']:
                        portfolio[tk]['grace_days'] = grace + 1
                        breakout_holds_used += 1
                        breakout_hold_days[tk] += 1
                        should_exit = False

            if should_exit and price:
                entry_price = portfolio[tk]['entry_price']
                ret = (price - entry_price) / entry_price * 100
                trades.append({
                    'ticker': tk, 'entry_date': portfolio[tk]['entry_date'],
                    'exit_date': today, 'entry_price': entry_price,
                    'exit_price': price, 'return': ret,
                    'hold_used': portfolio[tk].get('grace_days', 0),
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
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr_ in daily_returns:
        cum_ret *= (1 + dr_ / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    closed = [t['return'] for t in trades]
    n_trades = len(closed)

    return {
        'total_return': round((cum_ret - 1) * 100, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': n_trades,
        'n_open': len(portfolio),
        'breakout_holds': breakout_holds_used,
        'trades': trades,
    }


def sample_test_trigger_freq(dates, data):
    """표본 테스트: 각 조건의 트리거 발생 빈도"""
    print("=" * 70)
    print("표본 테스트: Breakout Hold 트리거 빈도 (G 위에서)")
    print("=" * 70)

    for name, params in HOLD_CONFIGS.items():
        r = simulate_with_hold(dates, data, 3, 10, 2, hold_params=params)
        print(f"\n[{name}] {params}")
        print(f"  수익: {r['total_return']:+.2f}%, 거래: {r['n_trades']}, "
              f"트리거: {r['breakout_holds']}회")

    # baseline 비교 (no hold)
    r_base = simulate_with_hold(dates, data, 3, 10, 2, hold_params=None)
    print(f"\n[no_hold] baseline")
    print(f"  수익: {r_base['total_return']:+.2f}%, 거래: {r_base['n_trades']}")


def multistart_compare_hold(dates, data, label, e, x, s):
    """multistart: 4 hold 변형 + no hold"""
    print(f"\n=== {label}: E{e}/X{x}/S{s} (multistart 17개 시작일) ===")

    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::2]

    variants = [('no_hold', None)] + [(name, params) for name, params in HOLD_CONFIGS.items()]

    print(f"{'Variant':<14s} {'평균':>7s} {'중앙값':>7s} {'표준편차':>8s} "
          f"{'최저':>7s} {'최고':>7s} {'트리거':>7s}")
    print("-" * 65)

    base_avg = None
    for name, params in variants:
        rets = []
        triggers = []
        for sd in samples:
            r = simulate_with_hold(dates, data, e, x, s,
                                    hold_params=params, start_date=sd)
            rets.append(r['total_return'])
            triggers.append(r['breakout_holds'])
        avg = sum(rets) / len(rets)
        std = (sum((r - avg) ** 2 for r in rets) / len(rets)) ** 0.5
        rets_sorted = sorted(rets)
        median = rets_sorted[len(rets_sorted) // 2]
        avg_trigger = sum(triggers) / len(triggers)
        print(f"{name:<14s} {avg:+6.2f}% {median:+6.2f}% {std:>7.2f} "
              f"{min(rets):+6.1f}% {max(rets):+6.1f}% {avg_trigger:>6.1f}")
        if name == 'no_hold':
            base_avg = avg

    print(f"\n  no_hold 대비 차이:")
    for name, params in variants:
        if name != 'no_hold':
            rets = []
            for sd in samples:
                r = simulate_with_hold(dates, data, e, x, s, hold_params=params, start_date=sd)
                rets.append(r['total_return'])
            avg = sum(rets) / len(rets)
            print(f"    {name:<14s}: {avg - base_avg:+5.2f}%p")


def main():
    print("S4: Breakout Hold 효과 검증\n")
    dates, data = load_data_full()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n")

    # Step 1: 표본 테스트 - 트리거 빈도
    sample_test_trigger_freq(dates, data)

    # Step 2: 3 baseline에서 hold 효과
    print("\n\n" + "=" * 70)
    print("Multistart 비교 (3 baseline)")
    print("=" * 70)

    multistart_compare_hold(dates, data, "G: E3/X10/S2", 3, 10, 2)
    multistart_compare_hold(dates, data, "E3/X9/S2", 3, 9, 2)
    multistart_compare_hold(dates, data, "E3/X10/S1", 3, 10, 1)


if __name__ == '__main__':
    main()
