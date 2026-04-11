"""그리드 서치: 기존(old) vs 신규(new) 순위 기반 최적 조건 탐색
진입 TopN(2~5), 이탈 TopM(10~20), 슬롯(2~4)
3일 가중 조건 고정, 3일 검증 고정
"""
import sqlite3
import csv
from collections import defaultdict

DB_PATH = 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            # seg cap +/-100%
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0
            data[d][r[0]] = {
                'old_p2': r[1],
                'price': r[2],
                'comp_rank': r[3],
                'min_seg': min_seg,
            }
    conn.close()
    return dates, data


def load_new_ranks():
    new_ranks = {}
    with open('rank_comparison_v73.csv') as f:
        for r in csv.DictReader(f):
            d, tk = r['date'], r['ticker']
            if d not in new_ranks:
                new_ranks[d] = {}
            if r['new_part2_rank']:
                new_ranks[d][tk] = int(r['new_part2_rank'])
    return new_ranks


def simulate(dates, data, rank_source, entry_top, exit_top, max_slots):
    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]

        if rank_source == 'old':
            rank_map = {tk: v['old_p2'] for tk, v in today_data.items()
                        if v['old_p2'] is not None}
        else:
            rank_map = rank_source.get(today, {})

        today_ranked = set(rank_map.keys())
        new_consecutive = defaultdict(int)
        for tk in today_ranked:
            if tk in rank_map and rank_map[tk] <= 30:
                new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')
            should_exit = False
            if rank is None or rank > exit_top:
                should_exit = True
            if min_seg < -2:
                should_exit = True
            if should_exit and price:
                entry_price = portfolio[tk]
                ret = (price - entry_price) / entry_price * 100
                trades.append(ret)
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

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
                portfolio[tk] = price

        if portfolio:
            day_ret = 0
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev_data = data.get(dates[di - 1], {})
                    prev_price = prev_data.get(tk, {}).get('price')
                    if prev_price and prev_price > 0:
                        day_ret += (price - prev_price) / prev_price * 100
            if portfolio:
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

    total_return = (cum_ret - 1) * 100
    n_trades = len(trades)
    win_rate = (sum(1 for t in trades if t > 0) / n_trades * 100
                if n_trades > 0 else 0)
    avg_trade = sum(trades) / n_trades if n_trades > 0 else 0

    return {
        'total_return': round(total_return, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': n_trades,
        'win_rate': round(win_rate, 1),
        'avg_trade': round(avg_trade, 2),
    }


def main():
    print("Loading data...")
    dates, data = load_data()
    new_ranks = load_new_ranks()
    print(f"Period: {dates[0]} ~ {dates[-1]} ({len(dates)} days)")

    entry_range = [2, 3, 4, 5]
    exit_range = [10, 12, 14, 15, 16, 18, 20]
    slot_range = [2, 3, 4]

    results_old = []
    results_new = []

    for entry in entry_range:
        for exit_th in exit_range:
            if exit_th <= entry:
                continue
            for slots in slot_range:
                perf_old = simulate(dates, data, 'old', entry, exit_th, slots)
                results_old.append({
                    'entry': entry, 'exit': exit_th, 'slots': slots, **perf_old
                })
                perf_new = simulate(dates, data, new_ranks, entry, exit_th, slots)
                results_new.append({
                    'entry': entry, 'exit': exit_th, 'slots': slots, **perf_new
                })

    best_old = max(results_old, key=lambda x: x['total_return'])
    print(f"\n=== OLD RANK BEST ===")
    print(f"E{best_old['entry']}/X{best_old['exit']}/S{best_old['slots']}")
    print(f"Return: {best_old['total_return']:+.1f}% | MDD: {best_old['max_dd']:.1f}% | Trades: {best_old['n_trades']} | WR: {best_old['win_rate']:.0f}%")

    current_old = [r for r in results_old
                   if r['entry'] == 5 and r['exit'] == 12 and r['slots'] == 3]
    if current_old:
        c = current_old[0]
        print(f"\nOLD current (E5/X12/S3):")
        print(f"Return: {c['total_return']:+.1f}% | MDD: {c['max_dd']:.1f}% | Trades: {c['n_trades']} | WR: {c['win_rate']:.0f}%")

    best_new = max(results_new, key=lambda x: x['total_return'])
    print(f"\n=== NEW RANK BEST ===")
    print(f"E{best_new['entry']}/X{best_new['exit']}/S{best_new['slots']}")
    print(f"Return: {best_new['total_return']:+.1f}% | MDD: {best_new['max_dd']:.1f}% | Trades: {best_new['n_trades']} | WR: {best_new['win_rate']:.0f}%")

    current_new = [r for r in results_new
                   if r['entry'] == 5 and r['exit'] == 12 and r['slots'] == 3]
    if current_new:
        c = current_new[0]
        print(f"\nNEW current (E5/X12/S3):")
        print(f"Return: {c['total_return']:+.1f}% | MDD: {c['max_dd']:.1f}% | Trades: {c['n_trades']} | WR: {c['win_rate']:.0f}%")

    print(f"\n=== OLD RANK Top 10 ===")
    for i, r in enumerate(sorted(results_old, key=lambda x: -x['total_return'])[:10], 1):
        print(f"{i:2d}. E{r['entry']}/X{r['exit']}/S{r['slots']} : "
              f"{r['total_return']:+6.1f}% MDD {r['max_dd']:5.1f}% "
              f"trades={r['n_trades']:2d} WR={r['win_rate']:.0f}%")

    print(f"\n=== NEW RANK Top 10 ===")
    for i, r in enumerate(sorted(results_new, key=lambda x: -x['total_return'])[:10], 1):
        print(f"{i:2d}. E{r['entry']}/X{r['exit']}/S{r['slots']} : "
              f"{r['total_return']:+6.1f}% MDD {r['max_dd']:5.1f}% "
              f"trades={r['n_trades']:2d} WR={r['win_rate']:.0f}%")

    print(f"\n=== SAME CONDITION COMPARE (old vs new, S=3) ===")
    print(f"{'Config':>12s}  {'Old Ret':>8s}  {'New Ret':>8s}  {'Diff':>6s}  "
          f"{'Old MDD':>7s}  {'New MDD':>7s}")
    for entry in [3, 4, 5]:
        for exit_th in [10, 12, 15, 20]:
            for slots in [3]:
                old_r = [r for r in results_old
                         if r['entry'] == entry and r['exit'] == exit_th
                         and r['slots'] == slots]
                new_r = [r for r in results_new
                         if r['entry'] == entry and r['exit'] == exit_th
                         and r['slots'] == slots]
                if old_r and new_r:
                    o, n = old_r[0], new_r[0]
                    diff = n['total_return'] - o['total_return']
                    label = f"E{entry}/X{exit_th}/S{slots}"
                    print(f"{label:>12s}  {o['total_return']:+7.1f}%  "
                          f"{n['total_return']:+7.1f}%  {diff:+5.1f}%  "
                          f"{o['max_dd']:6.1f}%  {n['max_dd']:6.1f}%")


if __name__ == '__main__':
    main()
