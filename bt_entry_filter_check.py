"""진입 시 이탈조건 충족 종목 스킵 효과 검증

비교:
A. 기존 v55 (진입 필터 없음, 이탈만 min_seg<-2%)
B. 진입 시에도 min_seg<-2% 스킵 (논리적 일관성)
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('eps_momentum_data.db')
c = conn.cursor()

dates = [d[0] for d in c.execute(
    'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
).fetchall()]

rank_data = {}
all_prices = {}
seg_data = {}

for date in dates:
    rows = c.execute('''
        SELECT ticker, part2_rank, adj_gap, price,
               ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
        FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank
    ''', (date,)).fetchall()

    rank_data[date] = {}
    seg_data[date] = {}
    for t, r, ag, p, nc, n7, n30, n60, n90 in rows:
        rank_data[date][t] = {'rank': r, 'price': p}
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            if b and abs(b) > 0.01:
                segs.append((a - b) / abs(b) * 100)
            else:
                segs.append(0)
        seg_data[date][t] = tuple(segs)

    rows2 = c.execute(
        'SELECT ticker, price FROM ntm_screening WHERE date=? AND price IS NOT NULL', (date,)
    ).fetchall()
    all_prices[date] = {t: p for t, p in rows2}

conn.close()


def simulate(entry_n, exit_n, max_pos, skip_entry_unhealthy=False):
    portfolio = {}
    nav = 100.0
    trades = []
    entry_skips = 0

    for i, date in enumerate(dates):
        today = rank_data.get(date, {})
        prices = all_prices.get(date, {})
        prev_prices = all_prices.get(dates[i - 1], {}) if i > 0 else {}
        segs = seg_data.get(date, {})

        day_pnl = 0.0
        if portfolio and i > 0:
            weight = 1.0 / max_pos
            for t in portfolio:
                p_today = prices.get(t)
                p_prev = prev_prices.get(t)
                if p_today and p_prev and p_prev > 0:
                    day_pnl += weight * (p_today - p_prev) / p_prev
            nav *= (1 + day_pnl)

        # 이탈 1: 순위
        for t in list(portfolio.keys()):
            if t not in today or today[t]['rank'] > exit_n:
                sell_p = prices.get(t, 0)
                ret = (sell_p - portfolio[t]['price']) / portfolio[t]['price'] if sell_p and portfolio[t]['price'] else 0
                trades.append(ret)
                del portfolio[t]

        # 이탈 2: min_seg < -2%
        for t in list(portfolio.keys()):
            s = segs.get(t, (0, 0, 0, 0))
            if min(s) < -2:
                sell_p = prices.get(t, 0)
                ret = (sell_p - portfolio[t]['price']) / portfolio[t]['price'] if sell_p and portfolio[t]['price'] else 0
                trades.append(ret)
                del portfolio[t]

        # 진입
        open_slots = max_pos - len(portfolio)
        candidates = [t for t, _ in sorted(today.items(), key=lambda x: x[1]['rank'])[:exit_n]]
        for t in candidates:
            if open_slots <= 0:
                break
            if t not in portfolio and t in prices and prices[t]:
                # 진입 시 이탈조건 스킵?
                if skip_entry_unhealthy:
                    s = segs.get(t, (0, 0, 0, 0))
                    if min(s) < -2:
                        entry_skips += 1
                        continue
                portfolio[t] = {'price': prices[t], 'date': date}
                open_slots -= 1

    # 미청산
    last_p = all_prices.get(dates[-1], {})
    for t, pos in portfolio.items():
        p = last_p.get(t, 0)
        ret = (p - pos['price']) / pos['price'] if p and pos['price'] else 0
        trades.append(ret)

    total_ret = (nav - 100) / 100
    wins = sum(1 for r in trades if r > 0)
    wr = wins / len(trades) * 100 if trades else 0
    return nav, total_ret, len(trades), wr, entry_skips


# Top3/Top15 전략 비교
strategies = [
    (3, 5, 3),
    (3, 7, 3),
    (3, 10, 3),
    (3, 15, 3),
    (5, 7, 5),
    (5, 10, 5),
    (5, 15, 5),
]

print(f'데이터: {len(dates)}거래일 ({dates[0]} ~ {dates[-1]})')
print()
print(f'{"전략":>14} | {"A(기존)":>10} {"거래":>4} {"승률":>5} | {"B(스킵)":>10} {"거래":>4} {"승률":>5} {"스킵":>4} | {"차이":>7}')
print('-' * 85)

for entry_n, exit_n, max_pos in strategies:
    label = f'Top{entry_n}/Top{exit_n}({max_pos})'

    # A: 기존 (진입 필터 없음)
    nav_a, ret_a, n_a, wr_a, _ = simulate(entry_n, exit_n, max_pos, skip_entry_unhealthy=False)

    # B: 진입 시에도 min_seg<-2% 스킵
    nav_b, ret_b, n_b, wr_b, skips = simulate(entry_n, exit_n, max_pos, skip_entry_unhealthy=True)

    diff = ret_b - ret_a
    print(f'{label:>14} | {ret_a*100:>+9.1f}% {n_a:>4} {wr_a:>4.0f}% | {ret_b*100:>+9.1f}% {n_b:>4} {wr_b:>4.0f}% {skips:>4} | {diff*100:>+6.1f}%')
