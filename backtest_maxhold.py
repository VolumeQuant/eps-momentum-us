"""Top5 진입 + Top30 이탈 + 최대 보유 제한 비교"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
from collections import defaultdict

DB = 'eps_momentum_data.db'
conn = sqlite3.connect(DB)
c = conn.cursor()
c.execute('''
    SELECT date, ticker, part2_rank, price
    FROM ntm_screening
    WHERE part2_rank IS NOT NULL AND price IS NOT NULL
    ORDER BY date, part2_rank
''')
rows = c.fetchall()
conn.close()

daily = defaultdict(dict)
for date, ticker, rank, price in rows:
    daily[date][ticker] = (rank, price)
dates = sorted(daily.keys())

def simulate(max_hold):
    portfolio = {}  # {ticker: {entry_date, entry_price, last_price}}
    trades = []

    for date in dates:
        data = daily[date]
        top5 = [(t, r, p) for t, (r, p) in data.items() if r <= 5]
        top5.sort(key=lambda x: x[1])  # rank순
        top30 = {t for t, (r, p) in data.items() if r <= 30}

        # 매도: Top30 이탈
        for t in list(portfolio.keys()):
            if t not in top30:
                info = portfolio.pop(t)
                exit_price = data.get(t, (None, None))[1] or info.get('last_price', info['entry_price'])
                ret = (exit_price - info['entry_price']) / info['entry_price'] * 100
                trades.append({'ticker': t, 'entry_date': info['entry_date'],
                    'exit_date': date, 'return': ret})

        # 매수: Top5 신규 (빈자리만큼)
        slots = max_hold - len(portfolio)
        if slots > 0:
            for t, r, p in top5:
                if t not in portfolio and slots > 0:
                    portfolio[t] = {'entry_date': date, 'entry_price': p}
                    slots -= 1

        # 가격 업데이트
        for t in portfolio:
            if t in data:
                portfolio[t]['last_price'] = data[t][1]

    # 미실현
    unrealized = []
    for t, info in portfolio.items():
        last = info.get('last_price', info['entry_price'])
        ret = (last - info['entry_price']) / info['entry_price'] * 100
        unrealized.append((t, ret))

    all_ret = [t['return'] for t in trades] + [u[1] for u in unrealized]

    # 동일비중 포트폴리오 수익률 (일별 계산)
    # 간단히: 모든 종목 동일비중 평균
    avg = sum(all_ret) / len(all_ret) if all_ret else 0
    total_stocks = len(trades) + len(unrealized)
    holding_now = len(portfolio)

    return avg, total_stocks, holding_now, trades, unrealized

print(f"데이터: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n")
print(f"{'제한':>4s} | {'종목수':>5s} | {'현보유':>4s} | {'평균':>7s} | 보유 종목")
print("-" * 75)

for max_h in [3, 4, 5, 6, 7, 8, 10, 99]:
    avg, total, holding, trades, unreal = simulate(max_h)
    label = "무제한" if max_h == 99 else f"{max_h}종목"
    held = [f"{t}({r:+.0f}%)" for t, r in sorted(unreal, key=lambda x: x[1], reverse=True)]
    exited = [f"{t['ticker']}({t['return']:+.0f}%)" for t in trades]
    all_names = held + [f"×{e}" for e in exited]
    print(f"{label:>6s} | {total:5d} | {holding:4d} | {avg:+6.1f}% | {', '.join(all_names)}")

# 상세: 5종목 제한
print(f"\n{'='*60}")
print(f"=== 5종목 제한 상세 ===")
print(f"{'='*60}")
portfolio = {}
for date in dates:
    data = daily[date]
    top5 = [(t, r, p) for t, (r, p) in data.items() if r <= 5]
    top5.sort(key=lambda x: x[1])
    top30 = {t for t, (r, p) in data.items() if r <= 30}

    exits = []
    for t in list(portfolio.keys()):
        if t not in top30:
            info = portfolio.pop(t)
            ep = data.get(t, (None, None))[1] or info.get('last_price', info['entry_price'])
            ret = (ep - info['entry_price']) / info['entry_price'] * 100
            exits.append(f"{t}({ret:+.1f}%)")

    entries = []
    slots = 5 - len(portfolio)
    for t, r, p in top5:
        if t not in portfolio and slots > 0:
            portfolio[t] = {'entry_date': date, 'entry_price': p}
            entries.append(f"{t}(#{r})")
            slots -= 1

    for t in portfolio:
        if t in data:
            portfolio[t]['last_price'] = data[t][1]

    if entries or exits:
        held = sorted(portfolio.keys())
        print(f"{date}: +[{', '.join(entries or ['-'])}] -[{', '.join(exits or ['-'])}] → 보유 {held}")

print(f"\n최종 보유:")
for t, info in sorted(portfolio.items(), key=lambda x: x[1].get('last_price', x[1]['entry_price'])/x[1]['entry_price'], reverse=True):
    last = info.get('last_price', info['entry_price'])
    ret = (last - info['entry_price']) / info['entry_price'] * 100
    print(f"  {t:6s} {info['entry_date']}~ ${info['entry_price']:.1f}→${last:.1f} {ret:+.1f}%")
