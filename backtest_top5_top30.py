"""Top5 진입 매수 + Top30 이탈 매도 백테스트"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

DB = 'eps_momentum_data.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# 날짜별 part2_rank + price
c.execute('''
    SELECT date, ticker, part2_rank, price
    FROM ntm_screening
    WHERE part2_rank IS NOT NULL AND price IS NOT NULL
    ORDER BY date, part2_rank
''')
rows = c.fetchall()
conn.close()

# 날짜별 정리
from collections import defaultdict
daily = defaultdict(dict)  # {date: {ticker: (rank, price)}}
for date, ticker, rank, price in rows:
    daily[date][ticker] = (rank, price)

dates = sorted(daily.keys())
print(f"데이터 기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

# 포트폴리오 시뮬레이션
portfolio = {}  # {ticker: {'entry_date', 'entry_price'}}
trades = []     # completed trades
daily_log = []

for date in dates:
    data = daily[date]
    top5 = {t for t, (r, p) in data.items() if r <= 5}
    top30 = {t for t, (r, p) in data.items() if r <= 30}

    # 매도: Top30 이탈
    exits = []
    for t in list(portfolio.keys()):
        if t not in top30:
            entry = portfolio.pop(t)
            exit_price = data.get(t, (None, None))[1]
            if exit_price is None:
                # 이탈일에 가격 없으면 전날 가격 사용
                exit_price = entry.get('last_price', entry['entry_price'])
            ret = (exit_price - entry['entry_price']) / entry['entry_price'] * 100
            trades.append({
                'ticker': t, 'entry_date': entry['entry_date'],
                'entry_price': entry['entry_price'],
                'exit_date': date, 'exit_price': exit_price, 'return': ret
            })
            exits.append(f"{t}({ret:+.1f}%)")

    # 매수: Top5 신규
    entries = []
    for t in top5:
        if t not in portfolio:
            price = data[t][1]
            portfolio[t] = {'entry_date': date, 'entry_price': price}
            entries.append(t)

    # 보유 종목 최신 가격 업데이트
    for t in portfolio:
        if t in data:
            portfolio[t]['last_price'] = data[t][1]

    if entries or exits:
        holding = len(portfolio)
        daily_log.append(f"{date}: 매수 {entries if entries else '-'} | 매도 {exits if exits else '-'} | 보유 {holding}종목")

print("\n=== 일별 매매 ===")
for log in daily_log:
    print(log)

# 현재 보유 중 (미실현)
print(f"\n=== 완료 거래 ({len(trades)}건) ===")
for t in sorted(trades, key=lambda x: x['return'], reverse=True):
    print(f"  {t['ticker']:6s} {t['entry_date']}→{t['exit_date']} ${t['entry_price']:.1f}→${t['exit_price']:.1f} {t['return']:+.1f}%")

print(f"\n=== 미실현 보유 ({len(portfolio)}종목) ===")
unrealized = []
for t, info in portfolio.items():
    last = info.get('last_price', info['entry_price'])
    ret = (last - info['entry_price']) / info['entry_price'] * 100
    unrealized.append((t, info['entry_date'], info['entry_price'], last, ret))
    print(f"  {t:6s} {info['entry_date']}~ ${info['entry_price']:.1f}→${last:.1f} {ret:+.1f}%")

# 전체 수익률 (동일 비중 가정)
all_returns = [t['return'] for t in trades] + [u[4] for u in unrealized]
if all_returns:
    avg = sum(all_returns) / len(all_returns)
    print(f"\n=== 요약 ===")
    print(f"총 매매: {len(trades)}건 완료 + {len(unrealized)}건 보유중")
    print(f"종목 평균 수익률: {avg:+.1f}%")
    print(f"승률: {sum(1 for r in all_returns if r > 0)}/{len(all_returns)} ({sum(1 for r in all_returns if r > 0)/len(all_returns)*100:.0f}%)")
