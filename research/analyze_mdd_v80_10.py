"""v80.10 MDD 악화 원인 분석 — 어떤 종목이 drawdown 주도?"""
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent


def trace_portfolio(db_path, label):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()

    portfolio = {}
    cum = 1.0
    peak = 1.0
    daily = []
    consecutive = defaultdict(int)

    for di, today in enumerate(dates):
        if today not in data: continue
        td = data[today]
        rank_map = {tk: v['p2'] for tk, v in td.items() if v.get('p2') is not None}
        new_cons = defaultdict(int)
        for tk in rank_map:
            new_cons[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_cons

        day_ret = 0
        per_ticker = {}
        if portfolio:
            for tk in portfolio:
                price = td.get(tk, {}).get('price')
                if price and di > 0:
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        r = (price - prev) / prev * 100
                        day_ret += r
                        per_ticker[tk] = r
            day_ret /= len(portfolio)

        cum *= (1 + day_ret / 100)
        peak = max(peak, cum)
        dd = (cum - peak) / peak * 100
        daily.append({
            'date': today, 'cum': cum, 'peak': peak, 'dd': dd,
            'portfolio': dict(portfolio), 'per_ticker': per_ticker, 'day_ret': day_ret,
        })

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            ms = td.get(tk, {}).get('min_seg', 0)
            price = td.get(tk, {}).get('price')
            should = (rank is None or rank > 8 or ms < -2)
            if should and price:
                exited.append(tk)
        for tk in exited: del portfolio[tk]

        # 진입
        vac = 3 - len(portfolio)
        if vac > 0:
            cands = []
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > 3: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                ms = td.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = td.get(tk, {}).get('price')
                if price and price > 0: cands.append((tk, price))
            for tk, price in cands[:vac]:
                portfolio[tk] = {'entry_price': price, 'entry_date': today}

    return daily


for db, label in [
    (ROOT / 'eps_momentum_data.bak_pre_v80_10.db', 'v80.9'),
    (ROOT / 'eps_momentum_data.db', 'v80.10'),
]:
    daily = trace_portfolio(db, label)

    # MDD peak/trough 찾기
    peak_idx = 0
    trough_idx = 0
    max_dd = 0
    cur_peak = 1.0
    cur_peak_idx = 0
    for i, d in enumerate(daily):
        if d['cum'] > cur_peak:
            cur_peak = d['cum']
            cur_peak_idx = i
        dd = (d['cum'] - cur_peak) / cur_peak * 100
        if dd < max_dd:
            max_dd = dd
            trough_idx = i
            peak_idx = cur_peak_idx

    print(f'\n{"="*80}')
    print(f'{label}: MDD {max_dd:+.2f}%')
    print(f'{"="*80}')
    print(f'  Peak: {daily[peak_idx]["date"]} cum={daily[peak_idx]["cum"]:.4f}')
    print(f'  Trough: {daily[trough_idx]["date"]} cum={daily[trough_idx]["cum"]:.4f}')
    print(f'  기간: {trough_idx - peak_idx}일')

    print(f'\n  Trough 시점 portfolio:')
    for tk, info in daily[trough_idx]['portfolio'].items():
        td = daily[trough_idx]
        # 종목별 누적 손실
        ep = info['entry_price']
        # 현재 가격
        price = None
        for d2 in daily[trough_idx:trough_idx+1]:
            pass
        print(f'    {tk}: 진입 {info["entry_date"]} @ {ep:.2f}')

    # peak~trough 사이 누적 종목별 기여
    print(f'\n  Peak({daily[peak_idx]["date"]})~Trough({daily[trough_idx]["date"]}) 종목별 누적 기여:')
    ticker_contrib = defaultdict(float)
    for i in range(peak_idx + 1, trough_idx + 1):
        for tk, r in daily[i]['per_ticker'].items():
            ticker_contrib[tk] += r / len(daily[i]['portfolio'])
    for tk, c in sorted(ticker_contrib.items(), key=lambda x: x[1]):
        print(f'    {tk}: {c:+6.2f}%p')

    # 5/8 시점 portfolio + 미실현 손익
    last = daily[-1]
    print(f'\n  5/8 portfolio 미실현 손익:')
    for tk, info in last['portfolio'].items():
        ep = info['entry_price']
        ed = info['entry_date']
        # 5/8 price
        bts2.DB_PATH = str(db)
        dates_x, data_x = bts2.load_data()
        price = data_x.get('2026-05-08', {}).get(tk, {}).get('price')
        if price:
            ret = (price - ep) / ep * 100
            print(f'    {tk}: 진입 {ed} @ {ep:.2f} → 5/8 @ {price:.2f} ({ret:+.2f}%)')
