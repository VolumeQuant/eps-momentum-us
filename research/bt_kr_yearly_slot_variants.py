# -*- coding: utf-8 -*-
"""KR production 룰 연도별 + slot 변형 BT
- 연도별: 2019, 2020 COVID, 2021, 2022 약세, 2023, 2024, 2025-26
- Slot 3 (현재) vs Slot 5 균등 vs Slot 1 (집중) 비교
- 룰: rank ≤ entry, rank > exit, SL -10%, daily
"""
import sys, json, glob, os
from pathlib import Path
import pandas as pd
import numpy as np
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

STATE = Path(r'C:\dev\state')
OHLCV = pd.read_parquet(r'C:\dev\data_cache\all_ohlcv_20170601_20260529.parquet')
OHLCV.index = pd.to_datetime(OHLCV.index)


def load_rankings():
    files = sorted(glob.glob(str(STATE / 'ranking_*.json')))
    daily = {}
    for fp in files:
        d_str = os.path.basename(fp).replace('ranking_', '').replace('.json', '')
        try: d = pd.to_datetime(d_str, format='%Y%m%d')
        except: continue
        try:
            with open(fp, encoding='utf-8') as f: data = json.load(f)
            wr_map = {r['ticker']: r.get('weighted_rank', r.get('rank', 999)) for r in data.get('rankings', [])}
            daily[d] = wr_map
        except: pass
    return daily


def get_price(ticker, date):
    if ticker not in OHLCV.columns: return None
    try:
        p = OHLCV[ticker].asof(date)
        if p and not pd.isna(p) and p > 0: return float(p)
    except: pass
    return None


def simulate(daily, dates, entry=3, exit_=4, slots=3, sl=-0.10, init_per=1_000_000):
    holdings = {}
    trades = []
    for d in dates:
        if d not in daily: continue
        wr = daily[d]
        # Exits
        for tk in list(holdings.keys()):
            buy_d, buy_p = holdings[tk]
            cur_p = get_price(tk, d)
            if cur_p is None: continue
            should_exit = False; reason = ''
            if (cur_p / buy_p - 1) <= sl:
                should_exit = True; reason = 'SL'
            else:
                r = wr.get(tk)
                if r is None or r > exit_:
                    should_exit = True; reason = f'rank>{exit_}'
            if should_exit:
                ret = (cur_p/buy_p - 1) * 100
                pl = init_per * (cur_p/buy_p - 1)
                trades.append({'ticker': tk, 'buy_date': buy_d, 'sell_date': d,
                              'ret_pct': ret, 'pl_krw': pl, 'reason': reason})
                del holdings[tk]
        # Entries
        if len(holdings) < slots:
            cands = [(tk, r) for tk, r in wr.items() if r <= entry and tk not in holdings]
            cands.sort(key=lambda x: x[1])
            for tk, _ in cands:
                if len(holdings) >= slots: break
                p = get_price(tk, d)
                if p: holdings[tk] = (d, p)
    return trades


def trade_stats(trades):
    if not trades: return None
    total = len(trades)
    wins = sum(1 for tr in trades if tr['ret_pct'] > 0)
    pl = sum(tr['pl_krw'] for tr in trades)
    avg = np.mean([tr['ret_pct'] for tr in trades])
    sl_count = sum(1 for tr in trades if 'SL' in tr['reason'])
    return {'trades': total, 'wins': wins, 'pl': pl, 'avg': avg, 'sl': sl_count}


def equity_curve(trades, dates, init=100):
    """trade-level approximate 누적 — 매 trade 1단위 가정"""
    if not trades: return [init]
    # 단순: sequential P&L 누적
    val = init
    cum = [init]
    for tr in sorted(trades, key=lambda x: x['sell_date']):
        # 단위 자본 1 가정, 누적
        val *= (1 + tr['ret_pct']/100/len(set([t['ticker'] for t in trades])))
        cum.append(val)
    return cum


def main():
    print('='*100)
    print('KR Production 룰 — 연도별 + Slot 변형 BT')
    print('='*100)
    daily = load_rankings()
    all_dates = sorted(daily.keys())

    # 1. Slot 변형 비교 (전체 8년)
    print('\n--- 1. Slot 변형 비교 (전체 8년, entry=3, exit=4, SL-10%) ---')
    print(f'{"Slot":<10}{"거래":>7}{"승률":>10}{"평균%":>9}{"누적(만)":>13}{"SL%":>7}{"종목수":>8}')
    print('-' * 65)
    for slot_n in [1, 2, 3, 4, 5]:
        trades = simulate(daily, all_dates, entry=3, exit_=4, slots=slot_n)
        if not trades: continue
        s = trade_stats(trades)
        unique = len(set(tr['ticker'] for tr in trades))
        wr_str = f'{s["wins"]}/{s["trades"]}'
        print(f'  slot {slot_n:<5}{s["trades"]:>7}{wr_str:>10}{s["avg"]:>+8.1f}%{s["pl"]/10000:>+12,.0f}{s["sl"]/s["trades"]*100:>6.0f}%{unique:>8}')

    # 2. Entry rank 변형 (현재 entry=3)
    print('\n--- 2. Entry rank 변형 (slot 3, exit=4) ---')
    print(f'{"Entry":<10}{"거래":>7}{"승률":>10}{"평균%":>9}{"누적(만)":>13}')
    print('-' * 50)
    for entry_n in [2, 3, 4, 5]:
        trades = simulate(daily, all_dates, entry=entry_n, exit_=4, slots=3)
        if not trades: continue
        s = trade_stats(trades)
        wr_str = f'{s["wins"]}/{s["trades"]}'
        print(f'  entry≤{entry_n}{"":<3}{s["trades"]:>7}{wr_str:>10}{s["avg"]:>+8.1f}%{s["pl"]/10000:>+12,.0f}')

    # 3. Exit rank 변형
    print('\n--- 3. Exit rank 변형 (entry=3, slot 3) ---')
    print(f'{"Exit":<10}{"거래":>7}{"승률":>10}{"평균%":>9}{"누적(만)":>13}')
    print('-' * 50)
    for exit_n in [3, 4, 5, 7, 10]:
        trades = simulate(daily, all_dates, entry=3, exit_=exit_n, slots=3)
        if not trades: continue
        s = trade_stats(trades)
        wr_str = f'{s["wins"]}/{s["trades"]}'
        print(f'  exit>{exit_n}{"":<4}{s["trades"]:>7}{wr_str:>10}{s["avg"]:>+8.1f}%{s["pl"]/10000:>+12,.0f}')

    # 4. SL 변형
    print('\n--- 4. SL 변형 (entry=3, exit=4, slot 3) ---')
    print(f'{"SL":<10}{"거래":>7}{"승률":>10}{"평균%":>9}{"누적(만)":>13}{"SL%":>7}')
    print('-' * 60)
    for sl_pct in [-0.05, -0.07, -0.10, -0.15, -0.20, -1.0]:  # -1.0 = effectively no SL
        trades = simulate(daily, all_dates, entry=3, exit_=4, slots=3, sl=sl_pct)
        if not trades: continue
        s = trade_stats(trades)
        wr_str = f'{s["wins"]}/{s["trades"]}'
        sl_str = 'No SL' if sl_pct <= -0.99 else f'{sl_pct*100:.0f}%'
        print(f'  {sl_str:<10}{s["trades"]:>7}{wr_str:>10}{s["avg"]:>+8.1f}%{s["pl"]/10000:>+12,.0f}{s["sl"]/s["trades"]*100:>6.0f}%')

    # 5. 연도별 production 룰 (current = entry 3, exit 4, slot 3, SL -10%)
    print('\n--- 5. 연도별 Production BT (current 룰) ---')
    print(f'{"기간":<18}{"거래":>7}{"승률":>10}{"평균%":>9}{"누적(만)":>13}{"SL%":>7}')
    print('-' * 65)
    periods = [
        ('2019 강세', '2019-01-01', '2019-12-31'),
        ('2020 COVID', '2020-01-01', '2020-12-31'),
        ('2021 횡보', '2021-01-01', '2021-12-31'),
        ('2022 약세 ⭐', '2022-01-01', '2022-12-31'),
        ('2023 회복', '2023-01-01', '2023-12-31'),
        ('2024 AI', '2024-01-01', '2024-12-31'),
        ('2025-26 YTD', '2025-01-01', '2026-05-29'),
    ]
    for label, s, e in periods:
        s_ts = pd.Timestamp(s); e_ts = pd.Timestamp(e)
        period_dates = [d for d in all_dates if s_ts <= d <= e_ts]
        if len(period_dates) < 30: continue
        trades = simulate(daily, period_dates, entry=3, exit_=4, slots=3)
        if not trades:
            print(f'  {label:<18}{"(거래 없음)":<30}'); continue
        st = trade_stats(trades)
        wr_str = f'{st["wins"]}/{st["trades"]}'
        print(f'  {label:<18}{st["trades"]:>7}{wr_str:>10}{st["avg"]:>+8.1f}%{st["pl"]/10000:>+12,.0f}{st["sl"]/st["trades"]*100:>6.0f}%')

    # 6. 최적 조합 찾기 — Top candidates
    print('\n--- 6. 최적 (entry, exit, slot, SL) 탐색 (전체 8년 평균 거래수익률 기준) ---')
    best = []
    for e in [2, 3, 4]:
        for x in [3, 4, 5, 7]:
            for s in [2, 3, 5]:
                if x <= e: continue
                trades = simulate(daily, all_dates, entry=e, exit_=x, slots=s, sl=-0.10)
                if not trades: continue
                st = trade_stats(trades)
                best.append((e, x, s, st['avg'], st['pl']/10000, st['wins']/st['trades']*100, st['trades']))
    best.sort(key=lambda x: -x[4])  # 누적 손익 기준
    print(f'  {"E":<3}{"X":<3}{"S":<3}{"평균%":>9}{"누적(만)":>13}{"승률%":>9}{"거래":>7}')
    print('  ' + '-' * 45)
    for e, x, s, avg, pl, wr, t in best[:15]:
        marker = ' ⭐ current' if (e, x, s) == (3, 4, 3) else ''
        print(f'  {e:<3}{x:<3}{s:<3}{avg:>+8.1f}%{pl:>+12,.0f}{wr:>+8.1f}%{t:>7}{marker}')


if __name__ == '__main__':
    main()
