# -*- coding: utf-8 -*-
"""Production 수익 종목 분석
A. US: a.csv 시스템 픽 (p2 ≤ 3) 실거래 손익
B. KR: 8년 ranking 시뮬레이션 — 종목별 누적 P&L
"""
import sys, csv, io, json, glob, os
from pathlib import Path
import sqlite3
import pandas as pd
import numpy as np
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

# ===========================================================
# A. US a.csv 시스템 픽 분석
# ===========================================================
def us_analysis():
    print('='*100)
    print('A. US production 실거래 수익 종목 (a.csv, 시스템 픽 p2≤3 한정)')
    print('='*100)
    csv_path = r'C:\Users\user\Downloads\a.csv'
    raw = open(csv_path, 'rb').read().decode('cp949')
    rows = list(csv.reader(io.StringIO(raw)))[1:]  # skip header

    # parse — 매도 leg 추출
    sells = []
    for r in rows:
        if not r or not r[0].strip(): continue
        date, ccy, ticker, name, balq = r[0], r[1], r[2], r[3], r[4]
        sellq = float(r[11] or 0)
        if sellq == 0: continue
        sells.append({
            'date': date, 'ticker': ticker,
            'ret_pct': float(r[23] or 0),
            'pl_krw': float(r[22] or 0),
            'avg_buy': float(r[18] or 0),
            'sell_px': float(r[12] or 0),
            'qty': sellq,
        })

    # 시스템 픽 분류 (DB lookup)
    conn = sqlite3.connect(r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db')
    cur = conn.cursor()
    def best_rank(t, sell_date, lookback=30):
        d = pd.to_datetime(sell_date, format='%Y/%m/%d')
        start = (d - pd.Timedelta(days=lookback)).date().isoformat()
        end = d.date().isoformat()
        q = cur.execute(
            "SELECT MIN(part2_rank) FROM ntm_screening "
            "WHERE ticker=? AND date BETWEEN ? AND ? AND part2_rank IS NOT NULL",
            (t, start, end)).fetchone()
        return q[0] if q and q[0] else None

    system_trades = []  # p2 ≤ 3
    radar_trades = []  # p2 4-8
    other_trades = []
    for s in sells:
        p2 = best_rank(s['ticker'], s['date'])
        s['best_p2'] = p2
        if p2 is not None and p2 <= 3:
            system_trades.append(s)
        elif p2 is not None and p2 <= 8:
            radar_trades.append(s)
        else:
            other_trades.append(s)
    conn.close()

    print(f'\n총 청산 {len(sells)}건')
    print(f'  시스템 픽 (p2≤3): {len(system_trades)}건')
    print(f'  Radar (p2 4-8):  {len(radar_trades)}건')
    print(f'  Other (재량):   {len(other_trades)}건')

    print('\n📊 시스템 픽 거래 명세 (시간순):')
    print(f'  {"date":<12}{"ticker":<7}{"매수가":>10}{"매도가":>10}{"ret%":>8}{"손익(원)":>15}')
    print('  ' + '-'*70)
    sys_total = 0
    sys_wins = 0
    for s in sorted(system_trades, key=lambda x: x['date']):
        flag = '✅' if s['ret_pct'] > 0 else '❌'
        print(f'  {s["date"]:<12}{s["ticker"]:<7}{s["avg_buy"]:>10.2f}{s["sell_px"]:>10.2f}'
              f'{s["ret_pct"]:>+7.2f}%{s["pl_krw"]:>+14,.0f} {flag}')
        sys_total += s['pl_krw']
        if s['ret_pct'] > 0: sys_wins += 1

    print(f'  {"━"*70}')
    print(f'  합계: {sys_total:+,.0f}원 (승률 {sys_wins}/{len(system_trades)} = {sys_wins/len(system_trades)*100:.0f}%)')

    # 종목별 집계
    by_ticker = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pl': 0, 'rets': []})
    for s in system_trades:
        by_ticker[s['ticker']]['trades'] += 1
        if s['ret_pct'] > 0: by_ticker[s['ticker']]['wins'] += 1
        by_ticker[s['ticker']]['pl'] += s['pl_krw']
        by_ticker[s['ticker']]['rets'].append(s['ret_pct'])

    print('\n📊 시스템 픽 종목별 누적 (수익 큰 순):')
    print(f'  {"종목":<8}{"거래":>5}{"승률":>10}{"평균%":>9}{"누적손익(원)":>15}')
    print('  ' + '-'*55)
    for t, d in sorted(by_ticker.items(), key=lambda x: -x[1]['pl']):
        avg = np.mean(d['rets'])
        print(f'  {t:<8}{d["trades"]:>5}{d["wins"]}/{d["trades"]:>5}{avg:>+8.1f}%{d["pl"]:>+14,.0f}')


# ===========================================================
# B. KR system signal P&L (8년 시뮬)
# ===========================================================
def kr_analysis():
    print('\n' + '='*100)
    print('B. KR production 수익 종목 (8년 ranking 시뮬, monthly rebal, top3 균등)')
    print('='*100)

    STATE = Path(r'C:\dev\state')
    OHLCV = pd.read_parquet(r'C:\dev\data_cache\all_ohlcv_20170601_20260529.parquet')
    OHLCV.index = pd.to_datetime(OHLCV.index)

    files = sorted(glob.glob(str(STATE / 'ranking_*.json')))
    rebal = []
    last_month = -1
    for fp in files:
        d_str = os.path.basename(fp).replace('ranking_', '').replace('.json', '')
        try:
            d = pd.to_datetime(d_str, format='%Y%m%d')
        except: continue
        if d.month != last_month:
            rebal.append((d, fp))
            last_month = d.month

    # 매월 매수→매도 1cycle 시뮬, 각 매수에 KRW 100,000 (3종목 균등 33,333씩)
    INIT_PER_TRADE = 1_000_000  # 매 cycle 각 종목 100만원 매수 가정 (P&L 추적 용)
    trades_log = defaultdict(list)  # ticker -> list of (buy_date, sell_date, ret%, pl_krw)

    prev_picks = []  # (ticker, buy_price, buy_date)
    for d, fp in rebal:
        with open(fp, encoding='utf-8') as f:
            data = json.load(f)
        picks = [r['ticker'] for r in data.get('rankings', [])[:3]]

        # 매도 (이전 보유)
        for tk, buy_p, buy_d in prev_picks:
            if tk in OHLCV.columns:
                try:
                    sell_p = OHLCV[tk].asof(d)
                    if sell_p and not pd.isna(sell_p) and buy_p > 0:
                        ret = (sell_p/buy_p - 1) * 100
                        pl = INIT_PER_TRADE * (sell_p/buy_p - 1)
                        trades_log[tk].append((buy_d, d, ret, pl))
                except: pass

        # 새 매수
        new_picks = []
        for tk in picks:
            if tk in OHLCV.columns:
                try:
                    p = OHLCV[tk].asof(d)
                    if p and not pd.isna(p) and p > 0:
                        new_picks.append((tk, p, d))
                except: pass
        prev_picks = new_picks

    # 종목별 집계
    print(f'\n📊 KR 시뮬 매매 — 매 cycle 종목당 100만원 매수 가정 (총 {len(rebal)} cycle, 8년)')
    by_ticker = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pl': 0, 'rets': [], 'first_date': None, 'last_date': None})
    for tk, trades in trades_log.items():
        for buy_d, sell_d, ret, pl in trades:
            by_ticker[tk]['trades'] += 1
            if ret > 0: by_ticker[tk]['wins'] += 1
            by_ticker[tk]['pl'] += pl
            by_ticker[tk]['rets'].append(ret)
            if by_ticker[tk]['first_date'] is None or buy_d < by_ticker[tk]['first_date']:
                by_ticker[tk]['first_date'] = buy_d
            if by_ticker[tk]['last_date'] is None or sell_d > by_ticker[tk]['last_date']:
                by_ticker[tk]['last_date'] = sell_d

    sorted_tickers = sorted(by_ticker.items(), key=lambda x: -x[1]['pl'])
    print('\n📊 누적 수익 큰 종목 Top 20 (8년 ranking 시뮬):')
    print(f'  {"순위":<5}{"종목":<8}{"거래":>5}{"승률":>9}{"평균%":>10}{"누적손익(만원)":>15}{"첫매수":<12}{"마지막매도":<12}')
    print('  ' + '-'*95)
    for i, (tk, d) in enumerate(sorted_tickers[:20]):
        avg = np.mean(d['rets'])
        wr = f'{d["wins"]}/{d["trades"]}'
        pl_man = d['pl'] / 10000  # 만원
        fd = d['first_date'].strftime('%Y-%m') if d['first_date'] else 'N/A'
        ld = d['last_date'].strftime('%Y-%m') if d['last_date'] else 'N/A'
        print(f'  {i+1:<5}{tk:<8}{d["trades"]:>5}{wr:>9}{avg:>+9.1f}%{pl_man:>+13,.0f}{fd:<14}{ld:<12}')

    # 손실 큰 종목
    print('\n📊 누적 손실 큰 종목 Bottom 15 (8년 ranking 시뮬):')
    print(f'  {"순위":<5}{"종목":<8}{"거래":>5}{"승률":>9}{"평균%":>10}{"누적손익(만원)":>15}')
    print('  ' + '-'*55)
    for i, (tk, d) in enumerate(sorted_tickers[-15:]):
        avg = np.mean(d['rets'])
        wr = f'{d["wins"]}/{d["trades"]}'
        pl_man = d['pl'] / 10000
        print(f'  {i+1:<5}{tk:<8}{d["trades"]:>5}{wr:>9}{avg:>+9.1f}%{pl_man:>+13,.0f}')

    # 종합
    total_pl = sum(d['pl'] for d in by_ticker.values())
    total_trades = sum(d['trades'] for d in by_ticker.values())
    total_wins = sum(d['wins'] for d in by_ticker.values())
    unique_tickers = len(by_ticker)
    print(f'\n💰 종합:')
    print(f'  총 거래 cycle: {total_trades}건 (월 회전)')
    print(f'  승률: {total_wins}/{total_trades} = {total_wins/total_trades*100:.1f}%')
    print(f'  평균 거래 수익률: {np.mean([np.mean(d["rets"]) for d in by_ticker.values()]):+.2f}%')
    print(f'  종목 다양성: {unique_tickers}개')
    print(f'  총 누적 손익: {total_pl/10000:+,.0f}만원 (매 cycle 종목당 100만원 매수 가정)')

    # Top 5 contribution 비율
    top5_pl = sum(d['pl'] for tk, d in sorted_tickers[:5])
    top10_pl = sum(d['pl'] for tk, d in sorted_tickers[:10])
    print(f'\n  Top 5 종목 누적 손익 비중: {top5_pl/total_pl*100:.1f}%')
    print(f'  Top 10 종목 누적 손익 비중: {top10_pl/total_pl*100:.1f}%')


if __name__ == '__main__':
    us_analysis()
    kr_analysis()
