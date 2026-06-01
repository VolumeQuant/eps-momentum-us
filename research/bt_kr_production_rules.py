# -*- coding: utf-8 -*-
"""KR production 룰 정확 BT — daily ranking 기반
룰 (v80.20+v80.21):
- 진입: weighted_rank ≤ 3
- 이탈: weighted_rank > 4 (= rank>4)
- 슬롯: 3 균등
- SL: -10% per position (v80.21 TS 제거)
- daily 갱신
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

ENTRY_RANK = 3.0
EXIT_RANK = 4.0
MAX_SLOTS = 3
SL = -0.10
INIT_PER_TRADE = 1_000_000  # 종목별 진입 100만원


def load_rankings():
    """일별 ranking 데이터 로드 (date → {ticker: weighted_rank})"""
    files = sorted(glob.glob(str(STATE / 'ranking_*.json')))
    daily = {}
    for fp in files:
        d_str = os.path.basename(fp).replace('ranking_', '').replace('.json', '')
        try:
            d = pd.to_datetime(d_str, format='%Y%m%d')
        except: continue
        try:
            with open(fp, encoding='utf-8') as f:
                data = json.load(f)
            wr_map = {}
            for r in data.get('rankings', []):
                wr = r.get('weighted_rank', r.get('rank', 999))
                wr_map[r['ticker']] = wr
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


def main():
    print('='*100)
    print('KR Production 룰 정확 BT — daily ranking (rank≤3 entry, rank>4 exit, SL -10%, slot 3)')
    print('='*100)
    daily = load_rankings()
    dates = sorted(daily.keys())
    print(f'ranking dates: {len(dates)} ({dates[0].date()} ~ {dates[-1].date()})')

    # Production rule simulator
    # Per ticker: list of (buy_date, buy_price, sell_date, sell_price, ret%, pl_krw, reason)
    holdings = {}  # ticker -> (buy_date, buy_price)
    trades = []  # log
    portfolio_val = 0  # 진입 시점부터 누적

    for i, d in enumerate(dates):
        wr = daily[d]

        # 1. Exits (SL OR rank>4)
        for tk in list(holdings.keys()):
            buy_d, buy_p = holdings[tk]
            cur_p = get_price(tk, d)
            if cur_p is None: continue
            should_exit = False
            reason = ''
            # SL check
            if (cur_p / buy_p - 1) <= SL:
                should_exit = True; reason = 'SL'
            # Rank check
            r = wr.get(tk)
            if not should_exit and (r is None or r > EXIT_RANK):
                should_exit = True; reason = f'rank>{EXIT_RANK}'
            if should_exit:
                ret = (cur_p / buy_p - 1) * 100
                pl = INIT_PER_TRADE * (cur_p / buy_p - 1)
                trades.append({
                    'ticker': tk, 'buy_date': buy_d, 'sell_date': d,
                    'buy_price': buy_p, 'sell_price': cur_p,
                    'ret_pct': ret, 'pl_krw': pl, 'reason': reason,
                    'hold_days': (d - buy_d).days,
                })
                del holdings[tk]

        # 2. Entries — rank ≤ 3 (slot 3까지)
        if len(holdings) < MAX_SLOTS:
            candidates = [(tk, r) for tk, r in wr.items() if r <= ENTRY_RANK and tk not in holdings]
            candidates.sort(key=lambda x: x[1])
            for tk, _ in candidates:
                if len(holdings) >= MAX_SLOTS: break
                p = get_price(tk, d)
                if not p: continue
                holdings[tk] = (d, p)

    print(f'\n총 매매 cycle: {len(trades)}')

    # 종목별 집계
    by_ticker = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pl': 0, 'rets': [], 'sl_count': 0, 'rank_count': 0})
    for tr in trades:
        t = tr['ticker']
        by_ticker[t]['trades'] += 1
        if tr['ret_pct'] > 0: by_ticker[t]['wins'] += 1
        by_ticker[t]['pl'] += tr['pl_krw']
        by_ticker[t]['rets'].append(tr['ret_pct'])
        if 'SL' in tr['reason']: by_ticker[t]['sl_count'] += 1
        else: by_ticker[t]['rank_count'] += 1

    sorted_tickers = sorted(by_ticker.items(), key=lambda x: -x[1]['pl'])

    print(f'\n📊 Production 룰 BT — 누적 수익 Top 20:')
    print(f'  {"순위":<5}{"종목":<8}{"거래":>5}{"승률":>9}{"평균%":>10}{"누적(만)":>15}{"SL":>5}{"rank>4":>8}')
    print('  ' + '-'*72)
    for i, (tk, d) in enumerate(sorted_tickers[:20]):
        wr_str = f'{d["wins"]}/{d["trades"]}'
        print(f'  {i+1:<5}{tk:<8}{d["trades"]:>5}{wr_str:>9}{np.mean(d["rets"]):>+9.1f}%{d["pl"]/10000:>+13,.0f}{d["sl_count"]:>5}{d["rank_count"]:>8}')

    print(f'\n📊 누적 손실 Bottom 15:')
    print(f'  {"순위":<5}{"종목":<8}{"거래":>5}{"승률":>9}{"평균%":>10}{"누적(만)":>15}{"SL":>5}{"rank>4":>8}')
    print('  ' + '-'*72)
    for i, (tk, d) in enumerate(sorted_tickers[-15:]):
        wr_str = f'{d["wins"]}/{d["trades"]}'
        print(f'  {i+1:<5}{tk:<8}{d["trades"]:>5}{wr_str:>9}{np.mean(d["rets"]):>+9.1f}%{d["pl"]/10000:>+13,.0f}{d["sl_count"]:>5}{d["rank_count"]:>8}')

    # 종합 통계
    total_trades = sum(d['trades'] for d in by_ticker.values())
    total_wins = sum(d['wins'] for d in by_ticker.values())
    total_pl = sum(d['pl'] for d in by_ticker.values())
    sl_trades = sum(d['sl_count'] for d in by_ticker.values())
    rank_trades = sum(d['rank_count'] for d in by_ticker.values())
    unique = len(by_ticker)

    print(f'\n💰 종합 (production 룰 정확 적용):')
    print(f'  총 거래: {total_trades}건')
    print(f'  승률: {total_wins}/{total_trades} = {total_wins/total_trades*100:.1f}%')
    print(f'  평균 거래 수익률: {np.mean([np.mean(d["rets"]) for d in by_ticker.values()]):+.2f}%')
    print(f'  종목 다양성: {unique}개')
    print(f'  매도 사유: SL {sl_trades}건 ({sl_trades/total_trades*100:.0f}%) / rank>4 {rank_trades}건 ({rank_trades/total_trades*100:.0f}%)')
    print(f'  총 누적 손익 (cycle당 100만): {total_pl/10000:+,.0f}만원')

    top5_pl = sum(d['pl'] for tk, d in sorted_tickers[:5])
    top10_pl = sum(d['pl'] for tk, d in sorted_tickers[:10])
    top20_pl = sum(d['pl'] for tk, d in sorted_tickers[:20])
    print(f'\n  Top 5 종목 누적 손익 비중: {top5_pl/total_pl*100:.1f}%')
    print(f'  Top 10 종목 누적 손익 비중: {top10_pl/total_pl*100:.1f}%')
    print(f'  Top 20 종목 누적 손익 비중: {top20_pl/total_pl*100:.1f}%')

    # 보유 기간 분포
    hold_days = [tr['hold_days'] for tr in trades]
    print(f'\n  평균 보유 기간: {np.mean(hold_days):.1f}일 (median {np.median(hold_days):.0f}일)')
    print(f'  매우 짧음 (≤7일): {sum(1 for h in hold_days if h<=7)}건 ({sum(1 for h in hold_days if h<=7)/len(hold_days)*100:.0f}%)')
    print(f'  보통 (7~30일): {sum(1 for h in hold_days if 7<h<=30)}건')
    print(f'  김 (>30일): {sum(1 for h in hold_days if h>30)}건')

    # Production rule vs monthly rebal 비교
    print(f'\n📊 Production daily 룰 vs 단순 monthly 비교:')
    print(f'  Production (daily, rank>4 또는 SL): 총 거래 {total_trades}건')
    print(f'  Monthly rebal (앞서 BT): 282건')
    print(f'  → Production이 {total_trades/282*100:.0f}% 더 많은 회전 = winner 일찍 청산 위험')


if __name__ == '__main__':
    main()
