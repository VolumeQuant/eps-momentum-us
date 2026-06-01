# -*- coding: utf-8 -*-
"""EPS Momentum Proxy 8년 BT
EPS revision 데이터 없으니 학술 검증된 momentum factor를 proxy로 사용:
- 12-1 momentum (Asness): 12개월 전 → 1개월 전 가격 변화
- Top-2 picks 매월 리밸런싱
- 우리 시스템과 가장 비슷한 알파 채널
"""
import sys, time
from pathlib import Path
import pandas as pd
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
SP100_PATH = ROOT / 'research' / 'sp100_prices_8y.parquet'
ETF_PATH = ROOT / 'research' / 'sector_etf_8y.parquet'


def load_data():
    sp = pd.read_parquet(SP100_PATH)
    sp.index = pd.to_datetime(sp.index)
    sp = sp.sort_index()
    etf = pd.read_parquet(ETF_PATH)
    etf.index = pd.to_datetime(etf.index)
    return sp, etf


def momentum_score(prices_df, date, lookback_long=252, lookback_short=21):
    """12-1 momentum: 12개월 전(252일) ÷ 1개월 전(21일)"""
    if date not in prices_df.index:
        d = prices_df.index[prices_df.index <= date][-1] if len(prices_df.index[prices_df.index <= date]) else None
        if d is None: return {}
        date = d
    idx = prices_df.index.get_loc(date)
    if idx < lookback_long: return {}

    long_idx = idx - lookback_long
    short_idx = idx - lookback_short
    scores = {}
    for col in prices_df.columns:
        s = prices_df[col]
        p_long = s.iloc[long_idx]
        p_short = s.iloc[short_idx]
        if pd.isna(p_long) or pd.isna(p_short) or p_long <= 0:
            continue
        scores[col] = (p_short / p_long - 1) * 100
    return scores


def quality_filter(prices_df, date, top_n=50):
    """단순 quality: 1년 변동성 낮은 top_n"""
    if date not in prices_df.index:
        d = prices_df.index[prices_df.index <= date][-1] if len(prices_df.index[prices_df.index <= date]) else None
        if d is None: return set()
        date = d
    idx = prices_df.index.get_loc(date)
    if idx < 252: return set()
    window = prices_df.iloc[idx-252:idx]
    vols = window.pct_change().std() * np.sqrt(252)
    return set(vols.nsmallest(top_n).index)


def backtest_momentum(prices_df, n_picks=2, weights='50_50',
                      start='2019-06-01', end='2026-05-29',
                      use_quality_filter=False):
    """매월 첫 영업일 = momentum ranking → top N 매수 → 다음 달 리밸런싱"""
    prices_df = prices_df.loc[start:end]
    dates = prices_df.index.tolist()
    rebal_dates = []
    last_month = -1
    for d in dates:
        if d.month != last_month:
            rebal_dates.append(d)
            last_month = d.month

    INIT = 100.0
    cash = INIT
    holdings = {}
    daily_values = []
    prev_pv = INIT
    rebal_idx = 0
    trades = []

    for d in dates:
        pv = cash
        for tk, sh in holdings.items():
            if tk in prices_df.columns:
                p = prices_df[tk].loc[d]
                if not pd.isna(p): pv += sh * p
        daily_values.append((d, pv))
        prev_pv = pv

        if rebal_idx < len(rebal_dates) and d == rebal_dates[rebal_idx]:
            scores = momentum_score(prices_df, d)
            if use_quality_filter:
                qual = quality_filter(prices_df, d)
                scores = {k: v for k, v in scores.items() if k in qual}
            if not scores:
                rebal_idx += 1; continue
            ranked = sorted(scores.items(), key=lambda x: -x[1])
            picks = [tk for tk, _ in ranked[:n_picks]]

            # 청산
            for tk, sh in list(holdings.items()):
                p = prices_df[tk].loc[d] if tk in prices_df.columns else None
                if p is not None and not pd.isna(p):
                    cash += sh * p
                holdings.pop(tk)

            # 매수
            if weights == '50_50' and len(picks) >= 2:
                w = [0.5, 0.5]
            elif weights == '90_10' and len(picks) >= 2:
                w = [0.9, 0.1]
            elif weights == '100_0' or n_picks == 1:
                w = [1.0]
                picks = picks[:1]
            else:
                w = [1.0/n_picks]*n_picks
            for i, tk in enumerate(picks):
                if i >= len(w): break
                if tk not in prices_df.columns: continue
                p = prices_df[tk].loc[d]
                if pd.isna(p) or p <= 0: continue
                allocated = cash * w[i]
                holdings[tk] = allocated / p
            cash = cash - sum(w[:len(picks)]) * cash
            trades.append((d, picks))
            rebal_idx += 1

    final = daily_values[-1][1]
    total_ret = (final/INIT - 1) * 100
    n = len(daily_values)
    cagr = (final/INIT)**(252/n) - 1
    pvs = [v for _, v in daily_values]
    rets = pd.Series(pvs).pct_change().fillna(0)
    sigma = rets.std() * np.sqrt(252)
    sharpe = (rets.mean() * 252) / sigma if sigma > 0 else 0
    peak = pd.Series(pvs).cummax()
    dd = (pd.Series(pvs) - peak)/peak
    mdd = dd.min() * 100
    cal = (cagr*100)/abs(mdd) if mdd<0 else 0
    return {
        'total_return': total_ret, 'cagr': cagr*100, 'mdd': mdd,
        'sharpe': sharpe, 'calmar': cal, 'n_trades': len(trades),
        'final': final, 'trades_log': trades,
    }


def backtest_buyhold(df, ticker, start='2019-06-01', end='2026-05-29'):
    df = df.loc[start:end]
    if ticker not in df.columns: return None
    s = df[ticker].dropna()
    if s.empty: return None
    INIT = 100.0
    final = INIT * (s.iloc[-1]/s.iloc[0])
    total = (final/INIT - 1) * 100
    n = len(s)
    cagr = (final/INIT)**(252/n) - 1
    rets = s.pct_change().fillna(0)
    sigma = rets.std() * np.sqrt(252)
    sharpe = (rets.mean() * 252) / sigma if sigma > 0 else 0
    peak = s.cummax(); dd = (s-peak)/peak; mdd = dd.min()*100
    cal = (cagr*100)/abs(mdd) if mdd<0 else 0
    return {'total_return': total, 'cagr': cagr*100, 'mdd': mdd,
            'sharpe': sharpe, 'calmar': cal, 'final': final}


def main():
    print('='*110)
    print('EPS Momentum Proxy 8년 BT (S&P 100 종목, 12-1 momentum)')
    print('='*110)
    sp, etf = load_data()
    print(f'sp100: {sp.shape}, range {sp.index[0].date()}~{sp.index[-1].date()}')

    start_bt = '2019-06-01'
    end_bt = '2026-05-29'
    print(f'BT period: {start_bt} ~ {end_bt} (약 7년)\n')

    # 1. Baselines
    print('--- 1. Baseline (buy-hold) ---')
    print(f'{"Strategy":<18}{"return":>10}{"CAGR":>9}{"MDD":>9}{"Sharpe":>9}{"Calmar":>9}{"final":>10}')
    print('-' * 74)
    for tk in ['SPY', 'XLK', 'QQQ']:
        r = backtest_buyhold(etf, tk, start_bt, end_bt)
        if r:
            print(f'  {tk+" buy-hold":<16}{r["total_return"]:>+9.1f}%{r["cagr"]:>+8.2f}%{r["mdd"]:>+8.2f}%{r["sharpe"]:>8.2f}{r["calmar"]:>8.2f}{r["final"]:>10.1f}')

    # 2. Momentum proxy BT — picks 종류 sweep
    print('\n--- 2. Momentum Proxy BT (n_picks × weights) ---')
    print(f'{"Strategy":<24}{"return":>10}{"CAGR":>9}{"MDD":>9}{"Sharpe":>9}{"Calmar":>9}{"trades":>8}')
    print('-' * 80)
    variants = [
        ('top1_100',         {'n_picks': 1, 'weights': '100_0'}),
        ('top2_50_50',       {'n_picks': 2, 'weights': '50_50'}),
        ('top2_90_10',       {'n_picks': 2, 'weights': '90_10'}),
        ('top3_equal',       {'n_picks': 3, 'weights': 'equal'}),
        ('top5_equal',       {'n_picks': 5, 'weights': 'equal'}),
        ('top2_50_50_quality', {'n_picks': 2, 'weights': '50_50', 'use_quality_filter': True}),
    ]
    for name, kw in variants:
        r = backtest_momentum(sp, start=start_bt, end=end_bt, **kw)
        print(f'  {name:<24}{r["total_return"]:>+9.1f}%{r["cagr"]:>+8.2f}%{r["mdd"]:>+8.2f}%{r["sharpe"]:>8.2f}{r["calmar"]:>8.2f}{r["n_trades"]:>8}')

    # 3. 약세장 분리
    print('\n--- 3. 약세장 분리 (Momentum Proxy 일부) ---')
    bear_periods = [
        ('2020 COVID', '2020-02-15', '2020-04-15'),
        ('2022 인플레', '2022-01-01', '2022-12-31'),
    ]
    print(f'{"기간":<13}{"SPY":<20}{"XLK":<20}{"Mom top2 50/50":<20}')
    for name, s, e in bear_periods:
        spy_r = backtest_buyhold(etf, 'SPY', s, e)
        xlk_r = backtest_buyhold(etf, 'XLK', s, e)
        mom_r = backtest_momentum(sp, n_picks=2, weights='50_50', start=s, end=e)
        if spy_r and xlk_r:
            print(f'  {name:<11}{spy_r["total_return"]:+6.1f}%/MDD{spy_r["mdd"]:+5.1f}%  '
                  f'{xlk_r["total_return"]:+6.1f}%/MDD{xlk_r["mdd"]:+5.1f}%  '
                  f'{mom_r["total_return"]:+6.1f}%/MDD{mom_r["mdd"]:+5.1f}%')

    # 4. Picks 변화 추적 (어떤 종목이 자주 등장)
    print('\n--- 4. Top 종목 등장 빈도 (Momentum top2 BT) ---')
    r = backtest_momentum(sp, n_picks=2, weights='50_50', start=start_bt, end=end_bt)
    pick_count = {}
    for d, picks in r['trades_log']:
        for tk in picks:
            pick_count[tk] = pick_count.get(tk, 0) + 1
    print(f'  매월 picks 종목 등장 횟수 (top 15):')
    for tk, c in sorted(pick_count.items(), key=lambda x: -x[1])[:15]:
        print(f'    {tk:<6} {c}회')

    print('\n' + '='*110)
    print('★ 종합')
    print('='*110)
    print('Momentum proxy strategy가 XLK 단순 buy-hold 능가하는지 확인.')


if __name__ == '__main__':
    main()
