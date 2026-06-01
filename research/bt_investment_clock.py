# -*- coding: utf-8 -*-
"""Investment Clock 7년 BT
- 데이터: research/sector_etf_8y.parquet (2018-2026)
- 사이클 분류: SPY 200d MA + VIX percentile 기반 (FRED 안 되면 fallback)
- 단계별 sector rotation BT
- 7년 누적 vs SPY buy-hold 비교
- 약세장 분리 분석 (2020 COVID, 2022 인플레)
"""
import sys, time
from pathlib import Path
import pandas as pd
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DATA = ROOT / 'research' / 'sector_etf_8y.parquet'

# Sector 매핑 (단계별 winner — 학술/실무 통설)
STAGE_SECTORS = {
    'spring':       ['XLF', 'XLY', 'XLRE', 'XLI'],   # 회복 (금융, 임의소비, 부동산, 산업)
    'summer':       ['XLK', 'XLE', 'XLB', 'XLI'],    # 확장 (기술, 에너지, 소재, 산업)
    'late_summer':  ['XLE', 'XLV', 'GLD', 'XLP'],    # 후기 (에너지, 헬스, 금, 필수)
    'autumn':       ['XLP', 'XLV', 'XLU', 'GLD'],    # 방어 (필수, 헬스, 유틸, 금)
    'late_autumn':  ['TLT', 'GLD', 'XLU'],            # 침체 임박 (장기채, 금, 유틸)
    'winter':       ['TLT', 'XLP', 'GLD'],            # 침체 (채권, 필수, 금)
    'late_winter':  ['XLF', 'XLY', 'XLRE'],          # 봄 winner 선행 (Buffett 패턴)
}

# 단순화 4-state 매핑 (시작용)
STAGE_4 = {
    'spring': ['XLF', 'XLY', 'XLI'],
    'summer': ['XLK', 'XLE', 'XLI'],
    'autumn': ['XLP', 'XLV', 'XLU'],
    'winter': ['TLT', 'GLD', 'XLP'],
}


def load_data():
    df = pd.read_parquet(DATA)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index().dropna(how='all')
    return df


def classify_state_simple(df, date):
    """단순 4-state 분류: SPY MA + VIX 결합"""
    spy = df['SPY'].loc[:date].dropna()
    vix = df['^VIX'].loc[:date].dropna()
    if len(spy) < 200 or len(vix) < 252:
        return 'summer'  # default

    spy_now = spy.iloc[-1]
    ma200 = spy.rolling(200).mean().iloc[-1]
    ma50 = spy.rolling(50).mean().iloc[-1]
    vix_now = vix.iloc[-1]
    vix_pct = (vix.iloc[-252:] <= vix_now).sum() / min(252, len(vix.iloc[-252:]))

    # 4-state classify
    if spy_now > ma200:
        if vix_pct < 0.5:
            return 'summer'  # 강세장 + 저변동성
        else:
            return 'autumn'  # 강세장 but 변동성 ↑ (전환 임박)
    else:  # SPY < MA200
        if vix_pct > 0.7:
            return 'winter'  # 약세장 + 고변동성
        else:
            return 'spring'  # 약세장 후반 / 회복 시작


def classify_state_12(df, date):
    """12-state 정밀 분류 (사용자 요청 — 초/중/늦)"""
    spy = df['SPY'].loc[:date].dropna()
    vix = df['^VIX'].loc[:date].dropna()
    if len(spy) < 200: return 'summer'

    spy_now = spy.iloc[-1]
    ma200 = spy.rolling(200).mean().iloc[-1]
    ma50 = spy.rolling(50).mean().iloc[-1]
    ma200_slope = (ma200 - spy.rolling(200).mean().iloc[-21]) / spy.rolling(200).mean().iloc[-21] if len(spy) > 220 else 0
    vix_now = vix.iloc[-1]
    vix_pct = (vix.iloc[-252:] <= vix_now).sum() / min(252, len(vix.iloc[-252:]))

    # 12-state
    if spy_now > ma200 and ma50 > ma200 and ma200_slope > 0.01:
        # 강세 진행
        if vix_pct < 0.3: return 'summer'
        elif vix_pct < 0.6: return 'late_summer'
        else: return 'autumn'
    elif spy_now > ma200 and ma50 < ma200:
        return 'late_autumn'  # SPY 정점 후 하락 시작
    elif spy_now < ma200 and ma200_slope < -0.01:
        # 침체
        if vix_pct > 0.7: return 'winter'
        else: return 'late_winter'
    else:
        return 'spring'


def backtest_clock(df, stage_func=classify_state_simple, sector_map=STAGE_4,
                   start='2019-01-01', end='2026-05-29',
                   rebalance='monthly'):
    """
    Investment Clock BT:
    - 매월 1일 = 사이클 재판정
    - 단계별 sector 균등 매수
    - 리밸런싱 시 모두 청산 → 새 sector 매수
    """
    df = df.loc[start:end]
    dates = df.index.tolist()

    # 월별 첫 영업일 추출
    rebal_dates = []
    last_month = -1
    for d in dates:
        if d.month != last_month:
            rebal_dates.append(d)
            last_month = d.month

    # 운영
    INIT = 100.0
    cash = INIT
    holdings = {}  # ticker -> shares
    states_log = []
    daily_values = []
    prev_pv = INIT

    rebal_idx = 0
    for d in dates:
        # PV 계산
        pv = cash
        for tk, sh in holdings.items():
            p = df[tk].loc[d] if tk in df.columns else None
            if p is None or pd.isna(p):
                # last known price
                hist = df[tk].loc[:d].dropna()
                p = hist.iloc[-1] if len(hist) > 0 else 0
            pv += sh * p
        daily_values.append((d, pv))
        if prev_pv > 0:
            ret = (pv - prev_pv) / prev_pv
        else: ret = 0
        prev_pv = pv

        # 리밸런싱
        if rebal_idx < len(rebal_dates) and d == rebal_dates[rebal_idx]:
            state = stage_func(df, d)
            states_log.append((d, state))
            target_sectors = sector_map.get(state, ['SPY'])
            # 청산
            for tk, sh in list(holdings.items()):
                p = df[tk].loc[d]
                if not pd.isna(p):
                    cash += sh * p
                holdings.pop(tk)
            # 매수 (균등)
            n = len(target_sectors)
            for tk in target_sectors:
                if tk not in df.columns:
                    continue
                p = df[tk].loc[d]
                if pd.isna(p) or p <= 0:
                    continue
                allocated = cash / n
                holdings[tk] = allocated / p
            cash = 0
            rebal_idx += 1

    # 마지막 PV
    final_pv = daily_values[-1][1]
    total_ret = (final_pv / INIT - 1) * 100
    pvs = [v for _, v in daily_values]
    cum = pd.Series(pvs).pct_change().fillna(0)
    n = len(cum)
    cagr = (final_pv / INIT) ** (252/n) - 1 if n > 0 else 0
    mu = cum.mean() * 252
    sigma = cum.std() * np.sqrt(252)
    sharpe = mu / sigma if sigma > 0 else 0
    # MDD
    peak = pd.Series(pvs).cummax()
    dd = (pd.Series(pvs) - peak) / peak
    mdd = dd.min() * 100
    cal = (cagr * 100) / abs(mdd) if mdd < 0 else 0

    return {
        'total_return': total_ret, 'cagr': cagr * 100,
        'mdd': mdd, 'sharpe': sharpe, 'calmar': cal,
        'final_pv': final_pv, 'states_log': states_log,
        'daily_values': daily_values,
    }


def backtest_buyhold(df, ticker='SPY', start='2019-01-01', end='2026-05-29'):
    """버이앤홀드 baseline"""
    df = df.loc[start:end]
    px = df[ticker].dropna()
    if px.empty: return None
    INIT = 100.0
    final_pv = INIT * (px.iloc[-1] / px.iloc[0])
    total_ret = (final_pv / INIT - 1) * 100
    n = len(px)
    cagr = (final_pv / INIT) ** (252/n) - 1 if n > 0 else 0
    rets = px.pct_change().fillna(0)
    mu = rets.mean() * 252; sigma = rets.std() * np.sqrt(252)
    sharpe = mu / sigma if sigma > 0 else 0
    peak = px.cummax(); dd = (px - peak) / peak
    mdd = dd.min() * 100
    cal = (cagr * 100) / abs(mdd) if mdd < 0 else 0
    return {
        'total_return': total_ret, 'cagr': cagr*100,
        'mdd': mdd, 'sharpe': sharpe, 'calmar': cal,
        'final_pv': final_pv,
    }


def main():
    print('=' * 100)
    print('Investment Clock 7년 BT (2019-01-01 ~ 2026-05-29)')
    print('=' * 100)
    df = load_data()
    print(f'data: {df.shape}, range {df.index[0].date()} ~ {df.index[-1].date()}')

    # 1. Baseline: SPY 단독
    print('\n--- 1. Baseline (SPY buy-hold) ---')
    spy = backtest_buyhold(df, 'SPY')
    print(f'  SPY: total {spy["total_return"]:+.1f}%, CAGR {spy["cagr"]:+.2f}%, MDD {spy["mdd"]:+.2f}%, '
          f'Sharpe {spy["sharpe"]:.2f}, Calmar {spy["calmar"]:.2f}')

    qqq = backtest_buyhold(df, 'XLK')  # 기술
    print(f'  XLK: total {qqq["total_return"]:+.1f}%, CAGR {qqq["cagr"]:+.2f}%, MDD {qqq["mdd"]:+.2f}%, '
          f'Sharpe {qqq["sharpe"]:.2f}, Calmar {qqq["calmar"]:.2f}')

    # 2. Investment Clock — 4-state classifier
    print('\n--- 2. Investment Clock (4-state classifier) ---')
    clock4 = backtest_clock(df, classify_state_simple, STAGE_4)
    print(f'  Clock 4-state: total {clock4["total_return"]:+.1f}%, CAGR {clock4["cagr"]:+.2f}%, '
          f'MDD {clock4["mdd"]:+.2f}%, Sharpe {clock4["sharpe"]:.2f}, Calmar {clock4["calmar"]:.2f}')

    print('\n  State 분포 (월별 86개):')
    state_count = {}
    for d, s in clock4['states_log']:
        state_count[s] = state_count.get(s, 0) + 1
    for s, c in sorted(state_count.items(), key=lambda x: -x[1]):
        print(f'    {s:<15} {c}회 ({c/len(clock4["states_log"])*100:.1f}%)')

    # 3. Investment Clock — 12-state classifier
    print('\n--- 3. Investment Clock (12-state classifier, Buffett 선행 매수 포함) ---')
    clock12 = backtest_clock(df, classify_state_12, STAGE_SECTORS)
    print(f'  Clock 12-state: total {clock12["total_return"]:+.1f}%, CAGR {clock12["cagr"]:+.2f}%, '
          f'MDD {clock12["mdd"]:+.2f}%, Sharpe {clock12["sharpe"]:.2f}, Calmar {clock12["calmar"]:.2f}')
    state_count = {}
    for d, s in clock12['states_log']:
        state_count[s] = state_count.get(s, 0) + 1
    print('  State 분포:')
    for s, c in sorted(state_count.items(), key=lambda x: -x[1]):
        print(f'    {s:<15} {c}회 ({c/len(clock12["states_log"])*100:.1f}%)')

    # 4. 약세장 분리 — 2020 COVID + 2022 인플레
    print('\n--- 4. 약세장 분리 분석 ---')
    bear_periods = [
        ('2020 COVID', '2020-02-15', '2020-04-15'),
        ('2022 인플레', '2022-01-01', '2022-12-31'),
    ]
    print(f'  {"기간":<15}{"SPY":<25}{"Clock 12-state":<30}{"Alpha":>10}')
    for name, start, end in bear_periods:
        spy_b = backtest_buyhold(df, 'SPY', start, end)
        clock_b = backtest_clock(df, classify_state_12, STAGE_SECTORS, start, end)
        if spy_b and clock_b:
            print(f'  {name:<15}'
                  f'{spy_b["total_return"]:+7.1f}% MDD{spy_b["mdd"]:+6.1f}%  '
                  f'{clock_b["total_return"]:+7.1f}% MDD{clock_b["mdd"]:+6.1f}%'
                  f'{clock_b["total_return"] - spy_b["total_return"]:>+8.1f}%p')

    # 5. 강세장 분리
    print('\n--- 5. 강세장 분리 분석 (Clock vs SPY) ---')
    bull_periods = [
        ('2019 강세', '2019-01-01', '2019-12-31'),
        ('2020-21 복구', '2020-04-15', '2021-12-31'),
        ('2023-24 AI', '2023-01-01', '2024-12-31'),
        ('2025-26 AI continuation', '2025-01-01', '2026-05-29'),
    ]
    print(f'  {"기간":<25}{"SPY":<25}{"Clock 12":<30}{"Alpha":>10}')
    for name, start, end in bull_periods:
        spy_b = backtest_buyhold(df, 'SPY', start, end)
        clock_b = backtest_clock(df, classify_state_12, STAGE_SECTORS, start, end)
        if spy_b and clock_b:
            print(f'  {name:<25}'
                  f'{spy_b["total_return"]:+7.1f}% MDD{spy_b["mdd"]:+6.1f}%  '
                  f'{clock_b["total_return"]:+7.1f}% MDD{clock_b["mdd"]:+6.1f}%'
                  f'{clock_b["total_return"] - spy_b["total_return"]:>+8.1f}%p')

    # 6. 종합 평가
    print('\n' + '=' * 100)
    print('★ 종합 평가 — 7년 누적')
    print('=' * 100)
    print(f'{"Strategy":<25}{"Return":>10}{"CAGR":>9}{"MDD":>9}{"Sharpe":>9}{"Calmar":>9}')
    print('-' * 75)
    for name, r in [('SPY buy-hold', spy), ('XLK buy-hold (tech)', qqq),
                    ('Clock 4-state', clock4), ('Clock 12-state', clock12)]:
        print(f'  {name:<23}{r["total_return"]:+9.1f}%{r["cagr"]:+8.2f}%{r["mdd"]:+8.2f}%{r["sharpe"]:>8.2f}{r["calmar"]:>8.2f}')


if __name__ == '__main__':
    main()
