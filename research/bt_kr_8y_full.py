# -*- coding: utf-8 -*-
"""KR 시스템 8년 전체 BT — 2018-2026, 약세장 (2022) 포함
- 기존 시스템 (top3 시총 무관) vs 시총 5천억+ filter
- 연도별 분리 (강세 vs 약세)
- TIGER 200 / KODEX 코스닥150 / KOSPI / 삼전 hold 비교
- 매월 리밸런싱
"""
import sys, json, glob, os
from pathlib import Path
import pandas as pd
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

STATE = Path(r'C:\dev\state')
MC_DIR = Path(r'C:\dev\data_cache')
OHLCV = pd.read_parquet(r'C:\dev\data_cache\all_ohlcv_20170601_20260529.parquet')
OHLCV.index = pd.to_datetime(OHLCV.index)

# Pre-load all MC files into memory (한 번만)
print('Loading market_cap files...', file=sys.stderr)
mc_cache = {}
for fp in sorted(glob.glob(str(MC_DIR / 'market_cap_ALL_*.parquet'))):
    d_str = os.path.basename(fp).replace('market_cap_ALL_', '').replace('.parquet', '')
    try:
        df = pd.read_parquet(fp)
        mc_col = '시가총액' if '시가총액' in df.columns else df.columns[1]
        df['mc'] = pd.to_numeric(df[mc_col], errors='coerce')
        mc_cache[d_str] = df['mc']
    except Exception as e:
        pass
print(f'  loaded {len(mc_cache)} MC files', file=sys.stderr)


def get_mc(date_str):
    """date_str = '20260529'. 가까운 날짜 mc 반환"""
    if date_str in mc_cache: return mc_cache[date_str]
    day = pd.to_datetime(date_str, format='%Y%m%d')
    for offset in range(7):
        for sign in [-1, 1]:
            alt = day + pd.Timedelta(days=offset * sign)
            alt_str = alt.strftime('%Y%m%d')
            if alt_str in mc_cache: return mc_cache[alt_str]
    return None


def simulate(rebal_dates, picks_fn, init=100.0):
    """rebal_dates = [(date, fp)], picks_fn(date, ranking_data, mc) -> [tickers]"""
    val = init
    prev_picks = []
    prev_prices = {}
    history = []

    for d, fp in rebal_dates:
        with open(fp, encoding='utf-8') as f:
            data = json.load(f)
        d_str = d.strftime('%Y%m%d')
        mc = get_mc(d_str)
        if mc is None:
            history.append((d, val)); continue

        new_picks = picks_fn(d, data, mc)
        if not new_picks:
            history.append((d, val)); continue

        # 이전 보유 종목 가치 변화 (오늘 종가)
        if prev_picks:
            ret = 0
            n = len(prev_picks)
            for tk in prev_picks:
                if tk in OHLCV.columns:
                    try:
                        p_now = OHLCV[tk].asof(d)
                        p_prev = prev_prices.get(tk)
                        if p_now and p_prev and p_prev > 0 and not pd.isna(p_now):
                            ret += (p_now/p_prev - 1) / n
                    except: pass
            val *= (1 + ret)

        # 새 picks 진입 가격
        new_prices = {}
        for tk in new_picks:
            if tk in OHLCV.columns:
                try:
                    p = OHLCV[tk].asof(d)
                    if p and not pd.isna(p) and p > 0:
                        new_prices[tk] = p
                except: pass
        prev_picks = list(new_prices.keys())
        prev_prices = new_prices
        history.append((d, val))

    return val, history


def picks_original(d, data, mc, n=3):
    """기존: rank top n"""
    return [r['ticker'] for r in data.get('rankings', [])[:n]]


def picks_large_only(d, data, mc, n=3, threshold=5e11):
    """시총 threshold+ filter → top n"""
    filt = []
    for r in data.get('rankings', [])[:30]:
        m = mc.get(r['ticker'])
        if m and not pd.isna(m) and m >= threshold:
            filt.append(r['ticker'])
        if len(filt) >= n: break
    return filt


def picks_mid_only(d, data, mc, n=3, lo=1e11, hi=5e11):
    """중형주만"""
    filt = []
    for r in data.get('rankings', [])[:30]:
        m = mc.get(r['ticker'])
        if m and not pd.isna(m) and lo <= m < hi:
            filt.append(r['ticker'])
        if len(filt) >= n: break
    return filt


def annual_metrics(history, val_final):
    if len(history) < 2: return None
    d0, d1 = history[0][0], history[-1][0]
    n_years = (d1 - d0).days / 365.25
    cagr = (val_final/100)**(1/n_years) - 1 if n_years > 0 else 0
    # daily-ish returns
    vals = [v for _, v in history]
    s = pd.Series(vals)
    rets = s.pct_change().fillna(0)
    sigma = rets.std() * np.sqrt(12)  # monthly rebal
    sharpe = (rets.mean() * 12) / sigma if sigma > 0 else 0
    peak = s.cummax(); dd = (s-peak)/peak; mdd = dd.min()*100
    cal = (cagr*100)/abs(mdd) if mdd<0 else 0
    return {'cagr': cagr*100, 'mdd': mdd, 'sharpe': sharpe, 'cal': cal}


def get_monthly_rebal(start, end):
    files = sorted(glob.glob(str(STATE / 'ranking_*.json')))
    rebal = []
    last_month = -1
    for fp in files:
        d_str = os.path.basename(fp).replace('ranking_', '').replace('.json', '')
        d = pd.to_datetime(d_str, format='%Y%m%d')
        if not (start <= d <= end): continue
        if d.month != last_month:
            rebal.append((d, fp))
            last_month = d.month
    return rebal


def main():
    print('='*100)
    print('KR 시스템 8년 BT — 2018-2026, 시기별 분리')
    print('='*100)

    full_start = pd.Timestamp('2018-07-01')  # state file 시작 부근
    full_end = pd.Timestamp('2026-05-29')
    rebal = get_monthly_rebal(full_start, full_end)
    print(f'리밸런싱 {len(rebal)}회 ({rebal[0][0].date()} ~ {rebal[-1][0].date()})\n')

    print('--- 1. 전체 기간 BT ---')
    strategies = [
        ('기존 시스템 (top3)',     lambda d, data, mc: picks_original(d, data, mc, n=3)),
        ('시총 5천억+ filter',     lambda d, data, mc: picks_large_only(d, data, mc, n=3, threshold=5e11)),
        ('시총 1조+ filter',       lambda d, data, mc: picks_large_only(d, data, mc, n=3, threshold=1e12)),
        ('시총 1천억-5천억 (중형)',  lambda d, data, mc: picks_mid_only(d, data, mc, n=3, lo=1e11, hi=5e11)),
        ('top5 균등 (시총 무관)',   lambda d, data, mc: picks_original(d, data, mc, n=5)),
    ]

    results = {}
    print(f'  {"전략":<28}{"최종":>10}{"누적%":>10}{"CAGR":>9}{"MDD":>9}{"Sharpe":>8}{"Calmar":>8}')
    print('  ' + '-'*78)
    for name, fn in strategies:
        val, hist = simulate(rebal, fn)
        m = annual_metrics(hist, val)
        if m:
            print(f'  {name:<28}{val:>10.1f}{(val-100):>+9.1f}%{m["cagr"]:>+8.2f}%{m["mdd"]:>+8.2f}%{m["sharpe"]:>8.2f}{m["cal"]:>8.2f}')
            results[name] = (val, hist, m)

    # ETF baseline
    etf = pd.read_parquet(r'C:\dev\claude code\eps-momentum-us\research\kr_etf_8y.parquet')
    etf.index = pd.to_datetime(etf.index)
    s = rebal[0][0]; e = rebal[-1][0]
    print('\n  --- ETF/지수 buy-hold ---')
    for t, name in [('102110.KS', 'TIGER 200'), ('229200.KS', 'KODEX 코스닥150'),
                    ('091160.KS', 'KODEX 반도체'), ('^KS11', 'KOSPI'),
                    ('005930.KS', '삼성전자'), ('000660.KS', 'SK하이닉스')]:
        if t not in etf.columns: continue
        sx = etf[t].loc[s:e].dropna()
        if len(sx) < 2: continue
        v = 100 * sx.iloc[-1]/sx.iloc[0]
        n_yr = (e - s).days / 365.25
        cagr = ((v/100)**(1/n_yr) - 1) * 100
        peak = sx.cummax(); dd = (sx-peak)/peak; mdd = dd.min()*100
        print(f'  {name:<28}{v:>10.1f}{(v-100):>+9.1f}%{cagr:>+8.2f}%{mdd:>+8.2f}%')

    # 2. 연도별 분리
    print('\n--- 2. 연도별 분리 (기존 vs 신규 vs TIGER 200) ---')
    print(f'{"기간":<20}{"기존":>12}{"신규(5천억+)":>15}{"TIGER 200":>12}')
    yearly = [
        ('2019', '2019-01-01', '2019-12-31'),
        ('2020 (COVID)', '2020-01-01', '2020-12-31'),
        ('2021', '2021-01-01', '2021-12-31'),
        ('2022 (약세장)', '2022-01-01', '2022-12-31'),
        ('2023', '2023-01-01', '2023-12-31'),
        ('2024', '2024-01-01', '2024-12-31'),
        ('2025-26 YTD', '2025-01-01', '2026-05-29'),
    ]
    for label, s, e in yearly:
        s_ts = pd.Timestamp(s); e_ts = pd.Timestamp(e)
        rebal_yr = [(d, fp) for d, fp in rebal if s_ts <= d <= e_ts]
        if len(rebal_yr) < 3: continue
        v_o, _ = simulate(rebal_yr, lambda d, data, mc: picks_original(d, data, mc, n=3))
        v_n, _ = simulate(rebal_yr, lambda d, data, mc: picks_large_only(d, data, mc, n=3, threshold=5e11))
        # TIGER
        tig = etf['102110.KS'].loc[s_ts:e_ts].dropna() if '102110.KS' in etf.columns else None
        tig_ret = (tig.iloc[-1]/tig.iloc[0]*100 - 100) if (tig is not None and len(tig) >= 2) else None
        tig_str = f'{tig_ret:+8.1f}%' if tig_ret is not None else '   N/A  '
        print(f'  {label:<18}{v_o-100:>+10.1f}%{v_n-100:>+13.1f}%{tig_str:>12}')

    print('\n' + '='*100)
    print('★ 종합')
    print('='*100)
    print('주의: monthly rebalancing, slippage·세금 0 가정. 실제 라이브는 -15~30%p 차감 가능.')


if __name__ == '__main__':
    main()
