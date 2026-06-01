# -*- coding: utf-8 -*-
"""KR 시스템 leave-one-winner-out — 대박 종목 제외 시 alpha 유지?"""
import sys, json, glob, os
from pathlib import Path
import pandas as pd
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

STATE = Path(r'C:\dev\state')
MC_DIR = Path(r'C:\dev\data_cache')
OHLCV = pd.read_parquet(r'C:\dev\data_cache\all_ohlcv_20170601_20260529.parquet')
OHLCV.index = pd.to_datetime(OHLCV.index)


def simulate(rebal_dates, exclude=set(), n=3, init=100.0):
    val = init; prev_picks = []; prev_prices = {}; history = []
    for d, fp in rebal_dates:
        with open(fp, encoding='utf-8') as f:
            data = json.load(f)
        picks = []
        for r in data.get('rankings', [])[:30]:
            if r['ticker'] not in exclude:
                picks.append(r['ticker'])
            if len(picks) >= n: break
        if not picks: history.append((d, val)); continue

        if prev_picks:
            ret = 0
            for tk in prev_picks:
                if tk in OHLCV.columns:
                    try:
                        p_now = OHLCV[tk].asof(d)
                        p_prev = prev_prices.get(tk)
                        if p_now and p_prev and p_prev > 0:
                            ret += (p_now/p_prev - 1) / len(prev_picks)
                    except: pass
            val *= (1 + ret)
        new_prices = {}
        for tk in picks:
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
    print('=' * 100)
    print('KR Leave-one-winner-out — 대박 종목 빼면 alpha 유지되는가?')
    print('=' * 100)
    rebal = get_monthly_rebal(pd.Timestamp('2018-07-01'), pd.Timestamp('2026-05-29'))
    n_years = (rebal[-1][0] - rebal[0][0]).days / 365.25
    print(f'8년 BT, {len(rebal)} rebal')

    # 본인 picks 중 가장 자주 등장한 종목 5-10개 추출 = 대박 가능
    pick_count = {}
    for d, fp in rebal:
        with open(fp, encoding='utf-8') as f:
            d_data = json.load(f)
        for r in d_data.get('rankings', [])[:5]:
            pick_count[r['ticker']] = pick_count.get(r['ticker'], 0) + 1
    top_picks = sorted(pick_count.items(), key=lambda x: -x[1])[:10]
    print('\n2026년 ranking 자주 등장 top 10:')
    for tk, c in top_picks: print(f'  {tk}: {c}회')

    # 단순 시점별 8년 누적 수익률 — top 10 picks 각각
    print('\n각 picks의 8년 buy-hold (대박 확인):')
    start_bt = rebal[0][0]; end_bt = rebal[-1][0]
    bh_returns = []
    for tk, c in top_picks:
        if tk in OHLCV.columns:
            try:
                p0 = OHLCV[tk].asof(start_bt); p1 = OHLCV[tk].asof(end_bt)
                if p0 and p1 and p0 > 0:
                    ret = (p1/p0 - 1) * 100
                    bh_returns.append((tk, c, ret))
                    print(f'  {tk}: {ret:+.0f}% (등장 {c}회)')
            except: pass

    # Baseline
    print('\n--- Leave-out BT ---')
    val_base, _ = simulate(rebal, exclude=set())
    cagr_base = ((val_base/100)**(1/n_years) - 1) * 100
    print(f'  baseline (전체 picks): 누적 {val_base-100:+.1f}%, CAGR {cagr_base:+.2f}%')

    # Top 5 winner 제외
    top5_picks = [tk for tk, _ in top_picks[:5]]
    val_excl5, _ = simulate(rebal, exclude=set(top5_picks))
    cagr_excl5 = ((val_excl5/100)**(1/n_years) - 1) * 100
    print(f'\n  Top 5 picks 제외 ({", ".join(top5_picks)}):')
    print(f'    누적 {val_excl5-100:+.1f}%, CAGR {cagr_excl5:+.2f}%')
    print(f'    baseline 대비 alpha 변화: {cagr_excl5 - cagr_base:+.2f}%p')

    # Top 10 winner 제외
    top10_picks = [tk for tk, _ in top_picks[:10]]
    val_excl10, _ = simulate(rebal, exclude=set(top10_picks))
    cagr_excl10 = ((val_excl10/100)**(1/n_years) - 1) * 100
    print(f'\n  Top 10 picks 제외:')
    print(f'    누적 {val_excl10-100:+.1f}%, CAGR {cagr_excl10:+.2f}%')
    print(f'    baseline 대비 alpha 변화: {cagr_excl10 - cagr_base:+.2f}%p')

    # 개별 제외 (각 종목 단독 효과)
    print('\n  각 winner 단독 제외 시 alpha 영향:')
    print(f'    {"종목":<10}{"누적%":>10}{"CAGR%":>10}{"vs baseline":>15}')
    individual = []
    for tk, c in top_picks[:7]:
        val_x, _ = simulate(rebal, exclude={tk})
        cagr_x = ((val_x/100)**(1/n_years) - 1) * 100
        diff = cagr_x - cagr_base
        individual.append((tk, c, val_x-100, cagr_x, diff))
        print(f'    {tk:<10}{val_x-100:>+9.1f}%{cagr_x:>+9.2f}%{diff:>+13.2f}%p')

    # KOSPI 200 비교
    etf = pd.read_parquet(r'C:\dev\claude code\eps-momentum-us\research\kr_etf_8y.parquet')
    etf.index = pd.to_datetime(etf.index)
    tiger = etf['102110.KS'].loc[start_bt:end_bt].dropna()
    tiger_ret = (tiger.iloc[-1]/tiger.iloc[0] - 1) * 100
    tiger_cagr = ((1 + tiger_ret/100)**(1/n_years) - 1) * 100
    print(f'\n  TIGER 200 buy-hold: 누적 {tiger_ret:+.1f}%, CAGR {tiger_cagr:+.2f}%')
    print(f'\n  ★ Top 5 제외 시스템 vs TIGER 200 alpha: {cagr_excl5 - tiger_cagr:+.2f}%p')
    print(f'  ★ Top 10 제외 시스템 vs TIGER 200 alpha: {cagr_excl10 - tiger_cagr:+.2f}%p')


if __name__ == '__main__':
    main()
