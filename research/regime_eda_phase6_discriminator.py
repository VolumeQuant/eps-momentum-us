"""Regime Phase 6 — 휘프소 vs 진짜 약세장 판별자 비교

버퍼(Phase5)는 4월 휘프소 제거하지만 26년 보호력 깎음(Cal 0.26→0.19). 과적합 위험.
더 나은 판별자 후보:
  - 긴 confirm (SPX<MA200 15/20d): 짧은 dip 무시
  - MA50<MA200 데드크로스: 2주 얕은 dip엔 MA50 안 꺾여 구조적으로 휘프소 회피
모두 VIX>36(2d) override (COVID 급락 대비). 약세장은 cash defense.

목표: 우리 window defense 0일(4월 휘프소 제거) + 26년 Cal/MDD/포착 유지 = robust.
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from regime_eda_market import fetch_close, confirm, KNOWN_BEARS  # noqa


def main():
    spx = fetch_close('^GSPC')
    qqq = fetch_close('QQQ').reindex(spx.index).ffill()
    vix = fetch_close('^VIX').reindex(spx.index).ffill()
    ma200 = spx.rolling(200).mean()
    ma50 = spx.rolling(50).mean()
    vix_def = confirm((vix > 36).fillna(False), 2)

    import sqlite3
    con = sqlite3.connect(ROOT / 'eps_momentum_data.db')
    adates = [r[0] for r in con.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]

    def overlay(reg):
        pos = (~reg.reindex(qqq.index).ffill().fillna(False).astype(bool)).shift(1, fill_value=False)
        nav = (1 + pd.Series(np.where(pos.values, qqq.pct_change().fillna(0).values, 0.0), index=qqq.index)).cumprod()
        yrs = (nav.index[-1] - nav.index[0]).days / 365.25
        cagr = nav.iloc[-1] ** (1 / yrs) - 1
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else float('nan'))

    def our_defense_days(reg):
        r = reg.copy()
        r.index = r.index.strftime('%Y-%m-%d')
        return sum(1 for d in adates if bool(r.get(d, False)))

    candidates = [
        ('SPX<MA200 10d (base)', confirm((spx < ma200).fillna(False), 10) | vix_def),
        ('SPX<MA200 15d', confirm((spx < ma200).fillna(False), 15) | vix_def),
        ('SPX<MA200 20d', confirm((spx < ma200).fillna(False), 20) | vix_def),
        ('SPX<MA200*0.98 10d', confirm((spx < ma200 * 0.98).fillna(False), 10) | vix_def),
        ('MA50<MA200 death 5d', confirm((ma50 < ma200).fillna(False), 5) | vix_def),
        ('MA50<MA200 death 3d', confirm((ma50 < ma200).fillna(False), 3) | vix_def),
        ('MA50<MA200 death 1d', confirm((ma50 < ma200).fillna(False), 1) | vix_def),
    ]

    print('=' * 110)
    print('판별자 비교 — 26년 QQQ(현금 defense) + 우리 window defense 일수')
    print(f'{"신호":<24} {"%def":>6} {"전환":>5} | {"dot":>5} {"GFC":>5} {"COV":>5} {"22":>5} | {"CAGR":>7} {"MDD":>7} {"Cal":>5} | {"우리window":>10}')
    print('-' * 110)
    for name, reg in candidates:
        cov = {k: reg.loc[s:e].mean() * 100 for k, (s, e) in KNOWN_BEARS.items()}
        cagr, mdd, cal = overlay(reg)
        nd = our_defense_days(reg)
        flag = '✅ 0일' if nd == 0 else f'⚠️ {nd}일'
        print(f'{name:<24} {reg.mean()*100:>5.1f}% {int((reg!=reg.shift(1)).sum()):>5} | '
              f'{cov["dotcom 2000-02"]:>4.0f}% {cov["GFC 2008"]:>4.0f}% {cov["COVID 2020"]:>4.0f}% {cov["rate 2022"]:>4.0f}% | '
              f'{cagr:>+6.1f}% {mdd:>+6.1f}% {cal:>5.2f} | {flag:>10}')

    print('\n해석: 우리 window 0일(4월 휘프소 제거) + 26년 Cal 0.26↑ + 약세장 포착 높음 = 최적.')
    print('death cross 가설: 2주 얕은 dip엔 MA50 안 꺾여 4월 회피하면서 진짜 약세장 잡으면 버퍼보다 우월.')


if __name__ == '__main__':
    main()
