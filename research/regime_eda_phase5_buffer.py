"""Regime Phase 5 — MA200 버퍼로 휘프소(얕은 돌파) 거르기

문제: 우리 71일 데이터의 유일한 defense(2026-04 초)가 SPX 1% 얕은 돌파 후 V자 반등
→ 오버레이가 최고 랠리를 IEF로 놓쳐 -105%p. 전형적 MA 휘프소.
해결 후보: 'SPX < MA200 × (1-buf)' 버퍼로 얕은 돌파 무시. 진짜 약세장은 MA200 한참 아래라 통과.

측정:
 (1) 26년 QQQ: 버퍼 0~7% × (VIX>36 override 유지) → 약세장 포착 / Cal / MDD / 휘프소
 (2) 우리 71일: 버퍼별 defense 일수 + 4월 최대 돌파 깊이 (버퍼가 4월 거르는지)
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


def overlay_cash(proxy, reg):
    pos = (~reg.reindex(proxy.index).ffill().fillna(False).astype(bool)).shift(1, fill_value=False)
    strat = np.where(pos.values, proxy.pct_change().fillna(0).values, 0.0)
    nav = (1 + pd.Series(strat, index=proxy.index)).cumprod()
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else float('nan'))


def main():
    spx = fetch_close('^GSPC')
    qqq = fetch_close('QQQ').reindex(spx.index).ffill()
    vix = fetch_close('^VIX').reindex(spx.index).ffill()
    ma = spx.rolling(200).mean()
    vix_def = confirm((vix > 36).fillna(False), 2)

    print('=' * 104)
    print('(1) 26년 QQQ — MA200 버퍼 × VIX>36 override (defense=현금)')
    print(f'{"buf":>5} {"%def":>6} {"전환":>5} | {"dotcom":>6} {"GFC":>6} {"COVID":>6} {"2022":>6} | {"CAGR":>7} {"MDD":>7} {"Cal":>5}')
    print('-' * 104)
    for buf in (0.0, 0.02, 0.03, 0.05, 0.07):
        raw = (spx < ma * (1 - buf))
        reg = (confirm(raw.fillna(False), 10) | vix_def)
        cov = {k: reg.loc[s:e].mean() * 100 for k, (s, e) in KNOWN_BEARS.items()}
        cagr, mdd, cal = overlay_cash(qqq, reg)
        print(f'{buf*100:>4.0f}% {reg.mean()*100:>5.1f}% {int((reg!=reg.shift(1)).sum()):>5} | '
              f'{cov["dotcom 2000-02"]:>5.0f}% {cov["GFC 2008"]:>5.0f}% {cov["COVID 2020"]:>5.0f}% {cov["rate 2022"]:>5.0f}% | '
              f'{cagr:>+6.1f}% {mdd:>+6.1f}% {cal:>5.2f}')

    # (2) 우리 71일 window
    print('\n' + '=' * 104)
    print('(2) 우리 전략 window (2026-02~05) — 버퍼별 defense 일수')
    import sqlite3
    con = sqlite3.connect(ROOT / 'eps_momentum_data.db')
    adates = [r[0] for r in con.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    w0, w1 = adates[0], adates[-1]
    seg_spx = spx.loc[:w1]
    seg_ma = seg_spx.rolling(200).mean()
    # window 내 최대 돌파 깊이
    win_idx = [pd.Timestamp(d) for d in adates if pd.Timestamp(d) in seg_spx.index]
    ratios = [(seg_spx.loc[t] / seg_ma.loc[t] - 1) * 100 for t in win_idx if not np.isnan(seg_ma.loc[t])]
    print(f'window {w0}~{w1}: SPX/MA200 최저 = {min(ratios):+.1f}% (가장 깊은 돌파)')
    vix_seg = confirm((vix > 36).fillna(False), 2)
    for buf in (0.0, 0.02, 0.03, 0.05):
        raw = (spx < ma * (1 - buf))
        reg = (confirm(raw.fillna(False), 10) | vix_seg)
        reg.index = reg.index.strftime('%Y-%m-%d')
        ndef = sum(1 for d in adates if bool(reg.get(d, False)))
        print(f'  buf {buf*100:>2.0f}%: window 내 defense {ndef}일 → ' +
              ('휘프소 제거 ✅ (전부 boost, 수익 +248.7% 유지)' if ndef == 0 else f'아직 {ndef}일 defense'))

    print('\n해석: 버퍼가 4월 1% 돌파를 거르면서(우리 window defense 0일) '
          '26년 진짜 약세장(MA200 한참 아래)은 계속 잡으면 = 휘프소만 제거된 robust 신호.')


if __name__ == '__main__':
    main()
