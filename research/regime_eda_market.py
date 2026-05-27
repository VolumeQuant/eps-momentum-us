"""Regime 신호 EDA (Phase 1) — 26년 시장 데이터로 약세장 포착력 + 현금 오버레이 효과

US 시스템은 종목 DB가 71일(강세장)뿐이라 KR식 전략 풀백테스트 불가.
대신 regime을 종목전략과 분리된 오버레이로 보고:
  - 신호 품질: S&P/VIX/HY 기반 신호가 2000·2008·2020·2022 약세장 잡나? 휘프소?
  - 효과 프록시: 고베타 성장주(QQQ)를 boost에 보유 / defense에 현금 → MDD·Calmar 개선?

데이터: yfinance ^GSPC, ^VIX, QQQ (+ HY 캐시). auto_adjust=True (ETF 배당 포함).
주의: 신호 확정 다음날 매매(lookahead 방지).
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
HY_CACHE = ROOT / 'data_cache' / 'hy_spread.parquet'
START = '2000-01-01'

KNOWN_BEARS = {
    'dotcom 2000-02': ('2000-09-01', '2002-10-15'),
    'GFC 2008': ('2007-10-09', '2009-03-09'),
    'COVID 2020': ('2020-02-19', '2020-03-23'),
    'rate 2022': ('2022-01-03', '2022-10-12'),
}


def fetch_close(ticker):
    import yfinance as yf
    df = yf.download(ticker, start=START, auto_adjust=True, progress=False)
    if df.empty:
        return None
    cl = df['Close']
    if isinstance(cl, pd.DataFrame):
        cl = cl.iloc[:, 0]
    cl.index = pd.to_datetime(cl.index).tz_localize(None)
    return cl.dropna()


def confirm(raw_defense: pd.Series, n: int) -> pd.Series:
    """raw_defense(bool) → n일 연속 확인 후 regime 전환. True=defense."""
    regime = pd.Series(False, index=raw_defense.index)  # False=boost
    state = False
    streak_d = 0
    streak_b = 0
    for i, d in enumerate(raw_defense.values):
        if d:
            streak_d += 1
            streak_b = 0
        else:
            streak_b += 1
            streak_d = 0
        if not state and streak_d >= n:
            state = True
        elif state and streak_b >= n:
            state = False
        regime.iloc[i] = state
    return regime


def eval_overlay(proxy: pd.Series, regime_defense: pd.Series, defense_ret=None):
    """boost=proxy 보유, defense=현금(0) or defense_ret(채권 등). 신호 1일 지연."""
    px = proxy.reindex(regime_defense.index).ffill().dropna()
    reg = regime_defense.reindex(px.index).ffill().fillna(False)
    pos = (~reg).shift(1).fillna(False)  # 어제 boost였으면 오늘 proxy 보유
    pret = px.pct_change().fillna(0)
    if defense_ret is not None:
        dret = defense_ret.reindex(px.index).pct_change().fillna(0)
        strat = np.where(pos, pret, dret)
    else:
        strat = np.where(pos, pret, 0.0)
    nav = (1 + pd.Series(strat, index=px.index)).cumprod()
    yrs = (px.index[-1] - px.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1
    peak = nav.cummax()
    mdd = ((nav - peak) / peak).min()
    cal = cagr / abs(mdd) if mdd < 0 else float('nan')
    return {'cagr': cagr * 100, 'mdd': mdd * 100, 'cal': cal, 'nav': nav.iloc[-1]}


def bear_coverage(regime_defense: pd.Series):
    """각 약세장 구간에서 defense 비율."""
    out = {}
    for name, (s, e) in KNOWN_BEARS.items():
        seg = regime_defense.loc[s:e]
        out[name] = seg.mean() * 100 if len(seg) else float('nan')
    return out


def flips(regime_defense: pd.Series):
    return int((regime_defense != regime_defense.shift(1)).sum())


def main():
    print('=' * 110)
    print('Regime 신호 EDA — 데이터 수집')
    spx = fetch_close('^GSPC')
    vix = fetch_close('^VIX')
    qqq = fetch_close('QQQ')
    print(f'  ^GSPC {spx.index[0].date()}~{spx.index[-1].date()} ({len(spx)})')
    print(f'  ^VIX  {vix.index[0].date()}~{vix.index[-1].date()} ({len(vix)})')
    print(f'  QQQ   {qqq.index[0].date()}~{qqq.index[-1].date()} ({len(qqq)})')

    # HY
    hy = None
    if HY_CACHE.exists():
        hdf = pd.read_parquet(HY_CACHE)
        hdf.index = pd.to_datetime(hdf.index).tz_localize(None)
        hy = hdf['hy_spread'].dropna()
        print(f'  HY    {hy.index[0].date()}~{hy.index[-1].date()} ({len(hy)})')

    spx = spx.loc[START:]
    ma200 = spx.rolling(200).mean()
    ma50 = spx.rolling(50).mean()
    vix_ma20 = vix.rolling(20).mean().reindex(spx.index).ffill()

    # --- 신호 후보 (raw defense bool) ---
    signals = {}
    signals['SPX<MA200 (5d)'] = (spx < ma200)
    signals['SPX<MA200 (10d)'] = (spx < ma200)  # confirm n 다르게 적용
    signals['MA50<MA200 death (5d)'] = (ma50 < ma200)
    signals['VIX_ma20>22 (5d)'] = (vix_ma20 > 22)
    signals['VIX_ma20>25 (5d)'] = (vix_ma20 > 25)
    if hy is not None:
        hy_r = hy.reindex(spx.index).ffill()
        hy_med = hy_r.rolling(2520, min_periods=1260).median()
        hy_3m = hy_r.shift(63)
        # Q4 겨울(침체): wide(≥median) AND rising
        signals['HY Q4 wide&rising (5d)'] = (hy_r >= hy_med) & (hy_r >= hy_3m)
        # 조합: SPX<MA200 OR VIX>25
        signals['SPX<MA200 | VIX>25 (5d)'] = (spx < ma200) | (vix_ma20 > 25)

    confirm_n = {'SPX<MA200 (10d)': 10}

    print('\n' + '=' * 110)
    print('신호별 — defense 비율 / 전환수(휘프소) / 약세장 포착(%) / QQQ 현금오버레이 효과')
    print('=' * 110)
    # baseline: QQQ buy&hold
    bh = eval_overlay(qqq, pd.Series(False, index=spx.index))
    print(f'{"[QQQ buy&hold]":<30} {"":>8} flips={"-":>4}  | CAGR {bh["cagr"]:+6.1f}% MDD {bh["mdd"]:+6.1f}% Cal {bh["cal"]:.2f} NAV x{bh["nav"]:.1f}')
    print('-' * 110)
    hdr = f'{"signal":<30} {"%def":>6} {"flips":>5}  | ' + ' '.join(f'{k.split()[0][:6]:>6}' for k in KNOWN_BEARS) + '  | overlay'
    print(hdr)
    for name, raw in signals.items():
        n = confirm_n.get(name, 5)
        reg = confirm(raw.fillna(False), n)
        pct_def = reg.mean() * 100
        cov = bear_coverage(reg)
        ov = eval_overlay(qqq, reg)
        covs = ' '.join(f'{cov[k]:>5.0f}%' for k in KNOWN_BEARS)
        print(f'{name:<30} {pct_def:>5.1f}% {flips(reg):>5}  | {covs}  | '
              f'CAGR {ov["cagr"]:+6.1f}% MDD {ov["mdd"]:+6.1f}% Cal {ov["cal"]:.2f} NAV x{ov["nav"]:.1f}')

    print('\n해석: MDD 큰 폭 개선 + Cal 개선 + 약세장 포착 높음 + flips 적음(휘프소 낮음) = 좋은 신호.')
    print('QQQ buy&hold 대비 overlay가 CAGR 손해 작고 MDD 크게 줄이면 보험으로 우수.')


if __name__ == '__main__':
    main()
