# -*- coding: utf-8 -*-
"""look-ahead 전수 감사 (2026-07-31) — 메모리 경보에서 발견된 '신호를 당일 종가로 판정해
당일 수익에 적용' 버그가 국면 오버레이·브레드스에도 있는지, 있다면 결론이 바뀌는지.
"""
import sys, os
import numpy as np, pandas as pd, yfinance as yf
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr


def close(tk, period='max'):
    df = yf.download(tk, period=period, auto_adjust=True, progress=False)
    c = df['Close']
    if hasattr(c, 'columns'): c = c.iloc[:, 0]
    c.index = pd.to_datetime(c.index).tz_localize(None)
    return c.dropna()


def confirm(raw, ne, nx):
    out, st, sd, sb = [], False, 0, 0
    for v in raw.values:
        if bool(v): sd += 1; sb = 0
        else: sb += 1; sd = 0
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        out.append(st)
    return pd.Series(out, index=raw.index)


def stat(r):
    nav = (1 + r).cumprod(); yrs = len(r) / 252
    c = (nav.iloc[-1] ** (1 / yrs) - 1) * 100
    m = (nav / nav.cummax() - 1).min() * 100
    return c, m, c / abs(m)


if __name__ == '__main__':
    spx = close('^GSPC'); vix = close('^VIX'); qqq = close('^IXIC')
    idx = spx.index.intersection(qqq.index)
    spx, qqq = spx[idx], qqq[idx]
    vix = vix.reindex(idx).ffill()
    ret = qqq.pct_change().fillna(0)

    print('국면 오버레이 (S&P<MA200 %d일확인 OR VIX>%s %d일확인) — 나스닥 프록시 %d년'
          % (dr.REGIME_MA_CONFIRM, dr.REGIME_VIX_THRESH, dr.REGIME_VIX_CONFIRM, len(ret)//252))
    ma = spx.rolling(dr.REGIME_MA_PERIOD).mean()
    ma_def = confirm((spx < ma).fillna(False), dr.REGIME_MA_CONFIRM, dr.REGIME_MA_CONFIRM)
    vx_def = confirm((vix > dr.REGIME_VIX_THRESH).fillna(False), dr.REGIME_VIX_CONFIRM, dr.REGIME_VIX_CONFIRM)
    D = (ma_def | vx_def)
    print('\n%-26s %10s %10s %8s' % ('', 'CAGR', 'MDD', 'Calmar'))
    print('-' * 58)
    c, m, k = stat(ret); print('%-26s %+9.1f%% %+9.1f%% %8.2f' % ('국면 무시', c, m, k))
    for lag, lbl in [(0, 'lag=0 (당일적용·오염)'), (1, 'lag=1 (정직)'), (2, 'lag=2 (보수)')]:
        S = D.shift(lag).fillna(False)
        c, m, k = stat(ret.where(~S, 0.0))
        print('%-26s %+9.1f%% %+9.1f%% %8.2f' % (lbl, c, m, k))

    print('\n확인일수를 없애면 (즉시발동·즉시해제) — 오염 민감도 비교')
    D0 = ((spx < ma) | (vix > dr.REGIME_VIX_THRESH)).fillna(False)
    for lag in (0, 1):
        c, m, k = stat(ret.where(~D0.shift(lag).fillna(False), 0.0))
        print('  확인 0일 lag=%d: CAGR %+7.1f%% MDD %+7.1f%% Calmar %.2f' % (lag, c, m, k))

    print('\n섹터 브레드스 (11 SPDR, 200일선 위 비율 < %s)' % dr.REGIME_BREADTH_THR)
    bdf = yf.download(dr.SECTOR_ETFS, period='max', auto_adjust=True, progress=False, threads=2)
    bc = bdf['Close']
    ab = av = None
    for cc in bc.columns:
        s = bc[cc].dropna()
        if len(s) < 200: continue
        m2 = s.rolling(200).mean(); ok = s.notna() & m2.notna()
        a = (s > m2).astype(float).where(ok, 0)
        ab = a if ab is None else ab.add(a, fill_value=0)
        av = ok.astype(float) if av is None else av.add(ok.astype(float), fill_value=0)
    frac = (ab / av.replace(0, np.nan)).reindex(idx).ffill()
    braw = (frac < dr.REGIME_BREADTH_THR).fillna(False)
    B = confirm(braw, dr.REGIME_BREADTH_NE, dr.REGIME_BREADTH_NX)
    print('%-26s %10s %10s %8s' % ('', 'CAGR', 'MDD', 'Calmar'))
    print('-' * 58)
    c, m, k = stat(ret); print('%-26s %+9.1f%% %+9.1f%% %8.2f' % ('무시', c, m, k))
    for lag, lbl in [(0, 'lag=0 (오염)'), (1, 'lag=1 (정직)')]:
        w = pd.Series(1.0, index=idx)
        w = w.where(~B.shift(lag).fillna(False), 0.5)      # 브레드스 단독 = 50% 스케일
        w = w.where(~D.shift(lag).fillna(False), 0.0)
        c, m, k = stat(ret * w)
        print('%-26s %+9.1f%% %+9.1f%% %8.2f' % ('국면+브레드스 ' + lbl, c, m, k))
