# -*- coding: utf-8 -*-
"""추가 검증 — 거래비용·2023협소장비용·신호정밀도·방어자산/프록시 민감도."""
import sys
import numpy as np
import pandas as pd
import harness as H
import validate as V
import candidates2 as C2
sys.stdout.reconfigure(encoding='utf-8')

IDX = H.IDX
base = H.base_regime()
br = V.combined(C2.sector_breadth_weak(0.45), 3, 15)


def nav_metrics(dfn, ret_series, cost=0.0):
    """전환당 cost(편도%) 차감해 NAV 지표. ret_series=프록시 일수익."""
    dfn = dfn.reindex(IDX).fillna(False)
    pos = (~dfn).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, ret_series.values, H.NOTE), index=IDX)
    switches = pos.astype(int).diff().abs().fillna(0)
    r = r - switches * cost
    nav = (1 + r).cumprod()
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else 0)


print('=== [1] 거래비용 스트레스 (전환당 편도 비용 차감, 프록시=QQQ) ===')
print(f'{"비용/전환":>10}{"기존 Cal":>12}{"브레드스 Cal":>14}{"기존 CAGR":>12}{"브레드스 CAGR":>14}')
for cost in [0.0, 0.001, 0.0025, 0.005, 0.01]:
    cb = nav_metrics(base, H.D['qr'], cost); cr = nav_metrics(br, H.D['qr'], cost)
    print(f'{cost*100:>9.2f}%{cb[2]:>12.2f}{cr[2]:>14.2f}{cb[0]:>+11.1f}{cr[0]:>+13.1f}')
print('  → 브레드스 Cal이 비용 1%까지 기존 이상 유지하면 통과(전환 +8회 비용 흡수).')

print('\n=== [2] 2023 메가캡 협소장 — 헛방어 비용 정량화 ===')
def yr_ret(dfn, y0, y1):
    win = IDX[(IDX >= y0) & (IDX <= y1)]
    pos = (~dfn.reindex(IDX).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, H.NOTE), index=IDX).reindex(win)
    return ((1 + r).prod() - 1) * 100
for lbl, a, b in [('2023만', '2023-01-01', '2023-12-31'), ('2023-2024', '2023-01-01', '2024-12-31')]:
    qqqr = (H.D['qqq'].reindex(IDX[(IDX >= a) & (IDX <= b)]).iloc[-1] /
            H.D['qqq'].reindex(IDX[(IDX >= a) & (IDX <= b)]).iloc[0] - 1) * 100
    print(f'  {lbl}: QQQ {qqqr:+.1f}% | 기존 {yr_ret(base,a,b):+.1f}% | 브레드스 {yr_ret(br,a,b):+.1f}% '
          f'(헛방어 비용 {yr_ret(br,a,b)-yr_ret(base,a,b):+.1f}%p)')

print('\n=== [3] 신호 정밀도 — 브레드스 단독 발동(기존 OFF)이 옳았나 ===')
# 브레드스 defense인데 base는 boost인 "단독 구간"의 진입일 → forward 21/63일 QQQ 수익
breadth_only = (br & ~base).reindex(IDX).fillna(False)
entries = IDX[breadth_only.astype(int).diff() == 1]
px = H.D['qqq']
fwd21, fwd63, neg21 = [], [], 0
for e in entries:
    i = IDX.get_loc(e)
    if i + 63 >= len(IDX):
        continue
    f21 = px.iloc[i + 21] / px.iloc[i] - 1
    f63 = px.iloc[i + 63] / px.iloc[i] - 1
    fwd21.append(f21 * 100); fwd63.append(f63 * 100)
    if f21 < 0:
        neg21 += 1
n = len(fwd21)
print(f'  브레드스 단독 발동 {n}회 (기존 게이트는 OFF인데 브레드스만 방어).')
print(f'  발동 후 21일 평균수익 {np.mean(fwd21):+.1f}% (음수=방어가 옳았음). 하락적중 {neg21}/{n} = {neg21/n*100:.0f}%')
print(f'  발동 후 63일 평균수익 {np.mean(fwd63):+.1f}%')
print(f'  → 단독발동 후 시장이 평균 하락하면 "조기방어 정당", 평균 상승하면 "헛방어 경고".')

print('\n=== [4] 방어자산·프록시 민감도 (가정 바꿔도 결론 유지되나) ===')
# 4a 방어자산: NOTE(현행) vs 현금0 vs BIL(2007+)
A = pd.read_parquet(H._R / 'regime_assets.parquet'); A.index = pd.to_datetime(A.index)
bil = A['BIL'].reindex(IDX).ffill().pct_change().fillna(0)
def nav_alt_defense(dfn, defense_ret):
    pos = (~dfn.reindex(IDX).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, defense_ret.values), index=IDX)
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr / abs(mdd) if mdd < 0 else 0, mdd * 100
zero = pd.Series(0.0, index=IDX)
print('  방어자산별 Calmar / MDD:')
for lbl, dr in [('발행어음3%(현행)', pd.Series(H.NOTE, index=IDX)), ('현금0%', zero), ('BIL(2007+)', bil)]:
    cb = nav_alt_defense(base, dr); cr = nav_alt_defense(br, dr)
    print(f'    {lbl:<16} 기존 {cb[0]:.2f}/{cb[1]:+.0f}   브레드스 {cr[0]:.2f}/{cr[1]:+.0f}')
# 4b 프록시: QQQ vs SPY
spy = H.D['spy'].reindex(IDX).ffill()
spyr = spy.pct_change().fillna(0)
print('  프록시별 Calmar / MDD (방어=발행어음3%):')
for lbl, rr in [('QQQ(현행)', H.D['qr']), ('SPY(광범위)', spyr)]:
    cb = nav_metrics(base, rr); cr = nav_metrics(br, rr)
    print(f'    {lbl:<14} 기존 {cb[2]:.2f}/{cb[1]:+.0f}   브레드스 {cr[2]:.2f}/{cr[1]:+.0f}')
