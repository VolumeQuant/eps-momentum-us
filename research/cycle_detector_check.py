# -*- coding: utf-8 -*-
# 감지기 현재 상태 + 발동 에피소드 전체 이력 (MA50 80% 설정)
import sys
import pandas as pd
import yfinance as yf
sys.stdout.reconfigure(encoding='utf-8')

TICKS = ['MU', 'WDC', 'STX', '005930.KS', '000660.KS']
px = yf.download(TICKS, start='2015-01-01', end='2026-07-09', progress=False, auto_adjust=True, threads=2)['Close']
px = px.dropna(how='all').ffill()

below = (px < px.rolling(50).mean()).mean(axis=1)
raw = below >= 0.8
fire = raw.rolling(3).sum() == 3
off = (~raw).rolling(15).sum() == 15
state = pd.Series(False, index=raw.index)
on = False
eps = []; s = None
for d in raw.index:
    if not on and fire.loc[d]:
        on = True; s = d
    elif on and off.loc[d]:
        on = False; eps.append((s, d))
    state.loc[d] = on
if on: eps.append((s, raw.index[-1]))

print('발동 에피소드 전체 (2015~):')
for a, b in eps:
    print(f'  {a.date()} ~ {b.date()}  ({(b-a).days}일)')
print(f'\n오늘(최종 {px.index[-1].date()}) 상태: {"🚨 발동 중" if state.iloc[-1] else "✅ 꺼짐"}')
print(f'오늘 브레드스(MA50 아래 비율): {below.iloc[-1]*100:.0f}%  (발동 기준 80%)')
for t in TICKS:
    b = px[t].iloc[-1] < px[t].rolling(50).mean().iloc[-1]
    print(f'  {t:10s} {"MA50 아래 ⚠️" if b else "MA50 위"}')
