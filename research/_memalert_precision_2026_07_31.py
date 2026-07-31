# -*- coding: utf-8 -*-
"""메모리 경보 정밀도/재현율 측정 (2026-07-31)
사용자 가설: "위기 때는 항상 3/6이 켜졌지만, 3/6 켜졌다고 항상 위기는 아니었다"
→ 맞다면 매매 명령이 아니라 주의 표시가 올바른 사용법.
"""
import sys, os
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import memory_cycle_alert as M
import yfinance as yf

px = yf.download(M.CLUSTER, period='10y', progress=False, auto_adjust=True,
                 threads=2)['Close'].ffill()
cov = px.notna().sum()
print('데이터 커버리지(일수):', {t: int(cov[t]) for t in M.CLUSTER})
# SNDK 등 신규 상장은 결측 → 그날 존재하는 종목만으로 비율 판정 (production과 동일 정신)
ma = px.rolling(M.MA).mean()
below = px.lt(ma) & px.notna() & ma.notna()
valid = (px.notna() & ma.notna()).sum(axis=1)
nb = below.sum(axis=1)
raw = (nb >= M.K_FIRE) & (valid >= 4)
fire = raw.rolling(M.N_CONFIRM).sum() == M.N_CONFIRM
off = (~raw).rolling(M.N_CLEAR).sum() == M.N_CLEAR
on = False; state = []
for d in raw.index:
    if not on and fire.loc[d]: on = True
    elif on and off.loc[d]: on = False
    state.append(on)
S = pd.Series(state, index=raw.index)
S = S[valid >= 4]

# 메모리 바스켓 = 그날 존재하는 종목 동일가중
ret = px.pct_change()
basket = ret.mean(axis=1).reindex(S.index).fillna(0)
nav = (1 + basket).cumprod()

print('\n=== 경보 발동 빈도 ===')
print('총 %d거래일 중 ON %d일 (%.1f%%)' % (len(S), S.sum(), S.mean()*100))
# 에피소드 분할
eps = []; st = None
for i, (d, v) in enumerate(S.items()):
    if v and st is None: st = i
    elif not v and st is not None: eps.append((st, i)); st = None
if st is not None: eps.append((st, len(S)-1))
print('발동 에피소드 %d회' % len(eps))

print('\n=== 각 발동 에피소드가 실제 급락으로 이어졌나 ===')
print('%-24s %8s %10s %12s' % ('기간', '지속일', '기간중 최대낙폭', '판정'))
hit = 0
for a, b in eps:
    seg = nav.iloc[a:b+1]
    dd = (seg / seg.cummax() - 1).min() * 100
    real = dd <= -20
    hit += real
    print('%-24s %8d %9.1f%% %12s' % ('%s~%s' % (S.index[a].date(), S.index[b].date()),
          b-a+1, dd, '위기(-20%↓)' if real else '헛방어'))
print('\n정밀도(발동 중 실제 급락 비율): %d/%d = %.0f%%' % (hit, len(eps), hit/len(eps)*100))

print('\n=== 재현율: 큰 급락기에 경보가 켜져 있었나 ===')
dd_all = (nav / nav.cummax() - 1)
crisis = dd_all <= -0.20
print('바스켓 −20%% 이상 낙폭 상태였던 날: %d일' % crisis.sum())
if crisis.sum():
    print('그 중 경보 ON이었던 비율: %.0f%%' % (S[crisis].mean()*100))

print('\n=== 조건부 성과 (경보 ON일 vs OFF일, 메모리 바스켓) ===')
for lbl, mask in [('경보 ON', S), ('경보 OFF', ~S)]:
    r = basket[mask]
    print('  %-8s 일평균 %+.3f%%  연율 %+6.1f%%  일수 %d' % (lbl, r.mean()*100, r.mean()*252*100, len(r)))
