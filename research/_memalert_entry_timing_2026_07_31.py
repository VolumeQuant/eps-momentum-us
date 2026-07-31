# -*- coding: utf-8 -*-
"""경보 발동 직후 단기 수익 — "신호등 뜨자마자 팔면 바닥에 파는 것 아닌가" (2026-07-31 사용자)
계기: 7/28 발동 → 7/30 반도체 폭등(SNDK +26%·MU +18.4%). 이 에피소드가 예외인지 패턴인지.
MA 브레드스는 '이미 하락한 뒤' 켜지는 구조라 단기 반등을 놓칠 소지가 구조적으로 있음.
"""
import sys, os
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE); sys.path.insert(0, os.path.join(BASE, 'research'))
from _memalert_variants_2026_07_31 import PX, BENCH, ALL

MA, K, NE, NX = 90, 3, 1, 5
px = PX[ALL]; ma = px.rolling(MA).mean(); ok = px.notna() & ma.notna()
nb = (px.lt(ma) & ok).sum(axis=1); valid = ok.sum(axis=1)
raw = (nb >= K) & (valid >= 3)
fire = raw.rolling(NE).sum() == NE
off = (~raw).rolling(NX).sum() == NX
on = False; st = []
for d in raw.index:
    if not on and fire.loc[d]: on = True
    elif on and off.loc[d]: on = False
    st.append(on)
S = pd.Series(st, index=raw.index)
b = BENCH.reindex(S.index).fillna(0)
nav = (1 + b).cumprod()
starts = [i for i in range(1, len(S)) if S.iloc[i] and not S.iloc[i - 1]]

print('=== 이번 에피소드(2026-07-28 발동) 실측 ===')
d0 = S.index[starts[-1]]
print('발동일:', d0.date())
for t in ['SNDK', 'MU', 'STX', 'WDC']:
    s = PX[t]
    try:
        p0 = s.loc[d0]; p1 = s.iloc[-1]
        print('  %-5s 발동일 %8.1f → 현재 %8.1f  %+6.1f%%' % (t, p0, p1, (p1/p0-1)*100))
    except Exception:
        pass

print('\n=== 발동 직후 단기 수익 분포 (메모리 바스켓, 과거 %d회) ===' % len(starts))
print('%-8s %10s %10s %10s %8s' % ('보유일', '평균', '중앙값', '플러스비율', '최대'))
for H in (1, 3, 5, 10, 20, 60):
    rs = []
    for i in starts:
        if i + H < len(nav):
            rs.append((nav.iloc[i+H] / nav.iloc[i] - 1) * 100)
    if rs:
        rs = np.array(rs)
        print('%-8d %+9.2f%% %+9.2f%% %9.0f%% %+7.1f%%'
              % (H, rs.mean(), np.median(rs), (rs > 0).mean()*100, rs.max()))

print('\n=== 비교: 임의 시점의 같은 기간 수익 (기준선) ===')
print('%-8s %10s %10s %10s' % ('보유일', '평균', '중앙값', '플러스비율'))
for H in (1, 3, 5, 10, 20, 60):
    rs = np.array([(nav.iloc[i+H] / nav.iloc[i] - 1) * 100
                   for i in range(0, len(nav)-H, 3)])
    print('%-8d %+9.2f%% %+9.2f%% %9.0f%%' % (H, rs.mean(), np.median(rs), (rs > 0).mean()*100))
