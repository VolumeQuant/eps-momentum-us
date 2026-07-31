# -*- coding: utf-8 -*-
"""경보를 '매일'이 아니라 '5일 리밸 시점에만' 판정하면? (2026-07-31 사용자 제안)
근거: R5는 매매 횟수를 줄이려고 고른 값인데 경보만 매일 발동하면 그 설계가 무력화됨.
      + 두 시스템이 다른 시계를 쓰는 문제(2026-07-10 '시계 3개' 사고와 같은 구조).
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
b = BENCH.reindex(S.index).fillna(0).dropna()
S = S.reindex(b.index).fillna(False)


def run(mode, R=5, phase=0):
    """mode: daily=매일 판정 / rebal=R일마다만 판정"""
    pos = 1.0; rets = []; trades = 0
    for i in range(len(b)):
        if mode == 'daily' or i % R == phase:
            newpos = 0.0 if S.iloc[i] else 1.0
            if newpos != pos: trades += 1
            pos = newpos
        rets.append(b.iloc[i] * pos)
    r = np.array(rets); nav = np.cumprod(1 + r)
    yrs = len(r) / 252
    cagr = (nav[-1] ** (1 / yrs) - 1) * 100
    mdd = (nav / np.maximum.accumulate(nav) - 1).min() * 100
    return cagr, mdd, cagr / abs(mdd), trades


if __name__ == '__main__':
    print('메모리 바스켓 10년 · 경보 시 현금')
    print('%-28s %9s %9s %8s %8s' % ('', 'CAGR', 'MDD', 'Calmar', '매매횟수'))
    print('-' * 66)
    c, m, k, t = run('daily'); print('%-28s %+8.1f%% %+8.1f%% %8.2f %8d' % ('경보 매일 판정(현행안)', c, m, k, t))
    for R in (5, 10):
        res = [run('rebal', R=R, phase=p) for p in range(R)]
        c = np.mean([x[0] for x in res]); m = np.mean([x[1] for x in res])
        t = np.mean([x[3] for x in res])
        print('%-28s %+8.1f%% %+8.1f%% %8.2f %8.0f' % ('경보 %d일 주기 판정' % R, c, m, c/abs(m), t))
    # 무시
    nav = np.cumprod(1 + b.values); yrs = len(b)/252
    c = (nav[-1]**(1/yrs)-1)*100; m = (nav/np.maximum.accumulate(nav)-1).min()*100
    print('%-28s %+8.1f%% %+8.1f%% %8.2f %8d' % ('경보 무시', c, m, c/abs(m), 0))
