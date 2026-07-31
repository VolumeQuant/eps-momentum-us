# -*- coding: utf-8 -*-
"""메모리 경보 — 더 정확한 신호가 있나 (2026-07-31, 사용자 질문)
 ① 임계 K 스윕 (몇 종목 꺾이면 발동): 정밀도/재현율 trade-off
 ② 후보 종목 구성 변경: STX(HDD)·SNDK(짧은 이력) 제외, DRAM 순수 등
판정 = 정밀도(발동 중 실제 −20% 급락 비율) · 재현율(급락기에 켜져있던 비율) ·
       조건부 성과(ON/OFF 메모리 수익 차) · 에피소드 수(휩쏘)
"""
import sys, os
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import yfinance as yf

ALL = ['MU', 'SNDK', 'WDC', 'STX', '005930.KS', '000660.KS']
PX = yf.download(ALL, period='10y', progress=False, auto_adjust=True, threads=2)['Close'].ffill()
print('데이터 시작일:', {t: str(PX[t].first_valid_index())[:10] for t in ALL})
# 벤치마크 바스켓 = DRAM/NAND 순수 3종(전 기간 존재) 동일가중
BENCH = PX[['MU', '005930.KS', '000660.KS']].pct_change().mean(axis=1)


def evaluate(names, K, MA=90, NE=1, NX=5):
    px = PX[names]
    ma = px.rolling(MA).mean()
    ok = px.notna() & ma.notna()
    nb = (px.lt(ma) & ok).sum(axis=1)
    valid = ok.sum(axis=1)
    raw = (nb >= K) & (valid >= max(3, K))
    fire = raw.rolling(NE).sum() == NE
    off = (~raw).rolling(NX).sum() == NX
    on = False; st = []
    for d in raw.index:
        if not on and fire.loc[d]: on = True
        elif on and off.loc[d]: on = False
        st.append(on)
    S = pd.Series(st, index=raw.index)
    S = S[valid >= max(3, K)]
    b = BENCH.reindex(S.index).fillna(0)
    nav = (1 + b).cumprod()
    dd = nav / nav.cummax() - 1
    eps = []; a = None
    for i, v in enumerate(S.values):
        if v and a is None: a = i
        elif not v and a is not None: eps.append((a, i)); a = None
    if a is not None: eps.append((a, len(S) - 1))
    hits = 0
    for x, y in eps:
        seg = nav.iloc[x:y + 1]
        if (seg / seg.cummax() - 1).min() <= -0.20: hits += 1
    crisis = dd <= -0.20
    return dict(n_ep=len(eps), prec=hits / len(eps) * 100 if eps else 0,
                rec=S[crisis].mean() * 100 if crisis.sum() else 0,
                on_pct=S.mean() * 100,
                on_ret=b[S].mean() * 252 * 100, off_ret=b[~S].mean() * 252 * 100)


if __name__ == '__main__':
    print('\n=== ① 임계 K 스윕 (현행 6종, MA90) ===')
    print('%-6s %7s %8s %8s %8s %10s %10s' % ('K', '에피소드', '정밀도', '재현율', 'ON비율', 'ON수익', 'OFF수익'))
    for K in (2, 3, 4, 5):
        r = evaluate(ALL, K)
        print('%-6s %7d %7.0f%% %7.0f%% %7.0f%% %+9.1f%% %+9.1f%%'
              % ('%d/6' % K, r['n_ep'], r['prec'], r['rec'], r['on_pct'], r['on_ret'], r['off_ret']))

    print('\n=== ② 후보 종목 구성 (K는 비율 유지: 절반 이상) ===')
    variants = [
        ('현행 6종 (3/6)', ALL, 3),
        ('−STX (HDD 제외) 5종 (3/5)', ['MU', 'SNDK', 'WDC', '005930.KS', '000660.KS'], 3),
        ('−STX −WDC 4종 (2/4)', ['MU', 'SNDK', '005930.KS', '000660.KS'], 2),
        ('DRAM 순수 3종 (2/3)', ['MU', '005930.KS', '000660.KS'], 2),
        ('미국만 4종 (2/4)', ['MU', 'SNDK', 'WDC', 'STX'], 2),
        ('한국만 2종 (2/2)', ['005930.KS', '000660.KS'], 2),
    ]
    print('%-26s %7s %8s %8s %8s %10s %10s' % ('구성', '에피소드', '정밀도', '재현율', 'ON비율', 'ON수익', 'OFF수익'))
    for lbl, nm, K in variants:
        r = evaluate(nm, K)
        print('%-26s %7d %7.0f%% %7.0f%% %7.0f%% %+9.1f%% %+9.1f%%'
              % (lbl, r['n_ep'], r['prec'], r['rec'], r['on_pct'], r['on_ret'], r['off_ret']))
