# -*- coding: utf-8 -*-
"""Phase 9 — 최종 정책 맹점/견고성: 재진입(인접·WF) + 방어자산(약세장별).
production 채택기준(WF안정+인접CV<0.3+약세장 사고없음) 적용."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
A = pd.read_parquet('research/regime_assets.parquet'); A.index = pd.to_datetime(A.index)
qqq, spx, vix, ief, bil = (A[c].dropna() for c in ['QQQ', '^GSPC', '^VIX', 'IEF', 'BIL'])
idx = qqq.index
q = qqq; s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill(); ma = s.rolling(200).mean()
qr = q.pct_change().fillna(0)

def conf(raw, ne, nx):
    st = False; sd = sb = 0; o = []
    for d in raw.values:
        sd = sd+1 if d else 0; sb = 0 if d else sb+1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        o.append(st)
    return pd.Series(o, index=raw.index)
vixdef = conf(v > 36, 2, 2)

def cal_seg(reentry, a=None, b=None):
    dfn = conf(s < ma, 15, reentry) | vixdef
    pos = (~dfn).shift(1, fill_value=False)
    note = (1.03)**(1/252)-1
    r = pd.Series(np.where(pos.values, qr.values, note), index=idx)
    if a is not None: r = r[(r.index >= a) & (r.index < b)]
    nav = (1+r).cumprod(); yrs = (nav.index[-1]-nav.index[0]).days/365.25
    cagr = nav.iloc[-1]**(1/yrs)-1; mdd = ((nav-nav.cummax())/nav.cummax()).min()
    return (cagr/abs(mdd) if mdd < 0 else 0), mdd*100

print('=== ① 재진입 인접안정성 (전체 1999~2026) ===')
print(f'{"재진입":>6}{"Calmar":>8}{"MDD%":>8}')
cals = {}
for nx in [8, 10, 12, 15, 18, 20]:
    c, m = cal_seg(nx); cals[nx] = c
    print(f'{nx:>4}일{c:>8.2f}{m:>+8.1f}')
arr = np.array(list(cals.values()))
print(f'  인접(8~20일) CV = {arr.std()/arr.mean():.3f}  (<0.30이면 plateau=robust)')

print('\n=== ② 재진입 walk-forward (3블록서 각각 최적 재진입) ===')
blocks = [('1999-2008', '1999-01-01', '2008-01-01'), ('2008-2016', '2008-01-01', '2016-01-01'), ('2016-2026', '2016-01-01', '2026-07-01')]
print(f'{"블록":<12}{"최적재진입":>10}{"15일Cal":>9}{"최적Cal":>9}')
for nm, a, b in blocks:
    res = {nx: cal_seg(nx, pd.Timestamp(a), pd.Timestamp(b))[0] for nx in [8, 10, 12, 15, 18, 20]}
    best = max(res, key=res.get)
    print(f'{nm:<12}{best:>8}일{res[15]:>9.2f}{res[best]:>9.2f}')
print('  → 각 블록 최적이 15 근처(8~20)면 WF 안정. 15가 매 블록 최적과 큰 차 없으면 채택OK.')

print('\n=== ③ 방어자산: 약세장별 IEF vs 발행어음3% (2022만의 우연인가) ===')
note_cum = lambda days: (1.03)**(days/252)-1
bears = {'2008GFC': ('2008-06-01', '2009-06-30'), '2018Q4': ('2018-10-01', '2018-12-31'),
         '2020COVID': ('2020-02-20', '2020-04-30'), '2022': ('2022-01-01', '2023-02-28'),
         '2025-4월': ('2025-02-15', '2025-05-31')}
dfn_full = conf(s < ma, 15, 15) | vixdef
print(f'{"약세장":<10}{"방어일수":>7}{"IEF수익":>9}{"발행어음":>9}{"승자":>8}')
for nm, (a, b) in bears.items():
    seg = idx[(idx >= a) & (idx < b)]
    dseg = seg[dfn_full.reindex(seg).fillna(False).values]
    if len(dseg) < 2: print(f'{nm:<10}{"방어0일(국면미발동)":>20}'); continue
    x = ief.reindex(dseg).ffill().dropna()
    ief_ret = (x.iloc[-1]/x.iloc[0]-1)*100 if len(x) > 1 else 0
    nr = note_cum(len(dseg))*100
    win = '발행어음' if nr > ief_ret else 'IEF'
    print(f'{nm:<10}{len(dseg):>6}일{ief_ret:>+8.1f}%{nr:>+8.1f}%{win:>8}')
print('  → 발행어음이 대부분 약세장서 IEF 이상이면 "2022만의 우연 아님" = 구조적 우월.')
