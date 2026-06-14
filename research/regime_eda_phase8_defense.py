# -*- coding: utf-8 -*-
"""Phase 8a — 방어자산 비교: 약세장(defense)에 IEF(국채) vs BIL(초단기/현금성) vs 현금(2.5%).
2022 교훈(금리인상에 채권도 하락) 검증. 국면=SPX200d 15/15 + VIX36. 프록시 QQQ.
IEF 2002~, BIL 2007~ → 공통구간 2007~(GFC·COVID·2022 포함)."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
A = pd.read_parquet('research/regime_assets.parquet'); A.index = pd.to_datetime(A.index)
qqq, spx, vix, ief, bil = (A[c].dropna() for c in ['QQQ', '^GSPC', '^VIX', 'IEF', 'BIL'])

def confirm_asym(raw, ne, nx):
    st = False; sd = sb = 0; out = []
    for d in raw.values:
        sd = sd+1 if d else 0; sb = 0 if d else sb+1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        out.append(st)
    return pd.Series(out, index=raw.index)

def metrics(r):
    nav = (1+r).cumprod(); yrs = (nav.index[-1]-nav.index[0]).days/365.25
    cagr = nav.iloc[-1]**(1/yrs)-1; mdd = ((nav-nav.cummax())/nav.cummax()).min()
    vol = r.std()*np.sqrt(252); sh = (r.mean()*252)/vol if vol > 0 else 0
    return cagr*100, mdd*100, (cagr/abs(mdd) if mdd < 0 else 0), sh

def run(start, defense_asset):
    idx = qqq[qqq.index >= start].index
    q = qqq.reindex(idx).ffill(); s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill()
    ma = s.rolling(200).mean()
    defense = confirm_asym(s < ma, 15, 15) | confirm_asym(v > 36, 2, 2)
    pos_eq = (~defense).shift(1, fill_value=False)
    qr = q.pct_change().fillna(0)
    if defense_asset == 'cash':
        dr = pd.Series((1.025)**(1/252)-1, index=idx)
    else:
        a = (ief if defense_asset == 'IEF' else bil).reindex(idx).ffill()
        dr = a.pct_change().fillna(0)
    r = np.where(pos_eq.values, qr.values, dr.values)
    return pd.Series(r, index=idx), defense

print('=== 방어자산 비교 (국면 진입 시 무엇을 들고 있나, 주식100%) ===\n')
for start, lbl in [(pd.Timestamp('2007-06-01'), '2007~(GFC·COVID·2022)'), (pd.Timestamp('2002-08-01'), '2002~(IEF/현금만, BIL제외)')]:
    print(f'[{lbl}]')
    print(f'{"방어자산":<8}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"Sharpe":>8}')
    assets = ['현금', 'IEF', 'BIL'] if start.year == 2007 else ['현금', 'IEF']
    for a in assets:
        key = {'현금': 'cash', 'IEF': 'IEF', 'BIL': 'BIL'}[a]
        r, dfn = run(start, key); c, m, cal, sh = metrics(r)
        print(f'{a:<8}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{sh:>8.2f}')
    print()

# 2022 핀포인트: 방어구간 동안 IEF vs BIL vs 현금 실제 수익
print('=== 2022 방어구간 동안 방어자산 실제 성과 (채권도 빠졌나?) ===')
idx = qqq[(qqq.index >= '2022-01-01') & (qqq.index <= '2023-02-28')].index
s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill(); ma = s.rolling(200).mean()
# 2022 방어구간만
full_s = spx.reindex(qqq.index).ffill(); full_ma = full_s.rolling(200).mean(); full_v = vix.reindex(qqq.index).ffill()
dfn = (confirm_asym(full_s < full_ma, 15, 15) | confirm_asym(full_v > 36, 2, 2)).reindex(idx)
dseg = idx[dfn.values]
if len(dseg) > 1:
    for a, ser in [('IEF', ief), ('BIL', bil)]:
        x = ser.reindex(dseg).ffill().dropna()
        if len(x) > 1: print(f'  방어기간 {a}: {(x.iloc[-1]/x.iloc[0]-1)*100:+.1f}%  ({dseg[0].date()}~{dseg[-1].date()}, {len(dseg)}일)')
    print(f'  (현금이면 +0~2.5%/연. → 채권이 마이너스면 현금이 우월)')
print('\n해석: 전구간 Calmar/MDD가 비슷하면 무난하나, 2022 방어기간에 IEF가 마이너스면 "현금/BIL이 더 안전" 입증.')
