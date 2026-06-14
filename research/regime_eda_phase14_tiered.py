# -*- coding: utf-8 -*-
"""Phase 14 — 단계적 방어(50%→100%) 검증. 약한 신호=절반 매도, 심각 신호=전량.
가설: 휩쏘땐 절반만 빠져 손해↓ / 단 진짜 약세장은 절반만 빠져 더 맞음. 26년 어느쪽 우세?
게이트: 풀Calmar + LOWO(V자제외) + 4대약세장 MDD + 비용. proxy QQQ, 방어=발행어음3%."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
Aa = pd.read_parquet('research/regime_assets.parquet'); Aa.index = pd.to_datetime(Aa.index)
T = pd.read_parquet('research/regime_vix3m.parquet'); T.index = pd.to_datetime(T.index)
qqq, spx, vix = (Aa[c].dropna() for c in ['QQQ', '^GSPC', '^VIX'])
idx = qqq.index[qqq.index >= '1999-06-01']
q = qqq.reindex(idx).ffill(); s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill()
v3 = T['VIX3M'].reindex(idx).ffill(); ma = s.rolling(200).mean()
NOTE = (1.03)**(1/252)-1; qr = q.pct_change().fillna(0)
sv, vv, v3v, mav = s.values, v.values, v3.values, ma.values
VMASK = ~(((idx >= '2020-02-01') & (idx <= '2020-12-31')) | ((idx >= '2025-02-15') & (idx <= '2025-07-31')))

def conf_state(raw, ne, nx):
    st = False; sd = sb = 0; o = np.zeros(len(raw), bool)
    for i, d in enumerate(raw):
        sd = sd+1 if d else 0; sb = 0 if d else sb+1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        o[i] = st
    return o

below = sv < mav
vix36 = vv > 36
ts_inv = np.where(v3v > 0, vv/v3v > 1.05, False) & below
deep = sv < mav*0.92   # 200일선 8% 아래 = 깊은 약세

mild_ma = conf_state(below, 15, 15)
sev_vix = conf_state(vix36, 2, 2)
sev_deep = conf_state(deep, 5, 10)
sev_ts = conf_state(ts_inv, 2, 5)
mild_or = conf_state(below | vix36, 15, 15)
ma100 = s.rolling(100).mean().values
mild_early = conf_state(sv < ma100, 5, 10)   # MA100 조기 이탈(50%)

def pos_of(mild, severe):
    p = np.where(severe, 0.0, np.where(mild, 0.5, 1.0))
    return pd.Series(p, index=idx).shift(1, fill_value=1.0)

STRAT = {
    '현행 binary(100%)': pos_of(mild_or, mild_or),
    '단계1: 200선50%/VIX36전량': pos_of(mild_ma, sev_vix),
    '단계2: 약함50%/깊은하락전량': pos_of(mild_or, sev_deep),
    '단계3: 200선50%/VIX36·텀구조전량': pos_of(mild_ma, sev_vix | sev_ts),
    '단계4: MA100조기50%/200선전량(보호유지)': pos_of(mild_early, mild_ma),
}

def metrics(pos, cost=0.0, mask=None):
    r = pos.values*qr.values + (1-pos.values)*NOTE
    if cost: r = r - np.abs(np.diff(pos.values, prepend=1.0))*cost
    r = pd.Series(r, index=idx)
    if mask is not None: r = r[mask]
    nav = (1+r).cumprod(); yrs = len(r)/252
    cagr = nav.iloc[-1]**(1/yrs)-1; mdd = ((nav-nav.cummax())/nav.cummax()).min()
    return cagr*100, mdd*100, (cagr/abs(mdd) if mdd < 0 else 0)

print('=== 단계적 방어 검증 (1999~2026, QQQ프록시) ===')
print(f'{"전략":<28}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"비용후":>8}{"V자제외Cal":>10}')
for nm, p in STRAT.items():
    c, m, cal = metrics(p); _, _, cc = metrics(p, cost=0.001); _, _, exv = metrics(p, mask=VMASK)
    print(f'{nm:<28}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{cc:>8.2f}{exv:>10.2f}')

print('\n=== 4대 약세장 MDD (단계적이 절반만 빠져 더 맞나?) ===')
bears = {'2000닷컴': ('2000-09-01', '2002-10-15'), '2008GFC': ('2007-10-01', '2009-03-15'),
         '2020COVID': ('2020-02-15', '2020-04-15'), '2022': ('2022-01-01', '2022-10-20')}
print(f'{"전략":<28}' + ''.join(f'{b:>11}' for b in bears))
for nm, p in STRAT.items():
    row = f'{nm:<28}'
    for b, (a, e) in bears.items():
        seg = idx[(idx >= a) & (idx <= e)]
        r = pd.Series(p.values*qr.values + (1-p.values)*NOTE, index=idx).reindex(seg)
        nav = (1+r).cumprod(); mdd = ((nav-nav.cummax())/nav.cummax()).min()*100
        row += f'{mdd:>+10.1f}%'
    print(row)
print('\n해석: 단계적이 풀Calmar·V자제외Cal 둘다 binary 이상이면서 4대약세장 MDD 크게 안나빠지면 채택. 약세장 MDD가 확 커지면 = 절반방어의 대가(기각).')
