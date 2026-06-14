# -*- coding: utf-8 -*-
"""Phase 13 — 전문가 자문 2개 검증 (레드팀 게이트 적용).
A) VIX 텀구조(VIX/VIX3M) 진입가속기 (변동성 전문가)
B) binary × 연속 vol-스케일 곱 (CTA 전문가)
게이트(레드팀): 풀 Calmar + ★LOWO(2020·2025 빼고도 개선?) + 4대약세장 무손상 + 인접 + 거래비용.
윈도우 2007~2026(VIX3M 시작). proxy QQQ, 방어=발행어음3%."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
A = pd.read_parquet('research/regime_assets.parquet'); A.index = pd.to_datetime(A.index)
T = pd.read_parquet('research/regime_vix3m.parquet'); T.index = pd.to_datetime(T.index)
qqq, spx, vix = (A[c].dropna() for c in ['QQQ', '^GSPC', '^VIX'])
idx = qqq.index[qqq.index >= '2007-01-03']
q = qqq.reindex(idx).ffill(); s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill()
v3 = T['VIX3M'].reindex(idx).ffill(); ma = s.rolling(200).mean()
NOTE = (1.03)**(1/252)-1
sv, vv, v3v, mav = s.values, v.values, v3.values, ma.values
qr = q.pct_change().fillna(0)
# 변동성(실현 20일 연율화)
rv = (q.pct_change().rolling(20).std()*np.sqrt(252)).values

# V자 구간(LOWO 제외용)
VMASK = ~(((idx >= '2020-02-01') & (idx <= '2020-12-31')) | ((idx >= '2025-02-15') & (idx <= '2025-07-31')))

def regime(mode='base', ts_thr=1.05):
    n = len(idx); st = False; below = abovec = vspk = tsc = 0; out = np.zeros(n, bool)
    for i in range(n):
        b = sv[i] < mav[i] if not np.isnan(mav[i]) else False
        below = below+1 if b else 0; abovec = abovec+1 if not b else 0
        vspk = vspk+1 if vv[i] > 36 else 0
        ts_inv = (v3v[i] > 0 and vv[i]/v3v[i] > ts_thr)
        tsc = tsc+1 if ts_inv else 0
        enter = (below >= 15) or (vspk >= 2)
        if mode == 'ts' and (tsc >= 2 and b):  # 텀구조 진입가속(가격 200선 아래일때만)
            enter = True
        if not st and enter: st = True
        elif st and abovec >= 15: st = False
        out[i] = st
    return pd.Series(out, index=idx)

def pos_series(mode='base', ts_thr=1.05, target_vol=0.16):
    dfn = regime('ts' if mode == 'ts' else 'base', ts_thr)
    trend_on = (~dfn).astype(float)
    if mode == 'vol':  # binary × vol-scale
        vs = np.minimum(target_vol/np.where(rv > 0, rv, np.nan), 16.0/np.clip(vv, 12, None))
        vs = np.clip(np.nan_to_num(vs, nan=1.0), 0, 1.0)
        return (trend_on * pd.Series(vs, index=idx)).shift(1, fill_value=0)
    return trend_on.shift(1, fill_value=0)

def metrics(pos, cost=0.0, mask=None):
    r = pos.values * qr.values + (1-pos.values)*NOTE
    if cost:
        flips = np.abs(np.diff(pos.values, prepend=0))
        r = r - flips*cost
    r = pd.Series(r, index=idx)
    if mask is not None: r = r[mask]
    nav = (1+r).cumprod(); yrs = len(r)/252
    cagr = nav.iloc[-1]**(1/yrs)-1; mdd = ((nav-nav.cummax())/nav.cummax()).min()
    return cagr*100, mdd*100, (cagr/abs(mdd) if mdd < 0 else 0)

print('=== 전문가 2개 검증 (2007~2026, QQQ프록시) ===')
print(f'{"전략":<22}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"비용10bps후":>11}')
strat = {'현행(baseline)': pos_series('base'), 'A: VIX텀구조 진입가속': pos_series('ts'), 'B: binary×vol스케일': pos_series('vol')}
base = strat['현행(baseline)']
for nm, p in strat.items():
    c, m, cal = metrics(p); _, _, cal_cost = metrics(p, cost=0.001)
    print(f'{nm:<22}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{cal_cost:>11.2f}')

print('\n=== ★LOWO 게이트: 2020·2025 V자 빼고도 개선되나 (레드팀 핵심) ===')
print(f'{"전략":<22}{"전체Calmar":>10}{"V자제외Calmar":>13}')
for nm, p in strat.items():
    _, _, full = metrics(p); _, _, exv = metrics(p, mask=VMASK)
    print(f'{nm:<22}{full:>10.2f}{exv:>13.2f}')
print('  → A/B의 V자제외 Calmar가 baseline V자제외보다 높아야 robust. 같거나 낮으면 = 2020/2025 착시(기각).')

print('\n=== 인접안정성: VIX텀구조 임계 (A) ===')
for thr in [1.03, 1.05, 1.08, 1.10]:
    _, m, cal = metrics(pos_series('ts', ts_thr=thr)); _, _, exv = metrics(pos_series('ts', ts_thr=thr), mask=VMASK)
    print(f'  TS>{thr}: 전체Cal {cal:.2f} / V자제외 {exv:.2f} / MDD {m:.1f}%')

print('\n=== 4대 약세장 무손상 점검 (방어 진입 시점, 고점대비) ===')
for nm, mode in [('현행', 'base'), ('A:텀구조', 'ts')]:
    dfn = regime(mode); print(f' [{nm}]')
    for bn, (a, b) in {'2008': ('2008-06-01', '2009-01-31'), '2018Q4': ('2018-10-01', '2018-12-31'), '2020': ('2020-02-01', '2020-04-30'), '2022': ('2022-01-01', '2022-06-30'), '2025-4월': ('2025-02-15', '2025-05-31')}.items():
        seg = idx[(idx >= a) & (idx <= b)]; dseg = seg[dfn.reindex(seg).fillna(False).values]
        if len(dseg):
            pk = s[(s.index >= dseg[0]-pd.Timedelta(days=400)) & (s.index <= dseg[0])].max()
            print(f'   {bn:<8} 방어진입 {dseg[0].date()} (고점대비 {(s[dseg[0]]/pk-1)*100:+.1f}%)')
        else: print(f'   {bn:<8} 방어 미발동')
print('\n해석: A가 4대약세장 진입을 더 빨리(고점대비 덜빠졌을때) 하면서 LOWO·인접 통과하면 채택. LOWO 깨지면 기각.')
