# -*- coding: utf-8 -*-
"""Phase 10 — 국면 전환기준 종합 최적화(심장). 강세→약세 / 약세→강세 진입·재진입 기준.
QQQ 프록시 26년, 방어=발행어음3%, 주식100%(국면 신호만 분리). WF(3블록)+인접+약세장 포착.
production 채택기준: 풀Calmar + WF최소 + 인접CV<0.3 + 4대약세장 포착 + 휩쏘 최소."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
A = pd.read_parquet('research/regime_assets.parquet'); A.index = pd.to_datetime(A.index)
qqq, spx, vix = (A[c].dropna() for c in ['QQQ', '^GSPC', '^VIX'])
idx = qqq.index; s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill(); qr = qqq.pct_change().fillna(0)
NOTE = (1.03)**(1/252)-1
def conf(raw, ne, nx):
    st = False; sd = sb = 0; o = []
    for d in raw.values:
        sd = sd+1 if d else 0; sb = 0 if d else sb+1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        o.append(st)
    return pd.Series(o, index=idx)
def regime(ma_p=200, ec=15, rc=15, vix_thr=36, death=False):
    if death:
        ma_s = s.rolling(50).mean(); ma_l = s.rolling(ma_p).mean(); raw = ma_s < ma_l
    else:
        raw = s < s.rolling(ma_p).mean()
    d = conf(raw, ec, rc)
    if vix_thr: d = d | conf(v > vix_thr, 2, 2)
    return d
def ev(dfn, a=None, b=None):
    pos = (~dfn).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, qr.values, NOTE), index=idx)
    if a is not None: r = r[(r.index >= a) & (r.index < b)]
    if len(r) < 30: return 0, 0, 0
    nav = (1+r).cumprod(); yrs = (nav.index[-1]-nav.index[0]).days/365.25
    cagr = nav.iloc[-1]**(1/yrs)-1; mdd = ((nav-nav.cummax())/nav.cummax()).min()
    return cagr*100, mdd*100, (cagr/abs(mdd) if mdd < 0 else 0)
BLK = [('99-08', '1999-01-01', '2008-01-01'), ('08-16', '2008-01-01', '2016-01-01'), ('16-26', '2016-01-01', '2026-07-01')]
def wf_min(dfn):
    return min(ev(dfn, pd.Timestamp(a), pd.Timestamp(b))[2] for _, a, b in BLK)
BEARS = {'2000닷컴': ('2000-09-01', '2001-04-01'), '2008GFC': ('2008-09-01', '2009-03-01'),
         '2020COVID': ('2020-02-20', '2020-04-15'), '2022': ('2022-04-01', '2022-10-15'),
         '2018Q4': ('2018-11-01', '2018-12-24'), '2025-4월(휩쏘)': ('2025-03-25', '2025-04-25')}
def capture(dfn):
    out = []
    for nm, (a, b) in BEARS.items():
        seg = idx[(idx >= a) & (idx < b)]
        if len(seg) == 0: out.append('·'); continue
        out.append('O' if dfn.reindex(seg).fillna(False).any() else 'X')
    return out

print('=== [A] 진입(ec) × 재진입(rc) 그리드 @ MA200+VIX36 (풀Calmar / WF최소Calmar) ===')
print('  ec=강세→약세 확인일, rc=약세→강세 확인일')
print(f'{"ec\\\\rc":>7}' + ''.join(f'{rc:>10}' for rc in [10, 15, 20]))
for ec in [10, 15, 20]:
    row = f'{ec:>7}'
    for rc in [10, 15, 20]:
        d = regime(200, ec, rc, 36); _, _, cal = ev(d); row += f'{cal:>6.2f}/{wf_min(d):>.2f}'
    print(row)

print('\n=== [B] 최적 근처서 MA기간·VIX임계·데드크로스 민감도 (ec15/rc15 기준) ===')
print(f'{"구성":<22}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"WF최소":>8}')
configs = [('MA200+VIX36 (현행)', dict(ma_p=200)), ('MA150+VIX36', dict(ma_p=150)), ('MA250+VIX36', dict(ma_p=250)),
           ('MA200+VIX30', dict(vix_thr=30)), ('MA200+VIX40', dict(vix_thr=40)), ('MA200 (VIX없음)', dict(vix_thr=0)),
           ('데드크로스50/200+VIX36', dict(death=True))]
for nm, kw in configs:
    d = regime(ec=15, rc=15, **{**dict(ma_p=200, vix_thr=36), **kw})
    c, m, cal = ev(d); print(f'{nm:<22}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{wf_min(d):>8.2f}')

print('\n=== [C] 약세장 포착 + 휩쏘 (현행 MA200/15/15/VIX36) ===')
d = regime(200, 15, 15, 36)
caps = capture(d)
print('  ' + '  '.join(f'{nm}:{c}' for nm, (c) in zip(BEARS.keys(), caps)))
print('  (앞4개 O=진짜약세장 포착 / 2025-4월 X=얕은dip 안잡음이 이상적)')
print('\n해석: 풀Calmar 높고 WF최소>0이며 약세장 4개 O·2025 X면 = robust 최적. 인접(ec/rc ±5)도 비슷하면 과적합 아님.')
