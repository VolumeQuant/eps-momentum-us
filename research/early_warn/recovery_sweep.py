# -*- coding: utf-8 -*-
"""복귀 확인일(nx) 스윕 — US(26y/4약세) vs KR(ETF 2007~/약세5 but KOSDAQ부진).
진입은 3일 고정. '복귀도 3일이 맞나?' = US는 명확히 아니오(3일은 효과 죽음), KR은 표본부족/프록시부진으로 판별불가.
결론: US 복귀 15일 유지(10~25 robust plateau, 3~5 절벽). KR 3일최적은 약세 n≈1 과적합 의심."""
import sys
import numpy as np
import pandas as pd
from pathlib import Path
import harness as H
import candidates2 as C2
sys.stdout.reconfigure(encoding='utf-8')
_P = Path(__file__).resolve().parent


def _conf(frac, IDX, ne, nx, thr):
    raw = (frac < thr)
    st = False; sd = sb = 0; o = []
    for d in np.asarray(raw.reindex(IDX).fillna(False).values, dtype=bool):
        sd = sd + 1 if d else 0; sb = 0 if d else sb + 1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        o.append(st)
    return pd.Series(o, index=IDX)


print('=== [US] 진입3 고정, 복귀(nx) @ thr45/scale50 (base 전량방어와 결합) ===')
IDX = H.IDX; qr = H.D['qr']; base = H.base_regime(); frac = C2.sector_breadth_frac()
print('%8s%8s%8s%8s%8s' % ('복귀일', 'CAGR%', 'MDD%', 'Calmar', '전환'))
for nx in [3, 5, 8, 10, 15, 20, 25]:
    bdef = _conf(frac, IDX, 3, nx, 0.45)
    bd = base.reindex(IDX).fillna(False); half = bdef & ~bd
    w = pd.Series(1.0, index=IDX).where(~half, 0.5).where(~bd, 0.0)
    pos = w.shift(1, fill_value=1.0); r = pos * qr + (1 - pos) * H.NOTE
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    tr = int((bdef.astype(int).diff().abs() == 1).sum())
    print('%8d%+8.1f%+8.1f%8.2f%8d' % (nx, cagr * 100, mdd * 100, cagr / abs(mdd) if mdd < 0 else 0, tr))
print('기존(브레드스無): MDD -36.5 / Cal 0.36')
print('→ 3~5일 복귀: MDD -36.5(개선0)·Cal 0.34(기존보다↓) = 효과 죽음. 10~25 plateau(0.40~0.47). 15 유지 권고.')

print('\n=== [KR] KOSDAQ프록시, 13ETF브레드스, 진입3 고정, 복귀(nx) @ thr35/scale50 ===')
df = pd.read_parquet(_P / 'kr_sectors.parquet'); df.index = pd.to_datetime(df.index)
SEC = ['091160.KS', '091170.KS', '091180.KS', '102960.KS', '102970.KS', '117680.KS', '140710.KS',
       '139260.KS', '139250.KS', '244580.KS', '266370.KS', '228810.KS', '228800.KS']
kq = df['^KQ11'].dropna(); KIDX = kq.index; kqr = kq.pct_change().fillna(0)
avail = above = None
for tk in SEC:
    if tk not in df: continue
    s = df[tk].reindex(KIDX).ffill(); m = s.rolling(200).mean()
    a = (s > m); ok = s.notna() & m.notna()
    above = a.astype(float).where(ok, 0) if above is None else above.add(a.astype(float).where(ok, 0), fill_value=0)
    avail = ok.astype(float) if avail is None else avail.add(ok.astype(float), fill_value=0)
kfrac = (above / avail.replace(0, np.nan))
print('%8s%8s%8s%8s' % ('복귀일', 'CAGR%', 'MDD%', 'Calmar'))
for nx in [3, 5, 8, 10, 15, 20]:
    bdef = _conf(kfrac, KIDX, 3, nx, 0.35)
    w = pd.Series(1.0, index=KIDX).where(~bdef, 0.5); pos = w.shift(1, fill_value=1.0)
    r = pos * kqr + (1 - pos) * H.NOTE
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    print('%8d%+8.1f%+8.1f%8.2f' % (nx, cagr * 100, mdd * 100, cagr / abs(mdd) if mdd < 0 else 0))
print('→ KOSDAQ 자체 부진(CAGR~0/MDD-65%)으로 Calmar 전부 0.01~0.03=판별불가. 방향만 보면 15~20 미세우위(US와 동일).')
print('→ KR 시스템의 "복귀 3일 최적"은 약세 n≈1(2022) 과적합 의심: 복귀일은 표본에 가장 민감한 파라미터.')
