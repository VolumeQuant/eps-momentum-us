# -*- coding: utf-8 -*-
"""HY-OAS 트로프-브레이크아웃 정밀 robust 검증 — 채택 전 의심.
핵심: NFCI처럼 2008만 잡는 mirage인가? leave-one-bear-out(특히 -2008,-2000) + OOS + 미세plateau + 확인일.
margin=0.85 a-priori 고정(plateau 0.75~1.0 중앙, best-cherry 회피)."""
import sys
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
import harness as H
from pathlib import Path

IDX = H.IDX
hy = pd.read_parquet(Path(H.__file__).resolve().parent.parent.parent / 'data_cache' / 'hy_spread.parquet', engine='fastparquet')
hy.index = pd.to_datetime(hy.index)
oas = hy['hy_spread'].reindex(IDX, method='ffill')
base = H.base_regime()


def gate(margin=0.85, ne=5, nx=20, twin=126):
    trough = oas.rolling(twin).min()
    raw = ((oas - trough) >= margin) & (oas > oas.shift(20))
    return (base | H.conf(raw.reindex(IDX).fillna(False), ne, nx)).reindex(IDX).fillna(False)


def metrics(dfn, a=None, b=None, excl=None):
    pos = (~dfn.reindex(IDX).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, H.NOTE), index=IDX)
    if a is not None:
        r = r[(r.index >= a) & (r.index < b)]
    if excl is not None:
        r = r[~((r.index >= excl[0]) & (r.index <= excl[1]))]
    if len(r) < 30:
        return 0, 0, 0
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cg = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cg * 100, mdd * 100, (cg / abs(mdd) if mdd < 0 else 0)

g = gate()
bm = metrics(base); gm = metrics(g)
print('전체: baseline Cal %.2f/MDD%.0f → HY_trough(0.85) Cal %.2f/MDD%.0f' % (bm[2], bm[1], gm[2], gm[1]))

print('\n=== [1] ★Leave-one-bear-out — 한 약세장 빼도 우위? (NFCI=2008만이었음) ===')
print(f'{"제외약세장":<14}{"base Cal/MDD":>16}{"HY Cal/MDD":>16}{"판정":>8}')
for nm, (a, b) in H.BEARS.items():
    bb = metrics(base, excl=(a, b)); gg = metrics(g, excl=(a, b))
    flag = '✓' if (gg[2] > bb[2] and gg[1] > bb[1] - 0.5) else '✗'
    print(f'{nm:<14}{bb[2]:>7.2f}/{bb[1]:>+6.0f}{gg[2]:>9.2f}/{gg[1]:>+6.0f}{flag:>8}')

print('\n=== [2] OOS 시기분할 (margin 0.85 고정) ===')
for nm, a, b in [('전반 99-12', '1999-01-01', '2013-01-01'), ('후반 13-26', '2013-01-01', '2026-07-01')]:
    bb = metrics(base, a, b); gg = metrics(g, a, b)
    print(f'  {nm}: base Cal {bb[2]:.2f}/MDD{bb[1]:+.0f} → HY Cal {gg[2]:.2f}/MDD{gg[1]:+.0f}')

print('\n=== [3] 미세 plateau (margin) + 확인일 ===')
print('  margin 미세:')
for m in [0.6, 0.7, 0.8, 0.9, 1.0, 1.1]:
    x = metrics(gate(margin=m))
    print(f'    {m}: Cal {x[2]:.2f} MDD {x[1]:+.0f}')
print('  확인일 (margin 0.85):')
for ne, nx in [(3, 15), (5, 15), (5, 20), (8, 20), (10, 30)]:
    x = metrics(gate(ne=ne, nx=nx))
    print(f'    ne{ne}/nx{nx}: Cal {x[2]:.2f} MDD {x[1]:+.0f}')

print('\n=== [4] LOWO(V-crash 제외) + breadth 비교 ===')
m = H.vmask()
def lowo_cal(dfn):
    pos = (~dfn.reindex(IDX).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, H.NOTE), index=IDX)[m.values]
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cg = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cg / abs(mdd) if mdd < 0 else 0
import candidates2 as C2, validate as V
breadth = V.combined(C2.sector_breadth_weak(0.45), 3, 15)
print(f'  HY_trough(0.85): LOWO Cal {lowo_cal(g):.2f} | breadth: LOWO {lowo_cal(breadth):.2f} | base {lowo_cal(base):.2f}')
print(f'  HY+breadth 결합: {metrics((g|breadth).reindex(IDX).fillna(False))}')
