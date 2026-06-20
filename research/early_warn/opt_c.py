# -*- coding: utf-8 -*-
"""전문가 패널 반론 검증 — (1) 2000 제거시 붕괴하나 (2) 옵션C(스케일·falling·빠른퇴출)가 우월한가."""
import sys
import numpy as np
import pandas as pd
import harness as H
import candidates2 as C2
sys.stdout.reconfigure(encoding='utf-8')

IDX = H.IDX
qr = H.D['qr']
base = H.base_regime()
frac = C2.sector_breadth_frac()


def weighted_nav(base_def, breadth_def, scale=0.0, a=None, b=None):
    """base_def=full방어(w=0), breadth_def 단독=부분방어(w=scale), else w=1. (CAGR%,MDD%,Cal)."""
    base_def = base_def.reindex(IDX).fillna(False)
    breadth_def = breadth_def.reindex(IDX).fillna(False)
    w = pd.Series(1.0, index=IDX)
    w = w.where(~breadth_def, scale)
    w = w.where(~base_def, 0.0)
    pos = w.shift(1, fill_value=1.0)
    r = pos * qr + (1 - pos) * H.NOTE
    if a is not None:
        r = r[(r.index >= a) & (r.index < b)]
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else 0)


def yr_ret(base_def, breadth_def, scale, y0, y1):
    win = IDX[(IDX >= y0) & (IDX <= y1)]
    bd = base_def.reindex(IDX).fillna(False); brd = breadth_def.reindex(IDX).fillna(False)
    w = pd.Series(1.0, index=IDX).where(~brd, scale).where(~bd, 0.0).shift(1, fill_value=1.0)
    r = (w * qr + (1 - w) * H.NOTE).reindex(win)
    return ((1 + r).prod() - 1) * 100


def metrics_excl(base_def, breadth_def, scale, excl):
    bd = base_def.reindex(IDX).fillna(False); brd = breadth_def.reindex(IDX).fillna(False)
    w = pd.Series(1.0, index=IDX).where(~brd, scale).where(~bd, 0.0).shift(1, fill_value=1.0)
    r = w * qr + (1 - w) * H.NOTE
    a, b = excl
    r = r[~((IDX >= a) & (IDX <= b))]
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr / abs(mdd) if mdd < 0 else 0, mdd * 100


# breadth raw 변형
braw = (frac < 0.45)
braw_fall = (frac < 0.45) & (frac < frac.shift(20))  # 낮고 + 하락중
bdef_15 = H.conf(braw, 3, 15)            # 현행
bdef_fall = H.conf(braw_fall, 3, 15)     # falling 필터
bdef_fast = H.conf(braw, 3, 5)           # 빠른 퇴출

print('=== 후보들 (base 풀방어 + breadth 처리방식) ===')
print(f'{"변형":<34}{"CAGR%":>7}{"MDD%":>8}{"Cal":>6}{"2023%":>8}{"-2000 Cal/MDD":>16}')
variants = [
    ('기존 게이트(브레드스 無)', base, pd.Series(False, index=IDX), 0.0),
    ('A: 브레드스 binary 100%(현행배포)', base, bdef_15, 0.0),
    ('C1: 브레드스 50% 스케일', base, bdef_15, 0.5),
    ('C2: 50% + falling필터', base, bdef_fall, 0.5),
    ('C3: 100% + falling필터', base, bdef_fall, 0.0),
    ('C4: 50% + 빠른퇴출(nx5)', base, bdef_fast, 0.5),
    ('C5: 100% + 빠른퇴출(nx5)', base, bdef_fast, 0.0),
    ('C6: 50% + falling + nx5', base, H.conf(braw_fall, 3, 5), 0.5),
]
for nm, bd, brd, sc in variants:
    c, m, cal = weighted_nav(bd, brd, sc)
    r23 = yr_ret(bd, brd, sc, '2023-01-01', '2023-12-31')
    ex = metrics_excl(bd, brd, sc, ('2000-03-01', '2002-12-31'))
    print(f'{nm:<34}{c:>+7.1f}{m:>+8.1f}{cal:>6.2f}{r23:>+8.1f}{ex[0]:>9.2f}/{ex[1]:>+5.0f}')

print('\n참고: 기존 2023 +37.1% / QQQ 2023 +55.9%. -2000 = 닷컴(2000-03~2002) 제거 후 전체 Cal/MDD.')
print('판정: MDD↓ + Cal 비악화 + 2023 헛방어 작게 + (-2000)서도 edge 유지 = 우월.')
