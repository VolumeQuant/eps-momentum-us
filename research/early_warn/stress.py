# -*- coding: utf-8 -*-
"""sector_breadth_weak 정밀 스트레스 — 채택 전 정직 검증.
OOS train/test + leave-one-bear-out + 증분가치(2000갭) + 확인일 robust + 룩어헤드 점검."""
import sys
import numpy as np
import pandas as pd
import harness as H
import validate as V
import candidates2 as C2
sys.stdout.reconfigure(encoding='utf-8')

IDX = H.IDX
THR = 0.45  # plateau 중앙, a-priori 고정


def cand(thr=THR, ne=3, nx=15):
    return V.combined(C2.sector_breadth_weak(thr), ne=ne, nx=nx)


def cal_window(dfn, a, b):
    return H.ev(dfn, pd.Timestamp(a), pd.Timestamp(b))


print('=== [1] 증분가치 증명: 2000 닷컴 -36% 갭에서 breadth가 base보다 먼저 방어했나 ===')
base = H.base_regime()
br = cand()
win = IDX[(IDX >= '2000-01-01') & (IDX < '2000-07-01')]
sub = pd.DataFrame({'base': base.reindex(win), 'breadth': br.reindex(win)})
first_base = sub[sub['base']].index.min()
first_br = sub[sub['breadth']].index.min()
print(f'  base 최초 방어진입: {first_base.date() if pd.notna(first_base) else "—(이 구간 방어 0)"}')
print(f'  breadth 최초 방어진입: {first_br.date() if pd.notna(first_br) else "—"}')
q = H.D['qqq']
if pd.notna(first_br):
    peak = q.loc[:first_br].loc["2000-01-01":].max()
    print(f'  → breadth는 고점대비 {(q.loc[first_br]/peak-1)*100:+.1f}%에서 진입 (base는 이 구간 미발화)')

print('\n=== [2] OOS train/test (임계는 train서 고르고 test에 적용) ===')
HALVES = [('전반 1999-2012', '1999-01-01', '2013-01-01'), ('후반 2013-2026', '2013-01-01', '2026-07-01')]
for trn, hold in [(HALVES[0], HALVES[1]), (HALVES[1], HALVES[0])]:
    # train서 best thr
    best = None
    for thr in [0.35, 0.40, 0.45, 0.50, 0.55]:
        c = cal_window(cand(thr), trn[1], trn[2])[2]
        if best is None or c > best[1]:
            best = (thr, c)
    thr = best[0]
    bc = cal_window(base, hold[1], hold[2])
    cc = cal_window(cand(thr), hold[1], hold[2])
    print(f'  train={trn[0]} → best thr={thr} | HOLDOUT {hold[0]}: '
          f'base Cal {bc[2]:.2f}/MDD{bc[1]:+.0f}  breadth Cal {cc[2]:.2f}/MDD{cc[1]:+.0f}')
# 고정 thr=0.45도 각 반에서
print('  [고정 thr=0.45 각 반기]')
for nm, a, b in HALVES:
    bc = cal_window(base, a, b); cc = cal_window(cand(0.45), a, b)
    print(f'    {nm}: base Cal {bc[2]:.2f}/MDD{bc[1]:+.0f}  breadth Cal {cc[2]:.2f}/MDD{cc[1]:+.0f}')

print('\n=== [3] Leave-one-bear-out: 한 약세장 제외해도 MDD개선 유지되나 ===')
# 각 bear 구간을 수익률에서 제외하고 전체 Calmar/MDD 재계산
def metrics_exclude(dfn, excl):
    pos = (~dfn.reindex(IDX).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, H.NOTE), index=IDX)
    mask = pd.Series(True, index=IDX)
    a, b = excl
    mask &= ~((IDX >= a) & (IDX <= b))
    r = r[mask.values]
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return cagr / abs(mdd) if mdd < 0 else 0, mdd * 100
for nm, (a, b) in H.BEARS.items():
    bc = metrics_exclude(base, (a, b)); cc = metrics_exclude(cand(), (a, b))
    flag = '✓' if cc[1] > bc[1] + 0.3 else ('=' if abs(cc[1]-bc[1]) <= 0.3 else '✗MDD악화')
    print(f'  -{nm:<14} base Cal {bc[0]:.2f}/MDD{bc[1]:+.1f}  breadth Cal {cc[0]:.2f}/MDD{cc[1]:+.1f}  {flag}')

print('\n=== [4] 확인일(ne/nx) robust ===')
for ne in (2, 3, 5):
    rowtxt = f'  ne={ne}: '
    for nx in (10, 15, 20):
        m = H.metrics(cand(ne=ne, nx=nx))
        rowtxt += f'nx{nx}[Cal{m["calmar"]:.2f}/MDD{m["mdd"]:+.0f}] '
    print(rowtxt)

print('\n=== [5] 룩어헤드 점검: breadth는 t시점 종가까지만 사용(각 섹터 자기200DMA, 1일지연 ev) ===')
print('  - sector_breadth_frac: 각 섹터 rolling(200) = 후행평균, 미래미사용')
print('  - conf(): 과거→현재 순차 상태기계, 미래미사용')
print('  - ev(): pos = (~defense).shift(1) = 신호 1일 지연 적용')
print('  - XLRE(2015)/XLC(2018) 중도편입: avail 분모 동적, 편입 전 미포함(생존편향 아님)')

print('\n=== [6] per-bear 전략 MDD (구간내 NAV 최대낙폭) ===')
def bear_mdd(dfn, a, b):
    seg = IDX[(IDX >= a) & (IDX < b)]
    pos = (~dfn.reindex(IDX).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, H.NOTE), index=IDX).reindex(seg)
    nav = (1 + r).cumprod()
    return ((nav - nav.cummax()) / nav.cummax()).min() * 100
print(f'{"약세장":<16}{"base MDD":>10}{"breadth MDD":>14}')
for nm, (a, b) in H.BEARS.items():
    print(f'{nm:<16}{bear_mdd(base,a,b):>+10.1f}{bear_mdd(cand(),a,b):>+14.1f}')
