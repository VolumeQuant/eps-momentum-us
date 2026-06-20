# -*- coding: utf-8 -*-
"""Baseline 측정 — 이겨야 할 기준선. production 게이트의 Calmar/CAGR/MDD + 약세장 포착 + lateness."""
import sys
import pandas as pd
import harness as H
sys.stdout.reconfigure(encoding='utf-8')

idx = H.IDX
print(f'데이터 구간: {idx[0].date()} ~ {idx[-1].date()}  (QQQ 프록시 {len(idx)}일)')

# 1) Buy&Hold QQQ vs 게이트
import numpy as np
bh = pd.Series(False, index=idx)  # 항상 boost
print('\n=== [1] Buy&Hold QQQ vs production 게이트 (방어=발행어음3%) ===')
print(f'{"전략":<26}{"CAGR%":>8}{"MDD%":>9}{"Calmar":>8}{"Sharpe":>8}{"전환":>6}{"방어일%":>8}')
for nm, d in [('Buy&Hold QQQ', bh), ('게이트 MA200/15/15+VIX36', H.base_regime())]:
    m = H.metrics(d)
    print(f'{nm:<26}{m["cagr"]:>+8.1f}{m["mdd"]:>+9.1f}{m["calmar"]:>8.2f}{m["sharpe"]:>8.2f}'
          f'{m["transitions"]:>6}{m["defense_frac"]*100:>8.1f}')

# 2) Walk-forward 3블록
print('\n=== [2] Walk-forward 3블록 Calmar (게이트) ===')
d = H.base_regime()
for nm, c in H.wf_each(d).items():
    print(f'  {nm}: {c:.2f}')
print(f'  WF-min: {H.wf_min(d):.2f}')

# 3) LOWO (V-crash 2020/2025 제외)
print('\n=== [3] LOWO: V-crash(2020/2025) 제외 Calmar ===')
m = H.vmask()
for nm, d in [('Buy&Hold', bh), ('게이트', H.base_regime())]:
    dd = d.copy()
    pos = (~dd.reindex(idx).fillna(False)).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, H.D['qr'].values, H.NOTE), index=idx)[m.values]
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    print(f'  {nm:<12} V-free Calmar {cagr/abs(mdd):.2f}  (CAGR{cagr*100:+.1f} MDD{mdd*100:+.1f})')

# 4) 약세장 포착 + 커버리지
print('\n=== [4] 약세장 포착 (O/X, 방어 커버리지%) ===')
caps = H.capture(H.base_regime())
for nm, (ox, cov) in caps.items():
    print(f'  {nm:<16} {ox}  ({cov:.0f}%)')
print('  (앞 6개 O=진짜약세장 포착 / 2025-4월 X 이상적)')

# 5) Lateness — 핵심: 고점대비 몇 % 빠진 뒤 방어로 가나
print('\n=== [5] LATENESS: 각 방어 진입이 고점대비 몇 % 빠진 뒤인가 ===')
lt = H.lateness(H.base_regime())
# 주요 에피소드만 (5%+ 회피 또는 20일+ 지속)
major = lt[(lt['avoided'] <= -5) | (lt['dur'] >= 20)].copy()
print(f'{"진입일":<12}{"직전고점":<12}{"고점대비%":>10}{"지속일":>7}{"추가회피%":>10}')
for _, row in major.iterrows():
    print(f'{str(row["entry"]):<12}{str(row["peak"]):<12}{row["dd_at_entry"]:>+10.1f}{row["dur"]:>7}{row["avoided"]:>+10.1f}')
print(f'\n  전체 진입 {len(lt)}회 중 주요 {len(major)}회. '
      f'평균 진입 시 고점대비 {lt["dd_at_entry"].mean():+.1f}%, 중앙값 {lt["dd_at_entry"].median():+.1f}%')
print(f'  → "늦음"의 정량: 방어 전환 시 이미 평균 {lt["dd_at_entry"].mean():+.1f}% 빠진 상태. '
      f'이 수치를 줄이는(0에 가깝게) 게 조기경보 목표.')
