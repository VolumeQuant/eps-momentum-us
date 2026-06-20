# -*- coding: utf-8 -*-
"""Tier-1/2 후보 전체 검증 + 인접 스윕. baseline 대비 정직 판정."""
import sys
import numpy as np
import pandas as pd
import harness as H
import validate as V
import candidates2 as C2
sys.stdout.reconfigure(encoding='utf-8')

print('=' * 96)
print('OVERLAY 모드 (base 게이트에 OR, 후보 ne=3/nx=15 확인)')
print('=' * 96)
b = V.print_header()
for nm, fn in C2.OVERLAY.items():
    V.compare(f'+{nm}', V.combined(fn(), ne=3, nx=15))

print('\n' + '=' * 96)
print('REPLACE-MA 모드 (MA200 leg를 후보 추세로 교체, VIX36 유지, ne=15/nx=15)')
print('=' * 96)
V._pr(V.row('BASELINE (현행)', V._BASE))
for K in (4, 5, 6):
    V.compare(f'ensemble K={K}/7', V.replace_ma(C2.trend_ensemble(K), 15, 15))
V.compare('absmom12 (price<1yr ago)', V.replace_ma((H.D['spx'] / H.D['spx'].shift(252) - 1 < 0), 15, 15))

print('\n' + '=' * 96)
print('GTT: 추세(가격<MA200,15일) AND 곡선역전이력 → 방어 (휩쏘 억제용 AND-게이트)')
print('=' * 96)
V._pr(V.row('BASELINE (현행)', V._BASE))
ma_raw = H.D['spx'] < H.D['spx'].rolling(200).mean()
armed = C2.gtt_macro_armed(504)
gtt = H.conf((ma_raw & armed), 15, 15) | H.conf(H.D['vix'] > 36, 2, 2)
V.compare('GTT trend&curve', gtt.reindex(H.IDX).fillna(False))

print('\n' + '=' * 96)
print('인접 스윕 (robust plateau 확인 — 단일임계 호조는 착시)')
print('=' * 96)
print('\n[A] 수익률곡선 dis-inversion margin(%p):')
V.adjacency(lambda p: V.combined(C2.ycurve_disinversion(p, 378), 3, 15), [0.10, 0.20, 0.25, 0.35, 0.50], 'margin')
print('\n[B] 방어/경기 로테이션 ROC 임계:')
V.adjacency(lambda p: V.combined(C2.defensive_cyclical(p, 63), 3, 15), [0.02, 0.04, 0.05, 0.07, 0.10], 'thr')
print('\n[C] 섹터 브레드스 임계(비율):')
V.adjacency(lambda p: V.combined(C2.sector_breadth_weak(p), 3, 15), [0.35, 0.40, 0.45, 0.50, 0.55], 'frac')
print('\n[D] 추세 앙상블 K (replace-MA):')
V.adjacency(lambda p: V.replace_ma(C2.trend_ensemble(p), 15, 15), [3, 4, 5, 6, 7], 'K')

print('\n참고 baseline: Cal 0.36 / MDD -36.5 / WFmin 0.34 / LOWO 0.37 / 늦음 -14.1 / 전환 30')
