# -*- coding: utf-8 -*-
"""B: NFCI(시카고연준 금융컨디션, FRED 1996~) 방어신호 검증. breadth 하네스(26년 프록시) 재사용.
⚠️ NFCI는 FRED 수정시계열 = look-ahead 편향(상한치). 통과해도 실시간 vintage 재검 필요. 못 통과하면 확실 기각.
신호: NFCI>0(평균보다 타이트) AND 상승(13주차 변화>0) → 방어. baseline/breadth에 추가해 증분가치 보나.
"""
import sys
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
import harness as H, validate as V, candidates2 as C2
from pathlib import Path

IDX = H.IDX
fred = pd.read_parquet(Path(__file__).resolve().parent / 'fred_macro.parquet')
fred.index = pd.to_datetime(fred.index)
nfci = fred['NFCI'].reindex(IDX, method='ffill')   # 주간→일 ffill
nfci13 = nfci - nfci.shift(63)                       # ~13주(63거래일) 변화


def nfci_raw(level=0.0, require_rising=True):
    r = (nfci > level)
    if require_rising:
        r = r & (nfci13 > 0)
    return r.reindex(IDX).fillna(False)


base = H.base_regime()
breadth = V.combined(C2.sector_breadth_weak(0.45), 3, 15)
print('NFCI 구간:', nfci.dropna().index[0].date(), '~', nfci.dropna().index[-1].date(),
      '| 결측 이전(1996전) 비율 %.0f%%' % (nfci.isna().mean() * 100))
print('⚠️ NFCI = FRED 수정시계열, look-ahead 상한치\n')

V.print_header()
# NFCI 단독 OR baseline
for lv in [0.0, 0.2]:
    for rise in [True, False]:
        d = (base | H.conf(nfci_raw(lv, rise), 5, 15)).reindex(IDX).fillna(False)
        V.compare(f'base+NFCI>{lv}{"&상승" if rise else ""}', d)
# NFCI를 breadth 위에 추가 (증분가치)
print('  -- breadth(현행 채택)에 NFCI 추가 시 증분 --')
V.compare('breadth(단독)', breadth)
for lv in [0.0, 0.2]:
    d = (breadth | H.conf(nfci_raw(lv, True), 5, 15)).reindex(IDX).fillna(False)
    V.compare(f'breadth+NFCI>{lv}&상승', d)

print('\n=== 약세장별 NFCI 단독 발동 시점 (선행성 확인) ===')
nd = H.conf(nfci_raw(0.0, True), 5, 15)
for nm, (a, b) in H.BEARS.items():
    seg = IDX[(IDX >= a) & (IDX < b)]
    if len(seg) == 0:
        continue
    on = nd.reindex(seg).fillna(False)
    first = on[on].index.min()
    print(f'  {nm:<14} NFCI방어 {"발동 "+str(first.date()) if pd.notna(first) else "미발동"} (커버 {on.mean()*100:.0f}%)')

print('\n=== 인접 민감도 (NFCI 레벨 임계) ===')
V.adjacency(lambda p: (base | H.conf(nfci_raw(p, True), 5, 15)).reindex(IDX).fillna(False),
            [-0.2, 0.0, 0.2, 0.4], 'NFCI_lvl')
print('\n참고 baseline: Cal 0.36 / MDD -36.5 / WFmin 0.34 / LOWO 0.37 / 늦음 -14.1')
print('breadth(현행): MDD -27.4 / Cal 0.44 — NFCI가 이걸 넘거나 추가가치 있어야 의미.')
