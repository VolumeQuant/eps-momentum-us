# -*- coding: utf-8 -*-
"""B(진짜): HY-OAS 변화율/트로프-브레이크아웃 방어신호. data_cache/hy_spread.parquet(1996~, 30년 PIT).
HY-OAS는 시장가격(수정無)=look-ahead 없음. 신용은 주식보다 먼저 리프라이싱(선행). breadth 하네스 26년 검증.
신호: ①3개월변화 ≥+Xbp ②6개월저점대비 +margin 상승(트로프-브레이크아웃). baseline/breadth 대비 + per-bear 선행성.
"""
import sys
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
import harness as H, validate as V, candidates2 as C2
from pathlib import Path

IDX = H.IDX
hy = pd.read_parquet(Path(H.__file__).resolve().parent.parent.parent / 'data_cache' / 'hy_spread.parquet', engine='fastparquet')
hy.index = pd.to_datetime(hy.index)
oas = hy['hy_spread'].reindex(IDX, method='ffill')
print('HY-OAS:', hy.index.min().date(), '~', hy.index.max().date(), '| QQQ구간 정렬 결측 %.0f%%' % (oas.isna().mean()*100))


def hy_roc(thr=1.0, win=63):
    """3개월(63d) OAS 변화 ≥ thr%p."""
    return ((oas - oas.shift(win)) >= thr).reindex(IDX).fillna(False)


def hy_trough(margin=1.0, twin=126):
    """6개월 저점 대비 +margin%p 상승 AND 20일 상승중(트로프-브레이크아웃)."""
    trough = oas.rolling(twin).min()
    return (((oas - trough) >= margin) & (oas > oas.shift(20))).reindex(IDX).fillna(False)


base = H.base_regime()
breadth = V.combined(C2.sector_breadth_weak(0.45), 3, 15)
V.print_header()
print('  -- HY-OAS 3개월 변화율(ROC) baseline에 OR --')
for thr in [0.75, 1.0, 1.5]:
    V.compare(f'base+HY_roc≥{thr}', (base | H.conf(hy_roc(thr), 5, 20)).reindex(IDX).fillna(False))
print('  -- HY-OAS 트로프-브레이크아웃 baseline에 OR --')
for m in [0.75, 1.0, 1.25]:
    V.compare(f'base+HY_trough≥{m}', (base | H.conf(hy_trough(m), 5, 20)).reindex(IDX).fillna(False))
print('  -- breadth(현행)에 HY 추가 (증분가치) --')
V.compare('breadth(단독)', breadth)
V.compare('breadth+HY_roc≥1.0', (breadth | H.conf(hy_roc(1.0), 5, 20)).reindex(IDX).fillna(False))
V.compare('breadth+HY_trough≥1.0', (breadth | H.conf(hy_trough(1.0), 5, 20)).reindex(IDX).fillna(False))

print('\n=== 약세장별 HY-OAS 발동 시점 (선행성 — 신용이 먼저 깨지나) ===')
hd = H.conf(hy_trough(1.0), 5, 20)
q = H.D['qqq']
for nm, (a, b) in H.BEARS.items():
    # bear 시작 전 6개월부터 보기
    seg = IDX[(IDX >= str(pd.Timestamp(a) - pd.Timedelta(days=200))) & (IDX < b)]
    if len(seg) == 0:
        continue
    on = hd.reindex(seg).fillna(False)
    first = on[on].index.min()
    peak = q.loc[:str(a)].iloc[-260:].max() if len(q.loc[:str(a)]) else None
    dd = (q.loc[first]/peak - 1)*100 if (pd.notna(first) and peak) else None
    print(f'  {nm:<14} HY발동 {str(first.date()) if pd.notna(first) else "미발동":12} (고점대비 {dd:+.0f}%)' if dd is not None else f'  {nm:<14} {"미발동"}')

print('\n=== 인접 민감도 (트로프 margin) ===')
V.adjacency(lambda p: (base | H.conf(hy_trough(p), 5, 20)).reindex(IDX).fillna(False), [0.5, 0.75, 1.0, 1.25, 1.5], 'margin')
print('\n참고: baseline Cal0.36/MDD-36.5/WFmin0.34/LOWO0.37/늦음-14.1 | breadth MDD-27.4/Cal0.44')
