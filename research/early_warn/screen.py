# -*- coding: utf-8 -*-
"""1차 스크리닝 — 후보들을 base에 OR해 배터리 한 줄씩. 합격 후보만 deep validation으로."""
import sys
import pandas as pd
import harness as H
import validate as V
import candidates as C
sys.stdout.reconfigure(encoding='utf-8')

# 각 후보 raw 신호의 데이터 시작일(부분커버 주의) 표기
print('후보 raw 신호 활성구간 / 발생빈도:')
for nm, fn in C.REGISTRY.items():
    raw = fn(nm) if False else fn()
    on = raw.reindex(H.IDX).fillna(False)
    first_true = on[on].index.min()
    print(f'  {nm:<24} ON비율 {on.mean()*100:5.1f}%  최초ON {str(first_true.date()) if pd.notna(first_true) else "—"}')

print('\n=== 1차 스크리닝: base | confirm(cand, ne, nx) ===')
b = V.print_header()
NE, NX = 3, 15  # 후보 3일 확인 진입, 15일 확인 퇴출
results = []
for nm, fn in C.REGISTRY.items():
    raw = fn()
    dfn = V.combined(raw, ne=NE, nx=NX)
    r = V.compare(f'+{nm}', dfn, verbose=True)
    results.append(r)

print('\n=== 합격(PASS) 후보 (MDD↓ & Cal/WF/LOWO 비악화) ===')
passed = [r for r in results if r['verdict']['PASS']]
if passed:
    for r in passed:
        v = r['verdict']
        print(f'  {r["name"]:<26} MDD {r["mdd"]:+.1f}(base{b["mdd"]:+.1f}) Cal {r["calmar"]:.2f} '
              f'늦음 {r["late"]:+.1f}(base{b["late"]:+.1f}) late_better={v["late_better"]}')
else:
    print('  없음 — 1차에서 MDD개선+비악화 동시충족 후보 0개.')

print('\n=== 참고: MDD는 개선했으나 Cal/WF/LOWO 악화(휩쏘 비용) ===')
for r in results:
    v = r['verdict']
    if v['mdd_better'] and not v['PASS']:
        print(f'  {r["name"]:<26} MDD {r["mdd"]:+.1f} Cal {r["calmar"]:.2f}(base{b["calmar"]:.2f}) '
              f'WF {r["wf_min"]:.2f} LOWO {r["lowo"]:.2f} 전환 {r["trans"]}(base{b["trans"]})')
