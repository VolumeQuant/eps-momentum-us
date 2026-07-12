# -*- coding: utf-8 -*-
"""안건1 부속 (2026-07-12): KR base-effect 계단과 최근성(rev30) 조건의 상호작용 EDA.

가설: KR 대형주 분기 잠정실적 계단(예: 삼성 4/9) 후 rev90은 ~90일 높게 유지되지만
rev30은 ~30일이면 소멸 → 최근성 조건이 base-effect stale을 60일 먼저 강등(완충)하는지.
EDA 전용(BT 불가 — 통합 원장 7/8~뿐). ⚠️ 2026-07-10 KR 유니버스 확대(~342) 이전 이력은
필터 아티팩트 주의 — 여기선 삼성/하이닉스 등 항시 수집 대형주 시계열만 사용.
"""
import sys, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

KR_DB = 'C:/dev/claude-code/quant_py-main/kr_eps_momentum/eps_momentum_data_kr.db'
conn = sqlite3.connect(KR_DB)

def seg(a, b):
    return (a - b) / abs(b) * 100 if (b and abs(b) > 100) else None  # 원화 저분모 가드 100

for tk, nm in [('005930.KS', '삼성전자'), ('000660.KS', 'SK하이닉스')]:
    rows = conn.execute(
        'SELECT date,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening '
        'WHERE ticker=? AND ntm_current>0 ORDER BY date', (tk,)).fetchall()
    print(f'--- {nm} ({tk}) n={len(rows)} {rows[0][0]}~{rows[-1][0]} ---')
    print('  date        rev90   rev60   rev30   rev7   (rev30/rev90 비)')
    for d, nc, n7, n30, n60, n90 in rows:
        if d < '2026-04-01' or d.endswith(('1', '4', '8')) and d < '2026-06-25':
            continue  # 4월 이후 위주, 표본 축약
        r90, r60, r30, r7 = seg(nc, n90), seg(nc, n60), seg(nc, n30), seg(nc, n7)
        f = lambda x: f'{x:+6.1f}' if x is not None else '   na '
        ratio = f'{r30/r90:.2f}' if (r30 is not None and r90) else 'na'
        print(f'  {d}  {f(r90)}  {f(r60)}  {f(r30)}  {f(r7)}   {ratio}')
conn.close()
