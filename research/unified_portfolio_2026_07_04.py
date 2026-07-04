# -*- coding: utf-8 -*-
"""단일 포트폴리오 재설계: 순위 상위 N개를 모두 동일 보유(단일기준=보유=매수, 비대칭·carryover·에폭 제거).
사용자 요구: ①EPS저평가 컨셉 유지 ②포트폴리오 하나 ③진입일 무관 ④보유=추천.
검증: N(3/5/8/10) × 리밸런싱주기(매일/주1/격주) 별 수익·MDD·회전 + coherence(시작일 무관 같나).
필터: min_seg>=0(EPS건강) + 거래대금 $1B(거래가능). 순위=part2_rank(EPS리비전=저평가 측정).
"""
import sys, os, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
AP = {d: {r[0]: r[1] for r in c.execute('SELECT ticker,price FROM ntm_screening WHERE date=?', (d,))} for d in ad}
DD = {}
for d in ad:
    DD[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7]}
             for r in c.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,))}
conn.close()
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def topN(d, N):
    data = DD.get(d, {})
    elig = [(t, v['p2']) for t, v in data.items()
            if v.get('p2') and ms(v) >= 0 and (v.get('dv') or 0) >= 1000]
    elig.sort(key=lambda x: x[1])
    return [t for t, _ in elig[:N]]
def run(N, R, start=2):
    """top-N 동일가중, R일마다 리밸런싱. 사이엔 보유 유지."""
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; turn = 0
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if (i - start) % R == 0:   # 리밸런싱일
            tgt = topN(d, N)
            turn += len([t for t in tgt if t not in hold])
            hold = tgt
    return (nav - 1) * 100, mdd * 100, turn, set(hold)

print('=== 단일 포트폴리오: top-N × 리밸런싱주기 (수익 / MDD / 매수횟수) ===')
print('  (참고: 현행 carryover 2슬롯 = +210% / MDD-27 / 9회 — 단 비일관·에폭문제)')
print(f'{"":8}' + ''.join(f'{"R="+str(r)+"일":>16}' for r in [1, 5, 10]))
for N in [3, 5, 8, 10]:
    row = f'top{N:<5}'
    for R in [1, 5, 10]:
        r = run(N, R)
        row += f'{f"{r[0]:+.0f}%/{r[1]:.0f}/{r[2]}회":>16}'
    print(row)

print('\n=== ★COHERENCE 확인 (top5, 주1회) 시작일 2/15/30/50 → 최종보유 같나 ===')
for s in [2, 15, 30, 50]:
    print(f'  시작 {ad[s]}: {sorted(run(5,5,s)[3])}')
print('  → 시작일 달라도 같으면 = 진입일 무관 = 형이 원한 그거')

print('\n=== 오늘 이 방식이면 실제 보유 (top-N) ===')
for N in [3, 5, 8]:
    print(f'  top{N}: {topN(ad[-1], N)}')
