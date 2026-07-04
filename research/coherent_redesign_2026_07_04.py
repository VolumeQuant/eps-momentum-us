# -*- coding: utf-8 -*-
"""'2슬롯+무한carryover=누가뭘드는지 모순' 해결 설계 검증.
측정: ①수익/MDD ②★coherence(시작일 2/15/30/50 달라도 최종보유 같나) ③매매횟수.
설계: carry(현행)/topK-pure(순위밀리면 즉시매도)/topK-hyst(밴드M까지 보유).
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
TE = json.load(open(os.path.join('data_cache', 'trailing_eps_ttm.json')))
def pit(tk, d):
    r = TE.get(tk)
    if not r: return None
    v = None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v
conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
AP = {d: {r[0]: r[1] for r in c.execute('SELECT ticker,price FROM ntm_screening WHERE date=?', (d,))} for d in ad}
DD = {}
for d in ad:
    DD[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7]}
             for r in c.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,))}
# 전체 가격·ntm (풀밖 종목 carryover 판정용)
PXALL, NCALL = {}, {}
for tk, d, p, nc in c.execute('SELECT ticker,date,price,ntm_current FROM ntm_screening WHERE price IS NOT NULL'):
    PXALL.setdefault(tk, {})[d] = p
    if nc is not None: NCALL.setdefault(tk, {})[d] = nc
conn.close()
PH = dr.PE_HOLD
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def gap(tk, v, d):
    te = pit(tk, d); return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None
def entry_ok(tk, v, d):
    return (v.get('p2') is not None and v['p2'] <= 5 and ms(v) >= 0
            and (v.get('dv') or 0) >= 1000 and not (gap(tk, v, d) is not None and gap(tk, v, d) < 2.5))

def run(mode, start=2, K=2, M=12):
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0; turn = 0
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        wr = {t: v['p2'] for t, v in data.items() if v.get('p2')}
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        for t in list(pf):
            it = data.get(t); rk = wr.get(t)
            mseg = ms(it) if it else 0
            if it is not None and mseg < -2:
                del pf[t]; continue  # 공통: EPS꺾임 매도
            if mode == 'carry':
                # 현행: 풀밖/순위>12 이어도 싸면 보유
                if it is None:
                    _p = PXALL.get(t, {}).get(d); _nc = NCALL.get(t, {}).get(d)
                    if _p and _nc and _nc > 0 and (_p / _nc) >= PH: del pf[t]
                    continue
                if rk is not None and rk <= 12: continue
                _pe = (it['price'] / it['nc']) if (it.get('price') and it['nc'] and it['nc'] > 0) else 999
                if _pe >= PH: del pf[t]
            else:  # topk: 순위>M(또는 풀밖)이면 매도 (M=K면 pure, M>K면 hysteresis)
                if rk is None or rk > M: del pf[t]
        # 진입: 빈슬롯을 top 순위 eligible로
        if len(pf) < K:
            cand = sorted([(t, wr[t]) for t, v in data.items() if t not in pf and entry_ok(t, v, d)], key=lambda x: x[1])
            for t, _ in cand:
                if len(pf) >= K: break
                pf[t] = 1; turn += 1
    return (nav - 1) * 100, mdd * 100, turn, set(pf)

DESIGNS = [
    ('①현행 carryover 2슬롯', 'carry', 2, 12),
    ('②-a Top2 순수(밀리면매도)', 'topk', 2, 2),
    ('②-b Top2 밴드(rank>8 매도)', 'topk', 2, 8),
    ('②-c Top3 순수', 'topk', 3, 3),
    ('②-d Top3 밴드(rank>12)', 'topk', 3, 12),
    ('③슬롯확대 carryover 3슬롯', 'carry', 3, 12),
]
print('=== 설계별 수익/안전/매매 (기본 시작) ===')
print(f'{"설계":30}{"전기간%":>8}{"MDD":>6}{"매수":>5}')
res = {}
for nm, mode, K, M in DESIGNS:
    r = run(mode, 2, K, M); res[nm] = (mode, K, M)
    print(f'{nm:30}{r[0]:>+7.0f}%{r[1]:>+6.0f}{r[2]:>4}회')

print('\n=== ★COHERENCE: 시작일 2/15/30/50 → 최종보유 같나? (같으면 coherent) ===')
for nm, mode, K, M in DESIGNS:
    finals = [tuple(sorted(run(mode, s, K, M)[3])) for s in [2, 15, 30, 50]]
    coherent = len(set(finals)) == 1
    print(f'  {nm:30} {"✅ 일관(같음)" if coherent else "❌ 제각각"}: {[list(f) for f in finals]}')
print('\n판정: coherent=✅ 이면서 수익·MDD 좋은 게 최적. 현행(carry)은 시작일마다 다르면=모순 확인.')
