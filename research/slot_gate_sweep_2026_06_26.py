# -*- coding: utf-8 -*-
"""우리 실제 EPS 시스템(faithful)서 슬롯수 N × gap게이트 동시 스윕 — 2슬롯이 진짜 최적인가?
질문: K=5/10이 좋다는 건 broad 가격모멘텀 얘기. 우리 좁은 리비전시스템선? + 게이트 켜면 달라지나?
base 2슬롯=production 256.9% 정합. 누적LOWO로 단일winner 의존 격리.
"""
import sys, os, json, sqlite3
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
TE = json.load(open(os.path.join('data_cache', 'trailing_eps_ttm.json')))


def pit_te(tk, d):
    rec = TE.get(tk)
    if not rec: return None
    v = None
    for rd, e in rec:
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
conn.close()
EXIT, PE_HOLD = dr.EXIT_RANK, dr.PE_HOLD


def _ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)


def gapA(tk, v, d):
    te = pit_te(tk, d)
    return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None


def run(N=2, gmin=None, pool=None, ban=()):
    """N슬롯 동일가중(1/N). 진입풀=top(pool or max(5,N)). 게이트 missing=pass."""
    pool = pool or max(5, N)
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        ms = {t: _ms(v) for t, v in data.items()}; wr = {t: v['p2'] for t, v in data.items() if v.get('p2')}
        elig = sorted([(t, v['p2']) for t, v in data.items() if ms.get(t, 0) >= -2 and v.get('p2')], key=lambda x: x[1])
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        for t in list(pf):
            it = data.get(t)
            if it is None or px.get(t) is None: continue
            rk = wr.get(t); m = ms.get(t, 0); nc = it.get('nc'); cp = px.get(t)
            if m < -2 or ((rk is None or rk > EXIT) and ((cp / nc if (cp and nc and nc > 0) else 999) >= PE_HOLD)):
                del pf[t]
        if len(pf) < N:
            cand = [t for t, _ in elig if t not in pf and t not in ban and ms.get(t, -9) >= 0
                    and wr.get(t, 999) <= pool and (data.get(t, {}).get('dv') or 0) >= 1000]
            if gmin is not None:
                cand = [t for t in cand if not (gapA(t, data[t], d) is not None and gapA(t, data[t], d) < gmin)]
            cand.sort(key=lambda t: wr.get(t, 999))
            for t in cand:
                if len(pf) >= N: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100


print('우리 EPS 시스템 (좁은 137·리비전모멘텀·faithful) — 슬롯수 × gap게이트')
print(f'{"슬롯N":>6}{"게이트無 누적%":>14}{"MDD":>7}{"+gap≥3.0":>12}{"MDD":>7}{"게이트Δ":>9}')
for N in [1, 2, 3, 5]:
    b = run(N, None); g = run(N, 3.0)
    print(f'{N:>6}{b[0]:>+13.0f}%{b[1]:>+7.0f}{g[0]:>+11.0f}%{g[1]:>+7.0f}{g[0]-b[0]:>+9.0f}p')
print('  (base N=2 게이트無 = production 256.9% 정합)')

print('\n=== 누적 LOWO: 슬롯 늘리면 단일winner 의존 줄어드나? (게이트無) ===')
print(f'{"슬롯":>5}{"full":>9}{"-MU·SNDK":>11}{"-+STX":>9}{"-+LITE":>9}')
for N in [2, 3, 5]:
    full = run(N)[0]
    r1 = run(N, ban={'MU', 'SNDK'})[0]; r2 = run(N, ban={'MU', 'SNDK', 'STX'})[0]; r3 = run(N, ban={'MU', 'SNDK', 'STX', 'LITE'})[0]
    print(f'{N:>5}{full:>+8.0f}%{r1:>+10.0f}%{r2:>+8.0f}%{r3:>+8.0f}%')
print('  (full↓하는데 winner제외값이 N클수록 덜 떨어지면=분산. 근데 full 자체가 낮으면 의미X)')
print('\n⚠️ 91일 단일강세장 — 슬롯최적화는 과적합주의. 8년 broad는 *다른 시스템*(가격모멘텀). 우리 시스템 slot3=과거 -90p였음(메모리).')
