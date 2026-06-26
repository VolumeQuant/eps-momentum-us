# -*- coding: utf-8 -*-
"""사용자: gap 진입게이트 '랜덤 진입일(multistart)' 테스트 여러번 + EPS모멘텀(base)과 비교분석.
= +24p가 특정 시작일 운이냐? 여러 시작일에서 cold-start로 base vs 게이트 paired 비교.
판정: 게이트가 시작일 무관 일관 승(높은 win-rate·양의 분포)이면 robust, 들쭉날쭉이면 시작일 운.
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


def gap(tk, v, d):
    te = pit_te(tk, d)
    return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None


def run(start, end, gmin=None, ban=()):
    """start~end 구간 cold-start. gmin=None=base(EPS모멘텀), 3.0=게이트."""
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(max(start, 2), end):
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
        if len(pf) < 2:
            cand = [t for t, _ in elig if t not in pf and t not in ban and ms.get(t, -9) >= 0
                    and wr.get(t, 999) <= 5 and (data.get(t, {}).get('dv') or 0) >= 1000]
            if gmin is not None:
                cand = [t for t in cand if not (gap(t, data[t], d) is not None and gap(t, data[t], d) < gmin)]
            cand.sort(key=lambda t: wr.get(t, 999))
            for t in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100


N = len(ad)
# ── 테스트1: 모든 가능한 시작일(매 영업일)에서 끝까지 — paired ──
print('=== 랜덤(전수) 진입일 multistart: 각 시작일→끝, base(EPS모멘텀) vs gap게이트 paired ===')
starts = list(range(2, N - 15))   # 끝까지 최소 15일 남는 시작일 전부
diffs = []; wins = 0; ties = 0
for s in starts:
    b = run(s, N, None)[0]; g = run(s, N, 3.0)[0]
    diffs.append(g - b)
    if g > b + 1e-9: wins += 1
    elif abs(g - b) <= 1e-9: ties += 1
diffs = np.array(diffs)
print(f'  시작일 {len(starts)}개 | 게이트 승 {wins} / 동률 {ties} / 패 {len(starts)-wins-ties}')
print(f'  Δ(게이트-base): 평균 {diffs.mean():+.1f}p / 중앙 {np.median(diffs):+.1f}p / 최악 {diffs.min():+.1f}p / 최선 {diffs.max():+.1f}p')
print(f'  Δ>0 비율 {100*(diffs>1e-9).mean():.0f}% / Δ<0(게이트가 손해본 시작일) 비율 {100*(diffs<-1e-9).mean():.0f}%')

# ── 테스트2: 랜덤 구간(시작·끝 둘 다 무작위, 최소 20일) ──
print('\n=== 랜덤 구간(start·end 무작위, ≥20일) 300회 paired ===')
rng = np.random.RandomState(42)
d2 = []; w2 = 0
for _ in range(300):
    s = rng.randint(2, N - 20); e = rng.randint(s + 20, N + 1)
    b = run(s, e, None)[0]; g = run(s, e, 3.0)[0]
    d2.append(g - b)
    if g > b + 1e-9: w2 += 1
d2 = np.array(d2)
print(f'  게이트 승 {w2}/300 ({100*w2/300:.0f}%) | 평균 Δ {d2.mean():+.1f}p / 중앙 {np.median(d2):+.1f}p / 최악 {d2.min():+.1f}p / 최선 {d2.max():+.1f}p')

# ── 비교분석: base vs 게이트 절대 성과(전기간) ──
print('\n=== EPS모멘텀(base) vs gap게이트 비교분석 (전기간) ===')
b = run(2, N, None); g = run(2, N, 3.0)
print(f'  base(EPS모멘텀): {b[0]:+.0f}%  MDD{b[1]:+.0f}')
print(f'  +gap게이트:      {g[0]:+.0f}%  MDD{g[1]:+.0f}   (수익 {g[0]-b[0]:+.0f}p / MDD {g[1]-b[1]:+.0f}p)')
print('\n판정: 승률 높고 최악Δ가 작은음수~양수면 시작일-robust. 최악Δ가 크게 음수면 특정 시작일에서 게이트가 winner 놓침.')
print('⚠️ 92일 단일강세장·종목 N작음 — multistart 시작일들이 겹쳐 독립표본 아님(상한 해석).')
