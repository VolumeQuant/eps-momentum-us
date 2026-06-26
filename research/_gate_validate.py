# -*- coding: utf-8 -*-
"""gap 게이트(missing=pass, gap>=THR) 검증:
  PART A — 91일 라이브: STX 단일종목 의존성 격리 (gate 이득이 STX 운인가?)
  PART B — 8년 broad(us-4factor): forward-gap 게이트가 모멘텀(price-mom)에서 저gap 승자(NVDA류)를 자르는 비용 vs 이득
결론: 게이트가 robust 개선인지, STX/단일기간 착시인지 판정.
"""
import sqlite3, pandas as pd, numpy as np, json, os, sys
sys.stdout.reconfigure(encoding='utf-8')
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP = r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PXF = SP + r'\px137.pkl'
cache = json.load(open(r'C:\dev\claude code\eps-momentum-us\data_cache\trailing_eps_ttm.json'))

# ════ PART A : 91일 라이브 STX 격리 ════
c = sqlite3.connect(DB); cur = c.cursor()
all_dates = [r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date").fetchall()]
D = {}
for d in all_dates:
    D[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7], 'px': r[8]}
            for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,price FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL", (d,))}
def pit_te(tk, d):
    rec = cache.get(tk)
    if not rec: return None
    v = None
    for rd, e in rec:
        if rd <= d: v = e
        else: break
    return v
def seg(a, b): return (a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0.0
def minseg(v):
    if not all(v.get(k) for k in ['nc', 'n7', 'n30', 'n60', 'n90']): return 0
    return min(seg(v['nc'], v['n7']), seg(v['n7'], v['n30']), seg(v['n30'], v['n60']), seg(v['n60'], v['n90']))
def gp(t, v, d):
    te = pit_te(t, d); nc = v['nc']
    return (nc / te) if (te and te > 0 and nc and nc > 0) else None
PX = pd.read_pickle(PXF)
pi = {pd.Timestamp(d).strftime('%Y-%m-%d'): i for i, d in enumerate(PX.index)}
def px(tk, d):
    i = pi.get(d)
    if i is None or tk not in PX.columns: return None
    v = PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def run91(gmin=None, ban=()):
    EXIT = 12; PE_HOLD = 30; nav = 1.0; peak = 1.0; mdd = 0.0; pf = {}
    for k in range(2, len(all_dates)):
        d, pv = all_dates[k], all_dates[k - 1]; dd = D.get(d, {})
        ms = {t: minseg(v) for t, v in dd.items()}
        wrank = {t: v['p2'] for t, v in dd.items() if v.get('p2')}
        elig = sorted([(t, v['p2']) for t, v in dd.items() if ms.get(t, 0) >= -2 and v.get('p2')], key=lambda x: x[1])
        dr = 0.0
        for t, info in pf.items():
            w = info['w'] / 100; cu, pp = px(t, d), px(t, pv)
            if cu and pp and pp > 0: dr += w * (cu - pp) / pp * 100
        nav *= (1 + dr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        for t in list(pf.keys()):
            cp = px(t, d); it = dd.get(t)
            if cp is None or it is None or it.get('p2') is None: continue
            rk = it['p2']; nc = it['nc']; mg = minseg(it); sell = False
            if mg < -2: sell = True
            elif rk > EXIT and ((cp / nc) if (nc and nc > 0) else 999) >= PE_HOLD: sell = True
            if sell: del pf[t]
        if len(pf) < 2:
            cs = []
            for t, _ in elig:
                if t in pf or t in ban or t not in PX.columns or not px(t, d): continue
                if ms.get(t, -9) < 0 or wrank.get(t, 999) > 5 or (dd.get(t, {}).get('dv') or 0) < 1000: continue
                if gmin is not None:
                    g = gp(t, dd[t], d)
                    if g is not None and g < gmin: continue   # 계산되는데 낮으면 컷, None은 통과
                cs.append(t)
            cs.sort(key=lambda t: wrank.get(t, 999))
            for t in cs:
                if len(pf) >= 2: break
                pf[t] = {'w': 0}
            n = len(pf)
            for i in pf.values(): i['w'] = 100 / n if n else 0
    return (nav - 1) * 100, mdd * 100

print("=== PART A: 91일 — gap>=2.5 게이트 이득이 STX 단일종목 운인가? ===")
print(f"{'제거 종목':28}{'현행':>8}{'gap>=2.5게이트':>14}{'Δ':>7}")
for ban, nm in [(set(), '없음'), ({'MU','SNDK'}, 'MU·SNDK'), ({'MU','SNDK','STX'}, 'MU·SNDK·STX'),
                ({'MU','SNDK','STX','LITE'}, 'MU·SNDK·STX·LITE')]:
    base, _ = run91(None, ban); gate, _ = run91(2.5, ban)
    print(f"{nm:28}{base:>7.0f}%{gate:>13.0f}%{gate-base:>+7.0f}")

# ════ PART B : 8년 broad forward-gap 게이트 ════
print("\n=== PART B: 8년 broad — forward-gap 게이트가 모멘텀(price-mom)에 도움/해? ===")
R = r'C:\dev\claude-code\us-4factor\research\mf_data'
px8 = pd.read_parquet(os.path.join(R, 'prices.parquet')).sort_index(); px8.index = pd.to_datetime(px8.index)
F = json.load(open(os.path.join(R, 'funda_edgar.json')))
fin = {}
for tk, e in F.items():
    if '_err' in e or not e.get('eps') or not e.get('filed') or tk not in px8.columns: continue
    recs = sorted([(pd.Timestamp(e['filed'][fy]), e['eps'][fy]) for fy in e['eps'] if e['filed'].get(fy) and e['eps'].get(fy) is not None])
    if len(recs) >= 4: fin[tk] = recs
UNIV = list(fin.keys())
pa = {tk: (px8[tk].dropna().index.values, px8[tk].dropna().values.astype(float)) for tk in UNIV}
def P8(tk, d):
    a = pa.get(tk)
    if a is None: return None
    i = np.searchsorted(a[0], np.datetime64(d), side='right') - 1
    return float(a[1][i]) if i >= 0 else None
def feat8(tk, d):
    recs = fin[tk]; past = [(fd, v) for fd, v in recs if fd <= d]; fut = [(fd, v) for fd, v in recs if fd > d]
    if not past: return None
    e0 = past[-1][1]; p = P8(tk, d)
    if not p or p <= 0 or e0 <= 0: return None
    gap = (fut[0][1] / e0) if (fut and fut[0][1] > 0) else None   # forward gap (look-ahead). 없으면 None
    a = pa[tk]; i = np.searchsorted(a[0], np.datetime64(d), side='right') - 1
    mom = None
    if i >= 252:
        p12 = a[1][i - 252]; p1 = a[1][i - 21] if i >= 21 else None
        if p12 > 0 and p1: mom = p1 / p12 - 1
    return {'gap': gap, 'mom': mom}
me = [d for d in px8.resample('ME').last().index if px8.index[260] <= d <= px8.index[-2]]
nxt = {me[i]: me[i + 1] for i in range(len(me) - 1)}
panel = {d: {t: feat8(t, d) for t in UNIV} for d in me}
panel = {d: {t: f for t, f in panel[d].items() if f} for d in me}
def run8(K, gmin=None):
    nav = 1; peak = 1; mdd = 0; rets = []; yr = {}
    for d in me[:-1]:
        dn = nxt.get(d)
        if dn is None: continue
        rec = panel[d]
        cand = [t for t in rec if rec[t]['mom'] is not None]
        cand = sorted(cand, key=lambda t: -rec[t]['mom'])   # 모멘텀 순
        sel = []
        for t in cand:
            if gmin is not None:
                g = rec[t]['gap']
                if g is not None and g < gmin: continue   # 계산되는데 낮으면 컷, None 통과
            sel.append(t)
            if len(sel) >= K: break
        if len(sel) < 2: continue
        rr = [P8(t, dn) / P8(t, d) - 1 for t in sel if P8(t, d) and P8(t, dn) and P8(t, d) > 0]
        r = np.mean(rr) if rr else 0
        nav *= (1 + r); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1); rets.append(r); yr.setdefault(d.year, []).append(r)
    n = len(rets); cagr = nav ** (12 / n) - 1 if n else 0
    return (nav - 1) * 100, cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else 0), yr
for K in [2, 5, 10]:
    b = run8(K, None); g = run8(K, 2.5)
    print(f"  K={K}: 모멘텀 Calmar {b[3]:.2f}(MDD{b[2]:.0f}%) → +gap>=2.5게이트 {g[3]:.2f}(MDD{g[2]:.0f}%)  [누적 {b[0]:.0f}%→{g[0]:.0f}%]")
print("\n  연도별 (K=5, 모멘텀 vs +게이트):")
_, _, _, _, yb = run8(5, None); _, _, _, _, yg = run8(5, 2.5)
for y in sorted(yb):
    bb = np.prod([1 + r for r in yb[y]]) - 1; gg = np.prod([1 + r for r in yg.get(y, [0])]) - 1
    print(f"    {y}: 모멘텀 {bb*100:+.0f}%  게이트 {gg*100:+.0f}%  Δ{(gg-bb)*100:+.0f}p{'  ←약세' if y in (2018,2022) else ''}")
