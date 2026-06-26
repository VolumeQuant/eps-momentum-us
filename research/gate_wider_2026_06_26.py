# -*- coding: utf-8 -*-
"""gap 진입게이트(missing=pass) 결판 — 2.5·3.0 둘 다, 기각/배포 명확히.
PART A: 91일 faithful (로컬 DB + trailing_eps_ttm PIT). base 216.95% 정합. THR 2.5/3.0 + LOWO.
PART B: 8년 broad (us-4factor). ★look-ahead gap + ★노이즈주입(거래가능 시뮬) 둘 다. K=2/5/10. 연도별.
합격: 91일+ & LOWO통과 & 8년-노이즈 K=2+ & 2.5·3.0 robust → 배포권고. look-ahead만 좋으면 기각(거래불가).
"""
import sys, os, json, sqlite3
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
np.random.seed(0)
TE = json.load(open(os.path.join('data_cache', 'trailing_eps_ttm.json')))   # PIT TTM EPS


def pit_te(tk, d):
    rec = TE.get(tk)
    if not rec: return None
    v = None
    for rd, e in rec:
        if rd <= d: v = e
        else: break
    return v


# ════ PART A: 91일 faithful (DB) ════
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


def runA(gmin=None, ban=()):
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        ms = {t: _ms(v) for t, v in data.items()}; wr = {t: v['p2'] for t, v in data.items() if v.get('p2')}
        elig = sorted([(t, v['p2']) for t, v in data.items() if ms.get(t, 0) >= -2 and v.get('p2')], key=lambda x: x[1])
        drr = 0.0; pn = len(pf)
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 if pn == 1 else 0.5) * (cu - pp) / pp * 100
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
                cand = [t for t in cand if not (gapA(t, data[t], d) is not None and gapA(t, data[t], d) < gmin)]  # missing=pass
            cand.sort(key=lambda t: wr.get(t, 999))
            for t in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    return (nav - 1) * 100, mdd * 100


print('=== PART A: 91일 faithful (실제 revision신호, missing=pass) ===')
_b = runA(None); print('base(게이트無): %.1f%% MDD%.0f (production 216.95%% 정합)' % _b)
WIN = ['SNDK', 'MU', 'STX', 'NVDA', 'LITE', 'COHR']
for thr in [2.5, 3.0]:
    full = runA(thr)[0]; base = runA(None)[0]
    worst = 999; ww = None
    for w in WIN:
        d = runA(thr, (w,))[0] - runA(None, (w,))[0]
        if d < worst: worst = d; ww = w
    print(f'  gap≥{thr} 게이트: {full:+.1f}% (base 대비 {full-base:+.1f}p) | 단일LOWO 최악 {worst:+.1f}p(-{ww})')
    # 누적 LOWO (회사 PC 방식)
    cum = []
    for bs in [{'MU', 'SNDK'}, {'MU', 'SNDK', 'STX'}, {'MU', 'SNDK', 'STX', 'LITE'}]:
        cum.append(runA(thr, bs)[0] - runA(None, bs)[0])
    print(f'    누적LOWO Δ: -MU·SNDK {cum[0]:+.0f}p / +STX {cum[1]:+.0f}p / +LITE {cum[2]:+.0f}p')

# ════ PART B: 8년 broad (us-4factor) — look-ahead + 노이즈 ════
print('\n=== PART B: 8년 broad (us-4factor) — look-ahead vs 노이즈주입(거래가능) ===')
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
    a = pa.get(tk); i = np.searchsorted(a[0], np.datetime64(d), side='right') - 1
    return float(a[1][i]) if i >= 0 else None
def feat8(tk, d, noise=0.0):
    recs = fin[tk]; past = [(fd, v) for fd, v in recs if fd <= d]; fut = [(fd, v) for fd, v in recs if fd > d]
    if not past: return None
    e0 = past[-1][1]; p = P8(tk, d)
    if not p or p <= 0 or e0 <= 0: return None
    gap = (fut[0][1] / e0) if (fut and fut[0][1] > 0) else None
    if gap is not None and noise > 0:
        gap = gap * (1 + np.random.uniform(-noise, noise))   # 컨센 오차 시뮬
    a = pa[tk]; i = np.searchsorted(a[0], np.datetime64(d), side='right') - 1
    mom = (a[1][i - 21] / a[1][i - 252] - 1) if (i >= 252 and a[1][i - 252] > 0 and i >= 21) else None
    return {'gap': gap, 'mom': mom}
me = [d for d in px8.resample('ME').last().index if px8.index[260] <= d <= px8.index[-2]]
nxt = {me[i]: me[i + 1] for i in range(len(me) - 1)}
def run8(K, gmin=None, noise=0.0):
    panel = {d: {t: feat8(t, d, noise) for t in UNIV} for d in me}
    panel = {d: {t: f for t, f in panel[d].items() if f} for d in me}
    nav = 1; peak = 1; mdd = 0; rets = []
    for d in me[:-1]:
        dn = nxt.get(d); rec = panel[d]
        cand = sorted([t for t in rec if rec[t]['mom'] is not None], key=lambda t: -rec[t]['mom'])
        sel = []
        for t in cand:
            if gmin is not None and rec[t]['gap'] is not None and rec[t]['gap'] < gmin: continue
            sel.append(t)
            if len(sel) >= K: break
        if len(sel) < 2: continue
        rr = [P8(t, dn) / P8(t, d) - 1 for t in sel if P8(t, d) and P8(t, dn) and P8(t, d) > 0]
        r = np.mean(rr) if rr else 0
        nav *= (1 + r); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1); rets.append(r)
    n = len(rets); cagr = nav ** (12 / n) - 1 if n else 0
    return (cagr / abs(mdd) if mdd < 0 else 0), mdd * 100
print(f'{"K":>4}{"모멘텀":>9}{"gap2.5 LA":>11}{"gap3.0 LA":>11}{"gap2.5 노이즈40%":>16}{"gap3.0 노이즈40%":>16}')
for K in [2, 5, 10]:
    b = run8(K)[0]
    g25 = run8(K, 2.5)[0]; g30 = run8(K, 3.0)[0]
    n25 = run8(K, 2.5, 0.4)[0]; n30 = run8(K, 3.0, 0.4)[0]
    print(f'{K:>4}{b:>9.2f}{g25:>11.2f}{g30:>11.2f}{n25:>16.2f}{n30:>16.2f}')
print('  (LA=look-ahead 상한치 / 노이즈40%=애널오차 시뮬=거래가능 근사)')
print('\n판정: 91일+·LOWO통과 + 8년 노이즈버전 K=2서 모멘텀↑ + 2.5·3.0 둘다 → 배포권고. 노이즈서 죽으면 거래불가 기각.')
