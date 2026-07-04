# -*- coding: utf-8 -*-
"""자율주행 R2: ①rev90 EPS리드 정밀검증 ②진입/이탈/슬롯 그리드(top2 위험? 3/5/3?) ③집중도·최근구간 진단."""
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
    DD[d] = {r[0]: {'p2': r[1], 'nc': r[2], 'n7': r[3], 'n30': r[4], 'n60': r[5], 'n90': r[6], 'dv': r[7], 'h30': r[8]}
             for r in c.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,high30 FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,))}
conn.close()
IDX = {d: i for i, d in enumerate(ad)}
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def gap(tk, v, d):
    te = pit(tk, d); return (v['nc'] / te) if (te and te > 0 and v['nc'] and v['nc'] > 0) else None
def rev(v, win):
    a = v['nc']; b = v[win]
    return (a - b) / abs(b) if (b and abs(b) > 0.01) else None
def z(dm):
    vv = np.array([x for x in dm.values() if x is not None])
    if len(vv) < 3: return {k: 0.0 for k in dm}
    mu, sd = vv.mean(), vv.std()
    if sd == 0: return {k: 0.0 for k in dm}
    return {k: ((x - mu) / sd if x is not None else 0.0) for k, x in dm.items()}
def newrank_rev(i, elig, ae):
    data = DD[ad[i]]
    sp = {t: -data[t]['p2'] for t, _ in elig}
    er = {t: rev(data[t], 'n90') for t, _ in elig}
    zp, ze = z(sp), z(er)
    order = sorted(sp, key=lambda t: -(zp[t] + ae * ze[t]))
    return {t: k + 1 for k, t in enumerate(order)}
def run(ae=0.0, slots=2, entry=5, exitr=12, gap_on=True, ban=(), rec=False):
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0; log = []
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        m = {t: ms(v) for t, v in data.items()}
        elig = [(t, v['p2']) for t, v in data.items() if m.get(t, 0) >= -2 and v.get('p2')]
        wr = newrank_rev(i, elig, ae) if ae else {t: v['p2'] for t, v in data.items() if v.get('p2')}
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if rec: log.append((d, drr))
        for t in list(pf):
            it = data.get(t)
            if it is None or px.get(t) is None: continue
            rk = wr.get(t); mm = m.get(t, 0); nc = it.get('nc'); cp = px.get(t); fpe = (cp / nc) if (cp and nc and nc > 0) else 999
            if mm < -2 or ((rk is None or rk > exitr) and fpe >= dr.PE_HOLD): del pf[t]
        if len(pf) < slots:
            for t, _ in sorted(elig, key=lambda x: wr.get(x[0], 999)):
                if len(pf) >= slots: break
                if t in pf or t in ban: continue
                v = data[t]
                if m.get(t, -9) < 0 or wr.get(t, 999) > entry or (v.get('dv') or 0) < 1000: continue
                h = v.get('h30'); pr = px.get(t)
                if h and pr and h > 0 and (pr - h) / h < -0.25: continue
                if gap_on:
                    g = gap(t, v, d)
                    if g is not None and g < 2.5: continue
                pf[t] = 1
    if rec: return (nav - 1) * 100, mdd * 100, log
    return (nav - 1) * 100, mdd * 100
def mret(log, mth):
    mm = [r for d, r in log if d.startswith(mth)]
    return (np.prod([1 + r / 100 for r in mm]) - 1) * 100 if mm else 0.0

# ─ R2-A: rev90 정밀 ─
print('=== R2-A: rev90 α 정밀 스윕 (plateau vs spike, 다중 LOWO) ===')
print(f'{"α":>8}{"전기간%":>9}{"MDD":>7}{"LOWO-2":>9}{"LOWO-4":>9}')
for a in [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.6]:
    r = run(ae=a); l2 = run(ae=a, ban={'MU', 'SNDK'})[0]; l4 = run(ae=a, ban={'MU', 'SNDK', 'STX', 'LITE'})[0]
    tag = ' ←현행' if a == 0 else (' ←1R리드' if a == 0.2 else '')
    print(f'{a:>8}{r[0]:>+8.0f}%{r[1]:>+7.0f}{l2:>+8.0f}%{l4:>+8.0f}%{tag}')

print('\n=== R2-B: rev90 α0.2가 최근 약세(6~7월)서 도움? ===')
for lbl, a in [('현행 α0', 0.0), ('rev90 α0.2', 0.2)]:
    _, _, lg = run(ae=a, rec=True)
    print(f'  {lbl:12} 5월{mret(lg,"2026-05"):+.1f}% 6월{mret(lg,"2026-06"):+.1f}% 7월{mret(lg,"2026-07"):+.1f}%')

# ─ R2-D: 진입/이탈/슬롯 그리드 (집중도 = top2 위험?) ─
print('\n=== R2-D: 슬롯/진입/이탈 그리드 (top2 위험? 3/5/3?) ===')
print(f'{"슬롯/진입/이탈":18}{"전기간%":>9}{"MDD":>7}{"LOWO-2":>9}{"7월%":>8}')
CONFIGS = [(2, 5, 12), (3, 5, 12), (5, 5, 12), (2, 3, 12), (3, 3, 12), (3, 5, 5), (3, 5, 8), (5, 5, 8), (3, 8, 12)]
for s, e, x in CONFIGS:
    r = run(slots=s, entry=e, exitr=x); l = run(slots=s, entry=e, exitr=x, ban={'MU', 'SNDK'})[0]
    _, _, lg = run(slots=s, entry=e, exitr=x, rec=True)
    tag = ' ←현행' if (s, e, x) == (2, 5, 12) else (' ←KR식?' if (s, e, x) == (3, 5, 5) else '')
    print(f'{f"{s}슬롯/{e}진입/{x}이탈":18}{r[0]:>+8.0f}%{r[1]:>+7.0f}{l:>+8.0f}%{mret(lg,"2026-07"):>+7.1f}%{tag}')
print('\n  판정: 슬롯↑는 수익↓지만 MDD·7월DD·LOWO(분산robust) 개선되면 집중리스크 헤지로 가치. 위험대비 최적 찾기.')
