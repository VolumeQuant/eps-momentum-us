# -*- coding: utf-8 -*-
"""자율주행: ①6월~현재 부진 진단 ②가격 민감도↑ ③EPS 민감도↑ (각 다변형·LOWO).
순위를 production rank에 가격/EPS 신호를 α만큼 블렌드해 틸트. α=0=현행. 전략 나머지(gap2.5/$1B/dd/pehold/2슬롯) 고정.
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
def mom(tk, i, w):
    if i - w < 0: return None
    p0 = AP.get(ad[i - w], {}).get(tk); p1 = AP.get(ad[i], {}).get(tk)
    return (p1 / p0 - 1) if (p0 and p1 and p0 > 0) else None
def ma_dist(tk, i, w=20):
    ps = [AP.get(ad[j], {}).get(tk) for j in range(max(0, i - w + 1), i + 1)]
    ps = [p for p in ps if p]; p1 = AP.get(ad[i], {}).get(tk)
    if len(ps) < 3 or not p1: return None
    return p1 / (sum(ps) / len(ps)) - 1
def rev(v, win):  # EPS revision magnitude
    a = v['nc']; b = v[win]
    return (a - b) / abs(b) if (b and abs(b) > 0.01) else None
def z(d_map):
    vals = np.array([x for x in d_map.values() if x is not None])
    if len(vals) < 3: return {k: 0.0 for k in d_map}
    mu, sd = vals.mean(), vals.std()
    if sd == 0: return {k: 0.0 for k in d_map}
    return {k: ((x - mu) / sd if x is not None else 0.0) for k, x in d_map.items()}

def newrank(i, elig, ap=0.0, ae=0.0, psig='mom20', esig='rev90'):
    """production rank에 가격(ap)·EPS(ae) 신호 블렌드 → 재순위."""
    d = ad[i]; data = DD[d]
    sp = {t: -data[t]['p2'] for t, _ in elig}
    if psig == 'mom20': pr = {t: mom(t, i, 20) for t, _ in elig}
    elif psig == 'mom5': pr = {t: mom(t, i, 5) for t, _ in elig}
    elif psig == 'mom60': pr = {t: mom(t, i, 60) for t, _ in elig}
    elif psig == 'ma20': pr = {t: ma_dist(t, i, 20) for t, _ in elig}
    elif psig == 'dip': pr = {t: (-(mom(t, i, 20) or 0)) for t, _ in elig}
    else: pr = {t: 0 for t, _ in elig}
    if esig == 'rev90': er = {t: rev(data[t], 'n90') for t, _ in elig}
    elif esig == 'rev30': er = {t: rev(data[t], 'n30') for t, _ in elig}
    elif esig == 'rev7': er = {t: rev(data[t], 'n7') for t, _ in elig}
    elif esig == 'accel': er = {t: ((rev(data[t], 'n7') or 0) - (rev(data[t], 'n90') or 0)) for t, _ in elig}
    elif esig == 'minseg': er = {t: ms(data[t]) for t, _ in elig}
    else: er = {t: 0 for t, _ in elig}
    zp, zpr, zer = z(sp), z(pr), z(er)
    comb = {t: zp[t] + ap * zpr[t] + ae * zer[t] for t, _ in elig}
    order = sorted(comb, key=lambda t: -comb[t])
    return {t: k + 1 for k, t in enumerate(order)}

def run(ap=0.0, ae=0.0, psig='mom20', esig='rev90', ban=(), rec=False):
    pf = {}; nav = 1.0; peak = 1.0; mdd = 0.0; log = []
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; data = DD.get(d, {}); px = AP.get(d, {}); ppx = AP.get(pv, {})
        m = {t: ms(v) for t, v in data.items()}
        elig = [(t, v['p2']) for t, v in data.items() if m.get(t, 0) >= -2 and v.get('p2')]
        wr = newrank(i, elig, ap, ae, psig, esig) if (ap or ae) else {t: v['p2'] for t, v in data.items() if v.get('p2')}
        n = len(pf); drr = 0.0
        for t in pf:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp * 100
        nav *= (1 + drr / 100); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if rec: log.append((d, drr, sorted(pf)))
        for t in list(pf):
            it = data.get(t)
            if it is None or px.get(t) is None: continue
            rk = wr.get(t); mm = m.get(t, 0); nc = it.get('nc'); cp = px.get(t)
            fpe = (cp / nc) if (cp and nc and nc > 0) else 999
            if mm < -2 or ((rk is None or rk > 12) and fpe >= dr.PE_HOLD):
                del pf[t]
        if len(pf) < 2:
            cand = []
            for t, _ in sorted(elig, key=lambda x: wr.get(x[0], 999)):
                if t in pf or t in ban: continue
                v = data[t]
                if m.get(t, -9) < 0 or wr.get(t, 999) > 5 or (v.get('dv') or 0) < 1000: continue
                h = v.get('h30'); pr = px.get(t)
                if h and pr and h > 0 and (pr - h) / h < -0.25: continue
                g = gap(t, v, d)
                if g is not None and g < 2.5: continue
                cand.append(t)
            for t in cand:
                if len(pf) >= 2: break
                pf[t] = 1
    if rec: return (nav - 1) * 100, mdd * 100, log
    return (nav - 1) * 100, mdd * 100

base = run()
print('=== 기준선(현행, α=0) ===')
print('  전기간 %.0f%%  MDD%.0f | LOWO(-MU·SNDK) %.0f%%' % (base[0], base[1], run(ban={'MU', 'SNDK'})[0]))

# ── Part 1: 6월~현재 진단 ──
print('\n════ Part1: 6월~현재 부진 진단 ════')
_, _, log = run(rec=True)
seg = [(d, r, h) for d, r, h in log if d >= '2026-06-01']
if seg:
    navseg = np.prod([1 + r / 100 for _, r, _ in seg]) - 1
    print(f'  6/1~현재 시스템 수익: {navseg*100:+.1f}%  ({len(seg)}거래일)')
    # 보유 분포
    from collections import Counter
    held = Counter()
    for _, r, h in seg:
        for t in h: held[t] += 1
    print('  이 기간 보유종목(일수):', dict(held.most_common()))
    # 종목별 기여
    contrib = {}
    for d, r, h in seg:
        i = IDX[d]; pv = ad[i - 1]
        for t in h:
            cu, pp = AP[d].get(t), AP[pv].get(t)
            if cu and pp and pp > 0: contrib[t] = contrib.get(t, 0) + (1.0 / len(h)) * (cu - pp) / pp * 100
    print('  종목별 누적기여(%p):', {k: round(v, 1) for k, v in sorted(contrib.items(), key=lambda x: x[1])})
    # 월별
    print('  월별 수익:')
    for mth in ['2026-02', '2026-03', '2026-04', '2026-05', '2026-06', '2026-07']:
        mm = [(d, r) for d, r, _ in log if d.startswith(mth)]
        if mm: print(f'    {mth}: {(np.prod([1+r/100 for _,r in mm])-1)*100:+.1f}%')

# ── Part 2: 가격 민감도 ──
print('\n════ Part2: 가격 민감도↑ (순위에 가격신호 블렌드) ════')
print(f'{"실험":30}{"전기간%":>9}{"MDD":>7}{"LOWO":>8}')
PX_EXP = [('mom20 α0.2', 'mom20', 0.2), ('mom20 α0.5', 'mom20', 0.5), ('mom20 α1.0', 'mom20', 1.0),
          ('mom5 α0.5', 'mom5', 0.5), ('mom60 α0.5', 'mom60', 0.5), ('MA20거리 α0.5', 'ma20', 0.5),
          ('buy-dip(역모멘텀) α0.5', 'dip', 0.5), ('mom20 α2.0(강)', 'mom20', 2.0)]
for nm, sig, a in PX_EXP:
    r = run(ap=a, psig=sig); lw = run(ap=a, psig=sig, ban={'MU', 'SNDK'})[0]
    print(f'{nm:30}{r[0]:>+8.0f}%{r[1]:>+7.0f}{lw:>+7.0f}%')

# ── Part 3: EPS 민감도 ──
print('\n════ Part3: EPS 민감도↑ (순위에 EPS리비전 블렌드) ════')
print(f'{"실험":30}{"전기간%":>9}{"MDD":>7}{"LOWO":>8}')
EPS_EXP = [('rev90 α0.2', 'rev90', 0.2), ('rev90 α0.5', 'rev90', 0.5), ('rev90 α1.0', 'rev90', 1.0),
           ('rev30(단기) α0.5', 'rev30', 0.5), ('rev7(초단기) α0.5', 'rev7', 0.5),
           ('가속(rev7-rev90) α0.5', 'accel', 0.5), ('minseg α0.5', 'minseg', 0.5), ('rev90 α2.0(강)', 'rev90', 2.0)]
for nm, sig, a in EPS_EXP:
    r = run(ae=a, esig=sig); lw = run(ae=a, esig=sig, ban={'MU', 'SNDK'})[0]
    print(f'{nm:30}{r[0]:>+8.0f}%{r[1]:>+7.0f}{lw:>+7.0f}%')

print('\n판정: 전기간 & LOWO 둘 다 기준선 넘으면 진짜 개선. 하나만이면 착시. MDD도 확인.')
