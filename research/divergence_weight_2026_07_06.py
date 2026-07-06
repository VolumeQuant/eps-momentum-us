# -*- coding: utf-8 -*-
"""① 괴리(전망상향−가격변동) 가중/정렬 변형 BT — "MU 더 실어야 하나" (2026-07-06 사용자 질의)
② 정렬 신호 변형: rev90 단독 vs rev30/rev7/블렌드(구시스템식 다창구 가중)

하네스 = weight_sweep와 동일(top4 PER<=30 gap>=2.5 R5). 판정 = 위상평균+최악MDD+LOWO ex2.
"""
import sys, os, json, sqlite3
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
IDX = {d: i for i, d in enumerate(ad)}
FULL = {}
for tk, d, px, nc, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}
DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DV = {d: {t: (None if pd.isna(DVF.loc[d, t]) else float(DVF.loc[d, t])) for t in DVF.columns} for d in DVF.index if d in set(ad)}
TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)
def industry_ok(tk):
    if tk in BAD_TK: return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD_IND)
TE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
def pit_te(tk, d):
    r = TE.get(tk); v = None
    if not r: return None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v
def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)
def rv(v, k):
    b = v[k]
    return (v['nc'] - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0

N, R, PE_MAX = 4, 5, 30

def px_chg(tk, d, lb=21):
    i = IDX[d]
    if i < lb: return None
    p0 = AP.get(ad[i - lb], {}).get(tk); p1 = AP.get(d, {}).get(tk)
    return (p1 / p0 - 1) * 100 if (p0 and p1) else None

def diverg(tk, v, d):
    pc = px_chg(tk, d)
    return rv(v, 'n30') - (pc if pc is not None else 0)

def run(order, wmode, exclude=frozenset(), phase=0):
    hold = []; wv = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        drr = 0.0
        for t, w in zip(hold, wv):
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += w * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if (i - phase) % R == 0:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not industry_ok(tk): continue
                dv = DV.get(d, {}).get(tk)
                if dv is None or dv < 1000: continue
                if ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if v['px'] / v['nc'] > PE_MAX: continue
                te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
                if g is not None and g < 2.5: continue
                if order == 'rev90': s = rv(v, 'n90')
                elif order == 'rev30': s = rv(v, 'n30')
                elif order == 'rev7': s = rv(v, 'n7')
                elif order == 'blend': s = 0.5 * rv(v, 'n90') + 0.3 * rv(v, 'n30') + 0.2 * rv(v, 'n7')
                elif order == 'oldw': s = 0.5 * rv(v, 'n90') + 0.1 * rv(v, 'n60') + 0.1 * rv(v, 'n30') + 0.3 * rv(v, 'n7')
                elif order == 'diverg': s = diverg(tk, v, d)
                cand.append((tk, s, v))
            cand.sort(key=lambda x: -x[1]); sel = cand[:N]
            hold = [t for t, _, _ in sel]
            if wmode == 'ew':
                wv = [1.0 / len(sel)] * len(sel) if sel else []
            elif wmode == 'divprop':  # 괴리 비례(하한 1)
                ds = [max(diverg(t, v, d), 1.0) for t, _, v in sel]
                tot = sum(ds) or 1.0; wv = [x / tot for x in ds]
            elif wmode == 'divtop':  # 괴리 1등만 40%, 나머지 20%
                ds = [diverg(t, v, d) for t, _, v in sel]
                if ds:
                    mx = ds.index(max(ds))
                    wv = [0.4 if j == mx else 0.6 / (len(sel) - 1) for j in range(len(sel))]
    return (nav - 1) * 100, mdd * 100

def phased(order, wmode, exclude=frozenset()):
    rs = []; ms_ = []
    for ph in range(R):
        r, m = run(order, wmode, exclude, ph); rs.append(r); ms_.append(m)
    return sum(rs) / R, min(rs), max(rs), min(ms_)

print(f'=== 괴리가중/정렬 변형 (top4 PER<=30 gap>=2.5 R5, {ad[2]}~{ad[-1]}) ===')
print(f'{"변형":34} {"평균수익":>8} {"범위":>13} {"최악MDD":>8} | {"ex2":>6} {"ex2 MDD":>8}')
CASES = [
    ('현행: rev90 정렬 + 동일비중', 'rev90', 'ew'),
    ('rev90 정렬 + 괴리비례 비중', 'rev90', 'divprop'),
    ('rev90 정렬 + 괴리1등 40%', 'rev90', 'divtop'),
    ('괴리 정렬 + 동일비중', 'diverg', 'ew'),
    ('rev30 정렬 + 동일비중', 'rev30', 'ew'),
    ('rev7 정렬 + 동일비중', 'rev7', 'ew'),
    ('블렌드 90/30/7=.5/.3/.2', 'blend', 'ew'),
    ('구시스템식 .5/.1/.1/.3(90/60/30/7)', 'oldw', 'ew'),
]
for name, o, w in CASES:
    a, lo_, hi_, m = phased(o, w)
    ea, _, _, em = phased(o, w, frozenset(['SNDK', 'MU']))
    print(f'{name:34} {a:+7.0f}% {lo_:+5.0f}~{hi_:+.0f}% {m:+7.0f}% | {ea:+5.0f}% {em:+7.0f}%')
