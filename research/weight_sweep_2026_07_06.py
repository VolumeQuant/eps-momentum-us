# -*- coding: utf-8 -*-
"""top4 비중 스윕 — 동일가중(각 25%) vs 순위가중/rev90비례 (2026-07-06, 사용자 질의).

하네스 = vm_final_lowo_2026_07_04.py 그대로(재설계 검증에 쓰인 것), production 최종셀
PER<=30 + gap>=2.5(missing=pass) + rev90순 top4 R5. 비중만 변형.
판정 = 위상평균(리밸 위상 0~4) 수익 + 위상최악 MDD + LOWO(-SNDK/-MU/-둘다).
"""
import sys, os, json, sqlite3
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
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
def rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

N, R, PE_MAX = 4, 5, 30

def run(weights, exclude=frozenset(), phase=0):
    """weights: 고정 리스트(순위순) 또는 'rev90'(선발시점 rev90 비례). 일일 목표비중 재정렬(기존 하네스 1/n 관행과 동일)."""
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
                cand.append((tk, rev90(v)))
            cand.sort(key=lambda x: -x[1]); sel = cand[:N]
            hold = [t for t, _ in sel]
            if weights == 'rev90':
                tot = sum(max(r, 1.0) for _, r in sel) or 1.0
                wv = [max(r, 1.0) / tot for _, r in sel]
            else:
                wv = [w / sum(weights[:len(sel)]) for w in weights[:len(sel)]]
    return (nav - 1) * 100, mdd * 100, list(zip(hold, [round(w * 100) for w in wv]))

VARIANTS = [
    ('동일 25/25/25/25 (현행)', [25, 25, 25, 25]),
    ('완만 30/27/23/20',       [30, 27, 23, 20]),
    ('선형 40/30/20/10',       [40, 30, 20, 10]),
    ('집중 50/25/15/10',       [50, 25, 15, 10]),
    ('역가중 10/20/30/40',     [10, 20, 30, 40]),
    ('rev90 비례(동적)',        'rev90'),
]

print(f'=== top4 비중 스윕 (PER<=30 gap>=2.5 R5, {ad[2]}~{ad[-1]}, 위상 0~4 평균) ===')
hdr = f'{"변형":24} {"평균수익":>8} {"최소~최대":>14} {"최악MDD":>8} | LOWO평균: {"-SNDK":>7} {"-MU":>7} {"-둘다":>7} (최악MDD)'
print(hdr)
for name, w in VARIANTS:
    rets = []; mdds = []
    for ph in range(R):
        r, m, _ = run(w, phase=ph); rets.append(r); mdds.append(m)
    lo = {}
    for ex in [('SNDK',), ('MU',), ('SNDK', 'MU')]:
        er = []; em = []
        for ph in range(R):
            r, m, _ = run(w, frozenset(ex), phase=ph); er.append(r); em.append(m)
        lo[ex] = (sum(er) / R, min(em))
    print(f'{name:24} {sum(rets)/R:+7.0f}% {min(rets):+6.0f}~{max(rets):+.0f}% {min(mdds):+7.0f}% | '
          f'{lo[("SNDK",)][0]:+6.0f}% {lo[("MU",)][0]:+6.0f}% {lo[("SNDK","MU")][0]:+6.0f}% '
          f'({lo[("SNDK","MU")][1]:+.0f}%)')

print()
print('오늘(위상0=앵커 07-02 정합) 보유·비중:')
for name, w in VARIANTS:
    _, _, h = run(w, phase=0)
    print(f'  {name:24} {h}')
