# -*- coding: utf-8 -*-
"""① R(리밸주기) 재스윕 — 최종셀(top4 PER<=30 gap>=2.5) 기준
② 급락 방어 변형 BT — 개별 손절/트레일링/급락시 조기재선발 (2026-07-06, 사용자 질의)

하네스 = vm_final_lowo / weight_sweep와 동일. 판정 = 위상평균 + 최악MDD + LOWO(-SNDK-MU).
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

N, PE_MAX = 4, 30

def select(d, exclude):
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
    cand.sort(key=lambda x: -x[1])
    return [t for t, _ in cand[:N]]

def run(R, phase=0, exclude=frozenset(), sl=None, ts=None, crash_resel=None):
    """sl: 편입가 대비 -x% 손절→현금(다음 리밸까지). ts: 편입후 고점 대비 -x% 트레일링.
    crash_resel: 포트 일수익 < -x%면 다음날 즉시 재선발(조기 리밸)."""
    hold = {}  # tk -> dict(entry, hi, dead)
    nav = 1.0; peak = 1.0; mdd = 0.0; force = False; stops = 0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t, st in hold.items():
            cu, pp = px.get(t), ppx.get(t)
            if st['dead'] or not (cu and pp and pp > 0): continue
            drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        # 손절/트레일링 체크(당일 종가 기준, 발동시 그 슬롯 현금화)
        for t, st in hold.items():
            cu = px.get(t)
            if st['dead'] or not cu: continue
            st['hi'] = max(st['hi'], cu)
            if sl is not None and cu <= st['entry'] * (1 - sl / 100):
                st['dead'] = True; stops += 1
            elif ts is not None and cu <= st['hi'] * (1 - ts / 100):
                st['dead'] = True; stops += 1
        if crash_resel is not None and drr < -crash_resel / 100:
            force = True
        if (i - phase) % R == 0 or force:
            force = False
            new = select(d, exclude)
            hold = {t: dict(entry=px.get(t) or FULL[d][t]['px'], hi=px.get(t) or FULL[d][t]['px'], dead=False) for t in new}
    return (nav - 1) * 100, mdd * 100, stops

def phased(R=5, **kw):
    rets = []; mdds = []; st = 0
    for ph in range(5):
        r, m, s = run(R, phase=ph, **kw); rets.append(r); mdds.append(m); st += s
    return sum(rets) / 5, min(rets), max(rets), min(mdds), st / 5

print(f'=== ① R 재스윕 (top4 PER<=30 gap>=2.5, {ad[2]}~{ad[-1]}, 위상평균) ===')
print(f'{"R":>3} {"평균수익":>8} {"범위":>13} {"최악MDD":>8} | {"ex2 평균":>8} {"ex2 MDD":>8}')
for R in [1, 2, 3, 4, 5, 7, 10, 15, 20]:
    a, lo_, hi_, m, _ = phased(R)
    e_a, _, _, e_m, _ = phased(R, exclude=frozenset(['SNDK', 'MU']))
    ph_note = '' if R != 1 else ' (위상무관)'
    print(f'{R:>3} {a:+7.0f}% {lo_:+5.0f}~{hi_:+.0f}% {m:+7.0f}% | {e_a:+7.0f}% {e_m:+7.0f}%{ph_note}')

print()
print('=== ② 급락 방어 변형 (R5 고정, 위상평균) ===')
print(f'{"변형":26} {"평균수익":>8} {"최악MDD":>8} {"발동/run":>8} | {"ex2 평균":>8} {"ex2 MDD":>8}')
VAR = [
    ('현행(방어 없음)', {}),
    ('손절 -10%', dict(sl=10)),
    ('손절 -15%', dict(sl=15)),
    ('손절 -20%', dict(sl=20)),
    ('트레일링 -15%', dict(ts=15)),
    ('트레일링 -20%', dict(ts=20)),
    ('급락일 -5% 조기재선발', dict(crash_resel=5)),
    ('급락일 -7% 조기재선발', dict(crash_resel=7)),
]
for name, kw in VAR:
    a, _, _, m, s = phased(5, **kw)
    e_a, _, _, e_m, _ = phased(5, exclude=frozenset(['SNDK', 'MU']), **kw)
    print(f'{name:26} {a:+7.0f}% {m:+7.0f}% {s:8.1f} | {e_a:+7.0f}% {e_m:+7.0f}%')

print()
print('=== ③ 7/1~2 급락 에피소드 분해 ===')
for tk in ['SNDK', 'MU', 'HPE', 'DELL']:
    ser = [(d, AP[d].get(tk)) for d in ad[-8:] if AP[d].get(tk)]
    if len(ser) < 2: continue
    pk = max(p for _, p in ser)
    last = ser[-1][1]
    d0, p0 = ser[0]
    print(f'  {tk:5} {d0} {p0:8.1f} → {ser[-1][0]} {last:8.1f} ({(last/p0-1)*100:+.1f}%, 8일고점대비 {(last/pk-1)*100:+.1f}%)')
