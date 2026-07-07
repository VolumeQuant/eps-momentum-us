# -*- coding: utf-8 -*-
"""정렬신호 창 블렌드 전수 그리드 — (w90,w60,w30,w7) 0.1간격 286조합 (2026-07-07, 사용자 질의
"90 단독이 최선? 주기별 스윗스팟 비율 찾아보자").

하네스 = per_gap_resweep와 동일(top4 PER<=30 gap>=2.5 R5 EW). 게이트/후보는 비율 무관이라
리밸일별 후보(rev 4성분 포함)를 선계산 → 그리드는 정렬만 바꿔 재실행.
판정 = 위상평균 + 최악MDD + ex2(-SNDK-MU) — 순수 rev90(1,0,0,0) 대비 노이즈 초과 + plateau 여부.
"""
import sys, os, json, sqlite3
from itertools import product
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
def seg(a, b):
    return (a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0
N, R, PE_MAX = 4, 5, 30

# 리밸 가능일 전체에 대해 게이트 통과 후보 + rev 4성분 선계산 (비율 무관 부분)
CAND = {}
for d in ad:
    lst = []
    for tk, v in FULL.get(d, {}).items():
        if not industry_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        segs = [seg(v['nc'], v['n7']), seg(v['n7'], v['n30']), seg(v['n30'], v['n60']), seg(v['n60'], v['n90'])]
        if min(segs) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > PE_MAX: continue
        te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
        if g is not None and g < 2.5: continue
        lst.append((tk, seg(v['nc'], v['n90']), seg(v['nc'], v['n60']), seg(v['nc'], v['n30']), seg(v['nc'], v['n7'])))
    CAND[d] = lst

def run(w, exclude=frozenset(), phase=0):
    w90, w60, w30, w7 = w
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if (i - phase) % R == 0:
            cand = [(tk, w90 * r90 + w60 * r60 + w30 * r30 + w7 * r7)
                    for tk, r90, r60, r30, r7 in CAND[d] if tk not in exclude]
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
    return (nav - 1) * 100, mdd * 100

def phased(w, exclude=frozenset()):
    rs = []; mm = []
    for ph in range(R):
        r, m = run(w, exclude, ph); rs.append(r); mm.append(m)
    return sum(rs) / R, min(rs), max(rs), min(mm)

if __name__ == '__main__':
    grid = [(a/10, b/10, c_/10, (10-a-b-c_)/10)
            for a, b, c_ in product(range(11), repeat=3) if a + b + c_ <= 10]
    rows = []
    for w in grid:
        avg, lo, hi, m = phased(w)
        rows.append((w, avg, lo, hi, m))
    rows.sort(key=lambda x: -x[1])
    base = next(r for r in rows if r[0] == (1.0, 0.0, 0.0, 0.0))
    print(f'조합 {len(grid)}개, 순수 rev90(1,0,0,0): 평균 {base[1]:+.0f}% ({base[2]:+.0f}~{base[3]:+.0f}) MDD{base[4]:+.0f}%  → 전체 {rows.index(base)+1}위')
    print()
    print('상위 12 (평균수익 기준) + ex2 검증:')
    print(f'{"(w90,w60,w30,w7)":>22} {"평균":>7} {"범위":>12} {"MDD":>6} | {"ex2":>6} {"ex2MDD":>7}')
    for w, avg, lo, hi, m in rows[:12]:
        ea, _, _, em = phased(w, frozenset(['SNDK', 'MU']))
        print(f'{str(w):>22} {avg:+6.0f}% {lo:+5.0f}~{hi:+4.0f}% {m:+5.0f}% | {ea:+5.0f}% {em:+6.0f}%')
    print()
    print('w90 레벨별 최고/평균 (지형 확인):')
    for lvl in [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]:
        sub = [r for r in rows if abs(r[0][0] - lvl/10) < 1e-9]
        if sub:
            bst = max(sub, key=lambda x: x[1])
            print(f'  w90={lvl/10:.1f}: best {bst[1]:+.0f}% {str(bst[0])}, 평균 {sum(r[1] for r in sub)/len(sub):+.0f}% (n={len(sub)})')
