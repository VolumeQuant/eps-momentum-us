# -*- coding: utf-8 -*-
"""PER/gap 임계 재스윕 — N4 확정셀 기준 (2026-07-06, 사용자 질의. FINDINGS 2f는 N5 시절 측정)

하네스 = minseg_sweep와 동일(top4 R5 rev90 EW), PER(gap2.5 고정)·gap(PER30 고정) 1D 스윕.
PER: 15 +87/ex2 -5·MDD-30 | 20 +103/+21 | 25 +126/+28·-25 | 30(현행) +115·MDD-18/ex2 +55·-19
   | 35 +123/+64·-20 | 40 +110·-22/+78 | 없음 +88·MDD-25/+50
   → 25~35 고원. 30 = 두 세계 균형 최선(헤드라인 무난 + ex2 수익/MDD 동시 양호).
     25는 헤드라인 피크지만 ex2 +28/-25로 취약(winner 없으면 중간PER 우량주가 필요한데 잘림).
     게이트 제거 시 MDD -25 = 게이트 가치는 수익 아니라 MDD(기존 결론 N4서 재확인).
gap: 없음 +106·-23/+44 | 1.5 +97/+68 | 2.0 +110·-20/+69 | 2.5(현행) +115·-18/+55·-19
   | 3.0 +123/+36·-21 | 3.5 +122/+23·-26
   → 2.0~3.0 고원. gap↑ = winner 집중(헤드라인↑ ex2 붕괴 36→23), gap↓ = 분산(ex2↑ 헤드라인·MDD↓).
     2.5 = 두 세계 교차점 중앙(06-28 그리드의 '3.0과 동일 robust + 분산 우위' 결론과 일치).

판정: PER30·gap2.5 유지 — 둘 다 칼날 아닌 고원 중앙 + 두 세계(winner 유/무) 균형점. N4 기준 재확인 완료.
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

N, R = 4, 5

def run(pe_max, gap_thr, exclude=frozenset(), phase=0):
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(2, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if (i - phase) % R == 0:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not industry_ok(tk): continue
                dv = DV.get(d, {}).get(tk)
                if dv is None or dv < 1000: continue
                if ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if pe_max is not None and v['px'] / v['nc'] > pe_max: continue
                if gap_thr is not None:
                    te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
                    if g is not None and g < gap_thr: continue
                cand.append((tk, rev90(v)))
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
    return (nav - 1) * 100, mdd * 100

if __name__ == '__main__':
    def phased(pe, gp, ex=frozenset()):
        rs = []; mm = []
        for ph in range(R):
            r, m = run(pe, gp, ex, ph); rs.append(r); mm.append(m)
        return sum(rs) / R, min(rs), max(rs), min(mm)
    print('=== PER 스윕 (gap2.5 고정, top4 R5, 위상평균) ===')
    for pe in [15, 20, 25, 30, 35, 40, None]:
        a, lo, hi, m = phased(pe, 2.5); ea, _, _, em = phased(pe, 2.5, frozenset(['SNDK', 'MU']))
        print(f'{str(pe):>5} {a:+7.0f}% ({lo:+.0f}~{hi:+.0f}) MDD{m:+.0f}% | ex2 {ea:+.0f}% MDD{em:+.0f}%')
    print('=== gap 스윕 (PER30 고정) ===')
    for gp in [None, 1.5, 2.0, 2.5, 3.0, 3.5]:
        a, lo, hi, m = phased(30, gp); ea, _, _, em = phased(30, gp, frozenset(['SNDK', 'MU']))
        print(f'{str(gp):>5} {a:+7.0f}% ({lo:+.0f}~{hi:+.0f}) MDD{m:+.0f}% | ex2 {ea:+.0f}% MDD{em:+.0f}%')
