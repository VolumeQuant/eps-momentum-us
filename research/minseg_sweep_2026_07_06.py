# -*- coding: utf-8 -*-
"""min_seg 진입게이트 임계 스윕 (2026-07-06, 사용자 질의 "0이 최적 맞나").
하네스 = weight_sweep/divergence_weight와 동일(top4 PER<=30 gap>=2.5 R5), min_seg 컷만 변형.
결과(위상평균/최악MDD/ex2): 없음 +105/-20/+61 | -5 +113/-20 | -2 +122/-17/+63 | -1 +122/-17/+67
| -0.5 +115/-17 | 0(현행) +115/-18/+55 | +0.5 +118/-18/ex2+43·MDD-26 | +1 +102/-21 | +2 +111/-27/ex2+7
| +3 +110/-28 | +5 +49/-32.
판정: 0 유지. ①-1~-2 우위 +7p는 위상범위(±15p)내 노이즈 + 표면 들쭉(+0.5서 ex2 붕괴=비단조)
②완화 이득은 91일 강세장 특성 — ms>=0의 존재가치는 전망 꺾임 초기 회피(이 표본에 그 국면 없음),
안전규칙 완화는 short-window BT로 결정 금지(교훈 메모리) ③0=자연경계(하향 0개), -1=신규 튜닝숫자(과적합 취약)
④조임(+0.5 이상)은 명확히 손해 — 이 방향만 확정 정보.

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
def run(msmin, exclude=frozenset(), phase=0):
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
                if msmin is not None and ms(v) < msmin: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if v['px'] / v['nc'] > PE_MAX: continue
                te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
                if g is not None and g < 2.5: continue
                cand.append((tk, rev90(v)))
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
    return (nav - 1) * 100, mdd * 100

if __name__ == '__main__':
    print('=== min_seg 임계 스윕 (top4 PER<=30 gap>=2.5 R5, 위상평균) ===')
    for thr in [None, -5, -2, -1, -0.5, 0, 0.5, 1, 2, 3, 5]:
        rs = []; mm = []
        for ph in range(R):
            r, m = run(thr, phase=ph); rs.append(r); mm.append(m)
        ers = []; emm = []
        for ph in range(R):
            r, m = run(thr, frozenset(['SNDK', 'MU']), ph); ers.append(r); emm.append(m)
        lbl = 'none' if thr is None else f'{thr:+.1f}%'
        print(f'{lbl:>7} {sum(rs)/R:+7.0f}% ({min(rs):+.0f}~{max(rs):+.0f}) MDD{min(mm):+.0f}% | ex2 {sum(ers)/R:+.0f}% MDD{min(emm):+.0f}%')
