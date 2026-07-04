# -*- coding: utf-8 -*-
"""리밸 주기 R 스윕 (최종 추천안 PER<=30 + gap>=2.5 + 모멘텀 top5 고정).
질문(사용자): '왜 하필 5거래일마다인가?' — R=1(매일)~20(월1회)을 위상평균+LOWO로 비교."""
import sys, os, json, sqlite3
import numpy as np
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

def run(pe_max, gap_thr, phase=0, exclude=frozenset(), N=5, R=5, start=2, cost_bp=0.0):
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; trades = 0
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % R == phase % R:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not industry_ok(tk): continue
                dv = DV.get(d, {}).get(tk)
                if dv is None or dv < 1000: continue
                if ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if v['px'] / v['nc'] > pe_max: continue
                if gap_thr:
                    te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
                    if g is not None and g < gap_thr: continue
                cand.append((tk, rev90(v)))
            cand.sort(key=lambda x: -x[1]); new = [t for t, _ in cand[:N]]
            diff = len(set(new) - set(hold)) + len(set(hold) - set(new))
            trades += diff
            if cost_bp and hold:  # 편도 비용: 바뀐 슬롯 비중만큼
                nav *= (1 - cost_bp / 1e4 * diff / max(N, 1))
            hold = new
    return (nav - 1) * 100, mdd * 100, trades

print('=== 리밸 주기 R 스윕 (PER<=30 + gap>=2.5 + 모멘텀 top5) ===')
print(f'{"R":>3} | {"수익(위상평균)":>14} | {"범위":>12} | {"MDD평균":>7} | {"회전(편도슬롯)":>10} | {"ex-SNDK/MU":>10} | {"비용30bp후":>9}')
for R in [1, 2, 3, 5, 7, 10, 15, 20]:
    rs = [run(30, 2.5, phase=p, R=R) for p in range(R)]
    rets = [r[0] for r in rs]; mdds = [r[1] for r in rs]; trs = [r[2] for r in rs]
    lo = run(30, 2.5, exclude=frozenset(['SNDK', 'MU']), R=R)
    cost = np.mean([run(30, 2.5, phase=p, R=R, cost_bp=30)[0] for p in range(R)])
    print(f'{R:>3} | {np.mean(rets):+13.0f}% | {min(rets):+4.0f}~{max(rets):+4.0f}% | {np.mean(mdds):+6.0f}% | {np.mean(trs):>10.0f} | {lo[0]:+9.0f}% | {cost:+8.0f}%')
print('\n읽는법: 위상=리밸 시작 오프셋(R개 전부 평균). 회전=리밸일 슬롯 교체 횟수 합. 비용=편도 30bp 가정.')
