# -*- coding: utf-8 -*-
"""VM 재설계 추가 robust 검증: 리밸런싱 위상(요일) 민감도 / N 민감도 / 6월 셀오프 구간 /
가중방식(동일 vs 모멘텀비례) / 거래비용 반영."""
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
for tk, d, p2, px, nc, n7, n30, n60, n90, ru in c.execute(
        'SELECT ticker,date,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,rev_up30 '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(p2=p2, px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90, ru=ru)
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

def pick(d, N, pe_max, use_gap=False):
    cand = []
    for tk, v in FULL.get(d, {}).items():
        if not industry_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > pe_max: continue
        if use_gap:
            te = pit_te(tk, d)
            g = (v['nc'] / te) if (te and te > 0) else None
            if g is not None and g < 2.5: continue
        cand.append((tk, rev90(v)))
    cand.sort(key=lambda x: -x[1])
    return [t for t, _ in cand[:N]]

def run(N, R, pe_max, start=2, end=None, phase=0, use_gap=False, cost_bp=0):
    end = end if end is not None else len(ad)
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; buys = 0
    for i in range(start, end):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        n = len(hold); drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / n) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % R == phase:
            tgt = pick(d, N, pe_max, use_gap)
            nb = len([t for t in tgt if t not in hold])
            buys += nb
            if cost_bp and n > 0:
                nav *= (1 - 2 * nb / max(len(tgt), 1) * cost_bp / 1e4)  # 왕복비용 근사
            hold = tgt
    return (nav - 1) * 100, mdd * 100, buys, set(hold)

print('=== ①리밸런싱 위상(어느 요일에 갈아타나) 민감도 — PER<=25 top5 R5 ===')
for g in [False, True]:
    rs = [run(5, 5, 25, phase=p, use_gap=g) for p in range(5)]
    rets = [r[0] for r in rs]; mdds = [r[1] for r in rs]
    print(f'  gap{"O" if g else "X"}: 수익 {min(rets):+.0f}~{max(rets):+.0f}% (평균{np.mean(rets):+.0f}) MDD {min(mdds):+.0f}~{max(mdds):+.0f}')

print('\n=== ②N 민감도 (PER<=25, R5, 위상평균) ===')
for N in [3, 4, 5, 6, 8]:
    rs = [run(N, 5, 25, phase=p) for p in range(5)]
    print(f'  top{N}: 평균 {np.mean([r[0] for r in rs]):+.0f}%  MDD평균 {np.mean([r[1] for r in rs]):+.0f}  (최악수익 {min(r[0] for r in rs):+.0f}%)')

print('\n=== ③PER게이트 (top5 R5, 위상평균 — 단일위상 노이즈 제거) ===')
for pe in [15, 20, 25, 30, 40, 999]:
    rs = [run(5, 5, pe, phase=p) for p in range(5)]
    print(f'  PER<={pe if pe<999 else "∞":>3}: 평균 {np.mean([r[0] for r in rs]):+.0f}%  MDD평균 {np.mean([r[1] for r in rs]):+.0f}')

# 6월 반도체 셀오프 구간 (6/15~7/2)
i0 = next(i for i, d in enumerate(ad) if d >= '2026-06-15')
print(f'\n=== ④6월 셀오프({ad[i0]}~{ad[-1]}) 구간 방어력 ===')
for nm, kw in [('VM PER25 top5', dict(N=5, R=5, pe_max=25)),
               ('VM PER25 top5 +gap', dict(N=5, R=5, pe_max=25, use_gap=True)),
               ('VM PER20 top3', dict(N=3, R=5, pe_max=20))]:
    r = run(start=i0, **kw)
    print(f'  {nm:22}: {r[0]:+.0f}%  MDD {r[1]:+.0f}')

print('\n=== ⑤거래비용 10bp 왕복 반영 (PER<=25 top5 R5, 위상0) ===')
for cb in [0, 10, 30]:
    r = run(5, 5, 25, cost_bp=cb)
    print(f'  비용 {cb}bp: {r[0]:+.0f}% (매수 {r[2]}회)')
