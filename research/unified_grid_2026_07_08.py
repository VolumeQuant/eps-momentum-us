# -*- coding: utf-8 -*-
"""통합(US+KR) 유니버스 전 조건 그리드 (2026-07-08, 사용자 "N 말고 다른 조건들도 전부").

⚠️ 목적 = 지형 파악 + 포워드 판정 항목 설정. 25거래일(6/1~7/7)·급락 1회 표본이라
여기서 나온 '최적값'을 채택하는 건 과적합 — 채택 판정은 unified_vm_track 포워드 누적으로.

스윕: R / PER캡 / gap임계 / min_seg / 비중 / 테마캡×N. 판정축 = 위상평균 수익 + 최악MDD
(급락 창이라 MDD가 더 의미).
"""
import sys, os, json, sqlite3
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr
from unified_vm_track import _kr_ttm_eps

# ── 데이터 로드 ──
conn = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db')); c = conn.cursor()
US_FULL = {}
DV = {}
# dv는 production DB(전종목 백필본) 사용 — research parquet(7/4까지)은 7/6+ 구멍(★US 전탈락 버그)
for tk, d, px, nc, n7, n30, n60, n90, dv in c.execute(
        'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0 AND date>="2026-06-01"'):
    US_FULL.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
    DV.setdefault(d, {})[tk] = dv
conn.close()
TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)
def get_ind(tk):
    v = TC.get(tk)
    return v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
def ind_ok(tk):
    if tk in BAD_TK: return False
    ind = get_ind(tk)
    return not (isinstance(ind, str) and ind in BAD)
TE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
def pit_te(tk, d):
    r = TE.get(tk); v = None
    if not r: return None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v
konn = sqlite3.connect('C:/dev/kr_eps_momentum/eps_momentum_data_kr.db'); kc = konn.cursor()
KR_FULL = {}; KR_TTM = {}
for tk, d, px, nc, n7, n30, n60, n90, mc, na in kc.execute(
        'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,market_cap,num_analysts '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    KR_FULL.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90, mc=mc, na=na)
    if tk not in KR_TTM:
        sh = (mc / px) if (mc and px) else None
        KR_TTM[tk] = _kr_ttm_eps(tk.split('.')[0], sh)
konn.close()
def seg(a, b):
    return (a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0
dates = [d for d in sorted(US_FULL) if d >= '2026-06-01']
KR_IND = {t: '메모리반도체' if t in ('005930.KS', '000660.KS') else 'KR기타' for d in KR_FULL for t in KR_FULL[d]}

def candidates(d, pe_max, gap_thr, ms_min):
    out = []
    for tk, v in US_FULL.get(d, {}).items():
        if not ind_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if min(seg(v['nc'], v['n7']), seg(v['n7'], v['n30']), seg(v['n30'], v['n60']), seg(v['n60'], v['n90'])) < ms_min: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if pe_max and v['px'] / v['nc'] > pe_max: continue
        te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
        if gap_thr and g is not None and g < gap_thr: continue
        out.append((tk, seg(v['nc'], v['n90'])))
    prior = [x for x in sorted(KR_FULL) if x <= d]
    kd = prior[-1] if prior else None
    if kd:
        for tk, v in KR_FULL[kd].items():
            if tk == '402340.KS': continue
            if (v['mc'] or 0) < 13e12 or (v['na'] or 0) < 5: continue
            if min(seg(v['nc'], v['n7']), seg(v['n7'], v['n30']), seg(v['n30'], v['n60']), seg(v['n60'], v['n90'])) < ms_min: continue
            if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
            if pe_max and v['px'] / v['nc'] > pe_max: continue
            te = KR_TTM.get(tk); g = (v['nc'] / te) if (te and te > 0) else None
            if gap_thr and g is not None and g < gap_thr: continue
            out.append((tk, seg(v['nc'], v['n90'])))
    out.sort(key=lambda x: -x[1])
    return out

def px_of(tk, d):
    if tk.endswith('.KS') or tk.endswith('.KQ'):
        for x in reversed([x for x in sorted(KR_FULL) if x <= d]):
            if tk in KR_FULL[x]: return KR_FULL[x][tk]['px']
        return None
    return US_FULL.get(d, {}).get(tk, {}).get('px')

def industry_of(tk):
    return KR_IND.get(tk) or get_ind(tk) or 'NA'

def run(N=4, R=5, pe_max=30, gap_thr=2.5, ms_min=0, weights=None, theme_cap=None, phase=0):
    hold = []; wv = []; nav = 1.0; peak = 1.0; mdd = 0.0
    for i in range(1, len(dates)):
        d, pv = dates[i], dates[i - 1]
        drr = 0.0
        for t, w in zip(hold, wv):
            cu, pp = px_of(t, d), px_of(t, pv)
            if cu and pp and pp > 0: drr += w * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if (i - phase) % R == 0:
            cand = candidates(d, pe_max, gap_thr, ms_min)
            if theme_cap is None:
                hold = [t for t, _ in cand[:N]]
            else:
                hold = []; cnt = {}
                for t, _ in cand:
                    ind = industry_of(t)
                    if cnt.get(ind, 0) >= theme_cap: continue
                    hold.append(t); cnt[ind] = cnt.get(ind, 0) + 1
                    if len(hold) >= N: break
            if weights is None:
                wv = [1.0 / len(hold)] * len(hold) if hold else []
            else:
                ws = weights[:len(hold)]
                tot = sum(ws) or 1
                wv = [w / tot for w in ws]
    return (nav - 1) * 100, mdd * 100

def phased(**kw):
    R = kw.get('R', 5)
    rs = []; mm = []
    for ph in range(R):
        r, m = run(phase=ph, **kw); rs.append(r); mm.append(m)
    return sum(rs) / len(rs), min(mm)

if __name__ == '__main__':
    print(f'=== 통합 유니버스 전 조건 그리드 (6/1~{dates[-1]}, {len(dates)}일, 위상평균/최악MDD) ===')
    print('-- R (리밸주기) --')
    for R in [1, 2, 3, 5, 7, 10]:
        a, m = phased(R=R); print(f'  R{R:<3} {a:+7.1f}% {m:+7.1f}%')
    print('-- PER 캡 (gap2.5 고정) --')
    for pe in [15, 20, 25, 30, 35, 40, None]:
        a, m = phased(pe_max=pe); print(f'  PER{str(pe):<5} {a:+7.1f}% {m:+7.1f}%')
    print('-- gap 임계 (PER30 고정) --')
    for g in [None, 2.0, 2.5, 3.0, 3.5]:
        a, m = phased(gap_thr=g); print(f'  gap{str(g):<5} {a:+7.1f}% {m:+7.1f}%')
    print('-- min_seg --')
    for ms in [-2, -1, 0, 0.5, 1]:
        a, m = phased(ms_min=ms); print(f'  ms{ms:<5} {a:+7.1f}% {m:+7.1f}%')
    print('-- 비중 --')
    for lbl, w in [('동일 25x4', None), ('선형 40/30/20/10', [40, 30, 20, 10])]:
        a, m = phased(weights=w); print(f'  {lbl:16} {a:+7.1f}% {m:+7.1f}%')
    print('-- 테마캡 × N (핵심 가설) --')
    for N in [4, 5, 6]:
        for cap in [None, 2]:
            a, m = phased(N=N, theme_cap=cap)
            print(f'  N{N} 캡{str(cap):<4} {a:+7.1f}% {m:+7.1f}%')
