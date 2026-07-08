# -*- coding: utf-8 -*-
"""BT 불일치 규명 이분탐색 (2026-07-09).
A(per_gap_grid run(), 1/len(hold) 가중, ad 무제한) vs B(dsr_shuffle run_daily(), 1/N 고정+현금, ad<=7/2)
축: ①종료일(7/2 컷 vs 최신) ②가중(1/n 집중 vs 1/N 현금) ③dv 소스(parquet vs DB dollar_volume_30d)
프로덕션 변경 없음. python research/bt_reconcile_2026_07_09.py
"""
import sys, os, json, sqlite3
import numpy as np
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
AD_ALL = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
FULL = {}
for tk, d, px, nc, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
# DB dv (dollar_volume_30d, $M 단위 가정 — parquet과 동일 스케일인지 아래서 검증)
DVDB = {}
for tk, d, dv in c.execute('SELECT ticker,date,dollar_volume_30d FROM ntm_screening WHERE dollar_volume_30d IS NOT NULL'):
    DVDB.setdefault(d, {})[tk] = float(dv)
conn.close()

DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DVPQ = {d: {t: (None if pd.isna(DVF.loc[d, t]) else float(DVF.loc[d, t])) for t in DVF.columns} for d in DVF.index}

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

def gate_pass(d, pe_max, gap_thr, dvmap, exclude=frozenset()):
    out = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not industry_ok(tk): continue
        dv = dvmap.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > pe_max: continue
        if gap_thr:
            te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
            if g is not None and g < gap_thr: continue
        out.append((tk, rev90(v)))
    return out

def run(ad, dvmap, weight_mode, pe_max=30, gap_thr=2.5, phase=0, N=4, R=5, start=2,
        exclude=frozenset(), trace=False):
    """weight_mode: 'conc'=1/len(hold) (A방식), 'cash'=1/N 고정+현금 (B방식)"""
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; log = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px = {t: vv['px'] for t, vv in FULL.get(d, {}).items()}
        ppx = {t: vv['px'] for t, vv in FULL.get(pv, {}).items()}
        n = len(hold); drr = 0.0
        denom = (n if weight_mode == 'conc' else N)
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / denom) * (cu - pp) / pp
        nav *= (1 + drr); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        if i % R == phase:
            cand = gate_pass(d, pe_max, gap_thr, dvmap, exclude)
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
            if trace: log.append((d, len(cand), list(hold)))
    return (nav - 1) * 100, mdd * 100, log

AD_CUT = [d for d in AD_ALL if d <= '2026-07-02']

def phase_avg(ad, dvmap, wm, **kw):
    rs = [run(ad, dvmap, wm, phase=p, **kw) for p in range(5)]
    return np.mean([r[0] for r in rs]), np.mean([r[1] for r in rs]), [round(r[0], 1) for r in rs]

if __name__ == '__main__':
    # dv 스케일 정합 검증 (같은 날짜·종목 몇 개 비교)
    d0 = '2026-06-15'
    common = [t for t in list(DVPQ.get(d0, {}))[:2000] if t in DVDB.get(d0, {}) and DVPQ[d0][t] is not None][:5]
    print('dv 스케일 검증 @', d0, [(t, round(DVPQ[d0][t], 1), round(DVDB[d0][t], 1)) for t in common])
    # DB dv 커버리지
    for d in ['2026-02-17', '2026-05-15', '2026-07-01', '2026-07-06', '2026-07-07']:
        print(f'  DB dv n({d}) =', len(DVDB.get(d, {})), ' parquet n =', sum(1 for x in DVPQ.get(d, {}).values() if x is not None))

    print('\n=== 이분탐색 표 (top4 R5 PER<=30 gap>=2.5, 위상0~4 평균수익%/MDD평균) ===')
    rows = [
        ('B 원형: ad<=7/2, dv=parquet, 1/N현금', AD_CUT, DVPQ, 'cash'),
        ('step1 가중만 A로: ad<=7/2, dv=parquet, 1/n집중', AD_CUT, DVPQ, 'conc'),
        ('step2 ad만 최신: ad=전체(~7/7), dv=parquet, 1/N현금', AD_ALL, DVPQ, 'cash'),
        ('step3 A 원형: ad=전체(~7/7), dv=parquet, 1/n집중', AD_ALL, DVPQ, 'conc'),
        ('step4 dv만 DB: ad<=7/2, dv=DB, 1/N현금', AD_CUT, DVDB, 'cash'),
        ('step5 dv=DB+최신: ad=전체, dv=DB, 1/N현금', AD_ALL, DVDB, 'cash'),
        ('step6 dv=DB+최신+집중: ad=전체, dv=DB, 1/n집중', AD_ALL, DVDB, 'conc'),
    ]
    results = {}
    for name, ad, dvm, wm in rows:
        m, md, per = phase_avg(ad, dvm, wm)
        results[name] = (m, md, per)
        print(f'  {name:<55} {m:+7.1f}% / {md:+5.1f}%   위상별 {per}')

    # 슬롯 미달(len(hold)<4) 리밸 발생 여부 — 가중 규약이 실제로 갈리는 지점
    print('\n=== 리밸일 후보수/보유수 추적 (ad<=7/2, dv=parquet, phase=2) ===')
    _, _, log = run(AD_CUT, DVPQ, 'cash', phase=2, trace=True)
    for d, nc_, h in log:
        flag = ' <-- 슬롯미달' if len(h) < 4 else ''
        print(f'  {d}: 통과 {nc_:3d}개, 보유 {len(h)}개 {h}{flag}')
