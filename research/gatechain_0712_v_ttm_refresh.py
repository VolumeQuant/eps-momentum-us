# -*- coding: utf-8 -*-
"""적대적 검증: 권고 ttm-weekly-refresh-now (fetch_full_ttm 즉시 재실행 + 주1회 룰).
Part 1 (오프라인): full TTM 캐시 신선도 분포 + 최신일 게이트 통과자 중 missing=pass 비율.
Part 2 (온라인, --fetch): 현재 게이트 통과자 + gap컷 종목 대상 yf 신선 refetch → gap 플립 수 실측.
읽기 전용 — DB 쓰기/프로덕션 파일 수정 없음.
"""
import sys, os, json, sqlite3, time
from datetime import datetime
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

TE_FULL_PATH = os.path.join(BASE, 'data_cache', 'trailing_eps_ttm_full.json')
TE_SPARSE_PATH = os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json')
DB = os.path.join(BASE, 'eps_momentum_data.db')

TE = json.load(open(TE_FULL_PATH, encoding='utf-8'))
print(f"파일: {TE_FULL_PATH}")
print(f"  mtime: {datetime.fromtimestamp(os.path.getmtime(TE_FULL_PATH))}")
print(f"  종목 수: {len(TE)}  (레코드 있는 종목: {sum(1 for v in TE.values() if v)})")

# ─── 1. 신선도 분포: 종목별 최신 rdate(=분기말+45일) ───
lat = {}
for tk, rec in TE.items():
    if rec:
        lat[tk] = max(r[0] for r in rec)
vals = sorted(lat.values())
n = len(vals)
print(f"\n[1] 최신 rdate 분포 (유효 {n}종목)")
for thr in ('2026-03-01', '2026-04-01', '2026-05-01', '2026-05-15', '2026-06-01', '2026-07-01', '2026-07-12'):
    c = sum(1 for v in vals if v < thr)
    print(f"  rdate < {thr}: {c} ({c/n*100:.1f}%)")
print(f"  최신 rdate 상위 5: {vals[-5:]}")
print(f"  최신 rdate 하위 5: {vals[:5]}")
# 월별 히스토그램
from collections import Counter
hist = Counter(v[:7] for v in vals)
print('  월별:', dict(sorted(hist.items())))
# 해석 키: rdate = 분기말+45. 캘린더 Q1(3/31)말 → rdate 2026-05-15. Q2(6/30)말 → 2026-08-14.
# 즉 오늘(7/12) 시점 '유효 최신값'이 Q1분기까지인 종목 = rdate<=2026-05-15 근방.

# ─── 2. 최신일 게이트 통과자 중 missing=pass 비율 (unified_vm_track.us_candidates 로직 재현) ───
import daily_runner as dr
TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)

def ind_ok(tk):
    if tk in BAD_TK:
        return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD)

def seg(a, b):
    try:
        return (a - b) / abs(b)
    except Exception:
        return -9

conn = sqlite3.connect(DB)
c = conn.cursor()
last = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
print(f"\n[2] 최신일 {last} 게이트 재현 (dv>=1000, PER<=30, gap>=1.5 missing=pass, A군 제외 전/후)")
fund = {}
for tk, om, fcf, roe in c.execute(
        "SELECT ticker, operating_margin, free_cashflow, roe FROM ntm_screening "
        "WHERE date<=? AND date>=date(?, '-60 day') ORDER BY date", (last, last)):
    e = fund.setdefault(tk, [None, None, None])
    if om is not None: e[0] = om
    if fcf is not None: e[1] = fcf
    if roe is not None: e[2] = roe

passers, gap_cut = [], []
for tk, p, nc, n7, n30, n60, n90, dv, na in c.execute(
        'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,'
        'num_analysts FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)):
    if not ind_ok(tk):
        continue
    if dv is None or dv < 1000:
        continue
    if min(seg(nc, n7), seg(n7, n30), seg(n30, n60), seg(n60, n90)) < 0:
        continue
    if nc <= 0 or (n90 or 0) <= 0.1:
        continue
    if p / nc > 30:
        continue
    rec = TE.get(tk)
    te = rec[-1][1] if rec else None   # ★프로덕션 동일: [-1] = 캐시 최신 레코드 (PIT 아님, live)
    g = (nc / te) if (te and te > 0) else None
    row = dict(tk=tk, px=p, nc=nc, n90=n90, te=te, gap=g, rev90=seg(nc, n90), na=na)
    if g is not None and g < 1.5:
        gap_cut.append(row)
        continue
    # A군
    om, fcf, roe = fund.get(tk, (None, None, None))
    if p < 10 or (na or 0) < 3 or seg(nc, n90) <= 0:
        continue
    if om is not None and om < 0.05:
        continue
    if fcf is not None and roe is not None and fcf < 0 and roe < 0:
        continue
    row['te_rdate'] = lat.get(tk)
    passers.append(row)
conn.close()

miss = [r for r in passers if r['gap'] is None]
print(f"  최종 통과자: {len(passers)}  | missing=pass: {len(miss)} ({len(miss)/max(len(passers),1)*100:.1f}%)")
stale = [r for r in passers if r['gap'] is not None and (r['te_rdate'] or '9999') < '2026-05-01']
print(f"  gap 계산됐지만 TTM rdate<2026-05-01(=Q1도 미반영 낡음): {len(stale)} ({len(stale)/max(len(passers),1)*100:.1f}%)")
print(f"  gap<1.5 컷: {len(gap_cut)}")
passers.sort(key=lambda r: -r['rev90'])
print("  rev90 상위 10 통과자 (tk, gap, te_rdate):")
for r in passers[:10]:
    print(f"    {r['tk']:6s} rev90={r['rev90']*100:+6.1f}% gap={('%.2f' % r['gap']) if r['gap'] else 'miss'} rdate={r.get('te_rdate')}")

json.dump(dict(last=last, passers=[r['tk'] for r in passers],
               gap_cut=[r['tk'] for r in gap_cut]),
          open(os.path.join(BASE, 'research', 'gatechain_0712_v_ttm_state.json'), 'w'))

# ─── 3. (--fetch) 신선 refetch로 gap 플립 실측 ───
if '--fetch' in sys.argv:
    import pandas as pd
    import yfinance as yf
    targets = [r['tk'] for r in passers] + [r['tk'] for r in gap_cut]
    print(f"\n[3] 신선 refetch 대상 {len(targets)}종목 (통과자 {len(passers)} + gap컷 {len(gap_cut)})")
    LAG = 45
    fresh = {}
    fails = []
    for i, tk in enumerate(targets):
        try:
            qi = yf.Ticker(tk).quarterly_income_stmt
            rec = []
            if qi is not None and not qi.empty:
                row = None
                for k in ('Diluted EPS', 'Basic EPS'):
                    if k in qi.index:
                        row = qi.loc[k]
                        break
                if row is not None:
                    q = row.dropna().sort_index()
                    qe = list(q.items())
                    for j in range(3, len(qe)):
                        ttm = sum(float(qe[j - k][1]) for k in range(4))
                        rdate = (qe[j][0] + pd.Timedelta(days=LAG)).strftime('%Y-%m-%d')
                        rec.append([rdate, ttm])
            fresh[tk] = rec
        except Exception as e:
            fails.append(tk)
        time.sleep(0.3)
        if (i + 1) % 25 == 0:
            print(f"  … {i+1}/{len(targets)}", flush=True)
    print(f"  refetch 완료: {len(fresh)} 성공, 실패 {fails}")
    json.dump(fresh, open(os.path.join(BASE, 'research', 'gatechain_0712_v_ttm_fresh.json'), 'w'))

    def gap_of(tk, nc, rec):
        te = rec[-1][1] if rec else None
        return (nc / te) if (te and te > 0) else None

    flips_out, flips_in, changed = [], [], []
    for r in passers:
        g_new = gap_of(r['tk'], r['nc'], fresh.get(r['tk'], TE.get(r['tk'])))
        g_old = r['gap']
        if g_old is not None and g_new is not None and abs(g_new - g_old) > 1e-9:
            changed.append((r['tk'], round(g_old, 2), round(g_new, 2)))
        if (g_old is None or g_old >= 1.5) and (g_new is not None and g_new < 1.5):
            flips_out.append((r['tk'], g_old and round(g_old, 2), round(g_new, 2), round(r['rev90']*100, 1)))
    for r in gap_cut:
        g_new = gap_of(r['tk'], r['nc'], fresh.get(r['tk'], TE.get(r['tk'])))
        if g_new is None or g_new >= 1.5:
            flips_in.append((r['tk'], round(r['gap'], 2), g_new and round(g_new, 2)))
    print(f"\n  통과→컷 플립(신선 TTM으로 gap<1.5): {len(flips_out)} {flips_out}")
    print(f"  컷→통과 플립: {len(flips_in)} {flips_in}")
    print(f"  gap 값 변경(통과자 중): {len(changed)}")
    for t in changed[:20]:
        print(f"    {t}")
    # top5 변화 확인
    def top5(gapf):
        ok = []
        for r in passers:
            g = gapf(r)
            if g is not None and g < 1.5:
                continue
            ok.append(r)
        ok.sort(key=lambda x: -x['rev90'])
        return [x['tk'] for x in ok[:5]]
    t5_old = top5(lambda r: r['gap'])
    t5_new = top5(lambda r: gap_of(r['tk'], r['nc'], fresh.get(r['tk'], TE.get(r['tk']))))
    # 컷→통과 복귀 종목은 rev90 랭킹 재진입 필요
    pool_new = list(passers)
    for r in gap_cut:
        g_new = gap_of(r['tk'], r['nc'], fresh.get(r['tk'], TE.get(r['tk'])))
        if g_new is None or g_new >= 1.5:
            # A군 재검
            om, fcf, roe = fund.get(r['tk'], (None, None, None))
            if r['px'] < 10 or (r['na'] or 0) < 3 or r['rev90'] <= 0:
                continue
            if om is not None and om < 0.05:
                continue
            if fcf is not None and roe is not None and fcf < 0 and roe < 0:
                continue
            rr = dict(r); rr['gap'] = g_new
            pool_new.append(rr)
    pool_new = [r for r in pool_new
                if (lambda g: g is None or g >= 1.5)(gap_of(r['tk'], r['nc'], fresh.get(r['tk'], TE.get(r['tk']))))]
    pool_new.sort(key=lambda x: -x['rev90'])
    t5_new_full = [x['tk'] for x in pool_new[:5]]
    print(f"\n  US top5 (구 캐시): {t5_old}")
    print(f"  US top5 (신선 TTM, 컷복귀 포함): {t5_new_full}")
    print(f"  → top5 변화: {'없음' if t5_old == t5_new_full else '있음 ★'}")
