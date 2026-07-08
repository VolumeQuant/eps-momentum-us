# -*- coding: utf-8 -*-
"""VM(가치게이트+모멘텀 topN) 정본 BT 하네스 — 단일 구현 (2026-07-09).

배경: 같은 전략이 +115(per_gap_grid 인라인, 위상0 단일) vs +104.9(dsr_shuffle, 위상평균)로
보고돼 온 원인 = **위상 규약 차이**(데이터/dv/가중 아님, research/bt_reconcile_2026_07_09.py 이분탐색으로 확정).
이후 모든 VM 계열 BT는 이 함수 하나로 돌리고, 반드시 위상 0~R-1 평균을 정본으로 보고한다.
(앵커위상0 단일 수치는 '위상0'이라 명시할 때만 병기 허용 — 위상0은 91일 창에서 +10p 낙관 편향)

정본 사양:
- 유니버스/자격: ntm_screening(price>0, ntm_current>0) → 업종제외(ticker_info_cache.json,
  COMMODITY+OFF_STRATEGY+COMMODITY_TICKERS) → dv>=$1B(missing=컷) → min_seg>=0 → nc>0, n90>0.1
- 게이트: fwd_PER=price/ntm_current <= pe_max · gap=ntm_current/TTM_EPS >= gap_thr
  (data_cache/trailing_eps_ttm.json PIT, missing=통과)
- 선택: rev90=(nc-n90)/|n90| 내림차순 topN, 각 1/N 고정 가중, 빈 슬롯/가격결측=현금
- 캘린더: ad = DB 전체 날짜 오름차순(2026-02-12=인덱스0), start=2(첫 리밸=2026-02-17),
  리밸 조건 i % R == phase. end_date로 구간 컷(ad 뒤쪽만 잘림 → 위상 정렬 불변).
- dv 소스: 기본 'db' = production ntm_screening.dollar_volume_30d ($M).
  ('parquet'=research/dv_full_2026_07_04.parquet은 2026-07-02까지만 — 7/6+ 구멍, 회귀비교 전용)
- 정본 보고: 위상 0~R-1 각각 (수익%, MDD%) + 평균.

사용: python research/vm_canonical_bt.py [end_date]
"""
import sys, os, json, sqlite3, functools
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr


@functools.lru_cache(maxsize=1)
def _load():
    conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
    ad = tuple(r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'))
    full = {}
    for tk, d, px, nc, n7, n30, n60, n90 in c.execute(
            'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
            'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
        full.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
    dvdb = {}
    for tk, d, dv in c.execute(
            'SELECT ticker,date,dollar_volume_30d FROM ntm_screening WHERE dollar_volume_30d IS NOT NULL'):
        dvdb.setdefault(d, {})[tk] = float(dv)
    conn.close()
    tc = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
    te = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
    return ad, full, dvdb, tc, te


@functools.lru_cache(maxsize=1)
def _load_dv_parquet():
    import pandas as pd
    dvf = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
    dvf.index = pd.to_datetime(dvf.index).strftime('%Y-%m-%d')
    return {d: {t: (None if pd.isna(dvf.loc[d, t]) else float(dvf.loc[d, t]))
                for t in dvf.columns} for d in dvf.index}


_BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
_BAD_TK = set(dr.COMMODITY_TICKERS)


def _industry_ok(tk, tc):
    if tk in _BAD_TK: return False
    v = tc.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in _BAD_IND)


def _pit_te(te, tk, d):
    r = te.get(tk); v = None
    if not r: return None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v


def _ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)


def _rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0


def canonical_bt(pe_max=30, gap_thr=2.5, N=4, R=5, start=2, end_date=None,
                 dv_source='db', dv_min=1000.0, phase=0, exclude=frozenset(),
                 return_daily=False, trace=False):
    """정본 BT 1회(단일 위상). 반환 (수익%, MDD%) 또는 return_daily=True 시 (dates, rets, log)."""
    ad, FULL, DVDB, TC, TE = _load()
    if end_date: ad = tuple(d for d in ad if d <= end_date)
    dvmap = DVDB if dv_source == 'db' else _load_dv_parquet()
    hold = []; rets = []; dates = []; log = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]
        px = FULL.get(d, {}); ppx = FULL.get(pv, {})
        drr = 0.0
        for t in hold:  # 고정 1/N + 현금 (빈 슬롯/가격결측=현금)
            cu = px.get(t, {}).get('px'); pp = ppx.get(t, {}).get('px')
            if cu and pp and pp > 0: drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr); dates.append(d)
        if i % R == phase:
            cand = []
            for tk, v in FULL.get(d, {}).items():
                if tk in exclude or not _industry_ok(tk, TC): continue
                dv = dvmap.get(d, {}).get(tk)
                if dv is None or dv < dv_min: continue
                if _ms(v) < 0: continue
                if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
                if v['px'] / v['nc'] > pe_max: continue
                if gap_thr:
                    te_v = _pit_te(TE, tk, d); g = (v['nc'] / te_v) if (te_v and te_v > 0) else None
                    if g is not None and g < gap_thr: continue
                cand.append((tk, _rev90(v)))
            cand.sort(key=lambda x: -x[1]); hold = [t for t, _ in cand[:N]]
            if trace: log.append((d, len(cand), list(hold)))
    r = np.array(rets)
    nav = np.cumprod(1 + r); peak = np.maximum.accumulate(nav)
    tot = float(nav[-1] - 1) * 100; mdd = float((nav / peak - 1).min()) * 100
    if return_daily: return dates, r, log
    return tot, mdd


def canonical_report(pe_max=30, gap_thr=2.5, N=4, R=5, end_date=None, **kw):
    """정본 보고: 위상 0~R-1 전부 + 평균. dict 반환."""
    per_phase = {p: canonical_bt(pe_max, gap_thr, N, R, phase=p, end_date=end_date, **kw)
                 for p in range(R)}
    return dict(per_phase=per_phase,
                avg_ret=float(np.mean([v[0] for v in per_phase.values()])),
                avg_mdd=float(np.mean([v[1] for v in per_phase.values()])),
                phase0=per_phase[0])


if __name__ == '__main__':
    end = sys.argv[1] if len(sys.argv) > 1 else None
    ad, *_ = _load()
    for label, ed in [('7/2 마감 기준', '2026-07-02'), (f'최신 데이터({ad[-1]}) 포함', end)]:
        rep = canonical_report(end_date=ed)
        print(f'=== top4 R5 PER<=30 gap>=2.5 — {label} (dv=DB) ===')
        for p, (t, m) in rep['per_phase'].items():
            print(f'  위상{p}: {t:+7.1f}% / MDD {m:+6.1f}%')
        print(f'  ★정본(위상평균): {rep["avg_ret"]:+.1f}% / MDD {rep["avg_mdd"]:+.1f}%'
              f'   (앵커위상0: {rep["phase0"][0]:+.1f}/{rep["phase0"][1]:+.1f})\n')
