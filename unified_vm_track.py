# -*- coding: utf-8 -*-
"""US+KR 통합 VM top4 — 페이퍼 병기 트랙 (2026-07-08, 사용자 지시. 매매 무관·기록 전용)

규칙 = US 재설계와 동일: min_seg>=0 + 거래대금 $1B + 업종제외 → fwd_PER<=30 + gap>=2.5(missing=pass)
→ rev90 내림차순 top4 각 25%, R5(앵커 = 로그 첫날). KR 종목도 같은 게이트를 실데이터로 통과:
  - gap: KR = yf NTM(원화) ÷ DART TTM 지배순이익 EPS(원화, fs_dart + 주식수=시총/가격 역산)
  - 거래대금: KR = yfinance 30일 평균 거래대금 × 환율(USDKRW) — 실패 시 시총>=13조 프록시
  - 업종: US = 기존 필터 / KR = 지주 제외 + 애널>=5(저커버 KQ 턴어라운드 artifact 차단)
근거: research/kr_merge_universe_2026_07_08.py (6월 실측 — 메모리 쏠림 증폭 확인, 포워드 판정 대기)
사용: --run (오늘 계산+로그 append+출력) / --nav (로그 리플레이 NAV)
로그: data_cache/unified_vm_log.csv (append-only)
"""
import os, sys, json, csv, sqlite3
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
KR_DB = 'C:/dev/kr_eps_momentum/eps_momentum_data_kr.db'
KR_FS_DIR = 'C:/dev/data_cache'
LOG = os.path.join(HERE, 'data_cache', 'unified_vm_log.csv')
N_TOP, REBAL = 4, 5
PE_MAX, GAP_MIN, DV_MIN_MUSD = 30.0, 2.5, 1000.0
KR_HOLDCO = {'402340.KS'}  # SK스퀘어(지주) — KR production 지주제외 준용


def _seg(a, b):
    return (a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0


def _fx_usdkrw():
    try:
        import yfinance as yf
        v = yf.Ticker('KRW=X').fast_info.last_price
        if v and 900 < v < 2500:
            return float(v)
    except Exception:
        pass
    return 1380.0


def _kr_ttm_eps(t6, shares):
    """DART fs_dart 분기 지배순이익(억원) TTM ÷ 주식수. conviction_fusion_tracker.ttm_eps 로직 준용."""
    import pandas as pd
    p = f'{KR_FS_DIR}/fs_dart_{t6}.parquet'
    if not os.path.exists(p) or not (shares and shares > 0):
        return None
    try:
        fs = pd.read_parquet(p)
        fs['rcept_dt'] = pd.to_datetime(fs['rcept_dt'], errors='coerce')
        for acct in ('지배주주당기순이익', '당기순이익'):
            q = fs[(fs['공시구분'] == 'q') & (fs['계정'] == acct) & (fs['rcept_dt'].notna())].sort_values('rcept_dt')
            v = q['값'].astype(float).values
            if len(v) >= 4:
                return (v[-4:].sum() * 1e8) / shares
    except Exception:
        return None
    return None


def us_candidates():
    import daily_runner as dr
    TC = json.load(open(os.path.join(HERE, 'ticker_info_cache.json'), encoding='utf-8'))
    BAD = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
    BAD_TK = set(dr.COMMODITY_TICKERS)

    def ind_ok(tk):
        if tk in BAD_TK:
            return False
        v = TC.get(tk)
        ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
        return not (isinstance(ind, str) and ind in BAD)

    TE = json.load(open(os.path.join(HERE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))
    conn = sqlite3.connect(os.path.join(HERE, 'eps_momentum_data.db'))
    c = conn.cursor()
    last = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    out = []
    for tk, p, nc, n7, n30, n60, n90, dv in c.execute(
            'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d '
            'FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)):
        if not ind_ok(tk):
            continue
        if dv is None or dv < DV_MIN_MUSD:
            continue
        if min(_seg(nc, n7), _seg(n7, n30), _seg(n30, n60), _seg(n60, n90)) < 0:
            continue
        if nc <= 0 or (n90 or 0) <= 0.1:
            continue
        if p / nc > PE_MAX:
            continue
        rec = TE.get(tk)
        te = rec[-1][1] if rec else None
        g = (nc / te) if (te and te > 0) else None
        if g is not None and g < GAP_MIN:
            continue
        out.append(dict(ticker=tk, market='US', rev90=_seg(nc, n90), fwd_per=p / nc,
                        gap=g, dv_musd=dv, price=p))
    conn.close()
    return last, out


def kr_candidates(fx):
    conn = sqlite3.connect(KR_DB)
    c = conn.cursor()
    last = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    rows = c.execute(
        'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,market_cap,num_analysts '
        'FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)).fetchall()
    conn.close()
    pre = []
    for tk, p, nc, n7, n30, n60, n90, mc, na in rows:
        if tk in KR_HOLDCO:
            continue
        if (na or 0) < 5:
            continue
        if min(_seg(nc, n7), _seg(n7, n30), _seg(n30, n60), _seg(n60, n90)) < 0:
            continue
        if nc <= 0 or (n90 or 0) <= 0.1:
            continue
        if p / nc > PE_MAX:
            continue
        shares = (mc / p) if (mc and p) else None
        te = _kr_ttm_eps(tk.split('.')[0], shares)
        g = (nc / te) if (te and te > 0) else None
        if g is not None and g < GAP_MIN:
            continue
        pre.append(dict(ticker=tk, market='KR', rev90=_seg(nc, n90), fwd_per=p / nc,
                        gap=g, dv_musd=None, price=p, mc=mc))
    # 거래대금 게이트: yf 30일 평균 거래대금 → USD. 실패 시 시총>=13조 프록시.
    if pre:
        try:
            import yfinance as yf
            hist = yf.download([d['ticker'] for d in pre], period='2mo', threads=2,
                               progress=False, auto_adjust=False)
            for d in pre:
                try:
                    cl = hist['Close'][d['ticker']].dropna()
                    vo = hist['Volume'][d['ticker']].dropna()
                    dvk = (cl * vo).tail(30).mean()
                    d['dv_musd'] = float(dvk) / fx / 1e6
                except Exception:
                    d['dv_musd'] = None
        except Exception:
            pass
    out = []
    for d in pre:
        if d['dv_musd'] is not None:
            if d['dv_musd'] < DV_MIN_MUSD:
                continue
        elif (d.get('mc') or 0) < 13e12:
            continue
        d.pop('mc', None)
        out.append(d)
    return last, out


def compute():
    fx = _fx_usdkrw()
    us_date, us = us_candidates()
    kr_date, kr = kr_candidates(fx)
    merged = sorted(us + kr, key=lambda d: -d['rev90'])
    return us_date, kr_date, fx, merged


def cmd_run():
    us_date, kr_date, fx, merged = compute()
    run_date = datetime.now().strftime('%Y-%m-%d')
    print(f'=== 통합 VM top4 (US {us_date} / KR {kr_date}, USDKRW {fx:.0f}) ===')
    for i, d in enumerate(merged[:10], 1):
        mark = ' ★top4' if i <= N_TOP else ''
        gap_s = f"{d['gap']:.1f}" if d['gap'] else 'pass'
        print(f"{i:2}. [{d['market']}] {d['ticker']:10} rev90 {d['rev90']:+7.1f}%  "
              f"fwdPER {d['fwd_per']:5.1f}  gap {gap_s:>5}  dv ${(d['dv_musd'] or 0):,.0f}M{mark}")
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    new = not os.path.exists(LOG)
    with open(LOG, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if new:
            w.writerow(['run_date', 'us_date', 'kr_date', 'rank', 'market', 'ticker',
                        'rev90', 'fwd_per', 'gap', 'dv_musd', 'price', 'in_top4'])
        for i, d in enumerate(merged[:10], 1):
            w.writerow([run_date, us_date, kr_date, i, d['market'], d['ticker'],
                        round(d['rev90'], 2), round(d['fwd_per'], 2),
                        round(d['gap'], 3) if d['gap'] else '',
                        round(d['dv_musd'], 1) if d['dv_musd'] else '', d['price'], int(i <= N_TOP)])
    print(f'로그 append: {LOG}')


def cmd_nav():
    """로그 리플레이 NAV — R5(로그 앵커 기준) top4 EW, 로컬통화 수익률(FX 미반영) 근사."""
    if not os.path.exists(LOG):
        print('로그 없음')
        return
    import pandas as pd
    df = pd.read_csv(LOG, dtype={'ticker': str})
    days = sorted(df['run_date'].unique())
    hold = []
    nav = 1.0
    prev_px = {}
    for i, d in enumerate(days):
        day = df[df['run_date'] == d]
        px = dict(zip(day['ticker'], day['price']))
        if hold:
            r = [px[t] / prev_px[t] - 1 for t in hold if t in px and t in prev_px and prev_px[t] > 0]
            if r:
                nav *= 1 + sum(r) / len(r)
        if i % REBAL == 0:
            hold = day[day['in_top4'] == 1]['ticker'].tolist()
        prev_px.update(px)
    print(f'통합 트랙 NAV: {(nav - 1) * 100:+.2f}% ({days[0]} ~ {days[-1]}, {len(days)}일)')


if __name__ == '__main__':
    sys.path.insert(0, HERE)
    if '--nav' in sys.argv:
        cmd_nav()
    else:
        cmd_run()
