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
# ★2026-07-09 프로덕션 재캘리브레이션 동기화: gap 2.5→1.5(전수검사 기준), top4→top5.
#   US 프로덕션과 패리티 유지가 이 트랙의 존재 이유(같은 게이트를 양국에). in_top4 컬럼명은 로그 연속성
#   위해 유지(의미 = topN 멤버십). ⚠️gap 1.5로 낮추며 KR에 US 업종제외 등가 필터 신설(정유 등 —
#   구 2.5에선 gap이 우연히 걸렀지만 1.5에선 S-Oil이 상위 진입, US 규칙이면 원자재/정유 제외 대상).
N_TOP, REBAL = 5, 5
PE_MAX, GAP_MIN, DV_MIN_MUSD = 30.0, 1.5, 1000.0
KR_HOLDCO = {'402340.KS'}  # SK스퀘어(지주) — KR production 지주제외 준용
KR_IND_BLOCK = {'010950.KS', '096770.KS'}  # S-Oil·SK이노베이션(정유) — US COMMODITY(석유정제) 등가
# 병기 변형: 메모리 테마 캡2 (6월 그리드서 유일 유효 손잡이 — 급락창 1회라 채택 아닌 병기 관찰)
MEMORY_THEME = {'SNDK', 'MU', 'WDC', 'STX', '005930.KS', '000660.KS'}
THEME_CAP = 2


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

    # 2026-07-09: 전수검사 전환 — full 캐시(1,445종목) 우선, 없으면 구 sparse 폴백 (프로덕션 _vm_trailing_eps 패리티)
    _te_full = os.path.join(HERE, 'data_cache', 'trailing_eps_ttm_full.json')
    _te_path = _te_full if os.path.exists(_te_full) else os.path.join(HERE, 'data_cache', 'trailing_eps_ttm.json')
    TE = json.load(open(_te_path, encoding='utf-8'))
    conn = sqlite3.connect(os.path.join(HERE, 'eps_momentum_data.db'))
    c = conn.cursor()
    last = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    # 안전필터 패리티 (2026-07-09 production A군): OM/FCF/ROE는 회전수집이라 60일 carry-forward
    fund = {}
    for tk, om, fcf, roe in c.execute(
            "SELECT ticker, operating_margin, free_cashflow, roe FROM ntm_screening "
            "WHERE date<=? AND date>=date(?, '-60 day') ORDER BY date", (last, last)):
        e = fund.setdefault(tk, [None, None, None])
        if om is not None: e[0] = om
        if fcf is not None: e[1] = fcf
        if roe is not None: e[2] = roe
    out = []
    for tk, p, nc, n7, n30, n60, n90, dv, na in c.execute(
            'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,'
            'num_analysts FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)):
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
        # A군 안전필터 (production _vm_pick 패리티): 동전주·저커버·마진<5%·FCF/ROE 동시음수·rev90>0
        if p < 10 or (na or 0) < 3 or _seg(nc, n90) <= 0:
            continue
        om, fcf, roe = fund.get(tk, (None, None, None))
        if om is not None and om < 0.05:
            continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0:
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
    # 안전필터 패리티 (KR도 동일): OM/FCF/ROE carry-forward
    kconn = sqlite3.connect(KR_DB)
    kfund = {}
    for tk_, om_, fcf_, roe_ in kconn.execute(
            'SELECT ticker, operating_margin, free_cashflow, roe FROM ntm_screening ORDER BY date'):
        e = kfund.setdefault(tk_, [None, None, None])
        if om_ is not None: e[0] = om_
        if fcf_ is not None: e[1] = fcf_
        if roe_ is not None: e[2] = roe_
    kconn.close()
    pre = []
    for tk, p, nc, n7, n30, n60, n90, mc, na in rows:
        if tk in KR_HOLDCO or tk in KR_IND_BLOCK:
            continue
        if (na or 0) < 5:
            continue
        if min(_seg(nc, n7), _seg(n7, n30), _seg(n30, n60), _seg(n60, n90)) < 0:
            continue
        if nc <= 0 or (n90 or 0) <= 0.1:
            continue
        if p / nc > PE_MAX:
            continue
        if _seg(nc, n90) <= 0:
            continue
        om, fcf, roe = kfund.get(tk, (None, None, None))
        if om is not None and om < 0.05:
            continue
        if fcf is not None and roe is not None and fcf < 0 and roe < 0:
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


def _capped_top(merged):
    """테마캡2 변형 top4 (메모리 테마 최대 2종목)."""
    hold = []; mem = 0
    for d in merged:
        if d['ticker'] in MEMORY_THEME:
            if mem >= THEME_CAP:
                continue
            mem += 1
        hold.append(d['ticker'])
        if len(hold) >= N_TOP:
            break
    return hold


def cmd_run():
    us_date, kr_date, fx, merged = compute()
    run_date = datetime.now().strftime('%Y-%m-%d')
    capped = _capped_top(merged)
    print(f'=== 통합 VM top4 (US {us_date} / KR {kr_date}, USDKRW {fx:.0f}) ===')
    for i, d in enumerate(merged[:10], 1):
        mark = ' ★top4' if i <= N_TOP else ''
        if d['ticker'] in capped and i > N_TOP:
            mark += ' (캡2픽)'
        gap_s = f"{d['gap']:.1f}" if d['gap'] else 'pass'
        print(f"{i:2}. [{d['market']}] {d['ticker']:10} rev90 {d['rev90']:+7.1f}%  "
              f"fwdPER {d['fwd_per']:5.1f}  gap {gap_s:>5}  dv ${(d['dv_musd'] or 0):,.0f}M{mark}")
    print('테마캡2 변형 top4:', capped)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    new = not os.path.exists(LOG)
    with open(LOG, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if new:
            w.writerow(['run_date', 'us_date', 'kr_date', 'rank', 'market', 'ticker',
                        'rev90', 'fwd_per', 'gap', 'dv_musd', 'price', 'in_top4', 'in_top4_cap2'])
        for i, d in enumerate(merged[:10], 1):
            w.writerow([run_date, us_date, kr_date, i, d['market'], d['ticker'],
                        round(d['rev90'], 2), round(d['fwd_per'], 2),
                        round(d['gap'], 3) if d['gap'] else '',
                        round(d['dv_musd'], 1) if d['dv_musd'] else '', d['price'],
                        int(i <= N_TOP), int(d['ticker'] in capped)])
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
        # 통합(US+KR) TOP5 + 메모리 경보 — 개인봇 일일 발송 (2026-07-09 사용자 "미리 합쳐놓자")
        # US 채널 메시지는 불변. 실매매 기준을 통합으로 쓸지는 사용자 선택(신호는 하나만 따를 것).
        try:
            import csv as _csv
            import requests as _rq
            rows = list(_csv.DictReader(open(LOG, encoding='utf-8')))
            today = rows[-1]['run_date'] if rows else None
            top = [r for r in rows if r['run_date'] == today and r.get('in_top4') == '1']
            top = sorted(top, key=lambda r: int(r['rank']))[:N_TOP]
            KRN = {'000660.KS': 'SK하이닉스', '005930.KS': '삼성전자', '011070.KS': 'LG이노텍'}
            lines = ['🌏 <b>US+KR 통합 TOP5</b> (모의 병기)', '']
            for i, r in enumerate(top, 1):
                nm = KRN.get(r['ticker'], r['ticker'])
                g = r['gap']
                try:
                    gtxt = f"{float(g):.1f}배"
                except (TypeError, ValueError):
                    gtxt = '-'
                lines.append(f"{i}. [{r['market']}] {nm}")
                lines.append(f"   전망 +{float(r['rev90']):.0f}% · PER {float(r['fwd_per']):.0f} · 이익 {gtxt}")
            lines += ['', '⚠️ 실매매 기준은 신호 하나만 따를 것',
                      '(US 채널 신호와 혼용 금지)']
            from memory_cycle_alert import build_message
            amsg, fired = build_message()
            msg = '\n'.join(lines) + '\n\n' + amsg
            print('\n' + msg.replace('<b>', '').replace('</b>', ''))
            sys.path.insert(0, r'C:\dev')
            from config import TELEGRAM_BOT_TOKEN as _tk, TELEGRAM_PRIVATE_ID as _pid
            _rq.post(f'https://api.telegram.org/bot{_tk}/sendMessage',
                     data={'chat_id': _pid, 'text': msg, 'parse_mode': 'HTML'}, timeout=20)
        except Exception as _e:
            print(f'[통합신호/경보 발송 실패(무해): {_e}]')
