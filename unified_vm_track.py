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
KR_DV_MIN_MUSD = DV_MIN_MUSD  # ★$1B 단일 잣대 (2026-07-09 사용자 결정: KR 특례 폐지 — LG이노텍 사례에서 $100M 검토했으나 동률비교 전항목 HPE 우위 + 단일 기준 선호)
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
        # ★KR 유동성 하한 = $100M (2026-07-09): US $1B은 US 유니버스 검증치(v117) — KR에 들이대면
        #   사실상 삼성·하이닉스 외 전원 금지(LG이노텍 $444M/일도 컷). KR 자체 증거는 반대(7.4년
        #   유동성 필터 전부 기각·실전 하한 20~50억원) → 시장별 하한. $100M(~1,350억원/일)은
        #   개인 체결에 충분히 보수적. 통합 60일 재보정 때 재확인.
        if d['dv_musd'] is not None:
            if d['dv_musd'] < KR_DV_MIN_MUSD:
                continue
        elif (d.get('mc') or 0) < 13e12:  # dv 조회 실패 시 시총 13조 프록시($1B/일 체급)
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


def _wrap(text, width=32):
    """텔레그램 표시폭(한글 2칸) 기준 단어 줄바꿈."""
    def w(t):
        return sum(2 if ord(c) > 0x2E7F else 1 for c in t)
    out, cur = [], ''
    for word in str(text).split():
        cand = (cur + ' ' + word).strip()
        if cur and w(cand) > width:
            out.append(cur)
            cur = word
        else:
            cur = cand
    if cur:
        out.append(cur)
    return out


def _ai_market_brief():
    """AI 시황 3~4문장 (2026-07-09 사용자 요청). 키/패키지 없으면 None(섹션 생략)."""
    key = os.environ.get('GEMINI_API_KEY', '')
    if not key:
        try:
            import json as _j
            key = _j.load(open(os.path.join(HERE, 'config.json'), encoding='utf-8')).get('gemini_api_key', '')
        except Exception:
            pass
    if not key:
        return None
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=key, http_options={'timeout': 90_000})
        tool = types.Tool(google_search=types.GoogleSearch())
        prompt = ('지금 한국시간 저녁이다. 오늘 마감한 한국 증시(코스피·반도체 중심)와 '
                  '지난밤 미국 증시(나스닥·반도체 중심), 그리고 오늘 밤 미국장 주목 포인트를 '
                  '검색해 한국어로 정확히 4문장 요약. 각 문장 25자 이내로 짧게, 과장 없이 사실만.')
        resp = client.models.generate_content(
            model='gemini-2.5-flash', contents=prompt,
            config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
        return (resp.text or '').strip() or None
    except Exception as e:
        print(f'[AI 시황 스킵: {e}]')
        return None


def _industry_tag(d):
    """'(미 · 반도체)' 형식 업종 태그 — US=ticker_info_cache, KR=고정 맵."""
    KR_IND = {'000660.KS': '메모리 반도체', '005930.KS': '전자', '011070.KS': '전자부품'}
    cc = '한' if d['market'] == 'KR' else '미'
    ind = ''
    if d['market'] == 'KR':
        ind = KR_IND.get(d['ticker'], '')
    else:
        try:
            global _TC_CACHE
            if '_TC_CACHE' not in globals():
                import json as _j
                _TC_CACHE = _j.load(open(os.path.join(HERE, 'ticker_info_cache.json'), encoding='utf-8'))
            v = _TC_CACHE.get(d['ticker'])
            ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v) or ''
        except Exception:
            ind = ''
    return '(%s · %s)' % (cc, ind) if ind else '(%s)' % cc


def _us_cards(tickers):
    """US 종목 건강성 카드 (US DB carry-forward 최신값). {tk: [줄,...]}"""
    out = {}
    try:
        conn = sqlite3.connect(os.path.join(HERE, 'eps_momentum_data.db'))
        for tk in tickers:
            r = conn.execute(
                "SELECT num_analysts, rev_up30, rev_down30, rev_growth, market_cap, "
                "dollar_volume_30d, roe, free_cashflow, operating_margin FROM ntm_screening "
                "WHERE ticker=? AND date>=date((SELECT MAX(date) FROM ntm_screening), '-60 day') "
                "ORDER BY date", (tk,)).fetchall()
            f = [None] * 9
            for row in r:
                for k, v in enumerate(row):
                    if v is not None:
                        f[k] = v
            na, up, dn, rg, mc, dv, roe, fcf, om = f
            l1, l2 = [], []
            if na:
                l1.append('분석가 %d명(↑%d/↓%d)' % (na, up or 0, dn or 0))
            if rg is not None:
                l1.append('매출 %+.0f%%' % (rg * 100))
            if mc:
                l2.append('시총 $%.0fB' % (mc / 1e9))
            if dv:
                l2.append('거래 $%.1fB/일' % (dv / 1e3))
            if om is not None:
                l2.append('마진 %.0f%%' % (om * 100))
            out[tk] = [' · '.join(x) for x in (l1, l2) if x]
        conn.close()
    except Exception as e:
        print('[US 카드 스킵: %s]' % e)
    return out


def _ai_stock_briefs(entries):
    """종목 브리핑 1콜 — 1~5위 3문장, 6~20위 1문장. {ticker: text} (실패시 빈 dict)."""
    key = os.environ.get('GEMINI_API_KEY', '')
    if not key:
        try:
            import json as _j
            key = _j.load(open(os.path.join(HERE, 'config.json'), encoding='utf-8')).get('gemini_api_key', '')
        except Exception:
            pass
    if not key or not entries:
        return {}
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=key, http_options={'timeout': 150_000})
        tool = types.Tool(google_search=types.GoogleSearch())
        dl = []
        for d in entries[:5]:
            dl.append('[상세] %s: 90일 이익전망 %+.0f%%, 선행PER %.0f' % (d['ticker'], d['rev90'], d['fwd_per']))
        for d in entries[5:20]:
            dl.append('[한줄] %s: 90일 이익전망 %+.0f%%' % (d['ticker'], d['rev90']))
        prompt = ('한국+미국 주식 퀀트 시스템의 오늘 순위다. [상세] 종목은 정확히 3문장'
                  '(1)뭐하는 회사 (2)최근 이익전망 급상향의 구체적 이유-검색확인 (3)리스크 하나, '
                  '[한줄] 종목은 정확히 1문장(뭐하는 회사+왜 전망이 오르는지 압축). '
                  '한국어, 문장당 25자 이내로 짧게, 과장 없이 사실만. '
                  '.KS로 끝나는 티커는 한국 종목이다. '
                  '형식: "TICKER: 문장들" 한 줄씩.\n' + '\n'.join(dl))
        resp = client.models.generate_content(
            model='gemini-2.5-flash', contents=prompt,
            config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
        text = resp.text or ''
        out = {}
        for d in entries[:20]:
            tk = d['ticker']
            base = tk.split('.')[0]
            for ln in text.splitlines():
                t = ln.strip().lstrip('-*• ')
                if t.upper().startswith(tk.upper() + ':') or t.upper().startswith(base.upper() + ':'):
                    out[tk] = t.split(':', 1)[1].strip()
                    break
        return out
    except Exception as e:
        print('[종목 브리핑 스킵: %s]' % e)
        return {}


def _market_page():
    """두 번째 메시지: 시장 지수(KR 마감+US 전일/선물) + AI 시황."""
    lines = ['━━━━━━━━━━━━━━━', '  🤖 <b>AI 시장 분석</b>', '━━━━━━━━━━━━━━━']
    try:
        import yfinance as yf
        idx = []
        for sym, nm in [('^KS11', '코스피'), ('^KQ11', '코스닥'), ('^GSPC', 'S&P(전일)'), ('NQ=F', '나스닥선물')]:
            try:
                fi = yf.Ticker(sym).fast_info
                px, pv = fi.last_price, fi.previous_close
                if px and pv:
                    idx.append('%s %s(%+.1f%%)' % (nm, format(px, ',.0f'), (px / pv - 1) * 100))
            except Exception:
                pass
        if idx:
            lines += ['', '📊 <b>시장 지수</b>']
            for k in range(0, len(idx), 2):
                lines.append(' · '.join(idx[k:k + 2]))
    except Exception as e:
        print('[지수 스킵: %s]' % e)
    brief = _ai_market_brief()
    if brief:
        lines += ['', '📰 <b>시장 동향</b>']
        import re as _re
        for sent in _re.split(r'(?<=[.다])\s+', brief):
            for wl in _wrap(sent.strip(), 32):
                if wl:
                    lines.append(wl)
    return '\n'.join(lines) if len(lines) > 3 else None


def _send_long(token, chat_id, msg):
    """4096자 제한 분할 발송 (줄 경계)."""
    import requests as _rq
    chunks, cur = [], ''
    for ln in msg.split('\n'):
        if len(cur) + len(ln) + 1 > 3500:
            chunks.append(cur)
            cur = ln
        else:
            cur = (cur + '\n' + ln) if cur else ln
    if cur:
        chunks.append(cur)
    for ch in chunks:
        _rq.post('https://api.telegram.org/bot%s/sendMessage' % token,
                 data={'chat_id': chat_id, 'text': ch, 'parse_mode': 'HTML'}, timeout=20)


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
        for i, d in enumerate(merged[:20], 1):
            w.writerow([run_date, us_date, kr_date, i, d['market'], d['ticker'],
                        round(d['rev90'], 2), round(d['fwd_per'], 2),
                        round(d['gap'], 3) if d['gap'] else '',
                        round(d['dv_musd'], 1) if d['dv_musd'] else '', d['price'],
                        int(i <= N_TOP), int(d['ticker'] in capped)])
    print(f'로그 append: {LOG}')
    return merged


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
        _merged_for_msg = cmd_run()
        # 통합(US+KR) TOP5 + 메모리 경보 — 개인봇 일일 발송 (2026-07-09 사용자 "미리 합쳐놓자")
        # US 채널 메시지는 불변. 실매매 기준을 통합으로 쓸지는 사용자 선택(신호는 하나만 따를 것).
        try:
            import csv as _csv
            import requests as _rq
            rows = list(_csv.DictReader(open(LOG, encoding='utf-8')))
            today = rows[-1]['run_date'] if rows else None
            trows = [r for r in rows if r['run_date'] == today]
            # 같은 날 중복 실행 방어: 마지막 블록(마지막 rank=1 이후)만 사용
            starts = [i for i, r in enumerate(trows) if r['rank'] == '1']
            if starts:
                trows = trows[starts[-1]:]
            top = [r for r in trows if r.get('in_top4') == '1']
            top = sorted(top, key=lambda r: int(r['rank']))[:N_TOP]
            KRN = {'000660.KS': 'SK하이닉스', '005930.KS': '삼성전자', '011070.KS': 'LG이노텍'}
            IND = {'SNDK': '미국 · 낸드 반도체', 'MU': '미국 · 메모리 반도체',
                   'HPE': '미국 · AI서버', 'DELL': '미국 · 서버·PC', 'FLEX': '미국 · 전자 제조',
                   'WDC': '미국 · 데이터 저장장치', 'STX': '미국 · 데이터 저장장치',
                   'MCHP': '미국 · 반도체', 'NVDA': '미국 · AI 반도체', 'AVGO': '미국 · 반도체',
                   'TSM': '미국 · 반도체 위탁생산', 'AAL': '미국 · 항공',
                   '000660.KS': '한국 · 메모리 반도체', '005930.KS': '한국 · 전자',
                   '011070.KS': '한국 · 전자부품'}
            all_days = sorted({r['run_date'] for r in rows})
            idx = all_days.index(today) if today in all_days else len(all_days) - 1
            is_rebal = (idx % REBAL == 0)
            next_in = REBAL - (idx % REBAL)
            rebal_line = ('🔄 오늘은 교체일 — 아래 구성으로 조정'
                          if is_rebal else f'다음 교체까지 {next_in}거래일 (그때까지 유지)')
            # 교체일 매수/매도 diff (2026-07-09 본선 승격 — 지시가 명확해야 함)
            diff_lines = []
            if is_rebal and idx >= REBAL:
                prev_day = all_days[idx - REBAL]
                prows = [r for r in rows if r['run_date'] == prev_day]
                pstarts = [i for i, r in enumerate(prows) if r['rank'] == '1']
                if pstarts:
                    prows = prows[pstarts[-1]:]
                prev_set = {r['ticker'] for r in prows if r.get('in_top4') == '1'}
                cur_set = {r['ticker'] for r in top}
                _knm = {'000660.KS': 'SK하이닉스', '005930.KS': '삼성전자', '011070.KS': 'LG이노텍'}
                buys = [_knm.get(t, t) for t in sorted(cur_set - prev_set)]
                sells = [_knm.get(t, t) for t in sorted(prev_set - cur_set)]
                if buys:
                    diff_lines.append('🟢 매수: ' + '·'.join(buys))
                if sells:
                    diff_lines.append('🔴 매도: ' + '·'.join(sells))
                if not (buys or sells):
                    diff_lines.append('변경 없음 — 그대로 유지')
            _m20 = (_merged_for_msg or [])[:20]
            briefs = _ai_stock_briefs(_m20)
            cards = _us_cards([d['ticker'] for d in _m20 if d['market'] == 'US'])
            import re as _re3
            def _brief_lines(tk, indent='   '):
                b = briefs.get(tk)
                if not b:
                    return []
                out = []
                for sent in _re3.split(r'(?<=[.다])\s+', b):
                    for wl in _wrap(sent.strip(), 32):
                        if wl:
                            out.append(indent + wl)
                return out
            lines = ['🌏 <b>미국+한국 이익전망 TOP5</b>',
                     '애널리스트 이익전망이 가장 빠르게',
                     '좋아지는 5종목을 각 20%씩 담습니다.',
                     rebal_line, '']
            for i, r in enumerate(top, 1):
                nm = KRN.get(r['ticker'], r['ticker'])
                sect = IND.get(r['ticker'], '')
                if not sect:
                    _dd = next((x for x in (_merged_for_msg or []) if x['ticker'] == r['ticker']), None)
                    sect = _industry_tag(_dd).strip('()') if _dd else ''
                lines.append(f"{i}. <b>{nm}</b>" + (f' ({sect})' if sect else ''))
                lines.append(f"   90일간 이익전망 +{float(r['rev90']):.0f}% 상향")
                sub = f"   예상이익 대비 주가 {float(r['fwd_per']):.0f}배"
                try:
                    sub += f" · 이익 {float(r['gap']):.1f}배 성장 예상"
                except (TypeError, ValueError):
                    pass
                lines.append(sub)
                for cl in cards.get(r['ticker'], []):
                    lines.append('   ' + cl)
                lines += _brief_lines(r['ticker'])
                lines.append('')
            lines2 = None
            # 게이트 통과자 6~20위 (2026-07-09 사용자 요청: 1차 통과 rev90 순위 보기)
            try:
                _m = _merged_for_msg
                if _m and len(_m) > N_TOP:
                    lines2 = ['📊 <b>다음 후보 6~20위</b> (참고용 · 매수 아님)',
                              'TOP5와 같은 검사를 통과한',
                              '다음 순위 종목들이에요.', '']
                    for j, d in enumerate(_m[N_TOP:20], N_TOP + 1):
                        nm2 = KRN.get(d['ticker'], d['ticker'])
                        gtxt = f" · 이익 {d['gap']:.1f}배 예상" if d.get('gap') else ''
                        lines2.append(f"<b>{j}. {nm2}</b> {_industry_tag(d)}")
                        lines2.append(f"   전망 +{d['rev90']:.0f}% · 선행PER {d['fwd_per']:.0f}{gtxt}")
                        lines2 += _brief_lines(d['ticker'])
                        lines2.append('')
            except Exception as _e2:
                print(f'[6~20위 섹션 스킵: {_e2}]')
            if diff_lines:
                lines += [''] + diff_lines
            # 전략 누적 (로그 리플레이, 본선 트랙레코드)
            try:
                _nav = 1.0
                _hold = []
                _ppx = {}
                for _i, _d in enumerate(all_days):
                    _day = [r for r in rows if r['run_date'] == _d]
                    _st2 = [k for k, r in enumerate(_day) if r['rank'] == '1']
                    if _st2:
                        _day = _day[_st2[-1]:]
                    _px = {r['ticker']: float(r['price']) for r in _day if r.get('price')}
                    if _hold:
                        _r = [_px[t] / _ppx[t] - 1 for t in _hold if t in _px and t in _ppx and _ppx[t] > 0]
                        if _r:
                            _nav *= 1 + sum(_r) / len(_r)
                    if _i % REBAL == 0:
                        _hold = [r['ticker'] for r in _day if r.get('in_top4') == '1']
                    _ppx.update(_px)
                lines += ['', f'전략 누적 {(_nav - 1) * 100:+.1f}% ({all_days[0][5:].replace("-", "/")}~)']
            except Exception as _e3:
                print(f'[누적 계산 스킵: {_e3}]')
            lines += ['', '📋 매매는 교체일에만 합니다.',
                      '미국 종목 = 당일 밤 개장,',
                      '한국 종목 = 다음날 아침 개장에.']
            from memory_cycle_alert import build_message
            amsg, fired = build_message()
            msg = '\n'.join(lines) + '\n\n' + amsg
            print('\n' + msg.replace('<b>', '').replace('</b>', ''))
            sys.path.insert(0, r'C:\dev')
            from config import TELEGRAM_BOT_TOKEN as _tk, TELEGRAM_PRIVATE_ID as _pid
            _send_long(_tk, _pid, msg)
            if lines2:
                _send_long(_tk, _pid, '\n'.join(lines2))
            _mp = _market_page()
            if _mp:
                _send_long(_tk, _pid, _mp)
        except Exception as _e:
            print(f'[통합신호/경보 발송 실패(무해): {_e}]')
