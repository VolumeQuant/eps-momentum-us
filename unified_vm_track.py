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
# 2026-07-10: KR 경로 머신별 자동탐색 — 회사PC=KR 프로덕션 직접(C:/dev/kr_eps_momentum, 18:10
#   schtask가 여기서 돎), 집PC=quant_py-main 클론(회사PC가 매일 ~16:40 커밋 → git pull 수신).
#   집PC의 C:/dev/kr_eps_momentum·C:/dev/data_cache는 07-09 정리 때 삭제됨(스크래치 복사본).
def _first_existing(env_key, cands):
    v = os.environ.get(env_key)
    if v:
        return v
    for p in cands:
        if os.path.exists(p):
            return p
    return cands[0]

KR_DB = _first_existing('KR_DB_PATH', [
    'C:/dev/kr_eps_momentum/eps_momentum_data_kr.db',                          # 회사PC 프로덕션
    'C:/dev/claude-code/quant_py-main/kr_eps_momentum/eps_momentum_data_kr.db',  # 집PC 클론
    'C:/dev/claude code/quant_py-main/kr_eps_momentum/eps_momentum_data_kr.db',
])
KR_FS_DIR = _first_existing('KR_FS_DIR', [
    'C:/dev/data_cache',                                # 회사PC
    'C:/dev/claude-code/quant_py-main/data_cache',      # 집PC 클론
    'C:/dev/claude code/quant_py-main/data_cache',
])
LOG = os.path.join(HERE, 'data_cache', 'unified_vm_log.csv')
# ★2026-07-09 프로덕션 재캘리브레이션 동기화: gap 2.5→1.5(전수검사 기준), top4→top5.
#   US 프로덕션과 패리티 유지가 이 트랙의 존재 이유(같은 게이트를 양국에). in_top4 컬럼명은 로그 연속성
#   위해 유지(의미 = topN 멤버십). ⚠️gap 1.5로 낮추며 KR에 US 업종제외 등가 필터 신설(정유 등 —
#   구 2.5에선 gap이 우연히 걸렀지만 1.5에선 S-Oil이 상위 진입, US 규칙이면 원자재/정유 제외 대상).
N_TOP, REBAL = 5, 5
PE_MAX, GAP_MIN, DV_MIN_MUSD = 30.0, 1.5, 1000.0
# ★KR 하한 = 백분위 등가 $0.3B (2026-07-09 밤 사용자 승인 — "거래대금 상위 10%로 반영").
#   근거: US $1B = 유니버스 상위 9.9%(124/1,250) vs KR $1B = 상위 1.8%(4/225) = 5.6배 가혹(실측).
#   잣대는 하나("각 시장 상위 ~10%"), 환율만 시장별 — 과거 $100M 특례 폐지 사유(자의적 숫자)를 해소.
#   KR 통과 4→~22종목(현대차·LG전자·LG이노텍 등 재편입). 저커버리지 가드=기존 애널≥5 유지.
#   상세: research/KR_DV_PARITY_2026_07_09.md
KR_DV_MIN_MUSD = 300.0
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
    """반환: (last_date, 후보리스트, health). health = 수집/게이트 건강성 (2026-07-10 감사수리:
    ①7/9 GH Actions 샘플 실행에서 fs_dart parquet 부재 → gap 전원 None → missing=pass로
    KR 가치게이트가 조용히 전멸했던 사고 감지 ②KR yf 수집 붕괴(210→73) 감시)."""
    conn = sqlite3.connect(KR_DB)
    c = conn.cursor()
    last = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    rows = c.execute(
        'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,market_cap,num_analysts '
        'FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)).fetchall()
    conn.close()
    health = {'today_n': sum(1 for r in rows if r[2] and r[2] > 0 and (r[6] or 0) > 100),
              'gap_reach': 0, 'gap_computed': 0, 'warnings': []}
    if not os.path.isdir(KR_FS_DIR):
        health['warnings'].append(f'KR 재무 폴더 없음({KR_FS_DIR}) — 가치게이트(gap) 전면 미작동')
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
        # 저분모 가드 100원 (2026-07-10 감사수리: 구 0.1은 USD용 임계를 원화에 그대로 써 무가드)
        if nc <= 0 or (n90 or 0) <= 100:
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
        health['gap_reach'] += 1
        if g is not None:
            health['gap_computed'] += 1
        if g is not None and g < GAP_MIN:
            continue
        pre.append(dict(ticker=tk, market='KR', rev90=_seg(nc, n90), fwd_per=p / nc,
                        gap=g, dv_musd=None, price=p, mc=mc))
    if health['gap_reach'] >= 3 and health['gap_computed'] == 0:
        health['warnings'].append(
            f"KR 가치게이트(gap) 계산 0/{health['gap_reach']}건 — 재무 데이터 접근 실패 의심, "
            'missing=pass 규칙으로 KR 전원이 무검사 통과 중')
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
        if all(d['dv_musd'] is None for d in pre):
            health['warnings'].append('KR 거래대금(yf) 전건 조회 실패 — 시총 13조 프록시로만 필터 중')
    out = []
    for d in pre:
        # ★KR 유동성 하한 = KR_DV_MIN_MUSD($0.3B, 2026-07-09 사용자 승인 '각 시장 상위 ~10%'
        #   백분위 등가 — KR_DV_PARITY_2026_07_09.md). (구 주석의 $100M은 폐기된 특례 —
        #   2026-07-10 감사에서 주석 부패 정정.)
        if d['dv_musd'] is not None:
            if d['dv_musd'] < KR_DV_MIN_MUSD:
                continue
        elif (d.get('mc') or 0) < 13e12:  # dv 조회 실패 시 시총 13조 프록시($1B/일 체급)
            continue
        d.pop('mc', None)
        out.append(d)
    return last, out, health


def _universe_rev90(db, dv_min=None, n90_floor=0.1, window_days=30):
    """해당 시장 투자가능 유니버스의 rev90 분포 (백분위·z 환산용).

    ★2026-07-10 감사수리 — 분모 = '당일 스냅샷'이 아니라 **최근 window_days 내 관측된
    종목별 최신 유효 행(트레일링 유니온)**. 당일 분모는 KR yf 수집 붕괴(유효 6/1 184 →
    7/9 69, 탈락종목이 체계적으로 차가움: 탈락 중앙값 rev90 +11 vs 생존 +17)로 뜨거운
    생존자만 남아 KR 백분위를 ~5%p 하향 왜곡했음(삼성 84.1%ile vs 온전분모 89.1%ile —
    top5 경계 여유와 같은 스케일). 트레일링 유니온은 attrition에 강건.
    dv_min(백만$) 지정 시 종목별 최신 행의 dollar_volume_30d로 필터(US $1B 유동주 패리티).
    KR은 dv 컬럼 부재라 전체(수집 자체가 애널 커버 엘리트, CROSS_MARKET_NORM_2026_07_09.md).
    n90_floor: 저분모 rev90 폭발 가드 — 구현이 0.1을 양국 동일 적용해 원화(KR)엔 사실상
    무가드였음 → KR 호출부는 100(원)을 넘길 것.
    """
    c = sqlite3.connect(db)
    dt = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
    has_dv = any(r[1] == 'dollar_volume_30d' for r in c.execute("PRAGMA table_info(ntm_screening)"))
    dv_col = ', dollar_volume_30d' if has_dv else ', NULL'
    rows = c.execute(
        f'SELECT ticker, ntm_current, ntm_90d{dv_col} FROM ntm_screening '
        'WHERE date>=date(?, ?) AND ntm_current>0 AND ntm_90d>? ORDER BY date',
        (dt, f'-{int(window_days)} day', n90_floor)).fetchall()
    c.close()
    latest = {}
    for tk, nc, n90, dv in rows:  # ORDER BY date라 뒤 행이 최신 — 종목별 최신 행만 남김
        latest[tk] = (nc, n90, dv)
    vals = []
    for nc, n90, dv in latest.values():
        if dv_min is not None and has_dv and (dv is None or dv < dv_min):
            continue
        vals.append((nc - n90) / abs(n90) * 100)
    return vals


def _dist_med_mad(vals):
    import statistics as _s
    med = _s.median(vals)
    mad = _s.median([abs(v - med) for v in vals]) or 1e-9
    return med, mad


def compute():
    fx = _fx_usdkrw()
    us_date, us = us_candidates()
    kr_date, kr, kr_health = kr_candidates(fx)
    merged = sorted(us + kr, key=lambda d: -d['rev90'])
    meta = {'norm': 'pct', 'warnings': list(kr_health.get('warnings') or []),
            'kr_today_n': kr_health.get('today_n'), 'base_n': {}}
    # 백분위 결합 = 본선 (2026-07-09 사용자 승인, CROSS_MARKET_NORM 연구): rev90 절대값이
    # 아니라 자기 시장 '유동성 유니버스' 내 백분위로 환산해 결합 — KR 리비전 인플레 보정.
    # (실측: 횡단면 중앙값 US +4.3% vs KR +17.0%, MAD 3.4 vs 13.5. LG이노텍 +50.6%=KR
    #  82%ile vs HPE +49.2%=US 94%ile → 절대값이 아니라 백분위로 재야 HPE 우위.)
    # denominator = 각 시장 애널 커버 전체(무필터), 30일 트레일링 유니온.
    # ★2026-07-10 2차 감사수리(사용자 "둘 다 거래대금 조건을 걸거나 둘 다 안 걸어야"):
    #   구 스펙은 US만 $1B 필터(121) / KR 무필터 = 잣대 둘 — 이 비대칭이 5위(삼성 vs FLEX)를
    #   결정하고 있었음. 대칭 대안 중 '둘 다 필터'는 KR이 20종목(등수 1칸=5%p)이라 통계 불능 →
    #   '둘 다 무필터' 채택. 근거: ①정규화의 근거 측정(US +4.4 vs KR +10.1 중앙값)부터 무필터
    #   전체끼리 잰 것(증거-구현 일관성) ②유동성은 후보 게이트(US $1B/KR $0.3B)가 이미 담당 —
    #   자(분모)에 또 섞으면 개념 이중적용 ③top4(SNDK·MU·HPE·하이닉스)는 잣대 무관 확고,
    #   5위는 razor-thin(판정일 재확인 항목). 상세: research/AUDIT_FIXES_2026_07_10.md 2차.
    try:
        uus = _universe_rev90(os.path.join(HERE, 'eps_momentum_data.db'))
        ukr = _universe_rev90(KR_DB, n90_floor=100.0)  # 원화 저분모 가드
        meta['base_n'] = {'US': len(uus), 'KR': len(ukr)}
        if len(ukr) < 30:
            meta['warnings'].append(f'KR 백분위 분모 {len(ukr)}종목뿐 — 순위 신뢰 낮음')
        kt = kr_health.get('today_n') or 0
        if kt and (kt < 60 or kt < 0.6 * len(ukr)):
            meta['warnings'].append(
                f'KR 수집 부실: 오늘 {kt}종목 (최근 30일 관측 {len(ukr)}종목) — KR 순위 참고만')
        mus, dus = _dist_med_mad(uus)
        mkr, dkr = _dist_med_mad(ukr)
        for d in merged:
            base, med, mad = (uus, mus, dus) if d['market'] == 'US' else (ukr, mkr, dkr)
            d['pct'] = sum(1 for v in base if v < d['rev90']) / len(base) * 100
            d['rz'] = (d['rev90'] - med) / mad * 0.6745  # robust-z 병기 관찰
        merged.sort(key=lambda d: (-d['pct'], -d['rev90']))
    except Exception as e:
        # 2026-07-10 감사수리: 조용한 폴백 금지 — 본선 결합 방식이 바뀌면 메시지에 명시
        meta['norm'] = 'abs_fallback'
        meta['warnings'].append(f'백분위 환산 실패 → 절대 상향폭 순위로 임시 결합됨: {e}')
        print(f'[!!] 백분위 환산 실패 — 절대 rev90 결합으로 폴백: {e}')
    return us_date, kr_date, fx, merged, meta


def _git_sha():
    """실행 코드 버전 — 메시지·로그에 박아 '낡은 코드로 발송' 사고를 사후 식별 가능하게 (감사수리 3)."""
    try:
        import subprocess
        return subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True,
                              text=True, cwd=HERE, timeout=10).stdout.strip()
    except Exception:
        return ''


def _us_grid():
    """리밸 시계의 단일 기준 = US 거래일 그리드(앵커 2026-07-02, R5).
    2026-07-10 감사수리: 표시(is_rebal)는 이 그리드, NAV 리플레이는 '로그 실행일 인덱스 i%5'로
    서로 다른 시계였음(지시한 매매와 표시한 누적 성과가 다른 날 리밸) → 전부 이 그리드로 통일."""
    c = sqlite3.connect(os.path.join(HERE, 'eps_momentum_data.db'))
    usd = [x[0] for x in c.execute(
        "SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL AND date>='2026-07-02' ORDER BY date")]
    c.close()
    return usd


def _ledger_blocks(rows):
    """일자별 마지막 완전 블록(rank==1 시작) — 같은 날 중복 실행(수동+schtask) dedup.
    구 cmd_nav는 dedup 없이 전 행을 써 중복 시 보유 비중이 왜곡됐음(감사수리 4)."""
    days = sorted({r['run_date'] for r in rows})
    blocks = {}
    for d in days:
        day = [r for r in rows if r['run_date'] == d]
        st = [i for i, r in enumerate(day) if r.get('rank') == '1']
        blocks[d] = day[st[-1]:] if st else day
    return days, blocks


def _replay(rows):
    """원장 리플레이 — 단일 리밸 시계(US 그리드)로 보유·NAV 재구성. 로컬통화 수익률(FX 미반영) 근사.
    반환: {'nav', 'days', 'state': {day: {'is_rebal', 'held_before', 'held_after'}}}
    held_before = 그날 리밸 직전 보유(교체 diff의 올바른 기준), held_after = 리밸 반영 후."""
    days, blocks = _ledger_blocks(rows)
    usd = _us_grid()
    nav, hold, ppx = 1.0, [], {}
    state = {}
    for i, d in enumerate(days):
        day = blocks[d]
        px = {}
        for r in day:
            try:
                if r.get('price'):
                    px[r['ticker']] = float(r['price'])
            except (TypeError, ValueError):
                pass
        if hold:
            rr = [px[t] / ppx[t] - 1 for t in hold if t in px and t in ppx and ppx[t] > 0]
            if rr:
                nav *= 1 + sum(rr) / len(rr)
        ud = day[0].get('us_date') if day else None
        gi = usd.index(ud) if ud in usd else None
        is_rb = (i == 0) or (gi is not None and gi % REBAL == 0)  # 첫 로그일 = 페이퍼 개시(초기 편입)
        held_before = list(hold)
        if is_rb:
            hold = [r['ticker'] for r in day if r.get('in_top4') == '1']
        state[d] = {'is_rebal': is_rb, 'held_before': held_before, 'held_after': list(hold)}
        ppx.update(px)
    return {'nav': nav, 'days': days, 'state': state}


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
        prompt = ('지금 한국시간 저녁이다. 검색해서 한국어로 정확히 5문장: '
                  '지난밤 미국 증시 마감과 주도 섹터 2문장, 오늘 밤 미국장 주목 포인트(지표·실적 일정) 1문장, '
                  '반도체·메모리 업황 1문장, 오늘 한국 증시 마감 1문장. '
                  '미확인 루머(상장설·인수설) 금지, 문장당 30자 이내, 과장 없이 사실만.')
        resp = client.models.generate_content(
            model='gemini-2.5-flash', contents=prompt,
            config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
        return (resp.text or '').strip() or None
    except Exception as e:
        print(f'[AI 시황 스킵: {e}]')
        return None


_NAME_SEED = {
    'SNDK': '샌디스크', 'MU': '마이크론', 'HPE': 'HPE', 'DELL': '델', 'FLEX': '플렉스',
    'MCHP': '마이크로칩', 'AVGO': '브로드컴', 'TSM': 'TSMC', 'WDC': '웨스턴디지털',
    'STX': '씨게이트', 'NVDA': '엔비디아', 'AMD': 'AMD', 'SMCI': '슈퍼마이크로',
    'CRDO': '크레도', 'AAL': '아메리칸항공', 'ADI': '아날로그디바이스',
    'AMAT': '어플라이드', 'LNG': '셰니어에너지', 'NOW': '서비스나우', 'MRK': '머크',
    'META': '메타', 'INTU': '인튜이트', 'ORCL': '오라클', 'KLAC': 'KLA',
    'LRCX': '램리서치', 'ON': '온세미', 'NOK': '노키아', 'CLS': '셀레스티카',
    'ANET': '아리스타', 'CIEN': '시에나', 'COHR': '코히런트', 'LITE': '루멘텀',
    'APH': '암페놀', 'CRM': '세일즈포스', 'ADBE': '어도비', 'CSCO': '시스코',
    'SNPS': '시놉시스', 'NXPI': 'NXP반도체', 'APP': '앱러빈', 'GOOG': '알파벳',
    'IBM': 'IBM', 'MSFT': '마이크로소프트', 'JNJ': '존슨앤드존슨', 'MA': '마스터카드',
    '000660.KS': 'SK하이닉스', '005930.KS': '삼성전자', '011070.KS': 'LG이노텍',
    '009150.KS': '삼성전기', '402340.KS': 'SK스퀘어',
}


def _display_name(tk):
    """종목명 표시 — 시드맵 → 캐시(ticker_names.json) → yf shortName(1회 후 캐시)."""
    if tk in _NAME_SEED:
        return _NAME_SEED[tk]
    import json as _j
    cp = os.path.join(HERE, 'data_cache', 'ticker_names.json')
    try:
        cache = _j.load(open(cp, encoding='utf-8'))
    except Exception:
        cache = {}
    if tk in cache:
        return cache[tk]
    try:
        import yfinance as yf
        nm = (yf.Ticker(tk).info or {}).get('shortName') or tk
        nm = nm.replace(', Inc.', '').replace(' Inc.', '').replace(' Corporation', '').replace(' Corp.', '').replace(' Company', '').strip()
        cache[tk] = nm
        _j.dump(cache, open(cp, 'w', encoding='utf-8'), ensure_ascii=False)
        return nm
    except Exception:
        return tk


def _industry_tag(d):
    """'(미 · 반도체)' 형식 업종 태그 — US=ticker_info_cache, KR=고정 맵."""
    KR_IND = {'000660.KS': '메모리 반도체', '005930.KS': '전자', '011070.KS': '전자부품'}
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
    return ind or ''


def _kr_card(ticker, dv_musd=None):
    """KR 종목 카드 — yf(분석가·시총) + 거래대금. 실패 항목은 생략."""
    parts1, parts2 = [], []
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        na = info.get('numberOfAnalystOpinions')
        mc = info.get('marketCap')
        if na:
            parts1.append('분석가 %d명' % na)
        if mc:
            parts2.append('시총 %.0f조원' % (mc / 1e12))
    except Exception:
        pass
    if dv_musd:
        parts2.append('거래 $%.1fB/일' % (dv_musd / 1e3))
    out = []
    if parts1:
        out.append(' · '.join(parts1))
    if parts2:
        out.append(' · '.join(parts2))
    return out


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


def _brief_dict(x):
    """브리핑을 {biz, why, risk} dict로 강제 — 문자열이 와도 카드 렌더가 깨지지 않게."""
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip():
        parts = [p.strip() for p in x.split('||')]
        return {'biz': parts[0], 'why': '', 'risk': parts[1] if len(parts) > 1 else ''}
    return {}


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
            dl.append('[상세] %s(%s, %s): 90일 이익전망 %+.0f%%, 선행PER %.0f'
                      % (d['ticker'], _display_name(d['ticker']), _industry_tag(d) or '업종미상', d['rev90'], d['fwd_per']))
        for d in entries[5:20]:
            dl.append('[한줄] %s(%s, %s): 90일 이익전망 %+.0f%%'
                      % (d['ticker'], _display_name(d['ticker']), _industry_tag(d) or '업종미상', d['rev90']))
        prompt = ('한국+미국 주식 퀀트 시스템의 오늘 순위다. '
                  '★모든 종목은 지금 상장되어 활발히 거래 중이다. 상장폐지·인수 소멸 서술 절대 금지, '
                  '미확인 루머(상장 추진설·인수설 등) 금지, 반드시 2026년 최신 정보를 검색해 확인하라'
                  '(예: SNDK는 2025년 웨스턴디지털에서 분사 재상장한 샌디스크). '
                  '[상세] 종목은 5~6문장의 미니 분석: (1)무슨 사업으로 돈 버는 회사인지 1문장 '
                  '(2)왜 이익전망이 급상향되는지 2문장 — 최근 실적발표·수주·제품가격 등 구체 숫자 포함(검색 확인) '
                  '(3)경쟁지위나 재무 강점 1문장 (4)리스크 1~2문장. '
                  '[한줄] 종목은 정확히 2문장(무슨 회사인지 + 전망 상향 이유). '
                  '한국어, 문장당 30자 이내, 뻔한 일반론 금지, 과장 없이 사실만. '
                  '형식(한 줄씩): [상세]는 "TICKER: 사업·상향이유·강점 문장들 || 리스크 문장들" '
                  '(리스크 앞에 반드시 ||), [한줄]은 "TICKER: 문장들".\n' + '\n'.join(dl))
        text = ''
        for _try in range(3):  # 재시도 (2026-07-09: 단발 실패로 브리핑 0건 발송 사고)
            try:
                resp = client.models.generate_content(
                    model='gemini-2.5-flash', contents=prompt,
                    config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
                text = resp.text or ''
                if text:
                    break
            except Exception as _e:
                print('[브리핑 시도 %d 실패: %s]' % (_try + 1, _e))
                import time as _t
                _t.sleep(15)
        out = {}
        for d in entries[:20]:
            tk = d['ticker']
            base = tk.split('.')[0]
            for ln in text.splitlines():
                t = ln.strip().lstrip('-*• ')
                if t.upper().startswith(tk.upper() + ':') or t.upper().startswith(base.upper() + ':'):
                    out[tk] = _brief_dict(t.split(':', 1)[1].strip())
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
        for sym, nm in [('^KS11', '코스피'), ('^KQ11', '코스닥'), ('^GSPC', 'S&P'), ('^IXIC', '나스닥')]:
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
            for wl in _wrap(sent.strip(), 90):
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
    us_date, kr_date, fx, merged, meta = compute()
    run_date = datetime.now().strftime('%Y-%m-%d')
    capped = _capped_top(merged)
    print(f'=== 통합 VM top{N_TOP} (US {us_date} / KR {kr_date}, USDKRW {fx:.0f}, '
          f'code {_git_sha() or "?"}) ===')
    if meta.get('base_n'):
        print(f"백분위 분모(30일 유니온): US {meta['base_n'].get('US')} / KR {meta['base_n'].get('KR')} "
              f"(KR 당일 수집 {meta.get('kr_today_n')})")
    for wmsg in meta.get('warnings', []):
        print(f'[경고] {wmsg}')
    for i, d in enumerate(merged[:10], 1):
        mark = ' ★top4' if i <= N_TOP else ''
        if d['ticker'] in capped and i > N_TOP:
            mark += ' (캡2픽)'
        gap_s = f"{d['gap']:.1f}" if d['gap'] else 'pass'
        print(f"{i:2}. [{d['market']}] {d['ticker']:10} rev90 {d['rev90']:+7.1f}%  "
              f"fwdPER {d['fwd_per']:5.1f}  gap {gap_s:>5}  dv ${(d['dv_musd'] or 0):,.0f}M{mark}")
    print('테마캡2 변형 top4:', capped)
    # 절대 rev90 결합 = 구 본선, 관찰 컬럼으로 강등 (2026-07-09 백분위 승격의 비교군)
    abs_top = [x['ticker'] for x in sorted(merged, key=lambda z: -z['rev90'])[:N_TOP]]
    print('절대결합 변형 top5(관찰):', abs_top)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    rz_top = [x['ticker'] for x in sorted(merged, key=lambda z: -z.get('rz', 0))[:N_TOP]]
    print('robust-z 변형 top5(관찰):', rz_top)
    if os.environ.get('UNIFIED_NO_LOG') == '1':
        # 2026-07-10 감사수리 2: 샘플/일회성 실행이 append-only 공식 원장을 오염시킨 사고
        # (7/9 블록 = GH Actions 샘플, KR gap 전원 공란) 재발 방지 — 원장 기록은 옵트아웃 가능.
        print('[UNIFIED_NO_LOG=1] 원장 기록 생략')
        return merged, meta
    COLS = ['run_date', 'us_date', 'kr_date', 'rank', 'market', 'ticker',
            'rev90', 'fwd_per', 'gap', 'dv_musd', 'price', 'in_top4', 'in_top4_cap2',
            'pct', 'in_top5_abs', 'rz', 'in_top5_rz', 'pct_base_n']
    if os.path.exists(LOG):  # 구헤더(13/14컬럼) → 신헤더 마이그레이션, 과거 행은 공란 패딩
        lines = open(LOG, encoding='utf-8').read().splitlines()
        hdr = lines[0].split(',')
        if len(hdr) < len(COLS):
            with open(LOG, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow(COLS)
                for ln in lines[1:]:
                    cells = next(csv.reader([ln]))
                    w.writerow((cells + [''] * len(COLS))[:len(COLS)])
    new = not os.path.exists(LOG)
    with open(LOG, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if new:
            w.writerow(COLS)
        for i, d in enumerate(merged[:20], 1):
            w.writerow([run_date, us_date, kr_date, i, d['market'], d['ticker'],
                        round(d['rev90'], 2), round(d['fwd_per'], 2),
                        round(d['gap'], 3) if d['gap'] else '',
                        round(d['dv_musd'], 1) if d['dv_musd'] else '', d['price'],
                        int(i <= N_TOP), int(d['ticker'] in capped),
                        round(d.get('pct', 0), 2), int(d['ticker'] in abs_top),
                        round(d.get('rz', 0), 2), int(d['ticker'] in rz_top),
                        meta.get('base_n', {}).get(d['market'], '')])
    print(f'로그 append: {LOG}')
    return merged, meta


def cmd_nav():
    """로그 리플레이 NAV — 공용 _replay 사용 (2026-07-10 감사수리 4: 구현이 '로그일 i%5'
    자체 시계 + 중복블록 dedup 없음으로 표시(is_rebal)와 어긋났음 → US 그리드 단일화)."""
    if not os.path.exists(LOG):
        print('로그 없음')
        return
    import csv as _csv
    rows = list(_csv.DictReader(open(LOG, encoding='utf-8')))
    rp = _replay(rows)
    days = rp['days']
    for d in days:
        s = rp['state'][d]
        if s['is_rebal']:
            print(f"{d} REBAL → {s['held_after']}")
    print(f"통합 트랙 NAV: {(rp['nav'] - 1) * 100:+.2f}% ({days[0]} ~ {days[-1]}, {len(days)}일)")



# ═══ 메시지 렌더링 (2026-07-09 UX 전문가 스펙: 행동→회사→근거3+앵커→위험→규모 고정 카드) ═══
PER_ANCHOR = {'US': '미국 평균 22배', 'KR': '한국 평균 11배'}
_MEDAL = {1: '🥇', 2: '🥈', 3: '🥉'}


def _fmt_scale(d, cards_map):
    """기업 규모 줄 — US: $B / KR: 조원 근사."""
    out = []
    cl = cards_map.get(d['ticker']) or []
    for c in cl:
        out.append(c)
    return out


def _stock_card(rank, d, brief, cards_map, first=False):
    """종목 카드 — 무슨 회사 → 왜(숫자 3개) → 위험 → 규모."""
    nm = _display_name(d['ticker'])
    tk = d['ticker'].replace('.KS', '').replace('.KQ', '')
    sect = _industry_tag(d)
    nation = '한국' if d['market'] == 'KR' else '미국'
    medal = _MEDAL.get(rank, '')
    L = ['━━━━━━━━━━━━━━',
         f"{medal} <b>{rank}위 {nm}</b> ({tk}) · {nation} {sect}".replace('  ', ' '), '']
    b = _brief_dict(brief)
    if b.get('biz') or b.get('why'):
        L.append('<b>무슨 회사?</b>')
        for key in ('biz', 'why'):
            for sent in _split_sents(b.get(key, '')):
                L.append(sent)
        L.append('')
    L.append('<b>왜 순위권? — 숫자 3개만 보세요</b>')
    L.append('')
    L.append(f"① 전문가 전망 상향 +{d['rev90']:.0f}% (3개월)")
    if d.get('pct') is not None:
        mk = '미국' if d['market'] == 'US' else '한국'
        L.append(f"· {mk} 시장에서 상위 {100 - d['pct']:.0f}% (공정 비교 기준)")
    if first:
        L.append('· EPS(주당순이익) = 1주가 버는 돈.')
        L.append(f"· 이익 전망치를 석 달 만에 {1 + d['rev90'] / 100:.1f}배로")
        L.append('  올렸다는 뜻입니다.')
    an = _analyst_line(d, cards_map)
    if an:
        L.append(an)
    if d.get('gap'):
        L.append('')
        L.append(f"② 이익 성장 {d['gap']:.1f}배")
        L.append('· 올해 예상 이익 = 작년 실적의'
                 f" {d['gap']:.1f}배입니다.")
    L.append('')
    L.append(f"③ 가격은 선행PER {d['fwd_per']:.0f}배")
    if first:
        L.append('· PER = 주가가 이익의 몇 배인가.')
    L.append(f"· 낮을수록 싼 것. {PER_ANCHOR[d['market']]}.")
    L.append('')
    if b.get('risk'):
        L.append('<b>⚠️ 이런 위험이 있어요</b>')
        for sent in _split_sents(b['risk']):
            L.append(sent)
        L.append('그래서 5거래일마다 점검해 교체합니다.')
        L.append('')
    scale = _scale_line(d, cards_map)
    if scale:
        L.append('<b>기업 규모</b>')
        L += scale
        L.append('')
    return L


def _split_sents(text):
    import re
    out = []
    for sent in re.split(r'(?<=[.다])\s+', (text or '').strip()):
        s = sent.strip()
        if s:
            out.append(s)
    return out


def _analyst_line(d, cards_map):
    cl = cards_map.get(d['ticker']) or []
    for c in cl:
        if '분석가' in c:
            m = c.split('·')[0].strip()
            return '· 애널리스트 ' + m.replace('분석가 ', '') \
                .replace('(↑', ' 중 30일간 ').replace('/↓', '명 상향, ') \
                .replace(')', '명 하향.')
    return None


def _scale_line(d, cards_map):
    cl = cards_map.get(d['ticker']) or []
    out = []
    for c in cl:
        parts = [x.strip() for x in c.split('·')]
        keep = [x for x in parts if ('시총' in x or '거래' in x)]
        if keep:
            out.append(' / '.join(keep))
    if out:
        out.append('→ 대형주라 원할 때 사고팔기 쉽습니다.')
    return out


def _compose_and_send(merged, meta=None):
    import csv as _csv
    from datetime import datetime as _dt
    meta = meta or {}
    rows = list(_csv.DictReader(open(LOG, encoding='utf-8'))) if os.path.exists(LOG) else []
    # 2026-07-10 감사수리 4: 시계 단일화 — diff·NAV·is_rebal 전부 _replay(US 그리드) 기준.
    #   (구현은 diff=로그일 idx-5, NAV=로그일 i%5, is_rebal=US그리드로 3개 시계가 달랐음)
    rp = _replay(rows) if rows else {'nav': 1.0, 'days': [], 'state': {}}
    all_days = rp['days']
    today = all_days[-1] if all_days else _dt.now().strftime('%Y-%m-%d')
    _, blocks = _ledger_blocks(rows) if rows else (None, {})
    trows = blocks.get(today, [])
    st_today = rp['state'].get(today, {})
    is_rebal = st_today.get('is_rebal', False)
    usd = _us_grid()
    us_latest = trows[0]['us_date'] if trows else None
    gi = usd.index(us_latest) if us_latest in usd else len(usd) - 1
    next_in = REBAL - (gi % REBAL)
    m20 = merged[:20]
    top5 = m20[:N_TOP]
    briefs = _ai_stock_briefs(m20)
    cards = _us_cards([d['ticker'] for d in m20 if d['market'] == 'US'])
    for d in m20:
        if d['market'] == 'KR' and d['ticker'] not in cards:
            kc = _kr_card(d['ticker'], d.get('dv_musd'))
            if kc:
                cards[d['ticker']] = kc
    # 교체 diff = 오늘 top5 vs 리밸 직전 보유(held_before) — 원장 리플레이와 같은 기준
    diff = None
    if is_rebal:
        prev_set = set(st_today.get('held_before') or [])
        cur_set = {d['ticker'] for d in top5}
        buys = sorted(cur_set - prev_set)
        sells = sorted(prev_set - cur_set)
        diff = (buys, sells)
    nav = rp['nav']
    # 신호등
    try:
        from memory_cycle_alert import build_message
        amsg, fired = build_message()
    except Exception as _ae:
        amsg, fired = f'🚦 신호등 계산 실패: {_ae}', False
    # ── 메시지 1: 상단 공통 + TOP5 카드 ──
    kdt = _dt.now()
    wd = '월화수목금토일'[kdt.weekday()]
    m1 = [f'📬 <b>오늘의 주식 신호</b> | {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━', '']
    if meta.get('warnings'):
        m1.append('⚠️ <b>데이터 품질 주의</b>')
        for wmsg in meta['warnings']:
            m1 += _wrap('· ' + wmsg, 44)
        m1.append('')
    if fired:
        m1 += ['🔴 <b>메모리 위험 경보 발동!</b>',
               '아래 위험 신호등 안내에 따라',
               '메모리 종목을 정리하세요.', '']
    if is_rebal and diff and (diff[0] or diff[1]):
        n_ch = max(len(diff[0]), len(diff[1]))
        m1.append(f'🔁 <b>오늘 할 일: 종목 {n_ch}개 교체</b>')
        for t in diff[1]:
            m1.append(f'🔴 팔기: {_display_name(t)} — 보유분 전량 매도')
        for t in diff[0]:
            m1.append(f'🟢 사기: {_display_name(t)} — 자산의 20% 매수')
        m1.append('나머지 종목은 그대로 유지하세요.')
        m1.append('(이미 처리했거나 갖고 있지 않은')
        m1.append(' 종목은 건너뛰면 됩니다)')
    elif is_rebal:
        m1 += ['✅ <b>오늘 할 일: 없음</b>', '점검 결과 교체 없이 그대로 갑니다.']
    else:
        m1 += ['✅ <b>오늘 할 일: 없음</b>',
               '보유 중인 5종목 그대로 두시면 됩니다.',
               f'다음 교체 점검까지 {next_in}거래일.']
    m1 += ['', '<b>이 신호, 어떻게 고르나요?</b>',
           '증권사 애널리스트(기업 분석 전문가)들이',
           '"이 회사 돈 더 벌겠다"며 이익 전망을',
           '최근 3개월간 가장 크게 올린 종목 중,',
           '아직 싸고 이익이 급성장하는 회사만',
           '5개를 담습니다. 각 20%씩, 주 1회 점검.', '',
           '<b>한국·미국을 공정하게 비교합니다</b>',
           '한국 애널리스트는 미국보다 전망을',
           '훨씬 후하게 올립니다(중앙값 +17% vs +4%).',
           '그래서 상향폭을 그대로 비교하면',
           '한국 종목이 부풀려 보입니다.',
           '이를 막으려고 각 종목을 "자기 시장',
           '안에서 상위 몇 %인지"로 환산해 비교합니다.', '',
           f"📊 전략 누적 성과: {(nav - 1) * 100:+.1f}%",
           f"({all_days[0][5:].replace('-', '/')} 시작)" if all_days else '']
    for i, d in enumerate(top5, 1):
        m1 += _stock_card(i, d, briefs.get(d['ticker']), cards, first=(i == 1))
    m1 += ['━━━━━━━━━━━━━━', '📖 <b>용어 한 줄 정리</b>',
           'EPS: 주식 1주가 벌어들이는 이익',
           'PER: 주가가 이익의 몇 배인지 (낮을수록 저렴)',
           '선행: 과거가 아닌 "올해 예상" 기준']
    # ── 메시지 2: 6~20위 미니카드 ──
    m2 = None
    if len(m20) > N_TOP:
        m2 = [f'📋 <b>다음 후보 6~20위</b> | {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━',
              '<b>지금 사는 종목이 아닙니다.</b>',
              'TOP5에서 빠지는 종목이 생기면',
              '이 명단의 위쪽부터 차례로 들어옵니다.', '']
        for j, d in enumerate(m20[N_TOP:], N_TOP + 1):
            nm = _display_name(d['ticker'])
            tk = d['ticker'].replace('.KS', '').replace('.KQ', '')
            sect = _industry_tag(d)
            nation = '한국' if d['market'] == 'KR' else '미국'
            b = _brief_dict(briefs.get(d['ticker']))
            m2 += ['─────────────',
                   f"<b>{j}위 {nm}</b> ({tk}) · {nation} {sect}".replace('  ', ' '), '']
            if b.get('biz') or b.get('why'):
                m2.append('<b>무슨 회사?</b>')
                for key in ('biz', 'why'):
                    for sent in _split_sents(b.get(key, '')):
                        m2.append(sent)
                m2.append('')
            m2.append('<b>핵심 숫자</b>')
            m2.append(f"전문가 전망 상향 +{d['rev90']:.0f}% (3개월)")
            if d.get('gap'):
                m2.append(f"이익 성장 {d['gap']:.1f}배 (올해 예상÷작년)")
            m2.append(f"가격 선행PER {d['fwd_per']:.0f}배 ({PER_ANCHOR[d['market']]})")
            m2.append('')
    # ── 메시지 3: 시장 브리핑 + 신호등 ──
    m3 = [f'🌐 <b>시장 브리핑</b> | {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━']
    try:
        import yfinance as yf
        idx_lines = []
        for sym, nm in [('^GSPC', 'S&P500'), ('^IXIC', '나스닥'), ('^SOX', '반도체지수'),
                        ('^KS11', '코스피'), ('^KQ11', '코스닥')]:
            try:
                fi = yf.Ticker(sym).fast_info
                px, pv = fi.last_price, fi.previous_close
                if px and pv:
                    idx_lines.append(f"{nm} {px:,.0f} ({(px / pv - 1) * 100:+.1f}%)")
            except Exception:
                pass
        if idx_lines:
            m3 += ['', '<b>주요 지수</b>'] + idx_lines
    except Exception:
        pass
    brief_mkt = _ai_market_brief()
    if brief_mkt:
        m3 += ['', '<b>무슨 일이 있었나</b>'] + _split_sents(brief_mkt)
    m3 += ['', amsg]
    _sha = _git_sha()
    if _sha:
        m3 += ['', f'<i>sys {_sha} · {today}</i>']  # 코드버전 — 낡은 코드 발송 식별용 (감사수리 3)
    # ── 발송 ──
    print('\n' + '\n'.join(m1).replace('<b>', '').replace('</b>', ''))
    if os.environ.get('UNIFIED_DRY_RUN') == '1':
        print('\n[UNIFIED_DRY_RUN=1] 발송 생략 — 메시지 2·3 미리보기:')
        if m2:
            print('\n'.join(m2).replace('<b>', '').replace('</b>', ''))
        print('\n'.join(m3).replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', ''))
        return
    _tk = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    _pid = os.environ.get('TELEGRAM_PRIVATE_ID', '')
    if not (_tk and _pid):
        # 폴백 1 = C:\dev\config.py (회사PC — 검증된 프로덕션 토큰. 집PC 것은 07-09 정리 때 삭제됨)
        try:
            sys.path.insert(0, r'C:\dev')
            from config import TELEGRAM_BOT_TOKEN as _tk, TELEGRAM_PRIVATE_ID as _pid
        except ImportError:
            # 폴백 2 = repo config.json (telegram_chat_id = 개인 유저 ID(양수) — daily_runner 패턴 준용.
            #   ⚠️집PC config.json 토큰은 2026-07-10 현재 401(폐기됨) — 유효 토큰으로 갱신 필요)
            import json as _j
            _cfg = _j.load(open(os.path.join(HERE, 'config.json'), encoding='utf-8'))
            _tk = _cfg['telegram_bot_token']
            _pid = _cfg.get('telegram_private_id') or _cfg['telegram_chat_id']
    _r = __import__('requests').get('https://api.telegram.org/bot%s/getMe' % _tk, timeout=15)
    if not _r.json().get('ok'):
        raise RuntimeError('텔레그램 토큰 무효(401) — 발송 불가. config 토큰을 갱신하세요.')
    _send_long(_tk, _pid, '\n'.join(m1))
    if m2:
        _send_long(_tk, _pid, '\n'.join(m2))
    _send_long(_tk, _pid, '\n'.join(m3))


if __name__ == '__main__':
    sys.path.insert(0, HERE)
    if '--nav' in sys.argv:
        cmd_nav()
    else:
        _merged_for_msg, _meta_for_msg = cmd_run()
        # 통합(US+KR) 신호 3종 발송 — 본선 (2026-07-09 사용자 확정)
        try:
            _compose_and_send(_merged_for_msg, _meta_for_msg)
        except Exception as _e:
            import traceback
            traceback.print_exc()
            print(f'[!!] 통합신호 발송 실패 — 메시지가 나가지 않았습니다: {_e}')
