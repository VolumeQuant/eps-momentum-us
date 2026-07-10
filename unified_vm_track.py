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
    반환: {'nav', 'days', 'state': {day: {'is_rebal', 'held_before', 'held_after'}}, 'ew_last'}
    held_before = 그날 리밸 직전 보유(교체 diff의 올바른 기준), held_after = 리밸 반영 후.
    ★국면 오버레이 (2026-07-10 사용자 승인): US 메인의 검증된 방어 신호(S&P200일선
    15일확인 OR VIX36 OR HY-OAS, daily_runner._regime_defense_series 재사용)를 날짜별
    주식비중으로 반영 — 방어일 수익 = ret×weight(0.0=현금). 지시(전량 현금)와 NAV 정합."""
    days, blocks = _ledger_blocks(rows)
    usd = _us_grid()
    # 날짜별 국면 주식비중 (실패 시 전부 1.0 = 기존과 동일)
    ew = {}
    try:
        import daily_runner as dr
        uds = sorted({blocks[d][0].get('us_date') for d in days if blocks.get(d)} - {None})
        if uds:
            ew, _ = dr._regime_defense_series(uds)
    except Exception as _e:
        print(f'[국면 시리즈 스킵(전부 주식100% 가정): {_e}]')
    nav, hold, ppx = 1.0, [], {}
    state = {}
    ew_last = 1.0
    for i, d in enumerate(days):
        day = blocks[d]
        px = {}
        for r in day:
            try:
                if r.get('price'):
                    px[r['ticker']] = float(r['price'])
            except (TypeError, ValueError):
                pass
        ud = day[0].get('us_date') if day else None
        w = float(ew.get(ud, 1.0))
        if hold:
            rr = [px[t] / ppx[t] - 1 for t in hold if t in px and t in ppx and ppx[t] > 0]
            if rr:
                nav *= 1 + (sum(rr) / len(rr)) * w
        gi = usd.index(ud) if ud in usd else None
        is_rb = (i == 0) or (gi is not None and gi % REBAL == 0)  # 첫 로그일 = 페이퍼 개시(초기 편입)
        held_before = list(hold)
        if is_rb:
            hold = [r['ticker'] for r in day if r.get('in_top4') == '1']
        state[d] = {'is_rebal': is_rb, 'held_before': held_before, 'held_after': list(hold)}
        ppx.update(px)
        ew_last = w
    return {'nav': nav, 'days': days, 'state': state, 'ew_last': ew_last}


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


def _gemini_key():
    """Gemini 키 — env → repo config.json → C:/dev/config.py (텔레그램 토큰 폴백과 동일 패턴).
    2026-07-10: 회사PC 로컬 실행에 env가 없어 AI 섹션이 조용히 빠지던 구멍 봉합."""
    key = os.environ.get('GEMINI_API_KEY', '')
    if not key:
        try:
            import json as _j
            key = _j.load(open(os.path.join(HERE, 'config.json'), encoding='utf-8')).get('gemini_api_key', '')
        except Exception:
            pass
    if not key:
        try:
            sys.path.insert(0, r'C:\dev')
            import config as _c
            key = getattr(_c, 'GEMINI_API_KEY', '')
        except Exception:
            pass
    return key


_MKT_LABELS = ('[미국 증시]', '[반도체·메모리]', '[한국 증시]', '[오늘 밤 체크]')


def _ai_market_brief(idx_facts=None):
    """AI 시황 — 4단락 문단형 (2026-07-10 전면 개편: 구 5문장 단문은 '기계 같다' 피드백).
    idx_facts: yf 실측 지수 문자열 리스트 — 프롬프트에 ground truth로 주입해 stale 숫자
    발송 차단(폴백 lite가 검색 없이 2024년 지수를 답한 사례 실관측). 키 없으면 None."""
    key = _gemini_key()
    if not key:
        return None
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=key, http_options={'timeout': 120_000})
        tool = types.Tool(google_search=types.GoogleSearch())
        facts = ''
        if idx_facts:
            facts = ('★오늘 실측 지수(이 숫자를 그대로 써라, 다른 출처의 지수 숫자 금지): '
                     + ' / '.join(idx_facts) + '\n')
        prompt = ('지금 한국시간 저녁이다. 구글 검색으로 사실 확인 후, 한국+미국 주식을 함께 '
                  '투자하는 사람을 위한 오늘의 시황 브리핑을 한국어 문어체(~습니다)로 써라. '
                  '아래 4개 단락을 대괄호 라벨 그대로 시작하고, 단락 사이에 빈 줄 1개. '
                  '각 단락 3~4문장, 구체 숫자 포함. 마크다운 헤더(#) 금지. '
                  '과장·투자권유·미확인 루머(상장설·인수설) 금지, 확인된 사실만.\n' + facts +
                  '[미국 증시] 지난밤 마감 — 지수 등락과 원인, 주도 섹터와 종목.\n'
                  '[반도체·메모리] HBM·D램·낸드 가격과 수급, 주요 기업 뉴스.\n'
                  '[한국 증시] 오늘 코스피·코스닥 마감과 특징 업종, 원/달러 환율.\n'
                  '[오늘 밤 체크] 미국 경제지표·연준 발언·주요 실적 발표 일정과 관전 포인트.')
        # 모델 폴백 체인 (2026-07-10 실측): 2.5-flash 무료 20회/일 — KR 16:00 시스템과
        # 키 공유라 저녁엔 쿼터 소진 잦음(당일 실발생) → flash-lite 폴백(무료 한도 큼).
        # lite는 형식 이탈이 잦아 4개 라벨 검증 후 통과분만 채택, 실패 시 1회 더.
        for model in ('gemini-2.5-flash', 'gemini-2.5-flash-lite', 'gemini-2.5-flash-lite'):
            try:
                resp = client.models.generate_content(
                    model=model, contents=prompt,
                    config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
                txt = (resp.text or '').strip()
                if txt and all(lb in txt for lb in _MKT_LABELS):
                    if model != 'gemini-2.5-flash':
                        print(f'[AI 시황: {model} 폴백 사용]')
                    return txt
                if txt:
                    print(f'[AI 시황 {model}: 라벨 형식 미달 → 재시도]')
            except Exception as _e:
                print(f'[AI 시황 {model} 실패: {str(_e)[:120]}]')
        return None
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
    '009150.KS': '삼성전기', '402340.KS': 'SK스퀘어', '066570.KS': 'LG전자',
    '051910.KS': 'LG화학', '006400.KS': '삼성SDI', 'SOFI': '소파이',
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
    KR_IND = {'000660.KS': '메모리 반도체', '005930.KS': '전자', '011070.KS': '전자부품',
              '066570.KS': '가전·전장', '051910.KS': '화학·배터리', '006400.KS': '배터리',
              '009150.KS': '전자부품'}
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
                l2.append('시총 %.1f조달러' % (mc / 1e12) if mc >= 1e12 else '시총 $%.0fB' % (mc / 1e9))
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
    """브리핑을 {biz, why, risk} dict로 강제 — 문자열이 와도 카드 렌더가 깨지지 않게.
    형식: '회사소개 | 왜 핫한지 || 리스크' ('|' 앞=소개, '|' 뒤=상향 이유, '||' 뒤=리스크)."""
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip():
        parts = [p.strip() for p in x.split('||')]
        head = parts[0]
        risk = parts[1] if len(parts) > 1 else ''
        if '|' in head:
            biz, why = head.split('|', 1)
        else:
            biz, why = head, ''
        return {'biz': biz.strip(), 'why': why.strip(), 'risk': risk}
    return {}


def _ai_stock_briefs(entries):
    """종목 브리핑 1콜 — 1~5위 상세, 6~10위 두 문장. {ticker: dict} (실패시 빈 dict).
    2026-07-10 개편: 6~20위→6~10위, 소개/이유 분리('|'), 존댓말 문어체,
    top5 파싱 커버리지 검증 후 재시도(1위 브리핑 누락 발송 재발 방지)."""
    key = _gemini_key()
    if not key or not entries:
        return {}
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=key, http_options={'timeout': 150_000})
        tool = types.Tool(google_search=types.GoogleSearch())
        # 2026-07-10 사용자 결정: 6~10위도 1~5위와 동일한 풀 설명 (차별 없음)
        dl = []
        for d in entries[:10]:
            dl.append('%s(%s, %s): 90일 이익전망 %+.0f%%, 선행PER %.0f'
                      % (d['ticker'], _display_name(d['ticker']), _industry_tag(d) or '업종미상', d['rev90'], d['fwd_per']))
        prompt = ('한국+미국 주식 퀀트 시스템의 오늘 순위 10종목이다. 각 종목을 처음 듣는 일반 '
                  '투자자에게 설명하듯 한국어 존댓말 문어체(~습니다)로 써라. '
                  '★모든 종목은 지금 상장되어 활발히 거래 중이다. 상장폐지·인수 소멸 서술 절대 금지, '
                  '미확인 루머(상장 추진설·인수설 등) 금지, 반드시 2026년 최신 정보를 검색해 확인하라'
                  '(예: SNDK는 2025년 웨스턴디지털에서 분사 재상장한 샌디스크). '
                  '종목당 구성: (a)무슨 사업으로 돈 버는 회사인지 1~2문장(제품·고객이 그려지게) '
                  '(b)왜 지금 이익전망이 급상향되는지 2~3문장 — 최근 실적발표·수주·제품가격·점유율 등 '
                  '구체 숫자를 검색으로 확인해 포함 (c)리스크 1~2문장(막연한 일반론 금지, 이 회사 고유의 위험). '
                  '자연스러운 완결 문장으로, 전문용어는 한 번씩 풀어서. 과장 없이 사실만. '
                  '★형식(종목당 정확히 한 줄): "TICKER: 회사소개 문장 | 상향 이유 문장들 || 리스크 문장들" '
                  '(소개와 이유 사이 |, 리스크 앞 || 필수).\n' + '\n'.join(dl))

        def _parse(text):
            out = {}
            for d in entries[:10]:
                tk = d['ticker']
                base = tk.split('.')[0]
                for ln in text.splitlines():
                    t = ln.strip().lstrip('-*• ')
                    if t.upper().startswith(tk.upper() + ':') or t.upper().startswith(base.upper() + ':'):
                        out[tk] = _brief_dict(t.split(':', 1)[1].strip())
                        break
            return out

        out = {}
        need = {d['ticker'] for d in entries[:10]}
        # 모델 폴백 체인 (2026-07-10 실측): flash 무료 20회/일(KR 16:00 시스템과 키 공유,
        # 당일 소진 실발생) → flash-lite 폴백. lite는 형식 이탈이 잦아 flash 우선 2회.
        attempts = [('gemini-2.5-flash', 1), ('gemini-2.5-flash', 2),
                    ('gemini-2.5-flash-lite', 1), ('gemini-2.5-flash-lite', 2)]
        for _i, (model, _n) in enumerate(attempts, 1):
            try:
                resp = client.models.generate_content(
                    model=model, contents=prompt,
                    config=types.GenerateContentConfig(tools=[tool], temperature=0.2))
                cand = _parse(resp.text or '')
                out.update({k: v for k, v in cand.items() if k not in out or not out[k]})
                # 10종목 전원 파싱됐으면 성공 — 아니면 재시도 (브리핑 누락 발송 방지)
                if len(need - set(out)) == 0:
                    if model != 'gemini-2.5-flash':
                        print(f'[브리핑: {model} 폴백 사용]')
                    break
                print('[브리핑 시도 %d(%s): 10종목 중 %d개 누락 → 재시도]' % (_i, model, len(need - set(out))))
            except Exception as _e:
                print('[브리핑 시도 %d(%s) 실패: %s]' % (_i, model, str(_e)[:120]))
                import time as _t
                _t.sleep(10)
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



def _card_facts(d, cards_map):
    """cards_map 원시줄에서 팩트 추출 — {analysts, rev_growth, mcap, dv, margin}."""
    f = {}
    for c in (cards_map.get(d['ticker']) or []):
        for part in [x.strip() for x in c.split('·')]:
            if part.startswith('분석가'):
                f['analysts'] = part.replace('분석가 ', '')
            elif part.startswith('매출'):
                f['rev_growth'] = part
            elif part.startswith('시총'):
                f['mcap'] = part
            elif part.startswith('거래'):
                f['dv'] = part.replace('거래 ', '')
            elif part.startswith('마진'):
                f['margin'] = part
    return f


def _stock_card(rank, d, brief, cards_map, first=False):
    """종목 카드 (2026-07-10 개편) — 이름/티커/업종 → 무슨 회사 → 왜 뜨거운가 → 숫자 → 위험."""
    nm = _display_name(d['ticker'])
    tk = d['ticker'].replace('.KS', '').replace('.KQ', '')
    sect = _industry_tag(d)
    nation = '🇰🇷 한국' if d['market'] == 'KR' else '🇺🇸 미국'
    L = ['━━━━━━━━━━━━━━',
         f"<b>{rank}위 {nm}</b> ({tk})",
         f"{nation} · {sect}".replace('  ', ' ') if sect else nation, '']
    b = _brief_dict(brief)
    if b.get('biz'):
        L.append('<b>무슨 회사인가요?</b>')
        L += _split_sents(b['biz'])
        L.append('')
    if b.get('why'):
        L.append('<b>왜 지금 뜨거운가요?</b>')
        L += _split_sents(b['why'])
        L.append('')
    L.append('<b>숫자로 확인하기</b>')
    mk = '미국' if d['market'] == 'US' else '한국'
    L.append(f"· 이익전망 3개월간 <b>+{d['rev90']:.0f}%</b> 상향")
    if d.get('pct') is not None:
        L.append(f"  ({mk} 전체에서 상위 {max(100 - d['pct'], 1):.0f}% 희소성)")
    if first:
        L.append('  = 전문가들이 이 회사 이익 전망치를')
        L.append(f"    석 달 만에 {1 + d['rev90'] / 100:.1f}배로 올렸다는 뜻")
    fx = _card_facts(d, cards_map)
    if fx.get('analysts'):
        an = fx['analysts'].replace('(↑', ' (30일 ↑').replace('/↓', ' ↓')
        L.append(f"· 애널리스트 {an}")
    if d.get('gap'):
        L.append(f"· 올해 예상이익 = 작년의 <b>{d['gap']:.1f}배</b>")
    L.append(f"· 선행PER <b>{d['fwd_per']:.0f}배</b>"
             + (f" ({PER_ANCHOR[d['market']]})" if first or d['fwd_per'] >= 20 else ''))
    if first:
        L.append('  = 올해 예상이익 대비 주가 배수, 낮을수록 쌈')
    sz = ' · '.join(x for x in (fx.get('mcap'),
                                ('하루 거래 ' + fx['dv'].replace('/일', '')) if fx.get('dv') else None) if x)
    if sz:
        L.append('· ' + sz)
    L.append('')
    if b.get('risk'):
        L.append('<b>⚠️ 위험 요인</b>')
        L += _split_sents(b['risk'])
        L.append('→ 그래서 5거래일마다 점검해 교체합니다.')
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




def _next_msg_day(us_latest, next_in):
    """다음 교체 지시 메시지의 KST 날짜 근사 — us_latest에서 미국 거래일 next_in개 진행 후
    그 다음 KST 평일(주말이면 월요일). 미국 휴장일 미반영이라 '예정' 라벨과 함께 쓸 것."""
    from datetime import datetime, timedelta
    try:
        cur = datetime.strptime(us_latest, '%Y-%m-%d')
    except Exception:
        return None
    cnt = 0
    while cnt < next_in:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            cnt += 1
    msg = cur + timedelta(days=1)
    while msg.weekday() >= 5:
        msg += timedelta(days=1)
    return msg


def _earnings_lines(tickers):
    """보유종목 14일 내 실적발표 일정 (yf calendar, 실패 종목은 조용히 생략)."""
    out = []
    try:
        import yfinance as yf
        from datetime import datetime, timedelta
        lim = (datetime.now() + timedelta(days=14)).date()
        today = datetime.now().date()
        for tk in tickers:
            try:
                cal = yf.Ticker(tk).calendar or {}
                eds = cal.get('Earnings Date') or []
                for ed in eds[:1]:
                    d = ed.date() if hasattr(ed, 'date') and callable(getattr(ed, 'date')) else ed
                    if today <= d <= lim:
                        out.append(f"· {_display_name(tk)}: {d.month}/{d.day} 실적 발표 예정")
            except Exception:
                continue
    except Exception:
        pass
    return out


def _credit_vol_lines():
    """신용(HY-OAS)·변동성(VIX) 상태 — 시스템 방어 임계 대비 현재 위치."""
    out = []
    try:
        import daily_runner as dr
        hy = dr._compute_hy_oas_defense()
        if hy:
            oas, trough, fired = hy
            st = '🔴 경계 (방어 신호)' if fired else '🟢 안정'
            out.append(f"회사채 금리차(HY) {oas:.2f}%p {st}")
            out.append(f"  6개월 저점 대비 +{max(oas - trough, 0):.2f}%p (경보선 +1.0%p)")
    except Exception as e:
        print(f'[HY 스킵: {e}]')
    try:
        import yfinance as yf
        fi = yf.Ticker('^VIX').fast_info
        v = fi.last_price
        if v:
            st = '🔴 공포 구간' if v > 36 else ('🟡 다소 높음' if v > 25 else '🟢 안정')
            out.append(f"변동성지수(VIX) {v:.1f} {st} (방어선 36)")
    except Exception:
        pass
    if out:
        out.append('→ 신용시장이 흔들리면 주식보다 먼저')
        out.append('  움직여서, 방어 전환 신호로 씁니다.')
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
    m10 = merged[:10]  # 2026-07-10 사용자 결정: top20은 과다 → TOP5 + 대기 6~10위만
    top5 = m10[:N_TOP]
    briefs = _ai_stock_briefs(m10)
    cards = _us_cards([d['ticker'] for d in m10 if d['market'] == 'US'])
    for d in m10:
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
    # ── 국면 오버레이 (2026-07-10 사용자 승인): US 메인의 검증된 방어 신호 재사용 ──
    # S&P<200일선(15일 확인) OR VIX>36(2일) OR HY-OAS 신용경보 → defense(주식 0%).
    # 26~30년 검증(dotcom/GFC/COVID/2022 포착). 실패 시 boost 가정(기존과 동일).
    try:
        import daily_runner as _dr
        reg = _dr.get_market_regime() or {}
    except Exception as _e:
        print(f'[국면 조회 스킵(강세 가정): {_e}]')
        reg = {}
    regime = reg.get('regime', 'boost')
    reentry = (regime != 'defense') and (rp.get('ew_last', 1.0) == 0.0)  # 방어→강세 복귀 첫날

    # ── 메시지 1: 상단 공통 + TOP5 카드 (2026-07-10 전면 개편) ──
    kdt = _dt.now()
    wd = '월화수목금토일'[kdt.weekday()]
    alert_head = (amsg or '').split('\n')[0]  # 신호등 상태 한 줄 — 상단 노출 (상세는 메시지3)
    m1 = [f'📬 <b>한국+미국 TOP5 신호</b> | {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━']
    if regime == 'defense':
        m1.append('국면: 🛑 방어 — 주식 0% (전량 현금)')
    elif regime == 'half_defense':
        m1.append('국면: 🟠 부분 방어 — 주식 50%')
    else:
        m1.append('국면: 🟢 강세 — 주식 100%')
        _db = reg.get('days_below') or 0
        if _db:
            m1.append(f'⚠️ 약세 신호 누적 {_db}일 (15일 확인 중 — 아직 매매 변화 없음)')
    if alert_head:
        m1.append(alert_head)
    m1.append('')
    if meta.get('warnings'):
        m1.append('⚠️ <b>데이터 품질 주의</b>')
        for wmsg in meta['warnings']:
            m1 += _wrap('· ' + wmsg, 44)
        m1.append('')
    if fired:
        m1 += ['🔴 <b>메모리 위험 경보 발동!</b>',
               '시장 브리핑의 신호등 안내에 따라',
               '메모리 종목을 정리하세요.', '']
    nxt = _next_msg_day(us_latest, next_in) if us_latest else None
    nxt_s = f"{nxt.month}/{nxt.day}({'월화수목금토일'[nxt.weekday()]}) 저녁" if nxt else f"{next_in}거래일 후"
    if regime == 'defense':
        m1 += ['🛑 <b>오늘 할 일: 전량 현금</b>',
               '시장 전체가 약세 국면으로 판정됐습니다',
               '(S&P500 200일선 이탈 15일 확인 또는',
               ' 공포지수·신용시장 경보).',
               '보유 종목을 전부 팔고 현금으로',
               '보관하세요. 이미 파셨다면 그대로 유지.',
               '🟢 강세 복귀 알림이 올 때까지',
               '신규 매수는 하지 않습니다.']
    elif reentry:
        m1 += ['🟢 <b>오늘 할 일: 강세 복귀 — 재진입</b>',
               '방어 국면이 해제됐습니다.',
               '아래 TOP5를 각 20%씩 한 번에 매수하세요.',
               '(미국 종목 = 오늘 밤 개장,',
               ' 한국 종목 = 내일 아침 개장)']
        for t in [d['ticker'] for d in top5]:
            m1.append(f'🟢 사기: {_display_name(t)} — 자산의 20%')
    elif is_rebal and diff and (diff[0] or diff[1]):
        n_ch = max(len(diff[0]), len(diff[1]))
        m1.append(f'🔁 <b>오늘 할 일: 종목 {n_ch}개 교체</b>')
        for t in diff[1]:
            m1.append(f'🔴 팔기: {_display_name(t)} — 보유분 전량 매도')
        for t in diff[0]:
            m1.append(f'🟢 사기: {_display_name(t)} — 자산의 20% 매수')
        m1 += ['나머지 종목은 그대로 유지하세요.',
               '(이미 처리했거나 갖고 있지 않은',
               ' 종목은 건너뛰면 됩니다)',
               '미국 종목은 오늘 밤 개장에,',
               '한국 종목은 내일 아침 개장에 매매.']
    elif is_rebal:
        m1 += ['✅ <b>오늘 할 일: 없음</b> (교체 점검일)',
               '점검 결과 교체 없이 그대로 갑니다.']
    else:
        m1 += ['✅ <b>오늘 할 일: 없음</b>',
               '보유 중인 5종목 그대로 두시면 됩니다.',
               f'다음 교체 점검: <b>{nxt_s}</b> 예정']
    m1 += ['',
           '<b>이 서비스, 뭐 하는 건가요?</b>',
           '한국+미국 주요 상장사 약 1,600곳의',
           '애널리스트 이익 전망을 매일 추적해서,',
           '"전문가들이 이익 전망을 가장 가파르게',
           '올리는 중"인 회사 딱 5곳을 골라 담는',
           '퀀트 신호입니다. 각 20%씩, 5거래일마다',
           '점검해 순위에서 밀린 종목을 교체합니다.',
           '비싼 주식(선행PER 30↑)과 전망이 꺾인',
           '주식은 아무리 순위가 높아도 걸러냅니다.', '',
           '한국·미국은 상향폭 눈금이 달라서(뜨는',
           '종목 기준 한국이 약 2배 큼) 절대값 대신',
           '"자기 시장 상위 몇 %인지"로 공정 비교.', '',
           f"📊 전략 누적 성과: {(nav - 1) * 100:+.1f}%",
           f"({all_days[0][5:].replace('-', '/')} 모의운용 시작)" if all_days else '']
    if not any(briefs.get(d['ticker']) for d in top5):
        m1 += ['', '⚠️ 오늘은 AI 종목 설명 생성에 실패해',
               '숫자 지표만 표시됩니다. 다음 발송에서',
               '자동 복구됩니다.']
    if regime == 'defense':
        m1 += ['', '📋 아래 순위는 <b>관찰용</b>입니다.',
               '방어 국면에는 매수하지 않습니다.']
    for i, d in enumerate(top5, 1):
        m1 += _stock_card(i, d, briefs.get(d['ticker']), cards, first=(i == 1))
    m1 += ['━━━━━━━━━━━━━━', '📖 <b>용어 한 줄 정리</b>',
           'EPS: 주식 1주가 벌어들이는 이익',
           'PER: 주가가 이익의 몇 배인지 (낮을수록 저렴)',
           '선행: 과거가 아닌 "올해 예상" 기준']
    # ── 메시지 2: 대기 후보 6~10위 — 1~5위와 동일한 풀카드 (2026-07-10 사용자 "차별하지 마") ──
    m2 = None
    if len(m10) > N_TOP:
        m2 = [f'📋 <b>대기 후보 6~10위</b> | {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━',
              '<b>지금 사는 종목이 아닙니다.</b>',
              'TOP5에서 빠지는 종목이 생기면',
              '이 명단의 위쪽부터 차례로 들어옵니다.', '']
        for j, d in enumerate(m10[N_TOP:], N_TOP + 1):
            m2 += _stock_card(j, d, briefs.get(d['ticker']), cards)
    # ── 메시지 3: AI 시장 분석 (2026-07-10 개편: 단락형 시황+신용·변동성+보유종목 일정) ──
    m3 = [f'🤖 <b>AI 시장 분석</b> | {kdt.month}월 {kdt.day}일({wd})', '━━━━━━━━━━━━━━']
    idx_lines = []
    try:
        import yfinance as yf
        for sym, nm in [('^GSPC', 'S&P500'), ('^IXIC', '나스닥'), ('^SOX', '반도체지수'),
                        ('^KS11', '코스피'), ('^KQ11', '코스닥'), ('KRW=X', '원/달러')]:
            try:
                fi = yf.Ticker(sym).fast_info
                px, pv = fi.last_price, fi.previous_close
                if px and pv:
                    idx_lines.append(f"{nm} {px:,.0f} ({(px / pv - 1) * 100:+.1f}%)")
            except Exception:
                pass
        if idx_lines:
            m3 += ['', '📊 <b>주요 지수</b>'] + idx_lines
    except Exception:
        pass
    if reg:
        st = {'defense': '🛑 방어 (주식 0%, 현금)',
              'half_defense': '🟠 부분 방어 (주식 50%)'}.get(regime, '🟢 강세 (주식 100%)')
        m3 += ['', '🧭 <b>시장 국면</b>', st]
        if reg.get('spx') and reg.get('ma200'):
            pos = '위' if reg['spx'] > reg['ma200'] else '아래'
            m3.append(f"S&P500 {reg['spx']:,.0f} — 200일선({reg['ma200']:,.0f}) {pos}")
        m3 += ['200일선 15일 이탈·공포지수·신용경보 중',
               '하나라도 확정되면 전량 현금으로 피합니다.']
    cv = _credit_vol_lines()
    if cv:
        m3 += ['', '🏦 <b>신용·변동성</b>'] + cv
    brief_mkt = _ai_market_brief(idx_facts=idx_lines)
    if brief_mkt:
        m3 += ['', '📰 <b>시장 동향</b>']
        for para in brief_mkt.replace('\r', '').split('\n'):
            p = para.strip()
            if p:
                m3.append(p)
                m3.append('')
        if m3[-1] == '':
            m3.pop()
    el = _earnings_lines([d['ticker'] for d in top5])
    if el:
        m3 += ['', '📅 <b>보유종목 일정 (14일 내)</b>'] + el
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
    # ── 채널 발송 (2026-07-10 사용자 결정: US 채널 상품 = 아침 US 단독 → 저녁 통합으로 대체) ──
    # UNIFIED_CHANNEL_ID가 설정된 경우에만 발송(미설정 = 개인봇 단독 = 현행 유지).
    # 켜기: env UNIFIED_CHANNEL_ID(+봇이 다르면 UNIFIED_CHANNEL_BOT_TOKEN) 또는
    #       C:/dev/config.py에 UNIFIED_CHANNEL_ID(_BOT_TOKEN) 추가. 봇은 채널 관리자여야 함.
    # 안전판: 백분위 폴백(norm != 'pct') 등 본선 방식이 깨진 날은 채널 차단(개인봇만).
    ch_id = os.environ.get('UNIFIED_CHANNEL_ID', '')
    ch_tk = os.environ.get('UNIFIED_CHANNEL_BOT_TOKEN', '')
    if not ch_id:
        try:
            sys.path.insert(0, r'C:\dev')
            import config as _c2
            ch_id = str(getattr(_c2, 'UNIFIED_CHANNEL_ID', '') or '')
            ch_tk = ch_tk or str(getattr(_c2, 'UNIFIED_CHANNEL_BOT_TOKEN', '') or '')
        except ImportError:
            pass
    if ch_id:
        if (meta or {}).get('norm', 'pct') != 'pct':
            print('[채널 발송 차단: 본선(백분위) 폴백 상태 — 개인봇만 발송]')
            _send_long(_tk, _pid, '⚠️ 오늘 통합 신호는 백분위 계산 폴백 상태라 채널 발송을 건너뛰었습니다.')
        else:
            ch_tk = ch_tk or _tk
            _rc = __import__('requests').get('https://api.telegram.org/bot%s/getMe' % ch_tk, timeout=15)
            if _rc.json().get('ok'):
                _send_long(ch_tk, ch_id, '\n'.join(m1))
                if m2:
                    _send_long(ch_tk, ch_id, '\n'.join(m2))
                _send_long(ch_tk, ch_id, '\n'.join(m3))
                print(f'[채널 발송 완료: {ch_id[:6]}…]')
            else:
                print('[채널 봇 토큰 무효 — 채널 발송 실패, 개인봇은 발송됨]')


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
