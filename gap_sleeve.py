#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""기대성장(gap) 분산 Sleeve — 별도 시스템 (2026-06-25 설계·빌드)

gap = NTM EPS / TTM 실적 EPS = trailing PER / forward PER = 시장이 기대하는 EPS 성장률.
검증: cross-sectional IC +0.23(PIT-clean, 직교), 분산 sleeve로 유니버스 +19.6%p·SPY +56%p 초과,
      3월 조정·6월 이란 스트레스 구간 방어 확인. 상세: research/GAP_SLEEVE_DESIGN_2026_06_25.md.

★ EPS-모멘텀 2슬롯 시스템과 완전 별개 sleeve (gap을 그 점수공식에 주입하면 −25~−50p로 검증됨).
유니버스 = 기존 daily EPS-screen eligible(composite_rank) 재사용. 새 데이터 인프라 = trailing EPS 캐시뿐.

구성: gap 상위 TOP_K 동일가중, 월간 리밸(달 첫 거래일). SPY 200DMA / VIX 오버레이 → 현금(get_market_regime 재사용).
실행: `python gap_sleeve.py` (배포). 테스트: `python gap_sleeve.py --test [--date YYYY-MM-DD]` (발송 안 함).
킬스위치: 환경변수 SLEEVE_DISABLE=1.
"""
import os
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / 'eps_momentum_data.db'
CACHE_DIR = PROJECT_ROOT / 'data_cache'
TEPS_CACHE = CACHE_DIR / 'trailing_eps_ttm.json'
STATE_PATH = PROJECT_ROOT / 'gap_sleeve_state.json'

# ── 파라미터 (K=1~20 전수 + walk-forward 검증, 2026-06-25) ──
#   E/X/S: 슬롯 7 / 진입 gap 상위7위 / 이탈 7위밖(버퍼없음 strict). gap은 '신선도' 팩터라
#   버퍼(decayed 보유) 손해 → 진입=이탈 대칭. K=5~7 robust plateau, K=7이 WF 가장 대칭(전7.5/후6.8)·MDD-15%.
TOP_K = int(os.environ.get('SLEEVE_TOP_K', '7'))       # 슬롯=진입=이탈 (strict 대칭, 버퍼없음)
MIN_TRAILING_EPS = 0.5    # 턴어라운드(저베이스 gap 폭발) 제외
MAX_GAP = 10.0            # speculative tail 제외
MIN_DOLLAR_VOL = float(os.environ.get('SLEEVE_MIN_DV', '1000'))  # $M. $1B+ 유동성 필터(검증: +47→+67%·MDD-20→-18%, 얇은종목 제외)
REPORT_LAG_DAYS = 45      # 분기말 → 공시 지연(PIT)
TEPS_CACHE_STALE_DAYS = 7  # 트레일링 EPS 캐시 갱신 주기


def log(msg):
    print(f"[gap_sleeve] {msg}", flush=True)


# ════════════════════════════════════════════════════════════
# 1. Trailing EPS 캐시 (PIT TTM)
# ════════════════════════════════════════════════════════════
def _load_teps_cache():
    if TEPS_CACHE.exists():
        try:
            with open(TEPS_CACHE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def build_trailing_eps_cache(tickers, force=False):
    """yfinance 분기 diluted EPS → PIT TTM 시계열 캐시.

    cache[ticker] = [[report_date(보고지연반영), ttm_eps], ...] 시간순.
    증분: 캐시에 없는 종목 + stale(>TEPS_CACHE_STALE_DAYS) 종목만 갱신.
    """
    import pandas as pd
    import yfinance as yf
    CACHE_DIR.mkdir(exist_ok=True)
    cache = _load_teps_cache()
    meta = cache.get('_meta', {})
    today = datetime.now().strftime('%Y-%m-%d')

    def _stale(tk):
        fetched = meta.get(tk)
        if not fetched:
            return True
        try:
            age = (datetime.strptime(today, '%Y-%m-%d') - datetime.strptime(fetched, '%Y-%m-%d')).days
            return age >= TEPS_CACHE_STALE_DAYS
        except Exception:
            return True

    to_fetch = [t for t in tickers if force or t not in cache or _stale(t)]
    if not to_fetch:
        log(f"trailing EPS 캐시 최신 ({len(cache)-1 if '_meta' in cache else len(cache)}종목)")
        return cache
    log(f"trailing EPS 캐시 갱신: {len(to_fetch)}종목 fetch")
    ok = 0
    for tk in to_fetch:
        try:
            t = yf.Ticker(tk)
            try:
                inf = t.info or {}
            except Exception:
                inf = {}
            # 재무 통화가 USD가 아니면(외국 ADR: TSM=TWD, SKHY=KRW 등) TTM이 현지통화라
            # USD 추정치와 나눗셈 시 gap이 엉터리 → 캐시 제외(missing=pass)
            fc = inf.get('financialCurrency')
            if fc and fc != 'USD':
                cache.pop(tk, None)
                meta[tk] = today
                continue
            qi = t.quarterly_income_stmt
            if qi is None or qi.empty:
                meta[tk] = today
                continue
            row = None
            for k in ('Diluted EPS', 'Basic EPS'):
                if k in qi.index:
                    row = qi.loc[k]
                    break
            if row is None:
                meta[tk] = today
                continue
            q = row.dropna().sort_index()       # index = 분기말일 오름차순
            qe = list(q.items())
            rec = []
            for j in range(3, len(qe)):
                ttm = sum(float(qe[j - k][1]) for k in range(4))
                rdate = (qe[j][0] + pd.Timedelta(days=REPORT_LAG_DAYS)).strftime('%Y-%m-%d')
                rec.append([rdate, ttm])
            # 정합 가드: 최신 TTM이 yf trailingEps(분할·통화 조정본)와 3배 이상 어긋나면
            # 스플릿 미조정 등 오염(KLAC 사례) → 캐시 제외(missing=pass)
            te_ref = inf.get('trailingEps')
            if rec and te_ref and abs(te_ref) > 0.01:
                ratio = rec[-1][1] / te_ref
                if not (0.33 < ratio < 3.0):
                    cache.pop(tk, None)
                    meta[tk] = today
                    continue
            if rec:
                cache[tk] = rec
                ok += 1
            meta[tk] = today
        except Exception as e:
            meta[tk] = today
            continue
    cache['_meta'] = meta
    with open(TEPS_CACHE, 'w', encoding='utf-8') as f:
        json.dump(cache, f)
    log(f"trailing EPS 캐시 저장: {ok}/{len(to_fetch)} 신규 성공")
    return cache


def pit_trailing_eps(cache, ticker, date_str):
    """date_str 시점 PIT trailing TTM EPS (보고지연 반영). 없으면 None."""
    rec = cache.get(ticker)
    if not rec:
        return None
    val = None
    for rdate, ttm in rec:
        if rdate <= date_str:
            val = ttm
        else:
            break
    return val


# ════════════════════════════════════════════════════════════
# 2. gap 계산 (유니버스 = DB eligible)
# ════════════════════════════════════════════════════════════
def get_eligible_universe(today_str):
    """그날 EPS-screen eligible (composite_rank not null) + ntm_current + dollar_volume."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT ticker, ntm_current, dollar_volume_30d, price FROM ntm_screening "
        "WHERE date=? AND composite_rank IS NOT NULL", (today_str,)
    ).fetchall()
    conn.close()
    return {r[0]: {'nc': r[1], 'dv': r[2], 'price': r[3]} for r in rows}


def _dollar_volumes(tickers, today_str):
    """후보 종목 30일 평균 거래대금($M). DB(top30) 우선, 없으면 yfinance(Close×Volume)."""
    out = {}
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for tk in tickers:
        r = cur.execute("SELECT dollar_volume_30d FROM ntm_screening WHERE date=? AND ticker=?",
                        (today_str, tk)).fetchone()
        if r and r[0]:
            out[tk] = float(r[0])
    conn.close()
    missing = [t for t in tickers if t not in out]
    if missing:
        try:
            import yfinance as yf
            import pandas as pd
            end = (datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=2)).strftime('%Y-%m-%d')
            start = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=60)).strftime('%Y-%m-%d')
            df = yf.download(' '.join(missing), start=start, end=end, auto_adjust=False,
                             progress=False, threads=True, group_by='ticker')
            for tk in missing:
                try:
                    sub = df[tk] if isinstance(df.columns, pd.MultiIndex) and tk in df.columns.get_level_values(0) else df
                    sub = sub[sub.index < pd.to_datetime(today_str)]
                    dvm = (sub['Volume'] * sub['Close'] / 1e6).tail(30).mean()
                    if pd.notna(dvm):
                        out[tk] = float(dvm)
                except Exception:
                    pass
        except Exception as e:
            log(f"거래대금 fetch 실패(필터 완화): {e}")
    return out


def compute_gaps(today_str, cache):
    """eligible 종목별 gap = ntm_current / TTM_trailing_EPS. 정제필터 + $1B 유동성필터 후 gap 내림차순.

    $1B 필터: gap 상위 후보(넉넉히 TOP_K*4)만 거래대금 조회해 적용 → 전체 유니버스 fetch 불필요.
    """
    uni = get_eligible_universe(today_str)
    out = []
    for tk, v in uni.items():
        nc = v['nc']
        if not nc or nc <= 0:
            continue
        te = pit_trailing_eps(cache, tk, today_str)
        if te is None or te < MIN_TRAILING_EPS:        # 턴어라운드/저베이스 제외
            continue
        g = nc / te
        if g > MAX_GAP:                                 # speculative tail 제외
            continue
        out.append({'ticker': tk, 'gap': g, 'ntm': nc, 'trailing': te,
                    'dv': v['dv'], 'price': v['price']})
    out.sort(key=lambda x: -x['gap'])

    # $1B 유동성 필터 — gap 상위 후보만 거래대금 조회(DB→yfinance) 후 적용.
    #   조회한 후보 중 미달은 제외. 미조회(저gap 하위)는 통과(어차피 top-K 미선정).
    if MIN_DOLLAR_VOL > 0 and out:
        cand_n = max(TOP_K * 4, 30)
        dvs = _dollar_volumes([r['ticker'] for r in out[:cand_n]], today_str)
        result = []
        for r in out:
            d = dvs.get(r['ticker'])
            if d is not None:
                r['dv'] = d
                if d < MIN_DOLLAR_VOL:
                    continue                            # 거래대금 미달 → 제외
            result.append(r)
        out = result
    return out


# ════════════════════════════════════════════════════════════
# 3. 상태 (holdings, NAV, 리밸)
# ════════════════════════════════════════════════════════════
def load_state():
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {'holdings': [], 'last_prices': {}, 'last_rebal_month': None,
            'nav': 1.0, 'peak': 1.0, 'mdd': 0.0, 'inception': None, 'last_run': None,
            'history': []}


def save_state(state):
    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def fetch_prices(tickers, today_str):
    """held/후보 종목의 today_str 종가 (DB 우선, 없으면 yfinance)."""
    prices = {}
    # DB 우선 (당일 eligible이면 있음)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for tk in tickers:
        r = cur.execute("SELECT price FROM ntm_screening WHERE date=? AND ticker=?",
                        (today_str, tk)).fetchone()
        if r and r[0]:
            prices[tk] = float(r[0])
    conn.close()
    missing = [t for t in tickers if t not in prices]
    if missing:
        try:
            import yfinance as yf
            import pandas as pd
            end = (datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            start = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
            df = yf.download(' '.join(missing), start=start, end=end,
                             auto_adjust=False, progress=False, threads=True)['Close']
            for tk in missing:
                try:
                    s = df[tk].dropna() if hasattr(df, 'columns') and tk in df.columns else (
                        df.dropna() if not hasattr(df, 'columns') else None)
                    if s is not None and len(s):
                        prices[tk] = float(s.iloc[-1])
                except Exception:
                    pass
        except Exception as e:
            log(f"yfinance 가격 fetch 실패: {e}")
    return prices


# ════════════════════════════════════════════════════════════
# 4. 메인 로직
# ════════════════════════════════════════════════════════════
def run_sleeve(today_str=None, test_mode=True, private=True):
    if os.environ.get('SLEEVE_DISABLE') == '1':
        log("SLEEVE_DISABLE=1 → 중단")
        return None
    if today_str is None:
        # DB 최신일
        conn = sqlite3.connect(DB_PATH)
        today_str = conn.execute(
            "SELECT MAX(date) FROM ntm_screening WHERE composite_rank IS NOT NULL").fetchone()[0]
        conn.close()
    log(f"=== gap sleeve 실행 {today_str} (test={test_mode}) ===")

    state = load_state()
    uni = get_eligible_universe(today_str)
    cache = build_trailing_eps_cache(list(uni.keys()))

    # 1) 보유 종목 mark-to-market (리밸 전, 직전 보유 기준)
    ret_pct = None
    if state['holdings'] and state['last_prices']:
        cur_px = fetch_prices(state['holdings'], today_str)
        rs = []
        for tk in state['holdings']:
            p0 = state['last_prices'].get(tk)
            p1 = cur_px.get(tk)
            if p0 and p1 and p0 > 0:
                rs.append(p1 / p0 - 1)
        if rs:
            ret_pct = sum(rs) / len(rs) * 100
            state['nav'] *= (1 + ret_pct / 100)
            state['peak'] = max(state['peak'], state['nav'])
            state['mdd'] = min(state['mdd'], state['nav'] / state['peak'] - 1)
            # last_prices 갱신 (보유 종목)
            for tk in state['holdings']:
                if tk in cur_px:
                    state['last_prices'][tk] = cur_px[tk]

    # 2) 국면 오버레이
    try:
        from daily_runner import get_market_regime
        regime = get_market_regime()
    except Exception as e:
        log(f"regime 조회 실패(boost 가정): {e}")
        regime = {'regime': 'boost', 'equity_weight': 1.0, 'reason': 'regime fetch 실패'}
    ew = regime.get('equity_weight', 1.0)
    if ew is None:
        ew = 1.0

    # 3) 리밸 판단 (월간: 달 바뀌면 / 최초 / 방어→복귀)
    cur_month = today_str[:7]
    rebalanced = False
    ranked = compute_gaps(today_str, cache)

    if ew <= 0.0:
        # 전량 방어 → 현금
        if state['holdings']:
            log("방어 국면 → sleeve 청산(현금)")
        state['holdings'] = []
        state['last_prices'] = {}
    else:
        need_rebal = (state['last_rebal_month'] != cur_month) or (not state['holdings'])
        if need_rebal:
            new_holdings = [r['ticker'] for r in ranked[:TOP_K]]
            if new_holdings:
                entry_px = fetch_prices(new_holdings, today_str)
                state['holdings'] = [t for t in new_holdings if t in entry_px]
                state['last_prices'] = {t: entry_px[t] for t in state['holdings']}
                state['last_rebal_month'] = cur_month
                rebalanced = True
                log(f"리밸런스: {len(state['holdings'])}종목 신규 보유")

    if state['inception'] is None and state['holdings']:
        state['inception'] = today_str
    state['last_run'] = today_str
    state['history'].append({'date': today_str, 'nav': round(state['nav'], 4),
                             'ret': round(ret_pct, 2) if ret_pct is not None else None,
                             'n': len(state['holdings']), 'ew': ew})
    state['history'] = state['history'][-260:]

    # 4) 메시지
    msg = build_message(today_str, state, ranked, regime, rebalanced, ret_pct)

    if not test_mode:
        try:
            from daily_runner import load_config, send_telegram_long
            config = load_config()
            # 채널 발송 (TEST_MODE=1이면 차단 — KR/US 공통 안전장치 정합)
            if os.environ.get('TEST_MODE') == '1':
                log("TEST_MODE=1 → 채널 발송 차단 (메시지만 생성)")
            else:
                # 기본 = 개인봇(페이퍼 검증). --public 명시해야 채널 발송.
                chat_id = config.get('telegram_private_id') if private else None
                send_telegram_long(msg, config, chat_id=chat_id)
                log(f"발송 완료 ({'개인봇(페이퍼)' if private else '채널'})")
        except Exception as e:
            log(f"메시지 발송 실패: {e}")

    save_state(state)
    return {'message': msg, 'state': state, 'ranked': ranked, 'regime': regime}


def build_message(today_str, state, ranked, regime, rebalanced, ret_pct):
    lines = []
    lines.append("📊 <b>기대성장 Sleeve (Forward Growth)</b>")
    lines.append(f"기준일 {today_str} · 시장 기대 EPS성장 상위 {TOP_K}종목 분산")
    lines.append("")

    # 성과
    nav_pct = (state['nav'] - 1) * 100
    incept = state.get('inception')
    perf = f"📈 누적 {nav_pct:+.1f}%"
    if incept:
        perf += f" (운용 {incept}~)"
    if ret_pct is not None:
        perf += f" · 최근 {ret_pct:+.1f}%"
    lines.append(perf)
    lines.append(f"    MDD {state['mdd']*100:.1f}%")
    lines.append("")

    # 국면
    rg = regime.get('regime', 'boost')
    ew = regime.get('equity_weight', 1.0)
    if rg == 'defense' or (ew is not None and ew <= 0.0):
        lines.append("🛡️ <b>방어 국면 — sleeve 현금</b>")
        lines.append(f"사유: {regime.get('reason', '')}")
        lines.append("고성장 기대주는 약세장 변동이 커, 현금/IEF 권장.")
        lines.append("")
    elif ew is not None and ew < 1.0:
        lines.append(f"🟡 <b>조기경보 — 노출 {int(ew*100)}% 권고</b> ({regime.get('reason','')})")
        lines.append("")

    # 보유 종목
    if state['holdings']:
        gmap = {r['ticker']: r for r in ranked}
        lines.append("━━━━━━━━━━━━━━━")
        action = "🔄 월간 리밸런스 (신규 구성)" if rebalanced else "🟢 보유 중"
        lines.append(action)
        lines.append("━━━━━━━━━━━━━━━")
        for i, tk in enumerate(state['holdings'], 1):
            r = gmap.get(tk)
            if r:
                growth = (r['gap'] - 1) * 100
                lines.append(f"{i}. {tk} · 기대성장 +{growth:.0f}% (gap {r['gap']:.1f}x)")
            else:
                lines.append(f"{i}. {tk}")
    else:
        lines.append("보유 없음 (방어 국면 또는 후보 부족)")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append("📌 <b>운영 규칙</b>")
    lines.append("기대성장 = 향후12개월 예상EPS ÷ 최근12개월 실적EPS")
    lines.append(f"상위 {TOP_K}종목 동일가중 · 월 1회 리밸런스")
    lines.append("약세장(S&P 200일선 이탈/VIX) 시 현금")
    lines.append("※ EPS-모멘텀(2종목) 시스템과 별개 sleeve")
    return "\n".join(lines)


def main():
    test_mode = '--test' in sys.argv
    date_str = None
    if '--date' in sys.argv:
        date_str = sys.argv[sys.argv.index('--date') + 1]
    # 기본은 안전하게 test (실발송은 --send). --send는 개인봇(페이퍼), --public 명시해야 채널.
    if '--send' in sys.argv:
        test_mode = False
    private = '--public' not in sys.argv
    res = run_sleeve(today_str=date_str, test_mode=test_mode, private=private)
    if res:
        print("\n" + "=" * 50)
        print(res['message'])


if __name__ == '__main__':
    main()
