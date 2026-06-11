"""
EPS Momentum Daily Runner v19 - Safety & Trend Fusion (v44: Dynamic Universe + Commodity Exclusion)

기능:
1. NTM EPS 전 종목 수집 + MA60 계산 & DB 적재
2. 텔레그램 메시지 2종 + 로그 생성 & 발송
   - [1/2] 매수 후보 + 시장지수 + Death List + 보유 확인
   - [2/2] AI 점검 + 최종 추천 포트폴리오 (통합)
   - 시스템 로그 (개인봇)
3. Git 자동 commit/push

실행: python daily_runner.py
"""

import os
import sys
import io
import json
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
try:
    import pytz
    HAS_PYTZ = True
except ImportError:
    HAS_PYTZ = False
import math
import socket
import warnings
warnings.filterwarnings('ignore')

# yfinance 등 외부 HTTP 호출 전역 timeout (초) — GA hang 방지
socket.setdefaulttimeout(60)

# Windows에서 UTF-8 인코딩 강제 적용 (이모지 지원)
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 프로젝트 루트
PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / 'eps_momentum_data.db'
CONFIG_PATH = PROJECT_ROOT / 'config.json'

# 원자재/광업 제외 대상 — 금값·원자재 가격에 연동되는 업종
# EPS 모멘텀이 구조적 성장이 아닌 commodity 가격 패스스루이므로 제외
COMMODITY_INDUSTRIES = {
    '금', '귀금속', '산업금속', '구리', '철강', '알루미늄',
    '농업', '석유가스', '석유종합', '석유정제', '목재',
    # 영문 fallback (INDUSTRY_MAP 미매핑 시)
    'Gold', 'Other Precious Metals & Mining',
    'Other Industrial Metals & Mining', 'Copper', 'Steel', 'Aluminum',
    'Agricultural Inputs', 'Oil & Gas E&P', 'Oil & Gas Integrated',
    'Oil & Gas Refining & Marketing', 'Lumber & Wood Production',
}
# 업종 분류는 특수화학이지만 실제 원자재(리튬 광산) — 가격 패스스루
COMMODITY_TICKERS = {'SQM', 'ALB'}

# v85 (2026-06-02): 비(非)성장 소비/미디어 업종 제외 — "압도적 성장기업만" 목적 외.
# WMG(음반)·FIVE(전문소매)처럼 EPS revision은 떠도 구조적 저성장 catalyst형.
# 숫자 필터(성장/PEG/모멘텀) 전부 MU/SNDK 착시로 실패 → 유일하게 robust한 lever=업종.
# 300회 paired BT에서 winning trade 0개 차단 (비용 0). COMMODITY_INDUSTRIES와 동일 메커니즘.
OFF_STRATEGY_INDUSTRIES = {
    '엔터', '전문소매',
    # 영문 fallback (INDUSTRY_MAP 미매핑 시)
    'Entertainment', 'Specialty Retail',
}

# 기본 설정
DEFAULT_CONFIG = {
    "git_enabled": True,
    "git_remote": "origin",
    "git_branch": "master",
    "telegram_enabled": False,
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "telegram_channel_id": "",
    "telegram_private_id": "",
    "message_version": "v3",
}


def load_config():
    """설정 로드 (config.json → 환경변수 순으로 체크)"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
            for key, value in DEFAULT_CONFIG.items():
                if key not in config:
                    config[key] = value
    else:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2, ensure_ascii=False)
        config = DEFAULT_CONFIG.copy()

    # 환경변수 오버라이드 (GitHub Actions용)
    if os.environ.get('TELEGRAM_BOT_TOKEN'):
        config['telegram_bot_token'] = os.environ['TELEGRAM_BOT_TOKEN']
        config['telegram_enabled'] = True
    if os.environ.get('TELEGRAM_CHAT_ID'):
        config['telegram_channel_id'] = os.environ['TELEGRAM_CHAT_ID']
    if os.environ.get('TELEGRAM_PRIVATE_ID'):
        config['telegram_private_id'] = os.environ['TELEGRAM_PRIVATE_ID']
        config['telegram_chat_id'] = os.environ['TELEGRAM_PRIVATE_ID']

    config['is_github_actions'] = bool(os.environ.get('GITHUB_ACTIONS'))

    # 메시지 버전 (v3 고정)
    config['message_version'] = 'v3'

    # Gemini API 키 (AI 분석용)
    if os.environ.get('GEMINI_API_KEY'):
        config['gemini_api_key'] = os.environ['GEMINI_API_KEY']

    return config


def log(message, level="INFO"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


# ============================================================
# NTM EPS 데이터 수집
# ============================================================

def init_ntm_database():
    """ntm_screening 테이블 생성"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ntm_screening (
            date        TEXT,
            ticker      TEXT,
            rank        INTEGER,
            score       REAL,
            ntm_current REAL,
            ntm_7d      REAL,
            ntm_30d     REAL,
            ntm_60d     REAL,
            ntm_90d     REAL,
            is_turnaround INTEGER DEFAULT 0,
            adj_score   REAL,
            adj_gap     REAL,
            price       REAL,
            ma60        REAL,
            part2_rank  INTEGER,
            PRIMARY KEY (date, ticker)
        )
    ''')

    # 기존 DB 마이그레이션: 새 컬럼 추가
    for col, col_type in [('adj_score', 'REAL'), ('adj_gap', 'REAL'),
                          ('price', 'REAL'), ('ma60', 'REAL'), ('ma120', 'REAL'), ('part2_rank', 'INTEGER'),
                          ('rev_up30', 'INTEGER'), ('rev_down30', 'INTEGER'), ('num_analysts', 'INTEGER'),
                          ('high30', 'REAL')]:
        try:
            cursor.execute(f'ALTER TABLE ntm_screening ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass  # 이미 존재

    # composite_rank: 당일 composite 순위 (가중순위 계산 원본)
    try:
        cursor.execute('ALTER TABLE ntm_screening ADD COLUMN composite_rank INTEGER')
    except sqlite3.OperationalError:
        pass

    # v54: eps_chg_weighted (EPS 품질 보정용)
    try:
        cursor.execute('ALTER TABLE ntm_screening ADD COLUMN eps_chg_weighted REAL')
    except sqlite3.OperationalError:
        pass

    # v33: 재무 품질 + rev_growth 컬럼
    for col, col_type in [('rev_growth', 'REAL'),
                          ('market_cap', 'REAL'), ('free_cashflow', 'REAL'),
                          ('roe', 'REAL'), ('debt_to_equity', 'REAL'),
                          ('operating_margin', 'REAL'), ('gross_margin', 'REAL'),
                          ('current_ratio', 'REAL'), ('total_debt', 'REAL'),
                          ('total_cash', 'REAL'), ('ev', 'REAL'),
                          ('ebitda', 'REAL'), ('beta', 'REAL')]:
        try:
            cursor.execute(f'ALTER TABLE ntm_screening ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass

    # 기존 eps_snapshots 테이블 삭제
    cursor.execute('DROP TABLE IF EXISTS eps_snapshots')

    # Forward Test 트래커: 포트폴리오 이력 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_log (
            date        TEXT,
            ticker      TEXT,
            action      TEXT,
            price       REAL,
            weight      REAL,
            entry_date  TEXT,
            entry_price REAL,
            exit_price  REAL,
            return_pct  REAL,
            PRIMARY KEY (date, ticker)
        )
    ''')

    # AI 분석 저장 테이블 (대시보드용)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ai_analysis (
            date           TEXT NOT NULL,
            analysis_type  TEXT NOT NULL,
            ticker         TEXT DEFAULT '__ALL__',
            content        TEXT NOT NULL,
            created_at     TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, analysis_type, ticker)
        )
    ''')

    conn.commit()
    conn.close()
    log("NTM 데이터베이스 초기화 완료")


def fetch_dynamic_tickers(min_mcap=5_000_000_000):
    """NASDAQ API에서 시총 기준 이상 전체 상장 종목 동적 수집 (v44)

    NASDAQ/NYSE/AMEX 전체 조회 → 시총 필터.
    S&P 지수 미편입 종목(IPO, ADR 등)도 자동 포착.

    Returns:
        set of ticker symbols
    """
    import urllib.request

    base = "https://api.nasdaq.com/api/screener/stocks"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json',
    }

    # 우선주/채권/워런트 등 비보통주 필터
    _EXCLUDE_LOWER = [
        'preferred', 'warrant', ' notes due', 'debentures due',
        'corporate units', 'equity unit',
        'non-cumulative', 'perpetual sub',
        'fixed-to-floating', ' zones',
    ]

    tickers = set()
    skipped = 0
    for ex_idx, exchange in enumerate(['NASDAQ', 'NYSE', 'AMEX']):
        if ex_idx > 0:
            __import__('time').sleep(1)  # 거래소 간 rate limit 방지
        offset = 0
        total = None
        while True:
            url = f"{base}?tableType=earnings&limit=500&offset={offset}&exchange={exchange}"
            try:
                req = urllib.request.Request(url, headers=headers)
                resp = urllib.request.urlopen(req, timeout=15)
                data = json.loads(resp.read().decode('utf-8'))
            except Exception as e:
                log(f"  NASDAQ API {exchange} offset={offset} 실패: {e}", "WARN")
                break

            if total is None:
                total = int(data.get('data', {}).get('totalrecords', 0))

            rows = data.get('data', {}).get('table', {}).get('rows', [])
            if not rows:
                break

            for r in rows:
                sym = r.get('symbol', '').strip()
                mc_str = r.get('marketCap', '0').replace(',', '')
                try:
                    mc = int(mc_str)
                except ValueError:
                    mc = 0
                if not sym or mc < min_mcap:
                    continue
                # 슬래시 포함 티커 변환 (BRK/B → BRK-B)
                if '/' in sym:
                    sym = sym.replace('/', '-')
                # 비보통주 필터 (우선주, 채권, 워런트 등)
                name = r.get('name', '')
                name_lower = name.lower()
                # Depositary Shares 중 ADR이 아닌 것 = 우선주 예탁증서
                if 'depositary shares' in name_lower and 'american depositary' not in name_lower:
                    skipped += 1
                    continue
                if any(kw in name_lower for kw in _EXCLUDE_LOWER):
                    skipped += 1
                    continue
                tickers.add(sym)

            offset += 500
            if offset >= total:
                break

    if skipped:
        log(f"  비보통주 {skipped}개 제외 (우선주/채권/워런트)")
    return tickers


def _real_today_str():
    """현재 NY 시간 기준 영업일 추정 (yfinance와 일관)"""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime('%Y-%m-%d')
    except Exception:
        return datetime.now().strftime('%Y-%m-%d')


def is_historical_mode():
    """v83.1: MARKET_DATE 환경변수가 실제 오늘과 다른 historical 재실행 모드 감지

    Returns: True if MARKET_DATE is set and != real today
    """
    md = os.environ.get('MARKET_DATE', '').strip()
    if not md:
        return False
    return md != _real_today_str()


def load_historical_results_df(target_date):
    """v83.1: DB의 target_date row를 results_df 형식으로 재구성 (yfinance fetch 없이)

    historical 재실행 모드 (test workflow + MARKET_DATE 과거 날짜)에서
    yfinance lookback mismatch를 회피하기 위해 DB 데이터 그대로 사용.

    Returns: pd.DataFrame (run_ntm_collection 반환 결과와 동일 컬럼)
    """
    import sqlite3
    import pandas as pd
    import json

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        'SELECT * FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
        conn, params=(target_date,)
    )
    conn.close()

    if df.empty:
        return df

    # 컬럼 매핑: DB의 ntm_current ↔ results_df의 ntm_cur
    df['ntm_cur'] = df['ntm_current']

    # seg1~4: NTM 변화율 segment (calculate_ntm_score와 동일 로직)
    def _calc_seg(a, b):
        if b is not None and abs(b) > 0.01:
            return max(-100.0, min(100.0, (a - b) / abs(b) * 100))
        return 0.0
    df['seg1'] = df.apply(lambda r: _calc_seg(r['ntm_current'] or 0, r['ntm_7d'] or 0), axis=1)
    df['seg2'] = df.apply(lambda r: _calc_seg(r['ntm_7d'] or 0, r['ntm_30d'] or 0), axis=1)
    df['seg3'] = df.apply(lambda r: _calc_seg(r['ntm_30d'] or 0, r['ntm_60d'] or 0), axis=1)
    df['seg4'] = df.apply(lambda r: _calc_seg(r['ntm_60d'] or 0, r['ntm_90d'] or 0), axis=1)

    # fwd_pe: price / ntm_current (forward 12-month P/E)
    df['fwd_pe'] = df.apply(
        lambda r: (r['price'] / r['ntm_current']) if (r.get('ntm_current') and r['ntm_current'] > 0 and r.get('price')) else None,
        axis=1
    )

    # eps_change_90d: (ntm_current - ntm_90d) / abs(ntm_90d) * 100
    df['eps_change_90d'] = df.apply(
        lambda r: ((r['ntm_current'] - r['ntm_90d']) / abs(r['ntm_90d']) * 100)
                  if (r.get('ntm_90d') and abs(r['ntm_90d']) > 0.01) else None,
        axis=1
    )

    # trend_lights + trend_desc: get_trend_lights(seg1, seg2, seg3, seg4) — 표시용
    try:
        from eps_momentum_system import get_trend_lights
        def _lights(r):
            return get_trend_lights(r['seg1'], r['seg2'], r['seg3'], r['seg4'])
        lights_results = df.apply(_lights, axis=1)
        df['trend_lights'] = lights_results.apply(lambda x: x[0] if x else '')
        df['trend_desc'] = lights_results.apply(lambda x: x[1] if x else '')
    except Exception:
        df['trend_lights'] = ''
        df['trend_desc'] = ''

    # fwd_pe_chg, price_chg, price_chg_weighted: 표시/필터에 직접 사용 안 됨 또는 미세 영향
    # adj_gap에 이미 fwd_pe_chg 반영됨. 누락 시 None으로 채움 (get_part2_candidates filter 통과)
    if 'fwd_pe_chg' not in df.columns:
        df['fwd_pe_chg'] = None
    if 'price_chg' not in df.columns:
        df['price_chg'] = None
    if 'price_chg_weighted' not in df.columns:
        df['price_chg_weighted'] = None

    # direction: dir_factor 계산용. 단 adj_gap에 이미 반영됨 → 0 default
    if 'direction' not in df.columns:
        df['direction'] = 0.0

    # short_name + industry: ticker_info_cache.json
    cache_path = PROJECT_ROOT / 'ticker_info_cache.json'
    cache = {}
    if cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
        except Exception:
            cache = {}
    df['short_name'] = df['ticker'].map(lambda t: (cache.get(t) or {}).get('shortName', t))
    df['industry'] = df['ticker'].map(lambda t: (cache.get(t) or {}).get('industry', '기타'))
    df['name'] = df['short_name']

    # eps_chg_weighted 보장 (이미 DB 컬럼이지만 안전망)
    if 'eps_chg_weighted' not in df.columns:
        df['eps_chg_weighted'] = None

    return df


def run_ntm_collection(config):
    """NTM EPS 전 종목 수집 & DB 적재

    최적화:
    - 가격 데이터: yf.download() 일괄 다운로드 (내장 스레딩)
    - 종목 정보: JSON 캐시 (shortName, industry)
    - EPS 데이터: 순차 처리 (yfinance 스레딩 비호환)

    v83.1 (2026-05-24): HISTORICAL MODE — MARKET_DATE가 실제 오늘과 다르면
    yfinance fetch SKIP + DB의 historical row로 results_df 재구성.
    이유: yfinance eps_trend의 '90daysAgo' 컬럼이 호출 시점 기준이라
    사용자 지정 today (과거 날짜)와 window misaligned → adj_gap drift.
    해결: 정확한 historical 재현은 DB에 저장된 값으로만 가능.

    Returns:
        tuple (results_df, turnaround_df, stats_dict, today_str, hist_all)
    """
    import yfinance as yf
    import pandas as pd

    # v83.1: HISTORICAL MODE — fetch SKIP + DB로 재구성
    today_str_env = os.environ.get('MARKET_DATE', '').strip()
    if is_historical_mode():
        today_str = today_str_env
        log("=" * 60)
        log(f"⚠️  HISTORICAL MODE 활성 — MARKET_DATE={today_str} (real today={_real_today_str()})")
        log("    yfinance fetch SKIP, DB의 historical row로 results_df 재구성")
        log("    모든 DB write 차단 (production DB drift 방지)")
        log("=" * 60)
        results_df = load_historical_results_df(today_str)
        if results_df.empty:
            log(f"❌ DB에 {today_str} 데이터 없음 — historical 재실행 불가", "WARN")
            return pd.DataFrame(), pd.DataFrame(), {}, today_str, None
        log(f"✅ DB 로드 완료: {len(results_df)}종목")
        # hist_all: 시장 지수만 yfinance fetch (DB에 없으므로) — 단 read-only
        try:
            _INDEX_SYMBOLS = ['^GSPC', '^IXIC', '^DJI', '^RUT']
            hist_all = yf.download(_INDEX_SYMBOLS, start=today_str,
                                    end=(datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=2)).strftime('%Y-%m-%d'),
                                    progress=False, auto_adjust=False, threads=False)
            log(f"  시장 지수 fetch ({len(_INDEX_SYMBOLS)}개) — read-only, DB write 없음")
        except Exception as e:
            log(f"  시장 지수 fetch 실패: {e}", "WARN")
            hist_all = None
        return results_df, pd.DataFrame(), {}, today_str, hist_all

    from eps_momentum_system import (
        INDICES, INDUSTRY_MAP,
        calculate_ntm_eps, calculate_ntm_score, calculate_eps_change_90d,
        get_trend_lights,
    )

    init_ntm_database()

    today = datetime.now()
    today_str = os.environ.get('MARKET_DATE') or ''
    if not today_str:
        try:
            spy_hist = yf.Ticker("SPY").history(period="5d")
            today_str = spy_hist.index[-1].strftime('%Y-%m-%d')
        except Exception:
            today_str = today.strftime('%Y-%m-%d')
    log(f"마켓 날짜: {today_str}")

    # 유니버스: 하드코딩 지수 + NASDAQ API 동적 수집 ($5B+)
    base_tickers = set(t for tlist in INDICES.values() for t in tlist)
    base_original = set(base_tickers)  # MA120 사전 필터용 원본 보존
    log(f"기본 유니버스 (S&P500+400+NQ100): {len(base_tickers)}개")

    new_dynamic = set()  # 동적 신규 종목 (MA120 사전 필터 대상)
    try:
        dynamic = fetch_dynamic_tickers(min_mcap=5_000_000_000)
        new_dynamic = dynamic - base_original
        base_tickers |= dynamic
        log(f"동적 확장 ($5B+): +{len(new_dynamic)}개 → 총 {len(base_tickers)}개")
    except Exception as e:
        log(f"동적 수집 실패 (기본 유니버스로 진행): {e}", "WARN")

    # 시장 지수도 함께 다운로드 (별도 호출 시 rate limit 위험)
    _INDEX_SYMBOLS = ['^GSPC', '^IXIC', '^DJI', '^RUT']
    all_tickers = sorted(base_tickers) + _INDEX_SYMBOLS
    log(f"유니버스: {len(base_tickers)}개 종목 + 지수 {len(_INDEX_SYMBOLS)}개")

    # Step 1: 종목 정보 캐시 로드
    cache_path = PROJECT_ROOT / 'ticker_info_cache.json'
    ticker_cache = {}
    if cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                ticker_cache = json.load(f)
            log(f"종목 정보 캐시 로드: {len(ticker_cache)}개")
        except Exception:
            ticker_cache = {}

    # Step 2: 가격 데이터 일괄 다운로드 (retry 1회)
    log("가격 데이터 일괄 다운로드 중...")
    hist_all = None
    for _dl_attempt in range(2):
        try:
            hist_all = yf.download(all_tickers, period='1y', threads=True, progress=False)
            log("가격 다운로드 완료")
            break
        except Exception as e:
            if _dl_attempt == 0:
                log(f"일괄 다운로드 실패 (10초 후 재시도): {e}", "WARN")
                __import__('time').sleep(10)
            else:
                log(f"일괄 다운로드 재시도 실패: {e}, 개별 다운로드로 전환", "WARN")

    # Step 2.5: 동적 신규 종목 MA120 사전 필터
    # price < MA120인 동적 종목은 Top 30 진입 불가 → EPS 수집 생략
    if hist_all is not None and new_dynamic:
        ma120_skip = set()
        for t in new_dynamic:
            try:
                h = hist_all['Close'][t].dropna()
                if len(h) >= 120:
                    price = float(h.iloc[-1])
                    ma120 = float(h.tail(120).mean())
                    if price < ma120:
                        ma120_skip.add(t)
            except Exception:
                pass
        if ma120_skip:
            all_tickers = [t for t in all_tickers if t not in ma120_skip]
            log(f"MA120 사전 필터: 동적 종목 {len(ma120_skip)}개 제외 → {len(all_tickers)}개 수집")

    # Step 3: EPS 데이터 병렬 수집 (지수 심볼 제외)
    eps_tickers = [t for t in all_tickers if not t.startswith('^')]
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 전일 Top 30 로드 — 수집 실패 시 우선 재시도 + carry-forward 대상
    _prev_top30 = set()
    _prev_date = None
    try:
        _conn_tmp = sqlite3.connect(DB_PATH)
        _prev_date = _conn_tmp.execute(
            "SELECT MAX(date) FROM ntm_screening WHERE date < ? AND part2_rank IS NOT NULL",
            (today_str,)
        ).fetchone()[0]
        if _prev_date:
            _rows = _conn_tmp.execute(
                "SELECT ticker FROM ntm_screening WHERE part2_rank <= 30 AND date = ?",
                (_prev_date,)
            ).fetchall()
            _prev_top30 = {r[0] for r in _rows}
        _conn_tmp.close()
    except Exception:
        pass

    # v113 (2026-06-03): 시스템 보유 종목(메가 carryover)도 우선 재시도 대상에 추가.
    # MU 5/28 사고 — 5/27 cr=67로 Top 30 밖 → _prev_top30 미포함 → fetch 실패 시 우선 재시도 X
    # → MU MISSING → daily_runner.py:3404 자연 매도 발동.
    # _replay_holdings로 실 보유 종목 fetch + _prev_top30 union.
    try:
        _held_tickers = set(_replay_holdings(today_str))
        if _held_tickers:
            _new_holders = _held_tickers - _prev_top30
            if _new_holders:
                log(f"우선 재시도 대상 +{len(_new_holders)}종목 (시스템 보유 메가): {','.join(sorted(_new_holders))}")
            _prev_top30 = _prev_top30 | _held_tickers
    except Exception as _e:
        log(f"_replay_holdings 우선 재시도 union 실패: {_e}", "WARN")

    def _prefetch_eps(ticker):
        """워커: NTM EPS + 애널리스트 수집 (HTTP 1회 — eps_trend만)
        .info는 fetch_revenue_growth()에서 별도 수집하므로 여기서 생략.
        """
        try:
            stock = yf.Ticker(ticker)
            ntm = calculate_ntm_eps(stock, today)
            if ntm is None:
                return ticker, {'ntm': None}

            # _earnings_trend (calculate_ntm_eps 내부에서 이미 로드 → 캐시 히트)
            raw_trend = None
            try:
                raw_trend = stock._analysis._earnings_trend
            except Exception:
                pass

            return ticker, {'ntm': ntm, 'raw_trend': raw_trend}
        except Exception as e:
            return ticker, {'error': str(e)}

    log(f"NTM EPS 병렬 수집 중 (3스레드, {len(eps_tickers)}종목)...")
    _t_eps = __import__('time').time()
    _prefetched = {}
    BATCH_SIZE = 30
    for batch_start in range(0, len(eps_tickers), BATCH_SIZE):
        batch = eps_tickers[batch_start:batch_start + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(_prefetch_eps, t): t for t in batch}
            for future in as_completed(futures):
                result = future.result()
                _prefetched[result[0]] = result[1]
        done_count = batch_start + len(batch)
        if done_count % 200 < BATCH_SIZE:
            log(f"  수집: {done_count}/{len(eps_tickers)}")
        if batch_start + BATCH_SIZE < len(eps_tickers):
            __import__('time').sleep(1.5)
    # 에러 종목 재시도 (10초 대기 후, rate limit 해소)
    error_tickers = [t for t, d in _prefetched.items() if 'error' in d]
    if error_tickers:
        log(f"EPS 재시도: {len(error_tickers)}종목 (10초 대기 후)")
        __import__('time').sleep(10)
        for batch_start in range(0, len(error_tickers), BATCH_SIZE):
            batch = error_tickers[batch_start:batch_start + BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(_prefetch_eps, t): t for t in batch}
                for future in as_completed(futures):
                    t, data = future.result()
                    if 'error' not in data:
                        _prefetched[t] = data
            if batch_start + BATCH_SIZE < len(error_tickers):
                __import__('time').sleep(1.5)
        retry_ok = sum(1 for t in error_tickers if 'error' not in _prefetched[t])
        log(f"  재시도 복구: {retry_ok}/{len(error_tickers)}")

    # 전일 Top 20 우선 재시도 — 에러 또는 데이터없음 모두 대상, 최대 3회
    if _prev_top30:
        _top20_failed = [t for t in _prev_top30
                         if t in _prefetched and ('error' in _prefetched[t] or _prefetched[t].get('ntm') is None)]
        if _top20_failed:
            for _retry_round in range(1, 4):
                __import__('time').sleep(5)
                _still_bad = []
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {executor.submit(_prefetch_eps, t): t for t in _top20_failed}
                    for future in as_completed(futures):
                        t, data = future.result()
                        if 'error' not in data and data.get('ntm') is not None:
                            _prefetched[t] = data
                        else:
                            _still_bad.append(t)
                if not _still_bad:
                    log(f"  Top20 우선 재시도 {_retry_round}회차: {len(_top20_failed)}종목 전체 복구")
                    break
                _top20_failed = _still_bad
            else:
                if _still_bad:
                    log(f"  Top20 우선 재시도 3회 실패: {','.join(sorted(_still_bad))}", "WARN")

    log(f"EPS 수집 완료: {len(_prefetched)}종목, {__import__('time').time() - _t_eps:.0f}초")

    # Step 3b: DB 적재 + 스코어링 (순차, SQLite 안전)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    results = []
    turnaround = []
    no_data = []
    errors = []
    cache_updated = False

    for i, ticker in enumerate(eps_tickers):
        if (i + 1) % 200 == 0:
            log(f"  처리: {i+1}/{len(eps_tickers)} (메인: {len(results)}, 턴어라운드: {len(turnaround)})")
            conn.commit()

        data = _prefetched.get(ticker, {})

        if 'error' in data:
            errors.append((ticker, data['error']))
            continue

        ntm = data.get('ntm')
        if ntm is None:
            no_data.append(ticker)
            continue

        try:
            # Score 계산
            score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction = calculate_ntm_score(ntm)
            eps_change_90d = calculate_eps_change_90d(ntm)
            trend_lights, trend_desc = get_trend_lights(seg1, seg2, seg3, seg4)

            # EPS Revision & 애널리스트 수 추출 — max(0y, +1y)로 두 기간 모두 반영
            rev_up30 = 0
            rev_down30 = 0
            num_analysts = 0
            raw_trend = data.get('raw_trend')
            if raw_trend:
                for item in raw_trend:
                    if item.get('period') in ('0y', '+1y'):
                        eps_rev = item.get('epsRevisions', {})
                        up_data = eps_rev.get('upLast30days', {})
                        down_data = eps_rev.get('downLast30days', {})
                        up_val = up_data.get('raw', 0) if isinstance(up_data, dict) else 0
                        down_val = down_data.get('raw', 0) if isinstance(down_data, dict) else 0
                        ea = item.get('earningsEstimate', {})
                        na_data = ea.get('numberOfAnalysts', {})
                        na_val = na_data.get('raw', 0) if isinstance(na_data, dict) else 0
                        rev_up30 = max(rev_up30, up_val)
                        rev_down30 = max(rev_down30, down_val)
                        num_analysts = max(num_analysts, na_val)

            # DB 적재 (기본 데이터 — price/ma60/adj_gap은 후속 UPDATE로 추가)
            # INSERT ON CONFLICT: 기존 part2_rank 보존
            cursor.execute('''
                INSERT INTO ntm_screening
                (date, ticker, rank, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turnaround)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    rank=excluded.rank, score=excluded.score,
                    ntm_current=excluded.ntm_current, ntm_7d=excluded.ntm_7d,
                    ntm_30d=excluded.ntm_30d, ntm_60d=excluded.ntm_60d,
                    ntm_90d=excluded.ntm_90d, is_turnaround=excluded.is_turnaround
            ''', (today_str, ticker, 0, score,
                  ntm['current'], ntm['7d'], ntm['30d'], ntm['60d'], ntm['90d'],
                  1 if is_turnaround else 0))

            # 종목 정보 (캐시 우선, 미스면 플레이스홀더 — fetch_revenue_growth에서 갱신)
            if ticker in ticker_cache:
                short_name = ticker_cache[ticker]['shortName']
                industry_kr = ticker_cache[ticker]['industry']
            else:
                short_name = ticker
                industry_kr = '기타'
                ticker_cache[ticker] = {'shortName': short_name, 'industry': industry_kr}
                cache_updated = True

            # 가격 & 다중 주기 괴리율 (일괄 다운로드 데이터 사용)
            fwd_pe_now = None
            fwd_pe_chg = None  # 가중평균 괴리율
            price_chg = None
            price_chg_weighted = None
            eps_chg_weighted = None
            current_price = None
            ma60_val = None
            ma120_val = None

            try:
                if hist_all is not None:
                    hist = hist_all['Close'][ticker].dropna()
                else:
                    hist = pd.Series(dtype=float)

                if len(hist) >= 60:
                    p_now = hist.iloc[-1]
                    current_price = float(p_now)
                    ma60_val = float(hist.rolling(window=60).mean().iloc[-1])
                    if len(hist) >= 120:
                        ma120_val = float(hist.rolling(window=120).mean().iloc[-1])
                    # v119 (2026-06-11): high30 = DB 누적 가격 기반 결정적 계산.
                    # 기존 hist.tail(30)은 fetch 시점마다 30거래일 윈도우가 밀려 경계종목(VRT 등)이
                    # 실행마다 dd_30_25 제외/통과가 흔들림(비결정적). DB 과거가격은 고정 → 결정적 + BT 정합.
                    _dbpx = [r[0] for r in cursor.execute(
                        "SELECT price FROM ntm_screening WHERE ticker=? AND date<? AND price IS NOT NULL ORDER BY date DESC LIMIT 29",
                        (ticker, today_str)).fetchall()]
                    if len(_dbpx) >= 29:
                        high30_val = max(_dbpx + [current_price])
                    else:
                        high30_val = float(hist.tail(30).max()) if len(hist) >= 30 else None  # cold start fallback
                    hist_dt = hist.index.tz_localize(None) if hist.index.tz else hist.index

                    # 각 시점의 주가 찾기
                    prices = {}
                    for days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                        target = today - timedelta(days=days)
                        idx = (hist_dt - target).map(lambda x: abs(x.days)).argmin()
                        prices[key] = hist.iloc[idx]

                    # 90일 주가변화율 (내부용)
                    price_chg = (p_now - prices['90d']) / prices['90d'] * 100

                    # 가중평균 주가변화율 (⚠️ 경고 판별용)
                    price_w = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
                    pw_sum = sum(
                        w * (p_now - prices[k]) / prices[k] * 100
                        for k, w in price_w.items() if prices[k] > 0
                    )
                    pw_total = sum(w for k, w in price_w.items() if prices[k] > 0)
                    price_chg_weighted = pw_sum / pw_total if pw_total > 0 else None

                    # 가중평균 EPS변화율 (⚠️ 경고 판별용)
                    nc_val = ntm['current']
                    eps_w = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
                    ew_sum = sum(
                        w * (nc_val - ntm[k]) / abs(ntm[k]) * 100
                        for k, w in eps_w.items() if ntm[k] != 0
                    )
                    ew_total = sum(w for k, w in eps_w.items() if ntm[k] != 0)
                    eps_chg_weighted = ew_sum / ew_total if ew_total > 0 else None

                    # 현재 Fwd PE
                    nc = ntm['current']
                    if nc > 0:
                        fwd_pe_now = p_now / nc

                    # 각 주기별 괴리율 → 가중평균 (v80.10 long-tail: 90d 누적 PE 압축 강조)
                    weights = {'7d': 0.30, '30d': 0.10, '60d': 0.10, '90d': 0.50}
                    weighted_sum = 0.0
                    total_weight = 0.0

                    for key, w in weights.items():
                        ntm_val = ntm[key]
                        if nc > 0 and ntm_val > 0 and prices[key] > 0:
                            fwd_pe_then = prices[key] / ntm_val
                            pe_chg_period = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                            weighted_sum += w * pe_chg_period
                            total_weight += w

                    if total_weight > 0:
                        fwd_pe_chg = weighted_sum / total_weight
            except Exception as e:
                log(f"  {ticker} 가격/PE 계산 실패: {e}", "WARN")

            # adj_gap: 괴리율에 방향 보정 + EPS 건강도 보정
            #   dir_factor: EPS 가속도 (가속 → 보너스, 감속 → 페널티) [-0.3, 0.3]
            #   eps_quality: EPS 4구간 일관성 (min_seg 기반) [0.7, 1.3]
            #     min_seg ≥ 2% → 1.3 (전 구간 고른 상향)
            #     연속함수: eps_q = 1.0 + 0.3 × clamp(min_seg/2, -1, 1)
            #     min_seg ≤ -2% → 0.7, min_seg = 0% → 1.0, min_seg ≥ 2% → 1.3
            # v80.4 (2026-04-30): direction은 calculate_ntm_score에서 β1
            # (cap 발동 시 +9 = +0.3 보너스)로 계산됨. opt4: 정상 영역에서
            # C4 (고평가 fwd>0 + 둔화 dir<0) 케이스 sign flip → 매도 강조.
            # cap 발동 시는 β1 boost 그대로 적용 (이미 +0.3).
            adj_gap = None
            if fwd_pe_chg is not None and direction is not None:
                SEG_CAP = 100
                _segs = [seg1 or 0, seg2 or 0, seg3 or 0, seg4 or 0]
                cap_hit = any(abs(s) >= SEG_CAP for s in _segs)
                df_raw = max(-0.3, min(0.3, direction / 30))
                # opt4: 정상 + C4 (양수 fwd × 음수 dir) 시 sign flip
                if not cap_hit and fwd_pe_chg > 0 and direction < 0:
                    dir_factor = -df_raw  # +0.3 (매도 강조)
                else:
                    dir_factor = df_raw  # baseline / β1(cap 시 +0.3) / C1·C2·C3
                _valid = [s for s in _segs if abs(s) < SEG_CAP]
                min_seg = min(_valid) if _valid else 0
                eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
                adj_gap = fwd_pe_chg * (1 + dir_factor) * eps_q

            row = {
                'ticker': ticker,
                'short_name': short_name,
                'industry': industry_kr,
                'score': score,
                'adj_score': adj_score,
                'direction': direction,
                'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
                'ntm_cur': ntm['current'],
                'ntm_7d': ntm['7d'],
                'ntm_30d': ntm['30d'],
                'ntm_60d': ntm['60d'],
                'ntm_90d': ntm['90d'],
                'eps_change_90d': eps_change_90d,
                'trend_lights': trend_lights,
                'trend_desc': trend_desc,
                'price_chg': price_chg,
                'price_chg_weighted': price_chg_weighted,
                'eps_chg_weighted': eps_chg_weighted,
                'fwd_pe': fwd_pe_now,
                'fwd_pe_chg': fwd_pe_chg,
                'adj_gap': adj_gap,
                'is_turnaround': is_turnaround,
                'rev_up30': rev_up30,
                'rev_down30': rev_down30,
                'num_analysts': num_analysts,
                'price': current_price,
                'ma60': ma60_val,
                'ma120': ma120_val,
                'high30': high30_val,
            }

            # DB에 파생 데이터 업데이트
            cursor.execute('''
                UPDATE ntm_screening
                SET adj_score=?, adj_gap=?, price=?, ma60=?, ma120=?,
                    rev_up30=?, rev_down30=?, num_analysts=?, eps_chg_weighted=?, high30=?
                WHERE date=? AND ticker=?
            ''', (adj_score, adj_gap, current_price, ma60_val, ma120_val,
                  rev_up30, rev_down30, num_analysts, eps_chg_weighted, high30_val,
                  today_str, ticker))

            if is_turnaround:
                turnaround.append(row)
            else:
                results.append(row)

        except Exception as e:
            errors.append((ticker, str(e)))
            continue

    conn.commit()

    # ── carry-forward: 전일 Top30 수집 실패 종목 → 전일 EPS + 오늘 가격으로 row 삽입 ──
    _cf_inserted = []
    if _prev_top30 and _prev_date and hist_all is not None:
        _processed = {r['ticker'] for r in results} | {r['ticker'] for r in turnaround}
        _cf_candidates = [t for t in _prev_top30
                          if t in set(eps_tickers) and t not in _processed]
        if _cf_candidates:
            conn_cf = sqlite3.connect(DB_PATH)
            cur_cf = conn_cf.cursor()
            for ticker in _cf_candidates:
                try:
                    # 1) 전일 DB row 로드
                    prev = cur_cf.execute(
                        'SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, '
                        'rev_up30, rev_down30, num_analysts, score, adj_score, is_turnaround '
                        'FROM ntm_screening WHERE date=? AND ticker=?',
                        (_prev_date, ticker)
                    ).fetchone()
                    if not prev or prev[0] is None:
                        continue

                    ntm = {'current': prev[0], '7d': prev[1], '30d': prev[2],
                           '60d': prev[3], '90d': prev[4]}

                    # 2) 오늘 가격 (hist_all 배치 다운로드에서)
                    try:
                        hist = hist_all['Close'][ticker].dropna()
                    except (KeyError, TypeError):
                        continue  # 가격 없으면 skip
                    if len(hist) < 60:
                        continue

                    p_now = float(hist.iloc[-1])
                    ma60_val = float(hist.rolling(window=60).mean().iloc[-1])
                    ma120_val = float(hist.rolling(window=120).mean().iloc[-1]) if len(hist) >= 120 else None
                    # v119 (2026-06-11): high30 = DB 누적 가격 기반 결정적 계산 (carry-forward 경로)
                    _dbpx = [r[0] for r in cur_cf.execute(
                        "SELECT price FROM ntm_screening WHERE ticker=? AND date<? AND price IS NOT NULL ORDER BY date DESC LIMIT 29",
                        (ticker, today_str)).fetchall()]
                    if len(_dbpx) >= 29:
                        high30_val = max(_dbpx + [p_now])
                    else:
                        high30_val = float(hist.tail(30).max()) if len(hist) >= 30 else None  # cold start fallback

                    # 3) 스코어 재계산 (전일 EPS 기반)
                    score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction = calculate_ntm_score(ntm)
                    eps_change_90d = calculate_eps_change_90d(ntm)
                    trend_lights, trend_desc = get_trend_lights(seg1, seg2, seg3, seg4)

                    # 4) adj_gap 재계산 (전일 EPS + 오늘 가격)
                    fwd_pe_now = None
                    fwd_pe_chg = None
                    adj_gap = None
                    nc = ntm['current']
                    if nc > 0:
                        fwd_pe_now = p_now / nc
                    hist_dt = hist.index.tz_localize(None) if hist.index.tz else hist.index
                    prices = {}
                    for days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                        target = today - timedelta(days=days)
                        idx = (hist_dt - target).map(lambda x: abs(x.days)).argmin()
                        prices[key] = hist.iloc[idx]

                    weights_pe = {'7d': 0.30, '30d': 0.10, '60d': 0.10, '90d': 0.50}  # v80.10 long-tail
                    weighted_sum = 0.0
                    total_weight = 0.0
                    for key, w in weights_pe.items():
                        ntm_val = ntm[key]
                        if nc > 0 and ntm_val > 0 and prices[key] > 0:
                            fwd_pe_then = prices[key] / ntm_val
                            pe_chg_period = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                            weighted_sum += w * pe_chg_period
                            total_weight += w
                    if total_weight > 0:
                        fwd_pe_chg = weighted_sum / total_weight

                    if fwd_pe_chg is not None and direction is not None:
                        # v80.4: β1(cap 시 +0.3 보너스, calculate_ntm_score에서 적용)
                        # + opt4(정상 영역 C4 sign flip)
                        SEG_CAP = 100
                        _segs = [seg1 or 0, seg2 or 0, seg3 or 0, seg4 or 0]
                        cap_hit = any(abs(s) >= SEG_CAP for s in _segs)
                        df_raw = max(-0.3, min(0.3, direction / 30))
                        if not cap_hit and fwd_pe_chg > 0 and direction < 0:
                            dir_factor = -df_raw  # opt4: C4 매도 강조
                        else:
                            dir_factor = df_raw
                        _valid = [s for s in _segs if abs(s) < SEG_CAP]
                        min_seg = min(_valid) if _valid else 0
                        eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
                        adj_gap = fwd_pe_chg * (1 + dir_factor) * eps_q

                    if adj_gap is None:
                        continue  # adj_gap 없으면 순위 의미 없음

                    # 5) DB 삽입
                    cur_cf.execute('''
                        INSERT INTO ntm_screening
                        (date, ticker, rank, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turnaround)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(date, ticker) DO UPDATE SET
                            rank=excluded.rank, score=excluded.score,
                            ntm_current=excluded.ntm_current, ntm_7d=excluded.ntm_7d,
                            ntm_30d=excluded.ntm_30d, ntm_60d=excluded.ntm_60d,
                            ntm_90d=excluded.ntm_90d, is_turnaround=excluded.is_turnaround
                    ''', (today_str, ticker, 0, score,
                          ntm['current'], ntm['7d'], ntm['30d'], ntm['60d'], ntm['90d'],
                          1 if is_turnaround else 0))

                    cur_cf.execute('''
                        UPDATE ntm_screening
                        SET adj_score=?, adj_gap=?, price=?, ma60=?, ma120=?,
                            rev_up30=?, rev_down30=?, num_analysts=?, high30=?
                        WHERE date=? AND ticker=?
                    ''', (adj_score, adj_gap, p_now, ma60_val, ma120_val,
                          prev[5], prev[6], prev[7], high30_val,
                          today_str, ticker))

                    # 6) results 리스트에 추가
                    if ticker in ticker_cache:
                        short_name = ticker_cache[ticker]['shortName']
                        industry_kr = ticker_cache[ticker]['industry']
                    else:
                        short_name = ticker
                        industry_kr = '기타'

                    row = {
                        'ticker': ticker, 'short_name': short_name, 'industry': industry_kr,
                        'score': score, 'adj_score': adj_score, 'direction': direction,
                        'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
                        'ntm_cur': ntm['current'], 'ntm_7d': ntm['7d'],
                        'ntm_30d': ntm['30d'], 'ntm_60d': ntm['60d'], 'ntm_90d': ntm['90d'],
                        'eps_change_90d': eps_change_90d,
                        'trend_lights': trend_lights, 'trend_desc': trend_desc,
                        'price_chg': None, 'price_chg_weighted': None, 'eps_chg_weighted': None,
                        'fwd_pe': fwd_pe_now, 'fwd_pe_chg': fwd_pe_chg, 'adj_gap': adj_gap,
                        'is_turnaround': is_turnaround,
                        'rev_up30': prev[5], 'rev_down30': prev[6], 'num_analysts': prev[7],
                        'price': p_now, 'ma60': ma60_val, 'ma120': ma120_val,
                        'high30': high30_val,
                    }
                    results.append(row)
                    _cf_inserted.append(ticker)
                except Exception as e:
                    log(f"  carry-forward {ticker} 실패: {e}", "WARN")
                    continue

            conn_cf.commit()
            conn_cf.close()
            if _cf_inserted:
                log(f"carry-forward 삽입: {','.join(sorted(_cf_inserted))} ({len(_cf_inserted)}종목, 전일 EPS + 오늘 가격)")

    # 종목 정보 캐시 저장
    if cache_updated:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(ticker_cache, f, ensure_ascii=False, indent=2)
        log(f"종목 정보 캐시 저장: {len(ticker_cache)}개")

    # 메인 랭킹: adj_score(방향 보정 점수) 순 정렬 + rank 업데이트
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values('adj_score', ascending=False).reset_index(drop=True)
        results_df['rank'] = results_df.index + 1

        for _, row in results_df.iterrows():
            cursor.execute(
                'UPDATE ntm_screening SET rank = ? WHERE date = ? AND ticker = ?',
                (int(row['rank']), today_str, row['ticker'])
            )

    # 턴어라운드: score 순 정렬
    turnaround_df = pd.DataFrame(turnaround)
    if not turnaround_df.empty:
        turnaround_df = turnaround_df.sort_values('score', ascending=False).reset_index(drop=True)

    conn.commit()
    conn.close()

    # 통계
    stats = {
        'universe': len(eps_tickers),
        'main_count': len(results),
        'turnaround_count': len(turnaround),
        'no_data_count': len(no_data),
        'error_count': len(errors),
        'error_tickers': [t for t, _ in errors],
        'total_collected': len(results) + len(turnaround),
    }

    # score_gt0/gt3/aligned_count 제거 — 시스템 로그에서 미사용

    log(f"수집 완료: 메인 {len(results)}, 턴어라운드 {len(turnaround)}, "
        f"데이터없음 {len(no_data)}, 에러 {len(errors)}")

    return results_df, turnaround_df, stats, today_str, hist_all


def _validate_collection_health(stats, min_collected=900, max_error_rate=0.30):
    """수집 건강성 검증 (v86e++ 2026-06-03) — KR <150 안전망 이식 + US 적응.

    2026-05-28~29 사고: yfinance 대량 실패(에러 676/1272=53%, 수집 600/315 vs 정상 ~1240)인데
    가드가 없어 망가진 데이터로 순위 계산 + 채널 발송. universe 크기·에러율로 검증.
    (rev_growth는 DB캐시 fallback[v76]으로 유지되므로 universe·에러율이 진짜 지표.)

    Returns: (ok: bool, reason: str)
    """
    try:
        universe = stats.get('universe', 0) or 0
        collected = stats.get('total_collected', 0) or 0
        errors = stats.get('error_count', 0) or 0
        err_rate = (errors / universe) if universe else 1.0
        if collected < min_collected:
            return False, f"수집 종목 {collected} < {min_collected} (정상 ~1240, 5/28사고=600)"
        if err_rate > max_error_rate:
            return False, f"에러율 {err_rate*100:.0f}% > {max_error_rate*100:.0f}% (5/28사고=53%)"
        return True, f"수집 {collected}, 에러율 {err_rate*100:.0f}% — 정상"
    except Exception as e:
        return False, f"건강성 검증 오류: {e}"


# ============================================================
# Part 2 공통 필터 & 3일 교집합
# ============================================================

def fetch_revenue_growth(df, today_str):
    """전체 종목 매출 성장률 + 재무 품질 수집 (v33)

    1) 전체 종목 yfinance .info → rev_growth + 12개 재무 지표 DB 저장
    2) composite score용 rev_growth를 dataframe에 매핑
    10스레드 병렬 수집으로 ~3분 → ~30초 단축.
    """
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time as _time

    # 전날 Top 30 종목 로드 — 이 종목만 earnings_history 추가 수집
    _eh_priority = set()
    try:
        conn_tmp = sqlite3.connect(DB_PATH)
        rows = conn_tmp.execute(
            "SELECT ticker FROM ntm_screening WHERE part2_rank IS NOT NULL AND date = (SELECT MAX(date) FROM ntm_screening WHERE date < ? AND part2_rank IS NOT NULL)",
            (today_str,)
        ).fetchall()
        _eh_priority = {r[0] for r in rows}
        conn_tmp.close()
        if _eh_priority:
            log(f"어닝서프 우선 수집 대상: {len(_eh_priority)}종목 (전일 Top30)")
    except Exception:
        pass

    def _fetch_one(ticker):
        """단일 종목 .info 수집 (+ 우선 종목은 earnings_history도)
        v71.2: rev_growth/OM 의심 시 quarterly_income_stmt로 즉시 교정"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            # 우선 종목만 earnings_history 추가 수집 (전체 ~30종목뿐, rate limit 영향 미미)
            if ticker in _eh_priority:
                try:
                    eh = stock.earnings_history
                    if eh is not None and len(eh) > 0:
                        surps = eh['surprisePercent'].dropna().tolist()
                        if surps:
                            info['_earnings_surp'] = surps[-1]
                except Exception:
                    pass
            # v71.2: .info rev_growth/OM이 의심스러우면 income_stmt로 즉시 교정
            rg = info.get('revenueGrowth')
            om = info.get('operatingMargins')
            need_verify = (rg is not None and rg < 0.10) or (om is not None and om < 0.05)
            if need_verify:
                try:
                    qi = stock.quarterly_income_stmt
                    if qi is not None and not qi.empty and 'Total Revenue' in qi.index:
                        rev = qi.loc['Total Revenue'].dropna().sort_index(ascending=False)
                        # rev_growth 교정: YoY 비교
                        if rg is not None and rg < 0.10 and len(rev) >= 5:
                            recent_q, yoy_q = rev.iloc[0], rev.iloc[4]
                            if yoy_q > 0 and recent_q > 0:
                                real_rg = (recent_q - yoy_q) / yoy_q
                                if real_rg >= 0.10:
                                    info['revenueGrowth'] = real_rg
                                    info['_rg_verified'] = True
                        # OM 교정: 최근 분기 기준
                        if 'Operating Income' in qi.index and len(rev) > 0:
                            op_inc = qi.loc['Operating Income'].dropna().sort_index(ascending=False)
                            if len(op_inc) > 0:
                                real_om = float(op_inc.iloc[0]) / float(rev.iloc[0])
                                if om is not None and abs(real_om - om) > 0.01:
                                    info['operatingMargins'] = real_om
                                    info['_om_verified'] = True
                except Exception:
                    pass
            return ticker, info
        except Exception:
            return ticker, None

    tickers = list(df['ticker'].unique())
    log(f"매출+품질 수집 시작: {len(tickers)}종목 (5스레드, 배치 50)")

    # 배치 병렬 수집 (rate limit 방지)
    BATCH_SIZE = 50
    results = {}
    for batch_start in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[batch_start:batch_start + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_one, t): t for t in batch}
            for future in as_completed(futures):
                ticker, info = future.result()
                results[ticker] = info
        done = batch_start + len(batch)
        if done % 200 < BATCH_SIZE:
            log(f"  수집 진행: {done}/{len(tickers)}")
        if batch_start + BATCH_SIZE < len(tickers):
            __import__('time').sleep(0.5)

    # DB 일괄 저장
    rev_map = {}
    earnings_map = {}  # {ticker: datetime.date} — 어닝 날짜 (.info에서 추출)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    saved = 0

    for t in tickers:
        info = results.get(t)
        if not info:
            rev_map[t] = None
            continue

        rg = info.get('revenueGrowth')
        rev_map[t] = rg
        if info.get('_rg_verified'):
            log(f"  .info 교정: {t} rev_growth→{rg:.1%}" + (f" OM→{info.get('operatingMargins'):.1%}" if info.get('_om_verified') else ""))

        # 어닝 날짜 추출 (.info earningsTimestampEnd → calendar 별도 호출 불필요)
        # 장후(16시 ET 이후) 발표 → 시장 영향은 다음 거래일이므로 +1일
        ets = info.get('earningsTimestampEnd') or info.get('earningsTimestampStart') or info.get('earningsTimestamp')
        if ets and isinstance(ets, (int, float)) and ets > 0:
            try:
                from zoneinfo import ZoneInfo
                dt_et = datetime.fromtimestamp(ets, tz=ZoneInfo('America/New_York'))
                earn_date = dt_et.date()
                is_after_hours = dt_et.hour >= 16
                earnings_map[t] = {'date': earn_date, 'after_hours': is_after_hours}
            except (ValueError, OSError):
                pass

        if info.get('marketCap'):
            cursor.execute('''
                UPDATE ntm_screening
                SET rev_growth=?, market_cap=?, free_cashflow=?, roe=?,
                    debt_to_equity=?, operating_margin=?, gross_margin=?,
                    current_ratio=?, total_debt=?, total_cash=?,
                    ev=?, ebitda=?, beta=?
                WHERE date=? AND ticker=?
            ''', (
                rg,
                info.get('marketCap'),
                info.get('freeCashflow'),
                info.get('returnOnEquity'),
                info.get('debtToEquity'),
                info.get('operatingMargins'),
                info.get('grossMargins'),
                info.get('currentRatio'),
                info.get('totalDebt'),
                info.get('totalCash'),
                info.get('enterpriseValue'),
                info.get('ebitda'),
                info.get('beta'),
                today_str, t
            ))
            saved += 1

    conn.commit()
    conn.close()

    success = sum(1 for v in rev_map.values() if v is not None)
    log(f"매출+품질 수집 완료: {saved}/{len(tickers)} (rev_growth {success}개)")

    # margin 데이터도 dataframe에 추가 (구조적 저마진 필터용)
    om_map = {t: results[t].get('operatingMargins') for t in results if results[t]}
    gm_map = {t: results[t].get('grossMargins') for t in results if results[t]}

    # ── yfinance .info 수집 실패 시 DB 캐시 fallback ──
    # 재무 데이터(매출성장률/마진)는 분기 발표라 며칠 전 값이 오늘도 유효.
    # yfinance 불안정(Run 2 사례: 1118중 185개만 수집)에 대한 방어.
    failed_tickers = [t for t in tickers if rev_map.get(t) is None
                      or om_map.get(t) is None or gm_map.get(t) is None]
    if failed_tickers:
        conn2 = sqlite3.connect(DB_PATH)
        cur2 = conn2.cursor()
        filled_rev = filled_om = filled_gm = 0
        for t in failed_tickers:
            row = cur2.execute('''
                SELECT rev_growth, operating_margin, gross_margin FROM ntm_screening
                WHERE ticker=? AND date < ? AND date >= date(?, '-7 day')
                AND (rev_growth IS NOT NULL OR operating_margin IS NOT NULL OR gross_margin IS NOT NULL)
                ORDER BY date DESC LIMIT 1
            ''', (t, today_str, today_str)).fetchone()
            if row:
                rg, om, gm = row
                rev_filled = om_filled = gm_filled = False
                if rev_map.get(t) is None and rg is not None:
                    rev_map[t] = rg
                    filled_rev += 1
                    rev_filled = True
                if om_map.get(t) is None and om is not None:
                    om_map[t] = om
                    filled_om += 1
                    om_filled = True
                if gm_map.get(t) is None and gm is not None:
                    gm_map[t] = gm
                    filled_gm += 1
                    gm_filled = True
                # v77: fallback 값을 오늘 DB row에도 UPDATE
                #      → 다음날 이후 조회에서 NULL 체인 방지 (GMED 4/14 사례)
                if rev_filled or om_filled or gm_filled:
                    cur2.execute('''
                        UPDATE ntm_screening SET
                            rev_growth = COALESCE(rev_growth, ?),
                            operating_margin = COALESCE(operating_margin, ?),
                            gross_margin = COALESCE(gross_margin, ?)
                        WHERE date=? AND ticker=?
                    ''', (rg if rev_filled else None,
                          om if om_filled else None,
                          gm if gm_filled else None,
                          today_str, t))
        conn2.commit()
        conn2.close()
        log(f"DB 캐시 fallback: rev_growth {filled_rev}개, op_margin {filled_om}개, gross_margin {filled_gm}개 (최근 7일 값, DB에도 UPDATE)")

    df['rev_growth'] = df['ticker'].map(rev_map)
    df['operating_margin'] = df['ticker'].map(om_map)
    df['gross_margin'] = df['ticker'].map(gm_map)

    # industry + shortName 보정: 플레이스홀더('기타'/티커) → .info 실제 값으로 갱신
    from eps_momentum_system import INDUSTRY_MAP
    cache_path = PROJECT_ROOT / 'ticker_info_cache.json'
    ticker_cache = {}
    if cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                ticker_cache = json.load(f)
        except Exception:
            pass
    ind_map = {}
    updated_ind = 0
    cache_dirty = False
    for t in tickers:
        info = results.get(t)
        if not info:
            continue
        if info.get('industry'):
            kr_ind = INDUSTRY_MAP.get(info['industry'], info['industry'])
            ind_map[t] = kr_ind
        # 캐시에 플레이스홀더(shortName==티커)면 .info에서 갱신
        cached = ticker_cache.get(t, {})
        real_name = info.get('shortName') or info.get('longName')
        if real_name and cached.get('shortName') == t and real_name != t:
            cached['shortName'] = real_name
            cache_dirty = True
        if info.get('industry') and cached.get('industry') in ('기타', None):
            cached['industry'] = INDUSTRY_MAP.get(info['industry'], info['industry'])
            cache_dirty = True
        if cache_dirty or t not in ticker_cache:
            ticker_cache[t] = cached if cached else {'shortName': real_name or t, 'industry': ind_map.get(t, '기타')}
    if cache_dirty:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(ticker_cache, f, ensure_ascii=False, indent=2)
        log(f"종목 캐시 갱신: 플레이스홀더 보정 완료")
    # '기타'인 종목만 업데이트
    for idx, row in df.iterrows():
        if row.get('industry') == '기타' and row['ticker'] in ind_map:
            df.at[idx, 'industry'] = ind_map[row['ticker']]
            updated_ind += 1
    if updated_ind:
        log(f"Industry 보정: {updated_ind}종목 ('기타' → 실제 업종)")

    return df, earnings_map, results  # results = {ticker: info_dict} for alpha signal reuse


def get_part2_candidates(df, top_n=None, return_counts=False):
    """Part 2 매수 후보 필터링 (공통 함수)

    필터: adj_score > 9, fwd_pe > 0, eps > 0, price ≥ $10, price > MA120,
          rev_growth ≥ 10%, num_analysts ≥ 3, 하향 비율 ≤ 30%,
          구조적 저마진(OM<10%&GM<30%), OP<5%, 원자재 업종 제외
    정렬: adj_gap 오름차순 (가장 저평가된 종목이 1위)

    return_counts=True: (filtered_df, {'eps_screened': N, 'quality_filtered': N}) 반환
    """
    import numpy as np
    import pandas as pd

    # MA120 우선, NULL이면 MA60 fallback (ma120 컬럼 없으면 ma60만 사용)
    if 'ma120' in df.columns:
        ma_col = df['ma120'].where(df['ma120'].notna(), df['ma60'])
    else:
        ma_col = df['ma60']
    filtered = df[
        (df['adj_score'] > 9) &
        (df['adj_gap'].notna()) &
        (df['fwd_pe'].notna()) & (df['fwd_pe'] > 0) &
        (df['eps_change_90d'] > 0) &
        (df['price'].notna()) & (df['price'] >= 10) &
        (ma_col.notna()) & (df['price'] > ma_col)
    ].copy()

    # v84 (2026-05-30): dd_30_25 진입 필터 — 30일 high 대비 -25%+ drawdown 제외
    # 단기 폭락 종목 차단. BT robust 검증: incl +8.73%p (94/100), excl +7.16%p (77/100)
    if 'high30' in filtered.columns:
        high30 = filtered['high30']
        dd30 = (filtered['price'] - high30) / high30 * 100
        # high30 NULL이면 통과 (cold start 보호), 있으면 dd > -25 필수
        dd_pass = high30.isna() | (dd30 > -25)
        dropped = filtered[~dd_pass]
        if len(dropped) > 0:
            log(f"dd_30_25 진입필터: {len(dropped)}개 제외 (30일 high -25%+ drawdown): "
                f"{', '.join(dropped['ticker'].tolist()[:10])}{'...' if len(dropped) > 10 else ''}")
        filtered = filtered[dd_pass].copy()

    eps_screened = len(filtered)  # EPS 상향 + 추세 필터 통과 수

    # rev_growth 칼럼이 있고 유효 데이터가 충분하면 composite score 사용
    has_rev = 'rev_growth' in filtered.columns and filtered['rev_growth'].notna().sum() >= 10
    if has_rev:
        # 매출 데이터 없음 → 제외
        na_rev = filtered[filtered['rev_growth'].isna()]
        if len(na_rev) > 0:
            log(f"매출 데이터 없음 제외: {', '.join(na_rev['ticker'].tolist())}")
        filtered = filtered[filtered['rev_growth'].notna()].copy()

        # 매출 성장 10% 미만 → 제외 (사이클/기저효과 방지)
        # 주: .info 오류는 _fetch_one()에서 income_stmt로 즉시 교정 (v71.2)
        low_rev = filtered[filtered['rev_growth'] < 0.10]
        if len(low_rev) > 0:
            log(f"매출 성장 부족(<10%) 제외: {', '.join(low_rev['ticker'].tolist())}")
        filtered = filtered[filtered['rev_growth'] >= 0.10].copy()

    # 애널리스트 품질 필터: 저커버리지 + 하향 과다
    if 'num_analysts' in filtered.columns:
        low_cov = filtered[filtered['num_analysts'].fillna(0) < 3]
        if len(low_cov) > 0:
            log(f"저커버리지(<3명) 제외: {', '.join(low_cov['ticker'].tolist())}")
        filtered = filtered[filtered['num_analysts'].fillna(0) >= 3].copy()

    # v80.8 (2026-05-05): 합의 강도 필터 — rev_up30 ≥ 3 종목만 통과.
    # 단일 분석가 의존 종목(WELL 같은 케이스: rev_up=1) 차단.
    # 6시작일 multistart BT +8.51%p, 12시작일 +7.16%p 일관 검증.
    # 7개 맹점 검증 결과 conviction 공식 변경/eps_cap/T0 조정 등 모든 다른 알파를
    # 흡수하는 single-point fix로 확인됨. (project_v80_8_validation 메모리 참조)
    if 'rev_up30' in filtered.columns:
        low_consensus = filtered[filtered['rev_up30'].fillna(0) < 3]
        if len(low_consensus) > 0:
            log(f"낮은 합의(<3명 상향) 제외: {', '.join(low_consensus['ticker'].tolist())}")
        filtered = filtered[filtered['rev_up30'].fillna(0) >= 3].copy()

    if 'rev_up30' in filtered.columns and 'rev_down30' in filtered.columns:
        up = filtered['rev_up30'].fillna(0)
        dn = filtered['rev_down30'].fillna(0)
        total = up + dn
        down_ratio = dn / total.replace(0, float('nan'))
        high_down = filtered[down_ratio > 0.3]
        if len(high_down) > 0:
            details = [f"{r['ticker']}(↑{int(r.get('rev_up30',0))}↓{int(r.get('rev_down30',0))})" for _, r in high_down.iterrows()]
            log(f"하향 과다(>30%) 제외: {', '.join(details)}")
        filtered = filtered[~(down_ratio > 0.3)].copy()

    # 구조적 저마진 필터: OpMargin < 10% AND GrossMargin < 30% → 제외
    # + 영업이익률 극저 (v44): OP < 5% → 제외
    # 주: .info 오류는 verify_info_with_stmt()에서 파이프라인 단계로 사전 교정 (v71.2)
    if 'operating_margin' in filtered.columns and 'gross_margin' in filtered.columns:
        om = filtered['operating_margin']
        gm = filtered['gross_margin']
        low_margin = filtered[om.notna() & gm.notna() & (om < 0.10) & (gm < 0.30)]
        if len(low_margin) > 0:
            details = [f"{r['ticker']}(OM{r['operating_margin']*100:.0f}%/GM{r['gross_margin']*100:.0f}%)" for _, r in low_margin.iterrows()]
            log(f"구조적 저마진 제외: {', '.join(details)}")
        filtered = filtered[~(om.notna() & gm.notna() & (om < 0.10) & (gm < 0.30))].copy()

    # 영업이익률 극저 제외 (v44): OP < 5% — 턴어라운드 초기 종목 과대평가 방지
    if 'operating_margin' in filtered.columns:
        om = filtered['operating_margin']
        ultra_low_op = filtered[om.notna() & (om < 0.05)]
        if len(ultra_low_op) > 0:
            details = [f"{r['ticker']}(OM{r['operating_margin']*100:.0f}%)" for _, r in ultra_low_op.iterrows()]
            log(f"영업이익률 부족(<5%) 제외: {', '.join(details)}")
        filtered = filtered[~(om.notna() & (om < 0.05))].copy()

    # 원자재/광업 제외 (v44): 금, 귀금속, 구리 등 commodity 가격 패스스루 업종
    if 'industry' in filtered.columns:
        commodity = filtered[filtered['industry'].isin(COMMODITY_INDUSTRIES)]
        if len(commodity) > 0:
            log(f"원자재 제외(업종): {', '.join(commodity['ticker'].tolist())}")
        filtered = filtered[~filtered['industry'].isin(COMMODITY_INDUSTRIES)].copy()
    # 개별 원자재 티커 제외 (업종 분류 우회 종목: SQM 리튬 등)
    commodity_tk = filtered[filtered['ticker'].isin(COMMODITY_TICKERS)]
    if len(commodity_tk) > 0:
        log(f"원자재 제외(티커): {', '.join(commodity_tk['ticker'].tolist())}")
        filtered = filtered[~filtered['ticker'].isin(COMMODITY_TICKERS)].copy()

    # v85: 비성장 소비/미디어 업종 제외 (엔터/전문소매) — 사용자 "압도적 성장기업만" 전략 목적 외
    if 'industry' in filtered.columns:
        off_strat = filtered[filtered['industry'].isin(OFF_STRATEGY_INDUSTRIES)]
        if len(off_strat) > 0:
            log(f"비성장 소비/미디어 제외(업종): {', '.join(off_strat['ticker'].tolist())}")
        filtered = filtered[~filtered['industry'].isin(OFF_STRATEGY_INDUSTRIES)].copy()

    # v79.1: FCF<0 AND ROE<0 동시 → 제외 (현금 창출 불가 + 자본 수익 없는 종목)
    # FCF 단독 or ROE 단독 음수는 허용 (SNDK=ROE-, TTMI=FCF- 등 성장주 보호)
    # results_df에 FCF/ROE 컬럼 없을 수 있으므로 DB에서 직접 조회
    try:
        import sqlite3 as _sql
        _conn = _sql.connect(DB_PATH)
        _cur = _conn.cursor()
        _today = filtered['date'].iloc[0] if 'date' in filtered.columns else None
        if _today is None:
            # date 컬럼 없으면 DB에서 최신 날짜
            _today = _cur.execute('SELECT MAX(date) FROM ntm_screening WHERE composite_rank IS NOT NULL').fetchone()[0]
        _fcf_roe = {}
        if _today:
            for r in _cur.execute(
                'SELECT ticker, free_cashflow, roe FROM ntm_screening WHERE date=?', (_today,)
            ).fetchall():
                _fcf_roe[r[0]] = (r[1], r[2])
        _conn.close()

        both_neg_tickers = []
        for tk in filtered['ticker'].values:
            fcf_val, roe_val = _fcf_roe.get(tk, (None, None))
            if fcf_val is not None and roe_val is not None and fcf_val < 0 and roe_val < 0:
                both_neg_tickers.append(tk)
        if both_neg_tickers:
            details = [f"{tk}(FCF{_fcf_roe[tk][0]/1e9:+.1f}B/ROE{_fcf_roe[tk][1]:+.3f})" for tk in both_neg_tickers]
            log(f"FCF·ROE 동시 음수 제외: {', '.join(details)}")
            filtered = filtered[~filtered['ticker'].isin(both_neg_tickers)].copy()
    except Exception as e:
        log(f"FCF·ROE 필터 오류 (스킵): {e}", "WARN")

    # adj_gap 오름차순 정렬 (가장 음수 = 가장 저평가 = 1위)
    # rev_growth는 하드필터(≥10%)로만 사용, 순위 가중치에서 제거 (v52)
    filtered = filtered.sort_values('adj_gap', ascending=True)

    if top_n:
        filtered = filtered.head(top_n)

    if return_counts:
        return filtered, {'eps_screened': eps_screened, 'quality_filtered': len(filtered)}
    return filtered


def log_portfolio_trades(selected, today_str):
    """Forward Test: 포트폴리오 진입/유지/퇴출 기록

    selected = [{'ticker', 'weight', ...}, ...] — 오늘 포트폴리오 종목
    어제 포트폴리오와 비교하여 enter/hold/exit 판별
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 어제 포트폴리오 (hold 또는 enter인 종목)
    cursor.execute('''
        SELECT ticker, entry_date, entry_price, price
        FROM portfolio_log
        WHERE date = (SELECT MAX(date) FROM portfolio_log WHERE date < ?)
        AND action IN ('enter', 'hold')
    ''', (today_str,))
    prev = {r[0]: {'entry_date': r[1], 'entry_price': r[2], 'price': r[3]} for r in cursor.fetchall()}

    today_tickers = {s['ticker'] for s in selected}
    prev_tickers = set(prev.keys())

    # 퇴출: 어제 있었는데 오늘 없는 종목
    for t in prev_tickers - today_tickers:
        p = prev[t]
        # 퇴출 가격 = 오늘(퇴출 결정일) 종가
        row = cursor.execute(
            'SELECT price FROM ntm_screening WHERE date=? AND ticker=?',
            (today_str, t)
        ).fetchone()
        exit_price = row[0] if row and row[0] else p['price']
        entry_price = p['entry_price']
        ret = ((exit_price - entry_price) / entry_price * 100) if entry_price and entry_price > 0 else 0
        cursor.execute(
            'INSERT OR REPLACE INTO portfolio_log (date, ticker, action, price, weight, entry_date, entry_price, exit_price, return_pct) VALUES (?,?,?,?,?,?,?,?,?)',
            (today_str, t, 'exit', exit_price, 0, p['entry_date'], entry_price, exit_price, round(ret, 2))
        )
        log(f"📊 Forward Test: EXIT {t} (진입 {p['entry_date']} ${entry_price:.2f} → ${exit_price:.2f}, {ret:+.1f}%)")

    # 진입/유지
    for s in selected:
        t = s['ticker']
        price = s.get('price', 0) or 0
        weight = s.get('weight', 20)

        if t in prev_tickers:
            # 유지
            p = prev[t]
            cursor.execute(
                'INSERT OR REPLACE INTO portfolio_log (date, ticker, action, price, weight, entry_date, entry_price) VALUES (?,?,?,?,?,?,?)',
                (today_str, t, 'hold', price, weight, p['entry_date'], p['entry_price'])
            )
        else:
            # 신규 진입
            cursor.execute(
                'INSERT OR REPLACE INTO portfolio_log (date, ticker, action, price, weight, entry_date, entry_price) VALUES (?,?,?,?,?,?,?)',
                (today_str, t, 'enter', price, weight, today_str, price)
            )
            log(f"📊 Forward Test: ENTER {t} @ ${price:.2f} ({weight}%)")

    conn.commit()
    conn.close()


def get_forward_test_summary(today_str):
    """포워드 테스트 성과 요약 — NAV 기반 누적 수익률

    일별 포트폴리오 수익률을 복리 계산하여 정확한 누적 수익률 산출.
    exit 종목의 수익도 정확히 반영.
    최소 20거래일 이상이어야 표시 (짧으면 역효과).

    Returns: dict or None
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 날짜 목록
    c.execute('SELECT DISTINCT date FROM portfolio_log ORDER BY date')
    dates = [r[0] for r in c.fetchall()]

    if len(dates) < 2:
        conn.close()
        return None

    # 일별 NAV 계산 (복리)
    nav = 100.0
    for i in range(1, len(dates)):
        prev_date, curr_date = dates[i - 1], dates[i]

        # 전일 보유 종목 (가중치 + 가격)
        c.execute('''
            SELECT ticker, price, weight FROM portfolio_log
            WHERE date = ? AND action IN ('enter', 'hold')
        ''', (prev_date,))
        prev_holdings = {r[0]: {'price': r[1], 'weight': r[2]} for r in c.fetchall()}

        # 당일 전체 기록 (enter/hold/exit 모두)
        c.execute('''
            SELECT ticker, action, price FROM portfolio_log WHERE date = ?
        ''', (curr_date,))
        curr_records = {r[0]: {'action': r[1], 'price': r[2]} for r in c.fetchall()}

        # 전일 보유 종목의 일간 수익률 합산
        daily_return = 0.0
        for ticker, prev in prev_holdings.items():
            if ticker in curr_records and prev['price'] and prev['price'] > 0:
                curr_price = curr_records[ticker]['price']
                stock_return = (curr_price / prev['price']) - 1
                daily_return += stock_return * (prev['weight'] / 100)

        nav *= (1 + daily_return)

    cumulative_return = nav - 100.0

    # 완료된 거래 통계
    c.execute('SELECT return_pct FROM portfolio_log WHERE action = "exit"')
    exits = c.fetchall()
    n_wins = sum(1 for (r,) in exits if r and r > 0)

    conn.close()

    # SPY 동기간 수익률
    spy_return = None
    try:
        import yfinance as yf
        from datetime import datetime, timedelta
        start_dt = datetime.strptime(dates[0], '%Y-%m-%d')
        end_dt = datetime.strptime(dates[-1], '%Y-%m-%d') + timedelta(days=1)
        spy = yf.download('SPY', start=start_dt.strftime('%Y-%m-%d'),
                          end=end_dt.strftime('%Y-%m-%d'), progress=False)
        if len(spy) >= 2:
            spy_start = spy['Close'].iloc[0]
            spy_end = spy['Close'].iloc[-1]
            if hasattr(spy_start, 'item'):
                spy_start = spy_start.item()
            if hasattr(spy_end, 'item'):
                spy_end = spy_end.item()
            spy_return = (spy_end - spy_start) / spy_start * 100
    except Exception as e:
        log(f"SPY 수익률 조회 실패: {e}", "WARN")

    return {
        'start_date': dates[0],
        'n_days': len(dates),
        'cumulative_return': round(cumulative_return, 2),
        'n_exits': len(exits),
        'n_wins': n_wins,
        'spy_return': round(spy_return, 2) if spy_return is not None else None,
    }


def save_part2_ranks(results_df, today_str):
    """Part 2 eligible 종목 저장 — composite_rank + w_gap Top 30 (v58)

    1. 전체 eligible의 당일 adj_gap 순위 → composite_rank 컬럼에 저장
    2. w_gap(3일 가중 adj_gap) 오름차순 상위 30개 → part2_rank 저장
    Returns: Top 30 티커 리스트 (w_gap 순)
    """
    all_candidates = get_part2_candidates(results_df, top_n=None)
    if all_candidates.empty:
        log("Part 2 후보 0개 — part2_rank 저장 스킵")
        return []

    # min_seg < -2% 제외 — 매도 신호 종목은 순위 부여 전에 걸러냄
    def _calc_min_seg(row):
        segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
        return min(segs) if segs else 0
    all_candidates = all_candidates[all_candidates.apply(_calc_min_seg, axis=1) >= -2].copy()

    # 1. 오늘의 composite 순위 (1~N, 당일 conviction adj_gap 오름차순, v71)
    all_candidates = all_candidates.reset_index(drop=True)
    # conviction 적용: adj_gap × (1 + max(rev_up/N, eps_floor) + rev_bonus) (v75)
    def _conv_gap(row):
        ag = float(row.get('adj_gap') or 0)
        up = float(row.get('rev_up30') or 0)
        na = float(row.get('num_analysts') or 0)
        nc = float(row.get('ntm_cur') or 0)  # results_df는 'ntm_cur' 키
        n90 = float(row.get('ntm_90d') or 0)
        rg = row.get('rev_growth')
        rg = float(rg) if rg is not None else None
        return _apply_conviction(ag, up, na, nc, n90, rev_growth=rg)
    all_candidates['_conv_gap'] = all_candidates.apply(_conv_gap, axis=1)
    all_candidates = all_candidates.sort_values('_conv_gap', ascending=True).reset_index(drop=True)
    composite_ranks = {row['ticker']: i + 1 for i, (_, row) in enumerate(all_candidates.iterrows())}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # composite_rank 저장 (모든 eligible 종목)
    cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today_str,))
    for ticker, crank in composite_ranks.items():
        cursor.execute(
            'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
            (crank, today_str, ticker)
        )

    # 2. w_gap(3일 가중 z-score) 기준 Top 30 → part2_rank
    #    v73 percentile rank 시도 → 40일 백테스트에서 -8.6%p 열세로 롤백
    #    이유: conviction 배율(_apply_conviction)이 만든 magnitude 신호를
    #         percentile은 압축해서 버림 (자기모순). z-score는 magnitude 보존.
    eligible_tickers = list(composite_ranks.keys())
    wgap_map = _compute_w_gap_map(cursor, today_str, eligible_tickers)
    sorted_by_wgap = sorted(eligible_tickers, key=lambda tk: wgap_map.get(tk, 0), reverse=True)
    top30 = sorted_by_wgap[:30]

    # part2_rank 저장 (Top 30만)
    cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today_str,))
    top30_tickers = []
    for rank, ticker in enumerate(top30, 1):
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
            (rank, today_str, ticker)
        )
        top30_tickers.append(ticker)

    conn.commit()
    conn.close()
    log(f"Part 2 rank 저장: {len(top30_tickers)}개 종목 (w_gap Top 30, eligible {len(composite_ranks)}개)")
    # v117 (2026-06-09): dollar_volume_30d 업데이트 — 시장 주도주 필터용
    try:
        update_dollar_volumes(today_str, top30_tickers)
    except Exception as _e:
        log(f"dollar_volume 업데이트 실패: {_e}", "WARN")
    return top30_tickers


def update_dollar_volumes(today_str, ticker_list):
    """v117 (2026-06-09): cr Top 30 종목의 직전 30일 거래대금 평균 ($M) DB 업데이트.

    yfinance bulk fetch (45일 history) → 30일 rolling 평균 → ntm_screening.dollar_volume_30d
    매일 cron save_part2_ranks 끝에 자동 호출.
    """
    if not ticker_list:
        return
    try:
        import yfinance as yf
        import pandas as pd
        from datetime import datetime, timedelta
        # 직전 45영업일 fetch (30일 평균 + buffer)
        end_d = (datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=2)).strftime('%Y-%m-%d')
        start_d = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=60)).strftime('%Y-%m-%d')
        data = yf.download(' '.join(ticker_list), start=start_d, end=end_d,
                          auto_adjust=False, progress=False, threads=True, group_by='ticker')
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        updated = 0
        for tk in ticker_list:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    df = data[tk] if tk in data.columns.get_level_values(0) else None
                else:
                    df = data
                if df is None or df.empty:
                    continue
                # 오늘 이전 영업일만 (point-in-time)
                df = df[df.index < pd.to_datetime(today_str)]
                if len(df) < 5:
                    continue
                dv_M = (df['Volume'] * df['Close']) / 1e6
                avg_dv = float(dv_M.tail(30).mean())
                if pd.isna(avg_dv):
                    continue
                cursor.execute(
                    'UPDATE ntm_screening SET dollar_volume_30d=? WHERE date=? AND ticker=?',
                    (avg_dv, today_str, tk)
                )
                updated += 1
            except Exception:
                pass
        conn.commit()
        conn.close()
        log(f"dollar_volume_30d 업데이트: {updated}/{len(ticker_list)} 종목")
    except Exception as e:
        log(f"update_dollar_volumes 오류: {e}", "WARN")


def _apply_conviction(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None,
                       rev_growth=None):
    """adj_gap에 애널리스트 합의 + 매출성장 배율 적용 (v80.9 X2 적용)

    conviction = max(rev_up30/num_analysts, eps_floor) + rev_bonus
    - ratio = rev_up30/num_analysts: 최근 30일 상향 비율 (0.0~1.0)
    - eps_floor: min(|EPS변화율|/100, 3.0) — v80.9: cap 1.0→3.0 (정보 보존)
    - rev_bonus (v80.9): rev_growth × 0.6 비례 (cliff 제거, cap 0.3)
    배율 = 1 + conviction (범위 1.0~3.3)

    v80.9 (2026-05-05) X2 채택: cliff/cap 임의 임계값 제거 → 자연 함수
    근거: 경제학적 합리성 (smooth function, 정보 보존)
    BT: 12시작일 ret -0.44%p (미세), MDD/Sharpe/Sortino 미세 개선
    채택 이유: 미래 환경 변화(매출 30% 경계 종목 / NTM 200%+ 폭증) 대비 robust

    이전 (v75): cliff 30% / cap 1.0 — 6시작일 BT 검증됐으나 미래 환경 변동성 취약
    """
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 3.0)  # X2: 1.0 → 3.0
    base_conviction = max(ratio, eps_floor)

    # v80.9 X2: rev_growth 비례 (cliff 제거)
    if rev_growth is not None:
        rev_bonus = min(min(rev_growth, 0.5) * 0.6, 0.3)
    else:
        rev_bonus = 0.0

    conviction = base_conviction + rev_bonus
    return adj_gap * (1 + conviction)


def _compute_w_gap_map(cursor, today_str, tickers):
    """w_gap(3일 가중 conviction adj_gap) 계산 — T0×0.5 + T1×0.3 + T2×0.2

    v71: adj_gap × (1 + rev_up30/num_analysts) conviction 배율 적용
    Returns: {ticker: float(w_gap)}
    """
    dates = _get_recent_dates(cursor, 'composite_rank', today_str, 3)
    dates = sorted(dates)  # 오래된 순

    import numpy as np
    MISSING_PENALTY = 30  # 빈 날 최종 폴백

    score_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, '
            'rev_growth FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        ).fetchall()
        conv_gaps = {}
        for r in rows:
            tk = r[0]
            conv_gaps[tk] = _apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6])

        vals = list(conv_gaps.values())
        if len(vals) >= 2:
            mean_v = np.mean(vals)
            std_v = np.std(vals)
            if std_v > 0:
                score_by_date[d] = {
                    tk: max(30.0, 65 + (-(v - mean_v) / std_v) * 15)
                    for tk, v in conv_gaps.items()
                }
            else:
                score_by_date[d] = {tk: 65 for tk in conv_gaps}
        else:
            score_by_date[d] = {tk: 65 for tk in conv_gaps}

    weights = [0.2, 0.3, 0.5]  # T-2, T-1, T0 (오래된순)
    if len(dates) == 2:
        weights = [0.4, 0.6]
    elif len(dates) == 1:
        weights = [1.0]

    # v80.1 (2026-04-24): "빈 날" 기준을 composite_rank → part2_rank로 변경.
    # 이전(cr 기준): 그 날 eligible이면 실제 z-score 사용.
    #   → 궤적 표시("⏳"인 종목의 T-2는 "-")와 w_gap 계산 기준 불일치.
    #   → TSM 4/21 사례: ⏳(2일 검증)인데 3일치 실제 z-score 들어가서 wr 3위
    #      (✅ ASML 4위보다 앞섬) — 상태 라벨과 데이터 사용 일수 모순.
    # 현재(p2 기준): T-1/T-2에 당시 Top 30(p2_rank) 밖이면 penalty 30점.
    #   → ⏳ = 2일치 실제 + 1일 penalty, 🆕 = 1일 실제 + 2일 penalty 일관성 확보.
    #   T-0은 이 함수 실행 시점에 p2_rank가 아직 NULL이라 cr 기준 유지.
    # 영향 검증: 최근 30거래일 BT에서 ✅ 진입 3종목 변경 0건, Top 8 순위만
    #   ⏳/🆕 종목이 뒤로 밀림. 실거래 영향 없음.
    p2_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)
        ).fetchall()
        p2_by_date[d] = {r[0] for r in rows}

    result = {}
    for tk in tickers:
        wg = 0
        for i, d in enumerate(dates):
            is_today = (d == today_str)
            if not is_today and tk not in p2_by_date.get(d, set()):
                # 과거 날짜에 당시 Top 30 밖이었으면 빈 날 penalty
                score = MISSING_PENALTY
            else:
                score = score_by_date.get(d, {}).get(tk)
                if score is None:
                    score = MISSING_PENALTY
            wg += score * weights[i]
        result[tk] = wg
    return result


def _compute_weighted_rank_map(cursor, today_str, tickers):
    """[DEPRECATED 2026-04 — 연구용, production 미사용]
    v73 percentile rank 변형. 40일 백테스트에서 z-score 대비 -8.6%p 열세로 롤백.
    원인: conviction 배율(_apply_conviction)이 만든 magnitude 신호를 percentile이
    압축해서 버림. 미래 A/B shadow 테스트용으로만 보존.

    3일 가중 순위(percentile rank) 계산 — composite_rank 기반
    Returns: {ticker: float(weighted_rank)} — 값이 작을수록 상위
    """
    dates = _get_recent_dates(cursor, 'composite_rank', today_str, 3)
    dates = sorted(dates)  # 오래된 순

    weights = [0.2, 0.3, 0.5]  # T-2, T-1, T0
    if len(dates) == 2:
        weights = [0.4, 0.6]
    elif len(dates) == 1:
        weights = [1.0]

    # 일별 composite_rank 수집
    rank_by_date = {}
    n_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, composite_rank FROM ntm_screening '
            'WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        ).fetchall()
        rank_by_date[d] = {r[0]: r[1] for r in rows}
        n_by_date[d] = len(rows) if rows else 1

    result = {}
    for tk in tickers:
        wr = 0
        for i, d in enumerate(dates):
            rank = rank_by_date.get(d, {}).get(tk)
            if rank is not None:
                n = n_by_date[d]
                # percentile rank: (rank-1)/(N-1), 0=최상위, 1=최하위
                pct = (rank - 1) / max(n - 1, 1)
                wr += pct * weights[i]
            else:
                # 미존재 = 최하위 + 페널티 (carry-forward 방지)
                wr += 1.1 * weights[i]
        result[tk] = wr
    return result


def is_cold_start():
    """DB에 part2_rank 데이터가 3일 미만이면 True (채널 전송 제어용)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT date) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    count = cursor.fetchone()[0]
    conn.close()
    return count < 3


def _get_recent_dates(cursor, rank_col='part2_rank', today_str=None, limit=3):
    """공통 헬퍼: 최근 N개 distinct date 조회 (rank_col이 NOT NULL인 날짜만)"""
    if today_str:
        cursor.execute(
            f'SELECT DISTINCT date FROM ntm_screening WHERE {rank_col} IS NOT NULL AND date <= ? ORDER BY date DESC LIMIT ?',
            (today_str, limit)
        )
    else:
        cursor.execute(
            f'SELECT DISTINCT date FROM ntm_screening WHERE {rank_col} IS NOT NULL ORDER BY date DESC LIMIT ?',
            (limit,)
        )
    return [r[0] for r in cursor.fetchall()]


def get_3day_status(today_tickers, today_str=None):
    """3일 연속 Top 30 진입 여부 판별 → {ticker: '✅' or '⏳' or '🆕'}
    ✅ = 3일 연속 Top 30 (L3 동결 시 유지 대상)
    ⏳ = 2일 연속 Top 30
    🆕 = 오늘만 Top 30
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = _get_recent_dates(cursor, 'part2_rank', today_str, 3)

    if len(dates) < 2:
        conn.close()
        log(f"3일 교집합: DB {len(dates)}일뿐 — 전부 🆕 처리 (cold start)")
        return {t: '🆕' for t in today_tickers}

    placeholders = ','.join('?' * len(dates))

    # 3일 모두 Top 30 (part2_rank 존재)
    verified_3d = set()
    if len(dates) >= 3:
        cursor.execute(f'''
            SELECT ticker FROM ntm_screening
            WHERE date IN ({placeholders}) AND part2_rank IS NOT NULL
            GROUP BY ticker HAVING COUNT(DISTINCT date) = 3
        ''', dates)
        verified_3d = {r[0] for r in cursor.fetchall()}

    # 최근 2일 모두 Top 30
    dates_2d = dates[:2]
    ph2 = ','.join('?' * len(dates_2d))
    cursor.execute(f'''
        SELECT ticker FROM ntm_screening
        WHERE date IN ({ph2}) AND part2_rank IS NOT NULL
        GROUP BY ticker HAVING COUNT(DISTINCT date) = 2
    ''', dates_2d)
    verified_2d = {r[0] for r in cursor.fetchall()}

    conn.close()

    status = {}
    for t in today_tickers:
        if t in verified_3d:
            status[t] = '✅'
        elif t in verified_2d:
            status[t] = '⏳'
        else:
            status[t] = '🆕'

    v3 = sum(1 for v in status.values() if v == '✅')
    v2 = sum(1 for v in status.values() if v == '⏳')
    v1 = sum(1 for v in status.values() if v == '🆕')
    log(f"3일 교집합: ✅ {v3}개, ⏳ {v2}개, 🆕 {v1}개")
    return status


def get_rank_history(today_tickers, today_str=None):
    """최근 3일간 part2_rank 이력 → {ticker: '3→4→1'} 형태"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = sorted(_get_recent_dates(cursor, 'part2_rank', today_str, 3))

    if len(dates) < 2:
        conn.close()
        return {}

    rank_by_date = {}
    for d in dates:
        cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL AND part2_rank <= 30',
            (d,)
        )
        rank_by_date[d] = {r[0]: r[1] for r in cursor.fetchall()}
    conn.close()

    history = {}
    for t in today_tickers:
        parts = []
        for d in dates:
            r = rank_by_date.get(d, {}).get(t)
            parts.append(str(r) if r else '-')
        history[t] = '→'.join(parts)
    return history


def compute_weighted_ranks(today_tickers, today_str=None):
    """3일 순위 궤적 — composite_rank(당일 conviction 순위) 기반
    T0 × 0.5 + T1 × 0.3 + T2 × 0.2
    composite_rank = 각 날짜의 당일 conviction adj_gap 순위 (raw)
    Watchlist 표시 순서는 part2_rank(3일 가중), 여기는 추이 표시용
    Returns: {ticker: {'weighted': float, 'r0': int, 'r1': int, 'r2': int}}
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = sorted(_get_recent_dates(cursor, 'composite_rank', today_str, 3))

    if not dates:
        conn.close()
        return {}

    PENALTY = 50

    rank_by_date = {}
    for d in dates:
        cursor.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        )
        rank_by_date[d] = {r[0]: r[1] for r in cursor.fetchall()}
    conn.close()

    today = dates[-1]
    t1 = dates[-2] if len(dates) >= 2 else None
    t2 = dates[-3] if len(dates) >= 3 else None

    result = {}
    for t in today_tickers:
        r0 = rank_by_date.get(today, {}).get(t, PENALTY)
        r1 = rank_by_date.get(t1, {}).get(t, PENALTY) if t1 else PENALTY
        r2 = rank_by_date.get(t2, {}).get(t, PENALTY) if t2 else PENALTY

        weighted = r0 * 0.5 + r1 * 0.3 + r2 * 0.2
        result[t] = {
            'weighted': round(weighted, 1),
            'r0': r0, 'r1': r1, 'r2': r2
        }

    log(f"가중 순위: {len(result)}개 종목 계산 (날짜 {len(dates)}일)")
    return result


def get_rank_change_tags(today_tickers, weighted_ranks):
    """순위 변동 원인 태그 — 2축 독립 판정 (v36.4)

    가격축(실제 주가 변동%)과 실적축(adj_score 변동)을 독립적으로 판정.
    각 축의 일간 변동 표준편차(1.0σ) 기준으로 임계값 설정.
    둘 다 해당하면 둘 다 표시. |순위변동| < 3이면 태그 없음.

    3일 궤적(r2 < PENALTY) → T0 vs T2 비교 (2일치 누적 delta)
    2일 궤적(r2 = PENALTY) → T0 vs T1 비교 (1일치 delta)
    Returns: {ticker: tag_str}
    """
    RANK_THRESHOLD = 3
    # 1.0σ 기반 임계값 (7일 데이터 기준, 데이터 축적 후 업데이트)
    PRICE_STD = 2.83   # 주가 일간 수익률 σ (%)
    SCORE_STD = 1.48   # adj_score 일간 변동 σ
    PENALTY = 50

    if not weighted_ranks:
        return {}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 최근 3일 날짜 (T0, T1, T2)
    cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date DESC LIMIT 3'
    )
    dates = [r[0] for r in cursor.fetchall()]
    if len(dates) < 2:
        conn.close()
        return {}

    today_date = dates[0]
    t1_date = dates[1]
    t2_date = dates[2] if len(dates) >= 3 else None

    # 각 날짜별 메트릭 조회 (price + adj_score)
    metric_by_date = {}
    for d in dates:
        cursor.execute(
            'SELECT ticker, price, adj_score FROM ntm_screening '
            'WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        )
        metric_by_date[d] = {r[0]: {'price': r[1], 'adj_score': r[2]} for r in cursor.fetchall()}

    conn.close()

    today_data = metric_by_date.get(today_date, {})
    t1_data = metric_by_date.get(t1_date, {})
    t2_data = metric_by_date.get(t2_date, {}) if t2_date else {}

    tags = {}
    for ticker in today_tickers:
        w_info = weighted_ranks.get(ticker)
        if not w_info:
            tags[ticker] = ''
            continue

        r0 = w_info.get('r0', PENALTY)
        r1 = w_info.get('r1', PENALTY)
        r2 = w_info.get('r2', PENALTY)

        # 3일 궤적: r2 < PENALTY → T0 vs T2 비교
        # 2일 궤적: r2 = PENALTY → T0 vs T1 비교
        has_3day = r2 < PENALTY

        if has_3day:
            rank_chg = r0 - r2
            ref_data = t2_data
        else:
            if r1 >= PENALTY:
                tags[ticker] = ''
                continue
            rank_chg = r0 - r1
            ref_data = t1_data

        if abs(rank_chg) < RANK_THRESHOLD:
            tags[ticker] = ''
            continue

        t0 = today_data.get(ticker, {})
        ref = ref_data.get(ticker, {})

        # 가격축: 실제 주가 변동률 (%)
        p0 = t0.get('price')
        p_ref = ref.get('price')
        if p0 and p_ref and p_ref > 0:
            price_chg_pct = (p0 - p_ref) / p_ref * 100
        else:
            price_chg_pct = 0

        # 실적축: adj_score 변동
        score_delta = (t0.get('adj_score') or 0) - (ref.get('adj_score') or 0)

        # σ 넘은 변동은 방향 무관하게 전부 표시 (상태 정보)
        tag_parts = []
        if price_chg_pct >= PRICE_STD:
            tag_parts.append('주가↑')
        elif price_chg_pct <= -PRICE_STD:
            tag_parts.append('주가↓')
        if score_delta >= SCORE_STD:
            tag_parts.append('전망↑')
        elif score_delta <= -SCORE_STD:
            tag_parts.append('전망↓')

        tags[ticker] = ' '.join(tag_parts)

    tag_count = sum(1 for v in tags.values() if v)
    log(f"순위 변동 태그: {tag_count}개 종목 (1.0σ 기준: price±{PRICE_STD}%, score±{SCORE_STD})")
    return tags


def get_daily_changes(today_tickers, today_str=None):
    """어제 대비 리스트 변동 — 신규 진입 / 이탈 종목 (단순 set 비교)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 어제 날짜 (part2_rank 있는 가장 최근, today_str 이하)
    if today_str:
        cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL AND date <= ? ORDER BY date DESC LIMIT 2',
            (today_str,)
        )
    else:
        cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 2'
        )
    dates = [r[0] for r in cursor.fetchall()]

    if len(dates) < 2:
        conn.close()
        return [], []

    yesterday = dates[1]

    cursor.execute(
        'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL AND part2_rank <= 20',
        (yesterday,)
    )
    yesterday_ranks = {r[0]: r[1] for r in cursor.fetchall()}
    conn.close()

    yesterday_top20 = set(yesterday_ranks.keys())
    today_top20 = set(today_tickers[:20])  # w_gap 순 Top 20만 비교 (Watchlist 기준)
    entered = today_top20 - yesterday_top20
    exited = yesterday_top20 - today_top20
    exited_with_rank = {t: yesterday_ranks[t] for t in exited}

    log(f"어제 대비: +{len(entered)} 신규, -{len(exited)} 이탈")
    return sorted(entered), exited_with_rank


_HY_CACHE_PATH = Path(__file__).parent / 'data_cache' / 'hy_spread.parquet'


def _load_merge_save_hy_cache(fred_df):
    """로컬 장기 캐시 + FRED 최근분 병합 후 캐시 갱신

    FRED는 2026-04부터 BAMLH0A0HYM2를 최근 3년으로 제한 (series note 명시).
    1996년부터의 장기 데이터는 로컬 parquet으로 유지하고,
    매일 FRED 최근분을 받아 겹치는 구간을 덮어쓰며 꼬리를 연장한다.
    """
    import pandas as pd
    try:
        cache_df = pd.read_parquet(_HY_CACHE_PATH) if _HY_CACHE_PATH.exists() else None
    except Exception as e:
        log(f"HY Spread: 캐시 로드 실패: {e} — 신규 생성", level="WARN")
        cache_df = None

    if cache_df is None or cache_df.empty:
        merged = fred_df.copy()
    else:
        merged = cache_df.copy()
        for ts, val in fred_df['hy_spread'].items():
            merged.loc[ts, 'hy_spread'] = val
        merged = merged.sort_index()

    try:
        _HY_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(_HY_CACHE_PATH)
    except Exception as e:
        log(f"HY Spread: 캐시 저장 실패 (계속 진행): {e}", level="WARN")

    return merged


def fetch_hy_quadrant():
    """HY Spread Verdad 4분면 + 해빙 신호 (FRED BAMLH0A0HYM2)

    수준: HY vs 10년 롤링 중위수 (넓/좁)
    방향: 현재 vs 63영업일(3개월) 전 (상승/하락)
    → Q1 회복(넓+하락), Q2 성장(좁+하락), Q3 과열(좁+상승), Q4 침체(넓+상승)

    FRED 최근분(3년) + 로컬 장기 캐시 병합. FRED가 2026-04부터 3년 제한.
    """
    import urllib.request
    import json as _json
    import io
    import pandas as pd
    import numpy as np
    import time

    for attempt in range(3):
      try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365 * 11)).strftime('%Y-%m-%d')
        fred_key = os.environ.get('FRED_API_KEY', '')

        if fred_key:
            # FRED 공식 API (JSON) — 안정적
            url = (f"https://api.stlouisfed.org/fred/series/observations"
                   f"?series_id=BAMLH0A0HYM2&api_key={fred_key}&file_type=json"
                   f"&observation_start={start_date}&observation_end={end_date}")
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = _json.loads(response.read().decode('utf-8'))
            rows = [(r['date'], r['value']) for r in data['observations'] if r['value'] != '.']
            fred_df = pd.DataFrame(rows, columns=['date', 'hy_spread'])
            fred_df['date'] = pd.to_datetime(fred_df['date'])
        else:
            # fallback: CSV 엔드포인트 (API key 없을 때)
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2&cosd={start_date}&coed={end_date}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=60) as response:
                csv_data = response.read().decode('utf-8')
            fred_df = pd.read_csv(io.StringIO(csv_data), parse_dates=['observation_date'])
            fred_df.columns = ['date', 'hy_spread']

        fred_df = fred_df.dropna(subset=['hy_spread'])
        fred_df['hy_spread'] = pd.to_numeric(fred_df['hy_spread'], errors='coerce')
        fred_df = fred_df.dropna().set_index('date').sort_index()

        # FRED는 최근 3년만 반환 → 로컬 장기 캐시와 병합
        df = _load_merge_save_hy_cache(fred_df)

        if len(df) < 1260:  # 최소 5년치 필요
            log(f"HY Spread: 데이터 부족 ({len(df)}/1260)", level="WARN")
            return None

        # 10년 롤링 중위수 (min 5년)
        df['median_10y'] = df['hy_spread'].rolling(2520, min_periods=1260).median()

        hy_spread = df['hy_spread'].iloc[-1]
        hy_prev = df['hy_spread'].iloc[-2]
        median_10y = df['median_10y'].iloc[-1]

        if pd.isna(median_10y):
            log("HY Spread: 중위수 계산 불가", level="WARN")
            return None

        # 3개월(63영업일) 전
        hy_3m_ago = df['hy_spread'].iloc[-63] if len(df) >= 63 else df['hy_spread'].iloc[0]

        # 분면 판정
        is_wide = hy_spread >= median_10y
        is_rising = hy_spread >= hy_3m_ago

        if is_wide and not is_rising:
            quadrant, label, icon = 'Q1', '봄(회복국면)', '🌸'
        elif not is_wide and not is_rising:
            quadrant, label, icon = 'Q2', '여름(성장국면)', '☀️'
        elif not is_wide and is_rising:
            quadrant, label, icon = 'Q3', '가을(과열국면)', '🍂'
        else:  # wide and rising
            quadrant, label, icon = 'Q4', '겨울(침체국면)', '❄️'

        # 해빙 신호 감지
        signals = []
        daily_change_bp = (hy_spread - hy_prev) * 100

        # 1) HY 4~5%에서 -20bp 급축소
        if 4 <= hy_spread <= 5 and daily_change_bp <= -20:
            signals.append(f'💎 회사채 금리차 {hy_spread:.2f}%, 전일 대비 {daily_change_bp:+.0f}bp 급축소')

        # 2) 5% 하향 돌파
        if hy_prev >= 5 and hy_spread < 5:
            signals.append(f'💎 회사채 금리차 {hy_spread:.2f}%로 5% 하회 — 신용 여건 개선 신호')

        # 3) 60일 고점 대비 -300bp 이상 하락
        peak_60d = df['hy_spread'].rolling(60).max().iloc[-1]
        from_peak_bp = (hy_spread - peak_60d) * 100
        if from_peak_bp <= -300:
            signals.append(f'💎 60일 고점 대비 {from_peak_bp:.0f}bp 하락 — 과거 이 신호 후 반등 확률 높음.')

        # 4) Q4→Q1 전환 (전일 분면 계산)
        prev_wide = hy_prev >= median_10y
        hy_3m_ago_prev = df['hy_spread'].iloc[-64] if len(df) >= 64 else df['hy_spread'].iloc[0]
        prev_rising = hy_prev >= hy_3m_ago_prev
        prev_was_q4 = prev_wide and prev_rising
        now_is_q1 = is_wide and not is_rising
        if prev_was_q4 and now_is_q1:
            signals.append('💎 겨울→봄 전환 — 과거 30년 평균 연 +14.3% 구간')

        # 현재 분면 지속 일수 (최대 252영업일=1년까지 역추적)
        df['hy_3m'] = df['hy_spread'].shift(63)
        valid_mask = df['median_10y'].notna() & df['hy_3m'].notna()
        df.loc[valid_mask, 'q'] = np.where(
            df.loc[valid_mask, 'hy_spread'] >= df.loc[valid_mask, 'median_10y'],
            np.where(df.loc[valid_mask, 'hy_spread'] >= df.loc[valid_mask, 'hy_3m'], 'Q4', 'Q1'),
            np.where(df.loc[valid_mask, 'hy_spread'] >= df.loc[valid_mask, 'hy_3m'], 'Q3', 'Q2')
        )
        q_days = 1
        for i in range(len(df) - 2, max(len(df) - 253, 0) - 1, -1):
            if i >= 0 and df['q'].iloc[i] == quadrant:
                q_days += 1
            else:
                break

        # HY 단독 상황 기술 (fallback용, 최종은 get_market_risk_status에서 결정)
        if quadrant == 'Q1':
            action = '회복 구간 — 과거 30년 연평균 +14.3%'
        elif quadrant == 'Q2':
            action = '안정 구간 — 과거 30년 연평균 +9.4%'
        elif quadrant == 'Q3':
            action = '과열 구간 — 수익률 둔화 경향'
        else:  # Q4
            action = '침체 구간 — 관망 유리'

        # HY 퍼센타일 (10년 rolling, VIX와 동일 방식)
        hy_pct = float(df['hy_spread'].rolling(2520, min_periods=1260).rank(pct=True).iloc[-1] * 100)

        return {
            'hy_spread': hy_spread,
            'median_10y': median_10y,
            'hy_3m_ago': hy_3m_ago,
            'hy_prev': hy_prev,
            'hy_percentile': hy_pct,
            'quadrant': quadrant,
            'quadrant_label': label,
            'quadrant_icon': icon,
            'signals': signals,
            'q_days': q_days,
            'action': action,
        }

      except Exception as e:
        if attempt < 2:
            log(f"HY Spread 수집 재시도 ({attempt+1}/3): {e}", level="WARN")
            time.sleep(5 * (attempt + 1))
        else:
            log(f"HY Spread 수집 실패: {e}", level="WARN")
            return None


def fetch_vix_data():
    """VIX(CBOE 변동성 지수) 레짐 판단 + 현금비중 가감 (FRED VIXCLS)

    252일(1년) 퍼센타일 기반 레짐 판정 — 시대 변화에 자동 적응
    < 10th: 안일 | 10~67th: 정상 | 67~80th: 경계 | 80~90th: 상승경보 | 90th+: 위기

    Returns:
        dict or None: {vix_current, vix_5d_ago, vix_slope, vix_slope_dir,
                       vix_ma_20, vix_percentile, regime, regime_label, regime_icon,
                       cash_adjustment, direction}
    """
    import urllib.request
    import json as _json
    import io
    import pandas as pd
    import time

    for attempt in range(3):
      try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
        fred_key = os.environ.get('FRED_API_KEY', '')

        if fred_key:
            url = (f"https://api.stlouisfed.org/fred/series/observations"
                   f"?series_id=VIXCLS&api_key={fred_key}&file_type=json"
                   f"&observation_start={start_date}&observation_end={end_date}")
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = _json.loads(response.read().decode('utf-8'))
            rows = [(r['date'], r['value']) for r in data['observations'] if r['value'] != '.']
            df = pd.DataFrame(rows, columns=['date', 'vix'])
            df['date'] = pd.to_datetime(df['date'])
        else:
            url = (f"https://fred.stlouisfed.org/graph/fredgraph.csv"
                   f"?id=VIXCLS&cosd={start_date}&coed={end_date}")
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as response:
                csv_data = response.read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data), parse_dates=['observation_date'])
            df.columns = ['date', 'vix']

        df['vix'] = pd.to_numeric(df['vix'], errors='coerce')
        df = df.dropna().set_index('date').sort_index()

        # FRED 지연 보완: yfinance ^VIX로 최신 종가 추가
        try:
            import yfinance as yf
            vix_yf = yf.download('^VIX', period='5d', progress=False)
            if not vix_yf.empty:
                vix_yf.index = vix_yf.index.tz_localize(None)
                # Close 컬럼 추출 (MultiIndex 대응)
                if isinstance(vix_yf.columns, pd.MultiIndex):
                    close_col = vix_yf['Close']['^VIX']
                else:
                    close_col = vix_yf['Close']
                for d, v in close_col.items():
                    d = d.normalize()
                    v = float(v)
                    if d not in df.index and v > 0:
                        df.loc[d] = v
                df = df.sort_index()
        except Exception as e:
            log(f"VIX yfinance 보완 실패 (FRED만 사용): {e}", level="WARN")

        if len(df) < 20:
            log("VIX: 데이터 부족", level="WARN")
            return None

        vix_current = float(df['vix'].iloc[-1])
        vix_5d_ago = float(df['vix'].iloc[-5]) if len(df) >= 5 else float(df['vix'].iloc[0])
        vix_slope = vix_current - vix_5d_ago
        vix_ma_20 = float(df['vix'].rolling(20).mean().iloc[-1])

        # 252일(1년) 퍼센타일 계산 (최소 126일)
        vix_pct = float(df['vix'].rolling(252, min_periods=126).rank(pct=True).iloc[-1] * 100)

        # Slope direction (±0.5 threshold to avoid noise)
        if vix_slope > 0.5:
            slope_dir = 'rising'
        elif vix_slope < -0.5:
            slope_dir = 'falling'
        else:
            slope_dir = 'flat'

        # 퍼센타일 기반 레짐 + 현금 가감
        if vix_pct >= 90:
            # 위기 (상위 10%)
            if slope_dir in ('rising', 'flat'):
                regime, label, icon = 'crisis', '위기', '🔴'
                cash_adj = 15
            else:
                regime, label, icon = 'crisis_relief', '공포완화', '💎'
                cash_adj = -10
        elif vix_pct >= 80:
            # 상승경보 (상위 10~20%)
            if slope_dir == 'rising':
                regime, label, icon = 'high', '상승경보', '🔶'
                cash_adj = 10
            else:
                regime, label, icon = 'high_stable', '높지만안정', '🟡'
                cash_adj = 0
        elif vix_pct >= 67:
            # 경계 (상위 20~33%)
            if slope_dir == 'rising':
                regime, label, icon = 'elevated', '경계', '⚠️'
                cash_adj = 5
            elif slope_dir == 'falling':
                regime, label, icon = 'stabilizing', '안정화', '🌡️'
                cash_adj = -5
            else:
                regime, label, icon = 'elevated_flat', '보통', '🟡'
                cash_adj = 0
        elif vix_pct < 10:
            # 안일 (하위 10% — 과도한 낙관)
            regime, label, icon = 'complacency', '안일', '⚠️'
            cash_adj = 5
        else:
            # 정상 (10~67th)
            regime, label, icon = 'normal', '안정', '🌡️'
            cash_adj = 0

        # Simplified direction for concordance check
        direction = 'warn' if regime in ('crisis', 'crisis_relief', 'high', 'elevated', 'complacency') else 'stable'

        log(f"VIX: {vix_current:.1f} (252일 {vix_pct:.0f}th) → {regime} ({label}), 가감 {cash_adj:+d}%")

        return {
            'vix_current': vix_current,
            'vix_5d_ago': vix_5d_ago,
            'vix_slope': vix_slope,
            'vix_slope_dir': slope_dir,
            'vix_ma_20': vix_ma_20,
            'vix_percentile': vix_pct,
            'regime': regime,
            'regime_label': label,
            'regime_icon': icon,
            'cash_adjustment': cash_adj,
            'direction': direction,
        }

      except Exception as e:
        if attempt < 2:
            log(f"VIX 수집 재시도 ({attempt+1}/3): {e}", level="WARN")
            time.sleep(5 * (attempt + 1))
        else:
            log(f"VIX FRED 수집 실패, yfinance fallback 시도: {e}", level="WARN")
            return _fetch_vix_yfinance_fallback()


def _fetch_vix_yfinance_fallback():
    """FRED 실패 시 yfinance ^VIX로 VIX 데이터 수집"""
    try:
        import yfinance as yf
        import pandas as pd

        ticker = yf.Ticker('^VIX')
        df = ticker.history(period='1y')
        if df.empty or len(df) < 20:
            log("VIX yfinance fallback: 데이터 부족", level="WARN")
            return None

        df = df[['Close']].rename(columns={'Close': 'vix'}).dropna()

        vix_current = float(df['vix'].iloc[-1])
        vix_5d_ago = float(df['vix'].iloc[-5]) if len(df) >= 5 else float(df['vix'].iloc[0])
        vix_slope = vix_current - vix_5d_ago
        vix_ma_20 = float(df['vix'].rolling(20).mean().iloc[-1])

        vix_pct = float(df['vix'].rolling(252, min_periods=126).rank(pct=True).iloc[-1] * 100)

        if vix_slope > 0.5:
            slope_dir = 'rising'
        elif vix_slope < -0.5:
            slope_dir = 'falling'
        else:
            slope_dir = 'flat'

        if vix_pct >= 90:
            if slope_dir in ('rising', 'flat'):
                regime, label, icon = 'crisis', '위기', '🔴'
                cash_adj = 15
            else:
                regime, label, icon = 'crisis_relief', '공포완화', '💎'
                cash_adj = -10
        elif vix_pct >= 80:
            if slope_dir == 'rising':
                regime, label, icon = 'high', '상승경보', '🔶'
                cash_adj = 10
            else:
                regime, label, icon = 'high_stable', '높지만안정', '🟡'
                cash_adj = 0
        elif vix_pct >= 67:
            if slope_dir == 'rising':
                regime, label, icon = 'elevated', '경계', '⚠️'
                cash_adj = 5
            elif slope_dir == 'falling':
                regime, label, icon = 'stabilizing', '안정화', '🌡️'
                cash_adj = -5
            else:
                regime, label, icon = 'elevated_flat', '보통', '🟡'
                cash_adj = 0
        elif vix_pct < 10:
            regime, label, icon = 'complacency', '안일', '⚠️'
            cash_adj = 5
        else:
            regime, label, icon = 'normal', '안정', '🌡️'
            cash_adj = 0

        direction = 'warn' if regime in ('crisis', 'crisis_relief', 'high', 'elevated', 'complacency') else 'stable'

        log(f"VIX (yfinance fallback): {vix_current:.1f} (252일 {vix_pct:.0f}th) → {regime} ({label}), 가감 {cash_adj:+d}%")

        return {
            'vix_current': vix_current,
            'vix_5d_ago': vix_5d_ago,
            'vix_slope': vix_slope,
            'vix_slope_dir': slope_dir,
            'vix_ma_20': vix_ma_20,
            'vix_percentile': vix_pct,
            'regime': regime,
            'regime_label': label,
            'regime_icon': icon,
            'cash_adjustment': cash_adj,
            'direction': direction,
        }
    except Exception as e:
        log(f"VIX yfinance fallback도 실패: {e}", level="WARN")
        return None


# ── 국면(regime) 오버레이 (2026-05-27) ──
# S&P 500 < 200일선(15일 확인) OR VIX > 36(2일 확인) → defense(방어).
# defense 시 주식 매수 중단 + 채권ETF(IEF 기본 / BIL 안전) 권장.
# 26년 시장데이터 EDA(research/regime_eda_*.py): 4대 약세장(dotcom/GFC/COVID/2022) 포착,
#   QQQ 프록시 MDD -83%→-29%, Cal 0.11→0.50. 인버스ETF는 측정 결과 열위(탈락).
# 확인 15일: 10일이면 2026-04 1~4% 얕은 dip(15일 안에 V자 회복)에 휘프소 → -105%p 손실.
#   15일은 그 휘프소 거르면서(우리 window defense 0일) 진짜 약세장(15일+ 지속) 그대로 포착.
#   26년 검증서도 15일이 10일보다 Cal 0.27>0.26, MDD -36.5%<-40%, 전환 31<41로 우월.
# ⚠️ 신호 품질은 26년 검증, 우리 전략 이득은 프록시 추정 (약세장 종목데이터 없음).
# 현재(2026-05) regime=boost → 배포 시 즉시 영향 0, 미래 약세장에만 자동 발동.
REGIME_MA_PERIOD = int(os.environ.get('REGIME_MA_PERIOD', '200'))
REGIME_MA_CONFIRM = int(os.environ.get('REGIME_MA_CONFIRM', '15'))
REGIME_VIX_THRESH = float(os.environ.get('REGIME_VIX_THRESH', '36'))
REGIME_VIX_CONFIRM = int(os.environ.get('REGIME_VIX_CONFIRM', '2'))
REGIME_OVERLAY_DISABLE = os.environ.get('REGIME_OVERLAY_DISABLE', '') == '1'


def _confirm_regime(raw_seq, n):
    """raw(bool 시퀀스, 오래된→최신) → 최신 시점 defense 여부 (n일 연속 확인, 히스테리시스)."""
    state = False
    sd = sb = 0
    for d in raw_seq:
        if d:
            sd += 1
            sb = 0
        else:
            sb += 1
            sd = 0
        if not state and sd >= n:
            state = True
        elif state and sb >= n:
            state = False
    return state


def get_market_regime():
    """국면 판단 — SPX<MA200(확인) OR VIX>thresh(확인) → defense.

    stateless: 매 실행마다 ~2년 히스토리에서 confirm 재계산 (상태파일 불필요).
    Returns: {regime:'boost'|'defense', reason, spx, ma200, vix, days_below}
    """
    if REGIME_OVERLAY_DISABLE:
        return {'regime': 'boost', 'reason': 'overlay disabled',
                'spx': None, 'ma200': None, 'vix': None, 'days_below': 0}
    forced = os.environ.get('REGIME_FORCE', '').strip().lower()
    if forced in ('boost', 'defense'):
        return {'regime': forced, 'reason': f'[테스트 강제 {forced} 모드]',
                'spx': None, 'ma200': None, 'vix': None, 'days_below': 0}
    try:
        import yfinance as yf
        spx = yf.download('^GSPC', period='2y', auto_adjust=True, progress=False)
        cl = spx['Close']
        if hasattr(cl, 'columns'):
            cl = cl.iloc[:, 0]
        cl = cl.dropna()
        ma = cl.rolling(REGIME_MA_PERIOD).mean()
        below = (cl < ma).dropna()
        ma_defense = _confirm_regime(list(below.values[-260:]), REGIME_MA_CONFIRM)
        spx_now, ma_now = float(cl.iloc[-1]), float(ma.iloc[-1])
        days_below = 0
        for v in reversed(below.values):
            if v:
                days_below += 1
            else:
                break

        vix_df = yf.download('^VIX', period='1mo', auto_adjust=True, progress=False)
        vcl = vix_df['Close']
        if hasattr(vcl, 'columns'):
            vcl = vcl.iloc[:, 0]
        vcl = vcl.dropna()
        vix_now = float(vcl.iloc[-1])
        vix_defense = _confirm_regime(list((vcl > REGIME_VIX_THRESH).values), REGIME_VIX_CONFIRM)

        reasons = []
        if ma_defense:
            reasons.append(f'S&P 200일선 이탈 {days_below}일')
        if vix_defense:
            reasons.append(f'VIX 급등({vix_now:.0f}>{REGIME_VIX_THRESH:.0f})')
        return {
            'regime': 'defense' if (ma_defense or vix_defense) else 'boost',
            'reason': ' + '.join(reasons) if reasons else '정상 (S&P 200일선 위, VIX 안정)',
            'spx': spx_now, 'ma200': ma_now, 'vix': vix_now, 'days_below': days_below,
        }
    except Exception as e:
        log(f"regime 판단 실패 (boost 유지): {e}", level="WARN")
        return {'regime': 'boost', 'reason': 'regime 판단 실패',
                'spx': None, 'ma200': None, 'vix': None, 'days_below': 0}


_REGIME_STATE_PATH = Path(__file__).parent / 'regime_state.json'
_RISK_STATUS_CACHE = None


def _detect_regime_transition(current_regime):
    """이전 실행 regime과 비교해 전환 감지 (regime_state.json, GA가 commit해 영속).
    Returns: None | 'to_defense' | 'to_boost'."""
    import json
    prev = None
    try:
        if _REGIME_STATE_PATH.exists():
            prev = json.loads(_REGIME_STATE_PATH.read_text(encoding='utf-8')).get('regime')
    except Exception:
        pass
    transition = None
    if prev and prev != current_regime:
        transition = 'to_defense' if current_regime == 'defense' else 'to_boost'
    try:
        _REGIME_STATE_PATH.write_text(json.dumps({'regime': current_regime}), encoding='utf-8')
    except Exception:
        pass
    return transition


def get_market_risk_status():
    """시장 위험 통합 상태 (HY + VIX + Concordance + 국면 regime)

    Returns:
        dict {hy, vix, concordance, final_action, portfolio_mode, regime}
    """
    global _RISK_STATUS_CACHE
    if _RISK_STATUS_CACHE is not None:
        return _RISK_STATUS_CACHE
    hy = fetch_hy_quadrant()
    vix = fetch_vix_data()

    # Concordance Check
    hy_dir = 'warn' if hy and hy['quadrant'] in ('Q3', 'Q4') else 'stable'
    vix_dir = vix['direction'] if vix else 'stable'

    if hy_dir == 'warn' and vix_dir == 'warn':
        concordance = 'both_warn'
    elif hy_dir == 'warn' and vix_dir == 'stable':
        concordance = 'hy_only'
    elif hy_dir == 'stable' and vix_dir == 'warn':
        concordance = 'vix_only'
    else:
        concordance = 'both_stable'

    # Concordance 기반 행동 권장 (계절 × 지표 × q_days 조합, 30년 EDA 기반)
    if hy:
        q = hy['quadrant']
        q_days = hy.get('q_days', 1)
        vix_ok = vix_dir == 'stable'

        if q == 'Q1':
            # 봄(회복기) — 30년 평균: 연+14.3%
            if vix_ok:
                final_action = '회복 구간 (과거 30년 연 +14.3%)'
            else:
                final_action = '회복 구간, 변동성 높음'
        elif q == 'Q2':
            # 여름(성장기) — 30년 평균: 연+9.4%
            if vix_ok:
                final_action = '성장 구간 (과거 30년 연 +9.4%)'
            else:
                final_action = '성장 구간, 변동성 높음'
        elif q == 'Q3':
            # 가을(과열기) — 60일 기준 2단계
            if q_days < 60:
                if vix_ok:
                    final_action = f'과열 초기 ({q_days}일째)'
                else:
                    final_action = f'과열 초기 ({q_days}일째), 변동성 높음'
            else:
                if vix_ok:
                    final_action = f'과열 지속 ({q_days}일째)'
                else:
                    final_action = f'과열 지속 ({q_days}일째), 변동성 높음'
        else:
            # 겨울(Q4) — 20일/60일 기준 3단계
            if q_days <= 20:
                if vix_ok:
                    final_action = f'침체 초기 ({q_days}일째)'
                else:
                    final_action = f'⚠️ 침체 초기 ({q_days}일째), 변동성 높음'
            elif q_days <= 60:
                if vix_ok:
                    final_action = f'침체 지속 ({q_days}일째)'
                else:
                    final_action = f'⚠️ 침체 지속 ({q_days}일째), 변동성 높음'
            else:
                if vix_ok:
                    final_action = f'침체 후기 ({q_days}일째) — 회복 가능성'
                else:
                    final_action = f'침체 후기 ({q_days}일째), 변동성 높음'
    else:
        if vix and vix_dir == 'warn':
            final_action = '변동성 높음'
        else:
            final_action = ''

    # 국면 오버레이 (2026-05-27): S&P 200일선 이탈(15d) OR VIX>36(2d) → defense.
    #   defense 시 주식 매수 중단 + 채권ETF(IEF/BIL) 권장. 현재 강세장이면 normal.
    regime = get_market_regime()
    if regime:
        regime['transition'] = _detect_regime_transition(regime.get('regime', 'boost'))
    portfolio_mode = 'defense' if (regime and regime.get('regime') == 'defense') else 'normal'

    log(f"Concordance: {concordance} (q_days={hy.get('q_days', 'N/A') if hy else 'N/A'}) → {final_action} "
        f"[regime: {regime.get('regime') if regime else '?'} "
        f"({regime.get('transition') if regime else ''}) — {regime.get('reason') if regime else ''}] "
        f"[portfolio: {portfolio_mode}]")

    _RISK_STATUS_CACHE = {
        'hy': hy,
        'vix': vix,
        'concordance': concordance,
        'final_action': final_action,
        'portfolio_mode': portfolio_mode,
        'regime': regime,
    }
    return _RISK_STATUS_CACHE


def get_market_context(hist_all=None):
    """미국 시장 지수 컨텍스트 — hist_all에서 추출 (추가 HTTP 호출 없음)"""
    lines = []
    for symbol, name in [("^GSPC", "S&P 500"), ("^IXIC", "나스닥"), ("^DJI", "다우"), ("^RUT", "러셀")]:
        try:
            # hist_all에서 지수 데이터 추출 (yf.download에 포함)
            if hist_all is not None and 'Close' in hist_all.columns.get_level_values(0):
                try:
                    col = hist_all['Close'][symbol].dropna()
                    if len(col) >= 2:
                        close = float(col.iloc[-1])
                        prev = float(col.iloc[-2])
                        chg = (close / prev - 1) * 100
                        icon = "🟢" if chg > 0.5 else ("🔴" if chg < -0.5 else "🟡")
                        lines.append(f"{icon} {name}  {close:,.0f} ({chg:+.2f}%)")
                        continue
                except (KeyError, TypeError):
                    pass
            # fallback: 개별 호출
            import yfinance as yf
            hist = yf.Ticker(symbol).history(period='5d')
            if len(hist) >= 2:
                close = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2]
                chg = (close / prev - 1) * 100
                icon = "🟢" if chg > 0.5 else ("🔴" if chg < -0.5 else "🟡")
                lines.append(f"{icon} {name}  {close:,.0f} ({chg:+.2f}%)")
            else:
                log(f"시장 지수 {symbol}: 데이터 부족", "WARN")
        except Exception as e:
            log(f"시장 지수 {symbol} 수집 실패: {e}", "WARN")
            continue
    if not lines:
        log("시장 지수: 전부 수집 실패", "WARN")
    return lines


# ============================================================
# Git 자동 커밋
# ============================================================

def git_commit_push(config):
    """Git 자동 commit/push (GitHub Actions에서는 워크플로우가 처리)"""
    if not config.get('git_enabled', False):
        log("Git 동기화 비활성화됨")
        return False

    if config.get('is_github_actions', False):
        log("GitHub Actions 환경 — 워크플로우에서 Git 처리")
        return True

    log("Git commit/push 시작")

    try:
        today = datetime.now().strftime('%Y-%m-%d')

        subprocess.run(['git', 'add', '-A'], cwd=PROJECT_ROOT, check=True, capture_output=True)

        commit_msg = f"Daily update: {today}"
        result = subprocess.run(
            ['git', 'commit', '-m', commit_msg],
            cwd=PROJECT_ROOT, capture_output=True, text=True
        )

        if 'nothing to commit' in result.stdout or 'nothing to commit' in result.stderr:
            log("변경사항 없음, 커밋 스킵")
            return True

        remote = config.get('git_remote', 'origin')
        branch = config.get('git_branch', 'master')
        subprocess.run(['git', 'push', remote, branch], cwd=PROJECT_ROOT, check=True, capture_output=True)

        log("Git push 완료")
        return True

    except subprocess.CalledProcessError as e:
        log(f"Git 오류: {e}", "ERROR")
        return False


# ============================================================
# 텔레그램 메시지 생성
# ============================================================

def get_last_business_day():
    """가장 최근 미국 영업일 날짜"""
    if HAS_PYTZ:
        eastern = pytz.timezone('US/Eastern')
        now_et = datetime.now(eastern)
    else:
        now_et = datetime.now() - timedelta(hours=14)

    d = now_et.date()
    # 평일 장마감 후(16시 이후)면 오늘이 영업일
    if d.weekday() < 5 and now_et.hour >= 16:
        return d
    # 그 외: 전일로 가서 가장 최근 평일 찾기
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def get_today_kst():
    """오늘 날짜 (KST)"""
    if HAS_PYTZ:
        kst = pytz.timezone('Asia/Seoul')
        return datetime.now(kst).date()
    return datetime.now().date()

def create_system_log_message(stats, elapsed, config):
    """시스템 실행 로그 메시지 생성"""
    now = datetime.now()
    if HAS_PYTZ:
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.now(kst)
    time_str = now.strftime('%Y.%m.%d %H:%M')

    env = 'GitHub Actions' if config.get('is_github_actions') else 'Local'
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    collected = stats.get('total_collected', 0)
    universe = stats.get('universe', 0)
    err = stats.get('error_count', 0)

    lines = [f'🔧 <b>시스템 로그</b>']
    lines.append(f'{time_str} KST · {env}')

    # 수집 결과
    if err == 0:
        lines.append(f'\n✅ 수집 성공 ({collected}/{universe})')
    else:
        lines.append(f'\n⚠️ 수집 완료 ({collected}/{universe}, 실패 {err})')
        error_tickers = stats.get('error_tickers', [])
        if error_tickers:
            lines.append(f'실패: {", ".join(error_tickers[:10])}')

    # DB 데이터 범위
    try:
        conn = sqlite3.connect(config.get('db_path', 'eps_momentum_data.db'))
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date')
        dates = [r[0] for r in cur.fetchall()]
        cur.execute('SELECT COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL AND date=?',
                    (dates[-1],) if dates else ('',))
        ranked = cur.fetchone()[0] if dates else 0
        conn.close()
        if dates:
            lines.append(f'\n📂 DB: {dates[0]} ~ {dates[-1]} ({len(dates)}일)')
            exited = stats.get('exited_count', 0)
            lines.append(f'매수 후보: {ranked}개 / 이탈: {exited}개')
    except Exception:
        pass

    lines.append(f'\n⏱️ 소요: {minutes}분 {seconds}초')

    # 섹터 모멘텀 (개인봇 전용)
    sector_summary = stats.get('sector_summary', '')
    if sector_summary:
        lines.append(f'\n{sector_summary}')

    return '\n'.join(lines)

# 업종 대분류 매핑 (120개 → 15개) + 대표 ETF
SECTOR_GROUP = {
    '반도체': '반도체/HW', '반도체장비': '반도체/HW', '하드웨어': '반도체/HW',
    '전자부품': '반도체/HW', '전자유통': '반도체/HW', '가전': '반도체/HW',
    '통신장비': '반도체/HW', '계측기기': '반도체/HW',
    '응용SW': '소프트웨어', '인프라SW': '소프트웨어', 'IT서비스': '소프트웨어',
    '인터넷': '인터넷/플랫폼', '온라인유통': '인터넷/플랫폼', '게임': '인터넷/플랫폼',
    '엔터': '통신/미디어', '방송': '통신/미디어', '출판': '통신/미디어',
    '광고': '통신/미디어', '통신': '통신/미디어',
    '지역은행': '금융', '대형은행': '금융', '자산운용': '금융', '자본시장': '금융',
    '신용서비스': '금융', '금융데이터': '금융', '금융지주': '금융',
    '손해보험': '보험', '생명보험': '보험', '종합보험': '보험',
    '특수보험': '보험', '재보험': '보험', '보험중개': '보험',
    '의료기기': '헬스케어', '의료용품': '헬스케어', '의료시설': '헬스케어',
    '의약유통': '헬스케어', '진단연구': '헬스케어', '대형제약': '헬스케어',
    '특수제약': '헬스케어', '바이오': '헬스케어', '건강보험': '헬스케어',
    '의료정보': '헬스케어',
    '방산': '산업재', '산업기계': '산업재', '중장비': '산업재', '건설': '산업재',
    '건축자재': '산업재', '건자재': '산업재', '전기장비': '산업재', '공구': '산업재',
    '산업유통': '산업재', '비즈니스서비스': '산업재', '컨설팅': '산업재',
    '보안': '산업재', '폐기물': '산업재', '환경': '산업재', '복합기업': '산업재',
    '물류': '운송', '철도': '운송', '트럭운송': '운송', '항공': '운송',
    '해운': '운송', '렌탈리스': '운송',
    '자동차부품': '소비재(임의)', '자동차': '소비재(임의)', '자동차딜러': '소비재(임의)',
    '외식': '소비재(임의)', '전문소매': '소비재(임의)', '할인점': '소비재(임의)',
    '홈인테리어': '소비재(임의)', '의류소매': '소비재(임의)', '의류제조': '소비재(임의)',
    '백화점': '소비재(임의)', '신발잡화': '소비재(임의)', '명품': '소비재(임의)',
    '주택건설': '소비재(임의)', '가구가전': '소비재(임의)', '리조트카지노': '소비재(임의)',
    '도박': '소비재(임의)', '숙박': '소비재(임의)', '여행': '소비재(임의)',
    '레저차량': '소비재(임의)', '레저': '소비재(임의)', '생활서비스': '소비재(임의)',
    '식품': '소비재(필수)', '음료': '소비재(필수)', '맥주': '소비재(필수)',
    '주류': '소비재(필수)', '제과': '소비재(필수)', '생활용품': '소비재(필수)',
    '담배': '소비재(필수)', '식료품점': '소비재(필수)', '식품유통': '소비재(필수)',
    '교육': '소비재(필수)',
    '리츠특수': '리츠', '리츠주거': '리츠', '리츠소매': '리츠', '리츠산업': '리츠',
    '리츠의료': '리츠', '리츠오피스': '리츠', '리츠호텔': '리츠', '리츠모기지': '리츠',
    '리츠복합': '리츠', '부동산서비스': '리츠',
    '석유가스': '에너지', '석유미드스트림': '에너지', '석유장비': '에너지',
    '석유정제': '에너지', '석유종합': '에너지',
    '전력': '신재생/유틸', '가스': '신재생/유틸', '수도': '신재생/유틸',
    '유틸복합': '신재생/유틸', '독립발전': '신재생/유틸', '신재생': '신재생/유틸',
    '태양광': '신재생/유틸',
    '특수화학': '소재', '화학': '소재', '농업': '소재', '철강': '소재',
    '알루미늄': '소재', '구리': '소재', '금': '소재', '귀금속': '소재',
    '산업금속': '소재', '목재': '소재', '금속가공': '소재', '포장재': '소재',
    '농산물': '소재', '금속가공': '소재',
}

SECTOR_ETF = {
    '반도체/HW': 'SMH', '소프트웨어': 'IGV', '인터넷/플랫폼': 'SKYY',
    '통신/미디어': 'XLC', '금융': 'XLF', '보험': 'KIE',
    '헬스케어': 'XLV', '산업재': 'XLI', '운송': 'IYT',
    '소비재(임의)': 'XLY', '소비재(필수)': 'XLP', '리츠': 'VNQ',
    '에너지': 'XLE', '신재생/유틸': 'XLU', '소재': 'XLB',
}


def analyze_sector_momentum(results_df, today_str=None):
    """섹터별 EPS 모멘텀 분석 (개인봇 로그용)"""
    import pandas as pd
    from eps_momentum_system import INDUSTRY_MAP

    if results_df is None or results_df.empty:
        return ''

    df = results_df.copy()

    # 업종 → 대분류 매핑
    if 'industry' not in df.columns:
        return ''
    df['sector_group'] = df['industry'].map(
        lambda x: SECTOR_GROUP.get(x, '')
    )
    df = df[df['sector_group'].str.len() > 0].copy()
    if df.empty:
        return ''

    # 섹터별 집계
    threshold = 5
    sector = df.groupby('sector_group').agg(
        total=('ticker', 'count'),
        upward=('adj_score', lambda x: (x > threshold).sum()),
    ).reset_index()
    sector['pct'] = (sector['upward'] / sector['total'] * 100).round(0).astype(int)
    sector = sector[sector['total'] >= 5]  # 최소 5종목
    sector = sector.sort_values('pct', ascending=False)

    # 전주 대비 (5영업일 전)
    prev_pcts = {}
    if today_str:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            dates = [r[0] for r in c.execute(
                'SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC LIMIT 6'
            ).fetchall()]
            if len(dates) >= 6:
                prev_date = dates[5]
                prev_df = pd.read_sql_query(
                    'SELECT ticker, adj_score FROM ntm_screening WHERE date=? AND is_turnaround=0',
                    conn, params=(prev_date,)
                )
                prev_df['sector_group'] = prev_df['ticker'].map(
                    lambda t: SECTOR_GROUP.get(_get_cached_industry(t), '')
                )
                prev_df = prev_df[prev_df['sector_group'].str.len() > 0]
                if not prev_df.empty:
                    prev_sec = prev_df.groupby('sector_group').agg(
                        total=('ticker', 'count'),
                        upward=('adj_score', lambda x: (x > threshold).sum()),
                    ).reset_index()
                    prev_sec['pct'] = (prev_sec['upward'] / prev_sec['total'] * 100).round(0).astype(int)
                    prev_pcts = dict(zip(prev_sec['sector_group'], prev_sec['pct']))
            conn.close()
        except Exception as e:
            log(f"섹터 전주 비교 실패: {e}", "WARN")

    # 메시지 생성
    lines = ['📊 섹터 EPS 모멘텀']
    for _, r in sector.head(5).iterrows():
        name = r['sector_group']
        etf = SECTOR_ETF.get(name, '')
        pct = int(r['pct'])
        total = int(r['total'])
        upward = int(r['upward'])
        prev = prev_pcts.get(name)
        prev_str = f' (전주 {prev}%)' if prev is not None else ''
        etf_str = f'({etf})' if etf else ''
        lines.append(f'{name}{etf_str} {pct}% 상향 {upward}/{total}{prev_str}')

    return '\n'.join(lines)


def _get_cached_industry(ticker):
    """ticker_info_cache.json에서 industry 조회"""
    if not hasattr(_get_cached_industry, '_cache'):
        cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ticker_info_cache.json')
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                import json
                _get_cached_industry._cache = json.load(f)
        except Exception:
            _get_cached_industry._cache = {}
    return _get_cached_industry._cache.get(ticker, {}).get('industry', '')


def _get_prev_portfolio(today_str=None):
    """어제 포트폴리오 보유 종목 조회"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        if today_str:
            c.execute('''
                SELECT ticker FROM portfolio_log
                WHERE date = (SELECT MAX(date) FROM portfolio_log WHERE date < ?)
                AND action IN ('enter', 'hold')
            ''', (today_str,))
        else:
            c.execute('''
                SELECT ticker FROM portfolio_log
                WHERE date = (SELECT MAX(date) FROM portfolio_log)
                AND action IN ('enter', 'hold')
            ''')
        tickers = [r[0] for r in c.fetchall()]
        conn.close()
        return tickers
    except Exception:
        return []


def _recent_held_tickers(today_str=None, lookback=15, rank_thresh=10):
    """무상태 보유 추정 — 최근 lookback 거래일 내 part2_rank ≤ rank_thresh 경험 종목.

    v86e++ (2026-06-03): portfolio_log(log_portfolio_trades 미호출로 2026-03-05 동결)
    의존 제거. 데이터에서 직접 재구성 → freeze 불가능(무상태). carryover + watchlist
    공용 단일 소스 → 두 메가 메커니즘 영원히 일치. date <= today_str 로 PIT 안전.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if today_str:
            rds = [r[0] for r in cur.execute(
                'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL AND date <= ? ORDER BY date DESC LIMIT ?',
                (today_str, lookback))]
        else:
            rds = [r[0] for r in cur.execute(
                'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT ?',
                (lookback,))]
        out = set()
        if rds:
            ph = ','.join('?' * len(rds))
            for (tk,) in cur.execute(
                f'SELECT DISTINCT ticker FROM ntm_screening WHERE date IN ({ph}) AND part2_rank IS NOT NULL AND part2_rank <= ?',
                    rds + [rank_thresh]):
                out.add(tk)
        conn.close()
        return out
    except Exception as e:
        log(f"_recent_held_tickers 오류: {e}", "WARN")
        return set()


# v115 (2026-06-09): 휩쏘 보험밸브 — 하루 -10%+ 패닉 급락으로 MA12 깨진 첫날은 1일 매도 유예.
#   06-05 MU(-13%)/SNDK(-11%) 하루딥 휩쏘(MA12 깨짐→매도→직후 회복) 방어.
#   BT: 측정효과 0(알파 아님, 손해도 0), gap>=8% 임계 plateau, LOWO 양수. EPS꺾임 룰과 동일한
#   "무비용 보험" 논리(상승장이라 보험금 탈 일이 없었을 뿐). EPS꺾임(min_seg<-2) 즉시매도는 무관.
WHIPSAW_GUARD_GAP = -0.10

# v118 (2026-06-11): V118 전략 교체 (메가 carryover + entry, MA12 제거) = fresh start.
#   v116 epoch (06-09) → v118 epoch (06-11)로 갱신.
#   사용자: "오늘 전략 바꿔서 오늘 시작 고객"
#   → 보유/매도 표시는 이 날짜 이후 실제 진입만 집계(apply_epoch=True). 그 전엔 빈손.
#   성능(_get_system_performance)은 백테스트 track record라 epoch 미적용(전체 replay 유지).
HOLDINGS_EPOCH = '2026-06-11'

# v119 (2026-06-11): 제3방안 — 밸류에이션(fwd_PE) 저평가 보유. 메가 carryover(PEG<0.18)/MA12 전면 교체.
#   보유 규칙: EPS꺾임(min_seg<-2) 즉시매도 → 순위 10위 안이면 보유 → 10위 밖이면 fwd_PE<15(저평가)만 보유.
#   fwd_PE = price / ntm_current. SNDK(PE 9)·MU(10) 끝까지 저평가라 carryover, 비싸지면(PE>=15) 자동매도.
#   BT(research/auto_bt_v117_recheck.py): 전기간 +193%(v118 +168% 대비 +25p), MDD -21.9%(동일),
#     LOWO +55.7%, SNDK 끝까지보유. 매도규칙 후보(단순/MA12/제3) 중 수익·calmar·회전 최고.
#   PE<15 채택: PE<20과 수익 동일(+193%)이라 더 보수적(거품방어 강)인 15 선택. plateau 12~20.
#   ⚠️ 4개월 N=1, "비싸지면 매도"는 이 기간 미발동(SNDK 끝까지 쌈). MDD는 메타배분(80:20)으로 별도 관리.
PE_HOLD = 15.0


def _replay_holdings(before_date=None, return_detail=False, apply_epoch=False):
    """forward replay 보유 재구성 (v111 MA12-hold + v115 보험밸브, 무상태, BT==production).

    v111 (2026-06-03): PEG 메가홀드 → MA12 추세홀드로 전면 교체.
    규칙: 진입 part2_rank≤2 (빈 슬롯), MAX 2슬롯.
      보유 유지: rank≤10 OR (rank>10 이지만 가격>MA12, 상승추세 지속)
      이탈: min_seg<-2(EPS꺾임, v55~ 안전망) OR (rank>10 AND 가격≤MA12, 추세 붕괴)
      v115 보험밸브: 추세 붕괴라도 하루 -10%+ 패닉 급락 첫날은 1일 유예(휩쏘 방지)
      데이터 fetch 실패(가격 None)시 carryover (v113 robust 계승)
    근거: 모든 winner(MU/SNDK/STX/LITE)가 순위 밖에서도 상승 지속 → 추세(MA12)로 보유.
      BT: baseline 대비 +33p (100/100), 인접 MA10~15 고원, walk-forward 5/5, LOWO 무해.
      PEG(메가)보다 broad(STX 등 비메가 winner 포착) + 고객 설득력(추세 직관).

    return_detail=True 시 {ticker: (entry_date, entry_price)} 반환 (v115 보유 수익률 표시용).
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # 전체 가격 캘린더 (MA12용 — 순위 밖 종목도 가격 필요)
        alld = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
        didx = {d: i for i, d in enumerate(alld)}
        pxh = {}
        for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
            pxh.setdefault(tk, {})[d] = p
        def _ma12(tk, d):
            i = didx.get(d)
            if i is None or i - 11 < 0:
                return None
            v = [pxh.get(tk, {}).get(alld[j]) for j in range(i - 11, i + 1)]
            v = [x for x in v if x]
            return sum(v) / len(v) if len(v) >= 6 else None
        if before_date:
            dts = [r[0] for r in cur.execute(
                'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL AND date < ? ORDER BY date',
                (before_date,))]
        else:
            dts = [r[0] for r in cur.execute(
                'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
        if apply_epoch:
            # v116 fresh start: 배포일 이후 진입만 집계 (그 전 재구성 보유는 라이브 허구)
            dts = [d for d in dts if d >= HOLDINGS_EPOCH]
        port = set()
        entry_info = {}   # tk -> (entry_date, entry_price)  v115 보유 수익률 표시용
        grace = set()     # tk -> v115 보험밸브 1일 유예 중
        for d in dts:
            rows = cur.execute(
                'SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
                (d,)).fetchall()
            info = {}
            for tk, p2, nc, n7, n30, n60, n90 in rows:
                segs = []
                for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                    segs.append((a - b) / abs(b) * 100 if b and abs(b) > 0.01 else 0)
                info[tk] = dict(p2=p2, minseg=min(segs) if segs else 0,
                                nc=nc, price=pxh.get(tk, {}).get(d))
            # v119 (2026-06-11): 제3방안 — fwd_PE<15 저평가 보유 (메가 carryover/MA12 전면 교체)
            #   EPS꺾임(min_seg<-2) 즉시매도 → 10위 안 보유 → 10위 밖이면 PE<15만 보유.
            #   BT(auto_bt_v117_recheck.py): 전기간 +193% / MDD -21.9% / SNDK 끝까지보유.
            for tk in list(port):
                it = info.get(tk)
                if it is not None and it['minseg'] < -2:
                    port.discard(tk); entry_info.pop(tk, None); grace.discard(tk)
                    continue  # EPS 꺾임 = 즉시 매도
                if it is None:
                    continue  # 오늘 데이터 갭 → carryover (v113 robust)
                p2 = it['p2']
                if p2 is not None and p2 <= 10:
                    grace.discard(tk)
                    continue  # 10위 안 = 보유
                # 10위 밖: fwd_PE veto — 싸면(PE<15) 보유, 비싸면 매도 (BT 정합: 계산 불가 시 매도)
                _pe = (it['price'] / it['nc']) if (it['price'] and it['nc'] and it['nc'] > 0) else 999
                if _pe < PE_HOLD:
                    grace.discard(tk)
                    continue  # 저평가 → 보유
                port.discard(tk); entry_info.pop(tk, None); grace.discard(tk)  # 비싸짐 → 매도
            # 진입 v119: slot 1·2 모두 part2 Top (메가 전용 슬롯 제거 — BT 진입과 정합)
            if len(port) < 2:
                p2_sorted = sorted([(tk, it['p2']) for tk, it in info.items()
                                    if tk not in port and it['p2'] is not None and it['p2'] <= 5],
                                   key=lambda x: x[1])
                for tk, _ in p2_sorted:
                    if len(port) >= 2:
                        break
                    port.add(tk)
                    entry_info[tk] = (d, pxh.get(tk, {}).get(d))
        conn.close()
        if return_detail:
            return entry_info
        return port
    except Exception as e:
        log(f"_replay_holdings 오류: {e}", "WARN")
        return set()


def _above_ma12(ticker, today_str=None, n=12):
    """현재가 > MA12 (상승추세 유지) 여부 — v111 추세홀드 판정.
    데이터 fetch 실패(가격 부족)시 True 반환(carryover, v113 robust 계승)."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if today_str:
            rows = [r[0] for r in cur.execute(
                'SELECT price FROM ntm_screening WHERE ticker=? AND price IS NOT NULL AND date<=? ORDER BY date DESC LIMIT ?',
                (ticker, today_str, n))]
        else:
            rows = [r[0] for r in cur.execute(
                'SELECT price FROM ntm_screening WHERE ticker=? AND price IS NOT NULL ORDER BY date DESC LIMIT ?',
                (ticker, n))]
        conn.close()
        if len(rows) < 6:
            return True  # 데이터 부족(갭) → carryover 유지
        cp = rows[0]
        m = sum(rows) / len(rows)
        return cp > m
    except Exception as e:
        log(f"_above_ma12 {ticker} 오류: {e}", "WARN")
        return True


def _below_pe_live(ticker, today_str=None):
    """v119 제3방안: fwd_PE = price/ntm_current < PE_HOLD(15) → 저평가(보유).
    데이터 없으면 False(매도쪽 — BT 정합 pe=999 취급)."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if today_str:
            r = cur.execute('SELECT price,ntm_current FROM ntm_screening WHERE ticker=? AND price IS NOT NULL AND date<=? ORDER BY date DESC LIMIT 1', (ticker, today_str)).fetchone()
        else:
            r = cur.execute('SELECT price,ntm_current FROM ntm_screening WHERE ticker=? AND price IS NOT NULL ORDER BY date DESC LIMIT 1', (ticker,)).fetchone()
        conn.close()
        if not r or not r[0] or not r[1] or r[1] <= 0:
            return False
        return (r[0] / r[1]) < PE_HOLD
    except Exception as e:
        log(f"_below_pe_live {ticker} 오류: {e}", "WARN")
        return False


def _live_pe(ticker, today_str=None):
    """v119: 최신 fwd_PE 값 (표시/사유용). 데이터 없으면 None."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if today_str:
            r = cur.execute('SELECT price,ntm_current FROM ntm_screening WHERE ticker=? AND price IS NOT NULL AND date<=? ORDER BY date DESC LIMIT 1', (ticker, today_str)).fetchone()
        else:
            r = cur.execute('SELECT price,ntm_current FROM ntm_screening WHERE ticker=? AND price IS NOT NULL ORDER BY date DESC LIMIT 1', (ticker,)).fetchone()
        conn.close()
        if not r or not r[0] or not r[1] or r[1] <= 0:
            return None
        return r[0] / r[1]
    except Exception:
        return None


def _today_gap(ticker, today_str=None):
    """오늘(또는 최신) 1거래일 수익률 — v115 보험밸브용. 데이터 부족 시 None.
    하루 -10%+ 패닉 급락(휩쏘 신호) 판정에 사용."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if today_str:
            rows = [r[0] for r in cur.execute(
                'SELECT price FROM ntm_screening WHERE ticker=? AND price IS NOT NULL AND date<=? ORDER BY date DESC LIMIT 2',
                (ticker, today_str))]
        else:
            rows = [r[0] for r in cur.execute(
                'SELECT price FROM ntm_screening WHERE ticker=? AND price IS NOT NULL ORDER BY date DESC LIMIT 2',
                (ticker,))]
        conn.close()
        if len(rows) < 2 or not rows[1]:
            return None
        return rows[0] / rows[1] - 1
    except Exception as e:
        log(f"_today_gap {ticker} 오류: {e}", "WARN")
        return None


def _build_portfolio_entry(row, status_map, earnings_map):
    """포트폴리오 종목 entry dict 생성"""
    t = row.get('ticker', '')
    today_date = datetime.now().date()
    two_weeks = (datetime.now() + timedelta(days=14)).date()
    earnings_note = ""
    if earnings_map:
        ed_info = earnings_map.get(t)
        if ed_info:
            ed = ed_info['date']
            if today_date <= ed <= two_weeks:
                ah_tag = '(장후)' if ed_info['after_hours'] else ''
                earnings_note = f" 📅{ed.month}/{ed.day}{ah_tag}"
    return {
        'ticker': t,
        'name': row.get('short_name', t),
        'industry': row.get('industry', ''),
        'eps_chg': _safe_float(row.get('eps_change_90d')),
        'price_chg': _safe_float(row.get('price_chg')),
        'fwd_pe': _safe_float(row.get('fwd_pe')),
        'adj_gap': _safe_float(row.get('adj_gap')),
        'rev_up': int(row.get('rev_up30', 0) or 0),
        'rev_down': int(row.get('rev_down30', 0) or 0),
        'num_analysts': int(row.get('num_analysts', 0) or 0),
        'adj_score': _safe_float(row.get('adj_score')),
        'lights': row.get('trend_lights', ''),
        'desc': row.get('trend_desc', ''),
        'v_status': (status_map or {}).get(t, '✅'),
        'price': _safe_float(row.get('price')),
        'rev_growth': _safe_float(row.get('rev_growth')),
        'earnings_note': earnings_note,
    }


# v117 (2026-06-09): 거래량 필터 캐시 — 매수 후보 종목 거래대금 ($M) 메모리 캐시
# yfinance HTTP 호출 비용 최소화 (당일 cron 내 종목당 1회만)
_volume_dollar_cache = {}


def _get_avg_dollar_volume_M(ticker, hist_all=None, target_date=None):
    """일평균 거래대금 ($M) — point-in-time (직전 30일 평균).

    v117 (2026-06-09): 시장 주도주 universe filter용.
    BT 검증: v114 + $1B+ → calmar 5.93 (+14.5p, 양수 300/300)

    조회 우선순위:
      1. DB ntm_screening.dollar_volume_30d (BT==production 정합)
      2. hist_all (cron이 이미 fetch한 가격 history)
      3. yfinance info (fallback)
    """
    if ticker in _volume_dollar_cache:
        return _volume_dollar_cache[ticker]
    try:
        # 1) DB 조회 — target_date 또는 마지막 가용
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        if target_date:
            row = cursor.execute(
                'SELECT dollar_volume_30d FROM ntm_screening WHERE ticker=? AND date<=? AND dollar_volume_30d IS NOT NULL ORDER BY date DESC LIMIT 1',
                (ticker, target_date)).fetchone()
        else:
            row = cursor.execute(
                'SELECT dollar_volume_30d FROM ntm_screening WHERE ticker=? AND dollar_volume_30d IS NOT NULL ORDER BY date DESC LIMIT 1',
                (ticker,)).fetchone()
        conn.close()
        if row and row[0] is not None:
            v_M = float(row[0])
            _volume_dollar_cache[ticker] = v_M
            return v_M
        # 2) hist_all fallback
        if hist_all is not None and 'Close' in hist_all.columns.get_level_values(0):
            try:
                close = hist_all['Close'].get(ticker)
                volume = hist_all['Volume'].get(ticker) if 'Volume' in hist_all.columns.get_level_values(0) else None
                if close is not None and volume is not None:
                    df = (close * volume).dropna().tail(30)
                    if len(df) >= 5:
                        v_M = df.mean() / 1e6
                        _volume_dollar_cache[ticker] = v_M
                        return v_M
            except Exception:
                pass
        # 3) yfinance fallback
        import yfinance as yf
        info = yf.Ticker(ticker).info
        avg_vol = info.get('averageDailyVolume3Month') or info.get('averageVolume', 0) or 0
        price = info.get('currentPrice') or info.get('regularMarketPrice', 0) or 0
        v_M = avg_vol * price / 1e6
        _volume_dollar_cache[ticker] = v_M
        return v_M
    except Exception as e:
        log(f"_get_avg_dollar_volume_M {ticker} 오류: {e}", "WARN")
        _volume_dollar_cache[ticker] = 0
        return 0


def select_display_top5(results_df, status_map=None, weighted_ranks=None,
                        earnings_map=None, risk_status=None, score_100_map=None,
                        hist_all=None, today_str=None):
    """Signal 메시지용 종목 선정 (w_gap 순위 Top2 + min_seg ≥ 0%, 최대 2종목)

    part2_rank(w_gap 기반) 상위 2종목 중 EPS 추세 건강(min_seg ≥ 0%) 종목만 진입.
    이탈선: part2_rank > 10. 최대 2슬롯. (v82: 3→2)
    비중 (v84): score_100 1-2위 격차 기반 dynamic (2step_t15)
      - gap ≥ 15: 1위 100%, 2위 0% (slot 2 skip, 1종목만 매수)
      - gap < 15: 1위 50%, 2위 50% (1-2위 거의 동등시 분산)
    """
    if earnings_map is None:
        earnings_map = {}
    if status_map is None:
        status_map = {}
    if weighted_ranks is None:
        weighted_ranks = {}

    portfolio_mode = risk_status.get('portfolio_mode', 'normal') if risk_status else 'normal'

    if results_df is None or results_df.empty:
        return []

    if portfolio_mode in ('stop', 'defense'):
        return []

    all_eligible = get_part2_candidates(results_df, top_n=None)
    if all_eligible.empty:
        return []

    # min_seg < -2% 제외 — save_part2_ranks()와 동일 기준 (순위 부여 전 필터)
    def _calc_min_seg(row):
        segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
        return min(segs) if segs else 0
    all_eligible = all_eligible[all_eligible.apply(_calc_min_seg, axis=1) >= -2].copy()

    # v117c (2026-06-10): candidates 정렬 — DB의 part2_rank 사용 (BT 정합)
    # 기존 score_100_map 정렬 → BT(DB.part2_rank)와 불일치 → 6/09 VRT(p2=3) 누락 사고
    # save_part2_ranks가 DB에 저장한 part2_rank 그대로 사용 → BT==production 정합
    candidates = all_eligible.copy()
    # DB에서 part2_rank 직접 조회 (today_str 기준)
    try:
        _conn = sqlite3.connect(DB_PATH)
        _p2_rank_db = dict(_conn.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (today_str,)).fetchall())
        _conn.close()
    except Exception as _e:
        _p2_rank_db = {}
        log(f"part2_rank DB 조회 오류: {_e}", "WARN")
    if _p2_rank_db:
        candidates['_p2_rank'] = candidates['ticker'].map(lambda t: _p2_rank_db.get(t, 9999))
        candidates = candidates.sort_values('_p2_rank', ascending=True).reset_index(drop=True)
        p2r_map = dict(_p2_rank_db)  # DB 그대로
    elif score_100_map:
        # fallback: score_100_map (DB part2_rank 없을 때)
        candidates['_wgap'] = candidates['ticker'].map(lambda t: score_100_map.get(t, 0))
        candidates = candidates.sort_values('_wgap', ascending=False).reset_index(drop=True)
        p2r_map = {row['ticker']: i + 1 for i, (_, row) in enumerate(candidates.iterrows())}
    else:
        candidates = candidates.sort_values('adj_gap', ascending=True).reset_index(drop=True)
        p2r_map = {row['ticker']: i + 1 for i, (_, row) in enumerate(candidates.iterrows())}

    top_debug = [(row['ticker'], p2r_map.get(row['ticker'], 999),
                  round(float(score_100_map.get(row['ticker'], 0)) if score_100_map else float(row.get('adj_gap', 0) or 0), 1))
                 for _, row in candidates.head(7).iterrows()]
    log(f"w_gap 순위 상위 7: {top_debug}")

    # v83.3 (2026-05-28): 슬롯 (2,10,2) + 1위 90%/2위 10% (v83.2 80/20 → 90/10).
    # v83.2 (2026-05-27): C2 boost 제거.
    #   boost edge가 MU/SNDK 단일 종목 착시로 판명 (leave-one-superwinner-out 검증) →
    #   part2_rank = 순수 w_gap 순위. (candidates 정렬에 boost 없음)
    MAX_SLOTS = 2
    selected = []

    # v111 (2026-06-03): MA12 추세홀드 — PEG 메가홀드/v110 mega_score 전면 교체.
    # 진입: part2_rank Top (EPS 상향 + 저평가 = 시스템 핵심 철학).
    # 보유: 순위 10위 밖이어도 가격>MA12(상승추세 지속)면 유지 → "일찍 안 팔기".
    # 매도: min_seg<-2(EPS꺾임) OR 가격≤MA12(추세 붕괴).
    # 근거: 모든 winner(MU/SNDK/STX/LITE)가 순위 밖에서도 상승 지속 → 추세로 보유.
    #   BT baseline 대비 +33p(100/100), 인접 MA10~15 고원, walk-forward 5/5, LOWO 무해(STX 등
    #   비메가 winner 포착 — PEG보다 broad). 재점검: 슬롯2 / 50-50(LOWO robust) / exit10 확정.
    #   PEG<0.18(메가)보다 고객 설득력 우월("상승추세면 보유, 깨지면 매도" 직관).
    trend_held = []
    prev_held = _replay_holdings(today_str, apply_epoch=True)  # v116 fresh start (배포일 이후 실제 보유만)
    held_detail = _replay_holdings(today_str, return_detail=True, apply_epoch=True)  # v115 진입가(수익률 표시)
    if prev_held:
        cand_by_tk = {row['ticker']: row for _, row in candidates.iterrows()}
        def _cur_rank(t):
            r = cand_by_tk.get(t)
            v = r.get('part2_rank') if r is not None else None
            return v if v is not None else 9999
        for t in sorted(prev_held, key=_cur_rank):
            if len(selected) >= MAX_SLOTS:
                break
            row = cand_by_tk.get(t)
            if row is None:
                row = _fetch_last_full_row(t, today_str)  # 순위밖/데이터갭 — 마지막 가용 데이터
                if row is None:
                    continue
            segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
            if segs and min(segs) < -2:
                log(f"  🔓 저평가보유 해제 {t}: EPS 꺾임(min_seg<-2) → 매도")
                continue
            # v119 제3방안: 10위 밖이면 fwd_PE veto (10위 안이면 어차피 보유, PE 무관)
            _p2 = _cur_rank(t)
            _price = _safe_float(row.get('price')); _nc = _safe_float(row.get('ntm_current'))
            _pe = (_price / _nc) if (_price and _nc and _nc > 0) else 999
            if _p2 > 10 and _pe >= PE_HOLD:
                log(f"  🔓 저평가보유 해제 {t}: fwd_PE {_pe:.1f} ≥ {PE_HOLD:.0f} (비싸짐) → 매도")
                continue
            entry = _build_portfolio_entry(row, status_map, earnings_map)
            entry['_trend_hold'] = True
            entry['_hold_pe'] = _pe  # v119 저평가 보유 PE 표시용
            # v115 보유 수익률 (진입가 대비) — 표시용
            if held_detail and t in held_detail:
                _ed, _ep = held_detail[t]
                _cp = _safe_float(row.get('price'))
                if _ep and _cp:
                    entry['_hold_return'] = _cp / _ep - 1
            selected.append(entry)
            trend_held.append(t)
            log(f"  💎 저평가 보유 {t}: 순위 밀려도 fwd_PE {_pe:.1f}<{PE_HOLD:.0f} (w_rank={p2r_map.get(t, '?')})")

    # v110 (2026-06-03): "각 분야 1등 사는" 시스템
    #   슬롯 1: part2_rank Top 1 (mean reversion 신호 1위)
    #   슬롯 2: mega_score Top 1 메가 (PEG<0.25 + 매출≥25%) — 별도 entry 기준
    #   메가 없을 때: 슬롯 1 단독 100% (BT V110a: +198% > V110b +132%)
    #   메가 carryover (mega_held)는 이미 위에서 슬롯 차지
    def _entry_pass(row, t):
        """진입 필터 — V110/V113 공통 (v71/v58/하향/저커버리지)"""
        status = status_map.get(t, '🆕')
        if status != '✅':
            log(f"  ⏳ 제외 {t}: 검증 미완료 ({status})")
            return False
        segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
        if round(min(segs) if segs else 0, 1) < 0:
            log(f"  ⛔ 제외 {t}: min_seg<0")
            return False
        rev_up = int(row.get('rev_up30', 0) or 0)
        rev_down = int(row.get('rev_down30', 0) or 0)
        num_analysts = int(row.get('num_analysts', 0) or 0)
        total_rev = rev_up + rev_down
        if total_rev > 0 and rev_down / total_rev > 0.3:
            log(f"  ⛔ 제외 {t}: 하향과반")
            return False
        if rev_down >= rev_up and rev_down >= 2:
            log(f"  ⛔ 제외 {t}: 하향우세")
            return False
        if num_analysts < 3:
            log(f"  ⛔ 제외 {t}: 저커버리지")
            return False
        # v117 (2026-06-09): 거래량 universe filter — 일평균 거래대금 ≥ $1B
        # BT (auto_bt_v114_plus_volume.py): calmar 5.70 → 5.93, +14.5p, 양수 300/300
        # 비주도주 (AEIS $456M, KEYS $490M, HWM $595M) 차단 → 시장 주도주만 매수
        avg_dv_M = _get_avg_dollar_volume_M(t, hist_all)
        if avg_dv_M < 1000:
            log(f"  ⛔ 제외 {t}: 거래대금 ${avg_dv_M:,.0f}M < $1B (저거래량 비주도주)")
            return False
        return True

    # v119 제3방안: slot 1·2 모두 part2 Top (저평가+EPS상향 1·2위). 메가 전용 슬롯 제거.
    #   진입은 순위 기반(part2 Top ≤3), 보유는 fwd_PE<15 veto (위 carryover에서 처리).
    #   BT(auto_bt_v117_recheck.py): 전기간 +193% / SNDK·MU carryover.
    for _, row in candidates.iterrows():
        if len(selected) >= MAX_SLOTS:
            break
        t = row['ticker']
        p2_rank = p2r_map.get(t, 999)
        if p2_rank > 5:
            break
        if t in trend_held or any(s['ticker'] == t for s in selected):
            continue
        if not _entry_pass(row, t):
            continue
        entry = _build_portfolio_entry(row, status_map, earnings_map)
        entry['_entry_type'] = 'new_mr'  # mean reversion (part2 Top)
        selected.append(entry)
        log(f"  ✅ slot {len(selected)} (part2 Top) {t}: rank {p2_rank}")

    # v110 (2026-06-03): 50/50 고정 (각 분야 1등 entry, score 격차 무관 메가 비중 보존)
    #   슬롯 1 (part2) + 슬롯 2 (mega) 둘 다 차면 → 50/50
    #   슬롯 1만 (메가 부재) → 100%
    # BT calmar: V110a 9.09 (전체) — 단일 신호 1종목 100%가 절대 수익 + calmar 둘 다 best
    n = len(selected)
    if n == 1:
        selected[0]['weight'] = 100
        log(f"v111 비중: 1종목 100% ({selected[0]['ticker']})")
    elif n >= 2:
        selected[0]['weight'] = 50
        selected[1]['weight'] = 50
        for i in range(2, n):
            selected[i]['weight'] = 0
        log(f"v111 비중: 50/50 ({selected[0]['ticker']}, {selected[1]['ticker']})")

    log(f"디스플레이 {n}종목: " + ", ".join(f"{s['ticker']}({s['weight']}%)" for s in selected))

    # v119 제3방안: 신규 진입자용 매수후보 — slot 1·2 모두 part2 Top (메가 슬롯 제거)
    new_buy_top2 = []
    for _, row in candidates.iterrows():
        if len(new_buy_top2) >= 2:
            break
        t = row['ticker']
        p2_rank = p2r_map.get(t, 999)
        if p2_rank > 5:
            break
        if any(s['ticker'] == t for s in new_buy_top2):
            continue
        if not _entry_pass(row, t):
            continue
        entry = _build_portfolio_entry(row, status_map, earnings_map)
        entry['_entry_type'] = 'new_mr'
        new_buy_top2.append(entry)
    nb_n = len(new_buy_top2)
    if nb_n == 1:
        new_buy_top2[0]['weight'] = 100
    elif nb_n >= 2:
        new_buy_top2[0]['weight'] = 50
        new_buy_top2[1]['weight'] = 50

    return selected, new_buy_top2


def select_portfolio_stocks(results_df, status_map=None, weighted_ranks=None,
                            earnings_map=None, risk_status=None, today_str=None):
    """포트폴리오 종목 선정 — v72 (미사용, 참조용)

    전략:
    - 진입: w_gap 순위 Top5, min_seg ≥ 0%, 최대 3종목
    - 이탈: part2_rank > 11 (w_gap 기준) / min_seg < -2% / breakout hold 조건 시 2일 유예
    - 최대 3종목, 동일 비중

    Returns: (selected, portfolio_mode, concordance, final_action)
    """
    if earnings_map is None:
        earnings_map = {}
    if status_map is None:
        status_map = {}
    if weighted_ranks is None:
        weighted_ranks = {}

    concordance = risk_status.get('concordance', 'both_stable') if risk_status else 'both_stable'
    final_action = risk_status.get('final_action', '') if risk_status else ''
    portfolio_mode = risk_status.get('portfolio_mode', 'normal') if risk_status else 'normal'

    if results_df is None or results_df.empty:
        return [], portfolio_mode, concordance, final_action

    # stop/defense 모드: 빈 리스트 반환 (defense=약세장 방어, 주식 매수 중단)
    if portfolio_mode in ('stop', 'defense'):
        log(f"포트폴리오: portfolio_mode={portfolio_mode} → 추천 중단 ({final_action})")
        return [], portfolio_mode, concordance, final_action

    # Top 30 (하드 필터 적용, ✅ 필터 전) — Watchlist용 유지, 이탈은 Top 12 기준
    top30 = get_part2_candidates(results_df, top_n=30)
    if top30.empty:
        return [], portfolio_mode, concordance, final_action

    top11_tickers = set(top30.head(8)['ticker'].tolist())  # v78: X11→X8

    # ── 어제 보유 → Top 11 유지 시 홀드 (v74: 미사용, 참조용) ──
    prev_holdings = _get_prev_portfolio(today_str)

    # 1차 이탈: Top 11 밖
    holds_in_top20 = [t for t in prev_holdings if t in top11_tickers]
    exited_rank = [t for t in prev_holdings if t not in top11_tickers]
    if exited_rank:
        log(f"  📤 Top11 이탈: {', '.join(exited_rank)}")

    # 2차 이탈: min_seg < -2% (EPS 건강도 악화)
    hold_entries = []
    exited_health = []
    for t in holds_in_top20:
        row = top30[top30['ticker'] == t]
        if row.empty:
            continue
        row = row.iloc[0]
        # min_seg 계산
        segs = []
        for col in ['seg1', 'seg2', 'seg3', 'seg4']:
            val = row.get(col)
            segs.append(float(val) if val is not None else 0)
        min_seg = min(segs) if segs else 0
        if min_seg < -2:
            exited_health.append(f"{t}(min_seg={min_seg:.1f}%)")
            log(f"  📤 {t}: 건강도 이탈 (min_seg={min_seg:.1f}%)")
            continue
        entry = _build_portfolio_entry(row, status_map, earnings_map)
        hold_entries.append(entry)
        log(f"  🔄 {t}: HOLD (Top15 유지) gap={_safe_float(row.get('adj_gap')):+.1f} desc={row.get('trend_desc', '')}")

    if exited_health:
        log(f"  📤 건강도 이탈: {', '.join(exited_health)}")

    # ── 신규 진입 후보 (✅ + 리스크 필터) ──
    verified_tickers = {t for t, s in status_map.items() if s == '✅'} if status_map else set()

    max_stocks = 3  # v72: 최대 3종목 (미사용 함수)
    vacancies = max(0, max_stocks - len(hold_entries))

    new_entries = []
    if vacancies > 0:
        held_tickers = {h['ticker'] for h in hold_entries}
        candidates = top30[
            top30['ticker'].isin(verified_tickers) &
            ~top30['ticker'].isin(held_tickers)
        ].copy()

        # 가중 순위 정렬
        if weighted_ranks:
            candidates['_weighted'] = candidates['ticker'].map(
                lambda t: weighted_ranks.get(t, {}).get('weighted', 50.0)
            )
            candidates = candidates.sort_values('_weighted').reset_index(drop=True)

        log(f"포트폴리오: 빈 자리 {vacancies}개, 신규 후보 {len(candidates)}개 검토 중...")

        for _, row in candidates.iterrows():
            if len(new_entries) >= vacancies:
                break
            t = row['ticker']
            # 리스크 필터 (신규만 적용)
            rev_up = int(row.get('rev_up30', 0) or 0)
            rev_down = int(row.get('rev_down30', 0) or 0)
            num_analysts = int(row.get('num_analysts', 0) or 0)
            flags = []
            total_rev = rev_up + rev_down
            if total_rev > 0 and rev_down / total_rev > 0.3:
                flags.append("하향과반")
            elif rev_down >= rev_up and rev_down >= 2:
                flags.append("하향우세")
            if num_analysts < 3:
                flags.append("저커버리지")

            if flags:
                log(f"  ❌ {t}: {','.join(flags)} (gap={_safe_float(row.get('adj_gap')):+.1f} desc={row.get('trend_desc', '')})")
            else:
                entry = _build_portfolio_entry(row, status_map, earnings_map)
                new_entries.append(entry)
                log(f"  🆕 {t}: NEW gap={_safe_float(row.get('adj_gap')):+.1f} desc={row.get('trend_desc', '')}")

    # L3: both_warn 시 신규 진입만 제외 (보유는 유지)
    if concordance == 'both_warn' and new_entries:
        log(f"L3 시장 동결: both_warn — 신규 진입 {len(new_entries)}개 제외 (보유 {len(hold_entries)}개 유지)")
        new_entries = []

    selected = hold_entries + new_entries

    if not selected:
        log("포트폴리오: 보유+신규 종목 없음", "WARN")
        return [], portfolio_mode, concordance, final_action

    if len(selected) < 1:
        log(f"포트폴리오: 선정 종목 없음", "WARN")
        return [], portfolio_mode, concordance, final_action

    # 가중 순위로 정렬 (표시 순서)
    if weighted_ranks:
        for s in selected:
            s['_weighted'] = weighted_ranks.get(s['ticker'], {}).get('weighted', 50.0)
        selected.sort(key=lambda x: x['_weighted'])

    log("포트폴리오: 가중 순위 (T0×0.5 + T1×0.3 + T2×0.2):")
    for i, s in enumerate(selected):
        w = s.get('_weighted', '-')
        tag = '🔄' if s['ticker'] in holds_in_top20 else '🆕'
        log(f"    {i+1}. {tag} {s['ticker']}: 가중={w} gap={s['adj_gap']:+.1f} adj={s['adj_score']:.1f} {s['desc']} [{s['industry']}]")

    # 동일 비중
    n = len(selected)
    base = 100 // n
    remainder = 100 - base * n
    weights = [base] * n
    for i in range(remainder):
        weights[i] += 1
    for i, s in enumerate(selected):
        s['weight'] = weights[i]

    log(f"포트폴리오: {n}종목 (보유 {len(hold_entries)} + 신규 {len(new_entries)}) — " +
        ", ".join(f"{s['ticker']}({s['weight']}%)" for s in selected))

    return selected, portfolio_mode, concordance, final_action

# ============================================================
# Breakout Hold (이탈 유예) — v74
# ============================================================

def check_breakout_hold(ticker):
    """이탈 유예 조건 체크 (strict)

    조건 (모두 만족):
      1. 최근 20거래일 종가 +25% 이상
      2. ntm_90d → ntm_current 순방향 (EPS 동행)
      3. rev_up30 / num_analysts >= 0.4
      4. 현재가 > MA60

    Returns: True if hold (이탈 유예), False otherwise

    백테스트 검증 (v74): 평균 +5.4%p 향상, MDD 악화 없음 (33일 multistart).
    실제 운영에서는 사용자 참고용으로 표시 (자동 보유 추적은 미구현).
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 최근 21일 가격 + 오늘 NTM/애널/MA60
        cursor.execute('''
            SELECT date, price, ma60, ntm_current, ntm_90d, rev_up30, num_analysts
            FROM ntm_screening WHERE ticker=? ORDER BY date DESC LIMIT 21
        ''', (ticker,))
        rows = cursor.fetchall()
        conn.close()

        if len(rows) < 21:
            return False

        today_row = rows[0]
        past_row = rows[20]
        today_price = today_row[1]
        past_price = past_row[1]

        if not today_price or not past_price or past_price <= 0:
            return False

        # 1. 20일 가격 +25%
        price_chg_20d = (today_price - past_price) / past_price * 100
        if price_chg_20d < 25:
            return False

        # 2. ntm_90d → ntm_current 순방향
        nc = today_row[3]
        n90 = today_row[4]
        if not nc or not n90 or n90 <= 0 or nc <= n90:
            return False

        # 3. rev_up30 / num_analysts >= 0.4
        rev_up = today_row[5] or 0
        num_an = today_row[6] or 0
        if num_an < 1 or (rev_up / num_an) < 0.4:
            return False

        # 4. price > MA60
        ma60 = today_row[2]
        if not ma60 or today_price <= ma60:
            return False

        return True
    except Exception as e:
        log(f"check_breakout_hold {ticker} 오류: {e}", "WARN")
        return False


def _fetch_last_full_row(ticker, before_date=None):
    """Ticker의 마지막 ntm_screening 전체 row (before_date 이전, ntm_current 있는).

    v113 (2026-06-03): MU 5/28-5/29 cron 부분 fetch 실패 사고 fix.
    Part 2 풀 밖(composite_rank>30)이거나 데이터 누락 시 메가 carryover용 fallback.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        if before_date:
            cur.execute('''
                SELECT * FROM ntm_screening
                WHERE ticker=? AND date<=? AND ntm_current IS NOT NULL
                ORDER BY date DESC LIMIT 1
            ''', (ticker, before_date))
        else:
            cur.execute('''
                SELECT * FROM ntm_screening
                WHERE ticker=? AND ntm_current IS NOT NULL
                ORDER BY date DESC LIMIT 1
            ''', (ticker,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        # seg1~4 산정 (NTM 변화율, _calc_seg와 동일 로직)
        def _seg(a, b):
            if b is not None and abs(b) > 0.01:
                return max(-100.0, min(100.0, (a - b) / abs(b) * 100))
            return 0.0
        nc, n7, n30, n60, n90 = (float(d.get(k) or 0) for k in ('ntm_current','ntm_7d','ntm_30d','ntm_60d','ntm_90d'))
        d['seg1'] = _seg(nc, n7)
        d['seg2'] = _seg(n7, n30)
        d['seg3'] = _seg(n30, n60)
        d['seg4'] = _seg(n60, n90)
        return d
    except Exception as e:
        log(f"_fetch_last_full_row {ticker} 오류: {e}", "WARN")
        return None


def check_mega_hold(ticker):
    """Regime detector — EPS revision regime 판별 (v110, 2026-06-03)

    시스템 본질 = mean reversion (가격 vs PE 변화율 미스프라이싱).
    Mean reversion regime (대다수): adj_gap/w_gap/part2_rank로 자연 정렬.
    EPS revision regime (메가): EPS 폭발로 가격 영구 압도, PEG가 valuation 척도.

    v110b Regime 분기 조건 (둘 다 만족):
      1. PEG = (price/ntm_current) / (rev_growth×100) < 0.18 (저평가, 엄격)
      2. rev_growth ≥ 25% (매출 성장 둔화 아님)

    v110b 변경 사유: PEG 0.25 → 0.18 (UMBF 같은 가짜 메가 제외)
    - UMBF PEG 0.199 > 0.18 → 메가 진입 차단 (매출 49% but PEG 약함)
    - SNDK PEG 0.041, MU PEG 0.058 < 0.18 → "진짜 메가"만 통과
    - BT plateau 0.18~0.30 robust, 0.18이 calmar 9.34 미세 best

    Returns: True면 EPS revision regime → 순위 10위 밖이어도 보유 (regime-specific logic)

    BT 자율주행 입증 (V87/V88/V89/V90/V91 7 phase + 전문가 sub-agent):
      - 시스템 본질 재설계 모든 시도 실패 (V87 -57~-103p, V88 -167p, V89 -143p, V91 -32~-46p)
      - 단일 adj_gap으로 두 regime 동시 처리 불가능 (mathematical impossibility, 75일 N=2)
      - regime 분리(V86e+)가 베이지안 정보 가중으로 정당
      - BT 100×3 paired: +92.5p / 100/100 wins / LOWO -MU-SNDK +14.7p (95/100)
      - plateau: PEG 0.22~0.30 × rev_exit 0.25~0.30
    매도 트리거 (select_display_top5):
      1. min_seg<-2 (EPS 꺾임 = regime exit)
      2. rev_growth<0.25 (매출 성장 둔화 = regime exit)
    ⚠️ 75일 단일 상승장 검증. N=2(MU/SNDK) 메가 반전 미검증.
    research: research/auto_bt_v90_peg_rev_grid.py, V87_V88_V89_AUTONOMOUS_REPORT_2026_06_02.md
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT price, ntm_current, rev_growth
            FROM ntm_screening WHERE ticker=?
            AND date=(SELECT MAX(date) FROM ntm_screening WHERE ticker=? AND ntm_current IS NOT NULL)
        ''', (ticker, ticker))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return False
        price, nc, rg = row
        if not price or not nc or nc <= 0 or not rg or rg <= 0:
            return False
        peg = (price / nc) / (rg * 100)
        # v110 (2026-06-03): PEG 0.22 → 0.25 + rev_growth ≥ 25% 추가 (메가 정의 명확화)
        # BT V110 best plateau: PEG 0.22~0.30 robust. 0.25 = sample 확보.
        return peg < 0.18 and rg >= 0.25
    except Exception as e:
        log(f"check_mega_hold {ticker} 오류: {e}", "WARN")
        return False


def calc_mega_score(row_dict):
    """V110 메가 score: NTM_상향(%) + 매출성장(%) + 50 × PEG_inv

    Returns: score or None (메가 시그니처 불충족 시)
    """
    try:
        p = row_dict.get('price') or 0
        nc = row_dict.get('ntm_current') or 0
        n90 = row_dict.get('ntm_90d') or 0
        rg = row_dict.get('rev_growth') or 0
        if p <= 0 or nc <= 0 or rg <= 0 or n90 <= 0:
            return None
        peg = (p / nc) / (rg * 100)
        if peg >= 0.18 or rg < 0.25:
            return None  # 메가 시그니처 불충족 (v110b: PEG 0.18 — UMBF 같은 가짜 메가 제외)
        ntm_rev = (nc / n90 - 1) * 100
        rg_pct = rg * 100
        peg_inv = 1 / peg
        return ntm_rev + rg_pct + 50 * peg_inv
    except Exception:
        return None


def get_mega_hold_tickers(today_str=None):
    """v111: MA12 추세 보유 종목 중 순위 밖(rank>10/None)인 것 — '추세 보유' 표시용.

    Returns: [(ticker, part2_rank or None), ...]  part2_rank 오름차순 정렬

    _replay_holdings(MA12)가 단일 소스 → 실제 보유와 표시 일치(BT==production).
    rank≤10 보유는 일반 Top20에 나오므로, rank>10(순위 밀려도 추세로 보유 중)만 별도 표시.
    """
    try:
        held = _replay_holdings(today_str)
        if not held:
            return []
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        last = cursor.execute(
            'SELECT MAX(date) FROM ntm_screening WHERE composite_rank IS NOT NULL').fetchone()[0]
        out = []
        for tk in held:
            r = cursor.execute(
                'SELECT part2_rank FROM ntm_screening WHERE ticker=? AND date=?', (tk, last)).fetchone()
            p2 = r[0] if r else None
            if p2 is None or p2 > 10:  # 순위 밖 = 추세로 보유 중 (별도 표시)
                out.append((tk, p2))
        conn.close()
        return sorted(out, key=lambda x: x[1] if x[1] is not None else 999)
    except Exception as e:
        log(f"get_mega_hold_tickers 오류: {e}", "WARN")
        return []


# ============================================================
# 이탈 사유 분류 + AI 분석
# ============================================================

def classify_exit_reasons(exited_tickers, results_df):
    """이탈 종목 사유 분류 — 필터탈락(구체 사유) vs 순위밀림

    Returns: [(ticker, cur_rank or None, reason)]
    - part2_rank 있으면 → '순위밀림' (conviction w_gap 기준)
    - part2_rank 없지만 composite_rank 있으면 → Top30 밖 밀림
    - 둘 다 없으면 → '필터탈락: 구체사유'
    """
    import pandas as pd
    import numpy as np
    result = []
    if not exited_tickers or results_df is None or results_df.empty:
        return result

    # 오늘 순위 (conviction 기반 part2_rank + eligible 체크용 composite_rank)
    composite_map = {}
    part2_map = {}
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT ticker, composite_rank, part2_rank FROM ntm_screening WHERE date=(SELECT MAX(date) FROM ntm_screening WHERE composite_rank IS NOT NULL)'
    )
    for t, cr, pr in cursor.fetchall():
        if cr is not None:
            composite_map[t] = int(cr)
        if pr is not None:
            part2_map[t] = int(pr)
    conn.close()

    # adj_gap 맵 (괴리율 참조)
    adj_gap_map = {}
    conn2 = sqlite3.connect(DB_PATH)
    cursor2 = conn2.cursor()
    cursor2.execute(
        'SELECT ticker, adj_gap FROM ntm_screening WHERE date=(SELECT MAX(date) FROM ntm_screening WHERE adj_gap IS NOT NULL) AND adj_gap IS NOT NULL'
    )
    for t, ag in cursor2.fetchall():
        adj_gap_map[t] = ag
    conn2.close()

    full_data = {}
    for _, row in results_df.iterrows():
        t = row.get('ticker', '')
        if t and t in exited_tickers:
            full_data[t] = row

    # v111: '저평가보유' 재분류는 실제 보유 종목에만 적용 (Top20 이탈≠보유 종목).
    try:
        _held_set = set(_replay_holdings(apply_epoch=True))  # v116 fresh start (표시 일관)
    except Exception:
        _held_set = set()

    for t in sorted(exited_tickers, key=lambda x: exited_tickers[x]):
        cur_rank = part2_map.get(t)  # conviction 기반 순위
        is_eligible = t in composite_map  # 하드필터 통과 여부
        ag = adj_gap_map.get(t)
        # min_seg < -2% 체크 (save_part2_ranks에서 순위 부여 전 제거된 종목)
        row_data = full_data.get(t)
        if row_data is not None:
            segs = [float(row_data.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
            if segs and min(segs) < -2:
                reason = '이익전망↓'
                result.append((t, cur_rank, reason))
                continue
        if ag is not None and ag > 5.0:
            reason = '주가급등'
        elif is_eligible:
            reason = '순위밀림'
        else:
            # 어떤 필터에 걸렸는지 특정
            reason = _identify_filter_failure(full_data.get(t), t)

        # v80.10c (2026-05-11): ⏸️ 유예 분류 제거 — BT 결과 v80.10 환경에선 N=0 best.
        # check_breakout_hold 함수는 코드에 유지 (회귀 검증/약세장 재검토용).

        # v119 (2026-06-11): 실제 보유 종목이 순위 밀렸어도 fwd_PE<15(저평가)면 '저평가보유'(매도 아님).
        # 보유 안 하는 Top20 이탈 종목은 그냥 순위밀림 (보유한 것처럼 표시하면 모순).
        if reason in ('순위밀림', '주가급등') and t in _held_set and _below_pe_live(t):
            reason = '저평가보유'

        result.append((t, cur_rank, reason))

    return result


def _identify_filter_failure(row, ticker):
    """필터탈락 종목의 구체적 탈락 사유 특정"""
    import pandas as pd
    if row is None:
        # 오늘 수집 안 된 종목 → DB에서 최근 데이터로 사유 추정
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute(
                'SELECT * FROM ntm_screening WHERE ticker=? ORDER BY date DESC LIMIT 1',
                (ticker,)
            )
            r = cursor.fetchone()
            if r:
                cols = [d[0] for d in cursor.description]
                row = dict(zip(cols, r))
            conn.close()
        except Exception:
            pass
        if row is None:
            return '필터탈락'

    # 원자재 티커 최우선 체크 (다른 필터에 먼저 걸리는 것 방지)
    if ticker in COMMODITY_TICKERS:
        return '업종제외'

    score = row.get('adj_score', 0) or 0
    if score <= 9:
        return '이익전망↓'

    eps_90d = row.get('eps_change_90d', 0) or 0
    if eps_90d <= 0:
        return '이익전망↓'

    price = row.get('price', 0) or 0
    if price < 10:
        return '저가'

    # MA120 우선, 없으면 MA60
    ma120 = row.get('ma120')
    ma60 = row.get('ma60')
    ma_val = (ma120 if ma120 is not None and pd.notna(ma120) else ma60) or 0
    if ma_val > 0 and price < ma_val:
        return '120일선↓'

    ntm = row.get('ntm_cur') or row.get('ntm_current') or 0
    fwd_pe = price / ntm if ntm > 0 else 0
    if fwd_pe <= 0:
        return '적자'

    rev = row.get('rev_growth')
    if rev is not None and pd.notna(rev) and rev < 0.10:
        return '매출↓'

    analysts = row.get('num_analysts', 0) or 0
    if analysts < 3:
        return '의견부족'

    up = row.get('rev_up30', 0) or 0
    dn = row.get('rev_down30', 0) or 0
    if (up + dn) > 0 and dn / (up + dn) > 0.3:
        return 'EPS하향'

    om = row.get('operating_margin')
    gm = row.get('gross_margin')
    if om is not None and pd.notna(om):
        if om < 0.05:
            return '저수익'
        if gm is not None and pd.notna(gm) and om < 0.10 and gm < 0.30:
            return '저마진'

    ind = row.get('industry', '')
    if ind and ind in COMMODITY_INDUSTRIES:
        return '업종제외'

    return '필터탈락'


def run_ai_analysis(config, selected, biz_day, risk_status=None, market_lines=None,
                     alpha_signals=None):
    """Gemini 2회 호출 — (1) 시장 요약 (2) 종목 내러티브

    AI 실패 시에도 빈 결과를 반환하여 메시지 정상 작동 보장.
    ETF 추천은 별도 코드 기반 함수(find_etf_recommendations)로 이동.
    alpha_signals: 어닝 서프/공매도 → Gemini 내러티브에 자연어로 녹이기 위해 전달
    Returns: {'market_summary': str, 'narratives': {ticker: str}}
    """
    import re

    api_key = config.get('gemini_api_key', '')
    result = {'market_summary': '', 'narratives': {}}

    if not api_key:
        log("AI: GEMINI_API_KEY 미설정 — AI 없이 진행")
        return result

    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=api_key, http_options={'timeout': 180_000})
        grounding_tool = types.Tool(google_search=types.GoogleSearch())
    except Exception as e:
        log(f"AI: Gemini 초기화 실패: {e}", "WARN")
        return result

    def _gemini_call(prompt, temperature=0.2, label=""):
        """Gemini 호출 래퍼 — flash 3회 재시도 (10→20→40초) + lite fallback

        총 70초 회복 시간. 일시적 503은 대부분 복구됨.
        4번째 시도는 gemini-2.5-flash-lite (품질 약간 낮음, 시장동향 부정확 가능).
        """
        import time as _time

        # 1단계: gemini-2.5-flash 3회 재시도
        delays = [10, 20, 40]
        for _attempt in range(3):
            try:
                old_timeout = socket.getdefaulttimeout()
                socket.setdefaulttimeout(60)
                try:
                    resp = client.models.generate_content(
                        model='gemini-2.5-flash',
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            tools=[grounding_tool],
                            temperature=temperature,
                        ),
                    )
                finally:
                    socket.setdefaulttimeout(old_timeout)
                return resp
            except Exception as e:
                if _attempt < 2:
                    log(f"AI: {label} flash 실패 ({delays[_attempt]}초 후 재시도 {_attempt+1}/3): {e}", "WARN")
                    _time.sleep(delays[_attempt])
                else:
                    log(f"AI: {label} flash 3회 실패: {e}", "WARN")

        # 2단계: gemini-2.5-flash-lite로 fallback (품질 낮음, 빠름)
        log(f"AI: {label} → flash-lite fallback (품질 차이 있을 수 있음)", "WARN")
        try:
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(60)
            try:
                resp = client.models.generate_content(
                    model='gemini-2.5-flash-lite',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[grounding_tool],
                        temperature=temperature,
                    ),
                )
            finally:
                socket.setdefaulttimeout(old_timeout)
            log(f"AI: {label} flash-lite 성공", "WARN")
            return resp
        except Exception as e:
            log(f"AI: {label} flash-lite도 실패: {e}", "WARN")
            raise

    def extract_text(resp):
        try:
            if resp.text:
                return resp.text
        except Exception as e:
            log(f"AI: resp.text 접근 실패: {e}", "WARN")
        try:
            parts = resp.candidates[0].content.parts
            texts = [p.text for p in parts if hasattr(p, 'text') and p.text]
            if texts:
                return '\n'.join(texts)
        except Exception as e:
            # candidates 비어있으면 finish_reason, safety_ratings 등 출력
            try:
                cands = resp.candidates if resp.candidates else []
                finish = cands[0].finish_reason if cands else 'NO_CANDIDATES'
                safety = cands[0].safety_ratings if cands else 'N/A'
                log(f"AI: candidates 파싱 실패: {e} | finish={finish} | safety={safety}", "WARN")
            except Exception:
                log(f"AI: resp 구조 확인 불가: {type(resp)} | {str(resp)[:200]}", "WARN")
        return None

    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    # ── 호출 1: 시장 요약 ──
    try:
        # 실제 지수 데이터를 프롬프트에 포함 (AI가 상승/하락 반대로 말하는 것 방지)
        idx_ctx = ""
        if market_lines:
            import re as _re
            idx_parts = []
            for ml in market_lines:
                _m = _re.match(r'[🟢🔴🟡]\s*(.+)', ml)
                if _m:
                    idx_parts.append(_m.group(1).strip())
            if idx_parts:
                idx_ctx = f"[당일 지수 마감] {' / '.join(idx_parts)}"

        market_prompt = f"""{biz_str} 미국 주식시장 마감 결과를 Google 검색해서 요약해줘.

{idx_ctx}

[중요] 이 요약은 {biz_str} 미국 시장 마감(16시 ET) 이후에 작성하는 거야.
마감 시점까지 이미 발표된 경제지표(FOMC 결정, CPI, PPI, 고용 등)는 "결과"로 써.
"향후 예정", "발표될 예정" 같은 표현은 마감 이후 일정에만 써.
예: FOMC가 당일 14시에 금리 동결 발표 → "연준이 금리를 동결했습니다" (O) / "FOMC 결과가 예정되어 있습니다" (X)

[구조] 3문단, 총 400~550자로 작성 (문단 사이 빈 줄):
문단1. 당일 시장 흐름 — 상승/하락 원인과 핵심 이슈 (2~3문장)
문단2. 지수 움직임 — 주요 종목/섹터 동향, 수급 흐름 (2~3문장)
문단3. 업종별 강약 — 어떤 테마가 주도했는지 + 향후 주요 일정 (2~3문장)

[규칙]
- 400~550자. 3문단으로 나눠서 써. 문단 사이에 빈 줄 넣어.
- 위 [당일 지수 마감] 데이터와 반드시 일치해야 해. 지수가 마이너스면 "하락", 플러스면 "상승".
- 지수 수치(S&P, 나스닥 등)는 별도 표시하니 생략.
- 구체적으로 써 — "관세 이슈" 대신 "트럼프 15% 글로벌 관세 발표에..." 같이.
- 트럼프는 2025년 1월 재취임한 현직 대통령이야. "전 대통령"이라고 쓰지 마.
- 섹터 동향도 구체적으로 — "기술주 약세" 대신 "AI·반도체주가 2% 넘게 하락" 같이.
- 전일(어제) 이벤트를 당일 일처럼 쓰지 마. {biz_str} 당일 변동만.
- 개별 종목 급등락은 당일 변동만 언급해.
- 한국 투자자(서학개미) 동향은 쓰지 마. 미국 시장만.
- 한국어, ~입니다 체. 번역투 금지. 자연스럽게.
- 인사말/서두/맺음말 없이 바로 시작."""

        resp = _gemini_call(market_prompt, temperature=0.2, label="시장요약")
        text = extract_text(resp) if resp else None
        if text:
            # 마크다운 제거
            text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
            text = re.sub(r'#{1,3}\s*', '', text)
            # Gemini 그라운딩 인용 태그 제거
            text = re.sub(r'\[cite:.*?\]', '', text)
            result['market_summary'] = text.strip()
            log(f"AI: 시장요약 {len(result['market_summary'])}자")
        else:
            log("AI: 시장요약 Gemini 응답 없음", "WARN")
    except Exception as e:
        log(f"AI: 시장요약 실패: {e}", "WARN")

    # ── 호출 2: 종목 내러티브 ──
    if selected:
        try:
            stock_lines = []
            for i, s in enumerate(selected):
                rev = _safe_float(s.get('rev_growth'))
                gap = _safe_float(s.get('adj_gap'))
                lights = s.get('lights', '')
                desc = s.get('desc', '')
                # 어닝 서프/공매도 데이터를 Gemini에 전달
                alpha_parts = []
                if alpha_signals:
                    asig = alpha_signals.get(s['ticker'], {})
                    surp = asig.get('earnings_surp')
                    if surp is not None and (surp > 0.3 or surp < 0):
                        alpha_parts.append(f"어닝서프 {surp*100:+.0f}%")
                    sp = asig.get('short_pct', 0)
                    if sp >= 8:
                        sm = asig.get('short_mom', 0)
                        short_str = f"공매도 {sp:.1f}%"
                        if sm <= -20:
                            short_str += "(감소 중)"
                        elif sm >= 20:
                            short_str += "(급증)"
                        alpha_parts.append(short_str)
                alpha_line = f"\n   {' · '.join(alpha_parts)}" if alpha_parts else ""
                stock_lines.append(
                    f"{i+1}. {s['name']}({s['ticker']}) · {s['industry']}\n"
                    f"   EPS {s['eps_chg']:+.1f}% · 매출 {rev:+.0%} · 괴리 {gap:+.1f}%\n"
                    f"   EPS추세 {lights} {desc}{alpha_line}"
                )

            stock_prompt = f"""아래 {len(selected)}종목 각각에 대해 Google 검색해서 종목 브리핑을 써줘.
기준일: {biz_str}

[종목]
{chr(10).join(stock_lines)}

[시스템 배경]
이 종목들은 "애널리스트 EPS 전망이 상향되는데 주가가 덜 오른 종목"을 찾는 시스템이 선정한 매수 후보야.
괴리(adj_gap)가 음수일수록 EPS 대비 주가가 저평가 상태라는 뜻이야.

[형식]
종목별 2~3문장(150~200자). 종목 사이에 [SEP] 표시.
형식: TICKER: 설명

[규칙]
- 1문장: 실적 성장 배경 — 왜 EPS/매출이 오르는지 사업적 이유를 구체적으로.
  어떤 제품/서비스가, 어떤 시장에서, 왜 수요가 느는지 써.
- 2문장: {biz_str} 전후 1~2주 이내 관련 뉴스(실적 발표, 수주, 제품 출시, 규제 변화 등)가 있으면 한 줄로 언급.
  검색해도 최근 뉴스가 없으면 이 문장은 생략하고 성장 배경만 2~3문장으로 써.
  반드시 검색 결과에 있는 실제 뉴스만 써. 없는 뉴스를 만들어내지 마.
- 어닝 서프라이즈 데이터가 있으면 실적 설명 흐름에 자연스럽게 녹여.
  좋은 예: "3월 18일 발표된 분기 실적에서 EPS가 시장 예상을 36% 상회했습니다."
  나쁜 예: "어닝 서프라이즈는 +36%입니다." ← 숫자만 읽어주기 금지
- 공매도 비율이 높으면(8%+) 시장 심리 맥락에서 한 줄로 언급해.
  좋은 예: "다만 공매도 비율이 12%로 높아 단기 변동성에 유의할 필요가 있습니다."
  나쁜 예: "공매도 비율은 12%입니다." ← 맥락 없는 수치 나열 금지
- 이 데이터가 없는 종목은 어닝 서프/공매도를 언급하지 마.
- 좋은 예: "AI 서버용 HBM 수요 폭증과 DRAM 가격 상승으로 분기 매출이 사상 최고를 기록했습니다. 3월 19일 실적 발표에서 가이던스를 상향 조정하며 AI 메모리 투자 확대를 시사했습니다."
- 좋은 예: "데이터센터 및 방위 산업 PCB 수요 증가로 고부가가치 제품 비중이 높아지며 이익률이 크게 개선되고 있습니다."
- 나쁜 예: "SSD 매출 증가와 제품 믹스 개선으로 실적이 크게 성장했습니다." ← 너무 추상적
- 회사명은 티커만 써 (NVDA, APH 등). "Corporation", "Inc.", 풀네임 금지.
- 번역투 금지: "탁월한", "유기적", "전략적 인수 프로그램", "모멘텀에 힘입어" 같은 표현 쓰지 마.
- 자연스러운 한국어로 써: "AI 서버 수요가 늘면서", "반도체 가격이 오르면서" 같이 쉽게.
- 종목마다 문장 구조를 다르게 써. "~에 힘입어 ~성장" 패턴만 반복하지 마.
- 한국어, ~입니다 체.
- 서두/인사말/맺음말 금지. 첫 종목부터 바로 시작."""

            resp = _gemini_call(stock_prompt, temperature=0.3, label="종목내러티브")
            if resp:
                try:
                    log(f"AI: 내러티브 resp type={type(resp).__name__} candidates={len(resp.candidates) if resp.candidates else 0}")
                    if resp.candidates:
                        c = resp.candidates[0]
                        log(f"AI: 내러티브 finish={c.finish_reason} parts_type={type(c.content.parts).__name__ if c.content and c.content.parts else 'None'} parts_len={len(c.content.parts) if c.content and c.content.parts else 0}")
                except Exception as dbg_e:
                    log(f"AI: 내러티브 resp 디버그 실패: {dbg_e}")
            text = extract_text(resp) if resp else None
            if text:
                # 디버그: raw 텍스트 앞 500자 (파싱 실패 원인 추적)
                raw_preview = text[:500].replace('\n', '\\n')
                log(f"AI: 내러티브 raw[:500]={raw_preview}")
                # 마크다운 볼드 제거
                text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
                text = re.sub(r'#{1,3}\s*', '', text)

                # [SEP]로 먼저 분리 (Gemini가 한 줄로 반환하는 경우 대응)
                text = text.replace('[SEP]', '\n')
                # 파싱: "TICKER: 설명" 패턴 (여러 변형 허용)
                for line in text.split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    # "TICKER: 설명" / "N. TICKER: 설명" / "- TICKER: 설명" / "Company(TICKER): 설명"
                    m = re.match(r'(?:\d+\.\s*)?(?:-\s*)?([A-Z]{1,5})[\s:：]+(.{10,})', line)
                    if not m:
                        # "Company Name(TICKER): 설명" 형태 대응
                        m2 = re.match(r'.*?\(([A-Z]{1,5})\)[\s:：]+(.{10,})', line)
                        if m2:
                            m = m2
                    if m:
                        ticker = m.group(1)
                        narrative = m.group(2).strip()
                        # "TICKER:" 등 잔여 제거
                        narrative = re.sub(r'^[:\s]+', '', narrative)
                        if narrative:
                            result['narratives'][ticker] = narrative

                expected_tickers = {s['ticker'] for s in selected}
                parsed_n = sum(1 for t in result['narratives'] if t in expected_tickers)
                expected_n = len(selected)

                # 파싱 불완전 시 누락 종목만 재요청 (1회)
                missing = expected_tickers - set(result['narratives'].keys())
                if missing and parsed_n < expected_n:
                    log(f"AI: 내러티브 1차 {parsed_n}/{expected_n} — 누락 {','.join(sorted(missing))} 재요청", "WARN")
                    missing_selected = [s for s in selected if s['ticker'] in missing]
                    # 누락 종목만 포함한 stock_lines 재생성
                    retry_lines = []
                    for i, s in enumerate(missing_selected):
                        rev = _safe_float(s.get('rev_growth'))
                        gap = _safe_float(s.get('adj_gap'))
                        retry_lines.append(
                            f"{i+1}. {s['name']}({s['ticker']}) · {s['industry']}\n"
                            f"   EPS {s['eps_chg']:+.1f}% · 매출 {rev:+.0%} · 괴리 {gap:+.1f}%"
                        )
                    retry_prompt = (f"""아래 {len(missing_selected)}종목 각각에 대해 Google 검색해서 종목 브리핑을 써줘.

[종목]
{chr(10).join(retry_lines)}

[필수 형식] 반드시 `TICKER: 설명` 으로 시작. 종목 사이는 [SEP] 로 구분.
예: {missing_selected[0]['ticker']}: 설명... [SEP] ...

[내용 규칙]
- 종목당 2~3문장(150~200자), 실적 성장 배경 + 최근 뉴스
- 회사명은 티커만 써
- 한국어 ~입니다 체, 서두/맺음말 금지""")
                    resp2 = _gemini_call(retry_prompt, temperature=0.3, label="종목내러티브재시도")
                    text2 = extract_text(resp2) if resp2 else None
                    if text2:
                        raw2 = text2[:500].replace('\n', '\\n')
                        log(f"AI: 내러티브 재시도 raw[:500]={raw2}")
                        text2 = re.sub(r'\*\*(.+?)\*\*', r'\1', text2).replace('[SEP]', '\n')
                        text2 = re.sub(r'#{1,3}\s*', '', text2)
                        for line in text2.split('\n'):
                            line = line.strip()
                            if not line:
                                continue
                            m = re.match(r'(?:\d+\.\s*)?(?:-\s*)?([A-Z]{1,5})[\s:：]+(.{10,})', line)
                            if not m:
                                m2 = re.match(r'.*?\(([A-Z]{1,5})\)[\s:：]+(.{10,})', line)
                                if m2:
                                    m = m2
                            if m:
                                tk = m.group(1)
                                nar = re.sub(r'^[:\s]+', '', m.group(2).strip())
                                if nar and tk in missing:
                                    result['narratives'][tk] = nar
                    parsed_n = sum(1 for t in result['narratives'] if t in expected_tickers)

                if parsed_n == 0:
                    log(f"AI: 내러티브 파싱 0종목 (요청 {expected_n}종목) — Signal 메시지에서 생략됨", "WARN")
                elif parsed_n < expected_n:
                    final_missing = expected_tickers - set(result['narratives'].keys())
                    log(f"AI: 내러티브 최종 {parsed_n}/{expected_n}종목 — 최종 누락: {','.join(sorted(final_missing))}", "WARN")
                else:
                    log(f"AI: 내러티브 {parsed_n}종목")
            else:
                log("AI: 내러티브 Gemini 응답 없음", "WARN")
        except Exception as e:
            log(f"AI: 내러티브 실패: {e}", "WARN")

    return result


def _safe_float(val, default=0):
    """NaN/None/비숫자를 default로 변환 — float NaN은 truthy라 `or 0` 무효"""
    if val is None:
        return default
    try:
        if math.isnan(val):
            return default
    except TypeError:
        return default
    return val


def _clean_company_name(name, ticker):
    """회사명에서 법인 접미사 제거 — 'Sandisk Corporation' → 'Sandisk'
    캐시에 잘린 이름('Incorporat', 'Hold')도 처리.
    """
    import re
    if not name or name == ticker:
        return ticker
    # 법인격 접미사 (완전 + 부분 잘림 모두 대응)
    suffixes = r',?\s*(?:Inc(?:orporat(?:ed?)?)?\.?|Corp(?:orati(?:on)?)?\.?|Comp(?:any)?|Co\.?|Ltd\.?|Limi(?:ted)?|PLC|plc|Hold(?:ings?)?\.?|Group|Technolog(?:y|ies)|N\.?V\.?|(?<![A-Za-z])S\.?A\.?|(?<![A-Za-z])SE|(?<![A-Za-z])AG)\s*$'
    cleaned = re.sub(suffixes, '', name, flags=re.IGNORECASE).strip()
    # 반복 적용 (접미사가 중첩된 경우: "Holdings, Inc.")
    cleaned = re.sub(suffixes, '', cleaned, flags=re.IGNORECASE).strip()
    # 마지막 쉼표/마침표 제거
    cleaned = cleaned.rstrip(',.').strip()
    # 접미사 제거 후 남은 꼬리 접속사 제거 ("Eli Lilly and" → "Eli Lilly")
    cleaned = re.sub(r'\s+(?:and|&)\s*$', '', cleaned, flags=re.IGNORECASE).strip()
    return cleaned if cleaned else ticker


def compute_factor_ranks(results_df, today_tickers):
    """Top 30 내 팩터별 등수 계산 — 저평가(adj_gap)·매출성장(rev_growth)

    Returns: {ticker: {'gap_rank': int, 'rev_rank': int, 'gap_val': float, 'rev_val': float}}
    저평가: adj_gap 낮을수록(더 음수) 1등 (EPS 전망 대비 할인 큰 순)
    매출성장: rev_growth 높을수록 1등 (YoY 성장률 높은 순)
    """
    if results_df is None or results_df.empty or not today_tickers:
        return {}

    top30 = results_df[results_df['ticker'].isin(today_tickers)].copy()
    if top30.empty:
        return {}

    # 괴리 등수: adj_gap 오름차순 (가장 음수 = 1등 = 가장 저평가)
    top30 = top30.sort_values('adj_gap', ascending=True).reset_index(drop=True)
    gap_ranks = {row['ticker']: (i + 1, row.get('adj_gap', 0) or 0) for i, (_, row) in enumerate(top30.iterrows())}

    # 매출 등수: rev_growth 내림차순 (가장 높은 성장 = 1등)
    top30 = top30.sort_values('rev_growth', ascending=False, na_position='last').reset_index(drop=True)
    rev_ranks = {row['ticker']: (i + 1, _safe_float(row.get('rev_growth'))) for i, (_, row) in enumerate(top30.iterrows())}

    result = {}
    for t in today_tickers:
        gr, gv = gap_ranks.get(t, (30, 0))
        rr, rv = rev_ranks.get(t, (30, 0))
        result[t] = {'gap_rank': gr, 'rev_rank': rr, 'gap_val': gv, 'rev_val': rv}
    return result


# ============================================================
# v3 메시지 (Signal + AI Risk + Watchlist)
# ============================================================

def _build_top5_streak(today_str=None):
    """Top 30 연속 유지 일수 계산. Returns: {ticker: int(연속 일수)}"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    dates = _get_recent_dates(cursor, 'part2_rank', today_str, 30)

    streak = {}
    if not dates:
        conn.close()
        return streak

    # 최신 날짜의 Top 30 종목
    latest = dates[0]
    target_rows = cursor.execute(
        'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
        (latest,)
    ).fetchall()
    target_tickers = [r[0] for r in target_rows]

    for ticker in target_tickers:
        count = 0
        for d in dates:
            row = cursor.execute(
                'SELECT part2_rank FROM ntm_screening WHERE date=? AND ticker=? AND part2_rank IS NOT NULL',
                (d, ticker)
            ).fetchone()
            if row:
                count += 1
            else:
                break
        streak[ticker] = count

    conn.close()
    return streak


def _build_score_100_map(today_str=None):
    """3일 가중 점수 맵 (v71). 일별 z-score(30~100) → 3일 가중평균.

    v71: conviction adj_gap → 일별 z-score 변환 → 가중평균
    빈 날 → carry-forward (직전 가용 점수 이월), 최종 폴백 30점
    Returns: (w_score_map, score_display_map)
      - w_score_map: 3일 가중 점수 (높을수록 좋음, 순위/정렬용)
      - score_display_map: 고정 스케일 0~100 (v112, 표시용 — 날짜 안정 + 강도 보존)
    """
    import numpy as np
    MISSING_PENALTY = 30

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    dates = sorted(_get_recent_dates(cursor, 'composite_rank', today_str, 3))
    if not dates:
        conn.close()
        return {}, {}

    score_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, '
            'rev_growth FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        ).fetchall()
        conv_gaps = {}
        for r in rows:
            tk = r[0]
            conv_gaps[tk] = _apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6])

        vals = list(conv_gaps.values())
        if len(vals) >= 2:
            mean_v = np.mean(vals)
            std_v = np.std(vals)
            if std_v > 0:
                score_by_date[d] = {
                    tk: max(30.0, 65 + (-(v - mean_v) / std_v) * 15)
                    for tk, v in conv_gaps.items()
                }
            else:
                score_by_date[d] = {tk: 65 for tk in conv_gaps}
        else:
            score_by_date[d] = {tk: 65 for tk in conv_gaps}

    weights = [0.2, 0.3, 0.5]
    if len(dates) == 2:
        weights = [0.4, 0.6]
    elif len(dates) == 1:
        weights = [1.0]

    all_tickers = set()
    for d in dates:
        all_tickers.update(score_by_date.get(d, {}).keys())

    # v77 (2026-04-15): carry-forward 제거 — _compute_w_gap_map과 동일 정책.
    # 이전: 두 함수가 carry-forward 가짐 → 🆕 종목이 display Top 3에 표시되는 버그.
    # 이제: 빈 날 = 무조건 30점. display와 매매 순위 일관성 확보.
    # v80.1 (2026-04-24): 빈 날 기준 cr → p2 변경 (궤적 표시와 일관성).
    #   상세: _compute_w_gap_map 주석 참조.
    p2_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)
        ).fetchall()
        p2_by_date[d] = {r[0] for r in rows}

    # 3일 가중 점수 (순위/정렬용)
    w_score_map = {}
    today_key = today_str if today_str else (dates[-1] if dates else None)
    for tk in all_tickers:
        ws = 0
        for i, d in enumerate(dates):
            is_today = (d == today_key)
            if not is_today and tk not in p2_by_date.get(d, set()):
                score = MISSING_PENALTY
            else:
                score = score_by_date.get(d, {}).get(tk)
                if score is None:
                    score = MISSING_PENALTY
            ws += score * weights[i]
        w_score_map[tk] = ws

    # v112 (2026-06-04): 고정 스케일 — 날짜 안정 + 강도 보존.
    # 기존 ws/max*100은 분모가 "그날 최댓값"이라 같은 종목도 그날 1등이 누구냐에
    # 따라 점수가 출렁임(15일간 최댓값 83~112 변동, +1.2σ 종목이 74~100점 왔다갔다).
    # 고정 앵커: ws 30(하한/missing)→0, ws 100(+2.3σ)→100. 괴물주(MU급)는 100,
    # 밋밋한 날 1등은 낮게 → 점수가 강도의 절대 정보를 담음. EDA+사용자 승인(B안).
    score_display_map = {
        tk: round(max(0.0, min(100.0, (ws - 30) / 70 * 100)), 1)
        for tk, ws in w_score_map.items()
    }

    conn.close()
    return w_score_map, score_display_map


def _regime_defense_series(all_dates):
    """성과 계산용 — all_dates 각 날짜의 defense 여부 + IEF 일수익률.

    get_market_regime과 동일 신호 (SPX<MA200 10d OR VIX>36 2d), 전 구간 시리즈로.
    Returns: (defense_by_date {date:bool}, ief_ret_by_date {date:float}). 실패 시 전부 boost.
    """
    try:
        import yfinance as yf
        import pandas as pd
        from datetime import datetime, timedelta
        start = (datetime.strptime(all_dates[0], '%Y-%m-%d') - timedelta(days=400)).strftime('%Y-%m-%d')
        end = (datetime.strptime(all_dates[-1], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        def _close(tk):
            df = yf.download(tk, start=start, end=end, auto_adjust=True, progress=False)
            cl = df['Close']
            if hasattr(cl, 'columns'):
                cl = cl.iloc[:, 0]
            cl.index = pd.to_datetime(cl.index).tz_localize(None)
            return cl.dropna()

        def _ser(raw, n):
            out, state, sd, sb = [], False, 0, 0
            for d in raw.values:
                if bool(d):
                    sd += 1; sb = 0
                else:
                    sb += 1; sd = 0
                if not state and sd >= n:
                    state = True
                elif state and sb >= n:
                    state = False
                out.append(state)
            return pd.Series(out, index=raw.index)

        spx, vix, ief = _close('^GSPC'), _close('^VIX'), _close('IEF')
        ma = spx.rolling(REGIME_MA_PERIOD).mean()
        ma_def = _ser((spx < ma).fillna(False), REGIME_MA_CONFIRM)
        vix_def = _ser((vix.reindex(spx.index).ffill() > REGIME_VIX_THRESH).fillna(False), REGIME_VIX_CONFIRM)
        defense = (ma_def | vix_def)
        ief_r = ief.reindex(spx.index).ffill().pct_change()
        defense.index = defense.index.strftime('%Y-%m-%d')
        ief_r.index = ief_r.index.strftime('%Y-%m-%d')
        dmap = {d: bool(defense.get(d, False)) for d in all_dates}
        imap = {d: float(ief_r.get(d, 0.0) or 0.0) for d in all_dates}
        return dmap, imap
    except Exception as e:
        log(f"성과 regime 계산 실패 (boost 가정): {e}", level="WARN")
        return {d: False for d in all_dates}, {d: 0.0 for d in all_dates}


def _get_system_performance(apply_epoch=False):
    """시스템 누적 성과 계산 (DB 데이터 기반, defense 일자 IEF 반영).

    v119 (2026-06-11): apply_epoch=True 시 배포일(HOLDINGS_EPOCH) 이후 실제 성과만 계산.
      메시지 표시는 epoch 적용(fresh-start와 일관 — 못 사는 SNDK 백테스트 수익 자랑 제거).
      BT 검증용은 apply_epoch=False(전체 replay).
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        all_dates = [r[0] for r in c.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        if apply_epoch:
            all_dates = [d for d in all_dates if d >= HOLDINGS_EPOCH]
        if len(all_dates) < 3:
            conn.close()
            return None

        # 국면 오버레이 (2026-05-27): defense 일자엔 IEF 보유 수익으로 계산.
        #   현재 데이터 전부 boost라 영향 0, 미래 약세장이 쌓이면 자동 반영.
        regime_def, ief_ret = _regime_defense_series(all_dates)

        # 전체 가격 로드
        all_prices = {}
        for d in all_dates:
            rows = c.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)).fetchall()
            all_prices[d] = {r[0]: r[1] for r in rows}

        # 일별 데이터 로드 (v86e+ 메가 carryover 시뮬에 rev_growth 필요)
        # v117 (2026-06-09): dollar_volume_30d 추가 — 시장 주도주 필터용 (시뮬↔production 정합)
        daily_data = {}
        for d in all_dates:
            rows = c.execute('''
                SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, rev_growth, dollar_volume_30d
                FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL
            ''', (d,)).fetchall()
            daily_data[d] = {
                r[0]: {'price': r[1], 'part2_rank': r[2],
                       'nc': r[3], 'n7': r[4], 'n30': r[5], 'n60': r[6], 'n90': r[7],
                       'rg': r[8], 'dv': r[9]}
                for r in rows
            }

        def _min_seg(nc, n7, n30, n60, n90):
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            return min(segs)

        def _w_gap(date_str):
            """v71: 일별 z-score(30~100) → 3일 가중평균, 빈 날→carry-forward"""
            MISSING_PENALTY = 30
            di = all_dates.index(date_str)
            d0 = all_dates[di]
            d1 = all_dates[di - 1] if di >= 1 else None
            d2 = all_dates[di - 2] if di >= 2 else None
            ds = [d for d in [d2, d1, d0] if d]  # 오래된 순

            score_by_d = {}
            for d in ds:
                rows = c.execute(
                    'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, '
                    'rev_growth FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (d,)
                ).fetchall()
                conv = {r[0]: _apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6]) for r in rows}
                vals = list(conv.values())
                if len(vals) >= 2:
                    mv, sv = sum(vals)/len(vals), (sum((v-sum(vals)/len(vals))**2 for v in vals)/len(vals))**0.5
                    if sv > 0:
                        score_by_d[d] = {tk: max(30.0, 65 + (-(v - mv) / sv) * 15) for tk, v in conv.items()}
                    else:
                        score_by_d[d] = {tk: 65 for tk in conv}
                else:
                    score_by_d[d] = {tk: 65 for tk in conv}

            def _cf(tk, idx):
                """backward only — forward 탐색 금지 (신규 종목 3일 패널티 우회 방지)"""
                for j in range(idx - 1, -1, -1):
                    prev = score_by_d.get(ds[j], {}).get(tk)
                    if prev is not None:
                        return prev
                return MISSING_PENALTY

            result = {}
            tks = set()
            for d in ds:
                if d in score_by_d:
                    tks.update(score_by_d[d].keys())
            wts = [0.5, 0.3, 0.2]
            for tk in tks:
                wg = 0
                for i, d in enumerate([d0, d1, d2]):
                    if d:
                        score = score_by_d.get(d, {}).get(tk)
                        if score is None:
                            score = _cf(tk, ds.index(d) if d in ds else i)
                        wg += score * wts[i]
                result[tk] = wg
            return result

        # S&P500 지수 (^GSPC) — 벤치마크 표준, 배당 조정 불필요
        try:
            import yfinance as yf
            from datetime import datetime, timedelta
            # yfinance end는 exclusive → 하루 더 해야 all_dates[-1]까지 포함됨
            end_inclusive = (datetime.strptime(all_dates[-1], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            spy_df = yf.download('^GSPC', start=all_dates[0], end=end_inclusive,
                                 auto_adjust=False, progress=False)
            spy_prices = {}
            # v80.7 (2026-05-02): row.iloc[3]은 Low(일중 최저가) — Close가 아님.
            # yfinance auto_adjust=False 컬럼 순서: [Adj Close, Close, High, Low, Open, Volume]
            # 결과: SPY 누적 수익률이 -1~2%p 낮게 표시되는 버그 (메시지에 +4.59% vs 실제 +5.94%)
            # multi-index column 처리: spy_df['Close']는 DataFrame, .iloc[i, 0]으로 단일 값 추출
            close_df = spy_df['Close'] if 'Close' in spy_df.columns.get_level_values(0) else spy_df.iloc[:, [0]]
            for i, idx in enumerate(close_df.index):
                ds = idx.strftime('%Y-%m-%d')
                spy_prices[ds] = float(close_df.iloc[i, 0])
        except Exception:
            spy_prices = {}

        # 백테스트 실행
        start_idx = 2
        portfolio = {}
        sys_nav = 1.0
        spy_nav = 1.0
        wins = 0
        losses = 0

        for i in range(start_idx, len(all_dates)):
            date = all_dates[i]
            prev_date = all_dates[i - 1]
            data = daily_data.get(date, {})
            prices = all_prices.get(date, {})
            prev_prices = all_prices.get(prev_date, {})

            # 방어 국면: 주식 청산 + IEF 보유 수익 (현재 전부 boost라 미발동)
            if regime_def.get(date, False):
                sys_nav *= (1 + ief_ret.get(date, 0.0))
                sc, sp = spy_prices.get(date), spy_prices.get(prev_date)
                if sc and sp and sp > 0:
                    spy_nav *= (1 + (sc - sp) / sp)
                portfolio = {}
                continue

            ticker_ms = {}
            for tk, info in data.items():
                ticker_ms[tk] = _min_seg(info['nc'], info['n7'], info['n30'], info['n60'], info['n90'])

            # v119: 순위 = DB part2_rank 직접 사용 (BT/replay와 완전 정합 — _w_gap 재계산 제거).
            #   기존엔 perf만 _w_gap을 재계산해 part2_rank와 어긋남 → 시뮬 보유가 BT와 달랐음(BE vs MU).
            eligible = [(tk, info['part2_rank']) for tk, info in data.items()
                        if ticker_ms.get(tk, 0) >= -2 and info.get('part2_rank')]
            eligible.sort(key=lambda x: x[1])  # part2_rank 작을수록 상위
            wgap_rank = {tk: info['part2_rank'] for tk, info in data.items() if info.get('part2_rank')}

            # v84 (2026-05-30): 진입 시점 score gap 기반 dynamic weight (2step_t15)
            #   portfolio[tk]['weight']에 진입 시 결정된 weight 저장됨 (sticky)
            #   v83.3 정적 [90,10] → v84 dynamic ([100,0] or [50,50])
            day_ret = 0
            if portfolio:
                for tk, info in portfolio.items():
                    w = info.get('weight', 0) / 100.0
                    cur = prices.get(tk)
                    prev = prev_prices.get(tk)
                    if cur and prev and prev > 0:
                        day_ret += w * (cur - prev) / prev * 100

            # SPY는 portfolio 진입 후 첫 거래일부터 누적 (시스템과 동일 시점)
            spy_ret = 0
            if portfolio:  # 어제 portfolio가 있어야 SPY ret 누적 (첫 진입일은 skip)
                sc = spy_prices.get(date)
                sp = spy_prices.get(prev_date)
                if sc and sp and sp > 0:
                    spy_ret = (sc - sp) / sp * 100

            sys_nav *= (1 + day_ret / 100)
            spy_nav *= (1 + spy_ret / 100)

            # v119 (2026-06-11): 제3방안 fwd_PE<15 저평가 보유 — 시뮬↔production 정합
            #   매도: EPS꺾임(min_seg<-2) / (rank>10 AND fwd_PE>=15, 비싸짐)
            #   진입: slot 1·2 모두 part2 Top (메가 전용 슬롯 제거) + $1B+
            for tk in list(portfolio.keys()):
                ep = portfolio[tk]['entry_price']
                info_tk = daily_data.get(date, {}).get(tk)
                if info_tk is None:
                    continue  # 오늘 part2_rank 없음 → carryover (BT/replay 정합)
                cp = prices.get(tk)
                if cp is None:
                    continue  # 데이터 fetch 실패 → carryover (v113)
                rk = wgap_rank.get(tk)
                ms = ticker_ms.get(tk, 0)
                nc_tk = info_tk.get('nc')
                ret = (cp - ep) / ep * 100 if ep else 0
                sell = False
                if ms < -2:
                    sell = True  # EPS꺾임 즉시매도
                elif rk is None or rk > 10:
                    # 10위 밖: fwd_PE veto (BT 정합 — 계산 불가 시 pe=999 → 매도)
                    pe_tk = (cp / nc_tk) if (cp and nc_tk and nc_tk > 0) else 999
                    if pe_tk >= PE_HOLD:
                        sell = True  # 비싸짐 → 매도
                    # else 저평가(PE<15) → 보유
                # rk<=10이면 보유
                if sell:
                    if ret > 0: wins += 1
                    else: losses += 1
                    del portfolio[tk]

            # v119 진입: slot 1·2 모두 part2 Top 3 + $1B+ (메가 슬롯 제거, BT 진입과 정합)
            if len(portfolio) < 2:
                used_idx = {info['slot_idx'] for info in portfolio.values()}
                free_idx = sorted([si for si in range(2) if si not in used_idx])
                p2_cands = [tk for tk, _ in eligible
                            if tk not in portfolio and ticker_ms.get(tk, -999) >= 0
                            and wgap_rank.get(tk, 999) <= 5
                            and (daily_data.get(date, {}).get(tk, {}).get('dv') or 0) >= 1000]
                p2_cands.sort(key=lambda t: wgap_rank.get(t, 999))
                for tk in p2_cands:
                    if len(portfolio) >= 2:
                        break
                    if tk in portfolio:
                        continue
                    idx = free_idx.pop(0) if free_idx else len(portfolio)
                    portfolio[tk] = {'entry_price': prices.get(tk), 'slot_idx': idx, 'weight': 50}
                # 비중 rebalance
                pn = len(portfolio)
                for info in portfolio.values():
                    info['weight'] = 100 if pn == 1 else 50

        conn.close()
        # n_days: 실제 day_ret 누적 일수 (첫 진입일은 day_ret=0이므로 -1)
        n_days = len(all_dates) - start_idx - 1
        return {
            'sys_cum': (sys_nav - 1) * 100,
            'spy_cum': (spy_nav - 1) * 100,
            'alpha': (sys_nav - 1) * 100 - (spy_nav - 1) * 100,
            'n_days': n_days,
            'start_date': all_dates[start_idx],
            'end_date': all_dates[-1],
            'wins': wins,
            'losses': losses,
            'holdings': sorted(portfolio.keys()),  # v86e++ 검증용: 최종 보유집합 노출
        }
    except Exception as e:
        log(f"시스템 성과 계산 실패: {e}", level="WARN")
        return None


def _get_alpha_signals(tickers, info_cache=None):
    """선택 종목의 추가 알파 시그널 (어닝 서프/쇼크, 공매도)

    info_cache: fetch_revenue_growth에서 이미 수집한 {ticker: info_dict}
                공매도 + 어닝 서프라이즈 모두 info_cache에서 읽음 (추가 HTTP 호출 없음)
                어닝 서프는 전날 Top30 우선 종목에 대해서만 _fetch_one에서 수집됨
    """
    if info_cache is None:
        info_cache = {}
    results = {}
    for tk in tickers:
        sig = {'earnings_surp': None, 'short_pct': 0, 'short_mom': 0}
        info = info_cache.get(tk) or {}
        try:
            # 공매도: info_cache 재사용
            short_pct = info.get('shortPercentOfFloat', 0) or 0
            short_now = info.get('sharesShort', 0) or 0
            short_prior = info.get('sharesShortPriorMonth', 0) or 0
            sig['short_pct'] = short_pct * 100
            if short_prior > 0:
                sig['short_mom'] = (short_now - short_prior) / short_prior * 100
        except Exception:
            pass
        # 어닝 서프라이즈: info_cache에서 읽기 (fetch_revenue_growth에서 우선 종목만 수집됨)
        surp = info.get('_earnings_surp')
        if surp is not None:
            sig['earnings_surp'] = surp
        results[tk] = sig
    return results


def create_signal_message(selected, earnings_map, exit_reasons, biz_day, ai_content,
                          portfolio_mode, final_action,
                          weighted_ranks=None, filter_count=None,
                          status_map=None, eps_screened=None, universe_size=None,
                          exited_tickers=None, risk_status=None,
                          score_100_map=None, score_display_map=None, alpha_signals=None,
                          hist_all=None, new_buy_top2=None, today_str=None):
    """v3 Message 1: Signal — "오늘 뭘 사야 하나"

    종목당 4줄: 정체(이름·업종·가격) / 증거(EPS·매출) / 순위 / AI 내러티브
    시장 환경 없음 (AI Risk로 이동). 이탈 1줄 알림만.
    """
    import re

    if weighted_ranks is None:
        weighted_ranks = {}

    narratives = ai_content.get('narratives', {}) if ai_content else {}

    lines = []

    # ── 방어 국면 (regime defense) ──
    if portfolio_mode == 'defense':
        regime = risk_status.get('regime') if risk_status else None
        reason = regime.get('reason', '') if regime else ''
        transition = regime.get('transition') if regime else None
        lines.append('')
        if transition == 'to_defense':
            lines.append('🔄 <b>공격 → 방어 전환</b> (보유 주식 정리 권장)')
        lines.append('🛡️ <b>방어 국면 — 신규 매수 중단</b>')
        if reason:
            lines.append(f'사유: {reason}')
        lines.append('')
        lines.append('약세장 신호로 신규 매수를 멈춥니다.')
        lines.append('보유 종목은 매도 기준 그대로 적용 (10위 밖 &amp; 가격&lt;12일선 또는 이익전망↓).')
        lines.append('현금 또는 <b>IEF</b>(미국 중기 국채 ETF) 보유 권장.')
        lines.append('안전 우선 시 <b>BIL</b>(단기 국채). ※ 금리 급등기엔 장기채 회피.')
        lines.append('S&P 500이 200일선을 회복(15일 확인)하면 자동으로 매수 재개.')
        return '\n'.join(lines)

    # ── stop 모드 ──
    if portfolio_mode == 'stop':
        lines.append('')
        lines.append('🚫 <b>시장 경고 — 스크리닝 일시 중단</b>')
        lines.append(final_action)
        return '\n'.join(lines)

    # ── 상위권 유지 종목 없음 ──
    if not selected:
        lines.append('')
        lines.append('검증 종목 중 리스크 필터 통과 종목 없음.')
        lines.append('3일 연속 상위권 유지 종목이 없습니다.')
        return '\n'.join(lines)

    # ━━ 헤더 ━━
    biz_str = f'{biz_day.year}.{biz_day.month}.{biz_day.day}'
    weekdays = ['월', '화', '수', '목', '금', '토', '일']
    weekday = weekdays[biz_day.weekday()]
    lines.append(f'📡 <b>AI 종목 브리핑 US</b> · {biz_str}({weekday})')
    lines.append('월가 애널리스트의 이익 전망 변화를 추적해')
    lines.append('유망 종목을 매일 선별해 드립니다.')

    # 방어→공격 전환 알림 (regime이 방금 boost로 복귀)
    _rg = risk_status.get('regime') if risk_status else None
    if _rg and _rg.get('transition') == 'to_boost':
        lines.append('')
        lines.append('🔄 <b>방어 → 공격 전환 — 매수 재개</b>')
        lines.append('S&P 500이 200일선을 회복했습니다. 아래 후보로 복귀합니다.')

    # ━━ 시스템 성과 ━━ (v119: 배포일 이후 실제 성과만 — 백테스트 자랑 제거)
    try:
        perf = _get_system_performance(apply_epoch=True)
        if perf and perf['n_days'] >= 5:
            lines.append('')
            lines.append(f'📈 <b>시스템 누적 {perf["sys_cum"]:+.1f}%</b> ({perf["n_days"]}거래일)')
            lines.append(f'    S&P500 {perf["spy_cum"]:+.1f}% · 시뮬 기준')
    except Exception:
        pass

    # ━━ 섹션 1: 결론 먼저 ━━
    # v116 (2026-06-09): 기존 "오늘의 매수 후보" 명확 구조 유지 + 매도 한 줄 추가(Q2).
    #   v116 1차 시도(시스템포트폴리오/신규-기존 분기)는 오히려 혼란(사용자 피드백) → 폐기.
    #   단순 3리스트: 매수(오늘 살 것) / 보유(있으면 유지) / 매도(있으면 정리).
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')

    _ts = today_str or (biz_day.strftime('%Y-%m-%d') if hasattr(biz_day, 'strftime') else None)
    try:
        _prev_held = _replay_holdings(_ts, apply_epoch=True) if _ts else set()
    except Exception:
        _prev_held = set()
    sel = selected or []
    held_in_slot = [s for s in sel if s.get('_trend_hold')]
    sold_tks = [t for t in _prev_held if t not in {s['ticker'] for s in sel}]

    if new_buy_top2 is None:
        new_buy_top2 = [s for s in sel if not s.get('_trend_hold')]

    _nb_n = len(new_buy_top2) if new_buy_top2 else 0
    _w_hdr = ' (각 50%)' if _nb_n >= 2 else (' (100%)' if _nb_n == 1 else '')
    lines.append(f'🛒 <b>오늘의 매수 후보</b>{_w_hdr}')
    lines.append('━━━━━━━━━━━━━━━')
    if new_buy_top2:
        for idx, s in enumerate(new_buy_top2):
            name = _clean_company_name(s['name'], s['ticker'])
            w = s.get('weight', 0)
            w_tag = f' · {int(w)}%' if w else ''
            lines.append(f'<b>{idx+1}. {name}({s["ticker"]})</b>{w_tag}')
    else:
        lines.append('· 신규 매수 후보 없음 (보유 유지)')

    # v119 (2026-06-11): 보유 표시 제거 (B안) — 새 고객은 "오늘 살 것"만 보면 됨.
    #   🌟 보유줄은 "지금 못 사는 종목(순위 밖 보유)"이라 신규 진입자에게 혼란 → 제거.
    #   매도(🔴)는 실제 매도 발생 시에만 보유자 안내용으로 유지.
    if sold_tks:
        lines.append('')
        _name_cache = {}
        try:
            import json as _json
            with open(PROJECT_ROOT / 'ticker_info_cache.json', encoding='utf-8') as _f:
                _name_cache = _json.load(_f)
        except Exception:
            _name_cache = {}
        for t in sold_tks:
            _ci = _name_cache.get(t, {})
            nm = _clean_company_name(_ci.get('shortName', _ci.get('short_name', t)), t)
            # v119 매도 사유: EPS꺾임 / 저평가 해소(PE↑) / 순위 이탈
            _segr = None
            try:
                _cn = sqlite3.connect(DB_PATH); _cu = _cn.cursor()
                _rr = _cu.execute('SELECT seg1,seg2,seg3,seg4 FROM ntm_screening WHERE ticker=? AND date<=? ORDER BY date DESC LIMIT 1', (t, _ts)).fetchone()
                _cn.close()
                if _rr:
                    _ss = [x for x in _rr if x is not None]
                    _segr = min(_ss) if _ss else None
            except Exception:
                pass
            _per = _live_pe(t, _ts)
            if _segr is not None and _segr < -2:
                reason = '이익전망 꺾임'
            elif _per is not None and _per >= PE_HOLD:
                reason = f'PER {_per:.0f}배로 비싸짐'
            else:
                reason = '순위 이탈'
            lines.append(f'🔴 매도: {nm}({t}) · {reason}')

    # 주가 상관관계 표시 (90일 일간수익률 기준, 0.65 이상 페어만)
    # v111: 신규 매수 후보만 대상 (분산 권유는 '오늘 살 것' 한정).
    #   추세 보유(메가)는 이미 보유 중인 winner라 '택1' 권유 무의미 → 제외.
    try:
        tickers_list = [s['ticker'] for s in (new_buy_top2 or [])]
        # hist_all(1년치)에서 슬라이싱 — 추가 HTTP 호출 불필요
        close = None
        if hist_all is not None and 'Close' in hist_all.columns.get_level_values(0):
            try:
                close = hist_all['Close'][tickers_list].dropna(how='all')
            except (KeyError, TypeError):
                pass
        if close is None:
            import yfinance as yf
            hist = yf.download(tickers_list, period='120d', threads=True, progress=False)
            if 'Close' in hist.columns.get_level_values(0):
                close = hist['Close'].dropna(how='all')
        if close is not None and not close.empty:
            returns = close.pct_change().tail(90)
            corr_mat = returns.corr()
            high_corr_pairs = []
            for i in range(len(tickers_list)):
                for j in range(i+1, len(tickers_list)):
                    t1, t2 = tickers_list[i], tickers_list[j]
                    if t1 in corr_mat.columns and t2 in corr_mat.columns:
                        c = corr_mat.loc[t1, t2]
                        if c >= 0.65:
                            high_corr_pairs.append((t1, t2, c))
            if high_corr_pairs:
                # 페어를 연결 성분(connected component)으로 그룹핑 (BFS)
                from collections import defaultdict
                adj = defaultdict(set)
                pair_corr = {}
                for t1, t2, c in high_corr_pairs:
                    adj[t1].add(t2)
                    adj[t2].add(t1)
                    pair_corr[(t1, t2)] = c
                    pair_corr[(t2, t1)] = c
                visited = set()
                groups = []
                for t in tickers_list:
                    if t not in visited and t in adj:
                        group = []
                        stack = [t]
                        while stack:
                            node = stack.pop()
                            if node not in visited:
                                visited.add(node)
                                group.append(node)
                                stack.extend(adj[node] - visited)
                        group.sort(key=lambda x: tickers_list.index(x))
                        groups.append(group)
                if groups:
                    for g in groups:
                        # 유사도: 2종목=정확한 값, 3종목+=그룹 내 평균
                        corrs = [pair_corr[(g[i], g[j])]
                                 for i in range(len(g)) for j in range(i+1, len(g))
                                 if (g[i], g[j]) in pair_corr]
                        pct = int(round(sum(corrs) / len(corrs) * 100)) if corrs else 0
                        if len(g) >= 3:
                            lines.append(f'🔗 {"·".join(g)} 유사도 {pct}%')
                            lines.append(f'  → 택1~2 권장')
                        else:
                            lines.append(f'🔗 {"·".join(g)} 유사도 {pct}%')
                            lines.append(f'  → 택1 권장')
    except Exception as e:
        log(f"상관관계 계산 실패: {e}", level="WARN")

    # v71: 역변동성 비중 문구 삭제 (균등비중 전환)

    # ━━ 섹션 2: 선정 과정 ━━
    verified_count = sum(1 for v in (status_map or {}).values() if v == '✅')
    lines.append('')
    lines.append('📋 선정 과정')
    uni = universe_size or 959
    lines.append(f'미국 대형·중형주 {uni}종목')
    if eps_screened:
        lines.append(f'→ 애널리스트 이익 전망 상향 {eps_screened}종목')
    if filter_count:
        lines.append(f'→ 매출·마진·업종 품질 필터 {filter_count}종목')
    lines.append(f'→ 저평가 상위 20종목 매일 모니터링')
    lines.append(f'→ 거래대금 $1B+ 시장 주도주 필터')
    # v111: 퍼널은 신규 스크리닝 결과(매수 후보) 수를 설명 (selected=저평가보유는 별개 트랙).
    _n_screened = len(new_buy_top2) if new_buy_top2 else len(selected)
    lines.append(f'→ 3일 연속 상위 유지 {_n_screened}종목 선정')

    if alpha_signals is None:
        alpha_signals = {}

    # ━━ 섹션 3: 종목별 근거 (매수 후보만) ━━
    # v87 UX 재설계 (2026-06-03): 메가 영역 제거 (실 사용자 보유 X)
    # 종목별 근거 = new_buy_top2 (매수 후보)만 표시
    detail_list = list(new_buy_top2) if new_buy_top2 else []
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')
    lines.append('📌 <b>종목별 근거</b>')
    lines.append('━━━━━━━━━━━━━━━')

    new_buy_tks = {s['ticker'] for s in (new_buy_top2 or [])}
    for i, s in enumerate(detail_list):
        ticker = s['ticker']
        eps_chg = s['eps_chg']
        rev = _safe_float(s.get('rev_growth'))
        earnings_tag = s.get('earnings_note', '')

        # v119 (2026-06-11): 저평가 보유(fwd_PE<15) 종목은 "이미 보유한 경우만 유지" 표시.
        is_trend = s.get('_trend_hold') or (ticker not in new_buy_tks)
        if is_trend:
            weight_tag = ' · 저평가 보유'
            num_label = 'ℹ️'
        else:
            w = s.get('weight', 0)
            weight_tag = f' · {int(w)}%' if w else ''
            num_label = f'{i+1}.'

        # L0: 이름·업종·점수·비중
        display_name = _clean_company_name(s["name"], ticker)
        industry = s.get('industry', '')
        ind_str = f' · {industry}' if industry else ''
        score_str = ''
        if score_display_map and ticker in score_display_map:
            score_str = f' · {score_display_map[ticker]}점'
        lines.append(f'<b>{num_label} {display_name}({ticker}){ind_str}{score_str}{weight_tag}</b>{earnings_tag}')

        # L1: 증거 (EPS 전망 · 매출성장)
        growth_parts = []
        if eps_chg:
            growth_parts.append(f'EPS 전망 {int(round(eps_chg)):+d}%')
        if rev:
            growth_parts.append(f'매출성장 {int(round(rev * 100)):+d}%')
        lines.append(' · '.join(growth_parts))

        # v119 (2026-06-11): 저평가 보유 안내
        if s.get('_trend_hold'):
            _phe = s.get('_hold_pe')
            _pe_txt = f' PER {_phe:.0f}배' if (_phe is not None and _phe < 900) else ''
            lines.append(f'ℹ️ 저평가{_pe_txt} → 순위 밀려도 보유')

        # L2: 안정성 (순위 · 의견 · 저평가 streak)
        rev_up = int(s.get('rev_up', 0) or 0)
        rev_down = int(s.get('rev_down', 0) or 0)
        w_info = weighted_ranks.get(ticker)
        if w_info:
            r0, r1, r2 = w_info['r0'], w_info['r1'], w_info['r2']
            r2_s = str(r2) if r2 < 50 else '-'
            r1_s = str(r1) if r1 < 50 else '-'
            rank_str = f'{r2_s}→{r1_s}→{r0}위'
        else:
            rank_str = f'-→-→?위'
        rank_parts = [f'일별 {rank_str}']
        if rev_up or rev_down:
            rank_parts.append(f'의견 ↑{rev_up}↓{rev_down}')
        # 어닝 서프/공매도는 AI 내러티브에서 자연어로 표현 (v69)
        lines.append(' · '.join(rank_parts))

        # L3: 이야기 (AI 내러티브)
        narrative = narratives.get(ticker, '')
        if narrative:
            lines.append(f'💬 {narrative}')

        # 종목 간 구분선
        if i < len(selected) - 1:
            lines.append('─ ─ ─ ─ ─ ─ ─ ─')

    # ━━ 이탈 알림 (사유별 묶어서 표시) ━━
    if exit_reasons:
        from collections import defaultdict
        # v114: '저평가보유'(순위 밀렸지만 가격>MA12 보유 중)는 이탈 아님 → 이탈 목록에서 제외(미표시).
        real_exits = [(t, r, reason) for t, r, reason in exit_reasons if reason != '저평가보유']
        reason_groups = defaultdict(list)
        for t, _, reason in real_exits:
            reason_groups[reason or '순위밀림'].append(t)
        parts = []
        for reason, tickers in reason_groups.items():
            parts.append(f'{"·".join(tickers)}({reason})')
        if parts:
            lines.append('')
            lines.append(f'⚠️ 이탈: {" ".join(parts)}')
        # MA120 이탈 + 어제 상위권 종목 → 반등 관심 대상
        if exited_tickers:
            for t, _, reason in exit_reasons:
                if reason == '120일선↓':
                    prev_rank = exited_tickers.get(t)
                    if prev_rank is not None and prev_rank <= 10:
                        lines.append(f'💡 {t} — 120일선 이탈했지만 어제 {prev_rank}위, 반등 시 복귀 가능')

    # ━━ 범례 + 면책 ━━
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')
    lines.append('📌 <b>매매 규칙</b> (최대 2종목 · 각 50%)')
    lines.append('<b>매수</b>: 이익전망↑ + 저평가 상위 2 ($1B+)')
    lines.append('<b>보유</b>: 순위 10위 안, 또는 저평가(PER&lt;15)')
    lines.append('<b>매도</b>: 순위 10위 밖 + 비싸짐(PER 15↑)')
    lines.append('         또는 이익전망 꺾임')

    return '\n'.join(lines)


def _get_combined_return(hy_quadrant, vix_percentile):
    """HY 분면 × VIX 구간 조합 과거 S&P 연평균 수익률
    2000~2026 6,593거래일 SPY 20일 선행수익률 연환산 (bt_hy_vix_corrected.py 검증)"""
    RETURN_MATRIX = {
        'Q1': {'normal': 20.4, 'elevated': 23.7, 'high': 58.2, 'crisis': 39.6},
        'Q2': {'normal': 9.8, 'elevated': 14.8, 'high': 13.7, 'crisis': 15.2},
        'Q3': {'normal': 7.6, 'elevated': 5.3, 'high': 1.3, 'crisis': 15.3},
        'Q4': {'normal': 7.8, 'elevated': -12.1, 'high': 18.6, 'crisis': 18.9},
    }
    if vix_percentile < 67:
        vix_regime = 'normal'
    elif vix_percentile < 80:
        vix_regime = 'elevated'
    elif vix_percentile < 90:
        vix_regime = 'high'
    else:
        vix_regime = 'crisis'

    return RETURN_MATRIX.get(hy_quadrant, {}).get(vix_regime, 9.0)


def _credit_overall_status(hy_data, vix_data):
    """HY×VIX 조합 수익률 기반 종합 판정 (v65)

    🟢 수익률 ≥8%: 과거 수익률이 좋았던 구간
    🟡 수익률 <8%: 보통/낮았던 구간 (문구는 수익률에 따라)
    🔴 수익률 <5% AND (VIX ≥90p OR HY ≥90p): 실제 위기 구간

    VIX 95p 이상 → 최소 🟡 (공포 극대화 시점에 🟢 방지)
    """
    hy_q = hy_data.get('quadrant', 'Q2') if hy_data else 'Q2'
    vix_pct = vix_data.get('vix_percentile', 50) if vix_data else 50
    hy_pct = hy_data.get('hy_percentile', 50) if hy_data else 50

    combined_return = _get_combined_return(hy_q, vix_pct)

    # 🔴: 수익률 낮고 + 실제 지표도 극단
    is_extreme = (vix_pct >= 90) or (hy_pct >= 90)
    if combined_return < 5 and is_extreme:
        icon = '🔴'
    elif combined_return >= 8:
        icon = '🟢'
    else:
        icon = '🟡'

    # VIX 극단 시 최소 🟡 (Q1+crisis=28%여도 🟢 방지)
    if vix_pct >= 95 and icon == '🟢':
        icon = '🟡'

    # 문구는 실제 수익률에 맞게
    if combined_return >= 8:
        msg = '과거 수익률이 좋았던 구간입니다'
    elif combined_return >= 3:
        msg = '과거 수익률이 보통인 구간입니다'
    else:
        msg = '과거 수익률이 낮았던 구간입니다'

    return icon, msg, combined_return


def create_ai_risk_message(config, selected, biz_day, risk_status, market_lines,
                           earnings_map, ai_content):
    """v3 Message 2: AI 리스크 필터 — 시장 데이터+해석 통합

    📊 시장 환경 (데이터) + 📰 시장 동향 (AI 해석) + ⚠️ 매수 주의 (종목 리스크)
    """
    import re

    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append('  🤖 <b>AI 리스크 필터</b>')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append('상위 종목의 리스크 요소를 AI가 분석했습니다.')

    # ── 📊 시장 지수 ──
    lines.append('')
    lines.append('📊 <b>시장 지수</b>')

    if market_lines:
        idx_parts = []
        for ml in market_lines:
            m = re.match(r'[🟢🔴🟡]\s*(\S+(?:\s+\d+)?)\s+([\d,]+(?:\.\d+)?)\s+\(([^)]+)\)', ml)
            if m:
                name = m.group(1).replace(' 500', '').strip()
                val = m.group(2)
                chg = m.group(3)
                try:
                    chg_val = float(chg.replace('%', '').replace('+', ''))
                    chg = f'{chg_val:+.1f}%'
                except ValueError:
                    pass
                idx_parts.append(f'{name} {val}({chg})')
        if len(idx_parts) >= 3:
            lines.append(' · '.join(idx_parts[:2]))
            lines.append(' · '.join(idx_parts[2:]))
        elif idx_parts:
            lines.append(' · '.join(idx_parts))

    # ── 📉 신용·변동성 (v64: 1줄 결론 + 개별 근거) ──
    hy_data = risk_status.get('hy') if risk_status else None
    vix_data = risk_status.get('vix') if risk_status else None

    if hy_data or vix_data:
        lines.append('')
        lines.append('🏦 <b>신용·변동성</b>')

        # 종합 판정 (HY×VIX 조합 수익률 기반, v65)
        overall_icon, overall_msg, combined_ret = _credit_overall_status(hy_data, vix_data)
        lines.append(f'<b>{overall_icon} {overall_msg}</b>')

        # 개별 근거 (수치 + 퍼센타일)
        if hy_data:
            hy_pct = hy_data.get('hy_percentile', 50)
            lines.append(f'  회사채 금리차(HY): {hy_data["hy_spread"]:.2f}% (상위 {100 - hy_pct:.0f}%)')

        if vix_data:
            vix_cur = vix_data.get('vix_current', 0)
            vix_pct = vix_data.get('vix_percentile', 0)
            lines.append(f'  변동성지수(VIX): {vix_cur:.1f} (상위 {100 - vix_pct:.0f}%)')

        # 조합 과거 수익률
        lines.append(f'  → 이 구간 과거 S&P500 연평균 {combined_ret:+.1f}%')
    elif not hy_data and not vix_data:
        lines.append('')
        lines.append('🏦 <b>신용·변동성</b>')
        lines.append('⚠️ 시장 지표 수집 실패 — 보수적으로 접근하세요')

    # ── 📰 시장 동향 (AI 해석) ──
    market_summary = ai_content.get('market_summary', '') if ai_content else ''
    if market_summary:
        lines.append('')
        lines.append('📰 <b>시장 동향</b>')
        lines.append(market_summary)

    # ── ⚠️ 매수 주의 (14일 이내 어닝만) ──
    warnings = []
    if selected and earnings_map:
        # biz_day를 date로 통일
        biz_date = biz_day.date() if hasattr(biz_day, 'date') and callable(biz_day.date) else biz_day
        for s in selected:
            ticker = s['ticker']
            if ticker in earnings_map:
                ed_info = earnings_map[ticker]
                ed = ed_info['date']
                # ed를 date로 통일
                try:
                    ed_date = ed.date() if hasattr(ed, 'hour') else ed
                    days_until = (ed_date - biz_date).days
                except Exception:
                    continue  # 날짜 비교 실패 시 스킵
                if days_until < 0 or days_until > 14:
                    continue
                ah_tag = '(장후)' if ed_info['after_hours'] else ''
                warnings.append(f'{ticker} {ed_date.month}/{ed_date.day}{ah_tag} 실적발표 주의')

    if warnings:
        lines.append('')
        lines.append('⚠️ <b>매수 주의</b>')
        for w in warnings:
            lines.append(w)

    return '\n'.join(lines)


def create_watchlist_message(results_df, status_map, exit_reasons, today_tickers, biz_day,
                             weighted_ranks=None, score_100_map=None, score_display_map=None,
                             alpha_signals=None):
    """v3 Message 3: Watchlist — 상세 모니터링/검증

    종목당 4줄: 이름·업종 / EPS추이(아이콘+설명) / EPS·매출 / 의견+순위
    순위 변동 태그 제거. 이탈 사유 포함.
    """
    import pandas as pd
    from collections import Counter

    if results_df is None or results_df.empty:
        return None

    if weighted_ranks is None:
        weighted_ranks = {}
    if status_map is None:
        status_map = {}

    # DB의 가중순위 Top 30과 동일한 목록 사용
    if today_tickers:
        filtered = results_df[results_df['ticker'].isin(today_tickers)].copy()
    else:
        filtered = get_part2_candidates(results_df, top_n=30)

    # DB part2_rank 기준 정렬 (save_part2_ranks와 동일 순서 보장)
    if today_tickers:
        # today_tickers는 save_part2_ranks가 part2_rank 순으로 반환한 리스트
        ticker_order = {tk: i for i, tk in enumerate(today_tickers)}
        filtered = filtered.copy()
        filtered['_rank_order'] = filtered['ticker'].map(lambda t: ticker_order.get(t, 999))
        filtered = filtered.sort_values('_rank_order').reset_index(drop=True)
    elif weighted_ranks:
        filtered = filtered.copy()
        filtered['_weighted'] = filtered['ticker'].map(
            lambda t: weighted_ranks.get(t, {}).get('weighted', 50.0)
        )
        filtered = filtered.sort_values('_weighted').reset_index(drop=True)

    # min_seg < -2%: save_part2_ranks()에서 이미 제외됨 → 이탈은 classify_exit_reasons()에서 처리
    # -2% ≤ min_seg < 0%: 매수 불가지만 보유 추적용으로 표시 (⚠️ 마크)
    # v80.4 (2026-04-30): 저커버리지 (num_analysts < 3) 종목도 Watchlist에서 제외 —
    #   매수 후보 차단 필터와 일관성 (이전: Watchlist 표시되지만 매수 후보 X로 UX 혼란)
    healthy_rows = []
    caution_tickers = set()  # -2% ≤ min_seg < 0% — 매수 불가, 보유 추적용
    for _, row in filtered.iterrows():
        _segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
        _min_seg = min(_segs) if _segs else 0
        # 저커버리지 차단 (매수 후보 차단 필터와 일관성)
        n_analysts = int(row.get('num_analysts', 0) or 0)
        if n_analysts < 3:
            continue
        healthy_rows.append(row)
        if round(_min_seg, 1) < 0:
            caution_tickers.add(row['ticker'])
    import pandas as pd
    filtered = pd.DataFrame(healthy_rows).head(20) if healthy_rows else pd.DataFrame()

    if filtered.empty:
        return None

    lines = []
    lines.append('📋 <b>Top 20 종목 현황</b>')
    lines.append('EPS 상향 상위 20종목 현황입니다.')

    # 섹터 분포 표시
    sector_counts = Counter(row.get('industry', '?') for _, row in filtered.iterrows() if row.get('industry'))
    if sector_counts:
        top_sectors = sector_counts.most_common(5)
        etc_count = sum(c for _, c in sector_counts.most_common()[5:])
        sec_parts = [f'{s} {c}' for s, c in top_sectors]
        if etc_count > 0:
            sec_parts.append(f'기타 {etc_count}')
        lines.append(' | '.join(sec_parts))

    lines.append('✅ 검증완료 ⏳ 관찰중 🆕 신규 ⚠️ 전망둔화')
    lines.append('EPS추이(90→60→30→7일 변화율)')
    lines.append('🔥&gt;20% ☀️5~20% 🌤️1~5% ☁️±1% 🌧️&lt;-1%')
    lines.append('━━━━━━━━━━━━━━━')

    # ── 종목 리스트 (4줄 + 구분선) ── 추세둔화 종목은 위에서 이미 제외됨
    num_stocks = len(filtered)
    for idx, (_, row) in enumerate(filtered.iterrows()):
        rank = idx + 1
        ticker = row['ticker']
        industry = row.get('industry', '')
        lights = row.get('trend_lights', '')
        desc = row.get('trend_desc', '')
        eps_90d = row.get('eps_change_90d')
        rev_g = row.get('rev_growth')
        rev_up = int(row.get('rev_up30', 0) or 0)
        rev_down = int(row.get('rev_down30', 0) or 0)
        marker = status_map.get(ticker, '🆕')
        name = _clean_company_name(row.get('short_name', ticker), ticker)

        # L0: 이름·업종 (20자 제한)
        short_name = name
        if len(name) > 20:
            words = name.split()
            short_name = words[0]
            for w in words[1:]:
                if len(short_name) + 1 + len(w) <= 20:
                    short_name += ' ' + w
                else:
                    break
        ind_tag = f' · {industry}' if industry else ''
        caution_tag = ' ⚠️' if ticker in caution_tickers else ''
        # v80.10c (2026-05-11): ⏸️ 매도 유예 표시 제거 — v80.10 장기 가중치 전환 후
        # BT 검증 결과 유예 룰 N=0이 모든 N>0보다 우월 (paired 100/100). 단기 가중치
        # 체제의 노이즈 완충 알파였던 ⏸️ 룰이 v80.10 환경에선 -5.37%p 손해.
        # check_breakout_hold 함수는 코드에 유지 (참고/회귀용).
        lines.append(f'{marker} <b>{rank}. {short_name}({ticker})</b>{ind_tag}{caution_tag}')

        # L1: EPS추이 아이콘 + 설명
        if lights and desc:
            lines.append(f'EPS추이 {lights} {desc}')
        elif lights:
            lines.append(f'EPS추이 {lights}')

        # L2: EPS 전망 · 매출성장 · 점수
        growth_parts = []
        if eps_90d is not None and pd.notna(eps_90d):
            growth_parts.append(f'EPS 전망 {int(round(eps_90d)):+d}%')
        if rev_g is not None and pd.notna(rev_g):
            growth_parts.append(f'매출성장 {int(round(rev_g * 100)):+d}%')
        if score_display_map and ticker in score_display_map:
            growth_parts.append(f'{score_display_map[ticker]}점')
        lines.append(' · '.join(growth_parts))

        # L3: 의견 + 순위
        w_info = weighted_ranks.get(ticker)
        if w_info:
            r0, r1, r2 = w_info['r0'], w_info['r1'], w_info['r2']
            if marker == '🆕':
                rank_str = f'-→-→{r0}위'
            elif marker == '⏳':
                r1_s = str(r1) if r1 < 50 else '-'
                rank_str = f'-→{r1_s}→{r0}위'
            else:
                r2_s = str(r2) if r2 < 50 else '-'
                r1_s = str(r1) if r1 < 50 else '-'
                rank_str = f'{r2_s}→{r1_s}→{r0}위'
        else:
            rank_str = f'-→-→{rank}위'
        rank_parts = [f'일별 {rank_str}', f'의견 ↑{rev_up}↓{rev_down}']
        # 어닝 서프/공매도는 Signal 메시지의 AI 내러티브에서 표현 (v69)
        lines.append(' · '.join(rank_parts))

        # 매도 기준선 (10위 아래 = 퇴출 대상, v80.10)
        if rank == 10 and num_stocks > 10:
            lines.append('── 매도 기준선 ──')
        # 점선 구분선
        elif rank < num_stocks:
            lines.append('- - - - -')

    # ── 메가 영역 — v87 UX 재설계 (2026-06-03): 제거 ──
    # 사용자 분노: "지나간 홀드 종목 보여줘봤자 약올림" — 모든 고객 SNDK 매도 완료
    # 미래 carryover 안내는 footer 운영 규칙에

    # ── 순위 이탈 (사유별 묶어서 표시) — v111: '저평가보유'(보유 중)는 이탈 아님 ──
    if exit_reasons:
        from collections import defaultdict
        reason_groups = defaultdict(list)
        for t, _, reason in exit_reasons:
            if reason == '저평가보유':  # 보유 종목이 순위만 밀린 것 → 이탈 아님
                continue
            reason_groups[reason or '순위밀림'].append(t)
        if reason_groups:
            parts = []
            for reason, tickers in reason_groups.items():
                parts.append(f'{"·".join(tickers)}({reason})')
            lines.append('')
            lines.append('━━━━━━━━━━━━━━━')
            lines.append(f'📉 이탈: {" ".join(parts)}')

    # ── 범례 ──
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')
    lines.append('📌 <b>매매 규칙</b> (최대 2종목 · 각 50%)')
    lines.append('<b>매수</b>: 이익전망↑ + 저평가 상위 2 ($1B+)')
    lines.append('<b>보유</b>: 순위 10위 안, 또는 저평가(PER&lt;15)')
    lines.append('<b>매도</b>: 순위 10위 밖 + 비싸짐(PER 15↑)')
    lines.append('         또는 이익전망 꺾임')
    lines.append('※ 누적수익률은 시뮬 기준 (세금·수수료 미반영)')

    return '\n'.join(lines)


# ── ETF 후보 리스트 (섹터/테마별) ──
ETF_CANDIDATES = [
    # 반도체
    'SMH', 'SOXX', 'XSD', 'PSI', 'SOXQ',
    # 테크
    'IGV', 'SKYY', 'WCLD', 'QTEC', 'FXL', 'RSPT', 'RYT', 'XITK',
    # 통신/5G
    'FIVG', 'VOX', 'NXTG',
    # 산업재
    'XLI', 'VIS', 'PAVE', 'AIRR', 'FIDU', 'RGI',
    # 건설/인프라
    'ITB', 'PKB', 'IFRA',
    # 헬스케어
    'XLV', 'VHT', 'IHI', 'XBI', 'XHE', 'RYH',
    # 금융
    'XLF', 'KBE', 'KRE', 'IAI', 'KIE', 'IAK', 'KBWB',
    # 에너지/해운
    'XLE', 'XOP', 'BOAT',
    # 소비재
    'XLY', 'XLP', 'XRT', 'FDIS',
    # 방산
    'ITA', 'PPA', 'XAR',
    # 소재
    'XLB', 'XME',
    # AI/로보틱스
    'BOTZ', 'ROBO', 'AIQ',
    # 클린에너지
    'ICLN', 'TAN',
    # 기타 테마
    'ARKK', 'ARKW', 'ARKG', 'ARKQ',
    # Mid/Small cap
    'IJH', 'IJR', 'MDY', 'IWM', 'VO', 'VB', 'SCHA', 'SLYG',
    # Value/Factor
    'VTV', 'SCHD', 'VBR', 'SMOT',
]


def find_etf_recommendations(top30_tickers):
    """전체 홀딩 기반 ETF 매칭 — Top 30 종목 매칭 수 + 비중 합계 기준

    v2: etf-scraper 기반 전체 홀딩 캐시 사용 (기존 Top 10만 → 전체)
    Returns: list of {'ticker', 'name', 'matched', 'overlap_pct', 'match_count'} or empty list
    """
    top30_set = set(top30_tickers)

    # ── Step 1: v2 캐시 우선, 없으면 v1 fallback ──
    cache_path = PROJECT_ROOT / 'etf_holdings_cache_v2.json'
    if not cache_path.exists():
        cache_path = PROJECT_ROOT / 'etf_holdings_cache.json'
    etf_cache = {}
    if cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                etf_cache = json.load(f)
            log(f"ETF 캐시 로드: {len(etf_cache)}개 ETF ({cache_path.name})")
        except Exception as e:
            log(f"ETF 캐시 로드 실패: {e}", "WARN")

    if not etf_cache:
        log("ETF 캐시 없음 — ETF 추천 스킵", "WARN")
        return []

    # ── Step 2: 각 ETF의 Top 30 매칭 계산 ──
    #   매칭 종목의 평균 비중이 1% 미만이면 제외 (희석된 ETF 필터링)
    MIN_AVG_WEIGHT = 0.01  # 1%
    etf_scores = []
    for etf_t, data in etf_cache.items():
        holdings = data.get('holdings', {})
        if isinstance(holdings, list):
            continue
        matched = {h: w for h, w in holdings.items() if h in top30_set}
        if matched:
            avg_weight = sum(matched.values()) / len(matched)
            if avg_weight < MIN_AVG_WEIGHT:
                continue
            etf_scores.append({
                'ticker': etf_t,
                'name': data.get('name', etf_t),
                'matched_detail': matched,
                'matched': sorted(matched.keys(), key=lambda h: matched[h], reverse=True),
                'overlap_pct': sum(matched.values()),
                'match_count': len(matched),
            })

    # ── Step 3: 매칭 수 → 비중 순 정렬 → 중복 제거 → Top 5 ──
    #   기존 커버 종목과 50% 이상 겹치면 스킵 (섹터 다양성 확보)
    etf_scores.sort(key=lambda x: (x['match_count'], x['overlap_pct']), reverse=True)
    selected = []
    covered_tickers = set()
    for etf in etf_scores:
        ticker_set = set(etf['matched'])
        new_tickers = ticker_set - covered_tickers
        if not new_tickers:
            continue
        if covered_tickers and len(new_tickers) / len(ticker_set) < 0.5:
            continue
        selected.append(etf)
        covered_tickers.update(ticker_set)
        if len(selected) >= 5:
            break

    # ── Step 4: 커버 안 되는 종목 리스트 ──
    uncovered = [t for t in top30_tickers if t not in covered_tickers]

    if selected:
        top_info = ', '.join(f"{s['ticker']}({s['match_count']}종목)" for s in selected)
        log(f"ETF 추천: {top_info} (커버 {len(covered_tickers)}/{len(top30_tickers)}종목, 미커버 {len(uncovered)})")
    else:
        log("ETF 추천: 매칭 ETF 없음")

    return selected, uncovered


def create_etf_message(etf_results, biz_day, uncovered=None, top30_count=30):
    """v3 Message 4: 관련 ETF — 전체 홀딩 기반 매칭

    2종목 이상 포함 ETF만 표시. ETF 이름(섹터), 매칭 비중 포함.
    """
    if not etf_results:
        return None

    # 2종목 이상 포함 ETF만 필터
    meaningful = [e for e in etf_results
                  if e.get('match_count', len(e.get('matched', []))) >= 2]
    if not meaningful:
        return None

    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append('  📊 <b>관련 ETF</b>')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append('Top 30 종목을 여러 개 담고 있는 ETF입니다.')
    lines.append('개별 종목 대신 ETF로 분산 투자할 수 있습니다.')
    lines.append('')

    for i, etf in enumerate(meaningful, 1):
        matched_detail = etf.get('matched_detail', {})
        matched = etf.get('matched', [])
        cnt = etf.get('match_count', len(matched))
        overlap = etf.get('overlap_pct', 0)
        etf_name = etf.get('name', etf['ticker'])

        # L0: ETF 티커 + 이름(섹터) + 매칭 요약
        lines.append(f'<b>{etf["ticker"]}</b> {etf_name}')
        lines.append(f'{cnt}종목 포함 · 합산비중 {overlap*100:.1f}%')

        # 매칭 종목별 비중 (비중 높은 순 — 이미 sorted)
        stock_parts = []
        for t in matched:
            w = matched_detail.get(t, 0)
            stock_parts.append(f'{t}({w*100:.1f}%)')
        lines.append(f'  {", ".join(stock_parts)}')

        if i < len(meaningful):
            lines.append('')

    lines.append('')
    lines.append(f'<i>{biz_day.strftime("%m/%d")} 기준</i>')

    return '\n'.join(lines)


# ============================================================
# 텔레그램 전송
# ============================================================

def _sanitize_telegram_html(text):
    """Telegram HTML 안전하게 정리 — 허용 태그만 유지, 나머지 < > & 이스케이프"""
    import re
    # 허용 태그를 플레이스홀더로 대체
    _ALLOWED = re.compile(r'<(/?)([bi]|strong|em|u|ins|s|strike|del|code|pre|blockquote)(\s[^>]*)?>',
                          re.IGNORECASE)
    placeholders = []

    def _save(m):
        placeholders.append(m.group(0))
        return f'\x00PH{len(placeholders)-1}\x00'

    text = _ALLOWED.sub(_save, text)
    # <a href="...">도 허용
    _A_TAG = re.compile(r'<(/?)a(\s[^>]*)?>',  re.IGNORECASE)
    text = _A_TAG.sub(_save, text)
    # 기존 HTML 엔티티 보호 (&gt; &lt; &amp; &#123; &#x1F; 등)
    _ENTITY = re.compile(r'&(amp|lt|gt|quot|#[0-9]+|#x[0-9a-fA-F]+);')
    text = _ENTITY.sub(_save, text)
    # 남은 <, >, & 이스케이프
    text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    # 플레이스홀더 복원
    for i, original in enumerate(placeholders):
        text = text.replace(f'\x00PH{i}\x00', original)
    return text


def _send_personal_alert(config, msg):
    """개인봇 DM 경고만 발송 (채널 X) — 데이터 수집 사고 알림용 (v86e++ 2026-06-03)."""
    try:
        if not config.get('telegram_enabled', False):
            log(f"[건강성 경고/telegram off] {msg}", "WARN")
            return
        pid = config.get('telegram_private_id') or config.get('telegram_chat_id')
        if pid:
            send_telegram_long(msg, config, chat_id=pid)
    except Exception as e:
        log(f"_send_personal_alert 오류: {e}", "WARN")


def send_telegram_long(message, config, chat_id=None):
    """긴 메시지를 여러 개로 분할해서 전송 (chat_id 지정 가능)"""
    if not config.get('telegram_enabled', False):
        return False

    bot_token = config.get('telegram_bot_token', '')
    if chat_id is None:
        chat_id = config.get('telegram_chat_id', '')

    if not bot_token or not chat_id:
        log("텔레그램 설정 불완전", "WARN")
        return False

    try:
        import urllib.request
        import urllib.parse

        # HTML 안전 정리
        message = _sanitize_telegram_html(message)

        # 4000자씩 분할
        chunks = []
        remaining = message.strip()
        while remaining:
            if len(remaining) <= 4000:
                chunks.append(remaining)
                break
            else:
                split_point = remaining[:4000].rfind('\n')
                if split_point <= 0:
                    split_point = 4000
                chunks.append(remaining[:split_point])
                remaining = remaining[split_point:].strip()

        # 빈 청크 제거
        chunks = [c for c in chunks if c.strip()]

        for i, chunk in enumerate(chunks):
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

            data = urllib.parse.urlencode({
                'chat_id': chat_id,
                'text': chunk,
                'parse_mode': 'HTML'
            }).encode()

            req = urllib.request.Request(url, data=data)
            urllib.request.urlopen(req, timeout=10)

        log(f"텔레그램 전송 완료 ({len(chunks)}개 메시지)")
        return True

    except Exception as e:
        detail = ''
        if hasattr(e, 'read'):
            try:
                detail = e.read().decode('utf-8', errors='replace')
            except Exception:
                pass
        log(f"텔레그램 전송 실패: {e} {detail}", "ERROR")
        return False


# ============================================================
# 메인 실행
# ============================================================

def main():
    """NTM EPS 시스템 v31 메인 실행 — Balanced Review"""
    log("=" * 60)
    log("EPS Momentum Daily Runner v31 - Balanced Review")
    log("=" * 60)

    start_time = datetime.now()

    # 설정 로드
    config = load_config()
    log(f"설정 로드 완료: {CONFIG_PATH}")

    # 1. NTM 데이터 수집 + DB 적재 (MA60, price 포함)
    log("=" * 60)
    log("NTM EPS 데이터 수집 시작")
    log("=" * 60)
    results_df, turnaround_df, stats, today_str, hist_all = run_ntm_collection(config)

    # ── 수집 건강성 가드 (v86e++ 2026-06-03, KR <150 안전망 이식) ──
    # 2026-05-28~29 yfinance 대량실패 사고(에러53%, 수집600/315) 재발 방지.
    # 미달 시 30분 후 1회 재수집, 그래도 미달이면 랭킹 미기록 + 채널 발송 차단(개인봇 알림만).
    # historical mode는 fetch 안 하므로 스킵.
    if not is_historical_mode():
        _hok, _hreason = _validate_collection_health(stats)
        if not _hok:
            log(f"⚠️ 수집 건강성 미달: {_hreason} — 30분 후 재수집 시도", "WARN")
            _send_personal_alert(config, f"⚠️ <b>수집 건강성 미달</b>\n{_hreason}\n\n채널 발송 보류, 30분 후 재수집 시도.")
            import time as _time_guard
            _time_guard.sleep(1800)
            log("재수집 시도...")
            try:
                results_df, turnaround_df, stats, today_str, hist_all = run_ntm_collection(config)
            except Exception as _e_guard:
                log(f"재수집 오류: {_e_guard}", "WARN")
            _hok, _hreason = _validate_collection_health(stats)
            if not _hok:
                log(f"❌ 재시도 후에도 미달: {_hreason} — 랭킹 미기록 + 채널 발송 차단", "WARN")
                _send_personal_alert(config, f"❌ <b>재시도 후에도 수집 미달</b>\n{_hreason}\n\n오늘 채널 발송 보류, 랭킹 미기록. 수동 점검 필요.")
                stats['data_unhealthy'] = True
            else:
                log(f"✅ 재수집 통과: {_hreason}")

    # 2. Part 2 rank 저장 + 3일 교집합 + 어제 대비 변동
    import pandas as pd

    status_map = {}
    rank_history = {}
    weighted_ranks = {}
    rank_change_tags = {}
    exited_tickers = {}
    today_tickers = []
    earnings_map = {}

    # 2.5. 시장 지수 — hist_all에서 추출 (추가 yfinance 호출 없음, rate limit 안전)
    market_lines = get_market_context(hist_all=hist_all)
    if market_lines:
        log(f"시장 지수: {len(market_lines)}개")

    if not results_df.empty:
        if is_historical_mode():
            # v83.1: HISTORICAL MODE — fetch_revenue_growth (yfinance) skip + DB의 part2_rank 그대로 사용
            log("⚠️ HISTORICAL MODE: fetch_revenue_growth SKIP + save_part2_ranks SKIP (DB write 차단)")
            earnings_map = {}
            info_cache = {}
            # DB에서 today_str의 part2_rank 순서대로 today_tickers 로드
            import sqlite3
            _conn = sqlite3.connect(DB_PATH)
            today_tickers = [r[0] for r in _conn.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
                (today_str,)
            ).fetchall()]
            _conn.close()
            log(f"  DB의 {today_str} part2_rank Top 30: {len(today_tickers)}종목")
        else:
            # 매출+품질 수집 → rev_growth composite score + 12개 재무지표 DB 저장 (v33)
            results_df, earnings_map, info_cache = fetch_revenue_growth(results_df, today_str)

            # 가중순위 기반 Top 30 선정 + DB 저장 (건강성 미달 시 랭킹 미기록)
            if stats.get('data_unhealthy'):
                log("⚠️ 건강성 미달 → save_part2_ranks 스킵 (망가진 랭킹 미기록)")
                today_tickers = []
            else:
                today_tickers = save_part2_ranks(results_df, today_str) or []

        status_map = get_3day_status(today_tickers, today_str)
        rank_history = get_rank_history(today_tickers, today_str)
        weighted_ranks = compute_weighted_ranks(today_tickers, today_str)
        rank_change_tags = get_rank_change_tags(today_tickers, weighted_ranks)
        _, exited_tickers = get_daily_changes(today_tickers, today_str)

    stats['exited_count'] = len(exited_tickers) if exited_tickers else 0

    # HY Spread + VIX 수집 (FRED — yfinance와 별개)
    risk_status = get_market_risk_status()
    hy_data = risk_status['hy']
    vix_data = risk_status['vix']
    if hy_data:
        log(f"HY Spread: {hy_data['hy_spread']:.2f}% | 분면: {hy_data['quadrant']} {hy_data['quadrant_label']} ({hy_data['q_days']}일째)")
        log(f"  {hy_data['action']}")
        if hy_data['signals']:
            for sig in hy_data['signals']:
                log(f"  해빙 신호: {sig}")
    if vix_data:
        log(f"VIX: {vix_data['vix_current']:.1f} (252일 {vix_data.get('vix_percentile', 0):.0f}th) | slope {vix_data['vix_slope']:+.1f} ({vix_data['vix_slope_dir']}) | {vix_data['regime_label']}")
    log(f"일치도: {risk_status['concordance']} | {risk_status['final_action']}")

    # 3. 메시지 생성
    # 섹터 모멘텀 (개인봇 로그용)
    sector_summary = analyze_sector_momentum(results_df, today_str=today_str)
    if sector_summary:
        stats['sector_summary'] = sector_summary

    # 실행 시간
    elapsed = (datetime.now() - start_time).total_seconds()
    msg_log = create_system_log_message(stats, elapsed, config)

    # 4. 텔레그램 발송
    if config.get('telegram_enabled', False):
        is_github = config.get('is_github_actions', False)
        private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')
        channel_id = config.get('telegram_channel_id')

        # cold start: 3일 미만 데이터 → 채널 전송 안함 (개인봇만)
        cold_start = is_cold_start()
        send_to_channel = is_github and channel_id and not cold_start and not stats.get('data_unhealthy')
        if cold_start:
            log(f"Cold start — 채널 전송 비활성화 (3일 데이터 축적 전)")
        if stats.get('data_unhealthy'):
            log("⚠️ 데이터 건강성 미달 — 채널 전송 차단 (개인봇만)")

        dest = '채널+개인봇' if send_to_channel else '개인봇'
        biz_day = get_last_business_day()

        # ===== v3: Signal + AI Risk + Watchlist =====

        # risk_status에서 공통 값 추출
        concordance = risk_status.get('concordance', 'both_stable') if risk_status else 'both_stable'
        final_action = risk_status.get('final_action', '') if risk_status else ''
        portfolio_mode = risk_status.get('portfolio_mode', 'normal') if risk_status else 'normal'

        # conviction w_gap 맵(순위용) + 퍼센타일 점수 맵(표시용)
        score_100_map, score_display_map = _build_score_100_map(today_str)

        # 디스플레이용 종목 선정 — v87 (2026-06-03): selected (시뮬) + new_buy_top2 (신규)
        display_top5, new_buy_top2 = select_display_top5(
            results_df, status_map, weighted_ranks, earnings_map, risk_status,
            score_100_map=score_100_map, hist_all=hist_all, today_str=today_str
        )

        # 이탈 종목 사유 분류
        exit_reasons = classify_exit_reasons(exited_tickers, results_df)

        # 필터 통과 종목 수
        if not results_df.empty:
            _, funnel_counts = get_part2_candidates(results_df, return_counts=True)
            eps_screened = funnel_counts['eps_screened']
            filter_count = funnel_counts['quality_filtered']
        else:
            eps_screened, filter_count = 0, 0

        # 알파 시그널 (Top 20, 전부 info_cache에서 — 추가 HTTP 호출 없음)
        # AI 내러티브에서 어닝서프/공매도를 자연어로 녹이기 위해 AI 호출 전에 수집
        try:
            watchlist_tickers = [t for t in today_tickers[:20]]
            alpha_signals = _get_alpha_signals(watchlist_tickers, info_cache=info_cache)
            log(f"알파 시그널: {', '.join(f'{tk}' for tk, v in alpha_signals.items() if v.get('earnings_surp') and (v['earnings_surp'] > 0.3 or v['earnings_surp'] < 0))}")
        except Exception as e:
            log(f"알파 시그널 수집 실패: {e}", level="WARN")
            alpha_signals = {}

        # AI 2회 호출 — v87 (2026-06-03): display_top5 + new_buy_top2 union 입력
        # selected에 없는 신규 매수후보 (VIRT 등) narrative 누락 방지
        ai_input = list(display_top5)
        existing_tks = {s['ticker'] for s in display_top5}
        for s in (new_buy_top2 or []):
            if s['ticker'] not in existing_tks:
                ai_input.append(s)
                existing_tks.add(s['ticker'])
        ai_content = run_ai_analysis(config, ai_input, biz_day, risk_status,
                                     market_lines=market_lines,
                                     alpha_signals=alpha_signals)

        # 메시지 1: Signal — v57b 진입 조건 충족 종목
        msg_signal = create_signal_message(
            display_top5, earnings_map, exit_reasons, biz_day, ai_content,
            portfolio_mode, final_action,
            weighted_ranks=weighted_ranks, filter_count=filter_count,
            status_map=status_map, eps_screened=eps_screened,
            universe_size=stats.get('universe'),
            exited_tickers=exited_tickers, risk_status=risk_status,
            score_100_map=score_100_map, score_display_map=score_display_map,
            alpha_signals=alpha_signals, hist_all=hist_all,
            new_buy_top2=new_buy_top2, today_str=today_str,
        )
        if msg_signal:
            if send_to_channel:
                send_telegram_long(msg_signal, config, chat_id=channel_id)
            send_telegram_long(msg_signal, config, chat_id=private_id)
            log(f"Signal 전송 완료 → {dest}")

        # 메시지 2: AI 리스크 필터
        msg_ai_risk = create_ai_risk_message(
            config, display_top5, biz_day, risk_status, market_lines,
            earnings_map, ai_content
        )
        if msg_ai_risk:
            if send_to_channel:
                send_telegram_long(msg_ai_risk, config, chat_id=channel_id)
            send_telegram_long(msg_ai_risk, config, chat_id=private_id)
            log(f"AI Risk 전송 완료 → {dest}")

        # 메시지 3: Watchlist
        msg_watchlist = create_watchlist_message(
            results_df, status_map, exit_reasons, today_tickers, biz_day,
            weighted_ranks=weighted_ranks, score_100_map=score_100_map,
            score_display_map=score_display_map, alpha_signals=alpha_signals
        )
        if msg_watchlist:
            if send_to_channel:
                send_telegram_long(msg_watchlist, config, chat_id=channel_id)
            send_telegram_long(msg_watchlist, config, chat_id=private_id)
            log(f"Watchlist 전송 완료 → {dest}")

        # 메시지 4: 관련 ETF — 제거됨 (v52)

        # 시스템 로그 → 개인봇에만
        send_telegram_long(msg_log, config, chat_id=private_id)
        log("시스템 로그 전송 완료 → 개인봇")

    # 5. Git commit/push
    git_commit_push(config)

    # 완료
    elapsed = (datetime.now() - start_time).total_seconds()
    log("=" * 60)
    log(f"전체 완료: {elapsed:.1f}초 소요")
    log("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
