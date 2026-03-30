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
                          ('rev_up30', 'INTEGER'), ('rev_down30', 'INTEGER'), ('num_analysts', 'INTEGER')]:
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


def run_ntm_collection(config):
    """NTM EPS 전 종목 수집 & DB 적재

    최적화:
    - 가격 데이터: yf.download() 일괄 다운로드 (내장 스레딩)
    - 종목 정보: JSON 캐시 (shortName, industry)
    - EPS 데이터: 순차 처리 (yfinance 스레딩 비호환)

    Returns:
        tuple (results_df, turnaround_df, stats_dict)
    """
    import yfinance as yf
    import pandas as pd

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

    log(f"NTM EPS 병렬 수집 중 (5스레드, {len(eps_tickers)}종목)...")
    _t_eps = __import__('time').time()
    _prefetched = {}
    BATCH_SIZE = 50
    for batch_start in range(0, len(eps_tickers), BATCH_SIZE):
        batch = eps_tickers[batch_start:batch_start + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_prefetch_eps, t): t for t in batch}
            for future in as_completed(futures):
                result = future.result()
                _prefetched[result[0]] = result[1]
        done_count = batch_start + len(batch)
        if done_count % 200 < BATCH_SIZE:
            log(f"  수집: {done_count}/{len(eps_tickers)}")
        if batch_start + BATCH_SIZE < len(eps_tickers):
            __import__('time').sleep(0.5)
    # 에러 종목 1회 재시도 (rate limit 해소 후)
    error_tickers = [t for t, d in _prefetched.items() if 'error' in d]
    if error_tickers:
        log(f"EPS 재시도: {len(error_tickers)}종목 (3초 대기 후)")
        __import__('time').sleep(3)
        for batch_start in range(0, len(error_tickers), BATCH_SIZE):
            batch = error_tickers[batch_start:batch_start + BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(_prefetch_eps, t): t for t in batch}
                for future in as_completed(futures):
                    t, data = future.result()
                    if 'error' not in data:
                        _prefetched[t] = data
            if batch_start + BATCH_SIZE < len(error_tickers):
                __import__('time').sleep(0.5)
        retry_ok = sum(1 for t in error_tickers if 'error' not in _prefetched[t])
        log(f"  재시도 복구: {retry_ok}/{len(error_tickers)}")
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

                    # 각 주기별 괴리율 → 가중평균
                    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
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
            adj_gap = None
            if fwd_pe_chg is not None and direction is not None:
                dir_factor = max(-0.3, min(0.3, direction / 30))
                min_seg = min(seg1 or 0, seg2 or 0, seg3 or 0, seg4 or 0)
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
            }

            # DB에 파생 데이터 업데이트
            cursor.execute('''
                UPDATE ntm_screening
                SET adj_score=?, adj_gap=?, price=?, ma60=?, ma120=?,
                    rev_up30=?, rev_down30=?, num_analysts=?, eps_chg_weighted=?
                WHERE date=? AND ticker=?
            ''', (adj_score, adj_gap, current_price, ma60_val, ma120_val,
                  rev_up30, rev_down30, num_analysts, eps_chg_weighted,
                  today_str, ticker))

            if is_turnaround:
                turnaround.append(row)
            else:
                results.append(row)

        except Exception as e:
            errors.append((ticker, str(e)))
            continue

    conn.commit()

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
        """단일 종목 .info 수집 (+ 우선 종목은 earnings_history도)"""
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

    df['rev_growth'] = df['ticker'].map(rev_map)

    # margin 데이터도 dataframe에 추가 (구조적 저마진 필터용)
    om_map = {t: results[t].get('operatingMargins') for t in results if results[t]}
    gm_map = {t: results[t].get('grossMargins') for t in results if results[t]}
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
    # conviction 적용: adj_gap × (1 + max(rev_up/N, eps_floor))
    def _conv_gap(row):
        ag = float(row.get('adj_gap') or 0)
        up = float(row.get('rev_up30') or 0)
        na = float(row.get('num_analysts') or 0)
        nc = float(row.get('ntm_current') or 0)
        n90 = float(row.get('ntm_90d') or 0)
        return _apply_conviction(ag, up, na, nc, n90)
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

    # 2. w_gap(3일 가중 adj_gap) 기준 Top 30 → part2_rank (v58)
    eligible_tickers = list(composite_ranks.keys())
    wgap_map = _compute_w_gap_map(cursor, today_str, eligible_tickers)
    sorted_by_wgap = sorted(eligible_tickers, key=lambda tk: wgap_map.get(tk, 0))
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
    return top30_tickers


def _apply_conviction(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None):
    """adj_gap에 애널리스트 합의 배율 적용 (v71)

    conviction = max(rev_up30/num_analysts, eps_floor)
    - rev_up30/num_analysts: 최근 30일 상향 비율 (0.0~1.0)
    - eps_floor: min(|EPS변화율|/100, 1.0) — 30일 창 밖 대규모 상향 보완
    배율 = 1 + conviction (범위 1.0~2.0)
    """
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def _compute_w_gap_map(cursor, today_str, tickers):
    """w_gap(3일 가중 conviction adj_gap) 계산 — T0×0.5 + T1×0.3 + T2×0.2

    v71: adj_gap × (1 + rev_up30/num_analysts) conviction 배율 적용
    Returns: {ticker: float(w_gap)}
    """
    dates = _get_recent_dates(cursor, 'composite_rank', today_str, 3)
    dates = sorted(dates)  # 오래된 순

    gap_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d '
            'FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
            (d,)
        ).fetchall()
        gap_by_date[d] = {r[0]: _apply_conviction(r[1], r[2], r[3], r[4], r[5]) for r in rows}

    weights = [0.2, 0.3, 0.5]  # T-2, T-1, T0 (오래된순)
    if len(dates) == 2:
        weights = [0.4, 0.6]
    elif len(dates) == 1:
        weights = [1.0]

    result = {}
    for tk in tickers:
        wg = 0
        for i, d in enumerate(dates):
            wg += gap_by_date.get(d, {}).get(tk, 0) * weights[i]
        result[tk] = wg
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
    """3일 순위 궤적 — part2_rank(conviction w_gap) 기반 (v71)
    T0 × 0.5 + T1 × 0.3 + T2 × 0.2
    Returns: {ticker: {'weighted': float, 'r0': int, 'r1': int, 'r2': int}}
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = sorted(_get_recent_dates(cursor, 'part2_rank', today_str, 3))

    if not dates:
        conn.close()
        return {}

    PENALTY = 50

    rank_by_date = {}
    for d in dates:
        cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
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


def fetch_hy_quadrant():
    """HY Spread Verdad 4분면 + 해빙 신호 (FRED BAMLH0A0HYM2)

    수준: HY vs 10년 롤링 중위수 (넓/좁)
    방향: 현재 vs 63영업일(3개월) 전 (상승/하락)
    → Q1 회복(넓+하락), Q2 성장(좁+하락), Q3 과열(좁+상승), Q4 침체(넓+상승)
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
            df = pd.DataFrame(rows, columns=['date', 'hy_spread'])
            df['date'] = pd.to_datetime(df['date'])
        else:
            # fallback: CSV 엔드포인트 (API key 없을 때)
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2&cosd={start_date}&coed={end_date}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=60) as response:
                csv_data = response.read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data), parse_dates=['observation_date'])
            df.columns = ['date', 'hy_spread']

        df = df.dropna(subset=['hy_spread'])
        df['hy_spread'] = pd.to_numeric(df['hy_spread'], errors='coerce')
        df = df.dropna().set_index('date').sort_index()

        if len(df) < 1260:  # 최소 5년치 필요
            log("HY Spread: 데이터 부족", level="WARN")
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


def get_market_risk_status():
    """시장 위험 통합 상태 (HY + VIX + Concordance)

    Returns:
        dict {hy, vix, concordance, final_action}
    """
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

    # portfolio_mode: 항상 normal (TOP 5) — 시장 경고는 AI 리스크 필터에서 별도 안내
    portfolio_mode = 'normal'

    log(f"Concordance: {concordance} (q_days={hy.get('q_days', 'N/A') if hy else 'N/A'}) → {final_action} [portfolio: {portfolio_mode}]")

    return {
        'hy': hy,
        'vix': vix,
        'concordance': concordance,
        'final_action': final_action,
        'portfolio_mode': portfolio_mode,
    }


def get_market_context(hist_all=None):
    """미국 시장 지수 컨텍스트 — hist_all에서 추출 (추가 HTTP 호출 없음)"""
    lines = []
    for symbol, name in [("^GSPC", "S&P 500"), ("^IXIC", "나스닥"), ("^DJI", "다우"), ("^RUT", "러셀2000")]:
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


def select_display_top5(results_df, status_map=None, weighted_ranks=None,
                        earnings_map=None, risk_status=None, score_100_map=None,
                        hist_all=None):
    """Signal 메시지용 종목 선정 (v70: w_gap 순위 Top3 + min_seg ≥ 0%, 최대 5종목)

    part2_rank(w_gap 기반) 상위 3종목 중 EPS 추세 건강(min_seg ≥ 0%) 종목만 진입.
    이탈선: part2_rank > 15. 최대 5슬롯.
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

    if portfolio_mode == 'stop':
        return []

    all_eligible = get_part2_candidates(results_df, top_n=None)
    if all_eligible.empty:
        return []

    # min_seg < -2% 제외 — save_part2_ranks()와 동일 기준 (순위 부여 전 필터)
    def _calc_min_seg(row):
        segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
        return min(segs) if segs else 0
    all_eligible = all_eligible[all_eligible.apply(_calc_min_seg, axis=1) >= -2].copy()

    # v58: w_gap(score_100_map) 오름차순 정렬 — save_part2_ranks()와 동일 파이프라인
    candidates = all_eligible.copy()
    if score_100_map:
        candidates['_wgap'] = candidates['ticker'].map(lambda t: score_100_map.get(t, 0))
        candidates = candidates.sort_values('_wgap', ascending=True).reset_index(drop=True)
    else:
        candidates = candidates.sort_values('adj_gap', ascending=True).reset_index(drop=True)

    # w_gap 순위 맵 (1-based)
    p2r_map = {row['ticker']: i + 1 for i, (_, row) in enumerate(candidates.iterrows())}

    top_debug = [(row['ticker'], p2r_map.get(row['ticker'], 999),
                  round(float(score_100_map.get(row['ticker'], 0)) if score_100_map else float(row.get('adj_gap', 0) or 0), 1))
                 for _, row in candidates.head(7).iterrows()]
    log(f"w_gap 순위 상위 7: {top_debug}")

    # v70: part2_rank 순서대로 5종목 채울 때까지 탐색 (Top7 상한)
    MAX_SLOTS = 5
    selected = []
    for _, row in candidates.iterrows():
        if len(selected) >= MAX_SLOTS:
            break
        t = row['ticker']
        p2r = p2r_map.get(t, 999)

        # Top3만 진입 — 슬롯 5개는 여러 날에 걸쳐 누적
        if p2r > 3:
            break

        # v71: 3일 검증(✅) 필수 — 🆕/⏳ 종목은 Signal에서 제외
        status = status_map.get(t, '🆕')
        if status != '✅':
            log(f"  ⏳ 디스플레이 제외 {t}: 검증 미완료 ({status})")
            continue

        # v58 진입 조건: min_seg ≥ 0% (소수점 1자리 반올림 기준)
        segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
        min_seg = min(segs) if segs else 0
        if round(min_seg, 1) < 0:
            log(f"  ⛔ 디스플레이 제외 {t}: min_seg={min_seg:.1f}% (< 0%)")
            continue

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
            log(f"  ⛔ 디스플레이 제외 {t}: {','.join(flags)}")
        else:
            entry = _build_portfolio_entry(row, status_map, earnings_map)
            selected.append(entry)

    # 균등비중 (v71: 역변동성→균등 전환, 28일 검증 균등 +19.7% > 역변동성 +10.9%)
    n = len(selected)
    if n > 0:
        base = 100 // n
        remainder = 100 - base * n
        for i, s in enumerate(selected):
            s['weight'] = base + (1 if i < remainder else 0)

    log(f"디스플레이 {n}종목: " + ", ".join(f"{s['ticker']}({s['weight']}%)" for s in selected))

    return selected


def select_portfolio_stocks(results_df, status_map=None, weighted_ranks=None,
                            earnings_map=None, risk_status=None, today_str=None):
    """포트폴리오 종목 선정 — v58 (미사용, 참조용)

    전략:
    - 진입: w_gap 순위 Top3, min_seg ≥ 0%, 최대 3종목
    - 이탈: part2_rank > 15 (w_gap 기준) / min_seg < -2% / -10% 손절
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

    # stop 모드: 빈 리스트 반환
    if portfolio_mode == 'stop':
        log(f"포트폴리오: portfolio_mode=stop → 추천 중단 ({final_action})")
        return [], portfolio_mode, concordance, final_action

    # Top 30 (하드 필터 적용, ✅ 필터 전) — Watchlist용 유지, 이탈은 Top 15 기준
    top30 = get_part2_candidates(results_df, top_n=30)
    if top30.empty:
        return [], portfolio_mode, concordance, final_action

    top15_tickers = set(top30.head(15)['ticker'].tolist())

    # ── 어제 보유 → Top 15 유지 시 홀드 (v58: 미사용, 참조용) ──
    prev_holdings = _get_prev_portfolio(today_str)

    # 1차 이탈: Top 15 밖
    holds_in_top20 = [t for t in prev_holdings if t in top15_tickers]
    exited_rank = [t for t in prev_holdings if t not in top15_tickers]
    if exited_rank:
        log(f"  📤 Top15 이탈: {', '.join(exited_rank)}")

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

    max_stocks = 5  # v70: 최대 5종목 (미사용 함수)
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

    for t in sorted(exited_tickers, key=lambda x: exited_tickers[x]):
        cur_rank = part2_map.get(t)  # conviction 기반 순위
        is_eligible = t in composite_map  # 하드필터 통과 여부
        ag = adj_gap_map.get(t)
        # min_seg < -2% 체크 (save_part2_ranks에서 순위 부여 전 제거된 종목)
        row_data = full_data.get(t)
        if row_data is not None:
            segs = [float(row_data.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
            if segs and min(segs) < -2:
                reason = '추세둔화'
                result.append((t, cur_rank, reason))
                continue
        if ag is not None and ag > 5.0:
            reason = '주가선반영'
        elif is_eligible:
            reason = '순위밀림'
        else:
            # 어떤 필터에 걸렸는지 특정
            reason = _identify_filter_failure(full_data.get(t), t)
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
        return 'EPS↓'

    eps_90d = row.get('eps_change_90d', 0) or 0
    if eps_90d <= 0:
        return 'EPS↓'

    price = row.get('price', 0) or 0
    if price < 10:
        return '저가'

    # MA120 우선, 없으면 MA60
    ma120 = row.get('ma120')
    ma60 = row.get('ma60')
    ma_val = (ma120 if ma120 is not None and pd.notna(ma120) else ma60) or 0
    if ma_val > 0 and price < ma_val:
        return 'MA120↓'

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
        client = genai.Client(api_key=api_key)
        grounding_tool = types.Tool(google_search=types.GoogleSearch())
    except Exception as e:
        log(f"AI: Gemini 초기화 실패: {e}", "WARN")
        return result

    def _gemini_call(prompt, temperature=0.2, label=""):
        """Gemini 호출 래퍼 — timeout 60초 + 1회 retry"""
        import time as _time
        for _attempt in range(2):
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
                if _attempt == 0:
                    log(f"AI: {label} 실패 (15초 후 재시도): {e}", "WARN")
                    _time.sleep(15)
                else:
                    raise
        return None

    def extract_text(resp):
        try:
            if resp.text:
                return resp.text
        except Exception:
            pass
        try:
            parts = resp.candidates[0].content.parts
            texts = [p.text for p in parts if hasattr(p, 'text') and p.text]
            if texts:
                return '\n'.join(texts)
        except Exception:
            pass
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
            text = extract_text(resp) if resp else None
            if text:
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

                log(f"AI: 내러티브 {len(result['narratives'])}종목")
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
    suffixes = r',?\s*(?:Inc(?:orporat(?:ed?)?)?\.?|Corp(?:orati(?:on)?)?\.?|Comp(?:any)?|Co\.?|Ltd\.?|Limi(?:ted)?|PLC|plc|Hold(?:ings?)?\.?|Group|N\.?V\.?|(?<![A-Za-z])S\.?A\.?|(?<![A-Za-z])SE|(?<![A-Za-z])AG)\s*$'
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
    """w_gap(3일 가중 conviction adj_gap) 맵 (v71). 음수=저평가, 양수=고평가.

    v71: adj_gap × (1 + rev_up30/num_analysts) conviction 배율 적용
    Returns: {ticker: float(w_gap %)}
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    dates = sorted(_get_recent_dates(cursor, 'composite_rank', today_str, 3))
    if not dates:
        conn.close()
        return {}

    gap_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d '
            'FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
            (d,)
        ).fetchall()
        gap_by_date[d] = {r[0]: _apply_conviction(r[1], r[2], r[3], r[4], r[5]) for r in rows}

    weights = [0.2, 0.3, 0.5]
    if len(dates) == 2:
        weights = [0.4, 0.6]
    elif len(dates) == 1:
        weights = [1.0]

    # 모든 종목 수집
    all_tickers = set()
    for d in dates:
        all_tickers.update(gap_by_date.get(d, {}).keys())

    result = {}
    for tk in all_tickers:
        wg = 0
        for i, d in enumerate(dates):
            wg += gap_by_date.get(d, {}).get(tk, 0) * weights[i]
        result[tk] = wg

    # z-score 기반 점수 (평균=65, 1std=15, 범위 30~100)
    # conviction w_gap의 크기 차이가 점수에 반영됨 (MU -144.9 vs SNDK -50.3 → 100 vs 77)
    import numpy as np
    eligible_tks = [r[0] for r in cursor.execute(
        'SELECT DISTINCT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
        (dates[-1],)
    ).fetchall()]
    eligible_vals = [result[tk] for tk in eligible_tks if tk in result]
    score_map = {}
    if len(eligible_vals) >= 2:
        mean_v = np.mean(eligible_vals)
        std_v = np.std(eligible_vals)
        if std_v > 0:
            for tk in eligible_tks:
                if tk in result:
                    z = (result[tk] - mean_v) / std_v  # 음수 클수록 z 음수
                    score_map[tk] = int(round(min(100, max(30, 65 + (-z) * 15))))

    conn.close()
    return result, score_map


def _get_system_performance():
    """시스템 누적 성과 계산 (DB 데이터 기반, 역변동성 가중 복리)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        all_dates = [r[0] for r in c.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        if len(all_dates) < 3:
            conn.close()
            return None

        # 전체 가격 로드
        all_prices = {}
        for d in all_dates:
            rows = c.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (d,)).fetchall()
            all_prices[d] = {r[0]: r[1] for r in rows}

        # 일별 데이터 로드
        daily_data = {}
        for d in all_dates:
            rows = c.execute('''
                SELECT ticker, price, part2_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
                FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL
            ''', (d,)).fetchall()
            daily_data[d] = {
                r[0]: {'price': r[1], 'part2_rank': r[2],
                       'nc': r[3], 'n7': r[4], 'n30': r[5], 'n60': r[6], 'n90': r[7]}
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
            """v71: adj_gap × conviction 배율 적용 후 3일 가중"""
            di = all_dates.index(date_str)
            d0 = all_dates[di]
            d1 = all_dates[di - 1] if di >= 1 else None
            d2 = all_dates[di - 2] if di >= 2 else None
            gaps = {}
            for d in [d0, d1, d2]:
                if d:
                    rows = c.execute(
                        'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d '
                        'FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (d,)
                    ).fetchall()
                    gaps[d] = {r[0]: _apply_conviction(r[1], r[2], r[3], r[4], r[5]) for r in rows}
            result = {}
            tks = set()
            for d in [d0, d1, d2]:
                if d and d in gaps:
                    tks.update(gaps[d].keys())
            wts = [0.5, 0.3, 0.2]
            for tk in tks:
                wg = gaps.get(d0, {}).get(tk, 0) * wts[0]
                if d1:
                    wg += gaps.get(d1, {}).get(tk, 0) * wts[1]
                if d2:
                    wg += gaps.get(d2, {}).get(tk, 0) * wts[2]
                result[tk] = wg
            return result

        # SPY 가격
        try:
            import yfinance as yf
            spy_df = yf.download('SPY', start=all_dates[0], end=all_dates[-1],
                                 auto_adjust=True, progress=False)
            spy_prices = {}
            for idx, row in spy_df.iterrows():
                ds = idx.strftime('%Y-%m-%d')
                spy_prices[ds] = float(row.iloc[3]) if len(row) > 3 else float(row.iloc[0])
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

            w_gap = _w_gap(date)
            ticker_ms = {}
            for tk, info in data.items():
                ticker_ms[tk] = _min_seg(info['nc'], info['n7'], info['n30'], info['n60'], info['n90'])

            eligible = [(tk, w_gap.get(tk, 0)) for tk in data if ticker_ms.get(tk, 0) >= -2]
            eligible.sort(key=lambda x: x[1])
            wgap_rank = {tk: r + 1 for r, (tk, _) in enumerate(eligible)}

            # 이탈
            for tk in list(portfolio.keys()):
                ep = portfolio[tk]['entry_price']
                cp = prices.get(tk)
                if cp is None:
                    del portfolio[tk]; continue
                rk = wgap_rank.get(tk)
                ms = ticker_ms.get(tk, 0)
                ret = (cp - ep) / ep * 100
                if (rk is None or rk > 15) or ms < -2 or ret <= -10:
                    if ret > 0:
                        wins += 1
                    else:
                        losses += 1
                    del portfolio[tk]

            # 진입
            slots = 5 - len(portfolio)
            if slots > 0:
                cands = [tk for tk, _ in eligible[:30]
                         if tk not in portfolio and wgap_rank.get(tk, 999) <= 3
                         and ticker_ms.get(tk, -999) >= 0]
                for tk in cands[:slots]:
                    cp = prices.get(tk)
                    if cp:
                        portfolio[tk] = {'entry_price': cp}

            # 균등비중 수익률 (v71: 역변동성→균등 전환)
            day_ret = 0
            if portfolio:
                w = 1.0 / len(portfolio)
                for tk in portfolio:
                    cur = prices.get(tk)
                    prev = prev_prices.get(tk)
                    if cur and prev and prev > 0:
                        day_ret += w * (cur - prev) / prev * 100

            spy_ret = 0
            sc = spy_prices.get(date)
            sp = spy_prices.get(prev_date)
            if sc and sp and sp > 0:
                spy_ret = (sc - sp) / sp * 100

            sys_nav *= (1 + day_ret / 100)
            spy_nav *= (1 + spy_ret / 100)

        conn.close()
        n_days = len(all_dates) - start_idx
        return {
            'sys_cum': (sys_nav - 1) * 100,
            'spy_cum': (spy_nav - 1) * 100,
            'alpha': (sys_nav - 1) * 100 - (spy_nav - 1) * 100,
            'n_days': n_days,
            'start_date': all_dates[start_idx],
            'end_date': all_dates[-1],
            'wins': wins,
            'losses': losses,
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
                          hist_all=None):
    """v3 Message 1: Signal — "오늘 뭘 사야 하나"

    종목당 4줄: 정체(이름·업종·가격) / 증거(EPS·매출) / 순위 / AI 내러티브
    시장 환경 없음 (AI Risk로 이동). 이탈 1줄 알림만.
    """
    import re

    if weighted_ranks is None:
        weighted_ranks = {}

    narratives = ai_content.get('narratives', {}) if ai_content else {}

    lines = []

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

    # ━━ 시스템 성과 ━━
    try:
        perf = _get_system_performance()
        if perf and perf['n_days'] >= 5:
            lines.append('')
            lines.append(f'📈 <b>시스템 누적 수익률 {perf["sys_cum"]:+.1f}% ({perf["n_days"]}거래일)</b>')
            lines.append(f'    같은 기간 S&P500은 {perf["spy_cum"]:+.1f}%')
    except Exception:
        pass

    # ━━ 섹션 1: 결론 먼저 ━━
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')
    lines.append(f'🛒 <b>EPS 모멘텀 매수 후보</b>')
    lines.append('━━━━━━━━━━━━━━━')
    for idx, s in enumerate(selected):
        name = _clean_company_name(s['name'], s['ticker'])
        w = s.get('weight', 0)
        lines.append(f'<b>{idx+1}. {name}({s["ticker"]})</b>')

    # 주가 상관관계 표시 (90일 일간수익률 기준, 0.65 이상 페어만)
    try:
        tickers_list = [s['ticker'] for s in selected]
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
    lines.append(f'→ 3일 연속 상위 유지 {len(selected)}종목 선정')

    if alpha_signals is None:
        alpha_signals = {}

    # ━━ 섹션 3: 종목별 근거 ━━
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')
    lines.append('📌 <b>종목별 근거</b>')
    lines.append('━━━━━━━━━━━━━━━')

    for i, s in enumerate(selected):
        ticker = s['ticker']
        eps_chg = s['eps_chg']
        rev = _safe_float(s.get('rev_growth'))
        earnings_tag = s.get('earnings_note', '')

        # L0: 이름·업종·점수
        display_name = _clean_company_name(s["name"], ticker)
        industry = s.get('industry', '')
        ind_str = f' · {industry}' if industry else ''
        score_str = ''
        if score_display_map and ticker in score_display_map:
            score_str = f' · {score_display_map[ticker]}점'
        lines.append(f'<b>{i+1}. {display_name}({ticker}){ind_str}{score_str}</b>{earnings_tag}')

        # L1: 증거 (EPS 전망 · 매출성장)
        growth_parts = []
        if eps_chg:
            growth_parts.append(f'EPS 전망 {int(round(eps_chg)):+d}%')
        if rev:
            growth_parts.append(f'매출성장 {int(round(rev * 100)):+d}%')
        lines.append(' · '.join(growth_parts))

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
        rank_parts = [f'순위 {rank_str}']
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
        reason_groups = defaultdict(list)
        for t, _, reason in exit_reasons:
            reason_groups[reason or '순위밀림'].append(t)
        parts = []
        for reason, tickers in reason_groups.items():
            parts.append(f'{"·".join(tickers)}({reason})')
        lines.append('')
        lines.append(f'⚠️ 이탈: {" ".join(parts)}')
        # MA120 이탈 + 어제 상위권 종목 → 반등 관심 대상
        if exited_tickers:
            for t, _, reason in exit_reasons:
                if reason == 'MA120↓':
                    prev_rank = exited_tickers.get(t)
                    if prev_rank is not None and prev_rank <= 10:
                        lines.append(f'💡 {t} — MA120 이탈이지만 어제 {prev_rank}위, 반등 시 재진입 대상')

    # ━━ 범례 + 면책 ━━
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')
    lines.append('매수: 상위 3종목, 최대 5종목 보유')
    lines.append('매도: 15위 밖 or 실적하락 or -10%')

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

    # w_gap 오름차순 정렬 (v58: 음수가 저평가 = 좋음)
    if score_100_map:
        filtered = filtered.copy()
        filtered['_adj_gap'] = filtered['ticker'].map(
            lambda t: score_100_map.get(t, 0)
        )
        filtered = filtered.sort_values('_adj_gap', ascending=True).reset_index(drop=True)
    elif weighted_ranks:
        filtered = filtered.copy()
        filtered['_weighted'] = filtered['ticker'].map(
            lambda t: weighted_ranks.get(t, {}).get('weighted', 50.0)
        )
        filtered = filtered.sort_values('_weighted').reset_index(drop=True)

    # min_seg < -2%: save_part2_ranks()에서 이미 제외됨 → 이탈은 classify_exit_reasons()에서 처리
    # -2% ≤ min_seg < 0%: 매수 불가지만 보유 추적용으로 표시 (⚠️ 마크)
    healthy_rows = []
    caution_tickers = set()  # -2% ≤ min_seg < 0% — 매수 불가, 보유 추적용
    for _, row in filtered.iterrows():
        _segs = [float(row.get(c) or 0) for c in ('seg1', 'seg2', 'seg3', 'seg4')]
        _min_seg = min(_segs) if _segs else 0
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

    lines.append('✅ 검증완료 ⏳ 관찰중 🆕 신규 ⚠️ 추세주의')
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
        rank_parts = [f'순위 {rank_str}', f'의견 ↑{rev_up}↓{rev_down}']
        # 어닝 서프/공매도는 Signal 메시지의 AI 내러티브에서 표현 (v69)
        lines.append(' · '.join(rank_parts))

        # 점선 구분선
        if rank < num_stocks:
            lines.append('- - - - -')

    # ── EPS 추세 둔화 (메인 리스트에서 제외된 종목) ──
    # ── 순위 이탈 (사유별 묶어서 표시) ──
    if exit_reasons:
        from collections import defaultdict
        reason_groups = defaultdict(list)
        for t, _, reason in exit_reasons:
            reason_groups[reason or '순위밀림'].append(t)
        parts = []
        for reason, tickers in reason_groups.items():
            parts.append(f'{"·".join(tickers)}({reason})')
        lines.append('')
        lines.append('━━━━━━━━━━━━━━━')
        lines.append(f'📉 이탈: {" ".join(parts)}')

    # ── 범례 ──
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━')
    lines.append('📌 <b>운영 규칙</b>')
    lines.append('매수: 상위 3종목, 최대 5종목 보유')
    lines.append('매도: 15위 밖 or 실적하락 or -10%')
    lines.append('⚠️: 추세 약화, 보유시 추이 확인')

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
        # 매출+품질 수집 → rev_growth composite score + 12개 재무지표 DB 저장 (v33)
        results_df, earnings_map, info_cache = fetch_revenue_growth(results_df, today_str)

        # 가중순위 기반 Top 30 선정 + DB 저장
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
        send_to_channel = is_github and channel_id and not cold_start
        if cold_start:
            log(f"Cold start — 채널 전송 비활성화 (3일 데이터 축적 전)")

        dest = '채널+개인봇' if send_to_channel else '개인봇'
        biz_day = get_last_business_day()

        # ===== v3: Signal + AI Risk + Watchlist =====

        # risk_status에서 공통 값 추출
        concordance = risk_status.get('concordance', 'both_stable') if risk_status else 'both_stable'
        final_action = risk_status.get('final_action', '') if risk_status else ''
        portfolio_mode = risk_status.get('portfolio_mode', 'normal') if risk_status else 'normal'

        # conviction w_gap 맵(순위용) + 퍼센타일 점수 맵(표시용)
        score_100_map, score_display_map = _build_score_100_map(today_str)

        # 디스플레이용 종목 선정 (v57b: adj_gap ≤ -4% + min_seg ≥ 1%, 최대 3종목)
        display_top5 = select_display_top5(
            results_df, status_map, weighted_ranks, earnings_map, risk_status,
            score_100_map=score_100_map, hist_all=hist_all
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

        # AI 2회 호출 (시장 요약 + 종목 내러티브) — ETF는 코드 기반 별도 처리
        ai_content = run_ai_analysis(config, display_top5, biz_day, risk_status,
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
