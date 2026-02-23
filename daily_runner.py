"""
EPS Momentum Daily Runner v19 - Safety & Trend Fusion

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
import warnings
warnings.filterwarnings('ignore')

# Windows에서 UTF-8 인코딩 강제 적용 (이모지 지원)
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 프로젝트 루트
PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / 'eps_momentum_data.db'
CONFIG_PATH = PROJECT_ROOT / 'config.json'

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
    "message_version": "v1",
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

    # 메시지 버전 (v1=기존 6개, v2=압축 2개)
    if os.environ.get('MESSAGE_VERSION'):
        config['message_version'] = os.environ['MESSAGE_VERSION']

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
                          ('price', 'REAL'), ('ma60', 'REAL'), ('part2_rank', 'INTEGER'),
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

    all_tickers = sorted(set(t for tlist in INDICES.values() for t in tlist))
    log(f"유니버스: {len(all_tickers)}개 종목")

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

    # Step 2: 가격 데이터 일괄 다운로드
    log("가격 데이터 일괄 다운로드 중...")
    hist_all = None
    try:
        hist_all = yf.download(all_tickers, period='6mo', threads=True, progress=False)
        log("가격 다운로드 완료")
    except Exception as e:
        log(f"일괄 다운로드 실패: {e}, 개별 다운로드로 전환", "WARN")

    # Step 3: 종목별 EPS 데이터 순차 수집
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    results = []
    turnaround = []
    no_data = []
    errors = []
    cache_updated = False

    for i, ticker in enumerate(all_tickers):
        if (i + 1) % 100 == 0:
            log(f"  수집 진행: {i+1}/{len(all_tickers)} (메인: {len(results)}, 턴어라운드: {len(turnaround)})")
            conn.commit()

        try:
            stock = yf.Ticker(ticker)

            # NTM EPS 계산
            ntm = calculate_ntm_eps(stock, today)
            if ntm is None:
                no_data.append(ticker)
                continue

            # Score 계산
            score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction = calculate_ntm_score(ntm)
            eps_change_90d = calculate_eps_change_90d(ntm)
            trend_lights, trend_desc = get_trend_lights(seg1, seg2, seg3, seg4)

            # EPS Revision & 애널리스트 수 추출 — max(0y, +1y)로 두 기간 모두 반영
            rev_up30 = 0
            rev_down30 = 0
            num_analysts = 0
            try:
                raw_trend = stock._analysis._earnings_trend
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
            except Exception:
                pass

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

            # 종목 정보 (캐시 우선, 없으면 API 호출)
            if ticker in ticker_cache:
                short_name = ticker_cache[ticker]['shortName']
                industry_kr = ticker_cache[ticker]['industry']
            else:
                info = stock.info
                short_name = info.get('shortName', ticker)
                industry_en = info.get('industry', 'N/A')
                industry_kr = INDUSTRY_MAP.get(industry_en, industry_en)
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

            try:
                if hist_all is not None:
                    hist = hist_all['Close'][ticker].dropna()
                else:
                    h = stock.history(period='6mo')
                    hist = h['Close']

                if len(hist) >= 60:
                    p_now = hist.iloc[-1]
                    current_price = float(p_now)
                    ma60_val = float(hist.rolling(window=60).mean().iloc[-1])
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

            # adj_gap: 괴리율에 방향 보정 (가속 → 저평가 강화, 감속 → 저평가 약화)
            adj_gap = None
            if fwd_pe_chg is not None and direction is not None:
                dir_factor = max(-0.3, min(0.3, direction / 30))
                adj_gap = fwd_pe_chg * (1 + dir_factor)

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
            }

            # DB에 파생 데이터 업데이트
            cursor.execute('''
                UPDATE ntm_screening
                SET adj_score=?, adj_gap=?, price=?, ma60=?,
                    rev_up30=?, rev_down30=?, num_analysts=?
                WHERE date=? AND ticker=?
            ''', (adj_score, adj_gap, current_price, ma60_val,
                  rev_up30, rev_down30, num_analysts, today_str, ticker))

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
        'universe': len(all_tickers),
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

    return results_df, turnaround_df, stats, today_str


# ============================================================
# Part 2 공통 필터 & 3일 교집합
# ============================================================

def fetch_revenue_growth(df, today_str):
    """전체 916종목 매출 성장률 + 재무 품질 수집 (v33)

    1) 전체 종목 yfinance .info → rev_growth + 12개 재무 지표 DB 저장
    2) composite score용 rev_growth를 dataframe에 매핑
    10스레드 병렬 수집으로 ~3분 → ~30초 단축.
    """
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time as _time

    def _fetch_one(ticker):
        """단일 종목 .info 수집 (스레드 워커)"""
        try:
            info = yf.Ticker(ticker).info
            return ticker, info
        except Exception:
            return ticker, None

    tickers = list(df['ticker'].unique())
    log(f"매출+품질 수집 시작: {len(tickers)}종목 (10스레드)")

    # 병렬 수집
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_one, t): t for t in tickers}
        done = 0
        for future in as_completed(futures):
            ticker, info = future.result()
            results[ticker] = info
            done += 1
            if done % 100 == 0:
                log(f"  수집 진행: {done}/{len(tickers)}")

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
                if dt_et.hour >= 16:  # 장후 발표 → 다음 거래일
                    earn_date += timedelta(days=1)
                earnings_map[t] = earn_date
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
    return df, earnings_map


def get_part2_candidates(df, top_n=None):
    """Part 2 매수 후보 필터링 (공통 함수)

    필터: adj_score > 9, fwd_pe > 0, eps > 0, price ≥ $10, price > MA60,
          rev_growth ≥ 10%, num_analysts ≥ 3, 하향 비율 ≤ 30%
    정렬: composite score (adj_gap 70% + rev_growth 30%) 또는 adj_gap
    """
    import numpy as np
    import pandas as pd

    filtered = df[
        (df['adj_score'] > 9) &
        (df['adj_gap'].notna()) &
        (df['fwd_pe'].notna()) & (df['fwd_pe'] > 0) &
        (df['eps_change_90d'] > 0) &
        (df['price'].notna()) & (df['price'] >= 10) &
        (df['ma60'].notna()) & (df['price'] > df['ma60'])
    ].copy()

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

    if has_rev:
        # z-score 정규화
        gap_mean, gap_std = filtered['adj_gap'].mean(), filtered['adj_gap'].std()
        rev_mean, rev_std = filtered['rev_growth'].mean(), filtered['rev_growth'].std()

        if gap_std > 0 and rev_std > 0:
            z_gap = (filtered['adj_gap'] - gap_mean) / gap_std
            z_rev = (filtered['rev_growth'] - rev_mean) / rev_std
            # adj_gap은 음수가 좋으므로 부호 반전, rev_growth는 양수가 좋음
            filtered['composite'] = (-z_gap) * 0.7 + z_rev * 0.3
            filtered = filtered.sort_values('composite', ascending=False)
        else:
            filtered = filtered.sort_values('adj_gap', ascending=True)
    else:
        filtered = filtered.sort_values('adj_gap', ascending=True)

    if top_n:
        filtered = filtered.head(top_n)
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


def save_part2_ranks(results_df, today_str):
    """Part 2 eligible 종목 저장 — composite_rank + 가중순위 Top 30

    1. 전체 eligible의 composite 순위 → composite_rank 컬럼에 저장
    2. T-1/T-2의 composite_rank로 가중순위 계산 (누적 방지)
    3. 가중순위 상위 30개 → part2_rank 저장
    Returns: Top 30 티커 리스트 (가중순위 순)
    """
    all_candidates = get_part2_candidates(results_df, top_n=None)
    if all_candidates.empty:
        log("Part 2 후보 0개 — part2_rank 저장 스킵")
        return []

    # 1. 오늘의 composite 순위 (1~N)
    all_candidates = all_candidates.reset_index(drop=True)
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

    # 2. 이전 날짜의 composite_rank 조회
    cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL AND date < ? ORDER BY date DESC LIMIT 2',
        (today_str,)
    )
    prev_dates = sorted([r[0] for r in cursor.fetchall()])

    PENALTY = 50
    rank_by_date = {}
    for d in prev_dates:
        cursor.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (d,)
        )
        rank_by_date[d] = {r[0]: r[1] for r in cursor.fetchall()}

    t1 = prev_dates[-1] if len(prev_dates) >= 1 else None
    t2 = prev_dates[-2] if len(prev_dates) >= 2 else None

    # 3. 가중순위 = composite_T0 × 0.5 + composite_T1 × 0.3 + composite_T2 × 0.2
    weighted = {}
    for ticker, r0 in composite_ranks.items():
        r1 = rank_by_date.get(t1, {}).get(ticker, PENALTY) if t1 else PENALTY
        r2 = rank_by_date.get(t2, {}).get(ticker, PENALTY) if t2 else PENALTY
        weighted[ticker] = r0 * 0.5 + r1 * 0.3 + r2 * 0.2

    # 4. 가중순위로 정렬 → Top 30
    sorted_tickers = sorted(weighted.items(), key=lambda x: x[1])
    top30 = sorted_tickers[:30]

    # 5. part2_rank 저장 (Top 30만)
    cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today_str,))
    top30_tickers = []
    for rank, (ticker, w) in enumerate(top30, 1):
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
            (rank, today_str, ticker)
        )
        top30_tickers.append(ticker)

    conn.commit()
    conn.close()
    log(f"Part 2 rank 저장: {len(top30_tickers)}개 종목 (가중순위 Top 30, eligible {len(composite_ranks)}개)")
    return top30_tickers


def is_cold_start():
    """DB에 part2_rank 데이터가 3일 미만이면 True (채널 전송 제어용)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT date) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    count = cursor.fetchone()[0]
    conn.close()
    return count < 3


def get_3day_status(today_tickers):
    """3일 연속 Part 2 진입 여부 판별 → {ticker: '✅' or '⏳' or '🆕'}
    ✅ = 3일 연속 (포트폴리오 포함)
    ⏳ = 2일 연속 (표시만, 포트폴리오 제외)
    🆕 = 오늘만 (표시만, 포트폴리오 제외)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 최근 3개 distinct date (part2_rank 있는 날짜만)
    cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 3'
    )
    dates = [r[0] for r in cursor.fetchall()]

    if len(dates) < 2:
        conn.close()
        log(f"3일 교집합: DB {len(dates)}일뿐 — 전부 🆕 처리 (cold start)")
        return {t: '🆕' for t in today_tickers}

    placeholders = ','.join('?' * len(dates))

    # 3일 모두 리스트에 있는 종목
    verified_3d = set()
    if len(dates) >= 3:
        cursor.execute(f'''
            SELECT ticker FROM ntm_screening
            WHERE date IN ({placeholders}) AND part2_rank IS NOT NULL AND part2_rank <= 30
            GROUP BY ticker HAVING COUNT(DISTINCT date) = 3
        ''', dates)
        verified_3d = {r[0] for r in cursor.fetchall()}

    # 최근 2일 모두 리스트에 있는 종목
    dates_2d = dates[:2]
    ph2 = ','.join('?' * len(dates_2d))
    cursor.execute(f'''
        SELECT ticker FROM ntm_screening
        WHERE date IN ({ph2}) AND part2_rank IS NOT NULL AND part2_rank <= 30
        Group BY ticker HAVING COUNT(DISTINCT date) = 2
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


def get_rank_history(today_tickers):
    """최근 3일간 part2_rank 이력 → {ticker: '3→4→1'} 형태"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 3'
    )
    dates = sorted([r[0] for r in cursor.fetchall()])

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


def compute_weighted_ranks(today_tickers):
    """3일 가중 순위 계산 — composite_rank 기반
    T0_composite × 0.5 + T1_composite × 0.3 + T2_composite × 0.2
    Returns: {ticker: {'weighted': float, 'r0': int, 'r1': int, 'r2': int}}
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date DESC LIMIT 3'
    )
    dates = sorted([r[0] for r in cursor.fetchall()])

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


def get_daily_changes(today_tickers):
    """어제 대비 리스트 변동 — 신규 진입 / 이탈 종목 (단순 set 비교)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 어제 날짜 (part2_rank 있는 가장 최근)
    cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 2'
    )
    dates = [r[0] for r in cursor.fetchall()]

    if len(dates) < 2:
        conn.close()
        return [], []

    yesterday = dates[1]

    cursor.execute(
        'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL AND part2_rank <= 30',
        (yesterday,)
    )
    yesterday_ranks = {r[0]: r[1] for r in cursor.fetchall()}
    conn.close()

    yesterday_top30 = set(yesterday_ranks.keys())
    today_set = set(today_tickers)
    entered = today_set - yesterday_top30
    exited = yesterday_top30 - today_set
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
    import io
    import pandas as pd
    import numpy as np
    import time

    for attempt in range(3):
      try:
        # FRED에서 10년치 HY spread CSV 다운로드
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365 * 11)).strftime('%Y-%m-%d')
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2&cosd={start_date}&coed={end_date}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as response:
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
            signals.append(f'💎 HY {hy_spread:.2f}%, 전일 대비 {daily_change_bp:+.0f}bp 급락 — 반등 매수 기회에요!')

        # 2) 5% 하향 돌파
        if hy_prev >= 5 and hy_spread < 5:
            signals.append(f'💎 HY {hy_spread:.2f}%로 5% 밑으로 내려왔어요 — 적극 매수 구간이에요!')

        # 3) 60일 고점 대비 -300bp 이상 하락
        peak_60d = df['hy_spread'].rolling(60).max().iloc[-1]
        from_peak_bp = (hy_spread - peak_60d) * 100
        if from_peak_bp <= -300:
            signals.append(f'💎 60일 고점 대비 {from_peak_bp:.0f}bp 하락 — 바닥 신호, 적극 매수하세요!')

        # 4) Q4→Q1 전환 (전일 분면 계산)
        prev_wide = hy_prev >= median_10y
        hy_3m_ago_prev = df['hy_spread'].iloc[-64] if len(df) >= 64 else df['hy_spread'].iloc[0]
        prev_rising = hy_prev >= hy_3m_ago_prev
        prev_was_q4 = prev_wide and prev_rising
        now_is_q1 = is_wide and not is_rising
        if prev_was_q4 and now_is_q1:
            signals.append('💎 겨울→봄 전환 — 가장 좋은 매수 타이밍이에요!')

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

        # HY 단독 행동 권장 (fallback용, 최종은 get_market_risk_status에서 결정)
        if quadrant == 'Q1':
            action = '적극 매수하세요.'
        elif quadrant == 'Q2':
            action = '평소대로 투자하세요.'
        elif quadrant == 'Q3':
            action = '신규 매수 시 신중하세요.'
        else:  # Q4
            action = '신규 매수를 멈추고 관망하세요.'

        return {
            'hy_spread': hy_spread,
            'median_10y': median_10y,
            'hy_3m_ago': hy_3m_ago,
            'hy_prev': hy_prev,
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
            time.sleep(5)
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
    import io
    import pandas as pd
    import time

    for attempt in range(3):
      try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
        url = (
            f"https://fred.stlouisfed.org/graph/fredgraph.csv"
            f"?id=VIXCLS&cosd={start_date}&coed={end_date}"
        )
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as response:
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
            time.sleep(5)
        else:
            log(f"VIX 수집 실패: {e}", level="WARN")
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
            # 봄(회복기) — 연율+14.3%, 양수확률86%, 역사적 최고 수익
            if vix_ok:
                final_action = '모든 지표가 매수를 가리켜요. 적극 투자하세요!'
            else:
                final_action = '회복 구간이에요. VIX가 높지만 오히려 반등 기회일 수 있어요. 적극 투자하세요!'
        elif q == 'Q2':
            # 여름(성장기) — 연율+9.4%, 양수확률84%
            if vix_ok:
                final_action = '모든 지표가 안정적이에요. 평소대로 투자하세요.'
            else:
                final_action = '신용시장은 안정적이지만 VIX가 높아요. 신규 매수 시 신중하세요.'
        elif q == 'Q3':
            # 가을(과열기) — 60일 기준 2단계 (EDA: <60d +1.84%, ≥60d +0.39%)
            if q_days < 60:
                if vix_ok:
                    final_action = '과열 초기 신호에요. 신규 매수 시 신중하세요.'
                else:
                    final_action = '과열 초기 + 변동성 확대에요. 신규 매수를 멈추세요.'
            else:
                if vix_ok:
                    final_action = '과열이 지속되고 있어요. 신규 매수를 줄여가세요.'
                else:
                    final_action = '과열 장기화 + 변동성 확대에요. 보유 종목을 점검하고 신규 매수를 멈추세요.'
        else:
            # 겨울(Q4) — 20일/60일 기준 3단계 (EDA: ≤20d 약세, 21~60d 턴어라운드, >60d 바닥접근=Q1수준)
            if q_days <= 20:
                # 초기: 반등 가능성 높음, 급매도 금지
                if vix_ok:
                    final_action = '신용시장이 악화되기 시작했어요. 급매도는 금물, 관망하세요.'
                else:
                    final_action = '시장이 흔들리고 있지만 초기 반등 가능성이 있어요. 급매도는 금물, 지켜보세요.'
            elif q_days <= 60:
                # 중기: 턴어라운드 시작 가능 (EDA: 60일 +0.5~1.5%)
                if vix_ok:
                    final_action = '침체가 지속 중이지만 변동성은 안정적이에요. 신규 매수를 멈추고 관망하세요.'
                else:
                    final_action = '침체 + 변동성 확대에요. 보유 종목을 줄여가세요.'
            else:
                # 후기(>60d): 바닥권 접근, 사전 포석 (EDA: 60일 +1.5~3.5%, Q1 수준)
                if vix_ok:
                    final_action = '바닥권에 접근하고 있어요. 분할 매수를 고려하세요.'
                else:
                    final_action = '장기 침체이지만 바닥 가능성이 있어요. 관망하며 회복 신호를 기다리세요.'
    else:
        # HY 데이터 없음 — VIX만으로 판단
        if vix and vix_dir == 'warn':
            final_action = '변동성이 높아요. 신규 매수에 신중하세요.'
        else:
            final_action = '평소대로 투자하세요.'

    # portfolio_mode: 시장 상황에 따른 [4/4] 포트폴리오 표시 방식
    # normal: Top 5 정상 | caution: Top 5 + 경고 | reduced: Top 3 | stop: 추천 안 함
    if hy and q:
        if q == 'Q1':
            portfolio_mode = 'normal'
        elif q == 'Q2':
            portfolio_mode = 'normal' if vix_ok else 'caution'
        elif q == 'Q3':
            portfolio_mode = 'stop' if not vix_ok else 'caution'
        else:  # Q4
            if q_days <= 60:
                portfolio_mode = 'stop'
            else:
                portfolio_mode = 'reduced' if vix_ok else 'stop'
    else:
        portfolio_mode = 'caution' if vix and vix_dir == 'warn' else 'normal'

    log(f"Concordance: {concordance} (q_days={hy.get('q_days', 'N/A') if hy else 'N/A'}) → {final_action} [portfolio: {portfolio_mode}]")

    return {
        'hy': hy,
        'vix': vix,
        'concordance': concordance,
        'final_action': final_action,
        'portfolio_mode': portfolio_mode,
    }


def get_market_context():
    """미국 시장 지수 컨텍스트"""
    try:
        import yfinance as yf
        lines = []
        for symbol, name in [("^GSPC", "S&P 500"), ("^IXIC", "나스닥"), ("^DJI", "다우")]:
            try:
                hist = yf.Ticker(symbol).history(period='5d')
                if len(hist) >= 2:
                    close = hist['Close'].iloc[-1]
                    prev = hist['Close'].iloc[-2]
                    chg = (close / prev - 1) * 100
                    icon = "🟢" if chg > 0.5 else ("🔴" if chg < -0.5 else "🟡")
                    lines.append(f"{icon} {name}  {close:,.0f} ({chg:+.2f}%)")
                else:
                    log(f"시장 지수 {symbol}: 데이터 부족 ({len(hist)}행)", "WARN")
            except Exception as e:
                log(f"시장 지수 {symbol} 수집 실패: {e}", "WARN")
                continue
        if not lines:
            log("시장 지수: 전부 수집 실패", "WARN")
        return lines
    except Exception as e:
        log(f"시장 지수 모듈 오류: {e}", "WARN")
        return []


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


def create_part1_message(df, top_n=30):
    """Part 1: 이익 모멘텀 랭킹 메시지 생성 (EPS 점수 순)"""
    biz_day = get_last_business_day()
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f' [1/4] 📈 EPS 모멘텀 Top {top_n}')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('')
    lines.append('미국 916종목 중 애널리스트 EPS 전망치를')
    lines.append('가장 많이 올린 기업 순위예요.')
    lines.append('')

    for _, row in df.head(top_n).iterrows():
        rank = int(row['rank'])
        ticker = row['ticker']
        name = row.get('short_name', ticker)
        industry = row.get('industry', '')
        adj_score = row.get('adj_score', row.get('score', 0))
        lights = row.get('trend_lights', '')
        desc = row.get('trend_desc', '')

        lines.append(f'<b>{rank}위</b> {name} ({ticker})')
        lines.append(f'<i>{industry}</i> · {lights} {desc} · <b>{adj_score:.1f}</b>점')
        lines.append('──────────────────')

    lines.append('')
    lines.append('👉 다음: 매수 후보 선정 [2/4]')

    return '\n'.join(lines)


def create_guide_message():
    """📖 투자 가이드 — 시스템 개요, 선정 과정, 보유/매도 기준"""
    lines = [
        '━━━━━━━━━━━━━━━━━━━',
        '      📖 투자 가이드',
        '━━━━━━━━━━━━━━━━━━━',
        '🔎 <b>어떤 종목을 찾나요?</b>',
        '월가 애널리스트들이 "이익이 늘어날 거야"라고',
        '전망치를 올리는 종목을 찾아요.',
        '여러 전문가가 동시에 올리면 더 강한 신호예요.',
        '',
        '📊 <b>어떻게 골라요?</b>',
        '미국 916종목을 매일 5단계로 걸러요.',
        '',
        '① 이익 전망이 오르는 종목을 찾고',
        '② 주가 흐름이 건강한 종목만 남기고',
        '③ 복합 순위(괴리 70%+매출 30%) 상위 종목 선별',
        '④ 3일 연속 상위권 유지 종목만 매수 후보로',
        '⑤ AI 위험 점검 후 시장 상황에 맞게 최종 추천',
        '',
        '🛒 <b>어떻게 사나요?</b>',
        '[4/4] 최종 추천 Top 5를 동일 비중(20%씩) 매수.',
        '',
        '⏱️ <b>얼마나 보유하나요?</b>',
        'Top 5가 매일 바뀌어도 따라가지 마세요.',
        '매수 후보 30개 목록 안에 있으면 계속 보유.',
        '순위가 1위→15위로 밀려도 목록 안이면 보유예요.',
        '',
        '📉 <b>언제 파나요?</b>',
        '매수 후보 30개 목록에서 완전히 빠지면 매도.',
        '빈 자리는 현재 Top 5 중 미보유 종목으로 교체.',
        '',
        '💰 <b>얼마를 투자하나요?</b>',
        '전체 투자 자산의 20~30%만 이 전략에 적용하세요.',
        '나머지 70~80%는 VTI 같은 지수 ETF에 분산하면',
        '안정적인 포트폴리오가 됩니다.',
        '',
        '━━━━━━━━━━━━━━━━━━━',
        '       💡 읽는 법',
        '━━━━━━━━━━━━━━━━━━━',
        '🚨 <b>시장 현황 [1/4]</b>',
        '계절 = 신용시장 기반 시장 국면',
        '🌸봄(회복) · ☀️여름(성장)',
        '🍂가을(과열) · ❄️겨울(침체)',
        '🟢안정 🔴위험 — 🏦신용 · ⚡변동성',
        '🟢 많으면 적극, 🔴 많으면 매수 중단',
        '',
        '📋 <b>매수 후보 [2/4]</b>',
        '✅ 3일 연속 Top 30 → 매수 후보',
        '⏳ 2일 연속 → 내일 검증 완료',
        '🆕 오늘 첫 진입 → 관찰',
        '',
        '추세 아이콘 (90→60→30→7→오늘):',
        '🔥폭등 ☀️강세 🌤️상승 ☁️보합 🌧️하락',
        '예) ☁️🌤️☀️🔥 = 가속 · 🔥☀️🌤️☁️ = 둔화',
    ]
    return '\n'.join(lines)


def create_market_message(df, market_lines=None, risk_status=None, top_n=30):
    """[1/4] 시장 현황 — 지수, 시장 위험 지표"""
    biz_day = get_last_business_day()
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    hy_data = risk_status['hy'] if risk_status else None
    vix_data = risk_status.get('vix') if risk_status else None

    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(' [1/4] 📊 시장 현황')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    if market_lines:
        lines.extend(market_lines)

    # 시장 위험 — HY + VIX + 신호등 + 액션을 하나의 블록으로
    if hy_data or vix_data:
        lines.append('─────────────────')
        if hy_data:
            q_days = hy_data.get('q_days', 0)
            lines.append(f"🚨 <b>시장 위험</b> — {hy_data['quadrant_icon']} {hy_data['quadrant_label']} {q_days}일째")
        else:
            lines.append('🚨 <b>시장 위험</b>')
        lines.append('')

        # HY 1줄 요약
        if hy_data:
            hy_val = hy_data['hy_spread']
            med_val = hy_data['median_10y']
            q = hy_data['quadrant']
            if q == 'Q1':
                hy_desc = '평균 이상이지만 하락 중'
            elif q == 'Q2':
                hy_desc = '평균 이하, 안정'
            elif q == 'Q3':
                hy_desc = '평균 이하지만 상승 중'
            else:
                hy_desc = '평균 이상, 계속 상승'
            lines.append(f"🏦 <b>HY Spread</b>: {hy_val:.2f}% · {hy_desc}")

        # VIX 1줄 요약
        if vix_data:
            v = vix_data['vix_current']
            vix_pct = vix_data.get('vix_percentile', 0)
            slope_arrow = '↑' if vix_data['vix_slope_dir'] == 'rising' else ('↓' if vix_data['vix_slope_dir'] == 'falling' else '')
            regime_label = vix_data['regime_label']
            if vix_data['regime'] == 'normal':
                lines.append(f"⚡ <b>VIX</b>: {v:.1f} ({vix_pct:.0f}th) · 안정")
            else:
                lines.append(f"⚡ <b>VIX</b>: {v:.1f} ({vix_pct:.0f}th) {slope_arrow} · {regime_label}")
        lines.append('')

        # 신호등 + 액션 (결론)
        signals = []
        if hy_data:
            hy_ok = hy_data['quadrant'] in ('Q1', 'Q2')
            signals.append(('HY', hy_ok))
        if vix_data:
            vix_ok = vix_data['direction'] == 'stable'
            signals.append(('VIX', vix_ok))

        if signals:
            n_ok = sum(1 for _, ok in signals if ok)
            n_total = len(signals)
            dots = ''.join('🟢' if ok else '🔴' for _, ok in signals)
            if n_ok == n_total:
                conf = '확실한 신호'
            elif n_ok == 0:
                conf = '위험 신호'
            else:
                conf = '엇갈린 신호'
            lines.append(f"{dots} {n_ok}/{n_total} 안정 — {conf}")

        action = risk_status.get('final_action', '') if risk_status else ''
        if not action and hy_data:
            action = hy_data['action']
        if action:
            lines.append(f"→ {action}")
        if hy_data:
            for sig in hy_data.get('signals', []):
                lines.append(sig)

        # Q1 봄 + 전지표 안정 → 💎 매수 기회 강조
        concordance = risk_status.get('concordance', '') if risk_status else ''
        if hy_data and hy_data['quadrant'] == 'Q1' and concordance == 'both_stable':
            lines.append('')
            lines.append('💎 <b>역사적 매수 기회</b>')
            lines.append('회복기는 역사적으로 수익률이 가장 높은 구간이에요.')

    lines.append('')
    lines.append('👉 다음: 매수 후보 [2/4]')

    return '\n'.join(lines)


def create_candidates_message(df, status_map=None, exited_tickers=None, rank_history=None, top_n=30, risk_status=None, weighted_ranks=None, rank_change_tags=None, today_tickers=None):
    """[2/4] 매수 후보 — 가중 순위(T0×0.5+T1×0.3+T2×0.2) 정렬, ✅/⏳/🆕 표시, 이탈 사유"""
    import pandas as pd
    from collections import Counter

    # DB의 가중순위 Top 30과 동일한 목록 사용 (표시≠DB 불일치 방지)
    if today_tickers:
        filtered = df[df['ticker'].isin(today_tickers)].copy()
    else:
        filtered = get_part2_candidates(df, top_n=top_n)
    count = len(filtered)

    if status_map is None:
        status_map = {}
    if exited_tickers is None:
        exited_tickers = {}
    if rank_history is None:
        rank_history = {}
    if weighted_ranks is None:
        weighted_ranks = {}
    if rank_change_tags is None:
        rank_change_tags = {}

    # 가중 순위로 정렬 (없으면 composite 순 유지)
    if weighted_ranks:
        filtered = filtered.copy()
        filtered['_weighted'] = filtered['ticker'].map(
            lambda t: weighted_ranks.get(t, {}).get('weighted', 50.0)
        )
        filtered = filtered.sort_values('_weighted').reset_index(drop=True)

    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f' [2/4] 📋 매수 후보 {count}개')
    lines.append('━━━━━━━━━━━━━━━━━━━')

    # 주도 업종 (어떤 업종이 많이 올라오는지)
    sector_counts = Counter(row.get('industry', '기타') for _, row in filtered.iterrows())
    top_sectors = sector_counts.most_common()
    sector_parts = [f'{name} {cnt}' for name, cnt in top_sectors if cnt >= 2]

    if sector_parts:
        lines.append(f'📊 주도 업종: {" · ".join(sector_parts)}')

    lines.append('─────────────────')

    for idx, (_, row) in enumerate(filtered.iterrows()):
        rank = idx + 1
        ticker = row['ticker']
        industry = row.get('industry', '')
        lights = row.get('trend_lights', '')
        desc = row.get('trend_desc', '')
        eps_90d = row.get('eps_change_90d')

        marker = status_map.get(ticker, '🆕')
        rev_g = row.get('rev_growth')
        rev_up = int(row.get('rev_up30', 0) or 0)
        rev_down = int(row.get('rev_down30', 0) or 0)

        name = row.get('short_name', ticker)
        tag = rank_change_tags.get(ticker, '') if marker != '🆕' else ''
        lines.append(f'{marker} <b>{rank}.</b> {name}({ticker})')
        lines.append(f'{industry} · {lights} {desc}')
        parts = []
        if pd.notna(eps_90d):
            parts.append(f'EPS {eps_90d:+.0f}%')
        if pd.notna(rev_g):
            parts.append(f'매출 {rev_g*100:+.0f}%')
        if parts:
            lines.append(' · '.join(parts))

        # 순위 이력: 상태 마커와 일치 (🆕: 오늘만, ⏳: 2일, ✅: 3일)
        w_info = weighted_ranks.get(ticker)
        if w_info:
            r0, r1, r2 = w_info['r0'], w_info['r1'], w_info['r2']
            if marker == '🆕':
                rank_str = f'-→-→{r0}'
            elif marker == '⏳':
                r1_str = str(r1) if r1 < 50 else '-'
                rank_str = f'-→{r1_str}→{r0}'
            else:
                r2_str = str(r2) if r2 < 50 else '-'
                r1_str = str(r1) if r1 < 50 else '-'
                rank_str = f'{r2_str}→{r1_str}→{r0}'
        else:
            rank_str = f'-→-→{rank}'
        tag_suffix = f' ({tag})' if tag else ''
        lines.append(f'의견 ↑{rev_up}↓{rev_down} · 순위 {rank_str}{tag_suffix}')
        lines.append('──────────────────')

    # 이탈 종목: Top 30과 동일 포맷으로 표시
    if exited_tickers:
        all_eligible = get_part2_candidates(df)
        current_rank_map = {row['ticker']: i + 1 for i, (_, row) in enumerate(all_eligible.iterrows())}
        sorted_exits = sorted(exited_tickers.items(), key=lambda x: x[1])
        full_data = {row['ticker']: row for _, row in df.iterrows()}

        # 이탈 분류: 목표달성(괴리+만) vs 펀더멘탈 악화
        achieved = []  # (ticker, prev_rank, reasons)
        degraded = []
        for t, prev_rank in sorted_exits:
            cur_rank = current_rank_map.get(t)
            reasons = []
            if t in full_data:
                r = full_data[t]
                if (r.get('price', 0) or 0) < (r.get('ma60', 0) or 0) and (r.get('ma60', 0) or 0) > 0:
                    reasons.append('MA60↓')
                if (r.get('adj_gap', 0) or 0) > 0:
                    reasons.append('괴리+')
                if (r.get('adj_score', 0) or 0) <= 9:
                    reasons.append('점수↓')
                if (r.get('eps_change_90d', 0) or 0) <= 0:
                    reasons.append('EPS↓')
            if not reasons and cur_rank and cur_rank > top_n:
                reasons.append('순위↓')
            if not reasons:
                reasons.append('순위↓')

            if reasons == ['괴리+']:
                achieved.append((t, prev_rank, cur_rank, reasons))
            else:
                degraded.append((t, prev_rank, cur_rank, reasons))

        def _render_exit_block(exit_list):
            """이탈 종목을 Top 30과 동일 포맷으로 렌더링"""
            for t, prev_rank, cur_rank, reasons in exit_list:
                row = full_data.get(t, {})
                name = row.get('short_name', t) if isinstance(row, dict) else (row.get('short_name', t) if hasattr(row, 'get') else t)
                industry = row.get('industry', '') if hasattr(row, 'get') else ''
                lights = row.get('trend_lights', '') if hasattr(row, 'get') else ''
                desc = row.get('trend_desc', '') if hasattr(row, 'get') else ''
                eps_90d = row.get('eps_change_90d') if hasattr(row, 'get') else None
                rev_g = row.get('rev_growth') if hasattr(row, 'get') else None
                rev_up = int(row.get('rev_up30', 0) or 0) if hasattr(row, 'get') else 0
                rev_down = int(row.get('rev_down30', 0) or 0) if hasattr(row, 'get') else 0
                tag = rank_change_tags.get(t, '')

                lines.append(f'{name}({t})')
                lines.append(f'{industry} · {lights} {desc}')
                parts = []
                if eps_90d is not None and pd.notna(eps_90d):
                    parts.append(f'EPS {eps_90d:+.0f}%')
                if rev_g is not None and pd.notna(rev_g):
                    parts.append(f'매출 {rev_g*100:+.0f}%')
                if parts:
                    lines.append(' · '.join(parts))

                rank_info = f'{prev_rank}→{cur_rank}' if cur_rank else f'{prev_rank}→탈락'
                reason_tag = ' '.join(f'[{r}]' for r in reasons)
                tag_suffix = f' ({tag})' if tag else ''
                lines.append(f'의견 ↑{rev_up}↓{rev_down} · 순위 {rank_info} {reason_tag}{tag_suffix}')
                lines.append('──────────────────')

        lines.append('')
        lines.append('📉 <b>이탈 종목</b>')
        lines.append('─────────────────')
        if achieved:
            lines.append(f'✅ <b>목표 달성</b> ({len(achieved)}개) — 수익 실현 검토')
            _render_exit_block(achieved)
        if degraded:
            if achieved:
                lines.append('')
            lines.append(f'⚠️ <b>펀더멘탈 악화</b> ({len(degraded)}개) — 매도 검토')
            _render_exit_block(degraded)

    lines.append('─────────────────')
    lines.append('👉 다음: AI 리스크 필터 [3/4]')

    return '\n'.join(lines)


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

    return '\n'.join(lines)


# ============================================================
# AI 리스크 체크 (Gemini 2.5 Flash + Google Search)
# ============================================================

def run_ai_analysis(config, results_df=None, status_map=None, biz_day=None, risk_status=None, earnings_map=None, rank_change_tags=None, weighted_ranks=None):
    """[3/4] AI 브리핑 — 정량 위험 신호 + 시장 환경 기반 리스크 해석"""
    api_key = config.get('gemini_api_key', '')
    if not api_key:
        log("GEMINI_API_KEY 미설정 — AI 분석 스킵", "WARN")
        return None

    try:
        from google import genai
        from google.genai import types
    except ImportError:
        log("google-genai 패키지 미설치 — AI 분석 스킵", "WARN")
        return None

    try:
        client = genai.Client(api_key=api_key)

        import re
        import yfinance as yf

        if earnings_map is None:
            earnings_map = {}
        if rank_change_tags is None:
            rank_change_tags = {}
        if weighted_ranks is None:
            weighted_ranks = {}

        # Part 2 종목 추출 + 위험 신호 수집
        if results_df is None or results_df.empty:
            log("results_df 없음 — AI 분석 스킵", "WARN")
            return None

        filtered = get_part2_candidates(results_df, top_n=30)

        if filtered.empty:
            log("Part 2 종목 없음 — AI 분석 스킵", "WARN")
            return None

        stock_count = len(filtered)
        if biz_day is None:
            biz_day = get_last_business_day()
        biz_str = biz_day.strftime('%Y-%m-%d')
        today_date = datetime.now().date()
        two_weeks_date = (datetime.now() + timedelta(days=14)).date()

        # 종목별 위험 신호 구성
        log("위험 신호 & 어닝 일정 수집 중...")
        signal_lines = []
        earnings_tickers = []

        for _, row in filtered.iterrows():
            ticker = row['ticker']
            name = row.get('short_name', ticker)
            industry = row.get('industry', '')
            adj_score = row.get('adj_score', 0)
            eps_chg = row.get('eps_change_90d', 0) or 0
            price_chg = row.get('price_chg', 0) or 0
            fwd_pe = row.get('fwd_pe', 0) or 0
            rev_up = int(row.get('rev_up30', 0) or 0)
            rev_down = int(row.get('rev_down30', 0) or 0)
            lights = row.get('trend_lights', '')
            desc = row.get('trend_desc', '')

            # 위험 신호 플래그 (포트폴리오 필터와 동일 기준)
            num_analysts = int(row.get('num_analysts', 0) or 0)
            flags = []

            # 1. 애널리스트 하향 경고: 절대 30% 초과 OR 하향≥상향(2건 이상)
            total_rev = rev_up + rev_down
            if total_rev > 0 and rev_down / total_rev > 0.3:
                flags.append(f"🔻 의견 하향 ↓{rev_down}/↑{rev_up}")
            elif rev_down >= rev_up and rev_down >= 2:
                flags.append(f"🔻 하향 우세 ↓{rev_down}/↑{rev_up}")

            # 2. 저커버리지 (애널리스트 3명 미만)
            if num_analysts < 3:
                flags.append(f"📉 애널리스트 {num_analysts}명 (저커버리지)")

            # 3. 어닝 임박 (earnings_map에서 조회 — .calendar 별도 호출 불필요)
            ed = earnings_map.get(ticker)
            if ed and today_date <= ed <= two_weeks_date:
                flags.append(f"📅 어닝 {ed.month}/{ed.day}")
                earnings_tickers.append(f"{name} ({ticker}) {ed.month}/{ed.day}")

            # 순위 이력 + 태그
            w_info = weighted_ranks.get(ticker)
            if w_info:
                r0, r1, r2 = w_info['r0'], w_info['r1'], w_info['r2']
                r2_s = str(r2) if r2 < 50 else '-'
                r1_s = str(r1) if r1 < 50 else '-'
                rank_str = f'{r2_s}→{r1_s}→{r0}'
            else:
                rank_str = ''
            tag = rank_change_tags.get(ticker, '')
            rank_tag_info = ''
            if rank_str:
                rank_tag_info = f" · 순위 {rank_str}"
                if tag:
                    rank_tag_info += f" ({tag})"

            # 종목 라인 구성
            header = f"{name} ({ticker}) · {industry} · {lights} {desc} · 점수 {adj_score:.1f}"
            header += f"\n  EPS {eps_chg:+.1f}% / 주가 {price_chg:+.1f}% · 애널리스트 의견 ↑{rev_up} ↓{rev_down} · Fwd PE {fwd_pe:.1f}{rank_tag_info}"

            if flags:
                header += "\n  " + " | ".join(flags)

            signal_lines.append(header)

        signals_data = '\n\n'.join(signal_lines)
        earnings_info = ' · '.join(earnings_tickers) if earnings_tickers else '해당 없음'

        log(f"위험 신호 수집 완료: {stock_count}종목, 어닝 {len(earnings_tickers)}종목")

        # #3: 시장 환경 컨텍스트 구성
        market_env = ""
        if risk_status:
            hy = risk_status.get('hy')
            vix = risk_status.get('vix')
            conc = risk_status.get('concordance', '')
            f_action = risk_status.get('final_action', '')
            if hy:
                market_env += f"신용시장: HY Spread {hy['hy_spread']:.2f}% · {hy['quadrant_label']} ({hy.get('q_days', 0)}일째)\n"
            if vix:
                market_env += f"변동성: VIX {vix['vix_current']:.1f} (1년 중 {vix.get('vix_percentile', 0):.0f}th) · {vix['regime_label']}\n"
            market_env += f"종합 판단: {conc}\n"
            if f_action:
                market_env += f"행동 권장: {f_action}\n"

        prompt = f"""분석 기준일: {biz_str} (미국 영업일)

[현재 시장 환경]
{market_env if market_env else '데이터 없음'}

아래는 EPS 모멘텀 시스템의 매수 후보 {stock_count}종목과 각 종목의 정량적 위험 신호야.
이 종목들은 EPS 전망치가 상향 중이라 선정된 거야.
네 역할: 아래 섹션을 순서대로 반드시 모두 출력하는 거야. 인사말이나 서두 없이 바로 시작해.

[종목별 데이터 & 위험 신호 — 시스템이 계산한 팩트]
{signals_data}

[위험 신호 설명]
🔻 의견 하향 = 30일간 EPS 전망 하향 비율 > 30% 또는 하향 건수 ≥ 상향 건수 (의미 있는 반대 의견)
📉 저커버리지 = 커버리지 애널리스트 3명 미만 (추정치 신뢰도 낮음)
📅 어닝 = 2주 내 실적 발표 예정 (발표 전후 변동성 주의)

[순위 변동 태그 설명]
📈가격↑ = 주가가 1σ(2.83%) 이상 상승 → 순위 하락 원인이 가격 반영이면 양호한 신호
📉가격↓ = 주가가 1σ 이상 하락
💪전망↑ = EPS 전망(adj_score)이 1σ(1.48) 이상 상승
⚠️전망↓ = EPS 전망이 1σ 이상 하락 → 펀더멘탈 악화 주의
순위: A→B→C = T-2→T-1→오늘 composite_rank 이력

[출력 규칙]
- 한국어, 친절하고 따뜻한 말투 (~예요/~해요 체)
- 딱딱한 보고서 말투 금지. 친구에게 설명하듯 자연스럽게.
- 인사말, 서두, 맺음말 금지. 아래 3개 섹션만 출력.
- 총 2000자 이내.

=== 반드시 출력할 3개 섹션 ===

📰 시장 동향
(필수) {biz_str} 미국 시장 마감 결과를 Google 검색해서 2~3줄로 요약해줘. 이 섹션은 반드시 출력해야 해.
- 어제 시장의 핵심 이슈(원인, 테마)만 2~3줄로. 지수 수치(S&P 몇 포인트, 나스닥 몇% 등)는 [1/4]에 이미 있으니 반복하지 마.
- "이번 주" 전체 요약은 하지 마.
- 오늘/내일 예정된 주요 이벤트(FOMC, 고용지표, 대형 어닝 등)가 있으면 한 줄 추가.
- 위 [현재 시장 환경]의 계절과 행동 권장을 참고해서 투자 판단 한마디 덧붙여줘.

⚠️ 매수 주의 종목
위 데이터에서 위험 신호(🔻/📉/📅)가 있는 종목만 골라서 설명해줘.
형식: 종목명(티커)를 굵게(**) 쓰고, 1~2줄로 왜 주의해야 하는지 설명.
종목과 종목 사이에 반드시 [SEP] 한 줄을 넣어서 구분해줘.
위험 신호가 없는 종목은 절대 넣지 마. 시스템 데이터에 없는 내용을 지어내지 마.
만약 위험 신호가 있는 종목이 하나도 없으면 "✅ 모든 후보가 현재 양호해요." 한 줄만 출력해.

예시:
**ABC Corp(ABC)**
최근 5명의 애널리스트가 EPS 전망치를 낮췄어요. 의견 하향이 많으니 조심하세요.
[SEP]
**XYZ Inc(XYZ)**
커버리지 애널리스트가 2명뿐이라 추정치를 100% 믿기 어려워요.

📅 어닝 주의
{earnings_info}
(위 내용 그대로 표시. 수정/추가 금지. "해당 없음"이면 이 섹션 생략.)"""

        grounding_tool = types.Tool(google_search=types.GoogleSearch())
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[grounding_tool],
                temperature=0.2,
            ),
        )

        def extract_text(resp):
            """response.text가 None일 때 parts에서 직접 추출"""
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

        analysis_text = extract_text(response)

        # 응답 유효성 검증: 비어있거나 필수 섹션(📰/⚠️) 누락이면 재시도
        def is_valid_response(text):
            if not text or len(text) < 50:
                return False
            return '📰' in text or '시장' in text

        if not is_valid_response(analysis_text):
            try:
                if hasattr(response, 'candidates') and response.candidates:
                    candidate = response.candidates[0]
                    log(f"Gemini finish_reason: {candidate.finish_reason}", "WARN")
            except Exception:
                pass
            reason = "비어있음" if not analysis_text else f"섹션 누락 ({len(analysis_text)}자)"
            log(f"Gemini 응답 부적합 ({reason}) — 재시도", "WARN")
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[grounding_tool],
                    temperature=0.3,
                ),
            )
            analysis_text = extract_text(response)
            if not is_valid_response(analysis_text):
                log("Gemini 재시도도 부적합", "WARN")
                if not analysis_text:
                    return None

        # Markdown → Telegram HTML 변환
        analysis_html = analysis_text
        analysis_html = analysis_html.replace('&', '&amp;')
        analysis_html = analysis_html.replace('<', '&lt;')
        analysis_html = analysis_html.replace('>', '&gt;')
        analysis_html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', analysis_html)
        analysis_html = re.sub(r'(?<!\w)\*(?!\s)(.+?)(?<!\s)\*(?!\w)', r'<i>\1</i>', analysis_html)
        analysis_html = re.sub(r'#{1,3}\s*', '', analysis_html)
        analysis_html = analysis_html.replace('---', '━━━')
        analysis_html = re.sub(r'\n*\[SEP\]\n*', '\n─────────\n', analysis_html)

        # DB 저장 (대시보드용)
        if biz_day and analysis_text:
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.execute(
                    'INSERT OR REPLACE INTO ai_analysis (date, analysis_type, ticker, content) VALUES (?,?,?,?)',
                    (biz_day.strftime('%Y-%m-%d'), 'ai_review', '__ALL__', analysis_text)
                )
                conn.commit()
                conn.close()
                log("AI 분석 결과 DB 저장 완료")
            except Exception as e:
                log(f"AI 분석 DB 저장 실패: {e}", "WARN")

        # 텔레그램 메시지 포맷팅
        lines = []
        lines.append('━━━━━━━━━━━━━━━━━━━')
        lines.append('  [3/4] 🤖 AI 리스크 필터')
        lines.append('━━━━━━━━━━━━━━━━━━━')
        lines.append(f'📅 {biz_day.strftime("%Y년 %m월 %d일")} (미국장 기준)')
        lines.append('')
        lines.append('매수 후보의 위험 요소를 AI가 걸러냈어요.')
        lines.append('')
        lines.append(analysis_html)
        lines.append('')
        lines.append('👉 다음: 최종 추천 포트폴리오 [4/4]')

        log("AI 리스크 필터 완료")
        return '\n'.join(lines)

    except Exception as e:
        log(f"AI 분석 실패: {e}", "ERROR")
        return None


def select_portfolio_stocks(results_df, status_map=None, weighted_ranks=None, earnings_map=None, risk_status=None):
    """포트폴리오 종목 선정 — ✅ 필터 → 리스크 필터 → 가중순위 정렬 → Top N

    Returns: (selected, portfolio_mode, concordance, final_action)
        selected: 선정된 종목 리스트 (dict, weight 포함) 또는 빈 리스트
        portfolio_mode: 'normal'|'caution'|'reduced'|'stop'
        concordance: 'both_stable'|'both_warn'|...
        final_action: 행동 권장 메시지
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

    filtered = get_part2_candidates(results_df, top_n=30)
    if filtered.empty:
        return [], portfolio_mode, concordance, final_action

    # ✅ (3일 검증) 종목만 대상
    verified_tickers = {t for t, s in status_map.items() if s == '✅'}
    if status_map:
        filtered = filtered[filtered['ticker'].isin(verified_tickers)]

    # 가중 순위로 정렬
    if weighted_ranks:
        filtered = filtered.copy()
        filtered['_weighted'] = filtered['ticker'].map(
            lambda t: weighted_ranks.get(t, {}).get('weighted', 50.0)
        )
        filtered = filtered.sort_values('_weighted').reset_index(drop=True)

    if filtered.empty:
        log("포트폴리오: ✅ 검증 종목 없음", "WARN")
        return [], portfolio_mode, concordance, final_action

    today_date = datetime.now().date()
    two_weeks = (datetime.now() + timedelta(days=14)).date()

    # 리스크 플래그 → 안전 종목만 선별
    log("포트폴리오: ✅ 종목 리스크 필터 적용 중...")
    safe = []
    for _, row in filtered.iterrows():
        t = row['ticker']
        eps_chg = row.get('eps_change_90d', 0) or 0
        price_chg = row.get('price_chg', 0) or 0
        fwd_pe = row.get('fwd_pe', 0) or 0
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
        earnings_note = ""
        ed = earnings_map.get(t)
        if ed and today_date <= ed <= two_weeks:
            earnings_note = f" 📅어닝 {ed.month}/{ed.day}"

        if flags:
            log(f"  ❌ {t}: {','.join(flags)} (gap={row.get('adj_gap',0):+.1f} desc={row.get('trend_desc','')})")
        else:
            v_status = status_map.get(t, '✅') if status_map else '✅'
            safe.append({
                'ticker': t,
                'name': row.get('short_name', t),
                'industry': row.get('industry', ''),
                'eps_chg': eps_chg, 'price_chg': price_chg,
                'fwd_pe': fwd_pe,
                'adj_gap': row.get('adj_gap', 0) or 0,
                'rev_up': rev_up, 'rev_down': rev_down,
                'num_analysts': num_analysts,
                'adj_score': row.get('adj_score', 0) or 0,
                'lights': row.get('trend_lights', ''),
                'desc': row.get('trend_desc', ''),
                'v_status': v_status,
                'price': row.get('price', 0) or 0,
                'rev_growth': row.get('rev_growth', 0) or 0,
                'earnings_note': earnings_note,
            })
            log(f"  {v_status} {t}: gap={row.get('adj_gap',0):+.1f} desc={row.get('trend_desc','')} up={rev_up} dn={rev_down}{earnings_note}")

    if not safe:
        log("포트폴리오: ✅ 종목 없음", "WARN")
        return [], portfolio_mode, concordance, final_action

    # 가중 순위 정렬
    if weighted_ranks:
        for s in safe:
            s['_weighted'] = weighted_ranks.get(s['ticker'], {}).get('weighted', 50.0)
        safe.sort(key=lambda x: x['_weighted'])

    log("포트폴리오: 가중 순위 (T0×0.5 + T1×0.3 + T2×0.2):")
    for i, s in enumerate(safe):
        w = s.get('_weighted', '-')
        log(f"    {i+1}. {s['ticker']}: 가중={w} gap={s['adj_gap']:+.1f} adj={s['adj_score']:.1f} {s['desc']} [{s['industry']}]")

    # L3: both_warn 시 신규 진입 종목 포트폴리오 제외
    if concordance == 'both_warn':
        before = len(safe)
        safe = [s for s in safe if s['v_status'] == '✅']
        excluded = before - len(safe)
        if excluded > 0:
            log(f"L3 시장 동결: both_warn — 신규 진입 {excluded}개 제외 (기존 ✅만 유지)")

    # stop 모드: 빈 리스트 반환
    if portfolio_mode == 'stop':
        log(f"포트폴리오: portfolio_mode=stop → 추천 중단 ({final_action})")
        return [], portfolio_mode, concordance, final_action

    # reduced 모드: Top 3만
    if portfolio_mode == 'reduced':
        selected = safe[:3]
    else:
        selected = safe[:5]

    if len(selected) < 3:
        log("포트폴리오: 선정 종목 부족", "WARN")
        return [], portfolio_mode, concordance, final_action

    # 동일 비중
    n = len(selected)
    base = 100 // n
    remainder = 100 - base * n
    weights = [base] * n
    for i in range(remainder):
        weights[i] += 1
    for i, s in enumerate(selected):
        s['weight'] = weights[i]

    log(f"포트폴리오: {n}종목 선정 — " +
        ", ".join(f"{s['ticker']}({s['weight']}%)" for s in selected))

    return selected, portfolio_mode, concordance, final_action


def run_portfolio_recommendation(config, results_df, status_map=None, biz_day=None, risk_status=None, weighted_ranks=None, earnings_map=None):
    """포트폴리오 추천 — 3일 검증(✅) + 리스크 필터 통과 종목 + 가중 순위 정렬"""
    try:
        import re
        import yfinance as yf

        if biz_day is None:
            biz_day = get_last_business_day()

        selected, portfolio_mode, concordance, final_action = select_portfolio_stocks(
            results_df, status_map, weighted_ranks, earnings_map, risk_status
        )

        if portfolio_mode == 'stop':
            lines = [
                '━━━━━━━━━━━━━━━━━━━',
                '   [4/4] 🎯 최종 추천',
                '━━━━━━━━━━━━━━━━━━━',
                f'📅 {biz_day.strftime("%Y년 %m월 %d일")} (미국장 기준)',
                '',
                '🚫 <b>신규 매수 중단</b>',
                '',
                final_action,
                '',
                '기존 보유 종목은 Top 30 이탈 시 매도하세요.',
                '<i>시장 안정 후 추천을 재개합니다.</i>',
            ]
            return '\n'.join(lines)

        if not selected:
            log("포트폴리오: 선정 종목 없음", "WARN")
            return '\n'.join([
                '━━━━━━━━━━━━━━━━━━━',
                '   [4/4] 🎯 최종 추천',
                '━━━━━━━━━━━━━━━━━━━',
                f'📅 {biz_day.strftime("%Y년 %m월 %d일")} (미국장 기준)',
                '',
                '검증된 종목 중 안전한 종목이 없어요.',
                '이번 회차는 <b>관망</b>을 권장합니다.',
                '',
                '무리한 진입보다 기다림이 나을 때도 있어요.',
            ])

        # Forward Test: 포트폴리오 이력 기록
        try:
            log_portfolio_trades(selected, biz_day.strftime('%Y-%m-%d'))
        except Exception as e:
            log(f"Forward Test 기록 실패: {e}", "WARN")

        # 시장 위험 컨텍스트 (Gemini 프롬프트용)
        market_ctx = ""
        if risk_status:
            hy = risk_status.get('hy')
            if hy:
                market_ctx += f"HY Spread: {hy['hy_spread']:.2f}% ({hy['quadrant_label']}, {hy.get('q_days', 0)}일째)\n"
            vix = risk_status.get('vix')
            if vix:
                market_ctx += f"VIX: {vix['vix_current']:.1f} (1년 중 {vix.get('vix_percentile', 0):.0f}th, {vix['regime_label']})\n"
            market_ctx += f"시장 판단: {concordance}\n"
            if final_action:
                market_ctx += f"행동 권장: {final_action}\n"

        # Gemini 프롬프트
        stock_lines = []
        for i, s in enumerate(selected):
            stock_lines.append(
                f"{i+1}. {s['name']}({s['ticker']}) · {s['industry']}\n"
                f"   {s['lights']} {s['desc']} · 비중 {s['weight']}%\n"
                f"   EPS {s['eps_chg']:+.1f}% · 매출 {s.get('rev_growth', 0) or 0:+.0%}\n"
                f"   의견 ↑{s['rev_up']} ↓{s['rev_down']}"
            )

        prompt = f"""아래 {len(selected)}종목 각각의 최근 실적 성장 배경을 Google 검색해서 한 줄씩 써줘.

[종목]
{chr(10).join(stock_lines)}

[형식]
- 한국어, ~예요 체
- 종목별: **N. 종목명(티커) · 비중 N%**
  날씨아이콘 + 비즈니스 매력 한 줄
- 종목 사이에 [SEP]
- 맨 끝 별도 문구 없음

[규칙]
- 각 종목의 실적 성장 배경(왜 EPS/매출이 오르는지)을 검색해서 써.
  예: "AI 데이터센터 수요 확대로 GPU 매출 급증 중이에요"
  예: "전력 수요 폭증에 원전 재가동 기대감까지 더해졌어요"
- 단순히 "EPS X% 상승"처럼 숫자만 반복하지 마. 그 숫자 뒤의 사업적 이유를 써.
- 주의/경고/유의 표현 금지. 긍정적 매력만.
- "선정", "포함", "선택" 같은 시스템 용어 금지.
- 서두/인사말/도입문 금지. "다음은", "요청하신", "소개해" 등 절대 쓰지 마. 첫 번째 종목부터 바로 시작.
- 종목마다 다른 문장 구조로 써."""

        def generate_template_descriptions(stocks):
            """Approach B: 코드 템플릿 — AI 없이 기존 데이터로 생성"""
            parts = []
            for i, s in enumerate(stocks):
                eps = s['eps_chg']
                rev = s.get('rev_growth', 0) or 0
                rev_up = s['rev_up']
                rev_down = s['rev_down']
                detail_parts = []
                if eps >= 100:
                    detail_parts.append(f'EPS {eps:+.0f}% 폭등')
                elif eps >= 30:
                    detail_parts.append(f'EPS {eps:+.0f}% 급등')
                elif eps >= 10:
                    detail_parts.append(f'EPS {eps:+.0f}% 상승')
                else:
                    detail_parts.append(f'EPS {eps:+.1f}%')
                if rev >= 0.5:
                    detail_parts.append(f'매출 {rev:+.0%} 고성장')
                elif rev >= 0.1:
                    detail_parts.append(f'매출 {rev:+.0%}')
                if rev_down == 0 and rev_up >= 3:
                    detail_parts.append(f'전원 상향({rev_up}명)')
                elif rev_up > rev_down * 2 and rev_up >= 3:
                    detail_parts.append(f'상향 우세(↑{rev_up}↓{rev_down})')
                detail = ' · '.join(detail_parts)
                parts.append(
                    f"<b>{i+1}. {s['name']}({s['ticker']}) · 비중 {s['weight']}%</b>\n"
                    f"{s['lights']} {s['desc']} · {detail}"
                )
            return '\n──────────────────\n'.join(parts)

        api_key = config.get('gemini_api_key', '')
        html = None

        if api_key:
            try:
                from google import genai
                from google.genai import types

                client = genai.Client(api_key=api_key)
                grounding_tool = types.Tool(google_search=types.GoogleSearch())
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[grounding_tool],
                        temperature=0.3,
                    ),
                )

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

                text = extract_text(response)
                if text:
                    # Gemini 서두 제거: 첫 번째 종목(**1.) 전 텍스트 삭제
                    first_stock = re.search(r'\*\*1\.', text)
                    if first_stock and first_stock.start() > 0:
                        removed = text[:first_stock.start()].strip()
                        if removed:
                            log(f"포트폴리오: Gemini 서두 제거 — '{removed[:50]}'")
                        text = text[first_stock.start():]
                    html = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', html)
                    html = re.sub(r'(?<!\w)\*(?!\s)(.+?)(?<!\s)\*(?!\w)', r'<i>\1</i>', html)
                    html = re.sub(r'#{1,3}\s*', '', html)
                    html = re.sub(r'\n*\[SEP\]\n*', '\n──────────────────\n', html)
                    log("포트폴리오: Gemini Search Grounding 응답 사용")
                else:
                    log("포트폴리오: Gemini 응답 없음 — 템플릿 fallback", "WARN")
            except Exception as e:
                log(f"포트폴리오: Gemini 호출 실패 ({e}) — 템플릿 fallback", "WARN")
        else:
            log("GEMINI_API_KEY 미설정 — 템플릿 모드")

        if not html:
            html = generate_template_descriptions(selected)
            log("포트폴리오: 코드 템플릿 fallback")

        # DB 저장 (대시보드용) — 포트폴리오 AI 설명
        if biz_day and html:
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.execute(
                    'INSERT OR REPLACE INTO ai_analysis (date, analysis_type, ticker, content) VALUES (?,?,?,?)',
                    (biz_day.strftime('%Y-%m-%d'), 'portfolio_narrative', '__ALL__', html)
                )
                conn.commit()
                conn.close()
                log("포트폴리오 AI 설명 DB 저장 완료")
            except Exception as e:
                log(f"포트폴리오 AI 설명 DB 저장 실패: {e}", "WARN")

        lines = [
            '━━━━━━━━━━━━━━━━━━━',
            '   [4/4] 🎯 최종 추천',
            '━━━━━━━━━━━━━━━━━━━',
            f'📅 {biz_day.strftime("%Y년 %m월 %d일")} (미국장 기준)',
            '',
            f'916종목 → Top 30 → ✅ 3일 검증 → <b>최종 {len(selected)}종목</b>',
        ]

        # #6: Q1 봄 + 전지표 안정 → 💎 기회 강조
        hy_q = (risk_status.get('hy') or {}).get('quadrant', '') if risk_status else ''
        if hy_q == 'Q1' and concordance == 'both_stable':
            lines.append('')
            lines.append('💎 <b>역사적 매수 기회!</b> 모든 지표가 매수를 가리켜요.')

        lines.extend([
            '',
            '─────────────────',
            html,
        ])

        # 주의사항 — 실질적 경고만 표시
        warnings = []

        # 어닝 임박 종목
        earnings_stocks = [s for s in selected if s.get('earnings_note')]
        for s in earnings_stocks:
            ed = s["earnings_note"].replace("📅어닝", "").replace("📅", "").strip()
            warnings.append(f'{s["name"]}({s["ticker"]}) {ed} 어닝 변동성 주의')

        # 섹터 집중 경고
        from collections import Counter
        industries = [s['industry'] for s in selected if s.get('industry')]
        tech_keywords = ['반도체', '전자부품', 'HW', '통신장비', '계측']
        tech_count = sum(1 for ind in industries if any(kw in ind for kw in tech_keywords))
        sector_counts = Counter(industries)
        concentrated = [f'{name} {cnt}' for name, cnt in sector_counts.most_common() if cnt >= 3]
        if tech_count >= 3:
            warnings.append(f'테크/반도체 {tech_count}/{len(selected)}종목 집중 — 동반 하락 리스크')
        elif concentrated:
            warnings.append(f'업종 집중: {", ".join(concentrated)} — 분산 점검')

        # caution 모드: 시장 주의 경고
        if portfolio_mode == 'caution':
            warnings.append(f'시장 주의 — {final_action}')
        # reduced 모드: 분할 매수 안내
        if portfolio_mode == 'reduced':
            warnings.append('겨울 후기 분할 매수 — Top 3 축소 추천')

        if warnings:
            lines.append('')
            lines.append('⚠️ <b>주의</b>')
            for w in warnings:
                lines.append(f'  {w}')

        lines.extend([
            '',
            '목록에 있으면 보유, 빠지면 매도 검토',
            '최소 2주 보유 · 매일 후보 갱신 확인',
            '<i>참고용이며, 투자 판단은 본인 책임이에요.</i>',
        ])

        log("포트폴리오 추천 완료")
        return '\n'.join(lines)

    except Exception as e:
        log(f"포트폴리오 추천 실패: {e}", "ERROR")
        return None


# ============================================================
# v2 메시지 (압축 2개 포맷)
# ============================================================

def classify_exit_reasons(exited_tickers, results_df):
    """이탈 종목 사유 분류 — 목표달성(괴리+만) vs 펀더멘탈 악화

    Returns: {'achieved': [(ticker, reasons)], 'degraded': [(ticker, reasons)]}
    """
    import pandas as pd
    result = {'achieved': [], 'degraded': []}
    if not exited_tickers or results_df is None or results_df.empty:
        return result

    # 현재 데이터에서 이탈 종목 정보 조회
    full_data = {}
    for _, row in results_df.iterrows():
        t = row.get('ticker', '')
        if t in exited_tickers:
            full_data[t] = row

    for t, prev_rank in sorted(exited_tickers.items(), key=lambda x: x[1]):
        reasons = []
        if t in full_data:
            r = full_data[t]
            if (r.get('price', 0) or 0) < (r.get('ma60', 0) or 0) and (r.get('ma60', 0) or 0) > 0:
                reasons.append('MA60↓')
            if (r.get('adj_gap', 0) or 0) > 0:
                reasons.append('괴리+')
            if (r.get('adj_score', 0) or 0) <= 9:
                reasons.append('점수↓')
            if (r.get('eps_change_90d', 0) or 0) <= 0:
                reasons.append('EPS↓')
        if not reasons:
            reasons.append('순위↓')

        if reasons == ['괴리+']:
            result['achieved'].append((t, reasons))
        else:
            result['degraded'].append((t, reasons))

    return result


def run_v2_ai_analysis(config, selected, biz_day, risk_status=None):
    """v2: Gemini 2회 호출 — (1) 시장 요약 (2) 종목 내러티브

    AI 실패 시에도 빈 결과를 반환하여 메시지 정상 작동 보장.
    Returns: {'market_summary': str, 'narratives': {ticker: str}}
    """
    import re

    api_key = config.get('gemini_api_key', '')
    result = {'market_summary': '', 'narratives': {}}

    if not api_key:
        log("v2 AI: GEMINI_API_KEY 미설정 — AI 없이 진행")
        return result

    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=api_key)
        grounding_tool = types.Tool(google_search=types.GoogleSearch())
    except Exception as e:
        log(f"v2 AI: Gemini 초기화 실패: {e}", "WARN")
        return result

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
        market_ctx = ""
        if risk_status:
            f_action = risk_status.get('final_action', '')
            if f_action:
                market_ctx = f"현재 시장 판단: {f_action}"

        market_prompt = f"""{biz_str} 미국 주식시장 마감 결과를 Google 검색해서 2~3줄로 요약해줘.

{market_ctx}

규칙:
- 핵심 이슈(원인, 테마)만 간결하게.
- 지수 수치(S&P, 나스닥 등)는 별도 표시하니 생략.
- 주요 이벤트 있으면 한 줄 추가.
- 마지막에 투자 판단 한마디 (위 시장 판단 참고).
- 한국어, ~예요 체.
- 인사말/서두/맺음말 없이 바로 시작."""

        resp = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=market_prompt,
            config=types.GenerateContentConfig(
                tools=[grounding_tool],
                temperature=0.2,
            ),
        )
        text = extract_text(resp)
        if text:
            # 마크다운 제거
            text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
            text = re.sub(r'#{1,3}\s*', '', text)
            result['market_summary'] = text.strip()
            log(f"v2 AI: 시장요약 {len(result['market_summary'])}자")
        else:
            log("v2 AI: 시장요약 Gemini 응답 없음", "WARN")
    except Exception as e:
        log(f"v2 AI: 시장요약 실패: {e}", "WARN")

    # ── 호출 2: 종목 내러티브 (v1 프롬프트 패턴 활용) ──
    if selected:
        try:
            stock_lines = []
            for i, s in enumerate(selected):
                rev = s.get('rev_growth', 0) or 0
                stock_lines.append(
                    f"{i+1}. {s['name']}({s['ticker']}) · {s['industry']}\n"
                    f"   EPS {s['eps_chg']:+.1f}% · 매출 {rev:+.0%}"
                )

            stock_prompt = f"""아래 {len(selected)}종목 각각의 최근 실적 성장 배경을 Google 검색해서 한 줄씩 써줘.

[종목]
{chr(10).join(stock_lines)}

[형식]
종목별로 한 줄씩. 종목 사이에 [SEP] 표시.
형식: TICKER: 설명 한 줄

[규칙]
- 각 종목의 실적 성장 배경(왜 EPS/매출이 오르는지)을 검색해서 써.
  예: "NVDA: AI 데이터센터 GPU 수요 확대로 매출이 급증하고 있어요"
  예: "VST: 전력 수요 폭증에 원전 재가동 기대감까지 더해졌어요"
- 단순히 "EPS X% 상승"처럼 숫자만 반복하지 마. 그 숫자 뒤의 사업적 이유를 써.
- 주의/경고/유의 표현 금지. 긍정적 매력만.
- 한국어, ~예요 체, 종목마다 다른 문장 구조.
- 서두/인사말/맺음말 금지. 첫 종목부터 바로 시작."""

            resp = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=stock_prompt,
                config=types.GenerateContentConfig(
                    tools=[grounding_tool],
                    temperature=0.3,
                ),
            )
            text = extract_text(resp)
            if text:
                # 마크다운 볼드 제거
                text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
                text = re.sub(r'#{1,3}\s*', '', text)

                # 파싱: "TICKER: 설명" 패턴 (여러 변형 허용)
                for line in text.split('\n'):
                    line = line.strip()
                    if not line or line == '[SEP]':
                        continue
                    # "TICKER: 설명" / "N. TICKER: 설명" / "- TICKER: 설명"
                    m = re.match(r'(?:\d+\.\s*)?(?:-\s*)?([A-Z]{1,5})[\s:：]+(.{10,})', line)
                    if m:
                        ticker = m.group(1)
                        narrative = m.group(2).strip()
                        # "TICKER:" 등 잔여 제거
                        narrative = re.sub(r'^[:\s]+', '', narrative)
                        if narrative:
                            result['narratives'][ticker] = narrative

                log(f"v2 AI: 내러티브 {len(result['narratives'])}종목")
            else:
                log("v2 AI: 내러티브 Gemini 응답 없음", "WARN")
        except Exception as e:
            log(f"v2 AI: 내러티브 실패: {e}", "WARN")

    return result


def create_v2_signal_message(selected, risk_status, market_lines, earnings_map,
                              exit_reasons, biz_day, ai_content, portfolio_mode,
                              concordance, final_action,
                              weighted_ranks=None, rank_change_tags=None):
    """v2 메시지 1: 오늘의 추천

    구조: 성적표 → 프로세스 → 추천(스토리텔링) → 리스크 → 이탈 → 시장요약 → 면책
    각 종목: 업종+트렌드 → 실적+분석가 → 순위궤적 → AI내러티브
    → 읽으면서 "이 논리라면 사볼만하겠다"고 납득하는 흐름.
    """
    import re

    if weighted_ranks is None:
        weighted_ranks = {}
    if rank_change_tags is None:
        rank_change_tags = {}

    biz_str = biz_day.strftime('%m.%d')
    weekdays = ['월', '화', '수', '목', '금', '토', '일']
    weekday = weekdays[biz_day.weekday()]

    lines = []
    lines.append(f'📊 EPS 모멘텀 US · {biz_str}({weekday})')
    lines.append('')

    # ── 성적표: 시장 상태 2줄 ──
    hy_data = risk_status.get('hy') if risk_status else None

    signal_dots = ''
    if risk_status:
        conc = risk_status.get('concordance', 'both_stable')
        if conc == 'both_stable':
            signal_dots = '🟢🟢 안정'
        elif conc == 'both_warn':
            signal_dots = '🔴🔴 위험'
        else:
            signal_dots = '🟢🔴 주의'

    if hy_data:
        q_days = hy_data.get('q_days', 0)
        lines.append(f'{signal_dots} · {hy_data["quadrant_icon"]} {hy_data["quadrant_label"]} {q_days}일째')
    elif signal_dots:
        lines.append(signal_dots)

    # 지수 1줄
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
        if idx_parts:
            lines.append(' · '.join(idx_parts))

    # ── stop 모드 ──
    if portfolio_mode == 'stop':
        lines.append('')
        lines.append('🚫 <b>신규 매수 중단</b>')
        lines.append(final_action)
        lines.append('')
        lines.append('기존 보유 종목은 Top 30 이탈 시 매도하세요.')
        return '\n'.join(lines)

    # ── 추천 종목 없음 ──
    if not selected:
        lines.append('')
        lines.append('검증된 종목 중 안전한 종목이 없어요.')
        lines.append('이번 회차는 <b>관망</b>을 권장해요.')
        return '\n'.join(lines)

    # ── 프로세스 라인 ──
    lines.append('')
    lines.append(f'미국 916종목 중 EPS·매출 성장 상위 30개를 3일 검증, <b>최종 {len(selected)}종목</b> 선정')

    # Q1 + both_stable: 역사적 매수 기회
    hy_q = (risk_status.get('hy') or {}).get('quadrant', '') if risk_status else ''
    if hy_q == 'Q1' and concordance == 'both_stable':
        lines.append('💎 <b>역사적 매수 기회!</b> 모든 지표가 매수를 가리켜요.')

    # ── 추천 종목 (스토리텔링) ──
    lines.append('')
    lines.append(f'━━ 오늘의 포트폴리오 ━━')

    narratives = ai_content.get('narratives', {}) if ai_content else {}

    for i, s in enumerate(selected):
        lines.append('')
        ticker = s['ticker']
        eps_chg = s['eps_chg']
        rev = s.get('rev_growth', 0) or 0
        rev_pct = f'{rev*100:+.0f}%' if rev else ''
        rev_up = s.get('rev_up', 0)
        rev_down = s.get('rev_down', 0)
        num_analysts = s.get('num_analysts', rev_up + rev_down)
        earnings = s.get('earnings_note', '')
        earnings_tag = f' 📅{earnings.replace("📅어닝 ", "").replace("📅", "").strip()}' if earnings else ''

        # 라인 1: 종목명 + 비중 + 어닝
        lines.append(f'<b>{i+1}. {s["name"]}({ticker}) · {s["weight"]}%</b>{earnings_tag}')

        # 라인 2: 업종 + 트렌드 (이 종목이 어떤 흐름인지)
        lights = s.get('lights', '')
        desc = s.get('desc', '')
        if lights and desc:
            lines.append(f'{s["industry"]} · {lights} {desc}')
        else:
            lines.append(f'{s["industry"]}')

        # 라인 3: 실적 + 분석가 (숫자 근거)
        analyst_str = f' · 분석가 ↑{rev_up} ↓{rev_down}' if num_analysts > 0 else ''
        lines.append(f'EPS {eps_chg:+.0f}% · 매출 {rev_pct}{analyst_str}')

        # 라인 4: 순위 궤적 (3일간 안정성 증명)
        w_info = weighted_ranks.get(ticker)
        if w_info:
            r0, r1, r2 = w_info['r0'], w_info['r1'], w_info['r2']
            v_status = s.get('v_status', '✅')
            if v_status == '🆕':
                rank_str = f'-→-→{r0}'
            elif v_status == '⏳':
                r1_str = str(r1) if r1 < 50 else '-'
                rank_str = f'-→{r1_str}→{r0}'
            else:
                r2_str = str(r2) if r2 < 50 else '-'
                r1_str = str(r1) if r1 < 50 else '-'
                rank_str = f'{r2_str}→{r1_str}→{r0}'
            tag = rank_change_tags.get(ticker, '')
            tag_suffix = f' ({tag})' if tag else ''
            lines.append(f'3일순위 {rank_str}{tag_suffix}')

        # 라인 5: AI 내러티브 (왜 실적이 좋은지 — 있으면 보너스)
        narrative = narratives.get(ticker, '')
        if narrative:
            lines.append(f'💬 {narrative}')

    # ── 경고 ──
    warnings = []
    earnings_stocks = [s for s in selected if s.get('earnings_note')]
    for s in earnings_stocks:
        ed_str = s["earnings_note"].replace("📅어닝 ", "").replace("📅", "").strip()
        warnings.append(f'{s["ticker"]} 실적발표 {ed_str}')

    from collections import Counter
    industries = [s['industry'] for s in selected if s.get('industry')]
    tech_keywords = ['반도체', '전자부품', 'HW', '통신장비', '계측']
    tech_count = sum(1 for ind in industries if any(kw in ind for kw in tech_keywords))
    if tech_count >= 3:
        warnings.append(f'테크 {tech_count}/{len(selected)}종목 집중')

    if portfolio_mode == 'caution':
        warnings.append('시장 주의')
    if portfolio_mode == 'reduced':
        warnings.append('겨울 후기 — Top 3 축소')

    if warnings:
        lines.append('')
        lines.append('⚠️ ' + ' | '.join(warnings))

    # ── 이탈 종목 (사유 포함) ──
    if exit_reasons:
        achieved = exit_reasons.get('achieved', [])
        degraded = exit_reasons.get('degraded', [])
        exit_parts = []
        for t, reasons in achieved:
            exit_parts.append(f'{t}(목표달성)')
        for t, reasons in degraded:
            reason_str = ','.join(reasons)
            exit_parts.append(f'{t}({reason_str})')
        if exit_parts:
            lines.append(f'📉 Top 30 이탈: {" · ".join(exit_parts)}')

    # ── 시장 요약 (AI, 없으면 생략) ──
    market_summary = ai_content.get('market_summary', '') if ai_content else ''
    if market_summary:
        lines.append('')
        lines.append(f'📰 {market_summary}')

    # ── 면책 ──
    lines.append('')
    lines.append('목록에 있으면 보유, 빠지면 매도 검토해요.')
    lines.append('<i>참고용이며, 투자 판단은 본인 책임이에요.</i>')

    return '\n'.join(lines)


def create_v2_watchlist_message(results_df, status_map, exited_tickers, today_tickers, biz_day,
                                weighted_ranks=None, rank_change_tags=None):
    """v2 메시지 2: 매수 후보 30 — v1 [2/4]와 동일 포맷

    종목당 4줄(이름/업종·트렌드/실적/분석가·3일순위) + 이탈 상세 — 전체 증거를 보여주는 메시지.
    신용이 없는 발신자의 메시지에서 신뢰를 만드는 건 "과정의 투명성"이므로 정보를 줄이지 않는다.
    """
    import pandas as pd

    if results_df is None or results_df.empty:
        return None

    if weighted_ranks is None:
        weighted_ranks = {}
    if rank_change_tags is None:
        rank_change_tags = {}
    if status_map is None:
        status_map = {}
    if exited_tickers is None:
        exited_tickers = {}

    # DB의 가중순위 Top 30과 동일한 목록 사용
    if today_tickers:
        filtered = results_df[results_df['ticker'].isin(today_tickers)].copy()
    else:
        filtered = get_part2_candidates(results_df, top_n=30)
    count = len(filtered)

    # 가중 순위로 정렬
    if weighted_ranks:
        filtered = filtered.copy()
        filtered['_weighted'] = filtered['ticker'].map(
            lambda t: weighted_ranks.get(t, {}).get('weighted', 50.0)
        )
        filtered = filtered.sort_values('_weighted').reset_index(drop=True)

    from collections import Counter

    lines = []
    lines.append(f'📋 <b>매수 후보 {count}개</b>')

    # 주도 업종
    sector_counts = Counter(row.get('industry', '기타') for _, row in filtered.iterrows())
    top_sectors = sector_counts.most_common()
    sector_parts = [f'{name} {cnt}' for name, cnt in top_sectors if cnt >= 2]
    if sector_parts:
        lines.append(f'📊 주도 업종: {" · ".join(sector_parts)}')
    lines.append('─────────────────')

    # ── 30종목 상세 (v1 [2/4] 동일 포맷) ──
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
        name = row.get('short_name', ticker)
        tag = rank_change_tags.get(ticker, '') if marker != '🆕' else ''

        lines.append(f'{marker} <b>{rank}.</b> {name}({ticker})')
        lines.append(f'{industry} · {lights} {desc}')
        parts = []
        if pd.notna(eps_90d):
            parts.append(f'EPS {eps_90d:+.0f}%')
        if pd.notna(rev_g):
            parts.append(f'매출 {rev_g*100:+.0f}%')
        if parts:
            lines.append(' · '.join(parts))

        # 순위 궤적
        w_info = weighted_ranks.get(ticker)
        if w_info:
            r0, r1, r2 = w_info['r0'], w_info['r1'], w_info['r2']
            if marker == '🆕':
                rank_str = f'-→-→{r0}'
            elif marker == '⏳':
                r1_str = str(r1) if r1 < 50 else '-'
                rank_str = f'-→{r1_str}→{r0}'
            else:
                r2_str = str(r2) if r2 < 50 else '-'
                r1_str = str(r1) if r1 < 50 else '-'
                rank_str = f'{r2_str}→{r1_str}→{r0}'
        else:
            rank_str = f'-→-→{rank}'
        tag_suffix = f' ({tag})' if tag else ''
        lines.append(f'분석가 ↑{rev_up} ↓{rev_down} · 3일순위 {rank_str}{tag_suffix}')
        lines.append('──────────────────')

    # ── 이탈 종목: v1과 동일한 상세 포맷 ──
    if exited_tickers:
        all_eligible = get_part2_candidates(results_df)
        current_rank_map = {row['ticker']: i + 1 for i, (_, row) in enumerate(all_eligible.iterrows())}
        sorted_exits = sorted(exited_tickers.items(), key=lambda x: x[1])
        full_data = {row['ticker']: row for _, row in results_df.iterrows()}

        achieved = []
        degraded = []
        for t, prev_rank in sorted_exits:
            cur_rank = current_rank_map.get(t)
            reasons = []
            if t in full_data:
                r = full_data[t]
                if (r.get('price', 0) or 0) < (r.get('ma60', 0) or 0) and (r.get('ma60', 0) or 0) > 0:
                    reasons.append('MA60↓')
                if (r.get('adj_gap', 0) or 0) > 0:
                    reasons.append('괴리+')
                if (r.get('adj_score', 0) or 0) <= 9:
                    reasons.append('점수↓')
                if (r.get('eps_change_90d', 0) or 0) <= 0:
                    reasons.append('EPS↓')
            if not reasons and cur_rank and cur_rank > 30:
                reasons.append('순위↓')
            if not reasons:
                reasons.append('순위↓')
            if reasons == ['괴리+']:
                achieved.append((t, prev_rank, cur_rank, reasons))
            else:
                degraded.append((t, prev_rank, cur_rank, reasons))

        def _render_exit(exit_list):
            for t, prev_rank, cur_rank, reasons in exit_list:
                row = full_data.get(t, {})
                nm = row.get('short_name', t) if hasattr(row, 'get') else t
                ind = row.get('industry', '') if hasattr(row, 'get') else ''
                lt = row.get('trend_lights', '') if hasattr(row, 'get') else ''
                ds = row.get('trend_desc', '') if hasattr(row, 'get') else ''
                ep = row.get('eps_change_90d') if hasattr(row, 'get') else None
                rv = row.get('rev_growth') if hasattr(row, 'get') else None
                ru = int(row.get('rev_up30', 0) or 0) if hasattr(row, 'get') else 0
                rd = int(row.get('rev_down30', 0) or 0) if hasattr(row, 'get') else 0
                tg = rank_change_tags.get(t, '')

                lines.append(f'{nm}({t})')
                lines.append(f'{ind} · {lt} {ds}')
                pts = []
                if ep is not None and pd.notna(ep):
                    pts.append(f'EPS {ep:+.0f}%')
                if rv is not None and pd.notna(rv):
                    pts.append(f'매출 {rv*100:+.0f}%')
                if pts:
                    lines.append(' · '.join(pts))
                ri = f'{prev_rank}→{cur_rank}' if cur_rank else f'{prev_rank}→탈락'
                rt = ' '.join(f'[{r}]' for r in reasons)
                ts = f' ({tg})' if tg else ''
                lines.append(f'분석가 ↑{ru} ↓{rd} · 3일순위 {ri} {rt}{ts}')
                lines.append('──────────────────')

        lines.append('')
        lines.append('📉 <b>Top 30 이탈 종목</b>')
        lines.append('─────────────────')
        if achieved:
            lines.append(f'✅ <b>목표 달성</b> ({len(achieved)}개) — 수익 실현 검토')
            _render_exit(achieved)
        if degraded:
            if achieved:
                lines.append('')
            lines.append(f'⚠️ <b>펀더멘탈 악화</b> ({len(degraded)}개) — 매도 검토')
            _render_exit(degraded)

    lines.append('')
    lines.append('Top 5 = 포트폴리오, 6~30 = 대기')
    lines.append('이탈 = 매도 검토 대상이에요.')
    lines.append('')
    lines.append('☀️상승 ☁️보합 🌧️하락 🔥급등 | ✅3일검증 ⏳2일 🆕신규')

    return '\n'.join(lines)


# ============================================================
# 텔레그램 전송
# ============================================================

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
        log(f"텔레그램 전송 실패: {e}", "ERROR")
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
    results_df, turnaround_df, stats, today_str = run_ntm_collection(config)

    # 2. Part 2 rank 저장 + 3일 교집합 + 어제 대비 변동
    import pandas as pd

    status_map = {}
    rank_history = {}
    weighted_ranks = {}
    rank_change_tags = {}
    exited_tickers = {}
    today_tickers = []
    earnings_map = {}

    # 2.5. 시장 지수 수집 (yfinance rate limit 전에 먼저)
    market_lines = get_market_context()
    if market_lines:
        log(f"시장 지수: {len(market_lines)}개")

    if not results_df.empty:
        # 매출+품질 수집 → rev_growth composite score + 12개 재무지표 DB 저장 (v33)
        results_df, earnings_map = fetch_revenue_growth(results_df, today_str)

        # 가중순위 기반 Top 30 선정 + DB 저장
        today_tickers = save_part2_ranks(results_df, today_str) or []

        status_map = get_3day_status(today_tickers)
        rank_history = get_rank_history(today_tickers)
        weighted_ranks = compute_weighted_ranks(today_tickers)
        rank_change_tags = get_rank_change_tags(today_tickers, weighted_ranks)
        _, exited_tickers = get_daily_changes(today_tickers)

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
    msg_market = create_market_message(results_df, market_lines, risk_status=risk_status) if not results_df.empty else None
    msg_candidates = create_candidates_message(results_df, status_map, exited_tickers, rank_history, risk_status=risk_status, weighted_ranks=weighted_ranks, rank_change_tags=rank_change_tags, today_tickers=today_tickers) if not results_df.empty else None

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
        message_version = config.get('message_version', 'v1')
        biz_day = get_last_business_day()

        if message_version == 'v2':
            # ===== v2: 압축 2개 메시지 =====
            log(f"메시지 버전: v2 (압축 포맷)")

            # 포트폴리오 종목 선정
            selected, portfolio_mode, concordance, final_action = select_portfolio_stocks(
                results_df, status_map, weighted_ranks, earnings_map, risk_status
            )

            # Forward Test 기록
            if selected:
                try:
                    log_portfolio_trades(selected, biz_day.strftime('%Y-%m-%d'))
                except Exception as e:
                    log(f"Forward Test 기록 실패: {e}", "WARN")

            # 이탈 종목 사유 분류
            exit_reasons = classify_exit_reasons(exited_tickers, results_df)

            # AI 2회 호출 (시장 요약 + 종목 내러티브, 실패해도 OK)
            ai_content = run_v2_ai_analysis(config, selected, biz_day, risk_status)

            # 메시지 1: 오늘의 추천
            msg_signal = create_v2_signal_message(
                selected, risk_status, market_lines, earnings_map,
                exit_reasons, biz_day, ai_content, portfolio_mode,
                concordance, final_action,
                weighted_ranks=weighted_ranks, rank_change_tags=rank_change_tags
            )
            if msg_signal:
                if send_to_channel:
                    send_telegram_long(msg_signal, config, chat_id=channel_id)
                send_telegram_long(msg_signal, config, chat_id=private_id)
                log(f"v2 시그널 메시지 전송 완료 → {dest}")

            # 메시지 2: 매수 후보 30 (v1 [2/4] 동일 포맷)
            msg_watchlist = create_v2_watchlist_message(
                results_df, status_map, exited_tickers, today_tickers, biz_day,
                weighted_ranks=weighted_ranks, rank_change_tags=rank_change_tags
            )
            if msg_watchlist:
                if send_to_channel:
                    send_telegram_long(msg_watchlist, config, chat_id=channel_id)
                send_telegram_long(msg_watchlist, config, chat_id=private_id)
                log(f"v2 워치리스트 전송 완료 → {dest}")

        else:
            # ===== v1: 기존 6개 메시지 (변경 없음) =====
            log(f"메시지 버전: v1 (기존 포맷)")

            # 📖 투자 가이드
            msg_guide = create_guide_message()
            if send_to_channel:
                send_telegram_long(msg_guide, config, chat_id=channel_id)
            send_telegram_long(msg_guide, config, chat_id=private_id)
            log(f"📖 투자 가이드 전송 완료 → {dest}")

            # [1/4] 시장 현황
            if msg_market:
                if send_to_channel:
                    send_telegram_long(msg_market, config, chat_id=channel_id)
                send_telegram_long(msg_market, config, chat_id=private_id)
                log(f"[1/4] 시장 현황 전송 완료 → {dest}")

            # [2/4] 매수 후보
            if msg_candidates:
                if send_to_channel:
                    send_telegram_long(msg_candidates, config, chat_id=channel_id)
                send_telegram_long(msg_candidates, config, chat_id=private_id)
                log(f"[2/4] 매수 후보 전송 완료 → {dest}")

            # [3/4] AI 리스크 필터
            msg_ai = run_ai_analysis(config, results_df=results_df, status_map=status_map, biz_day=biz_day, risk_status=risk_status, earnings_map=earnings_map, rank_change_tags=rank_change_tags, weighted_ranks=weighted_ranks)
            if msg_ai:
                if send_to_channel:
                    send_telegram_long(msg_ai, config, chat_id=channel_id)
                send_telegram_long(msg_ai, config, chat_id=private_id)
                log(f"[3/4] AI 리스크 필터 전송 완료 → {dest}")

            # [4/4] 최종 추천
            msg_portfolio = run_portfolio_recommendation(config, results_df, status_map, biz_day=biz_day, risk_status=risk_status, weighted_ranks=weighted_ranks, earnings_map=earnings_map)
            if msg_portfolio:
                if send_to_channel:
                    send_telegram_long(msg_portfolio, config, chat_id=channel_id)
                send_telegram_long(msg_portfolio, config, chat_id=private_id)
                log(f"[4/4] 최종 추천 전송 완료 → {dest}")

        # 시스템 로그 → 개인봇에만 (항상, v1/v2 공통)
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
