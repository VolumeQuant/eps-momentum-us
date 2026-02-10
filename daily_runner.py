"""
EPS Momentum Daily Runner v19 - Safety & Trend Fusion

기능:
1. NTM EPS 전 종목 수집 + MA60 계산 & DB 적재
2. 텔레그램 메시지 3종 + 로그 생성 & 발송
   - [1/3] 매수 후보: adj_gap순, MA60+adj_gap≤0+$10 필터, ✅3일검증/🆕신규/🚨탈락
   - [2/3] AI 브리핑: Gemini 2.5 Flash + Google Search
   - [3/3] 포트폴리오: ✅ 종목만 선정, 리스크 필터
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
                          ('price', 'REAL'), ('ma60', 'REAL'), ('part2_rank', 'INTEGER')]:
        try:
            cursor.execute(f'ALTER TABLE ntm_screening ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass  # 이미 존재

    # 기존 eps_snapshots 테이블 삭제
    cursor.execute('DROP TABLE IF EXISTS eps_snapshots')

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
    today_str = today.strftime('%Y-%m-%d')

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

            # EPS Revision & 애널리스트 수 추출 (이미 캐시된 _earnings_trend에서)
            rev_up30 = 0
            rev_down30 = 0
            num_analysts = 0
            try:
                raw_trend = stock._analysis._earnings_trend
                if raw_trend:
                    for item in raw_trend:
                        if item.get('period') == '0y':
                            eps_rev = item.get('epsRevisions', {})
                            up_data = eps_rev.get('upLast30days', {})
                            down_data = eps_rev.get('downLast30days', {})
                            rev_up30 = up_data.get('raw', 0) if isinstance(up_data, dict) else 0
                            rev_down30 = down_data.get('raw', 0) if isinstance(down_data, dict) else 0
                            ea = item.get('earningsEstimate', {})
                            na_data = ea.get('numberOfAnalysts', {})
                            num_analysts = na_data.get('raw', 0) if isinstance(na_data, dict) else 0
                            break
            except Exception:
                pass

            # DB 적재 (기본 데이터 — price/ma60/adj_gap은 후속 UPDATE로 추가)
            cursor.execute('''
                INSERT OR REPLACE INTO ntm_screening
                (date, ticker, rank, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turnaround)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                SET adj_score=?, adj_gap=?, price=?, ma60=?
                WHERE date=? AND ticker=?
            ''', (adj_score, adj_gap, current_price, ma60_val, today_str, ticker))

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

    if not results_df.empty:
        stats['score_gt0'] = int((results_df['score'] > 0).sum())
        stats['score_gt3'] = int((results_df['score'] > 3).sum())
        stats['aligned_count'] = int((~results_df['trend_lights'].str.contains('🌧️')).sum())

    log(f"수집 완료: 메인 {len(results)}, 턴어라운드 {len(turnaround)}, "
        f"데이터없음 {len(no_data)}, 에러 {len(errors)}")

    return results_df, turnaround_df, stats


# ============================================================
# Part 2 공통 필터 & 3일 교집합
# ============================================================

def get_part2_candidates(df, top_n=None):
    """Part 2 매수 후보 필터링 (공통 함수)

    필터: adj_score > 9, adj_gap ≤ 0, fwd_pe > 0, eps > 0, price ≥ $10, price > MA60
    정렬: adj_gap 오름차순 (더 음수 = 더 저평가)
    """
    filtered = df[
        (df['adj_score'] > 9) &
        (df['adj_gap'].notna()) & (df['adj_gap'] <= 0) &
        (df['fwd_pe'].notna()) & (df['fwd_pe'] > 0) &
        (df['eps_change_90d'] > 0) &
        (df['price'].notna()) & (df['price'] >= 10) &
        (df['ma60'].notna()) & (df['price'] > df['ma60'])
    ].copy()

    filtered = filtered.sort_values('adj_gap', ascending=True)

    if top_n:
        filtered = filtered.head(top_n)
    return filtered


def save_part2_ranks(results_df, today_str):
    """Part 2 eligible 종목에 part2_rank 저장 (3일 교집합용)"""
    candidates = get_part2_candidates(results_df)
    if candidates.empty:
        log("Part 2 후보 0개 — part2_rank 저장 스킵")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for i, (_, row) in enumerate(candidates.iterrows()):
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
            (i + 1, today_str, row['ticker'])
        )

    conn.commit()
    conn.close()
    log(f"Part 2 rank 저장: {len(candidates)}개 종목")


def is_cold_start():
    """DB에 part2_rank 데이터가 3일 미만이면 True (채널 전송 제어용)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT date) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    count = cursor.fetchone()[0]
    conn.close()
    return count < 3


def get_3day_status(today_tickers):
    """3일 연속 Part 2 진입 여부 판별 → {ticker: '✅' or '🆕'}"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 최근 3개 distinct date
    cursor.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC LIMIT 3')
    dates = [r[0] for r in cursor.fetchall()]

    if len(dates) < 3:
        conn.close()
        log(f"3일 교집합: DB {len(dates)}일뿐 — 전부 ✅ 처리 (cold start)")
        return {t: '✅' for t in today_tickers}

    # 3일 모두 part2_rank가 있는 종목
    placeholders = ','.join('?' * len(dates))
    cursor.execute(f'''
        SELECT ticker FROM ntm_screening
        WHERE date IN ({placeholders}) AND part2_rank IS NOT NULL
        GROUP BY ticker HAVING COUNT(DISTINCT date) = 3
    ''', dates)
    verified = {r[0] for r in cursor.fetchall()}

    conn.close()

    status = {t: '✅' if t in verified else '🆕' for t in today_tickers}
    v_count = sum(1 for v in status.values() if v == '✅')
    n_count = sum(1 for v in status.values() if v == '🆕')
    log(f"3일 교집합: ✅ {v_count}개, 🆕 {n_count}개")
    return status


def get_death_list(today_str, today_tickers, results_df):
    """어제 Part 2에 있었지만 오늘 빠진 종목 + 사유"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 어제 날짜 (오늘 이전 가장 최근)
    cursor.execute(
        'SELECT MAX(date) FROM ntm_screening WHERE date < ?', (today_str,)
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        conn.close()
        return []

    yesterday = row[0]

    # 어제 Part 2 멤버
    cursor.execute(
        'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
        (yesterday,)
    )
    yesterday_members = {r[0] for r in cursor.fetchall()}
    conn.close()

    today_set = set(today_tickers)
    dropped_tickers = yesterday_members - today_set

    if not dropped_tickers:
        return []

    # 사유 판별 (오늘 데이터에서)
    death_list = []
    for ticker in sorted(dropped_tickers):
        row_data = results_df[results_df['ticker'] == ticker]
        if row_data.empty:
            death_list.append((ticker, '데이터없음'))
            continue

        r = row_data.iloc[0]
        reasons = []
        price = r.get('price')
        ma60 = r.get('ma60')
        if price is not None and ma60 is not None and price <= ma60:
            reasons.append('MA60↓')
        adj_gap = r.get('adj_gap')
        if adj_gap is not None and adj_gap > 0:
            reasons.append('괴리+')
        adj_score = r.get('adj_score', 0) or 0
        if adj_score <= 9:
            reasons.append('점수↓')
        eps_chg = r.get('eps_change_90d', 0) or 0
        if eps_chg <= 0:
            reasons.append('EPS↓')

        death_list.append((ticker, ','.join(reasons) if reasons else '순위밖'))

    log(f"Death List: {len(death_list)}개 탈락")
    return death_list


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
    today = get_today_kst()
    biz_day = get_last_business_day()
    today_str = today.strftime('%m월%d일')
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
    lines.append('💡 <b>읽는 법</b>')
    lines.append('EPS 점수 = 90일간 4구간 상승률의 합')
    lines.append('점수가 높아도 🌧️가 있으면 최근 주의!')
    lines.append('')
    lines.append('추세 = 구간별 EPS 변화 (왼→오)')
    lines.append('90→60일 | 60→30일 | 30→7일 | 7일→오늘')
    lines.append('🔥 폭등(20%↑) ☀️ 강세(5~20%)')
    lines.append('🌤️ 상승(1~5%) ☁️ 보합(±1%)')
    lines.append('🌧️ 하락(1%↓)')
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


def create_part2_message(df, status_map=None, death_list=None, top_n=30):
    """[1/3] 매수 후보 메시지 — adj_gap 순, MA60+3일 검증, Death List 포함"""
    import pandas as pd

    biz_day = get_last_business_day()
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    # 공통 필터 사용
    filtered = get_part2_candidates(df, top_n=top_n)
    count = len(filtered)

    if status_map is None:
        status_map = {}

    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f' [1/3] 💰 매수 후보 {count}개')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('')
    lines.append('EPS 개선이 주가에 덜 반영된 종목이에요.')
    lines.append('MA60 위 + 3일 연속 검증된 종목을 우선 표시합니다.')
    lines.append('')
    lines.append('💡 <b>읽는 법</b>')
    lines.append('✅ = 3일 연속 후보 (검증)')
    lines.append('🆕 = 오늘 새로 진입 (관찰)')
    lines.append('<b>괴리</b> = EPS↑ vs 주가 반영도 (음수=저평가)')
    lines.append('⚠️ = EPS↑인데 주가↓ (펀더멘탈 괴리)')
    lines.append('')

    for idx, (_, row) in enumerate(filtered.iterrows()):
        rank = idx + 1
        ticker = row['ticker']
        name = row.get('short_name', ticker)
        industry = row.get('industry', '')
        lights = row.get('trend_lights', '')
        desc = row.get('trend_desc', '')
        eps_90d = row.get('eps_change_90d')
        price_90d = row.get('price_chg')

        # ✅/🆕 마커
        marker = status_map.get(ticker, '🆕')

        # Line 3: EPS / 주가 / 괴리
        adj_gap = row.get('adj_gap', 0) or 0
        change_str = ''
        if pd.notna(eps_90d) and pd.notna(price_90d):
            change_str = f"EPS {eps_90d:+.1f}% / 주가 {price_90d:+.1f}% · <b>괴리 {adj_gap:+.1f}</b>"

        # Line 4: 의견 ↑N ↓N
        rev_up = row.get('rev_up30', 0) or 0
        rev_down = row.get('rev_down30', 0) or 0
        opinion_str = f"애널리스트 의견 ↑{rev_up} ↓{rev_down}"

        # ⚠️ 판별: EPS > 0이고 주가 < 0일 때, |주가변화| / |EPS변화| > 5
        eps_chg_w = row.get('eps_chg_weighted')
        price_chg_w = row.get('price_chg_weighted')
        is_warning = False
        if (pd.notna(eps_chg_w) and pd.notna(price_chg_w)
                and eps_chg_w > 0 and price_chg_w < 0):
            ratio = abs(price_chg_w) / abs(eps_chg_w)
            if ratio > 5:
                is_warning = True

        warn_mark = ' ⚠️' if is_warning else ''
        lines.append(f'<b>{rank}위</b> {marker} {name} ({ticker}){warn_mark}')
        lines.append(f'<i>{industry}</i> · {lights} {desc}')
        lines.append(change_str)
        lines.append(opinion_str)
        lines.append('──────────────────')

    # Death List (탈락 종목)
    if death_list:
        lines.append('')
        lines.append('🚨 <b>탈락 종목</b>')
        death_strs = [f'{t} ({reason})' for t, reason in death_list]
        lines.append(' · '.join(death_strs))

    lines.append('')
    lines.append('👉 다음: AI가 위험 신호를 점검해요 [2/3]')

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

    main_cnt = stats.get('main_count', 0)
    turn_cnt = stats.get('turnaround_count', 0)
    err = stats.get('error_count', 0)

    lines = [f'🔧 <b>시스템 로그</b>']
    lines.append(f'{time_str} KST · {env}\n')

    lines.append(f'수집 {main_cnt + turn_cnt}/{stats.get("universe", 0)} (에러 {err})')
    lines.append(f'├ 메인 {main_cnt}')
    lines.append(f'└ 턴어라운드 {turn_cnt}')

    if err > 0:
        error_tickers = stats.get('error_tickers', [])
        lines.append(f'에러: {", ".join(error_tickers)}')

    lines.append('')
    lines.append(f'Score &gt; 0: {stats.get("score_gt0", 0)} ({stats.get("score_gt0", 0) * 100 // max(main_cnt, 1)}%)')
    lines.append(f'Score &gt; 3: {stats.get("score_gt3", 0)} ({stats.get("score_gt3", 0) * 100 // max(main_cnt, 1)}%)')
    lines.append(f'전구간 양호(🌧️ 없음): {stats.get("aligned_count", 0)}')

    lines.append(f'\n소요: {minutes}분 {seconds}초')

    return '\n'.join(lines)


# ============================================================
# AI 리스크 체크 (Gemini 2.5 Flash + Google Search)
# ============================================================

def run_ai_analysis(config, results_df=None, status_map=None, death_list=None):
    """[2/3] AI 브리핑 — 정량 위험 신호 기반 리스크 해석 (데이터는 코드가, 해석은 AI가)"""
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

        # Part 2 종목 추출 + 위험 신호 수집
        if results_df is None or results_df.empty:
            log("results_df 없음 — AI 분석 스킵", "WARN")
            return None

        filtered = get_part2_candidates(results_df, top_n=30)

        if filtered.empty:
            log("Part 2 종목 없음 — AI 분석 스킵", "WARN")
            return None

        stock_count = len(filtered)
        today_dt = datetime.now()
        today_str = today_dt.strftime('%Y-%m-%d')
        today_date = today_dt.date()
        two_weeks_date = (today_dt + timedelta(days=14)).date()

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

            # 1. 애널리스트 다수 하향 (3건 이상)
            if rev_down >= 3:
                flags.append(f"🔻 의견 하향 {rev_down}건 (상향 {rev_up}건)")

            # 2. 저커버리지 (애널리스트 3명 미만)
            if num_analysts < 3:
                flags.append(f"📉 애널리스트 {num_analysts}명 (저커버리지)")

            # 3. 고평가 (Fwd PE > 100)
            if fwd_pe > 100:
                flags.append(f"💰 Fwd PE {fwd_pe:.1f}배 (고평가)")

            # 5. 어닝 임박
            try:
                stock = yf.Ticker(ticker)
                cal = stock.calendar
                if cal is not None:
                    earn_dates = cal.get('Earnings Date', [])
                    if not isinstance(earn_dates, list):
                        earn_dates = [earn_dates]
                    for ed in earn_dates:
                        if hasattr(ed, 'date'):
                            ed = ed.date()
                        if today_date <= ed <= two_weeks_date:
                            flags.append(f"📅 어닝 {ed.month}/{ed.day}")
                            earnings_tickers.append(f"{name} ({ticker}) {ed.month}/{ed.day}")
                            break
            except Exception:
                pass

            # 종목 라인 구성
            header = f"{name} ({ticker}) · {industry} · {lights} {desc} · 점수 {adj_score:.1f}"
            header += f"\n  EPS {eps_chg:+.1f}% / 주가 {price_chg:+.1f}% · 애널리스트 의견 ↑{rev_up} ↓{rev_down} · Fwd PE {fwd_pe:.1f}"

            if flags:
                header += "\n  " + " | ".join(flags)

            signal_lines.append(header)

        signals_data = '\n\n'.join(signal_lines)
        earnings_info = ' · '.join(earnings_tickers) if earnings_tickers else '해당 없음'

        log(f"위험 신호 수집 완료: {stock_count}종목, 어닝 {len(earnings_tickers)}종목")

        prompt = f"""오늘 날짜: {today_str}

아래는 EPS 모멘텀 시스템의 매수 후보 {stock_count}종목과 각 종목의 정량적 위험 신호야.
이 종목들은 EPS 전망치가 상향 중이라 선정된 거야.
네 역할: 위험 신호를 해석해서 "사면 위험한 종목"을 고객에게 알려주는 거야.

[종목별 데이터 & 위험 신호 — 시스템이 계산한 팩트]
{signals_data}

[위험 신호 설명]
🔻 의견 하향 N건 = 30일간 N명의 애널리스트가 EPS 전망치를 낮춤 (3건 이상만 표시)
📉 저커버리지 = 커버리지 애널리스트 3명 미만 (추정치 신뢰도 낮음)
💰 고평가 = Forward PE 100배 초과
📅 어닝 = 2주 내 실적 발표 예정 (발표 전후 변동성 주의)

[출력 형식]
- 한국어, 친절하고 따뜻한 말투 (~예요/~해요 체)
- 예시: "주가가 크게 빠졌어요", "조심하시는 게 좋겠어요", "아직은 괜찮아 보여요"
- 딱딱한 보고서 말투 금지. 친구에게 설명하듯 자연스럽게.
- 총 1500자 이내.

📰 시장 동향
어제 미국 시장 마감과 금주 주요 이벤트를 Google 검색해서 2~3줄 요약해줘.

⚠️ 매수 주의 종목
위 위험 신호를 종합해서 매수를 재고할 만한 종목을 골라줘.
형식: 종목명(티커)를 굵게(**) 쓰고, 1~2줄로 왜 주의해야 하는지 설명.
종목과 종목 사이에 반드시 [SEP] 한 줄을 넣어서 구분해줘.
위험 신호가 없는 종목은 절대 여기에 넣지 마.
시스템 데이터에 없는 내용을 추측하거나 지어내지 마.

예시:
**ABC Corp(ABC)**
최근 5명의 애널리스트가 EPS 전망치를 낮췄어요. 의견 하향이 많으니 조심하세요.
[SEP]
**XYZ Inc(XYZ)**
커버리지 애널리스트가 2명뿐이라 추정치를 100% 믿기 어려워요.

📅 어닝 주의
{earnings_info}
(위 내용 그대로 표시. 수정/추가 금지. "해당 없음"이면 이 섹션 생략.)

위험 신호가 없는 종목은 언급하지 마."""

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
        if not analysis_text:
            try:
                if hasattr(response, 'candidates') and response.candidates:
                    candidate = response.candidates[0]
                    log(f"Gemini finish_reason: {candidate.finish_reason}", "WARN")
            except Exception:
                pass
            log("Gemini 응답이 비어있음 — 재시도", "WARN")
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[grounding_tool],
                    temperature=0.3,
                ),
            )
            analysis_text = extract_text(response)
            if not analysis_text:
                log("Gemini 재시도도 실패", "WARN")
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
        analysis_html = re.sub(r'\n*\[SEP\]\n*', '\n──────────────────\n', analysis_html)

        # 텔레그램 메시지 포맷팅
        now = datetime.now()
        if HAS_PYTZ:
            kst = pytz.timezone('Asia/Seoul')
            now = datetime.now(kst)

        lines = []
        lines.append('━━━━━━━━━━━━━━━━━━━')
        lines.append('   [2/3] 🤖 AI 브리핑')
        lines.append('━━━━━━━━━━━━━━━━━━━')
        lines.append(f'📅 {now.strftime("%Y년 %m월 %d일")}')
        lines.append('')
        lines.append('매수 후보의 위험 신호를 AI가 점검했어요.')
        lines.append('')
        lines.append(analysis_html)
        lines.append('')
        lines.append('👉 다음: 최종 포트폴리오 [3/3]')

        log("AI 브리핑 완료")
        return '\n'.join(lines)

    except Exception as e:
        log(f"AI 분석 실패: {e}", "ERROR")
        return None


def run_portfolio_recommendation(config, results_df, status_map=None):
    """[3/3] 포트폴리오 추천 — 3일 검증(✅) + 리스크 필터 통과 종목"""
    try:
        import re
        import yfinance as yf

        if results_df is None or results_df.empty:
            return None

        # 공통 필터 사용
        filtered = get_part2_candidates(results_df, top_n=30)

        if filtered.empty:
            return None

        if status_map is None:
            status_map = {}

        # ✅ (3일 검증) 종목만 대상
        verified_tickers = {t for t, s in status_map.items() if s == '✅'}
        if verified_tickers:
            filtered = filtered[filtered['ticker'].isin(verified_tickers)]
        # verified_tickers가 비어있으면 cold start → 전체 대상

        if filtered.empty:
            log("포트폴리오: ✅ 검증 종목 없음", "WARN")
            # 관망 메시지 반환
            now = datetime.now()
            if HAS_PYTZ:
                kst = pytz.timezone('Asia/Seoul')
                now = datetime.now(kst)
            return '\n'.join([
                '━━━━━━━━━━━━━━━━━━━',
                '  [3/3] 💼 추천 포트폴리오',
                '━━━━━━━━━━━━━━━━━━━',
                f'📅 {now.strftime("%Y년 %m월 %d일")}',
                '',
                '3일 연속 검증된 종목 중 리스크 필터를',
                '통과한 종목이 없어요.',
                '',
                '이번 회차는 <b>관망</b>을 권장합니다.',
            ])

        today_dt = datetime.now()
        if HAS_PYTZ:
            kst = pytz.timezone('Asia/Seoul')
            today_dt = datetime.now(kst)
        today_date = today_dt.date()
        two_weeks = (today_dt + timedelta(days=14)).date()

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
            if rev_down >= 3:
                flags.append("하향")
            if num_analysts < 3:
                flags.append("저커버리지")
            if fwd_pe > 100:
                flags.append("고평가")
            try:
                cal = yf.Ticker(t).calendar
                if cal:
                    eds = cal.get('Earnings Date', [])
                    if not isinstance(eds, list):
                        eds = [eds]
                    for ed in eds:
                        if hasattr(ed, 'date'):
                            ed = ed.date()
                        if today_date <= ed <= two_weeks:
                            flags.append("어닝")
                            break
            except Exception:
                pass

            if flags:
                log(f"  ❌ {t}: {','.join(flags)} (gap={row.get('adj_gap',0):+.1f} desc={row.get('trend_desc','')})")
            else:
                safe.append({
                    'ticker': t,
                    'name': row.get('short_name', t),
                    'industry': row.get('industry', ''),
                    'eps_chg': eps_chg, 'price_chg': price_chg,
                    'fwd_pe': fwd_pe,
                    'adj_gap': row.get('adj_gap', 0) or 0,
                    'rev_up': rev_up, 'rev_down': rev_down,
                    'adj_score': row.get('adj_score', 0) or 0,
                    'lights': row.get('trend_lights', ''),
                    'desc': row.get('trend_desc', ''),
                })
                log(f"  ✅ {t}: gap={row.get('adj_gap',0):+.1f} desc={row.get('trend_desc','')} up={rev_up} dn={rev_down}")

        if not safe:
            log("포트폴리오: ✅ 종목 없음", "WARN")
            return None

        # adj_gap순 정렬 (더 음수 = EPS 대비 주가 저평가)
        safe.sort(key=lambda x: x['adj_gap'])
        log("포트폴리오: adj_gap 순위 (EPS 대비 저평가):")
        for i, s in enumerate(safe):
            mark = "→" if i < 5 else " "
            log(f"  {mark} {i+1}. {s['ticker']}: gap={s['adj_gap']:+.1f} adj={s['adj_score']:.1f} {s['desc']}")
        selected = safe[:5]

        if len(selected) < 3:
            log("포트폴리오: 선정 종목 부족", "WARN")
            return None

        # 비중 배분 (adj_gap 절대값 비례 — 더 저평가일수록 높은 비중, 5% 단위)
        gaps = [abs(s['adj_gap']) for s in selected]
        total_score = sum(gaps)
        for i, s in enumerate(selected):
            raw = gaps[i] / total_score * 100
            s['weight'] = round(raw / 5) * 5
        # 합계 100% 보정 (가장 비중 큰 종목에서 조정)
        diff = 100 - sum(s['weight'] for s in selected)
        if diff != 0:
            selected[0]['weight'] += diff

        log(f"포트폴리오: {len(selected)}종목 선정 — " +
            ", ".join(f"{s['ticker']}({s['weight']}%)" for s in selected))

        # Gemini 프롬프트
        stock_lines = []
        for i, s in enumerate(selected):
            stock_lines.append(
                f"{i+1}. {s['name']}({s['ticker']}) · {s['industry']} · "
                f"{s['lights']} {s['desc']} · 점수 {s['adj_score']:.1f}\n"
                f"   비중 {s['weight']}% · EPS {s['eps_chg']:+.1f}% · 주가 {s['price_chg']:+.1f}% · "
                f"괴리 {s['adj_gap']:+.1f}\n"
                f"   애널리스트 의견 ↑{s['rev_up']} ↓{s['rev_down']} · Fwd PE {s['fwd_pe']:.1f}"
            )

        prompt = f"""오늘 날짜: {today_dt.strftime('%Y-%m-%d')}

아래는 EPS 모멘텀 시스템이 자동 선정한 {len(selected)}종목 포트폴리오야.
선정 기준: Part 2 매수 후보 중 위험 신호 없고(✅), EPS 모멘텀(속도+방향) 상위.

[포트폴리오]
{chr(10).join(stock_lines)}

[출력 형식]
- 한국어, 친절하고 따뜻한 말투 (~예요/~해요 체)
- 각 종목을 아래 형식으로 출력:
  **종목명(티커)**
  비중 N%
  1~2줄 선정 이유
- 종목과 종목 사이에 반드시 [SEP] 한 줄을 넣어서 구분해줘.
- 맨 끝: "시스템 데이터 기반 참고용이며, 투자 판단은 본인 책임이에요."
- 500자 이내

각 종목의 비중과 선정 이유를 설명해줘.
시스템 데이터에 없는 내용을 지어내지 마."""

        api_key = config.get('gemini_api_key', '')
        if not api_key:
            log("GEMINI_API_KEY 미설정 — 포트폴리오 선정까지만 완료", "WARN")
            return None

        try:
            from google import genai
            from google.genai import types
        except ImportError:
            log("google-genai 패키지 미설치 — 포트폴리오 스킵", "WARN")
            return None

        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.2),
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
        if not text:
            log("포트폴리오: Gemini 응답 없음", "WARN")
            return None

        # Markdown → HTML
        html = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', html)
        html = re.sub(r'(?<!\w)\*(?!\s)(.+?)(?<!\s)\*(?!\w)', r'<i>\1</i>', html)
        html = re.sub(r'#{1,3}\s*', '', html)
        html = re.sub(r'\n*\[SEP\]\n*', '\n──────────────────\n', html)

        lines = [
            '━━━━━━━━━━━━━━━━━━━',
            '  [3/3] 💼 추천 포트폴리오',
            '━━━━━━━━━━━━━━━━━━━',
            f'📅 {today_dt.strftime("%Y년 %m월 %d일")}',
            '',
            '3일 검증 + 리스크 필터 통과 종목으로',
            f'최종 {len(selected)}종목을 선정했어요.',
            '',
            html,
        ]

        log("포트폴리오 추천 완료")
        return '\n'.join(lines)

    except Exception as e:
        log(f"포트폴리오 추천 실패: {e}", "ERROR")
        return None


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
    """NTM EPS 시스템 v19 메인 실행 — Safety & Trend Fusion"""
    log("=" * 60)
    log("EPS Momentum Daily Runner v19 - Safety & Trend Fusion")
    log("=" * 60)

    start_time = datetime.now()

    # 설정 로드
    config = load_config()
    log(f"설정 로드 완료: {CONFIG_PATH}")

    # 1. NTM 데이터 수집 + DB 적재 (MA60, price 포함)
    log("=" * 60)
    log("NTM EPS 데이터 수집 시작")
    log("=" * 60)
    results_df, turnaround_df, stats = run_ntm_collection(config)

    # 2. Part 2 rank 저장 + 3일 교집합 + Death List
    import pandas as pd

    today_str = datetime.now().strftime('%Y-%m-%d')
    status_map = {}
    death_list = []

    if not results_df.empty:
        save_part2_ranks(results_df, today_str)

        # 오늘 Part 2 후보 티커 목록
        candidates = get_part2_candidates(results_df, top_n=30)
        today_tickers = list(candidates['ticker']) if not candidates.empty else []

        status_map = get_3day_status(today_tickers)
        death_list = get_death_list(today_str, today_tickers, results_df)

    # 3. 메시지 생성
    msg_part2 = create_part2_message(results_df, status_map, death_list) if not results_df.empty else None

    # 실행 시간
    elapsed = (datetime.now() - start_time).total_seconds()
    msg_log = create_system_log_message(stats, elapsed, config)

    # 4. 텔레그램 발송: [1/3] 매수 후보 → [2/3] AI 브리핑 → [3/3] 포트폴리오 → 로그
    if config.get('telegram_enabled', False):
        is_github = config.get('is_github_actions', False)
        private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')
        channel_id = config.get('telegram_channel_id')

        # cold start: 3일 미만 데이터 → 채널 전송 안함 (개인봇만)
        cold_start = is_cold_start()
        send_to_channel = is_github and channel_id and not cold_start
        if cold_start:
            log(f"Cold start — 채널 전송 비활성화 (3일 데이터 축적 전)")

        # [1/3] 매수 후보
        if msg_part2:
            if send_to_channel:
                send_telegram_long(msg_part2, config, chat_id=channel_id)
            send_telegram_long(msg_part2, config, chat_id=private_id)
            log(f"[1/3] 매수 후보 전송 완료 → {'채널+개인봇' if send_to_channel else '개인봇'}")

        # [2/3] AI 브리핑
        msg_ai = run_ai_analysis(config, results_df=results_df, status_map=status_map, death_list=death_list)
        if msg_ai:
            if send_to_channel:
                send_telegram_long(msg_ai, config, chat_id=channel_id)
            send_telegram_long(msg_ai, config, chat_id=private_id)
            log(f"[2/3] AI 브리핑 전송 완료 → {'채널+개인봇' if send_to_channel else '개인봇'}")

        # [3/3] 포트폴리오 추천
        msg_portfolio = run_portfolio_recommendation(config, results_df, status_map)
        if msg_portfolio:
            if send_to_channel:
                send_telegram_long(msg_portfolio, config, chat_id=channel_id)
            send_telegram_long(msg_portfolio, config, chat_id=private_id)
            log(f"[3/3] 포트폴리오 전송 완료 → {'채널+개인봇' if send_to_channel else '개인봇'}")

        # 시스템 로그 → 개인봇에만 (항상)
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
