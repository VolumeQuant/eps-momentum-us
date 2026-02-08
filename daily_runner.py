"""
EPS Momentum Daily Runner v8.0 - NTM EPS 시스템

기능:
1. NTM EPS 전 종목 수집 & DB 적재
2. 텔레그램 메시지 4종 생성 & 발송
   - Part 1: 이익 모멘텀 랭킹 (채널/개인봇)
   - Part 2: 매수 후보 (채널/개인봇)
   - AI 리스크 체크 (개인봇) — Gemini 2.5 Flash + Google Search
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
            PRIMARY KEY (date, ticker)
        )
    ''')

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
            score, seg1, seg2, seg3, seg4, is_turnaround = calculate_ntm_score(ntm)
            eps_change_90d = calculate_eps_change_90d(ntm)
            trend_lights, trend_desc = get_trend_lights(seg1, seg2, seg3, seg4)

            # DB 적재
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

            try:
                if hist_all is not None:
                    hist = hist_all['Close'][ticker].dropna()
                else:
                    h = stock.history(period='6mo')
                    hist = h['Close']

                if len(hist) >= 60:
                    p_now = hist.iloc[-1]
                    hist_dt = hist.index.tz_localize(None) if hist.index.tz else hist.index

                    # 각 시점의 주가 찾기
                    prices = {}
                    for days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                        target = today - timedelta(days=days)
                        idx = (hist_dt - target).map(lambda x: abs(x.days)).argmin()
                        prices[key] = hist.iloc[idx]

                    # 90일 주가변화율 (내부용)
                    price_chg = (p_now - prices['90d']) / prices['90d'] * 100

                    # 가중평균 주가변화율 (Part 2 표시용)
                    price_w = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
                    pw_sum = sum(
                        w * (p_now - prices[k]) / prices[k] * 100
                        for k, w in price_w.items() if prices[k] > 0
                    )
                    pw_total = sum(w for k, w in price_w.items() if prices[k] > 0)
                    price_chg_weighted = pw_sum / pw_total if pw_total > 0 else None

                    # 가중평균 EPS변화율 (Part 2 표시용)
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

            row = {
                'ticker': ticker,
                'short_name': short_name,
                'industry': industry_kr,
                'score': score,
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
                'is_turnaround': is_turnaround,
            }

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

    # 메인 랭킹: Score 순 정렬 + rank 업데이트
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values('score', ascending=False).reset_index(drop=True)
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
        stats['aligned_count'] = int((results_df['trend_lights'].str.count('🟢') == 4).sum())

    log(f"수집 완료: 메인 {len(results)}, 턴어라운드 {len(turnaround)}, "
        f"데이터없음 {len(no_data)}, 에러 {len(errors)}")

    return results_df, turnaround_df, stats


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
    import pandas as pd

    today = get_today_kst()
    biz_day = get_last_business_day()
    today_str = today.strftime('%m월%d일')
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    lines = []
    lines.append(f'안녕하세요! 오늘({today_str}) EPS 모멘텀 리포트예요 📊')
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'      📈 EPS 모멘텀 Top {top_n}')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('')
    lines.append('월가 애널리스트들의')
    lines.append('EPS 전망치(향후 12개월 주당순이익 예상)를')
    lines.append('가장 많이 올린 기업 순위예요.')
    lines.append('EPS 전망치 상향은 실적 서프라이즈와')
    lines.append('주가 상승의 강력한 선행 신호예요.')
    lines.append('')
    lines.append('💡 <b>읽는 법</b>')
    lines.append('EPS 점수 = 90일간 4구간 상승률의 합')
    lines.append('점수가 높아도 🔴이 있으면 최근 주의!')
    lines.append('')
    lines.append('신호등 = 구간별 EPS 변화 (왼→오)')
    lines.append('90→60일 | 60→30일 | 30→7일 | 7일→오늘')
    lines.append('🟩 폭발(20%↑) 🟢 상승(2~20%)')
    lines.append('🔵 양호(0.5~2%) 🟡 보합(0~0.5%)')
    lines.append('🔴 하락(0~-10%) 🟥 급락(-10%↓)')
    lines.append('네모(🟩🟥) = 변동폭 큰 구간')
    lines.append('')

    for _, row in df.head(top_n).iterrows():
        rank = int(row['rank'])
        ticker = row['ticker']
        name = row.get('short_name', ticker)
        industry = row.get('industry', '')
        score = row.get('score', 0)
        lights = row.get('trend_lights', '')
        desc = row.get('trend_desc', '')

        lines.append(f'<b>{rank}위</b> {name} ({ticker})')
        lines.append(f'<i>{industry}</i> · {lights} {desc} · <b>{score:.1f}</b>점')
        lines.append('──────────────────')

    return '\n'.join(lines)


def create_part2_message(df, top_n=30):
    """Part 2: 매수 후보 메시지 생성 (괴리율 순, Score > 10 필터)"""
    import pandas as pd

    today = get_today_kst()
    biz_day = get_last_business_day()
    today_str = today.strftime('%m월%d일')
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    # Score > 10 필터 (상위 10% EPS 모멘텀만 매수 후보로)
    filtered = df[df['score'] > 10].copy()

    # 괴리율(fwd_pe_chg) 있는 것만 + Fwd PE > 0 + EPS 변화 양수
    filtered = filtered[
        filtered['fwd_pe_chg'].notna() &
        filtered['fwd_pe'].notna() &
        (filtered['fwd_pe'] > 0) &
        (filtered['eps_change_90d'] > 0)
    ].copy()

    # 괴리율 오름차순 (더 마이너스 = 더 좋은 매수 기회)
    filtered = filtered.sort_values('fwd_pe_chg').head(top_n)

    count = min(top_n, len(filtered))

    lines = []
    lines.append(f'오늘({today_str})의 핵심 리포트예요 💰')
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'      💰 매수 후보 Top {count}')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('')
    lines.append('EPS 전망치는 좋아졌는데')
    lines.append('주가가 아직 못 따라간 종목이에요.')
    lines.append('')
    lines.append('💡 <b>읽는 법</b>')
    lines.append('EPS = 90일간 EPS 전망치 변화율')
    lines.append('주가 = 같은 기간 주가 변화율')
    lines.append('EPS는 올랐는데 주가가 덜 오른 순서예요.')
    lines.append('⚠️ = 주가 하락이 EPS 대비 과도 → 뉴스 확인!')
    lines.append('')
    lines.append('신호등 = 구간별 EPS 변화 (왼→오)')
    lines.append('90→60일 | 60→30일 | 30→7일 | 7일→오늘')
    lines.append('🟩 폭발(20%↑) 🟢 상승(2~20%)')
    lines.append('🔵 양호(0.5~2%) 🟡 보합(0~0.5%)')
    lines.append('🔴 하락(0~-10%) 🟥 급락(-10%↓)')
    lines.append('네모(🟩🟥) = 변동폭 큰 구간')
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
        change_str = ''
        if pd.notna(eps_90d) and pd.notna(price_90d):
            change_str = f"EPS {eps_90d:+.1f}% / 주가 {price_90d:+.1f}%"

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
        lines.append(f'<b>{rank}위</b> {name} ({ticker}){warn_mark}')
        lines.append(f'<i>{industry}</i> · {lights} {desc}')
        lines.append(change_str)
        lines.append('──────────────────')

    lines.append('주가 하락에는 항상 이유가 있을 수 있으니')
    lines.append('뉴스와 실적 발표 일정을 꼭 확인하세요.')

    return '\n'.join(lines)


def create_turnaround_message(df, top_n=None):
    """턴어라운드 주목 메시지 생성 (|EPS| < $1.00, Score > 3 필터)"""
    import pandas as pd

    if df is None or df.empty:
        return None

    # Score > 3 필터 (EPS가 실제로 개선 중인 종목만)
    filtered = df[df['score'] > 3].copy()
    if filtered.empty:
        return None

    biz_day = get_last_business_day()
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    count = len(filtered)
    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'      ⚡ 턴어라운드 주목 ({count}종목)')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('')
    lines.append('적자가 빠르게 줄거나, 흑자 전환 가능성이')
    lines.append('보이는 기업이에요. 턴어라운드에 성공하면')
    lines.append('큰 수익이 가능하지만, 리스크도 높아요.')
    lines.append('')
    lines.append('💡 <b>읽는 법</b>')
    lines.append('EPS 옆 숫자 = 90일 전 → 현재 EPS 전망치')
    lines.append('예: $-0.50 → $0.20이면')
    lines.append('적자에서 흑자 전환이 예상되는 신호예요.')
    lines.append('마이너스(-)가 플러스(+)로 바뀌면 주목!')
    lines.append('')

    for idx, (_, row) in enumerate(filtered.iterrows()):
        rank = idx + 1
        ticker = row['ticker']
        name = row.get('short_name', ticker)
        industry = row.get('industry', '')
        lights = row.get('trend_lights', '')
        desc = row.get('trend_desc', '')
        ntm_90d = row.get('ntm_90d', 0)
        ntm_cur = row.get('ntm_cur', 0)

        lines.append(f'<b>{rank}위</b> {name} ({ticker})')
        lines.append(f'<i>{industry}</i> · EPS ${ntm_90d:.2f} → ${ntm_cur:.2f}')
        lines.append(f'{lights} {desc}')
        lines.append('')

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
    lines.append(f'강세 지속(전구간 상승): {stats.get("aligned_count", 0)}')

    lines.append(f'\n소요: {minutes}분 {seconds}초')

    return '\n'.join(lines)


# ============================================================
# AI 리스크 체크 (Gemini 2.5 Flash + Google Search)
# ============================================================

def run_ai_analysis(msg_part1, msg_part2, msg_turnaround, config):
    """Gemini 2.5 Flash 뉴스 스캐너 - 매수 후보 리스크 체크 (Google Search Grounding)"""
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

        # 3개 메시지 합치기 (HTML 태그 제거하여 토큰 절약)
        import re
        def strip_html(text):
            return re.sub(r'<[^>]+>', '', text or '')

        prompt = f"""너는 뉴스 스캐너야.
자동 스크리닝 시스템이 뽑은 매수 후보 30종목에 대해
최근 뉴스와 이벤트를 검색해서 알아야 할 사실만 전달해줘.
분석이나 판단은 하지 마. 팩트만 전달해.

[매수 후보 Top 30]
{strip_html(msg_part2)}

[네가 할 일]
30종목을 웹 검색해서 아래 항목만 찾아:
1. 최근 1~2주 내 중요 뉴스/이벤트 (악재 또는 호재)
2. 2주 내 실적발표(earnings) 예정 여부
3. 특이사항 없으면 보고하지 마

⚠️ 절대 금지:
- 데이터의 EPS%/주가% 수치 인용 금지 (시스템 내부 가중평균임)
- 주관적 판단/추천 금지 ("매수 유효", "괜찮아 보여요" 등)
- 일반론 금지 ("실적이 좋습니다", "성장세입니다" 등)

[출력 형식] 한국어, 친절한 말투(~예요/~해요)

📰 시장 한줄평
(Top 30 섹터 구성에서 읽히는 테마 1줄)

⚠️ 주의 종목
TICKER (업종)
→ 구체적 뉴스/이벤트 1-2줄

📅 어닝 임박
TICKER - M/DD 실적발표

✅ 나머지: 특이사항 없음

※ 뉴스가 없는 종목은 절대 언급하지 마.
※ 주의 종목이 없으면 "주의 종목 없음"으로.
총 1500자 이내."""

        grounding_tool = types.Tool(google_search=types.GoogleSearch())
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[grounding_tool],
                temperature=0.7,
            ),
        )

        analysis_text = response.text
        if not analysis_text:
            log("Gemini 응답이 비어있음", "WARN")
            return None

        # Markdown → Telegram HTML 변환
        analysis_html = analysis_text
        # 1. HTML 특수문자 이스케이프 (Telegram HTML 파서 호환)
        analysis_html = analysis_html.replace('&', '&amp;')
        analysis_html = analysis_html.replace('<', '&lt;')
        analysis_html = analysis_html.replace('>', '&gt;')
        # 2. Markdown → HTML 태그 변환
        analysis_html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', analysis_html)  # **bold**
        analysis_html = re.sub(r'(?<!\w)\*(?!\s)(.+?)(?<!\s)\*(?!\w)', r'<i>\1</i>', analysis_html)  # *italic* (리스트 항목 제외)
        analysis_html = re.sub(r'#{1,3}\s*', '', analysis_html)                # ### headings
        analysis_html = analysis_html.replace('---', '━━━')                    # hr

        # 텔레그램 메시지 포맷팅
        now = datetime.now()
        if HAS_PYTZ:
            kst = pytz.timezone('Asia/Seoul')
            now = datetime.now(kst)

        lines = []
        lines.append('━━━━━━━━━━━━━━━━━━━')
        lines.append('      🤖 AI 리스크 체크')
        lines.append('━━━━━━━━━━━━━━━━━━━')
        lines.append(f'📅 {now.strftime("%Y년 %m월 %d일")}')
        lines.append('')
        lines.append('매수 후보 30종목의 최근 뉴스/이벤트를')
        lines.append('AI가 검색한 결과예요. 참고용이에요!')
        lines.append('')
        lines.append(analysis_html)

        log("AI 종합 분석 완료")
        return '\n'.join(lines)

    except Exception as e:
        log(f"AI 분석 실패: {e}", "ERROR")
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
        remaining = message
        while remaining:
            if len(remaining) <= 4000:
                chunks.append(remaining)
                break
            else:
                split_point = remaining[:4000].rfind('\n')
                if split_point == -1:
                    split_point = 4000
                chunks.append(remaining[:split_point])
                remaining = remaining[split_point:].lstrip('\n')

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
    """NTM EPS 시스템 메인 실행"""
    log("=" * 60)
    log("EPS Momentum Daily Runner v8.0 - NTM EPS 시스템")
    log("=" * 60)

    start_time = datetime.now()

    # 설정 로드
    config = load_config()
    log(f"설정 로드 완료: {CONFIG_PATH}")

    # 1. NTM 데이터 수집 + DB 적재
    log("=" * 60)
    log("NTM EPS 데이터 수집 시작")
    log("=" * 60)
    results_df, turnaround_df, stats = run_ntm_collection(config)

    # 2. 텔레그램 메시지 생성
    import pandas as pd

    msg_part1 = create_part1_message(results_df) if not results_df.empty else None
    msg_part2 = create_part2_message(results_df) if not results_df.empty else None

    # 실행 시간
    elapsed = (datetime.now() - start_time).total_seconds()
    msg_log = create_system_log_message(stats, elapsed, config)

    # 3. 텔레그램 발송
    if config.get('telegram_enabled', False):
        is_github = config.get('is_github_actions', False)
        private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')
        channel_id = config.get('telegram_channel_id')

        # 발송 순서: Part 1 → Part 2 → AI 리스크 체크 → 시스템 로그

        # Part 1 (모멘텀 랭킹)
        if msg_part1:
            target = channel_id if (is_github and channel_id) else private_id
            send_telegram_long(msg_part1, config, chat_id=target)
            log(f"Part 1 (모멘텀 랭킹) 전송 완료 → {'채널' if target == channel_id else '개인봇'}")

        # Part 2 (매수 후보) — 핵심 리포트
        if msg_part2:
            target = channel_id if (is_github and channel_id) else private_id
            send_telegram_long(msg_part2, config, chat_id=target)
            log(f"Part 2 (매수 후보) 전송 완료 → {'채널' if target == channel_id else '개인봇'}")

        # AI 리스크 체크 → 개인봇에만
        msg_ai = run_ai_analysis(msg_part1, msg_part2, None, config)
        if msg_ai:
            send_telegram_long(msg_ai, config, chat_id=private_id)
            log("AI 종합 분석 전송 완료 → 개인봇")

        # 시스템 로그 → 개인봇에만 (항상)
        send_telegram_long(msg_log, config, chat_id=private_id)
        log("시스템 로그 전송 완료 → 개인봇")

    # 4. Git commit/push
    git_commit_push(config)

    # 완료
    log("=" * 60)
    log(f"전체 완료: {elapsed:.1f}초 소요")
    log("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
