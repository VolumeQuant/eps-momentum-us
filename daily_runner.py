"""
EPS Momentum Daily Runner v8.0 - NTM EPS 시스템

기능:
1. NTM EPS 전 종목 수집 & DB 적재
2. 텔레그램 메시지 4종 생성 & 발송
   - Part 1: 이익 모멘텀 랭킹 (개인봇)
   - Part 2: 매수 후보 (채널/개인봇)
   - 턴어라운드 (채널/개인봇)
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
    "indices": ["NASDAQ_100", "SP500", "SP400_MidCap"],
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
        get_trend_arrows,
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
            trend = get_trend_arrows(seg1, seg2, seg3, seg4)

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

                    # 90일 주가변화율
                    price_chg = (p_now - prices['90d']) / prices['90d'] * 100

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
            except Exception:
                pass

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
                'trend': trend,
                'price_chg': price_chg,
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

    # 메인 랭킹: 90일 이익변화율 순 정렬 + rank 업데이트
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values('eps_change_90d', ascending=False).reset_index(drop=True)
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
        stats['aligned_count'] = int((results_df['trend'] == '↑↑↑↑').sum())

    log(f"수집 완료: 메인 {len(results)}, 턴어라운드 {len(turnaround)}, "
        f"데이터없음 {len(no_data)}, 에러 {len(errors)}")

    return results_df, turnaround_df, stats


# ============================================================
# Git 자동 커밋
# ============================================================

def git_commit_push(config):
    """Git 자동 commit/push"""
    if not config.get('git_enabled', False):
        log("Git 동기화 비활성화됨")
        return False

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
        branch = config.get('git_branch', 'main')
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
    """Part 1: 이익 모멘텀 랭킹 메시지 생성 (90일 이익변화율 순)"""
    import pandas as pd

    today = get_today_kst()
    biz_day = get_last_business_day()
    today_str = today.strftime('%m월%d일')
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    lines = []
    lines.append(f'안녕하세요! 오늘({today_str}) 미국주식 EPS 모멘텀 리포트입니다 📊')
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append('      📈 이익 모멘텀 Top 30')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('90일간 이익 추정치 변화율 순')
    lines.append('')

    medals = {1: '🥇', 2: '🥈', 3: '🥉'}

    for _, row in df.head(top_n).iterrows():
        rank = int(row['rank'])
        ticker = row['ticker']
        name = row.get('short_name', ticker)
        industry = row.get('industry', '')
        eps_chg = row.get('eps_change_90d')
        eps_str = f"{eps_chg:+.1f}%" if pd.notna(eps_chg) else '-'
        trend = row.get('trend', '')

        icon = medals.get(rank, '📌')
        lines.append(f'{icon} <b>{rank}위</b> {name} ({ticker}) <i>{industry}</i>')
        lines.append(f'    이익변화 <b>{eps_str}</b> · 추세(90d/60d/30d/7d) {trend}')

        if rank == 3:
            lines.append('')

    lines.append('')
    lines.append('<i>추세 ↑↓ = 각 구간별 이익 추정치 변화 방향</i>')

    return '\n'.join(lines)


def create_part2_message(df, top_n=30):
    """Part 2: 매수 후보 메시지 생성 (괴리율 순, Score > 3 필터)"""
    import pandas as pd

    today = get_today_kst()
    biz_day = get_last_business_day()
    today_str = today.strftime('%m월%d일')
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    # Score > 3 필터
    filtered = df[df['score'] > 3].copy()

    # 괴리율(fwd_pe_chg) 있는 것만 + Fwd PE > 0
    filtered = filtered[
        filtered['fwd_pe_chg'].notna() &
        filtered['fwd_pe'].notna() &
        (filtered['fwd_pe'] > 0)
    ].copy()

    # 괴리율 오름차순 (더 마이너스 = 더 좋은 매수 기회)
    filtered = filtered.sort_values('fwd_pe_chg').head(top_n)

    count = min(top_n, len(filtered))

    lines = []
    lines.append(f'안녕하세요! 오늘({today_str}) 미국주식 매수 후보 리포트입니다 💰')
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'      💰 매수 후보 Top {count}')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('이익은 올랐는데 주가가 덜 따라간 종목')
    lines.append('')

    medals = {1: '🥇', 2: '🥈', 3: '🥉'}

    for idx, (_, row) in enumerate(filtered.iterrows()):
        rank = idx + 1
        ticker = row['ticker']
        name = row.get('short_name', ticker)
        industry = row.get('industry', '')
        trend = row.get('trend', '')
        eps_chg = row.get('eps_change_90d')
        eps_str = f"{eps_chg:+.1f}%" if pd.notna(eps_chg) else '-'
        price_chg = row.get('price_chg')
        price_str = f"{price_chg:+.1f}%" if pd.notna(price_chg) else '-'
        pe_chg = row.get('fwd_pe_chg')
        pe_str = f"{pe_chg:+.1f}%" if pd.notna(pe_chg) else '-'

        icon = medals.get(rank, '📌')
        lines.append(f'{icon} <b>{rank}위</b> {name} ({ticker}) <i>{industry}</i>')
        lines.append(f'    이익변화 {eps_str} · 주가변화 {price_str} · 괴리율 <b>{pe_str}</b>')
        lines.append(f'    추세(90d/60d/30d/7d) {trend}')

        if rank == 3:
            lines.append('')

    lines.append('')
    lines.append('<i>괴리율 = 7d~90d Fwd PE 변화율 가중평균 (최근↑), 마이너스일수록 저평가</i>')

    return '\n'.join(lines)


def create_turnaround_message(df, top_n=10):
    """턴어라운드 주목 메시지 생성 (|EPS| < $1.00 구간)"""
    import pandas as pd

    if df is None or df.empty:
        return None

    biz_day = get_last_business_day()
    biz_str = biz_day.strftime('%Y년 %m월 %d일')

    lines = []
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append('      ⚡ 턴어라운드 주목')
    lines.append('━━━━━━━━━━━━━━━━━━━')
    lines.append(f'📅 {biz_str} (미국장 기준)')
    lines.append('적자축소·흑자전환 신호 종목 (|EPS| &lt; $1)')
    lines.append('')

    medals = {1: '🥇', 2: '🥈', 3: '🥉'}

    for idx, (_, row) in enumerate(df.head(top_n).iterrows()):
        rank = idx + 1
        ticker = row['ticker']
        name = row.get('short_name', ticker)
        industry = row.get('industry', '')
        trend = row.get('trend', '')
        ntm_90d = row.get('ntm_90d', 0)
        ntm_cur = row.get('ntm_cur', 0)

        icon = medals.get(rank, '📌')
        lines.append(f'{icon} <b>{rank}위</b> {name} ({ticker}) <i>{industry}</i>')
        lines.append(f'    EPS ${ntm_90d:.2f} → ${ntm_cur:.2f} · 추세(90d/60d/30d/7d) {trend}')

        if rank == 3:
            lines.append('')

    lines.append('')
    lines.append('<i>90일 전 EPS → 현재 EPS 추이</i>')

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
    lines.append(f'정배열 ↑↑↑↑: {stats.get("aligned_count", 0)}')

    lines.append(f'\n소요: {minutes}분 {seconds}초')

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
    msg_turnaround = create_turnaround_message(turnaround_df) if not turnaround_df.empty else None

    # 실행 시간
    elapsed = (datetime.now() - start_time).total_seconds()
    msg_log = create_system_log_message(stats, elapsed, config)

    # 3. 텔레그램 발송
    if config.get('telegram_enabled', False):
        is_github = config.get('is_github_actions', False)
        private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')
        channel_id = config.get('telegram_channel_id')

        # Part 1 (모멘텀 랭킹) → 개인봇에만 (항상)
        if msg_part1:
            send_telegram_long(msg_part1, config, chat_id=private_id)
            log("Part 1 (모멘텀 랭킹) 전송 완료 → 개인봇")

        # Part 2 (매수 후보) → GitHub Actions: 채널 / 로컬: 개인봇
        if msg_part2:
            target = channel_id if (is_github and channel_id) else private_id
            send_telegram_long(msg_part2, config, chat_id=target)
            log(f"Part 2 (매수 후보) 전송 완료 → {'채널' if target == channel_id else '개인봇'}")

        # 턴어라운드 → GitHub Actions: 채널 / 로컬: 개인봇
        if msg_turnaround:
            target = channel_id if (is_github and channel_id) else private_id
            send_telegram_long(msg_turnaround, config, chat_id=target)
            log(f"턴어라운드 전송 완료 → {'채널' if target == channel_id else '개인봇'}")

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
