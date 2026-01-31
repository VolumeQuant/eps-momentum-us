"""
EPS Momentum Daily Runner - 자동화 시스템
매일 지정된 시간에 실행되어 Track 1, Track 2 수행 후 결과 저장

기능:
1. Track 1: 실시간 스크리닝 → 매수 후보 선정
2. Track 2: 전 종목 데이터 축적 → 백테스팅용
3. 일일 리포트 생성 (HTML + Markdown)
4. Git 자동 commit/push (선택)
5. 텔레그램 알림 (선택)

실행: python daily_runner.py
"""

import os
import sys
import json
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 프로젝트 루트
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'eps_data'
REPORTS_DIR = PROJECT_ROOT / 'reports'
DB_PATH = PROJECT_ROOT / 'eps_momentum_data.db'
CONFIG_PATH = PROJECT_ROOT / 'config.json'

# 디렉토리 생성
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# 기본 설정
DEFAULT_CONFIG = {
    "python_path": r"C:\Users\jkw88\miniconda3\envs\volumequant\python.exe",
    "git_enabled": True,
    "git_remote": "origin",
    "git_branch": "main",
    "telegram_enabled": False,
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "run_time": "07:00",  # 미국 장 마감 후 (한국 시간 07:00)
    "indices": ["NASDAQ_100", "SP500", "SP400_MidCap"],
    "min_score": 4.0,
    "kill_switch_threshold": -0.005,  # -0.5% (Hysteresis)
    "earnings_blackout_days": 5,  # 실적 발표 D-5 ~ D+1 진입 금지
}


def load_config():
    """설정 로드 (없으면 기본값 생성)"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
            # 기본값 병합
            for key, value in DEFAULT_CONFIG.items():
                if key not in config:
                    config[key] = value
            return config
    else:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        print(f"[INFO] 기본 설정 파일 생성: {CONFIG_PATH}")
        return DEFAULT_CONFIG


def log(message, level="INFO"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


# ============================================================
# Track 1 & 2 실행
# ============================================================

def run_screening(config):
    """Track 1: 실시간 스크리닝"""
    log("Track 1: 실시간 스크리닝 시작")

    try:
        import yfinance as yf
        import pandas as pd
        import numpy as np

        from eps_momentum_system import (
            INDICES, SECTOR_MAP,
            calculate_momentum_score_v2, calculate_slope_score,
            check_technical_filter, get_peg_ratio
        )

        today = datetime.now().strftime('%Y-%m-%d')
        min_score = config.get('min_score', 4.0)
        kill_threshold = config.get('kill_switch_threshold', -0.005)
        earnings_blackout = config.get('earnings_blackout_days', 5)

        # 종목 수집
        all_tickers = {}
        for idx_name in config.get('indices', ['NASDAQ_100', 'SP500', 'SP400_MidCap']):
            if idx_name in INDICES:
                for ticker in INDICES[idx_name]:
                    if ticker not in all_tickers:
                        all_tickers[ticker] = idx_name

        candidates = []
        stats = {
            'total': len(all_tickers),
            'no_eps': 0,
            'killed': 0,
            'low_volume': 0,
            'below_ma': 0,
            'earnings_blackout': 0,
            'data_error': 0,
            'passed': 0
        }

        for ticker, idx_name in all_tickers.items():
            try:
                stock = yf.Ticker(ticker)
                trend = stock.eps_trend
                info = stock.info

                if trend is None or '+1y' not in trend.index:
                    stats['no_eps'] += 1
                    continue

                eps_row = trend.loc['+1y']
                current = eps_row.get('current')
                d7 = eps_row.get('7daysAgo')
                d30 = eps_row.get('30daysAgo')
                d60 = eps_row.get('60daysAgo')

                # Kill Switch with Hysteresis
                if pd.notna(current) and pd.notna(d7) and d7 != 0:
                    chg_7d = (current - d7) / abs(d7)
                    if chg_7d < kill_threshold:
                        stats['killed'] += 1
                        continue

                # 스코어 계산
                score_321, eps_chg, passed = calculate_momentum_score_v2(current, d7, d30, d60)
                score_slope = calculate_slope_score(current, d7, d30, d60)

                if not passed or score_321 is None or score_321 < min_score:
                    continue

                # 가격/거래량
                hist = stock.history(period='1mo')
                if len(hist) < 5:
                    stats['data_error'] += 1
                    continue

                price = hist['Close'].iloc[-1]
                avg_volume = hist['Volume'].mean()
                dollar_volume = price * avg_volume

                if dollar_volume < 20_000_000:  # $20M
                    stats['low_volume'] += 1
                    continue

                # MA20 필터
                above_ma, current_price, ma_20 = check_technical_filter(hist)
                if not above_ma:
                    stats['below_ma'] += 1
                    continue

                # 실적 발표일 필터 (Earnings Blackout)
                try:
                    calendar = stock.calendar
                    if calendar is not None and 'Earnings Date' in calendar:
                        earnings_date = calendar['Earnings Date']
                        if isinstance(earnings_date, (list, tuple)):
                            earnings_date = earnings_date[0]
                        if earnings_date:
                            days_to_earnings = (earnings_date.date() - datetime.now().date()).days
                            if -1 <= days_to_earnings <= earnings_blackout:
                                stats['earnings_blackout'] += 1
                                continue
                except:
                    pass  # 실적 발표일 조회 실패 시 무시

                sector = SECTOR_MAP.get(ticker, info.get('sector', 'Other'))
                peg = get_peg_ratio(info)

                candidates.append({
                    'ticker': ticker,
                    'index': idx_name,
                    'score_321': score_321,
                    'score_slope': score_slope,
                    'eps_chg_60d': eps_chg,
                    'peg': peg,
                    'price': round(price, 2),
                    'ma_20': round(ma_20, 2),
                    'dollar_vol_M': round(dollar_volume / 1_000_000, 1),
                    'sector': sector,
                    'current': current,
                    '7d': d7,
                    '30d': d30,
                    '60d': d60,
                })
                stats['passed'] += 1

            except Exception as e:
                stats['data_error'] += 1
                continue

        # 결과 저장
        df = pd.DataFrame(candidates)
        if not df.empty:
            df = df.sort_values('score_321', ascending=False)
            csv_path = DATA_DIR / f'screening_{today}.csv'
            df.to_csv(csv_path, index=False)
            log(f"Track 1 완료: {len(df)}개 종목 → {csv_path}")
        else:
            log("Track 1: 조건 충족 종목 없음", "WARN")

        return df, stats

    except Exception as e:
        log(f"Track 1 실패: {e}", "ERROR")
        return pd.DataFrame(), {}


def run_data_collection(config):
    """Track 2: 전 종목 데이터 축적"""
    log("Track 2: 데이터 축적 시작")

    try:
        import yfinance as yf
        import pandas as pd

        from eps_momentum_system import (
            INDICES, SECTOR_MAP,
            calculate_momentum_score_v2, calculate_slope_score
        )

        today = datetime.now().strftime('%Y-%m-%d')

        # DB 연결
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 테이블 생성 (없으면)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS eps_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                index_name TEXT,
                period TEXT NOT NULL,
                eps_current REAL,
                eps_7d REAL,
                eps_30d REAL,
                eps_60d REAL,
                eps_90d REAL,
                price REAL,
                volume REAL,
                dollar_volume REAL,
                market_cap REAL,
                sector TEXT,
                ma_20 REAL,
                above_ma20 INTEGER,
                score_321 REAL,
                score_slope REAL,
                eps_chg_60d REAL,
                passed_screen INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, ticker, period)
            )
        ''')
        conn.commit()

        # 종목 수집
        all_tickers = {}
        for idx_name in config.get('indices', ['NASDAQ_100', 'SP500', 'SP400_MidCap']):
            if idx_name in INDICES:
                for ticker in INDICES[idx_name]:
                    if ticker not in all_tickers:
                        all_tickers[ticker] = idx_name

        collected = 0
        errors = 0

        for ticker, idx_name in all_tickers.items():
            try:
                stock = yf.Ticker(ticker)
                trend = stock.eps_trend
                info = stock.info

                # 가격/거래량
                hist = stock.history(period='1mo')
                if len(hist) < 5:
                    errors += 1
                    continue

                price = hist['Close'].iloc[-1]
                avg_volume = hist['Volume'].mean()
                dollar_volume = price * avg_volume
                ma_20 = hist['Close'].tail(20).mean() if len(hist) >= 20 else hist['Close'].mean()
                above_ma20 = 1 if price > ma_20 else 0
                market_cap = info.get('marketCap', 0)
                sector = SECTOR_MAP.get(ticker, info.get('sector', 'Other'))

                # EPS 데이터
                eps_current = eps_7d = eps_30d = eps_60d = eps_90d = None
                score_321 = score_slope = eps_chg_60d = None
                passed_screen = 0

                if trend is not None and '+1y' in trend.index:
                    eps_row = trend.loc['+1y']
                    eps_current = eps_row.get('current')
                    eps_7d = eps_row.get('7daysAgo')
                    eps_30d = eps_row.get('30daysAgo')
                    eps_60d = eps_row.get('60daysAgo')
                    eps_90d = eps_row.get('90daysAgo')

                    # 스코어 계산
                    score_321, eps_chg_60d, passed = calculate_momentum_score_v2(
                        eps_current, eps_7d, eps_30d, eps_60d
                    )
                    score_slope = calculate_slope_score(eps_current, eps_7d, eps_30d, eps_60d)

                    if passed and score_321 and score_321 >= 4.0:
                        if dollar_volume >= 20_000_000 and above_ma20:
                            passed_screen = 1

                # DB 저장
                cursor.execute('''
                    INSERT OR REPLACE INTO eps_snapshots
                    (date, ticker, index_name, period, eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                     price, volume, dollar_volume, market_cap, sector, ma_20, above_ma20,
                     score_321, score_slope, eps_chg_60d, passed_screen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (today, ticker, idx_name, '+1y',
                      eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                      price, avg_volume, dollar_volume, market_cap, sector,
                      ma_20, above_ma20, score_321, score_slope, eps_chg_60d, passed_screen))

                collected += 1

                if collected % 50 == 0:
                    conn.commit()
                    log(f"  진행: {collected}/{len(all_tickers)}")

            except Exception as e:
                errors += 1
                continue

        conn.commit()
        conn.close()

        log(f"Track 2 완료: {collected}개 수집, {errors}개 오류")
        return collected, errors

    except Exception as e:
        log(f"Track 2 실패: {e}", "ERROR")
        return 0, 0


# ============================================================
# 리포트 생성
# ============================================================

def get_portfolio_changes(screening_df, config):
    """전일 대비 편입/편출 종목 계산"""
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    today_tickers = set(screening_df['ticker'].tolist()) if not screening_df.empty else set()

    # 전일 데이터 조회
    yesterday_tickers = set()
    yesterday_file = DATA_DIR / f'screening_{yesterday}.csv'

    if yesterday_file.exists():
        try:
            import pandas as pd
            yesterday_df = pd.read_csv(yesterday_file)
            yesterday_tickers = set(yesterday_df['ticker'].tolist())
        except:
            pass

    # DB에서 전일 데이터 조회 (파일 없으면)
    if not yesterday_tickers and DB_PATH.exists():
        try:
            conn = sqlite3.connect(DB_PATH)
            query = f"SELECT DISTINCT ticker FROM eps_snapshots WHERE date = '{yesterday}' AND passed_screen = 1"
            result = pd.read_sql(query, conn)
            yesterday_tickers = set(result['ticker'].tolist())
            conn.close()
        except:
            pass

    # 편입/편출 계산
    added = today_tickers - yesterday_tickers  # 신규 편입
    removed = yesterday_tickers - today_tickers  # 편출
    maintained = today_tickers & yesterday_tickers  # 유지

    return {
        'added': sorted(list(added)),
        'removed': sorted(list(removed)),
        'maintained': sorted(list(maintained)),
        'today_count': len(today_tickers),
        'yesterday_count': len(yesterday_tickers)
    }


def generate_report(screening_df, stats, config):
    """일일 리포트 생성 (HTML + Markdown)"""
    log("리포트 생성 중...")

    # 편입/편출 계산
    changes = get_portfolio_changes(screening_df, config)

    today = datetime.now().strftime('%Y-%m-%d')
    today_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 섹터별 분포
    sector_dist = {}
    if not screening_df.empty:
        sector_dist = screening_df['sector'].value_counts().to_dict()

    # 지수별 분포
    index_dist = {}
    if not screening_df.empty:
        index_dist = screening_df['index'].value_counts().to_dict()

    # Top 20 종목
    top_20 = screening_df.head(20) if not screening_df.empty else pd.DataFrame()

    # ========== Markdown 리포트 ==========
    md_content = f"""# EPS Momentum Daily Report
**Date:** {today_time}

## Summary
| Metric | Value |
|--------|-------|
| Total Scanned | {stats.get('total', 0)} |
| Passed Screening | {stats.get('passed', 0)} |
| Kill Switch | {stats.get('killed', 0)} |
| No EPS Data | {stats.get('no_eps', 0)} |
| Low Volume | {stats.get('low_volume', 0)} |
| Below MA20 | {stats.get('below_ma', 0)} |
| Earnings Blackout | {stats.get('earnings_blackout', 0)} |

## Portfolio Changes (vs Yesterday)
| Type | Count | Tickers |
|------|-------|---------|
| Added (New) | {len(changes['added'])} | {', '.join(changes['added'][:10])}{'...' if len(changes['added']) > 10 else ''} |
| Removed | {len(changes['removed'])} | {', '.join(changes['removed'][:10])}{'...' if len(changes['removed']) > 10 else ''} |
| Maintained | {len(changes['maintained'])} | - |

## Sector Distribution
| Sector | Count |
|--------|-------|
"""
    for sector, count in sector_dist.items():
        md_content += f"| {sector} | {count} |\n"

    md_content += f"""
## Index Distribution
| Index | Count |
|-------|-------|
"""
    for idx, count in index_dist.items():
        md_content += f"| {idx} | {count} |\n"

    md_content += f"""
## Top 20 Candidates
| # | Ticker | Index | Score_321 | Score_Slope | EPS% | Price |
|---|--------|-------|-----------|-------------|------|-------|
"""
    for i, (_, row) in enumerate(top_20.iterrows()):
        md_content += f"| {i+1} | {row['ticker']} | {row['index']} | {row['score_321']:.1f} | {row.get('score_slope', 0):.4f} | {row['eps_chg_60d']:+.1f}% | ${row['price']:.2f} |\n"

    # Markdown 저장
    md_path = REPORTS_DIR / f'report_{today}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    # ========== HTML 리포트 ==========
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>EPS Momentum Report - {today}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 3px solid #007bff; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #007bff; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
        .positive {{ color: #28a745; font-weight: bold; }}
        .negative {{ color: #dc3545; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }}
        .stat-card {{ background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #007bff; }}
        .stat-label {{ color: #666; margin-top: 5px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>EPS Momentum Daily Report</h1>
        <p><strong>Generated:</strong> {today_time}</p>

        <h2>Summary</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{stats.get('total', 0)}</div>
                <div class="stat-label">Total Scanned</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #28a745;">{stats.get('passed', 0)}</div>
                <div class="stat-label">Passed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #dc3545;">{stats.get('killed', 0)}</div>
                <div class="stat-label">Kill Switch</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{stats.get('earnings_blackout', 0)}</div>
                <div class="stat-label">Earnings Blackout</div>
            </div>
        </div>

        <h2>Portfolio Changes (vs Yesterday)</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value" style="color: #28a745;">{len(changes['added'])}</div>
                <div class="stat-label">Added (New)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #dc3545;">{len(changes['removed'])}</div>
                <div class="stat-label">Removed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len(changes['maintained'])}</div>
                <div class="stat-label">Maintained</div>
            </div>
        </div>

        <div style="display: flex; gap: 20px; margin: 20px 0;">
            <div style="flex: 1;">
                <h4 style="color: #28a745;">+ Added</h4>
                <p>{', '.join(changes['added']) if changes['added'] else 'None'}</p>
            </div>
            <div style="flex: 1;">
                <h4 style="color: #dc3545;">- Removed</h4>
                <p>{', '.join(changes['removed']) if changes['removed'] else 'None'}</p>
            </div>
        </div>

        <h2>Top 20 Candidates</h2>
        <table>
            <tr>
                <th>#</th>
                <th>Ticker</th>
                <th>Index</th>
                <th>Score_321</th>
                <th>Score_Slope</th>
                <th>EPS Change</th>
                <th>Price</th>
                <th>Volume ($M)</th>
            </tr>
"""

    for i, (_, row) in enumerate(top_20.iterrows()):
        eps_class = 'positive' if row['eps_chg_60d'] > 0 else 'negative'
        html_content += f"""
            <tr>
                <td>{i+1}</td>
                <td><strong>{row['ticker']}</strong></td>
                <td>{row['index']}</td>
                <td>{row['score_321']:.1f}</td>
                <td>{row.get('score_slope', 0):.4f}</td>
                <td class="{eps_class}">{row['eps_chg_60d']:+.1f}%</td>
                <td>${row['price']:.2f}</td>
                <td>{row['dollar_vol_M']:.1f}M</td>
            </tr>
"""

    html_content += """
        </table>

        <h2>Sector Distribution</h2>
        <table>
            <tr><th>Sector</th><th>Count</th></tr>
"""
    for sector, count in sector_dist.items():
        html_content += f"<tr><td>{sector}</td><td>{count}</td></tr>\n"

    html_content += """
        </table>
    </div>
</body>
</html>
"""

    html_path = REPORTS_DIR / f'report_{today}.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    log(f"리포트 생성 완료: {md_path}, {html_path}")
    return md_path, html_path


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

        # git add
        subprocess.run(['git', 'add', '-A'], cwd=PROJECT_ROOT, check=True, capture_output=True)

        # git commit
        commit_msg = f"Daily update: {today}\n\nAutomated EPS Momentum screening results"
        result = subprocess.run(
            ['git', 'commit', '-m', commit_msg],
            cwd=PROJECT_ROOT, capture_output=True, text=True
        )

        if 'nothing to commit' in result.stdout or 'nothing to commit' in result.stderr:
            log("변경사항 없음, 커밋 스킵")
            return True

        # git push
        remote = config.get('git_remote', 'origin')
        branch = config.get('git_branch', 'main')
        subprocess.run(['git', 'push', remote, branch], cwd=PROJECT_ROOT, check=True, capture_output=True)

        log("Git push 완료")
        return True

    except subprocess.CalledProcessError as e:
        log(f"Git 오류: {e}", "ERROR")
        return False


# ============================================================
# 텔레그램 알림
# ============================================================

def send_telegram(message, config):
    """텔레그램 메시지 전송"""
    if not config.get('telegram_enabled', False):
        return False

    bot_token = config.get('telegram_bot_token', '')
    chat_id = config.get('telegram_chat_id', '')

    if not bot_token or not chat_id:
        log("텔레그램 설정 불완전", "WARN")
        return False

    try:
        import urllib.request
        import urllib.parse

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown'
        }).encode()

        req = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)

        log("텔레그램 전송 완료")
        return True

    except Exception as e:
        log(f"텔레그램 전송 실패: {e}", "ERROR")
        return False


def calculate_rsi(prices, period=14):
    """RSI 계산"""
    import pandas as pd
    if len(prices) < period + 1:
        return None

    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else None


def analyze_technical(ticker):
    """
    기술적 분석 함수 (요구사항 1)

    yfinance로 최근 6개월 일봉 데이터를 받아 RSI(14), 20일/200일 이동평균선을 계산하고,
    아래 조건에 따라 직관적인 한국어 매수 신호를 반환.

    로직 (우선순위 순):
    1. 현재가 < 200일 이평선: "📉 추세이탈 (200일선↓)"
    2. RSI ≥ 70: "✋ 진입금지 (과열)"
    3. RSI 50~65 & 현재가가 20일선 근처(-2% ~ +3%): "🚀 강력매수 (눌림목)"
    4. RSI < 40 & 현재가 > 200일선: "🟢 저점매수 (반등)"
    5. 현재가 > 20일선: "🟢 매수적기 (추세)"
    6. 그 외: "👀 관망 (20일선 이탈)"

    Returns:
        dict: {Ticker, Price, RSI, MA20, MA200, Action}
    """
    import yfinance as yf
    import pandas as pd

    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='6mo')

        if len(hist) < 20:
            return {
                'Ticker': ticker,
                'Price': None,
                'RSI': None,
                'MA20': None,
                'MA200': None,
                'Action': "⚠️ 데이터 부족"
            }

        # 현재가
        price = hist['Close'].iloc[-1]

        # RSI(14) 계산
        rsi = calculate_rsi(hist['Close'], 14)

        # 20일 이동평균선
        ma_20 = hist['Close'].tail(20).mean()

        # 200일 이동평균선 (6개월 데이터로는 ~126일이므로 있는 데이터로 계산)
        if len(hist) >= 200:
            ma_200 = hist['Close'].tail(200).mean()
        else:
            # 200일 데이터가 없으면 1년치 다시 가져오기
            hist_1y = stock.history(period='1y')
            if len(hist_1y) >= 200:
                ma_200 = hist_1y['Close'].tail(200).mean()
            else:
                ma_200 = hist_1y['Close'].mean()  # 있는 데이터로 계산

        # 20일선 대비 이격도 계산
        ma20_distance = ((price - ma_20) / ma_20) * 100

        # 매수 신호 결정 (우선순위 순)
        action = ""

        # 1. 현재가 < 200일 이평선
        if price < ma_200:
            action = "📉 추세이탈 (200일선↓)"
        # 2. RSI ≥ 70 (과열)
        elif rsi is not None and rsi >= 70:
            action = "✋ 진입금지 (과열)"
        # 3. RSI 50~65 & 20일선 근처(-2% ~ +3%) - 눌림목
        elif rsi is not None and 50 <= rsi <= 65 and -2 <= ma20_distance <= 3:
            action = "🚀 강력매수 (눌림목)"
        # 4. RSI < 40 & 현재가 > 200일선 - 저점 반등
        elif rsi is not None and rsi < 40 and price > ma_200:
            action = "🟢 저점매수 (반등)"
        # 5. 현재가 > 20일선 - 추세 매수
        elif price > ma_20:
            action = "🟢 매수적기 (추세)"
        # 6. 그 외
        else:
            action = "👀 관망 (20일선 이탈)"

        return {
            'Ticker': ticker,
            'Price': round(price, 2),
            'RSI': round(rsi, 1) if rsi else None,
            'MA20': round(ma_20, 2),
            'MA200': round(ma_200, 2),
            'Action': action
        }

    except Exception as e:
        return {
            'Ticker': ticker,
            'Price': None,
            'RSI': None,
            'MA20': None,
            'MA200': None,
            'Action': f"⚠️ 오류"
        }


def get_technical_action(ticker, price, ma_20, rsi=None):
    """
    기술적 액션 결정 (레거시 호환용)
    """
    if rsi is None:
        return "BUY", "Trend"

    # MA20 대비 위치
    ma_distance = (price - ma_20) / ma_20 * 100

    if rsi > 70:
        return "WAIT", "RSI High"
    elif rsi < 30 and ma_distance > 0:
        return "STRONG", "Oversold"
    elif rsi < 40 and ma_distance > 0:
        return "BUY", "Dip"
    elif 40 <= rsi <= 60 and ma_distance > 0:
        return "BUY", "Trend"
    elif rsi > 60 and ma_distance > 3:
        return "HOLD", "Extended"
    else:
        return "BUY", "Trend"


def analyze_sector_signal(screening_df):
    """
    섹터별 Broad/Narrow 분석 + ETF 추천
    """
    if screening_df.empty:
        return []

    # 섹터별 집계
    sector_stats = screening_df.groupby('sector').agg({
        'ticker': 'count',
        'score_321': 'sum'
    }).rename(columns={'ticker': 'count'})

    sector_stats = sector_stats.sort_values('score_321', ascending=False)

    # ETF 매핑
    SECTOR_ETF = {
        'Semiconductor': {'type': 'Narrow', 'etf_1x': 'SMH', 'etf_3x': 'SOXL'},
        'Tech': {'type': 'Broad', 'etf_1x': 'XLK', 'etf_3x': 'TECL'},
        'Technology': {'type': 'Broad', 'etf_1x': 'XLK', 'etf_3x': 'TECL'},
        'Financial Services': {'type': 'Broad', 'etf_1x': 'XLF', 'etf_3x': 'FAS'},
        'Financial': {'type': 'Broad', 'etf_1x': 'XLF', 'etf_3x': 'FAS'},
        'Industrials': {'type': 'Broad', 'etf_1x': 'XLI', 'etf_3x': 'DUSL'},
        'Healthcare': {'type': 'Broad', 'etf_1x': 'XLV', 'etf_3x': 'CURE'},
        'Consumer Cyclical': {'type': 'Broad', 'etf_1x': 'XLY', 'etf_3x': 'WANT'},
        'Consumer Defensive': {'type': 'Broad', 'etf_1x': 'XLP', 'etf_3x': 'None'},
        'Energy': {'type': 'Broad', 'etf_1x': 'XLE', 'etf_3x': 'ERX'},
        'Basic Materials': {'type': 'Narrow', 'etf_1x': 'XLB', 'etf_3x': 'MATL'},
        'Real Estate': {'type': 'Broad', 'etf_1x': 'XLRE', 'etf_3x': 'DRN'},
        'Utilities': {'type': 'Broad', 'etf_1x': 'XLU', 'etf_3x': 'UTSL'},
    }

    results = []
    for sector in sector_stats.head(2).index:
        info = SECTOR_ETF.get(sector, {'type': 'Broad', 'etf_1x': 'SPY', 'etf_3x': 'UPRO'})
        count = int(sector_stats.loc[sector, 'count'])
        results.append({
            'sector': sector,
            'type': info['type'],
            'etf_1x': info['etf_1x'],
            'etf_3x': info['etf_3x'],
            'count': count
        })

    return results


def get_earnings_warning(screening_df, config):
    """실적발표 임박 종목 체크"""
    import yfinance as yf

    warnings = []
    blackout_days = config.get('earnings_blackout_days', 5)

    # Top 10 종목만 체크 (API 호출 최소화)
    for ticker in screening_df.head(10)['ticker'].tolist():
        try:
            stock = yf.Ticker(ticker)
            calendar = stock.calendar

            if calendar is not None and 'Earnings Date' in calendar:
                earnings_date = calendar['Earnings Date']
                if isinstance(earnings_date, (list, tuple)):
                    earnings_date = earnings_date[0]
                if earnings_date:
                    days_to = (earnings_date.date() - datetime.now().date()).days
                    if 0 <= days_to <= blackout_days:
                        warnings.append(f"{ticker} (D-{days_to})")
        except:
            continue

    return warnings


def create_telegram_message(screening_df, stats, changes=None, config=None):
    """
    텔레그램 메시지 생성 함수 (요구사항 2)

    목표: 모바일 가독성을 위해 HTML 태그를 사용하고, 스크리닝된 모든 종목을 리스트업 (개수 제한 없음)

    포맷 가이드:
    - 헤더: 🚀 <b>[MM/DD] EPS 모멘텀 브리핑</b> (총 N건)
    - 본문 (종목별 루프):
      - 첫 줄: 순위. <b>티커</b> 회사명(15자로 자름)
      - 둘째 줄: └ 점수 | 섹터 | 액션
    - 하단: 시장 요약(Narrow/Broad 테마) 및 리스크 알림
    """
    import yfinance as yf

    today = datetime.now().strftime('%m/%d')
    config = config or {}
    total_count = len(screening_df)

    # 섹터 한국어 매핑
    sector_map = {
        'Semiconductor': '반도체', 'Tech': '기술', 'Technology': '기술',
        'Industrials': '산업재', 'Financial Services': '금융', 'Financial': '금융',
        'Healthcare': '헬스케어', 'Consumer Cyclical': '소비재',
        'Consumer Defensive': '필수소비', 'Energy': '에너지',
        'Basic Materials': '소재', 'Real Estate': '부동산', 'Utilities': '유틸리티',
        'Communication Services': '통신', 'Consumer': '소비재', 'Other': '기타'
    }

    # ========================================
    # 헤더
    # ========================================
    msg = f"🚀 <b>[{today}] EPS 모멘텀 브리핑</b> (총 {total_count}건)\n\n"

    # ========================================
    # 모든 종목 리스트업 (개수 제한 없음)
    # ========================================
    for idx, (_, row) in enumerate(screening_df.iterrows(), 1):
        ticker = row['ticker']
        score = row['score_321']
        sector = row.get('sector', 'Other')
        sector_kr = sector_map.get(sector, sector[:4] if len(sector) > 4 else sector)

        # 회사명 가져오기 (캐싱 없이 간단히)
        try:
            stock = yf.Ticker(ticker)
            company_name = stock.info.get('shortName', ticker)
            # 15자로 자름
            if len(company_name) > 15:
                company_name = company_name[:13] + '..'
        except:
            company_name = ticker

        # 기술적 분석으로 액션 결정
        tech_result = analyze_technical(ticker)
        action = tech_result.get('Action', '🟢 매수적기 (추세)')

        # 메시지 포맷
        # 첫 줄: 순위. <b>티커</b> 회사명
        msg += f"{idx}. <b>{ticker}</b> {company_name}\n"
        # 둘째 줄: └ 점수 | 섹터 | 액션
        msg += f"   └ {score:.1f} | {sector_kr} | {action}\n"

    # ========================================
    # 시장 요약 (Narrow/Broad 테마)
    # ========================================
    sector_signals = analyze_sector_signal(screening_df)
    if sector_signals:
        msg += "\n<b>📊 시장 테마</b>\n"
        for sig in sector_signals:
            theme_type = "Narrow" if sig['type'] == 'Narrow' else "Broad"
            msg += f"• {sig['sector']} ({theme_type}): {sig['count']}종목\n"
            msg += f"  ETF: {sig['etf_1x']} / {sig['etf_3x']}\n"

    # ========================================
    # 리스크 알림
    # ========================================
    earnings_warnings = get_earnings_warning(screening_df, config) if config else []
    if earnings_warnings:
        msg += "\n<b>⚠️ 실적발표 임박</b>\n"
        msg += f"{', '.join(earnings_warnings)}\n"

    # ========================================
    # 편입/편출 변경 사항
    # ========================================
    added_list = changes.get('added', []) if changes else []
    removed_list = changes.get('removed', []) if changes else []

    if added_list or removed_list:
        msg += "\n<b>📋 포트폴리오 변경</b>\n"
        if added_list:
            msg += f"+ 신규: {', '.join(added_list)}\n"
        if removed_list:
            msg += f"- 편출: {', '.join(removed_list)}\n"

    # ========================================
    # 시스템 상태
    # ========================================
    db_size = 0
    if DB_PATH.exists():
        db_size = DB_PATH.stat().st_size / (1024 * 1024)  # MB

    msg += f"\n<b>📈 통계</b>\n"
    msg += f"스캔: {stats.get('total', 0)} | 통과: {stats.get('passed', 0)} | DB: {db_size:.1f}MB\n"

    return msg


def format_telegram_message(screening_df, stats, changes=None, config=None):
    """
    텔레그램 메시지 (레거시 호환용 - create_telegram_message로 대체)
    """
    return create_telegram_message(screening_df, stats, changes, config)


def send_telegram_long(message, config):
    """긴 메시지를 여러 개로 분할해서 전송"""
    if not config.get('telegram_enabled', False):
        return False

    bot_token = config.get('telegram_bot_token', '')
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
    """메인 실행"""
    log("=" * 60)
    log("EPS Momentum Daily Runner 시작")
    log("=" * 60)

    start_time = datetime.now()

    # 설정 로드
    config = load_config()
    log(f"설정 로드 완료: {CONFIG_PATH}")

    # Track 1: 스크리닝
    screening_df, stats = run_screening(config)

    # Track 2: 데이터 축적
    collected, errors = run_data_collection(config)

    # 리포트 생성
    changes = None
    if not screening_df.empty:
        md_path, html_path = generate_report(screening_df, stats, config)
        changes = get_portfolio_changes(screening_df, config)
        log(f"편입: {len(changes['added'])}개, 편출: {len(changes['removed'])}개")

    # Git commit/push
    git_commit_push(config)

    # 텔레그램 알림
    if config.get('telegram_enabled', False) and not screening_df.empty:
        msg = format_telegram_message(screening_df, stats, changes, config)
        send_telegram_long(msg, config)

    # 완료
    elapsed = (datetime.now() - start_time).total_seconds()
    log(f"전체 완료: {elapsed:.1f}초 소요")
    log("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
