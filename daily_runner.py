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

def generate_report(screening_df, stats, config):
    """일일 리포트 생성 (HTML + Markdown)"""
    log("리포트 생성 중...")

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


def format_telegram_message(screening_df, stats):
    """텔레그램용 메시지 포맷"""
    today = datetime.now().strftime('%Y-%m-%d')

    msg = f"""*EPS Momentum Report - {today}*

*Summary:*
- Scanned: {stats.get('total', 0)}
- Passed: {stats.get('passed', 0)}
- Kill Switch: {stats.get('killed', 0)}
- Earnings Blackout: {stats.get('earnings_blackout', 0)}

*Top 10 Candidates:*
"""

    for i, (_, row) in enumerate(screening_df.head(10).iterrows()):
        msg += f"`{i+1}. {row['ticker']:<6} {row['score_321']:>+5.1f} {row['eps_chg_60d']:>+6.1f}%`\n"

    return msg


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
    if not screening_df.empty:
        md_path, html_path = generate_report(screening_df, stats, config)

    # Git commit/push
    git_commit_push(config)

    # 텔레그램 알림
    if config.get('telegram_enabled', False) and not screening_df.empty:
        msg = format_telegram_message(screening_df, stats)
        send_telegram(msg, config)

    # 완료
    elapsed = (datetime.now() - start_time).total_seconds()
    log(f"전체 완료: {elapsed:.1f}초 소요")
    log("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
