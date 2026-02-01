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
import io
import json
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Windows에서 UTF-8 인코딩 강제 적용 (이모지 지원)
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

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
    "kill_switch_threshold": -0.01,  # -1% (일시적 변동 허용)
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
# 시장 국면 (Market Regime) 체크
# ============================================================

def check_market_regime():
    """
    시장 국면 체크 - SPY(S&P 500 ETF) 기준

    Returns:
        dict: {
            'regime': 'BULL' | 'BEAR',
            'spy_price': float,
            'spy_ma200': float,
            'spy_above_ma200': bool,
            'spy_distance': float (MA200 대비 %)
        }
    """
    import yfinance as yf

    try:
        spy = yf.Ticker('SPY')
        hist = spy.history(period='1y')

        if len(hist) < 200:
            log("SPY 데이터 부족, 기본값(BULL) 사용", "WARN")
            return {
                'regime': 'BULL',
                'spy_price': None,
                'spy_ma200': None,
                'spy_above_ma200': True,
                'spy_distance': 0
            }

        spy_price = hist['Close'].iloc[-1]
        spy_ma200 = hist['Close'].tail(200).mean()
        spy_above_ma200 = spy_price > spy_ma200
        spy_distance = ((spy_price - spy_ma200) / spy_ma200) * 100

        regime = 'BULL' if spy_above_ma200 else 'BEAR'

        log(f"시장 국면: {regime} (SPY ${spy_price:.2f}, MA200 ${spy_ma200:.2f}, {spy_distance:+.1f}%)")

        return {
            'regime': regime,
            'spy_price': round(spy_price, 2),
            'spy_ma200': round(spy_ma200, 2),
            'spy_above_ma200': spy_above_ma200,
            'spy_distance': round(spy_distance, 1)
        }

    except Exception as e:
        log(f"시장 국면 체크 실패: {e}", "ERROR")
        return {
            'regime': 'BULL',
            'spy_price': None,
            'spy_ma200': None,
            'spy_above_ma200': True,
            'spy_distance': 0
        }


# ============================================================
# Track 1 & 2 실행
# ============================================================

def run_screening(config, market_regime=None):
    """
    Track 1: 실시간 스크리닝 v5.3

    === 필터 구조 ===

    0. Market Regime Check (v5.3):
       - SPY < MA200 (하락장): 필터 2배 강화
         - Score 4.0 → 6.0
         - PEG 2.0 → 1.5

    1. Fundamental Filters (필수 조건):
       - Score >= 4.0 (가중치 3-2-1 + 정배열 보너스)
       - Kill Switch: EPS(Current) < EPS(7d) * 0.99 시 탈락
       - Dollar Volume >= $20M
       - Price > MA200 (장기 상승 추세)
       - 실적발표 기간 (D-5 ~ D+1) 제외

    2. Quality & Value Filter (OR 조건):
       A. Quality Growth: Rev Growth >= 5% AND Op Growth >= Rev Growth
       B. Reasonable Value: PEG < 2.0
       C. Technical Rescue: 재무 데이터 없으면 Price > MA60
    """
    log("Track 1: 실시간 스크리닝 v5.3 시작")

    try:
        import yfinance as yf
        import pandas as pd
        import numpy as np

        from eps_momentum_system import (
            INDICES, SECTOR_MAP,
            calculate_momentum_score_v3, calculate_slope_score,
            check_technical_filter, get_peg_ratio
        )

        today = datetime.now().strftime('%Y-%m-%d')

        # 시장 국면에 따른 필터 강화
        if market_regime and market_regime.get('regime') == 'BEAR':
            min_score = 6.0  # 4.0 → 6.0 (강화)
            max_peg = 1.5    # 2.0 → 1.5 (강화)
            log(f"🚨 하락장 감지! 필터 강화: Score >= {min_score}, PEG < {max_peg}")
        else:
            min_score = config.get('min_score', 4.0)
            max_peg = 2.0

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
            'low_score': 0,
            'low_volume': 0,
            'below_ma200': 0,
            'earnings_blackout': 0,
            'no_quality_value': 0,
            'data_error': 0,
            'passed': 0,
            'aligned': 0,
            'quality_growth': 0,
            'reasonable_value': 0,
            'technical_rescue': 0,
            'market_regime': market_regime,
            'min_score_used': min_score,
            'max_peg_used': max_peg
        }

        for ticker, idx_name in all_tickers.items():
            try:
                stock = yf.Ticker(ticker)
                trend = stock.eps_trend
                info = stock.info

                # === FILTER 1: EPS 데이터 존재 ===
                if trend is None or '+1y' not in trend.index:
                    stats['no_eps'] += 1
                    continue

                eps_row = trend.loc['+1y']
                current = eps_row.get('current')
                d7 = eps_row.get('7daysAgo')
                d30 = eps_row.get('30daysAgo')
                d60 = eps_row.get('60daysAgo')
                d90 = eps_row.get('90daysAgo')

                # === FILTER 2: Kill Switch (1% 하락시 탈락) ===
                if pd.notna(current) and pd.notna(d7) and d7 != 0:
                    if current < d7 * 0.99:  # 1% 이상 하락
                        stats['killed'] += 1
                        continue

                # === FILTER 3: Score >= 4.0 ===
                score_321, eps_chg, passed, is_aligned = calculate_momentum_score_v3(current, d7, d30, d60, d90)
                score_slope = calculate_slope_score(current, d7, d30, d60)

                if not passed or score_321 is None or score_321 < min_score:
                    stats['low_score'] += 1
                    continue

                # === 가격/거래량/MA 데이터 ===
                hist_1m = stock.history(period='1mo')
                hist_1y = stock.history(period='1y')

                if len(hist_1m) < 5:
                    stats['data_error'] += 1
                    continue

                price = hist_1m['Close'].iloc[-1]
                avg_volume = hist_1m['Volume'].mean()
                dollar_volume = price * avg_volume

                # === FILTER 4: Dollar Volume >= $20M ===
                if dollar_volume < 20_000_000:
                    stats['low_volume'] += 1
                    continue

                # MA 계산
                ma_20 = hist_1m['Close'].tail(20).mean() if len(hist_1m) >= 20 else hist_1m['Close'].mean()
                ma_60 = hist_1y['Close'].tail(60).mean() if len(hist_1y) >= 60 else None
                ma_200 = hist_1y['Close'].tail(200).mean() if len(hist_1y) >= 200 else None

                # === FILTER 5: Price > MA200 (장기 상승 추세) ===
                if ma_200 is not None and price <= ma_200:
                    stats['below_ma200'] += 1
                    continue

                # === FILTER 6: 실적 발표일 Blackout ===
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
                    pass

                # === 펀더멘털 데이터 수집 ===
                peg = info.get('pegRatio')

                # 52주 고점 대비
                from_52w_high = None
                if len(hist_1y) > 50:
                    high_52w = hist_1y['High'].max()
                    from_52w_high = ((price - high_52w) / high_52w) * 100

                # 성장률 계산
                rev_growth = None
                op_growth = None
                q_fin = stock.quarterly_financials
                if q_fin is not None and not q_fin.empty and q_fin.shape[1] >= 5:
                    if 'Total Revenue' in q_fin.index:
                        rev_curr = q_fin.loc['Total Revenue'].iloc[0]
                        rev_prev = q_fin.loc['Total Revenue'].iloc[4]
                        if rev_prev and rev_prev != 0:
                            rev_growth = ((rev_curr - rev_prev) / abs(rev_prev)) * 100
                    if 'Operating Income' in q_fin.index:
                        op_curr = q_fin.loc['Operating Income'].iloc[0]
                        op_prev = q_fin.loc['Operating Income'].iloc[4]
                        if op_prev and op_prev != 0:
                            op_growth = ((op_curr - op_prev) / abs(op_prev)) * 100

                # === FILTER 7: Quality & Value Filter (OR 조건) ===
                pass_reason = None

                # A. Quality Growth: Rev >= 5% AND Op >= Rev
                is_quality_growth = False
                if rev_growth is not None and op_growth is not None:
                    if rev_growth >= 5 and op_growth >= rev_growth:
                        is_quality_growth = True
                        pass_reason = f"Quality Growth (Rev+{rev_growth:.0f}%, Op+{op_growth:.0f}%)"
                        stats['quality_growth'] += 1

                # B. Reasonable Value: PEG < max_peg (하락장시 1.5, 상승장시 2.0)
                is_reasonable_value = False
                if not pass_reason and peg is not None and peg < max_peg and peg > 0:
                    is_reasonable_value = True
                    pass_reason = f"Reasonable Value (PEG {peg:.1f})"
                    stats['reasonable_value'] += 1

                # C. Technical Rescue: 데이터 없으면 Price > MA60
                is_technical_rescue = False
                has_fund_data = (peg is not None or rev_growth is not None)
                if not pass_reason and not has_fund_data:
                    if ma_60 is not None and price > ma_60:
                        is_technical_rescue = True
                        pass_reason = "Technical Rescue (Price > MA60)"
                        stats['technical_rescue'] += 1

                # 아무 조건도 통과 못하면 제외
                if not pass_reason:
                    stats['no_quality_value'] += 1
                    continue

                # === 통과! ===
                sector = SECTOR_MAP.get(ticker, info.get('sector', 'Other'))
                if is_aligned:
                    stats['aligned'] += 1

                # RSI 계산
                rsi = None
                if len(hist_1m) >= 14:
                    delta = hist_1m['Close'].diff()
                    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi_series = 100 - (100 / (1 + rs))
                    rsi = rsi_series.iloc[-1] if not pd.isna(rsi_series.iloc[-1]) else None

                # Action 결정 (52주 고점 대비 위치 포함)
                action = get_action_label(price, ma_20, ma_200, rsi, from_52w_high)

                candidates.append({
                    'ticker': ticker,
                    'index': idx_name,
                    'score_321': round(score_321, 1),
                    'score_slope': round(score_slope, 1) if score_slope else None,
                    'eps_chg_60d': round(eps_chg, 1) if eps_chg else None,
                    'peg': round(peg, 2) if peg else None,
                    'price': round(price, 2),
                    'ma_20': round(ma_20, 2),
                    'ma_60': round(ma_60, 2) if ma_60 else None,
                    'ma_200': round(ma_200, 2) if ma_200 else None,
                    'rsi': round(rsi, 1) if rsi else None,
                    'dollar_vol_M': round(dollar_volume / 1_000_000, 1),
                    'sector': sector,
                    'current': current,
                    '7d': d7,
                    '30d': d30,
                    '60d': d60,
                    '90d': d90,
                    'is_aligned': is_aligned,
                    'is_quality_growth': is_quality_growth,
                    'is_reasonable_value': is_reasonable_value,
                    'is_technical_rescue': is_technical_rescue,
                    'pass_reason': pass_reason,
                    'rev_growth': round(rev_growth, 1) if rev_growth else None,
                    'op_growth': round(op_growth, 1) if op_growth else None,
                    'from_52w_high': round(from_52w_high, 1) if from_52w_high else None,
                    'action': action,
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
            log(f"Track 1 완료: {len(df)}개 종목 -> {csv_path}")
        else:
            log("Track 1: 조건 충족 종목 없음", "WARN")

        return df, stats

    except Exception as e:
        log(f"Track 1 실패: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), {}


def get_action_label(price, ma_20, ma_200, rsi, from_52w_high=None):
    """
    실전 매매용 액션 레이블 v2

    핵심 원칙:
    - 52주 고점 근처는 상승여력 제한 → 진입 금지
    - 진짜 눌림목 = 고점 대비 충분히 조정 + 추세 유지
    - RSI만으로 판단하지 않고 가격 위치 종합 고려

    === 액션 정의 ===

    1. 진입금지: 지금 사면 물릴 확률 높음
       - RSI >= 70 (과열)
       - 52주 고점 -5% 이내 (천장 근처)
       - MA20 대비 +8% 이상 (단기 과열)

    2. 적극매수 (눌림목): 좋은 진입 기회
       - 52주 고점 -10% ~ -25% (의미있는 조정)
       - RSI 35-55 (과매도~중립)
       - Price > MA200 (장기 추세 유지)
       - Price <= MA20*1.03 (MA20 근처 또는 아래)

    3. 저점매수 (반등): 공포에 매수
       - RSI < 35 (과매도)
       - 52주 고점 -20% 이상 (큰 조정)
       - Price > MA200 (장기 추세 유지)

    4. 매수적기 (추세): 정상적인 상승 추세
       - Price > MA20 > MA200 (정배열)
       - RSI 40-65 (건강한 범위)
       - 52주 고점 -5% ~ -15% (상승 여력 있음)

    5. 관망: 진입 애매
       - 위 조건 불충족
       - 또는 RSI 65-70 (과열 경계)

    6. 추세이탈: 매수 금지
       - Price < MA200 (장기 하락 추세)
    """
    # 기본값 처리
    if rsi is None:
        rsi = 50  # 중립 가정
    if from_52w_high is None:
        from_52w_high = -10  # 모르면 중간값 가정

    # MA 대비 거리 계산
    ma20_pct = ((price - ma_20) / ma_20 * 100) if ma_20 else 0
    ma200_pct = ((price - ma_200) / ma_200 * 100) if ma_200 else 0

    # === 1. 추세이탈 (최우선 체크) ===
    if ma_200 and price < ma_200:
        return "추세이탈 (MA200↓)"

    # === 2. 진입금지 조건 ===
    # 2a. RSI 과열
    if rsi >= 70:
        return "진입금지 (RSI과열)"

    # 2b. 52주 고점 근처 (-5% 이내)
    if from_52w_high > -5:
        return "진입금지 (고점근처)"

    # 2c. MA20 대비 +8% 이상 급등
    if ma20_pct >= 8:
        return "진입금지 (단기급등)"

    # === 3. 저점매수 (과매도 반등) ===
    if rsi <= 35 and from_52w_high <= -20:
        return "저점매수 (과매도)"

    # === 4. 적극매수 (진짜 눌림목) ===
    # 조건: 고점대비 조정폭 + RSI 중립 이하 + MA20 근처/아래
    is_meaningful_correction = -25 <= from_52w_high <= -10
    is_rsi_neutral = 35 <= rsi <= 55
    is_near_ma20 = ma20_pct <= 3  # MA20 근처 또는 아래

    if is_meaningful_correction and is_rsi_neutral and is_near_ma20:
        return "적극매수 (눌림목)"

    # === 5. 매수적기 (건강한 추세) ===
    # 조건: 정배열 + RSI 건강 + 상승 여력 있음
    is_aligned = ma_20 and ma_200 and price > ma_20 > ma_200
    is_rsi_healthy = 40 <= rsi <= 65
    has_upside = -15 <= from_52w_high <= -5

    if is_aligned and is_rsi_healthy and has_upside:
        return "매수적기 (추세)"

    # === 6. 관망 (진입 애매) ===
    # RSI 65-70 경계 구간
    if 65 <= rsi < 70:
        return "관망 (과열경계)"

    # 고점 대비 조정 부족 (-5% ~ -10%)
    if -10 < from_52w_high <= -5:
        return "관망 (조정부족)"

    # 기타
    return "관망"


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

        # 테이블 생성 (없으면) - v4: 추가 필드 포함
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
                is_aligned INTEGER DEFAULT 0,
                is_undervalued INTEGER DEFAULT 0,
                is_growth INTEGER DEFAULT 0,
                peg REAL,
                forward_pe REAL,
                from_52w_high REAL,
                rsi REAL,
                rev_growth_yoy REAL,
                op_growth_yoy REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, ticker, period)
            )
        ''')

        # 새 컬럼 추가 (기존 테이블에)
        new_columns = [
            ('is_aligned', 'INTEGER DEFAULT 0'),
            ('is_undervalued', 'INTEGER DEFAULT 0'),
            ('is_growth', 'INTEGER DEFAULT 0'),
            ('peg', 'REAL'),
            ('forward_pe', 'REAL'),
            ('from_52w_high', 'REAL'),
            ('rsi', 'REAL'),
            ('rev_growth_yoy', 'REAL'),
            ('op_growth_yoy', 'REAL'),
        ]
        for col_name, col_type in new_columns:
            try:
                cursor.execute(f'ALTER TABLE eps_snapshots ADD COLUMN {col_name} {col_type}')
            except:
                pass  # 이미 존재하면 무시

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
                is_aligned = 0

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

                    # 정배열 체크
                    if (pd.notna(eps_current) and pd.notna(eps_7d) and
                        pd.notna(eps_30d) and pd.notna(eps_60d)):
                        if eps_current > eps_7d > eps_30d > eps_60d:
                            is_aligned = 1

                    if passed and score_321 and score_321 >= 4.0:
                        if dollar_volume >= 20_000_000:
                            passed_screen = 1

                # 펀더멘털 분석 (백테스트용)
                fund_result = analyze_fundamentals(ticker)
                peg = fund_result.get('peg')
                forward_pe = fund_result.get('forward_pe')
                from_52w_high = fund_result.get('from_52w_high')
                rev_growth_yoy = fund_result.get('rev_growth_yoy')
                op_growth_yoy = fund_result.get('op_growth_yoy')
                is_undervalued = 1 if fund_result.get('is_undervalued') else 0
                is_growth = 1 if fund_result.get('is_growth') else 0

                # RSI 계산
                rsi = None
                if len(hist) >= 15:
                    rsi = calculate_rsi(hist['Close'])

                # DB 저장 (확장된 필드)
                cursor.execute('''
                    INSERT OR REPLACE INTO eps_snapshots
                    (date, ticker, index_name, period, eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                     price, volume, dollar_volume, market_cap, sector, ma_20, above_ma20,
                     score_321, score_slope, eps_chg_60d, passed_screen,
                     is_aligned, is_undervalued, is_growth, peg, forward_pe, from_52w_high, rsi,
                     rev_growth_yoy, op_growth_yoy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (today, ticker, idx_name, '+1y',
                      eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                      price, avg_volume, dollar_volume, market_cap, sector,
                      ma_20, above_ma20, score_321, score_slope, eps_chg_60d, passed_screen,
                      is_aligned, is_undervalued, is_growth, peg, forward_pe, from_52w_high, rsi,
                      rev_growth_yoy, op_growth_yoy))

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


def analyze_fundamentals(ticker):
    """
    펀더멘털 분석: 저평가 + 매출/영업이익 성장률

    저평가 조건 (OR):
    - PEG < 1
    - 52주 고점 대비 -10% 이상 조정

    성장 조건 (YoY 또는 QoQ):
    - 매출 >= +10% AND 영업이익 >= +5%

    Returns:
        dict: 펀더멘털 지표들
    """
    import yfinance as yf

    result = {
        'ticker': ticker,
        'peg': None,
        'forward_pe': None,
        'from_52w_high': None,
        'rev_growth_qoq': None,
        'rev_growth_yoy': None,
        'op_growth_qoq': None,
        'op_growth_yoy': None,
        'is_undervalued': False,
        'is_growth': False,
        'undervalued_reason': None,
        'growth_reason': None
    }

    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        # PEG, Forward P/E
        peg = info.get('pegRatio')
        forward_pe = info.get('forwardPE')
        result['peg'] = round(peg, 2) if peg else None
        result['forward_pe'] = round(forward_pe, 2) if forward_pe else None

        # 52주 고점 대비
        hist_1y = stock.history(period='1y')
        if len(hist_1y) > 50:
            high_52w = hist_1y['High'].max()
            current_price = hist_1y['Close'].iloc[-1]
            from_high = ((current_price - high_52w) / high_52w) * 100
            result['from_52w_high'] = round(from_high, 1)

        # 매출/영업이익 성장률
        q_fin = stock.quarterly_financials
        if q_fin is not None and not q_fin.empty and q_fin.shape[1] >= 2:
            # QoQ (전분기 대비)
            if 'Total Revenue' in q_fin.index:
                rev_curr = q_fin.loc['Total Revenue'].iloc[0]
                rev_prev = q_fin.loc['Total Revenue'].iloc[1]
                if rev_prev and rev_prev != 0:
                    result['rev_growth_qoq'] = round(((rev_curr - rev_prev) / abs(rev_prev)) * 100, 1)

            if 'Operating Income' in q_fin.index:
                op_curr = q_fin.loc['Operating Income'].iloc[0]
                op_prev = q_fin.loc['Operating Income'].iloc[1]
                if op_prev and op_prev != 0:
                    result['op_growth_qoq'] = round(((op_curr - op_prev) / abs(op_prev)) * 100, 1)

            # YoY (전년 동기 대비)
            if q_fin.shape[1] >= 5:
                if 'Total Revenue' in q_fin.index:
                    rev_curr = q_fin.loc['Total Revenue'].iloc[0]
                    rev_yoy_prev = q_fin.loc['Total Revenue'].iloc[4]
                    if rev_yoy_prev and rev_yoy_prev != 0:
                        result['rev_growth_yoy'] = round(((rev_curr - rev_yoy_prev) / abs(rev_yoy_prev)) * 100, 1)

                if 'Operating Income' in q_fin.index:
                    op_curr = q_fin.loc['Operating Income'].iloc[0]
                    op_yoy_prev = q_fin.loc['Operating Income'].iloc[4]
                    if op_yoy_prev and op_yoy_prev != 0:
                        result['op_growth_yoy'] = round(((op_curr - op_yoy_prev) / abs(op_yoy_prev)) * 100, 1)

        # 저평가 판단 (OR 조건)
        if peg and peg < 1:
            result['is_undervalued'] = True
            result['undervalued_reason'] = f'PEG {peg:.1f}'
        elif result['from_52w_high'] and result['from_52w_high'] <= -10:
            result['is_undervalued'] = True
            result['undervalued_reason'] = f'52주高{result["from_52w_high"]:.0f}%'

        # 성장 판단 (QoQ 또는 YoY에서 조건 충족)
        rev_qoq = result['rev_growth_qoq'] or 0
        rev_yoy = result['rev_growth_yoy'] or 0
        op_qoq = result['op_growth_qoq'] or 0
        op_yoy = result['op_growth_yoy'] or 0

        # 매출 >= +10% AND 영업이익 >= +5%
        if (rev_qoq >= 10 or rev_yoy >= 10) and (op_qoq >= 5 or op_yoy >= 5):
            result['is_growth'] = True
            best_rev = max(rev_qoq, rev_yoy)
            best_op = max(op_qoq, op_yoy)
            result['growth_reason'] = f'매출+{best_rev:.0f}%,영업+{best_op:.0f}%'

    except Exception as e:
        pass

    return result


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


def format_dollar_volume(dollar_vol_m):
    """거래대금을 M/B 단위로 포맷"""
    if dollar_vol_m is None:
        return "N/A"
    if dollar_vol_m >= 1000:
        return f"${dollar_vol_m/1000:.1f}B"
    else:
        return f"${dollar_vol_m:.0f}M"


def create_telegram_message(screening_df, stats, changes=None, config=None):
    """
    텔레그램 메시지 생성 함수 v5 - 전략 설명 + 상세 카드형 포맷

    [전략 설명 섹션]
    - 사용한 전략, 데이터 소스, 필터 기준 상세 설명
    - 왜 이 종목들이 선정되었는지 근거 제시

    [종목 카드 섹션]
    1번째 줄: 순위, 티커, 회사명, 현재가
    2번째 줄: EPS 모멘텀 점수, PEG, 섹터
    3번째 줄: 통과 사유 (Quality Growth / Reasonable Value / Technical Rescue)
    4번째 줄: 액션 (한국어), RSI, 거래대금
    """
    import yfinance as yf
    import math

    today = datetime.now().strftime('%m/%d')
    today_full = datetime.now().strftime('%Y-%m-%d %H:%M')
    config = config or {}
    total_count = len(screening_df)

    # 섹터 한국어 매핑
    sector_map = {
        'Semiconductor': '반도체', 'Tech': '기술', 'Technology': '기술',
        'Industrials': '산업재', 'Financial Services': '금융', 'Financial': '금융',
        'Healthcare': '헬스케어', 'Consumer Cyclical': '경기소비재',
        'Consumer Defensive': '필수소비재', 'Energy': '에너지',
        'Basic Materials': '소재', 'Real Estate': '부동산', 'Utilities': '유틸리티',
        'Communication Services': '통신서비스', 'Consumer': '소비재', 'Other': '기타'
    }

    # ========================================
    # 시장 국면 (Market Regime) 체크
    # ========================================
    market_regime = stats.get('market_regime', {})
    regime = market_regime.get('regime', 'BULL') if market_regime else 'BULL'
    spy_price = market_regime.get('spy_price') if market_regime else None
    spy_ma200 = market_regime.get('spy_ma200') if market_regime else None
    spy_distance = market_regime.get('spy_distance', 0) if market_regime else 0
    min_score_used = stats.get('min_score_used', 4.0)
    max_peg_used = stats.get('max_peg_used', 2.0)

    # ========================================
    # 헤더 + 시장 상태
    # ========================================
    if regime == 'BEAR':
        msg = f"🚨 <b>[{today}] EPS 모멘텀 v5.3 브리핑</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"⚠️ <b>시장 경보: 하락장 진입</b> ⚠️\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        if spy_price and spy_ma200:
            msg += f"🔴 SPY ${spy_price} &lt; MA200 ${spy_ma200} ({spy_distance:+.1f}%)\n"
        msg += f"📉 필터 강화: Score>={min_score_used:.0f}, PEG&lt;{max_peg_used:.1f}\n"
        msg += f"💡 <b>현금 비중 확대 권장</b>\n\n"
    else:
        msg = f"🚀 <b>[{today}] EPS 모멘텀 v5.3 브리핑</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        if spy_price and spy_ma200:
            msg += f"🟢 SPY ${spy_price} &gt; MA200 ${spy_ma200} ({spy_distance:+.1f}%)\n"
        msg += f"📈 시장 상승 추세 유지\n\n"

    msg += f"📅 {today_full} | 총 {total_count}개 통과\n\n"

    # 전략 설명 섹션
    msg += "<b>📋 전략 개요</b>\n"
    msg += "Forward EPS 컨센서스 상향 종목 중\n"
    msg += "품질/가치 기준을 충족하는 종목 선별\n\n"

    msg += "<b>🔍 데이터 소스</b>\n"
    msg += "• Yahoo Finance EPS Trend (+1Y Forward)\n"
    msg += "• 분기 재무제표 (매출/영업이익)\n"
    msg += f"• 유니버스: NASDAQ100 + S&P500 + S&P400\n\n"

    msg += "<b>⚙️ 필터 기준 (v5.3)</b>\n"
    if regime == 'BEAR':
        msg += "🚨 <b>하락장 강화 필터 적용중</b>\n"
    msg += "1️⃣ <b>필수 조건</b>\n"
    msg += f"   • EPS 모멘텀 점수 >= {min_score_used:.0f}\n"
    msg += "   • Kill Switch: 7일내 1%↓ 시 제외\n"
    msg += "   • 거래대금 >= $20M\n"
    msg += "   • <b>Price > MA200</b> (장기상승추세)\n"
    msg += "   • 실적발표 D-5~D+1 제외\n\n"

    msg += "2️⃣ <b>품질/가치 조건</b> (하나 이상 충족)\n"
    msg += "   A. Quality Growth: 매출↑5%+ & 영업익>=매출\n"
    msg += f"   B. Reasonable Value: PEG &lt; {max_peg_used:.1f}\n"
    msg += "   C. Technical Rescue: 데이터없으면 Price>MA60\n\n"

    # 필터 통계
    msg += "<b>📊 필터별 현황</b>\n"
    msg += f"• 총 스캔: {stats.get('total', 0)}개\n"
    msg += f"• EPS 없음: {stats.get('no_eps', 0)}개\n"
    msg += f"• Kill Switch: {stats.get('killed', 0)}개\n"
    msg += f"• 점수부족: {stats.get('low_score', 0)}개\n"
    msg += f"• 거래량부족: {stats.get('low_volume', 0)}개\n"
    msg += f"• MA200↓: {stats.get('below_ma200', 0)}개\n"
    msg += f"• 품질/가치 미충족: {stats.get('no_quality_value', 0)}개\n"
    msg += f"• <b>최종 통과: {total_count}개</b>\n"

    # 통과 사유별 분류
    msg += "\n<b>✅ 통과 사유 분류</b>\n"
    msg += f"• Quality Growth: {stats.get('quality_growth', 0)}개\n"
    msg += f"• Reasonable Value: {stats.get('reasonable_value', 0)}개\n"
    msg += f"• Technical Rescue: {stats.get('technical_rescue', 0)}개\n"

    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    # ========================================
    # 액션별 그룹화 (적극매수 우선)
    # ========================================
    action_priority = [
        ('적극매수', '🚀', '지금 매수 적기'),
        ('저점매수', '💎', '과매도 반등 기회'),
        ('매수적기', '🟢', '건강한 상승 추세'),
        ('관망', '👀', '진입 대기'),
        ('진입금지', '🚫', '매수 금지'),
        ('추세이탈', '⛔', '손절 검토'),
    ]

    aligned_count = 0
    quality_growth_count = 0
    reasonable_value_count = 0
    technical_rescue_count = 0

    # 통계 먼저 계산
    for _, row in screening_df.iterrows():
        if row.get('is_aligned', False):
            aligned_count += 1
        if row.get('is_quality_growth', False):
            quality_growth_count += 1
        if row.get('is_reasonable_value', False):
            reasonable_value_count += 1
        if row.get('is_technical_rescue', False):
            technical_rescue_count += 1

    # 액션별로 그룹화하여 출력
    for action_key, action_icon, action_desc in action_priority:
        # 해당 액션에 해당하는 종목 필터
        action_stocks = screening_df[screening_df['action'].str.contains(action_key, na=False)]

        if len(action_stocks) == 0:
            continue

        # 그룹 헤더
        msg += f"\n{action_icon} <b>{action_key}</b> ({len(action_stocks)}개) - {action_desc}\n"
        msg += "─" * 20 + "\n"

        for idx, (_, row) in enumerate(action_stocks.iterrows(), 1):
            ticker = row['ticker']
            score = row.get('score_321', 0)
            eps_chg = row.get('eps_chg_60d', 0)
            peg = row.get('peg', None)
            price = row.get('price', 0)
            sector = row.get('sector', 'Other')
            dollar_vol_m = row.get('dollar_vol_M', 0)
            is_aligned = row.get('is_aligned', False)
            rsi = row.get('rsi', None)
            action = row.get('action', '')
            from_52w_high = row.get('from_52w_high', None)

            sector_kr = sector_map.get(sector, sector[:6] if len(sector) > 6 else sector)

            # 포맷팅 (NaN 체크 포함)
            peg_str = f"{peg:.1f}" if (peg and not math.isnan(peg)) else "-"
            rsi_str = f"{rsi:.0f}" if (rsi and not math.isnan(rsi)) else "-"
            high_str = f"{from_52w_high:.0f}%" if from_52w_high else "-"
            eps_str = f"+{eps_chg:.0f}%" if eps_chg and eps_chg >= 0 else (f"{eps_chg:.0f}%" if eps_chg else "-")
            align_mark = "⬆" if is_aligned else ""

            # 간결한 2줄 포맷
            msg += f"<b>{ticker}</b> ${price:.0f} | 점수{score:.0f}{align_mark} | RSI{rsi_str} | 고점{high_str}\n"

            # 적극매수/저점매수/매수적기만 상세 사유 표시
            if action_key in ['적극매수', '저점매수', '매수적기']:
                # 액션 상세 사유 (괄호 안 내용)
                if '(' in action and ')' in action:
                    reason = action.split('(')[1].split(')')[0]
                    msg += f"   └ {reason} | {sector_kr} | PEG {peg_str}\n"

    # ========================================
    # 시장 테마 분석
    # ========================================
    sector_signals = analyze_sector_signal(screening_df)
    if sector_signals:
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>📊 시장 테마 분석</b>\n"
        for sig in sector_signals:
            theme_type = "🎯Narrow" if sig['type'] == 'Narrow' else "📈Broad"
            msg += f"• <b>{sig['sector']}</b> ({theme_type}): {sig['count']}종목\n"
            msg += f"  └ ETF: {sig['etf_1x']} (1x) / {sig['etf_3x']} (3x)\n"

    # ========================================
    # 포트폴리오 변경
    # ========================================
    added_list = changes.get('added', []) if changes else []
    removed_list = changes.get('removed', []) if changes else []

    if added_list or removed_list:
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>📋 전일 대비 변동</b>\n"
        if added_list:
            msg += f"🆕 편입({len(added_list)}): {', '.join(added_list[:10])}"
            if len(added_list) > 10:
                msg += f" 외 {len(added_list)-10}개"
            msg += "\n"
        if removed_list:
            msg += f"🚫 편출({len(removed_list)}): {', '.join(removed_list[:10])}"
            if len(removed_list) > 10:
                msg += f" 외 {len(removed_list)-10}개"
            msg += "\n"

    # ========================================
    # 요약 통계
    # ========================================
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>✨ 품질 요약</b>\n"
    if total_count > 0:
        msg += f"• 📈 정배열: {aligned_count}개 ({aligned_count/total_count*100:.0f}%)\n"
    msg += f"• 🌱 Quality Growth: {quality_growth_count}개\n"
    msg += f"• 💎 Reasonable Value: {reasonable_value_count}개\n"
    msg += f"• 🔧 Technical Rescue: {technical_rescue_count}개\n"

    # DB 상태
    db_size = 0
    if DB_PATH.exists():
        db_size = DB_PATH.stat().st_size / (1024 * 1024)
    msg += f"\n💾 DB: {db_size:.1f}MB\n"

    # 푸터
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>🤖 EPS Momentum Strategy v5.3</i>\n"
    if regime == 'BEAR':
        msg += "<i>🚨 Bear Market Filter Active</i>\n"
    else:
        msg += "<i>🟢 Bull Market + Quality/Value</i>\n"

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
    log("EPS Momentum Daily Runner v5.3 시작")
    log("=" * 60)

    start_time = datetime.now()

    # 설정 로드
    config = load_config()
    log(f"설정 로드 완료: {CONFIG_PATH}")

    # 시장 국면 체크 (v5.3)
    market_regime = check_market_regime()

    # Track 1: 스크리닝 (시장 국면 전달)
    screening_df, stats = run_screening(config, market_regime)

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
