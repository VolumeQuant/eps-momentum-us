"""
EPS Momentum Daily Runner v6.3 - Quality & Value Scorecard System

핵심 철학: "맛있는 사과를 좋은 값에" (Quality + Value)

기능:
1. Track 1: 실시간 스크리닝 → 3-Layer Filtering + Q/V Scorecard
2. Track 2: 전 종목 데이터 축적 → 백테스팅용
3. 일일 리포트 생성 (HTML + Markdown)
4. Git 자동 commit/push (선택)
5. 텔레그램 알림 (User Briefing + Admin Log 분리)

v6.3 주요 변경 (Quality & Value Scorecard):
- Quality Score (맛, 100점): EPS정배열 + ROE + EPS성장률 + MA200위 + 거래량스파이크
- Value Score (값, 100점): PEG + Forward PER + 52주고점대비 + RSI눌림목
- Actionable Score = (Quality × 0.5 + Value × 0.5) × Action Multiplier
- 거래량 스파이크 감지: 20일 평균 × 1.5 초과 시 신호
- 실적 D-Day 표시
- Fake Bottom 경고: RSI 낮지만 MA200 하회

v6.2 (이전):
- Action Multiplier로 RSI 과열 종목 페널티

v6.1 (이전):
- 가격위치 점수: 52주 고점 대비 위치

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
    """설정 로드 (config.json → 환경변수 순으로 체크)"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
            # 기본값 병합
            for key, value in DEFAULT_CONFIG.items():
                if key not in config:
                    config[key] = value
    else:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2, ensure_ascii=False)
        print(f"[INFO] 기본 설정 파일 생성: {CONFIG_PATH}")
        config = DEFAULT_CONFIG.copy()

    # 환경변수 오버라이드 (GitHub Actions용)
    if os.environ.get('TELEGRAM_BOT_TOKEN'):
        config['telegram_bot_token'] = os.environ['TELEGRAM_BOT_TOKEN']
        config['telegram_enabled'] = True
    if os.environ.get('TELEGRAM_CHAT_ID'):
        config['telegram_chat_id'] = os.environ['TELEGRAM_CHAT_ID']

    return config


def log(message, level="INFO"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def get_display_dates():
    """
    인사말과 시장 데이터에 표시할 날짜 계산

    Returns:
        dict: {
            'kr_date': '2월6일' (한국 날짜, 인사말용),
            'us_date': '2026년 02월 05일' (미국 최근 영업일, 시장 데이터용),
            'us_date_short': '02/05' (짧은 형식)
        }
    """
    import pandas as pd

    if HAS_PYTZ:
        # pytz 있으면 정확한 타임존 계산
        kst = pytz.timezone('Asia/Seoul')
        est = pytz.timezone('America/New_York')

        now_kst = datetime.now(kst)
        now_est = datetime.now(est)

        kr_date = now_kst.strftime('%m월%d일')

        # 미국 최근 영업일 계산
        us_date = now_est.date()

        # 미국 시간 기준 장 마감 전이면 전날로
        if now_est.hour < 16:
            us_date = us_date - timedelta(days=1)

        # 주말이면 금요일로
        while us_date.weekday() >= 5:  # 5=토, 6=일
            us_date = us_date - timedelta(days=1)
    else:
        # pytz 없으면 간단한 계산 (UTC 기준 추정)
        now = datetime.utcnow()

        # 한국 시간 = UTC + 9
        kr_time = now + timedelta(hours=9)
        kr_date = kr_time.strftime('%m월%d일')

        # 미국 동부 = UTC - 5 (겨울) / UTC - 4 (여름)
        # 대략 UTC - 5로 계산
        us_time = now - timedelta(hours=5)
        us_date = us_time.date()

        # 장 마감 전이면 전날
        if us_time.hour < 16:
            us_date = us_date - timedelta(days=1)

        # 주말이면 금요일로
        while us_date.weekday() >= 5:
            us_date = us_date - timedelta(days=1)

    return {
        'kr_date': kr_date,
        'us_date': us_date.strftime('%Y년 %m월 %d일'),
        'us_date_short': us_date.strftime('%m/%d'),
        'us_date_iso': us_date.strftime('%Y-%m-%d')
    }


# ============================================================
# 시장 국면 (Market Regime) 진단 시스템 v5.4
# ============================================================

def check_market_regime():
    """
    시장 국면 3단계 진단 - S&P 500, 나스닥, VIX 기반

    진단 기준 (우선순위 순):
    🔴 RED (위험/매매중단): S&P500 < MA50 OR 나스닥 < MA50 OR VIX >= 30
    🟡 YELLOW (경계/기준강화): S&P500 < MA20 OR 나스닥 < MA20 OR VIX >= 20
    🟢 GREEN (정상/적극매매): 위 조건에 해당하지 않음

    Returns:
        dict: {
            'regime': 'RED' | 'YELLOW' | 'GREEN',
            'reason': str,
            'spy_price': float,
            'spy_ma20': float,
            'spy_ma50': float,
            'spx_price': float,
            'spx_ma20': float,
            'spx_ma50': float,
            'ndx_price': float,
            'ndx_ma20': float,
            'ndx_ma50': float,
            'vix': float
        }
    """
    import yfinance as yf

    # 기본값 (데이터 실패시 보수적으로 YELLOW)
    default_result = {
        'regime': 'YELLOW',
        'reason': '데이터 로드 실패 - 보수적 접근',
        'spy_price': None, 'spy_ma20': None, 'spy_ma50': None,
        'spx_price': None, 'spx_ma20': None, 'spx_ma50': None,
        'ndx_price': None, 'ndx_ma20': None, 'ndx_ma50': None,
        'vix': None
    }

    try:
        # S&P 500 (^GSPC)
        spx = yf.Ticker('^GSPC')
        spx_hist = spx.history(period='3mo')
        spx_price = spx_hist['Close'].iloc[-1] if len(spx_hist) >= 50 else None
        spx_ma20 = spx_hist['Close'].tail(20).mean() if len(spx_hist) >= 20 else None
        spx_ma50 = spx_hist['Close'].tail(50).mean() if len(spx_hist) >= 50 else None

        # 나스닥 (^IXIC)
        ndx = yf.Ticker('^IXIC')
        ndx_hist = ndx.history(period='3mo')
        ndx_price = ndx_hist['Close'].iloc[-1] if len(ndx_hist) >= 50 else None
        ndx_ma20 = ndx_hist['Close'].tail(20).mean() if len(ndx_hist) >= 20 else None
        ndx_ma50 = ndx_hist['Close'].tail(50).mean() if len(ndx_hist) >= 50 else None

        # SPY (ETF, 보조)
        spy = yf.Ticker('SPY')
        spy_hist = spy.history(period='3mo')
        spy_price = spy_hist['Close'].iloc[-1] if len(spy_hist) >= 50 else None
        spy_ma20 = spy_hist['Close'].tail(20).mean() if len(spy_hist) >= 20 else None
        spy_ma50 = spy_hist['Close'].tail(50).mean() if len(spy_hist) >= 50 else None

        # VIX
        vix = None
        try:
            vix_ticker = yf.Ticker('^VIX')
            vix_hist = vix_ticker.history(period='5d')
            if len(vix_hist) > 0:
                vix = vix_hist['Close'].iloc[-1]
        except:
            log("VIX 데이터 로드 실패", "WARN")

        # 데이터 검증
        if spx_price is None or ndx_price is None:
            log("S&P 500 또는 나스닥 데이터 부족, 보수적으로 YELLOW 적용", "WARN")
            return default_result

        # === 진단 로직 (우선순위 순) ===
        regime = 'GREEN'
        reasons = []

        # 🔴 RED 체크
        if spx_ma50 and spx_price < spx_ma50:
            regime = 'RED'
            reasons.append(f'S&P500 ${spx_price:.0f} < MA50 ${spx_ma50:.0f}')
        if ndx_ma50 and ndx_price < ndx_ma50:
            regime = 'RED'
            reasons.append(f'나스닥 {ndx_price:.0f} < MA50 {ndx_ma50:.0f}')
        if vix is not None and vix >= 30:
            regime = 'RED'
            reasons.append(f'VIX {vix:.1f} (공포)')

        # 🟡 YELLOW 체크 (RED가 아닐 때만)
        if regime != 'RED':
            if spx_ma20 and spx_price < spx_ma20:
                regime = 'YELLOW'
                reasons.append(f'S&P500 ${spx_price:.0f} < MA20 ${spx_ma20:.0f}')
            if ndx_ma20 and ndx_price < ndx_ma20:
                regime = 'YELLOW'
                reasons.append(f'나스닥 {ndx_price:.0f} < MA20 {ndx_ma20:.0f}')
            if vix is not None and vix >= 20:
                regime = 'YELLOW'
                reasons.append(f'VIX {vix:.1f} (경계)')

        # 🟢 GREEN (정상)
        if regime == 'GREEN':
            reasons.append(f'S&P500 ${spx_price:.0f}, 나스닥 {ndx_price:.0f} 정상')
            if vix:
                reasons.append(f'VIX {vix:.1f}')

        reason = ', '.join(reasons)

        # 로그
        emoji = {'RED': '🔴', 'YELLOW': '🟡', 'GREEN': '🟢'}[regime]
        log(f"시장 국면: {emoji} {regime} - {reason}")

        return {
            'regime': regime,
            'reason': reason,
            'spy_price': round(spy_price, 2) if spy_price else None,
            'spy_ma20': round(spy_ma20, 2) if spy_ma20 else None,
            'spy_ma50': round(spy_ma50, 2) if spy_ma50 else None,
            'spx_price': round(spx_price, 2) if spx_price else None,
            'spx_ma20': round(spx_ma20, 2) if spx_ma20 else None,
            'spx_ma50': round(spx_ma50, 2) if spx_ma50 else None,
            'ndx_price': round(ndx_price, 2) if ndx_price else None,
            'ndx_ma20': round(ndx_ma20, 2) if ndx_ma20 else None,
            'ndx_ma50': round(ndx_ma50, 2) if ndx_ma50 else None,
            'vix': round(vix, 1) if vix else None
        }

    except Exception as e:
        log(f"시장 국면 체크 실패: {e}", "ERROR")
        return default_result


# ============================================================
# Track 1 & 2 실행
# ============================================================

def run_screening(config, market_regime=None):
    """
    Track 1: 실시간 스크리닝 v6.1 - Value-Momentum Hybrid System (Option A)

    === 3-Layer Filtering ===

    0. Market Regime Check:
       🔴 RED: 스크리닝 즉시 중단 (SPY < MA50 OR VIX >= 30)
       🟡 YELLOW: 필터 강화 (Score 6.0, PEG 1.5)
       🟢 GREEN: 기본 필터 (Score 4.0, PEG 2.0)

    Layer 1 [Momentum]: EPS Trend Alignment
       - Kill Switch: EPS(Current) < EPS(7d) * 0.99 시 탈락
       - Score >= min_score (가중치 3-2-1 + 정배열 보너스)
       - EPS 정배열: Current > 7d > 30d

    Layer 2 [Quality]: ROE > 0.10 (10%)
       - 저품질 성장 필터링
       - 예외: ROE 데이터 없으면 통과 (Technical Rescue)

    Layer 3 [Safety]: Forward PER < 60.0
       - 버블 종목 제외
       - 예외: 매우 높은 모멘텀 점수(>=8)시 PER 80까지 허용

    === Hybrid Ranking (Option A) ===
    Score = (Momentum × 0.5) + ((100 / PER) × 0.2) + (가격위치 × 0.3)

    가격위치 = 100 - (현재가/52주고점 × 100)
    - 고점 근처: 낮은 점수 (비쌈)
    - 고점에서 멀리: 높은 점수 (쌈)

    목표: "좋은 사과를 싸게" - A등급 싼 종목 > S등급 비싼 종목
    """
    import pandas as pd

    log("Track 1: 실시간 스크리닝 v6.3 (Quality & Value Scorecard) 시작")

    # === 시장 국면에 따른 동적 필터링 ===
    regime = market_regime.get('regime', 'GREEN') if market_regime else 'GREEN'
    reason = market_regime.get('reason', '') if market_regime else ''

    # 🔴 RED: 경고만 표시하고 스크리닝 진행 (가장 강화된 필터 적용)
    if regime == 'RED':
        log(f"🔴 시장 위험 경고! {reason}", "WARN")
        log(f"🔴 스크리닝은 계속 진행하되, 최고 수준 필터 적용 (Score >= 8.0, PEG < 1.0)")
        min_score = 8.0  # 가장 엄격한 필터
        max_peg = 1.0    # 가장 엄격한 필터

    # 🟡 YELLOW: 필터 강화
    elif regime == 'YELLOW':
        min_score = 6.0  # 4.0 → 6.0 (강화)
        max_peg = 1.5    # 2.0 → 1.5 (강화)
        log(f"🟡 경계 모드! 필터 강화: Score >= {min_score}, PEG < {max_peg}")
    # 🟢 GREEN: 기본 필터
    else:
        min_score = config.get('min_score', 4.0)
        max_peg = 2.0
        log(f"🟢 정상 모드: Score >= {min_score}, PEG < {max_peg}")

    try:
        import yfinance as yf
        import numpy as np

        from eps_momentum_system import (
            INDICES, SECTOR_MAP,
            calculate_momentum_score_v3, calculate_slope_score,
            check_technical_filter, get_peg_ratio,
            calculate_forward_per, get_roe, calculate_peg_from_growth,
            calculate_hybrid_score, calculate_price_position_score,
            get_action_multiplier, calculate_actionable_score,
            calculate_quality_score, calculate_value_score,
            # v7.0 신규 함수
            calculate_atr, calculate_stop_loss, forward_fill_eps,
            super_momentum_override, check_trend_exit
        )

        today = datetime.now().strftime('%Y-%m-%d')

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
            'max_peg_used': max_peg,
            # v6.0 추가 통계
            'low_roe': 0,           # Layer 2: ROE < 10% 탈락
            'high_per': 0,          # Layer 3: PER > 60 탈락
            'avg_fwd_per': 0,       # 통과 종목 평균 Forward PER
            'avg_roe': 0,           # 통과 종목 평균 ROE
            # v7.0 Sell Signal 지원
            'killed_tickers': [],    # Kill Switch 발동 종목 리스트
            'trend_exit_tickers': [],  # 추세 이탈 종목 리스트
        }

        processed = 0
        for ticker, idx_name in all_tickers.items():
            processed += 1
            if processed % 50 == 0:
                log(f"  진행: {processed}/{len(all_tickers)} 종목 처리 중...")
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

                # v7.0: Forward Fill (결측치 보정)
                is_filled = False
                if pd.notna(current):
                    d7_filled, d30_filled, d60_filled, is_filled = forward_fill_eps(current, d7, d30, d60)
                    d7, d30, d60 = d7_filled, d30_filled, d60_filled

                # === FILTER 2: Kill Switch (1% 하락시 탈락) ===
                if pd.notna(current) and pd.notna(d7) and d7 != 0:
                    if current < d7 * 0.99:  # 1% 이상 하락
                        stats['killed'] += 1
                        stats['killed_tickers'].append(ticker)
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
                ma_50 = hist_1y['Close'].tail(50).mean() if len(hist_1y) >= 50 else None  # v7.0
                ma_60 = hist_1y['Close'].tail(60).mean() if len(hist_1y) >= 60 else None
                ma_200 = hist_1y['Close'].tail(200).mean() if len(hist_1y) >= 200 else None

                # v7.0: ATR 및 손절가 계산
                exit_config = config.get('exit_strategy', {})
                atr_period = exit_config.get('atr_period', 14)
                atr_multiplier = exit_config.get('atr_multiplier', 2.0)
                atr = calculate_atr(hist_1m, period=atr_period)
                stop_loss = calculate_stop_loss(price, atr, multiplier=atr_multiplier)

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

                # === v6.0: Value-Momentum 지표 계산 ===
                fwd_per = calculate_forward_per(price, current)
                roe = get_roe(info)
                peg_calculated = calculate_peg_from_growth(fwd_per, eps_chg) if eps_chg else None

                # === LAYER 2 [Quality]: ROE > 10% ===
                # 예외: ROE 데이터 없으면 통과 (Technical Rescue 대상)
                roe_threshold = 0.10  # 10%
                if roe is not None and roe < roe_threshold:
                    stats['low_roe'] += 1
                    continue

                # === LAYER 3 [Safety]: Forward PER < 60 ===
                # 예외: 매우 높은 모멘텀(score >= 8)이면 PER 80까지 허용
                per_threshold = 60.0
                per_exception_threshold = 80.0
                if fwd_per is not None:
                    if score_321 >= 8.0:
                        # 높은 모멘텀 예외: PER 80까지 허용
                        if fwd_per > per_exception_threshold:
                            stats['high_per'] += 1
                            continue
                    else:
                        # 일반: PER 60 제한
                        if fwd_per > per_threshold:
                            stats['high_per'] += 1
                            continue

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

                # A. Quality Growth: Rev >= 10% AND Op > Rev (v7.1 강화)
                is_quality_growth = False
                if rev_growth is not None and op_growth is not None:
                    if rev_growth >= 10 and op_growth > rev_growth:
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

                # v6.3: 거래량 스파이크 감지 (최근 3일 중 20일 평균 × 1.5 초과)
                volume_spike = False
                if len(hist_1m) >= 20:
                    vol_avg_20 = hist_1m['Volume'].tail(20).mean()
                    vol_recent_3 = hist_1m['Volume'].tail(3)
                    if any(vol_recent_3 > vol_avg_20 * 1.5):
                        volume_spike = True

                # v6.3: 실적 발표 D-Day 계산
                earnings_dday = None
                try:
                    calendar = stock.calendar
                    if calendar is not None and 'Earnings Date' in calendar:
                        earnings_date = calendar['Earnings Date']
                        if isinstance(earnings_date, (list, tuple)):
                            earnings_date = earnings_date[0]
                        if earnings_date:
                            earnings_dday = (earnings_date.date() - datetime.now().date()).days
                except:
                    pass

                # Action 결정 (52주 고점 대비 위치 + 거래량 스파이크 포함)
                action = get_action_label(price, ma_20, ma_200, rsi, from_52w_high, volume_spike)

                # v7.0: Industry 정보
                industry = info.get('industry', '')

                # v6.1: Hybrid Score 계산 (Option A - 가격위치 포함)
                # 52주 고점 계산
                high_52w = None
                if len(hist_1y) > 50:
                    high_52w = hist_1y['High'].max()

                # 가격위치 점수 계산
                price_position_score = calculate_price_position_score(price, high_52w)

                # Hybrid Score = Momentum×0.5 + Value×0.2 + Position×0.3
                hybrid_score = calculate_hybrid_score(score_321, fwd_per, price_position_score)

                # v7.1: 기간별 EPS 변화율 계산
                eps_chg_7d = ((current - d7) / d7 * 100) if (d7 and d7 != 0) else None
                eps_chg_30d = ((current - d30) / d30 * 100) if (d30 and d30 != 0) else None
                eps_chg_60d = ((current - d60) / d60 * 100) if (d60 and d60 != 0) else None
                eps_chg_90d = ((current - d90) / d90 * 100) if (d90 and d90 != 0) else None

                # v7.1: Quality Score (품질) 계산 - EPS 모멘텀 집중 (50점 만점)
                above_ma200 = ma_200 is not None and price > ma_200
                roe_pct = roe * 100 if roe else 0
                quality_score, _ = calculate_quality_score(
                    is_aligned, roe_pct, eps_chg, above_ma200, volume_spike, score_321,
                    eps_chg_7d, eps_chg_30d, eps_chg_60d, eps_chg_90d
                )

                # v7.1: Value Score (가격) 계산 - 진입 타이밍 평가 (50점 만점)
                value_score, value_label = calculate_value_score(
                    peg_calculated, fwd_per, from_52w_high, rsi, volume_spike
                )

                # v7.1: 총점 기준 등급 산정 (100점 만점)
                # 밸류 100점, 가격 100점 각각 50%씩 반영
                total_score = (quality_score * 0.5) + (value_score * 0.5)
                if total_score >= 70:
                    quality_grade = 'S급'
                elif total_score >= 60:
                    quality_grade = 'A급'
                elif total_score >= 50:
                    quality_grade = 'B급'
                else:
                    quality_grade = 'C급'

                # v7.0: Super Momentum Override (Quality >= 35 + RSI 70-85 → 돌파매수)
                # 품질 50점 만점 기준으로 35점 이상 (구 80/120 = 신 33/50)
                action = super_momentum_override(quality_score, rsi, action, config)

                # v7.1: Actionable Score = total_score × Action Multiplier
                action_multiplier = get_action_multiplier(action, config)
                actionable_score_v63 = round(total_score * action_multiplier, 2)

                # v6.3: Fake Bottom 감지 (RSI 낮지만 MA200 아래)
                fake_bottom = False
                if rsi is not None and rsi < 40 and ma_200 is not None and price < ma_200:
                    fake_bottom = True

                # 종목명 가져오기
                company_name = info.get('shortName', '') or info.get('longName', ticker)

                # v7.1: 당일 등락률 계산
                price_change_pct = None
                if len(hist_1m) >= 2:
                    prev_close = hist_1m['Close'].iloc[-2]
                    if prev_close and prev_close != 0:
                        price_change_pct = ((price - prev_close) / prev_close) * 100

                candidates.append({
                    'ticker': ticker,
                    'company_name': company_name,
                    'index': idx_name,
                    'score_321': round(score_321, 1),
                    'score_slope': round(score_slope, 1) if score_slope else None,
                    'eps_chg_60d': round(eps_chg, 1) if eps_chg else None,
                    # v7.1: 기간별 EPS 변화율 저장
                    'eps_chg_7d': round(eps_chg_7d, 1) if eps_chg_7d else None,
                    'eps_chg_30d': round(eps_chg_30d, 1) if eps_chg_30d else None,
                    'eps_chg_90d': round(eps_chg_90d, 1) if eps_chg_90d else None,
                    # v7.1: 당일 등락률
                    'price_change_pct': round(price_change_pct, 2) if price_change_pct else None,
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
                    # v6.0 신규 필드
                    'fwd_per': round(fwd_per, 1) if fwd_per else None,
                    'roe': round(roe * 100, 1) if roe else None,  # % 단위로 저장
                    'peg_calculated': round(peg_calculated, 2) if peg_calculated else None,
                    'hybrid_score': round(hybrid_score, 2) if hybrid_score else None,
                    # v6.1 신규 필드 (Option A)
                    'price_position_score': round(price_position_score, 1) if price_position_score else None,
                    'high_52w': round(high_52w, 2) if high_52w else None,
                    # v6.2 신규 필드 (Action Multiplier)
                    'action_multiplier': action_multiplier,
                    'actionable_score': calculate_actionable_score(hybrid_score, action),
                    # v7.1 신규 필드 (Quality & Value Scorecard - 100점 만점)
                    'quality_score': round(quality_score, 1),
                    'value_score': round(value_score, 1),
                    'total_score': round(total_score, 1),
                    'quality_grade': quality_grade,
                    'value_label': value_label,
                    'actionable_score_v63': actionable_score_v63,
                    'volume_spike': volume_spike,
                    'earnings_dday': earnings_dday,
                    'fake_bottom': fake_bottom,
                    # v7.0 신규 필드 (Exit Strategy + Super Momentum)
                    'atr': round(atr, 2) if atr else None,
                    'stop_loss': round(stop_loss, 2) if stop_loss else None,
                    'ma_50': round(ma_50, 2) if ma_50 else None,
                    'industry': industry,
                    'is_filled': 1 if is_filled else 0,
                })
                stats['passed'] += 1

            except Exception as e:
                stats['data_error'] += 1
                # 에러 로그 (너무 많으면 첫 10개만)
                if stats['data_error'] <= 10:
                    log(f"  {ticker} 데이터 에러 (skip): {str(e)[:100]}", "DEBUG")
                continue

        # 결과 저장
        df = pd.DataFrame(candidates)
        if not df.empty:
            # v6.3: Actionable Score v6.3으로 정렬 (Quality + Value + Action Multiplier)
            df = df.sort_values('actionable_score_v63', ascending=False)

            # v6.0 통계 계산
            if 'fwd_per' in df.columns:
                valid_per = df['fwd_per'].dropna()
                stats['avg_fwd_per'] = round(valid_per.mean(), 1) if len(valid_per) > 0 else 0
            if 'roe' in df.columns:
                valid_roe = df['roe'].dropna()
                stats['avg_roe'] = round(valid_roe.mean(), 1) if len(valid_roe) > 0 else 0

            csv_path = DATA_DIR / f'screening_{today}.csv'
            df.to_csv(csv_path, index=False)
            log(f"Track 1 완료: {len(df)}개 종목 -> {csv_path}")
            log(f"  평균 Forward PER: {stats['avg_fwd_per']}, 평균 ROE: {stats['avg_roe']}%")
        else:
            log("Track 1: 조건 충족 종목 없음", "WARN")

        return df, stats

    except Exception as e:
        log(f"Track 1 실패: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), {}


def get_action_label(price, ma_20, ma_200, rsi, from_52w_high=None, volume_spike=False):
    """
    실전 매매용 액션 레이블 v3 - RSI Momentum Strategy 추가

    핵심 원칙:
    - RSI 70 이상을 무조건 진입금지로 처리하지 않음
    - 신고가 돌파 + 거래량 동반 = Super Momentum (강력 매수)
    - RSI 85 이상만 진짜 과열

    === v3 변경사항: RSI Momentum Strategy ===

    Super Momentum 조건 (RSI 70-84):
    - 신고가 근처 (52주 고점 -5% 이내)
    - 거래량 스파이크 (20일 평균 1.5배 이상)
    - → 진입금지 대신 "🚀강력매수 (돌파)" 등급 부여

    Extreme Overbought (진짜 위험):
    - RSI >= 85 → "과열/진입금지"

    === 기존 액션 정의 ===

    1. 추세이탈: Price < MA200 (장기 하락 추세)
    2. 적극매수 (눌림목): 고점 -10~25% + RSI 35-55 + MA20 근처
    3. 저점매수 (반등): RSI < 35 + 고점 -20% 이상
    4. 매수적기 (추세): 정배열 + RSI 40-65
    5. 관망: 진입 애매
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

    # === 2. RSI 85 이상: 진짜 과열 (진입 금지) ===
    if rsi >= 85:
        return "진입금지 (극과열)"

    # === 3. RSI 70-84: Super Momentum 조건 체크 ===
    if 70 <= rsi < 85:
        # 신고가 근처 (-5% 이내) + 거래량 스파이크 = 강력 매수!
        is_near_ath = from_52w_high > -5
        if is_near_ath and volume_spike:
            return "🚀강력매수 (돌파)"
        # 신고가 근처이지만 거래량 미동반 = 관망
        elif is_near_ath:
            return "관망 (RSI🚀고점)"
        # 신고가 아니면 기존 로직 (과열 경계)
        else:
            return "관망 (RSI🚀)"

    # === 4. MA20 대비 +8% 이상 급등 (단기 과열) ===
    if ma20_pct >= 8:
        return "진입금지 (단기급등)"

    # === 5. 52주 고점 근처 (-5% 이내) - RSI 70 미만일 때 ===
    # (RSI 70 이상은 위에서 이미 처리됨)
    if from_52w_high > -5 and rsi >= 65:
        return "관망 (고점경계)"

    # === 6. 저점매수 (과매도 반등) ===
    if rsi <= 35 and from_52w_high <= -20:
        return "저점매수 (과매도)"

    # === 7. 적극매수 (진짜 눌림목) ===
    # 조건: 고점대비 조정폭 + RSI 중립 이하 + MA20 근처/아래
    is_meaningful_correction = -25 <= from_52w_high <= -10
    is_rsi_neutral = 35 <= rsi <= 55
    is_near_ma20 = ma20_pct <= 3  # MA20 근처 또는 아래

    if is_meaningful_correction and is_rsi_neutral and is_near_ma20:
        return "적극매수 (눌림목)"

    # === 8. 매수적기 (건강한 추세) ===
    # 조건: 정배열 + RSI 건강 + 상승 여력 있음
    is_aligned = ma_20 and ma_200 and price > ma_20 > ma_200
    is_rsi_healthy = 40 <= rsi <= 65
    has_upside = -15 <= from_52w_high <= -5

    if is_aligned and is_rsi_healthy and has_upside:
        return "매수적기 (추세)"

    # === 9. 관망 (진입 애매) ===
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
            calculate_momentum_score_v2, calculate_slope_score,
            calculate_forward_per, get_roe, calculate_peg_from_growth,
            calculate_hybrid_score, calculate_price_position_score
        )

        today = datetime.now().strftime('%Y-%m-%d')

        # DB 연결
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 테이블 생성 (없으면) - v6: Value-Momentum Hybrid 필드 추가
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
                fwd_per REAL,
                roe REAL,
                peg_calculated REAL,
                hybrid_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, ticker, period)
            )
        ''')

        # 새 컬럼 추가 (기존 테이블에) - v7 포함
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
            # v6.0 신규 컬럼
            ('fwd_per', 'REAL'),
            ('roe', 'REAL'),
            ('peg_calculated', 'REAL'),
            ('hybrid_score', 'REAL'),
            # v7.0 신규 컬럼 (Exit Strategy + Super Momentum)
            ('atr', 'REAL'),
            ('stop_loss', 'REAL'),
            ('action_type', 'TEXT'),
            ('industry', 'TEXT'),
            ('is_filled', 'INTEGER DEFAULT 0'),
            ('ma_50', 'REAL'),
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

        # 오늘 이미 수집된 종목 조회 (증분 수집)
        cursor.execute('SELECT ticker FROM eps_snapshots WHERE date = ?', (today,))
        already_collected = set(row[0] for row in cursor.fetchall())

        # 미수집 종목만 필터링
        tickers_to_collect = {t: idx for t, idx in all_tickers.items() if t not in already_collected}

        if already_collected:
            log(f"  이미 수집된 종목: {len(already_collected)}개 (스킵)")
        log(f"  신규 수집 대상: {len(tickers_to_collect)}개")

        if not tickers_to_collect:
            log("  오늘 데이터 이미 수집 완료")
            conn.close()
            return len(already_collected), 0

        collected = 0
        errors = 0

        for ticker, idx_name in tickers_to_collect.items():
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

                # v6.1: Value-Momentum 지표 계산 (Option A - 가격위치 포함)
                fwd_per = calculate_forward_per(price, eps_current)
                roe = get_roe(info)
                peg_calculated = calculate_peg_from_growth(fwd_per, eps_chg_60d) if eps_chg_60d else None

                # 52주 고점에서 가격위치 점수 계산
                hist_1y = stock.history(period='1y')
                high_52w = hist_1y['High'].max() if len(hist_1y) > 50 else None
                price_position_score = calculate_price_position_score(price, high_52w)

                # Hybrid Score = Momentum×0.5 + Value×0.2 + Position×0.3
                hybrid_score = calculate_hybrid_score(score_321, fwd_per, price_position_score)

                # DB 저장 (v6 확장 필드 포함)
                cursor.execute('''
                    INSERT OR REPLACE INTO eps_snapshots
                    (date, ticker, index_name, period, eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                     price, volume, dollar_volume, market_cap, sector, ma_20, above_ma20,
                     score_321, score_slope, eps_chg_60d, passed_screen,
                     is_aligned, is_undervalued, is_growth, peg, forward_pe, from_52w_high, rsi,
                     rev_growth_yoy, op_growth_yoy, fwd_per, roe, peg_calculated, hybrid_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (today, ticker, idx_name, '+1y',
                      eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                      price, avg_volume, dollar_volume, market_cap, sector,
                      ma_20, above_ma20, score_321, score_slope, eps_chg_60d, passed_screen,
                      is_aligned, is_undervalued, is_growth, peg, forward_pe, from_52w_high, rsi,
                      rev_growth_yoy, op_growth_yoy, fwd_per, roe, peg_calculated, hybrid_score))

                collected += 1

                if collected % 50 == 0:
                    conn.commit()
                    log(f"  진행: {collected}/{len(all_tickers)}")

            except Exception as e:
                errors += 1
                continue

        conn.commit()
        conn.close()

        total_in_db = len(already_collected) + collected
        log(f"Track 2 완료: {collected}개 신규수집, {len(already_collected)}개 스킵 (DB총 {total_in_db}개), {errors}개 오류")
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
    md_content = f"""# EPS Momentum v6.1 Daily Report
## Value-Momentum Hybrid System (Option A)
**Date:** {today_time}
**Formula:** Hybrid = Momentum×0.5 + Value×0.2 + Position×0.3

## Summary
| Metric | Value |
|--------|-------|
| Total Scanned | {stats.get('total', 0)} |
| Passed Screening | {stats.get('passed', 0)} |
| Kill Switch | {stats.get('killed', 0)} |
| No EPS Data | {stats.get('no_eps', 0)} |
| Low Volume | {stats.get('low_volume', 0)} |
| Low ROE (<10%) | {stats.get('low_roe', 0)} |
| High PER (>60) | {stats.get('high_per', 0)} |
| Earnings Blackout | {stats.get('earnings_blackout', 0)} |

## v6.0 Value Metrics
| Metric | Value |
|--------|-------|
| Avg Forward PER | {stats.get('avg_fwd_per', 'N/A')} |
| Avg ROE | {stats.get('avg_roe', 'N/A')}% |

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
## Top 20 Candidates (Sorted by Hybrid Score)
| # | Ticker | Index | Hybrid | Momentum | Fwd PER | ROE% | EPS% | Price |
|---|--------|-------|--------|----------|---------|------|------|-------|
"""
    for i, (_, row) in enumerate(top_20.iterrows()):
        hybrid = row.get('hybrid_score', 0) or 0
        fwd_per = row.get('fwd_per', '-')
        roe = row.get('roe', '-')
        fwd_per_str = f"{fwd_per:.0f}" if isinstance(fwd_per, (int, float)) and fwd_per else "-"
        roe_str = f"{roe:.0f}" if isinstance(roe, (int, float)) and roe else "-"
        md_content += f"| {i+1} | {row['ticker']} | {row['index']} | {hybrid:.1f} | {row['score_321']:.1f} | {fwd_per_str} | {roe_str} | {row['eps_chg_60d']:+.1f}% | ${row['price']:.2f} |\n"

    # Markdown 저장
    md_path = REPORTS_DIR / f'report_{today}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    # ========== HTML 리포트 ==========
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>EPS Momentum v6.1 Report - {today}</title>
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
        <h1>EPS Momentum v6.1 Daily Report</h1>
        <p><strong>Value-Momentum Hybrid System (Option A)</strong></p>
        <p><strong>Formula:</strong> Hybrid = Momentum×0.5 + Value×0.2 + Position×0.3</p>
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

        <h2>Top 20 Candidates (by Hybrid Score)</h2>
        <table>
            <tr>
                <th>#</th>
                <th>Ticker</th>
                <th>Index</th>
                <th>Hybrid</th>
                <th>Momentum</th>
                <th>Fwd PER</th>
                <th>ROE%</th>
                <th>EPS%</th>
                <th>Price</th>
            </tr>
"""

    for i, (_, row) in enumerate(top_20.iterrows()):
        eps_class = 'positive' if row['eps_chg_60d'] > 0 else 'negative'
        hybrid = row.get('hybrid_score', 0) or 0
        fwd_per = row.get('fwd_per')
        roe = row.get('roe')
        fwd_per_str = f"{fwd_per:.0f}" if fwd_per else "-"
        roe_str = f"{roe:.0f}" if roe else "-"
        html_content += f"""
            <tr>
                <td>{i+1}</td>
                <td><strong>{row['ticker']}</strong></td>
                <td>{row['index']}</td>
                <td style="color: #007bff; font-weight: bold;">{hybrid:.1f}</td>
                <td>{row['score_321']:.1f}</td>
                <td>{fwd_per_str}</td>
                <td>{roe_str}</td>
                <td class="{eps_class}">{row['eps_chg_60d']:+.1f}%</td>
                <td>${row['price']:.2f}</td>
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


def generate_korean_rationale(row):
    """
    v6.3: 동적 한국어 추천 문구 생성

    종목의 특성에 따라 맞춤형 추천 이유를 생성합니다.
    v6.3: RSI 모멘텀 (돌파 매수) 해설 추가
    """
    action = row.get('action', '')
    rsi = row.get('rsi')
    from_high = row.get('from_52w_high')
    volume_spike = row.get('volume_spike', False)

    # === RSI 모멘텀 (돌파 매수) 특별 해설 ===
    if '🚀강력매수' in action:
        # 신고가 돌파 + 거래량 동반 = Super Momentum
        if volume_spike:
            return "신고가 돌파 + 거래량 폭발! 지금이 제일 쌉니다 (⚠️손절 -5% 필수)"
        else:
            return "신고가 돌파! 강한 매수세 지속 (⚠️손절 -5% 필수)"

    # RSI 70 이상이지만 관망인 경우
    if 'RSI🚀' in action:
        if rsi and rsi >= 70:
            return f"RSI {rsi:.0f} 과열이지만 상승세 강함, 거래량 확인 필요"

    # === 일반 해설 로직 ===
    parts = []

    # EPS 모멘텀 관련
    is_aligned = row.get('is_aligned', False)
    eps_chg = row.get('eps_chg_60d', 0)

    if is_aligned:
        parts.append("EPS 전망치 완전 정배열")
    elif eps_chg and eps_chg > 10:
        parts.append(f"EPS 전망 +{eps_chg:.0f}% 상향")
    elif eps_chg and eps_chg > 0:
        parts.append("EPS 전망 상향 추세")

    # Forward PER 관련
    fwd_per = row.get('fwd_per')
    if fwd_per:
        if fwd_per < 15:
            parts.append(f"PER {fwd_per:.0f}배 저평가")
        elif fwd_per < 25:
            parts.append(f"PER {fwd_per:.0f}배 적정")
        elif fwd_per < 40:
            parts.append(f"PER {fwd_per:.0f}배 성장주")

    # ROE 관련
    roe = row.get('roe')
    if roe:
        if roe > 30:
            parts.append(f"ROE {roe:.0f}% 고수익")
        elif roe > 20:
            parts.append(f"ROE {roe:.0f}% 우량")

    # Quality/Value 관련
    if row.get('is_quality_growth'):
        parts.append("매출+영업익 동반 성장")
    elif row.get('is_reasonable_value'):
        peg = row.get('peg')
        if peg:
            parts.append(f"PEG {peg:.1f}로 합리적")

    # 52주 고점 대비 (RSI 모멘텀이 아닌 경우)
    if from_high:
        if -15 <= from_high <= -5:
            parts.append("적절한 조정 후 반등 가능")
        elif from_high < -20:
            parts.append("큰 조정 후 저점 매수 기회")
        elif from_high > -3:
            parts.append("신고가 근처, 추세 강함")

    # 저점 매수 관련
    if '저점매수' in action:
        if rsi and rsi <= 35:
            return f"RSI {rsi:.0f} 과매도! 반등 기대"

    # 문장 조합
    if len(parts) >= 2:
        return f"{parts[0]}, {parts[1]}"
    elif len(parts) == 1:
        return parts[0]
    else:
        return "모멘텀 상승 중"


# ========================================
# v7.1 텔레그램 메시지 생성 함수들
# ========================================

def generate_rationale_bullets_v71(row):
    """
    v7.1: 선정이유를 불릿 포인트 리스트로 반환

    Returns:
        list: 2-3개의 선정이유 문자열 리스트
    """
    bullets = []

    quality_score = row.get('quality_score', 0)
    value_score = row.get('value_score', 0)
    rsi = row.get('rsi')
    from_high = row.get('from_52w_high')
    is_aligned = row.get('is_aligned', False)
    volume_spike = row.get('volume_spike', False)
    roe = row.get('roe')
    peg = row.get('peg')
    rev_growth = row.get('rev_growth')
    op_growth = row.get('op_growth')
    price_change = row.get('price_change_pct', 0)

    # 1. 밸류(품질) 관련
    if quality_score >= 80:
        if is_aligned:
            bullets.append(f"밸류 {quality_score:.0f}점 최상위 (EPS 정배열)")
        else:
            bullets.append(f"밸류 {quality_score:.0f}점 최상위")
    elif quality_score >= 60:
        bullets.append(f"밸류 {quality_score:.0f}점 우수")
    elif quality_score >= 40:
        bullets.append(f"밸류 {quality_score:.0f}점 (EPS 모멘텀 약함)")

    # 2. 펀더멘털 관련
    if roe and roe >= 50:
        bullets.append(f"ROE {roe:.0f}% 초고수익")
    elif roe and roe >= 30:
        bullets.append(f"ROE {roe:.0f}% 고수익")

    if op_growth and rev_growth:
        if op_growth > 100:
            bullets.append(f"영업익 +{op_growth:.0f}% 폭발 성장")
        elif op_growth > 50:
            bullets.append(f"영업익 +{op_growth:.0f}% 고성장")

    if peg and peg < 0.5:
        bullets.append(f"PEG {peg:.2f} 극저평가")
    elif peg and peg < 1.0:
        bullets.append(f"PEG {peg:.2f} 저평가")

    # 3. 가격/타이밍 관련
    if rsi and rsi <= 35:
        bullets.append(f"RSI {rsi:.0f} 과매도 → 반등 기회")
    elif rsi and rsi >= 70 and from_high and from_high > -3:
        bullets.append(f"신고가 돌파 모멘텀")
    elif rsi and 45 <= rsi <= 55:
        bullets.append(f"RSI {rsi:.0f} 중립 → 분할 진입 적기")

    if from_high and from_high > -3:
        bullets.append(f"52주 신고가 {from_high:+.0f}% 돌파 임박")
    elif from_high and from_high <= -20:
        bullets.append(f"52주 고점 대비 {from_high:.0f}% 대폭 할인")

    # 4. 당일 등락률 관련
    if price_change and price_change <= -5:
        bullets.append(f"{price_change:+.1f}% 급락 → 진입 기회")
    elif price_change and price_change >= 5:
        bullets.append(f"{price_change:+.1f}% 급등 (거래량 확인)")

    # 5. 거래량 스파이크
    if volume_spike:
        bullets.append("거래량 급증 (20일 평균 1.5배↑)")

    # 최소 2개, 최대 3개 반환
    if len(bullets) < 2:
        bullets.append("모멘텀 상승 추세")

    return bullets[:3]


def generate_risk_v71(row):
    """
    v7.1: 리스크 문구 자동 생성 (밸류/가격 약점 포함)

    Returns:
        str: 리스크 문구
    """
    risks = []

    rsi = row.get('rsi')
    from_high = row.get('from_52w_high')
    quality_score = row.get('quality_score', 0) or 0
    value_score = row.get('value_score', 0) or 0
    sector = row.get('sector', '')
    price_change = row.get('price_change_pct', 0) or 0

    # === 가격 측면 리스크 ===
    # RSI 과매수
    if rsi and rsi >= 75:
        risks.append(f"RSI {rsi:.0f} 과매수 주의")
    elif rsi and rsi >= 65:
        risks.append(f"RSI {rsi:.0f} 높음")

    # 52주 고점 근접 (조정 가능성)
    if from_high and from_high > -5:
        risks.append("고점 근접, 조정 가능")

    # 급락 후 안정 필요
    if price_change <= -7:
        risks.append(f"당일 {price_change:.1f}% 급락")
    elif price_change <= -5:
        risks.append(f"당일 {price_change:.1f}% 하락")

    # 가격 점수 낮음
    if value_score < 50:
        risks.append(f"가격점수 {value_score:.0f}점 (비쌈)")

    # === 밸류 측면 리스크 ===
    # 밸류 점수 낮음
    if quality_score < 50:
        risks.append(f"밸류 {quality_score:.0f}점 (EPS 모멘텀 약함)")
    elif quality_score < 65:
        risks.append(f"밸류 {quality_score:.0f}점 (보통)")

    # === 섹터별 리스크 (구체적 설명) ===
    sector_risks = {
        'Semiconductor': '반도체 수요 사이클 민감',
        'Technology': '금리 인상시 밸류에이션 부담',
        'Communication Services': '광고 시장 경기 민감',
        'Consumer Cyclical': '소비 심리 둔화시 타격',
        'Consumer Defensive': '성장성 제한적',
        'Industrials': '경기 침체시 수주 감소',
        'Basic Materials': '원자재 가격 변동 큼',
        'Energy': '유가 변동에 실적 연동',
        'Utilities': '금리 인상시 매력 감소',
        'Financial Services': '금리/부실채권 리스크',
        'Real Estate': '금리 인상/공실률 리스크',
        'Healthcare': 'FDA 승인/규제 불확실성',
    }
    if sector in sector_risks:
        risks.append(sector_risks[sector])

    # 기본 리스크 (아무것도 없으면)
    if not risks:
        risks.append("시장 전반 변동성")

    return ", ".join(risks[:2])


def get_recommendation_category_v71(row):
    """
    v7.1: 핵심 추천 카테고리 분류

    Returns:
        str: 카테고리 ('적극매수', '급락저가매수', '분할진입', '돌파확인', '조정대기', None)
    """
    quality_score = row.get('quality_score', 0)
    value_score = row.get('value_score', 0)
    rsi = row.get('rsi')
    from_high = row.get('from_52w_high')

    # 적극 매수: 밸류 70+ AND 가격 70+ AND RSI 적정
    if quality_score >= 70 and value_score >= 70 and rsi and 40 <= rsi <= 60:
        return '적극매수'

    # 급락 저가매수: 밸류 낮지만 가격 80+ (RSI 35 이하)
    if quality_score < 60 and value_score >= 80 and rsi and rsi <= 35:
        return '급락저가매수'

    # 분할 진입: 밸류 좋고 RSI 중립
    if quality_score >= 70 and rsi and 45 <= rsi <= 65:
        return '분할진입'

    # 돌파 확인 후: 신고가 근처
    if from_high and from_high > -3 and rsi and rsi >= 70:
        return '돌파확인'

    # 조정 대기: RSI 70+
    if rsi and rsi >= 70:
        return '조정대기'

    return None


def create_telegram_message_v71(screening_df, stats, config=None):
    """
    v7.1 텔레그램 메시지 생성 - 최종 형식

    포맷:
    - 헤더: 날짜, 시장 국면, 지수
    - 전략 설명
    - TOP 10: 순위 아이콘, 종목명(티커)업종, 가격, 점수, 진입타이밍, 선정이유(불릿), 리스크
    - 11-26위: 동일 형식
    - 순위 = 매수 우선순위 (별도 핵심추천 없음)
    """
    import pandas as pd
    from datetime import datetime

    # 날짜 계산: 인사말=한국날짜, 시장데이터=미국 최근 영업일
    dates = get_display_dates()
    kr_date = dates['kr_date']  # 인사말용 (2월6일)
    us_date = dates['us_date']  # 시장 데이터용 (2026년 02월 05일)

    total_count = len(screening_df)

    # 섹터 한국어 매핑
    sector_map = {
        'Semiconductor': '반도체', 'Technology': '기술', 'Tech': '기술',
        'Industrials': '산업재', 'Financial Services': '금융', 'Financial': '금융',
        'Healthcare': '헬스케어', 'Consumer Cyclical': '경기소비재',
        'Consumer Defensive': '필수소비재', 'Energy': '에너지',
        'Basic Materials': '소재', 'Real Estate': '부동산', 'Utilities': '유틸리티',
        'Communication Services': '통신', 'Consumer': '소비재', 'Other': '기타'
    }

    # 시장 국면
    market_regime = stats.get('market_regime', {})
    regime = market_regime.get('regime', 'GREEN') if market_regime else 'GREEN'
    ndx_price = market_regime.get('ndx_price') if market_regime else None
    ndx_ma50 = market_regime.get('ndx_ma50') if market_regime else None
    spx_price = market_regime.get('spx_price') if market_regime else None
    vix = market_regime.get('vix') if market_regime else None

    # 나스닥 등락률 계산 (추정)
    ndx_change = None
    if ndx_price and ndx_ma50:
        ndx_change = ((ndx_price - ndx_ma50) / ndx_ma50) * 100

    regime_emoji = {'RED': '🔴', 'YELLOW': '🟡', 'GREEN': '🟢'}.get(regime, '🟢')
    regime_text = {'RED': '하락장 (RED)', 'YELLOW': '경계 (YELLOW)', 'GREEN': '상승장 (GREEN)'}.get(regime, '상승장')

    total_scanned = stats.get('total', 917)

    # ========== 메시지 시작 ==========
    messages = []

    # === TOP 10 메시지 ===
    msg = f"안녕하세요! 오늘({kr_date}) 미국주식 EPS 모멘텀 포트폴리오입니다 📊\n\n"
    msg += "━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📅 {us_date} (미국장 기준)\n"
    msg += f"{regime_emoji} {regime_text}\n"

    if ndx_price:
        msg += f"• 나스닥 {ndx_price:,.0f}"
        if ndx_ma50 and ndx_price < ndx_ma50:
            msg += " ⚠️MA50 하회"
        msg += "\n"
    if spx_price:
        msg += f"• S&P500 {spx_price:,.0f}\n"
    if vix:
        vix_status = "정상" if vix < 20 else "경계" if vix < 30 else "공포"
        msg += f"• VIX {vix:.2f} ({vix_status})\n"

    msg += "━━━━━━━━━━━━━━━━━━━\n\n"

    # 전략 설명
    msg += "💡 전략 v7.1\n\n"
    msg += f"[1단계] 스크리닝: {total_scanned}개 → {total_count}개 통과 ({total_count/total_scanned*100:.1f}%)\n"
    msg += "• Kill Switch: FWD 1Y EPS가 7일 전 대비 1%↓ 시 제외\n"
    msg += "• EPS 상승 추세: 7일/30일/60일 가중 점수 4.0↑\n"
    msg += "• 성장 필터: 매출≥10% AND 영업익성장>매출성장\n\n"
    msg += "[2단계] 점수 산정 (총점 100점)\n"
    msg += "• 밸류 100점: EPS 모멘텀 기간별 + 정배열 보너스\n"
    msg += "• 가격 100점: RSI + 52주위치 + 거래량 + 신고가돌파\n"
    msg += "• 총점 = 밸류×50% + 가격×50%\n\n"

    # === 섹터 분석 (전체 통과 종목 기준) ===
    msg += "📊 섹터 분석\n"
    msg += "━━━━━━━━━━━━━━━━━━━\n"

    # industry 필드로 중분류 섹터 집계
    industry_col = 'industry' if 'industry' in screening_df.columns else 'sector'
    industry_counts = screening_df[industry_col].value_counts()

    # 상위 5개 섹터
    top_industries = industry_counts.head(5)

    # 업종 한국어 매핑 (단순화)
    industry_kr_map = {
        'Semiconductors': '반도체', 'Semiconductor Equipment & Materials': '반도체',
        'Computer Hardware': '하드웨어', 'Electronic Components': '전자부품',
        'Communication Equipment': '통신장비', 'Data Storage': '저장장치',
        'Biotechnology': '바이오', 'Drug Manufacturers - General': '제약',
        'Medical Devices': '의료기기', 'Medical Instruments & Supplies': '의료기기',
        'Medical Distribution': '의료유통', 'Diagnostics & Research': '헬스케어',
        'Gold': '금', 'Steel': '철강',
        'Oil & Gas Equipment & Services': '에너지',
        'Aerospace & Defense': '방산', 'Specialty Industrial Machinery': '산업기계',
        'Auto & Truck Dealerships': '자동차', 'Specialty Retail': '소매',
        'Luxury Goods': '명품', 'Personal Services': '서비스',
    }

    # 업종별 ETF 매핑
    industry_etf_map = {
        'Semiconductors': 'SMH/SOXL', 'Semiconductor Equipment & Materials': 'SMH/SOXL',
        'Computer Hardware': 'XLK/TECL', 'Electronic Components': 'XLK/TECL',
        'Communication Equipment': 'XLK', 'Data Storage': 'XLK',
        'Biotechnology': 'XBI/LABU', 'Drug Manufacturers - General': 'XLV/CURE',
        'Medical Devices': 'XLV', 'Medical Instruments & Supplies': 'XLV',
        'Gold': 'GDX/NUGT', 'Steel': 'XME',
        'Oil & Gas Equipment & Services': 'XLE/ERX',
        'Aerospace & Defense': 'ITA', 'Specialty Industrial Machinery': 'XLI',
        'Auto & Truck Dealerships': 'XLY', 'Specialty Retail': 'XRT', 'Luxury Goods': 'XLY',
    }

    # 주도 섹터 (1위가 2위보다 많을 때만 표시)
    if len(top_industries) >= 2:
        first_count = top_industries.iloc[0]
        second_count = top_industries.iloc[1]

        if first_count > second_count:
            leading_industry = top_industries.index[0]
            leading_pct = first_count / total_count * 100
            leading_kr = industry_kr_map.get(leading_industry, leading_industry[:8])
            leading_etf = industry_etf_map.get(leading_industry, '')
            etf_str = f" → {leading_etf}" if leading_etf else ""
            msg += f"🔥 주도섹터: {leading_kr}({leading_industry}) - {first_count}개 ({leading_pct:.0f}%){etf_str}\n\n"

    # 섹터별 분포 (한글+영문+ETF)
    msg += "📈 섹터별 분포:\n"
    for industry, count in top_industries.items():
        pct = count / total_count * 100
        industry_kr = industry_kr_map.get(industry, industry[:8])
        industry_etf = industry_etf_map.get(industry, '')
        etf_str = f" [{industry_etf}]" if industry_etf else ""
        msg += f"• {industry_kr}({industry}): {count}개 ({pct:.0f}%){etf_str}\n"

    msg += "\n"

    msg += "━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🏆 총점 기준 TOP 10 ({total_count}개 중 상위)\n"
    msg += "━━━━━━━━━━━━━━━━━━━\n"

    # 순위 아이콘
    def get_rank_icon(rank):
        if rank == 1:
            return "🥇"
        elif rank == 2:
            return "🥈"
        elif rank == 3:
            return "🥉"
        else:
            return "📌"

    # TOP 10 종목
    top_10 = screening_df.head(10)

    for idx, (_, row) in enumerate(top_10.iterrows(), 1):
        ticker = row['ticker']
        company = row.get('company_name', ticker)
        sector = row.get('sector', 'Other')
        sector_kr = sector_map.get(sector, sector[:4] if len(sector) > 4 else sector)
        price = row.get('price', 0)
        price_change = row.get('price_change_pct', 0)
        quality = row.get('quality_score', 0) or 0
        value = row.get('value_score', 0) or 0
        total = row.get('total_score') or (quality * 0.5 + value * 0.5)
        rsi = row.get('rsi')
        from_high = row.get('from_52w_high')

        icon = get_rank_icon(idx)
        change_str = f"({price_change:+.2f}%)" if price_change else ""

        msg += f"\n{icon} {idx}위 {company} ({ticker}) {sector_kr}\n"
        msg += f"💰 ${price:.2f} {change_str}\n"
        msg += f"📊 총 {total:.1f}점 = 밸류 {quality:.0f}점 + 가격 {value:.0f}점\n"

        rsi_str = f"RSI {rsi:.0f}" if rsi else "RSI -"
        high_str = f"52주 {from_high:+.0f}%" if from_high else "52주 -"
        msg += f"📈 진입타이밍: {rsi_str} | {high_str}\n"

        # 선정이유 (불릿 포인트)
        bullets = generate_rationale_bullets_v71(row)
        msg += "📝 선정이유:\n"
        for bullet in bullets:
            msg += f"• {bullet}\n"

        # 리스크
        risk = generate_risk_v71(row)
        msg += f"⚠️ 리스크: {risk}\n"
        msg += "━━━━━━━━━━━━━━━━━━━\n"

    msg += "\n💡 순위가 높을수록 매수 우선순위 높음\n"
    msg += "━━━━━━━━━━━━━━━━━━━\n"
    msg += "📊 EPS Momentum v7.1"

    messages.append(msg)

    # === 11-26위 메시지 (있으면) ===
    if total_count > 10:
        msg2 = f"📊 11-26위 종목 분석 (v7.1)\n\n"
        msg2 += "━━━━━━━━━━━━━━━━━━━\n"

        remaining = screening_df.iloc[10:26]
        for idx, (_, row) in enumerate(remaining.iterrows(), 11):
            ticker = row['ticker']
            company = row.get('company_name', ticker)
            sector = row.get('sector', 'Other')
            sector_kr = sector_map.get(sector, sector[:4] if len(sector) > 4 else sector)
            price = row.get('price', 0)
            price_change = row.get('price_change_pct', 0)
            quality = row.get('quality_score', 0) or 0
            value = row.get('value_score', 0) or 0
            total = row.get('total_score') or (quality * 0.5 + value * 0.5)
            rsi = row.get('rsi')
            from_high = row.get('from_52w_high')

            change_str = f"({price_change:+.2f}%)" if price_change else ""

            msg2 += f"📌 {idx}위 {company} ({ticker}) {sector_kr}\n"
            msg2 += f"💰 ${price:.2f} {change_str}\n"
            msg2 += f"📊 총 {total:.1f}점 = 밸류 {quality:.0f}점 + 가격 {value:.0f}점\n"

            rsi_str = f"RSI {rsi:.0f}" if rsi else "RSI -"
            high_str = f"52주 {from_high:+.0f}%" if from_high else "52주 -"
            msg2 += f"📈 진입타이밍: {rsi_str} | {high_str}\n"

            bullets = generate_rationale_bullets_v71(row)
            msg2 += "📝 선정이유:\n"
            for bullet in bullets:
                msg2 += f"• {bullet}\n"

            risk = generate_risk_v71(row)
            msg2 += f"⚠️ 리스크: {risk}\n"
            msg2 += "━━━━━━━━━━━━━━━━━━━\n"

        # 11-26위 주목 섹션
        msg2 += "\n📌 11-26위 중 주목\n\n"

        # 과매도 종목
        oversold = remaining[remaining['rsi'] <= 35] if 'rsi' in remaining.columns else pd.DataFrame()
        if len(oversold) > 0:
            msg2 += "✅ 과매도 반등 기회\n"
            for _, r in oversold.head(2).iterrows():
                r_total = r.get('total_score') or ((r.get('quality_score', 0) or 0) * 0.5 + (r.get('value_score', 0) or 0) * 0.5)
                msg2 += f"• {r['ticker']} (RSI{r['rsi']:.0f}) - {r_total:.1f}점\n"
            msg2 += "\n"

        # 방어주 (헬스케어, 유틸리티)
        defensive = remaining[remaining['sector'].isin(['Healthcare', 'Utilities', 'Consumer Defensive'])]
        if len(defensive) > 0:
            msg2 += "🛡️ 방어주\n"
            for _, r in defensive.head(2).iterrows():
                sector_kr = sector_map.get(r['sector'], r['sector'])
                msg2 += f"• {r['ticker']} - {sector_kr}\n"
            msg2 += "\n"

        msg2 += "━━━━━━━━━━━━━━━━━━━\n"
        msg2 += "📊 EPS Momentum v7.1"

        messages.append(msg2)

    return messages


def create_telegram_message_admin(stats, collected, errors, execution_time):
    """
    텔레그램 Admin 메시지 (Track 2) - 시스템 로그용

    Content:
    - DB 저장 상태 (Success/Fail)
    - 총 처리 티커 수
    - 실행 시간
    - v6.3 필터 통계
    """
    today = datetime.now().strftime('%m/%d %H:%M')

    msg = f"🔧 <b>[{today}] EPS v6.3 Admin Log</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # DB 저장 상태
    status = "✅ SUCCESS" if collected > 0 else "❌ FAILED"
    msg += f"📊 <b>Track 2 (Data Collection)</b>\n"
    msg += f"Status: {status}\n"
    msg += f"• 수집: {collected}개 종목\n"
    msg += f"• 오류: {errors}개\n"
    msg += f"• 실행시간: {execution_time:.1f}초\n\n"

    # Track 1 필터 통계
    msg += f"📈 <b>Track 1 (Screening) 통계</b>\n"
    msg += f"• 총 스캔: {stats.get('total', 0)}개\n"
    msg += f"• EPS 없음: {stats.get('no_eps', 0)}개\n"
    msg += f"• Kill Switch: {stats.get('killed', 0)}개\n"
    msg += f"• 점수부족: {stats.get('low_score', 0)}개\n"
    msg += f"• 거래량부족: {stats.get('low_volume', 0)}개\n"
    msg += f"• MA200↓: {stats.get('below_ma200', 0)}개\n"

    # v6 신규 통계
    msg += f"\n🆕 <b>v6.0 필터 통계</b>\n"
    msg += f"• ROE 10% 미만: {stats.get('low_roe', 0)}개\n"
    msg += f"• PER 60 초과: {stats.get('high_per', 0)}개\n"
    msg += f"• 평균 Forward PER: {stats.get('avg_fwd_per', 0)}\n"
    msg += f"• 평균 ROE: {stats.get('avg_roe', 0)}%\n"

    # DB 상태
    db_size = 0
    if DB_PATH.exists():
        db_size = DB_PATH.stat().st_size / (1024 * 1024)
    msg += f"\n💾 DB Size: {db_size:.1f}MB\n"

    msg += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"<i>🤖 EPS Momentum v6.0 Admin</i>"

    return msg


def get_stock_insight(ticker, max_chars=50):
    """
    yfinance에서 종목 인사이트(뉴스 헤드라인 또는 업종) 가져오기

    Args:
        ticker: 종목 티커
        max_chars: 최대 글자 수

    Returns:
        str: 뉴스 헤드라인 또는 업종 정보 (한국어)
    """
    import yfinance as yf

    # 업종 한국어 매핑
    industry_kr = {
        # Technology
        'Semiconductors': '반도체',
        'Semiconductor Equipment & Materials': '반도체 장비',
        'Software—Infrastructure': '인프라 소프트웨어',
        'Software—Application': '애플리케이션 소프트웨어',
        'Information Technology Services': 'IT 서비스',
        'Computer Hardware': '컴퓨터 하드웨어',
        'Electronic Components': '전자부품',
        'Consumer Electronics': '가전제품',
        'Communication Equipment': '통신장비',
        # Healthcare
        'Biotechnology': '바이오테크',
        'Drug Manufacturers—General': '대형 제약',
        'Drug Manufacturers—Specialty & Generic': '특수/제네릭 제약',
        'Medical Devices': '의료기기',
        'Medical Instruments & Supplies': '의료기기 및 소모품',
        'Diagnostics & Research': '진단 및 연구',
        'Health Information Services': '헬스케어 IT',
        'Healthcare Plans': '건강보험',
        # Financial
        'Banks—Diversified': '대형 은행',
        'Banks—Regional': '지역 은행',
        'Asset Management': '자산운용',
        'Capital Markets': '자본시장',
        'Insurance—Life': '생명보험',
        'Insurance—Property & Casualty': '손해보험',
        'Insurance—Diversified': '종합보험',
        'Credit Services': '신용서비스',
        'Financial Data & Stock Exchanges': '금융데이터/거래소',
        # Consumer
        'Internet Retail': '온라인 유통',
        'Specialty Retail': '전문 소매',
        'Home Improvement Retail': '홈임프루브먼트',
        'Auto Manufacturers': '자동차',
        'Auto Parts': '자동차 부품',
        'Restaurants': '레스토랑',
        'Apparel Retail': '의류 소매',
        'Apparel Manufacturing': '의류 제조',
        'Footwear & Accessories': '신발/액세서리',
        'Leisure': '레저',
        'Gambling': '게임/카지노',
        'Resorts & Casinos': '리조트/카지노',
        'Travel Services': '여행 서비스',
        'Lodging': '숙박',
        'Packaged Foods': '식품',
        'Beverages—Non-Alcoholic': '음료',
        'Beverages—Wineries & Distilleries': '주류',
        'Household & Personal Products': '생활용품',
        'Tobacco': '담배',
        # Industrials
        'Aerospace & Defense': '항공우주/방산',
        'Airlines': '항공',
        'Railroads': '철도',
        'Trucking': '트럭운송',
        'Integrated Freight & Logistics': '물류',
        'Marine Shipping': '해운',
        'Electrical Equipment & Parts': '전기장비',
        'Industrial Distribution': '산업재 유통',
        'Specialty Industrial Machinery': '특수 산업기계',
        'Farm & Heavy Construction Machinery': '건설/농업기계',
        'Metal Fabrication': '금속가공',
        'Building Products & Equipment': '건축자재',
        'Engineering & Construction': '엔지니어링/건설',
        'Consulting Services': '컨설팅',
        'Staffing & Employment Services': '인력서비스',
        'Waste Management': '폐기물관리',
        'Rental & Leasing Services': '렌탈/리스',
        'Security & Protection Services': '보안 서비스',
        'Conglomerates': '복합기업',
        # Energy
        'Oil & Gas Integrated': '종합 에너지',
        'Oil & Gas E&P': '원유/가스 탐사',
        'Oil & Gas Midstream': '원유/가스 중류',
        'Oil & Gas Refining & Marketing': '정유',
        'Oil & Gas Equipment & Services': '에너지 장비/서비스',
        'Uranium': '우라늄',
        # Basic Materials
        'Gold': '금',
        'Silver': '은',
        'Copper': '구리',
        'Steel': '철강',
        'Aluminum': '알루미늄',
        'Specialty Chemicals': '특수화학',
        'Chemicals': '화학',
        'Agricultural Inputs': '농업투입재',
        'Building Materials': '건축자재',
        'Paper & Paper Products': '종이/제지',
        'Lumber & Wood Production': '목재',
        # Communication Services
        'Telecom Services': '통신서비스',
        'Entertainment': '엔터테인먼트',
        'Internet Content & Information': '인터넷/미디어',
        'Electronic Gaming & Multimedia': '게임/멀티미디어',
        'Advertising Agencies': '광고',
        'Broadcasting': '방송',
        'Publishing': '출판',
        # Real Estate
        'REIT—Residential': '주거용 리츠',
        'REIT—Retail': '리테일 리츠',
        'REIT—Industrial': '산업용 리츠',
        'REIT—Office': '오피스 리츠',
        'REIT—Healthcare Facilities': '헬스케어 리츠',
        'REIT—Specialty': '특수 리츠',
        'REIT—Diversified': '복합 리츠',
        'Real Estate Services': '부동산 서비스',
        # Utilities
        'Utilities—Regulated Electric': '규제 전력',
        'Utilities—Diversified': '복합 유틸리티',
        'Utilities—Renewable': '신재생 에너지',
        'Utilities—Independent Power Producers': '독립 발전사',
    }

    def translate_to_korean(text, max_len=60):
        """영어 텍스트를 한국어로 번역 (googletrans 사용)"""
        try:
            from googletrans import Translator
            import time
            translator = Translator()
            # 타임아웃 설정 및 재시도
            for attempt in range(2):
                try:
                    result = translator.translate(text, src='en', dest='ko')
                    translated = result.text
                    if len(translated) > max_len:
                        translated = translated[:max_len-3] + '...'
                    return translated
                except Exception:
                    if attempt == 0:
                        time.sleep(0.5)  # 첫 실패시 대기 후 재시도
                    continue
            # 두 번 모두 실패시 원문 반환
            if len(text) > max_len:
                text = text[:max_len-3] + '...'
            return text
        except Exception:
            # 번역 라이브러리 없을 시 원문 반환
            if len(text) > max_len:
                text = text[:max_len-3] + '...'
            return text

    try:
        stock = yf.Ticker(ticker)

        # 1차: 뉴스 헤드라인 시도 (한국어 번역)
        news = stock.news
        if news and len(news) > 0:
            content = news[0].get('content', {})
            if isinstance(content, dict):
                title = content.get('title', '')
                if title:
                    # 한국어로 번역
                    title_kr = translate_to_korean(title, max_chars)
                    return f"📰 {title_kr}"

        # 2차: 업종 정보 (한국어 변환)
        info = stock.info
        industry = info.get('industry', '')
        if industry:
            industry_korean = industry_kr.get(industry, industry)  # 매핑 없으면 원문
            return f"🏢 {industry_korean}"

        return None
    except Exception:
        return None


def create_telegram_message(screening_df, stats, changes=None, config=None):
    """
    텔레그램 User 메시지 (Track 1) v7.0 - EPS Growth + RSI Dual Track

    [헤더]
    - 날짜, 시장 국면 (GREEN/YELLOW/RED)
    - ETF 추천 (Sector Booster)

    [TOP 10 추천주]
    - 종합점수, 매수근거, 손절가(ATR×2)
    - Quality Score (맛) + Value Score (값)
    - 뉴스/업종 인사이트

    [후순위 종목]
    - TOP 10과 동일 포맷

    [Warnings]
    - 섹터 집중 경고
    - Fake Bottom 경고
    """
    import yfinance as yf
    import math
    import pandas as pd

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
    regime = market_regime.get('regime', 'GREEN') if market_regime else 'GREEN'
    reason = market_regime.get('reason', '') if market_regime else ''
    spy_price = market_regime.get('spy_price') if market_regime else None
    spy_ma20 = market_regime.get('spy_ma20') if market_regime else None
    spy_ma50 = market_regime.get('spy_ma50') if market_regime else None
    spx_price = market_regime.get('spx_price') if market_regime else None
    spx_ma20 = market_regime.get('spx_ma20') if market_regime else None
    spx_ma50 = market_regime.get('spx_ma50') if market_regime else None
    ndx_price = market_regime.get('ndx_price') if market_regime else None
    ndx_ma20 = market_regime.get('ndx_ma20') if market_regime else None
    ndx_ma50 = market_regime.get('ndx_ma50') if market_regime else None
    vix = market_regime.get('vix') if market_regime else None
    skipped = stats.get('skipped', False)

    # ========================================
    # 🔴 RED: 경고 메시지만 전송
    # ========================================
    if regime == 'RED' or skipped:
        msg = f"🚨 <b>[{today}] EPS 모멘텀 v6.3 - 시장 경고</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🚦 <b>시장 상태: 🔴 RED (위험)</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

        msg += f"🚨 <b>[경고] 시장 위험 감지</b>\n"
        msg += f"📍 사유: {reason}\n\n"

        msg += f"📊 <b>주요 지수 현황</b>\n"
        if spx_price and spx_ma50:
            msg += f"• S&P 500: {spx_price:.0f} (MA20: {spx_ma20:.0f}, MA50: {spx_ma50:.0f})\n"
        if ndx_price and ndx_ma50:
            msg += f"• 나스닥: {ndx_price:.0f} (MA20: {ndx_ma20:.0f}, MA50: {ndx_ma50:.0f})\n"
        if vix:
            msg += f"• VIX: {vix:.1f}\n"

        msg += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"⛔ <b>오늘의 추천 종목 없음</b>\n"
        msg += f"💵 <b>Cash is King</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

        msg += f"💡 <b>권장 액션</b>\n"
        msg += f"• 신규 매수 중단\n"
        msg += f"• 기존 포지션 점검\n"
        msg += f"• 현금 비중 확대\n\n"

        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"<i>🤖 EPS Momentum v7.0.5</i>\n"
        msg += f"<i>🔴 Market Regime: RED</i>\n"

        return msg

    # ========================================
    # 🟡 YELLOW / 🟢 GREEN 헤더
    # ========================================
    regime_emoji = {'YELLOW': '🟡', 'GREEN': '🟢'}[regime]
    regime_text = {'YELLOW': 'YELLOW (경계)', 'GREEN': 'GREEN (상승장)'}[regime]

    # 총 스캔 종목 수 (NASDAQ100 + S&P500 + S&P400 = 917개)
    total_scanned = stats.get('total', 917)

    msg = f"🇺🇸 <b>미국주식 퀀트 랭킹 v7.0.6</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📅 {today_full} 마감\n"
    msg += f"📊 <b>{total_scanned}개 중 {total_count}개 통과</b> ({total_count/total_scanned*100:.1f}%)\n"
    msg += f"🚦 <b>시장: {regime_emoji} {regime_text}</b>\n"

    # 주요 지수 표시
    indices_str = []
    if spx_price:
        indices_str.append(f"S&P500 {spx_price:.0f}")
    if ndx_price:
        indices_str.append(f"나스닥 {ndx_price:.0f}")
    if vix:
        indices_str.append(f"VIX {vix:.1f}")
    if indices_str:
        msg += f"📈 {' | '.join(indices_str)}\n"

    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # v7.0.6 전략 설명 섹션 (상세)
    msg += "<b>📋 전략: EPS Growth + RSI Dual Track</b>\n"
    msg += "<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"

    msg += "<b>💎 펀더멘털 (Quality 100점)</b>\n"
    msg += "• EPS 모멘텀 (30점): 컨센서스 상향 추세\n"
    msg += "  └ Current &gt; 7일전 &gt; 30일전 (정배열)\n"
    msg += "• ROE 품질 (25점): 30%+ / 20%+ / 10%+\n"
    msg += "• EPS 성장률 (20점): 20%+ / 10%+ / 5%+\n"
    msg += "• 추세 (15점): MA200 위 = 상승 추세\n"
    msg += "• 거래량 (10점): 20일 평균 × 1.5 돌파\n\n"

    msg += "<b>💰 타이밍 (Value 100점)</b>\n"
    msg += "• PEG 평가 (35점): &lt;1.0 / &lt;1.5 / &lt;2.0\n"
    msg += "• Forward PER (25점): &lt;15 / &lt;25 / &lt;40\n"
    msg += "• 52주 고점대비 (25점): -25% / -15% / -10%\n"
    msg += "• RSI 눌림목 (15점): 30-45 / 45-55\n\n"

    msg += "<b>🎯 종합점수 = (펀더멘털×0.5 + 타이밍×0.5) × 액션배수</b>\n\n"

    # v7.0.5: ETF 추천 섹션 (전체 종목 섹터 분석)
    from sector_analysis import get_sector_etf_recommendation, format_etf_recommendation_text
    # 전체 통과 종목 기준 섹터 분석 (config의 top_n 무시)
    etf_recommendations = get_sector_etf_recommendation(
        screening_df,
        top_n=len(screening_df),  # 전체 종목 분석
        min_count=3,
        config=None  # config의 top_n=10 설정 무시
    )
    if etf_recommendations:
        msg += f"🔥 <b>[HOT] 섹터 집중</b> (전체 {total_count}개 분석)\n"
        for rec in etf_recommendations[:3]:  # 상위 3개 섹터
            sector = rec['sector']
            count = rec['count']
            pct = rec['pct']
            etf_1x = rec.get('etf_1x', '-')
            etf_3x = rec.get('etf_3x', '-')
            sector_kr = sector_map.get(sector, sector)
            msg += f"👉 {sector_kr} {count}개({pct:.0f}%) → {etf_1x}"
            if etf_3x:
                msg += f"/{etf_3x}"
            msg += "\n"
        msg += "\n"

    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"

    # ========================================
    # 🏆 TOP 10 추천주 (v7.0)
    # ========================================
    if total_count > 0:
        top_n_config = config.get('telegram_format', {}).get('top_n', 10)
        msg += f"\n<b>🏆 TOP {min(top_n_config, total_count)} 추천주</b>\n"

        medal = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']
        top_count = min(top_n_config, total_count)
        for idx, (_, row) in enumerate(screening_df.head(top_count).iterrows()):
            ticker = row['ticker']
            company_name = row.get('company_name', '')
            price = row.get('price', 0)
            sector = row.get('sector', 'Other')
            action = row.get('action', '')
            rsi = row.get('rsi')
            from_52w_high = row.get('from_52w_high')
            is_aligned = row.get('is_aligned', False)

            # v6.3 신규 필드
            quality_score = row.get('quality_score', 0)
            quality_grade = row.get('quality_grade', '-')
            value_score = row.get('value_score', 0)
            value_label = row.get('value_label', '-')
            actionable_v63 = row.get('actionable_score_v63', 0)
            volume_spike = row.get('volume_spike', False)
            earnings_dday = row.get('earnings_dday')

            sector_kr = sector_map.get(sector, sector[:4])

            # 실적 D-Day 표시
            dday_str = ""
            if earnings_dday is not None and pd.notna(earnings_dday):
                if earnings_dday >= 0:
                    dday_str = f" | 실적D-{int(earnings_dday)}"
                else:
                    dday_str = f" | 실적D+{abs(int(earnings_dday))}"

            # 거래량 스파이크 표시
            spike_str = "📈" if volume_spike else ""

            # EPS 정배열 표시
            eps_aligned_str = "EPS↗" if is_aligned else ""

            # RSI, 고점대비 표시 (RSI 70+ 시 🚀 추가)
            if rsi and rsi >= 70:
                rsi_str = f"🚀RSI{rsi:.0f}"
            elif rsi:
                rsi_str = f"RSI{rsi:.0f}"
            else:
                rsi_str = "RSI-"
            high_str = f"고점{from_52w_high:.0f}%" if from_52w_high else ""

            # v7.0 신규 필드: 손절가, ATR
            stop_loss = row.get('stop_loss')
            atr = row.get('atr')

            msg += f"\n{'─' * 22}\n"
            msg += f"{medal[idx]} <b>{ticker}</b> ${price:.0f} {spike_str}\n"
            if company_name:
                msg += f"   {company_name}\n"

            # v7.0 Action 표시: TOP 10은 무조건 매수 (돌파 or 분할)
            # RSI 60+ OR 신고가 근처(-5%) → 돌파매수, 그 외 → 분할매수
            is_near_high = from_52w_high is not None and from_52w_high >= -5
            is_momentum = (rsi and rsi >= 60) or is_near_high
            display_action = "🚀돌파매수" if is_momentum else "🛡️분할매수"
            msg += f"   [<b>{display_action}</b>] 종합점수: <b>{actionable_v63:.1f}점</b>\n"

            # 매수근거 (EPS 성장 + RSI)
            eps_growth_str = "EPS↗" if is_aligned else "EPS-"
            msg += f"   • 📊매수근거: {eps_growth_str} + {rsi_str}\n"

            # 맛/값 스코어 + 합산점수
            q_score = round(quality_score, 1) if quality_score else 0
            v_score = round(value_score, 1) if value_score else 0
            combined_score = (q_score + v_score) / 2
            msg += f"   • 🍎맛: {q_score}점({quality_grade}) | 💰값: {v_score}점({value_label})\n"
            msg += f"   • 📊합산: <b>{combined_score:.1f}점</b>/100 (맛+값 평균)\n"

            # 손절가 표시 (v7.0 핵심)
            if stop_loss and atr:
                msg += f"   • 📉대응: 손절가 ${stop_loss:.1f} (ATR×2)\n"

            # 섹터, 고점대비, 실적D-Day
            msg += f"   • {sector_kr}"
            if high_str:
                msg += f" | {high_str}"
            msg += f"{dday_str}\n"

            # v7.0: yfinance 인사이트 (뉴스/업종)
            insight = get_stock_insight(ticker, max_chars=45)
            if insight:
                msg += f"   {insight}\n"

            # 동적 한국어 해설
            rationale = generate_korean_rationale(row)
            msg += f"   💡 <i>{rationale}</i>\n"

        # v7.0: 전체 종목 상세 표시 (11위~끝까지) - TOP 10과 동일 포맷
        if total_count > top_count:
            msg += f"\n{'─' * 22}\n"
            msg += f"<b>📋 후순위 종목 ({top_count+1}~{total_count}위)</b>\n"
            remaining = screening_df.iloc[top_count:]
            for idx, (_, row) in enumerate(remaining.iterrows(), top_count + 1):
                ticker = row['ticker']
                company_name = row.get('company_name', '')
                price = row.get('price', 0)
                sector = row.get('sector', 'Other')
                rsi = row.get('rsi')
                from_52w_high = row.get('from_52w_high')
                is_aligned = row.get('is_aligned', False)
                quality_score = row.get('quality_score', 0)
                quality_grade = row.get('quality_grade', '-')
                value_score = row.get('value_score', 0)
                value_label = row.get('value_label', '-')
                actionable_v63 = row.get('actionable_score_v63', 0)
                volume_spike = row.get('volume_spike', False)
                earnings_dday = row.get('earnings_dday')
                stop_loss = row.get('stop_loss')
                atr = row.get('atr')

                sector_kr = sector_map.get(sector, sector[:4])
                spike_str = "📈" if volume_spike else ""

                # RSI 표시
                if rsi and rsi >= 70:
                    rsi_str = f"🚀RSI{rsi:.0f}"
                elif rsi:
                    rsi_str = f"RSI{rsi:.0f}"
                else:
                    rsi_str = "RSI-"

                # 고점대비
                high_str = f"고점{from_52w_high:.0f}%" if from_52w_high else ""

                # 실적 D-Day
                dday_str = ""
                if earnings_dday is not None and pd.notna(earnings_dday):
                    if earnings_dday >= 0:
                        dday_str = f" | 실적D-{int(earnings_dday)}"
                    else:
                        dday_str = f" | 실적D+{abs(int(earnings_dday))}"

                # 맛+값 합산 점수 (100점 만점)
                q_score = round(quality_score, 1) if quality_score else 0
                v_score = round(value_score, 1) if value_score else 0
                combined_score = (q_score + v_score) / 2

                # 액션 결정 (TOP 10과 동일)
                is_near_high = from_52w_high is not None and from_52w_high >= -5
                is_momentum = (rsi and rsi >= 60) or is_near_high
                display_action = "🚀돌파매수" if is_momentum else "🛡️분할매수"

                msg += f"\n{'─' * 22}\n"
                msg += f"<b>#{idx} {ticker}</b> ${price:.0f} {spike_str}\n"
                if company_name:
                    msg += f"   {company_name}\n"
                msg += f"   [<b>{display_action}</b>] 종합: <b>{actionable_v63:.1f}점</b>\n"

                # 매수근거
                eps_growth_str = "EPS↗" if is_aligned else "EPS-"
                msg += f"   • 📊매수근거: {eps_growth_str} + {rsi_str}\n"

                # 맛/값 + 합산점수
                msg += f"   • 🍎맛: {q_score}점({quality_grade}) | 💰값: {v_score}점({value_label})\n"
                msg += f"   • 📊합산: <b>{combined_score:.1f}점</b>/100 (맛+값 평균)\n"

                # 손절가 표시
                if stop_loss and atr:
                    msg += f"   • 📉대응: 손절가 ${stop_loss:.1f} (ATR×2)\n"

                # 섹터, 고점대비, 실적D-Day
                msg += f"   • {sector_kr}"
                if high_str:
                    msg += f" | {high_str}"
                msg += f"{dday_str}\n"

                # v7.0: yfinance 인사이트 (상위 20개만 - 속도 최적화)
                if idx <= 20:
                    insight = get_stock_insight(ticker, max_chars=45)
                    if insight:
                        msg += f"   {insight}\n"

                # 동적 해설
                rationale = generate_korean_rationale(row)
                msg += f"   💡 <i>{rationale}</i>\n"

    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"

    # ========================================
    # ⚠️ 경고 섹션
    # ========================================
    warnings_section = []

    # 1. 섹터 집중 경고 (특정 섹터가 50% 이상)
    if not screening_df.empty:
        sector_counts = screening_df['sector'].value_counts()
        for sector, count in sector_counts.items():
            pct = count / total_count * 100
            if pct >= 50:
                sector_kr = sector_map.get(sector, sector)
                warnings_section.append(f"⚠️ 섹터집중: {sector_kr} {pct:.0f}% ({count}개)")

    # 2. Fake Bottom 경고 (RSI 낮지만 MA200 아래)
    fake_bottom_stocks = screening_df[screening_df.get('fake_bottom', False) == True] if 'fake_bottom' in screening_df.columns else []
    if len(fake_bottom_stocks) > 0:
        fake_tickers = fake_bottom_stocks['ticker'].tolist()[:5]
        warnings_section.append(f"⚠️ Fake Bottom 주의: {', '.join(fake_tickers)}")
        warnings_section.append("   (RSI 낮지만 MA200 아래 = 하락추세)")

    # 3. 거래량 스파이크 종목 알림
    spike_stocks = screening_df[screening_df.get('volume_spike', False) == True] if 'volume_spike' in screening_df.columns else []
    if len(spike_stocks) > 0:
        spike_tickers = spike_stocks['ticker'].tolist()[:5]
        warnings_section.append(f"📈 거래량 스파이크: {', '.join(spike_tickers)}")

    if warnings_section:
        msg += "\n<b>⚠️ 경고 & 알림</b>\n"
        for warning in warnings_section:
            msg += f"{warning}\n"
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"

    # ========================================
    # v7.0: 액션별 분포 제거 (모든 종목이 돌파/분할매수로 표시되므로 혼란 방지)
    # ========================================

    # ========================================
    # 포트폴리오 변경
    # ========================================
    added_list = changes.get('added', []) if changes else []
    removed_list = changes.get('removed', []) if changes else []

    if added_list or removed_list:
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>📋 전일 대비 변동</b>\n"
        if added_list:
            msg += f"🆕 편입({len(added_list)}): {', '.join(added_list[:8])}"
            if len(added_list) > 8:
                msg += f" +{len(added_list)-8}"
            msg += "\n"
        if removed_list:
            msg += f"🚫 편출({len(removed_list)}): {', '.join(removed_list[:8])}"
            if len(removed_list) > 8:
                msg += f" +{len(removed_list)-8}"
            msg += "\n"

    # ========================================
    # 필터 통계 (간략)
    # ========================================
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>📈 필터 결과</b>\n"
    msg += f"• 스캔: {stats.get('total', 0)} → 통과: {total_count}개\n"
    msg += f"• Kill: {stats.get('killed', 0)} | ROE필터: {stats.get('low_roe', 0)} | PER필터: {stats.get('high_per', 0)}\n"
    if stats.get('avg_fwd_per'):
        msg += f"• 평균 PER: {stats.get('avg_fwd_per')} | ROE: {stats.get('avg_roe', 0)}%\n"

    # ========================================
    # 🚨 v7.0 Sell Signal 섹션
    # ========================================
    sell_signals = []

    # Kill Switch 발동 종목 (stats에서 가져오기)
    killed_tickers = stats.get('killed_tickers', [])
    if killed_tickers:
        for ticker in killed_tickers[:5]:
            sell_signals.append(f"🔻 {ticker}: 펀더멘털 훼손 (EPS -1% 하향)")

    # 추세 이탈 종목 (stats에서 가져오기)
    trend_exit_tickers = stats.get('trend_exit_tickers', [])
    if trend_exit_tickers:
        for ticker_info in trend_exit_tickers[:5]:
            if isinstance(ticker_info, dict):
                ticker = ticker_info.get('ticker', '')
                ma_type = ticker_info.get('ma_type', 20)
                sell_signals.append(f"🔻 {ticker}: 기술적 이탈 (MA{ma_type} 붕괴)")
            else:
                sell_signals.append(f"🔻 {ticker_info}: 기술적 이탈")

    if sell_signals:
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>🚨 보유 종목 긴급 점검 (Sell Signal)</b>\n"
        for signal in sell_signals:
            msg += f"{signal}\n"

    # 푸터
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>🤖 EPS Momentum v7.0.5</i>\n"
    msg += "<i>맛(Quality) + 값(Value) = 실전점수</i>\n"
    if regime == 'YELLOW':
        msg += "<i>🟡 Caution Mode Active</i>\n"
    else:
        msg += "<i>🟢 Normal Mode</i>\n"

    return msg


def format_telegram_message(screening_df, stats, changes=None, config=None):
    """
    텔레그램 메시지 v7.1 형식으로 생성

    Returns:
        list: 메시지 리스트 (TOP 10, 11-26위 등)
    """
    # v7.1 형식 사용
    return create_telegram_message_v71(screening_df, stats, config)


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
    log("EPS Momentum Daily Runner v7.1 - 밸류+가격 100점 체계")
    log("=" * 60)

    start_time = datetime.now()

    # 설정 로드
    config = load_config()
    log(f"설정 로드 완료: {CONFIG_PATH}")

    # 시장 국면 체크
    market_regime = check_market_regime()

    # Track 1: 스크리닝 (시장 국면 전달)
    log("=" * 60)
    log("Track 1: 실시간 스크리닝 시작")
    log("=" * 60)
    screening_df, stats = run_screening(config, market_regime)

    # Track 1 리포트 생성
    changes = None
    if not screening_df.empty:
        md_path, html_path = generate_report(screening_df, stats, config)
        changes = get_portfolio_changes(screening_df, config)
        log(f"편입: {len(changes['added'])}개, 편출: {len(changes['removed'])}개")

    # Track 1 완료 → 텔레그램 User 메시지 즉시 전송 (v7.1)
    if config.get('telegram_enabled', False):
        if not screening_df.empty or stats.get('skipped', False):
            messages = format_telegram_message(screening_df, stats, changes, config)
            # v7.1: 메시지 리스트 순차 전송 (TOP 10, 11-26위 등)
            if isinstance(messages, list):
                for i, msg in enumerate(messages):
                    send_telegram_long(msg, config)
                    log(f"✅ 텔레그램 메시지 {i+1}/{len(messages)} 전송 완료")
            else:
                # 하위 호환: 단일 문자열
                send_telegram_long(messages, config)
                log("✅ 텔레그램 User 메시지 전송 완료")
            log("=" * 60)

    # Track 2: 데이터 축적 (User 메시지 전송 후 진행)
    log("Track 2: 전체 데이터 축적 시작")
    log("=" * 60)
    collected, errors = run_data_collection(config)

    # Git commit/push
    git_commit_push(config)

    # 실행 시간 계산
    elapsed = (datetime.now() - start_time).total_seconds()

    # Track 2 완료 → 텔레그램 Admin 메시지 전송
    if config.get('telegram_enabled', False):
        msg_admin = create_telegram_message_admin(stats, collected, errors, elapsed)
        send_telegram_long(msg_admin, config)
        log("✅ 텔레그램 Admin 메시지 전송 완료")

    # 완료
    log("=" * 60)
    log(f"✅ 전체 완료: {elapsed:.1f}초 소요")
    log("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
