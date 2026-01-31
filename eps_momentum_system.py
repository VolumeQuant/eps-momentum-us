"""
EPS Momentum System - Two Track Design (v3)
Track 1: 실시간 스크리닝 (모멘텀 기반 종목 선정)
Track 2: 데이터 축적 (백테스팅용 Point-in-Time 저장)

v3 개선사항:
- A/B 테스팅: 두 가지 스코어링 동시 저장
  - Score_321: 가중치 기반 (3-2-1)
  - Score_Slope: 변화율 가중 평균 (Gemini 제안)
- 3개월 후 어떤 로직이 더 효과적인지 검증 가능

v2 개선사항:
- 거래량 → 거래대금 필터 ($20M+)
- 모멘텀 점수 가중치 (최근 변화에 높은 가중치)
- Kill Switch (Current < 7d면 제외)
- 기술적 필터 (20일 이평선 위)
- Track 2: 전 종목 저장 (생존편향 방지)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# 설정
# ============================================================
DB_PATH = 'eps_momentum_data.db'
DATA_DIR = 'eps_data'

# 필터 설정
MIN_DOLLAR_VOLUME = 20_000_000  # 일 거래대금 $20M 이상
MIN_EPS_CHANGE_60D = 5.0        # 60일 EPS 변화율 Sweet Spot
MAX_PEG = 3.0                   # 최대 PEG
MAX_SECTOR_PCT = 0.30           # 섹터당 최대 30%

# 지수별 티커
INDICES = {
    'NASDAQ_100': [
        'AAPL','MSFT','AMZN','NVDA','GOOGL','GOOG','META','TSLA','AVGO','COST',
        'ASML','PEP','ADBE','NFLX','AZN','AMD','CSCO','TMUS','CMCSA','INTC',
        'INTU','TXN','QCOM','AMGN','HON','AMAT','ISRG','BKNG','SBUX','VRTX',
        'LRCX','GILD','ADI','REGN','MU','MDLZ','PANW','KLAC','SNPS','CDNS',
        'PYPL','MELI','CSX','MNST','MRVL','CTAS','ORLY','MAR','NXPI','ADP',
        'PCAR','WDAY','CHTR','ADSK','ROP','CPRT','CRWD','MCHP','ABNB','DXCM',
        'PAYX','KDP','AEP','EA','ROST','FAST','VRSK','ODFL','KHC','CTSH',
        'BKR','EXC','DLTR','GEHC','CEG','FANG','ON','DDOG','XEL','CSGP',
        'WBD','ILMN','TTWO','ZS','ALGN','TEAM','GFS','BIIB','FTNT','DASH','IDXX'
    ],
    'SP500': [
        'AAPL','MSFT','AMZN','NVDA','GOOGL','META','TSLA','BRK-B','UNH','XOM',
        'JNJ','JPM','V','PG','MA','HD','CVX','MRK','ABBV','LLY','PEP','KO',
        'AVGO','COST','WMT','MCD','CSCO','TMO','ACN','ABT','DHR','NEE','DIS',
        'VZ','ADBE','CRM','NKE','PM','WFC','TXN','BMY','UNP','COP','MS','RTX',
        'UPS','QCOM','ORCL','BA','AMD','HON','LOW','IBM','GE','CAT','INTC',
        'SBUX','AMAT','INTU','DE','ISRG','BLK','GS','PLD','NOW','MDLZ','AMGN',
        'ADI','SYK','GILD','ADP','TJX','BKNG','VRTX','REGN','MMC','LRCX','CI',
        'ZTS','CVS','SCHW','MO','CB','TMUS','BDX','BSX','CME','SO','DUK','ITW',
        'PGR','SLB','EOG','AON','NOC','ETN','SPGI','CL','MU','FDX','FCX','APD',
        'MCK','SNPS','HUM','CDNS','TGT','PSA','EQIX','ICE','NSC','EW','WM','EMR',
        'MPC','MAR','GD','OXY','MCO','KLAC','AZO','ROP','PH','NXPI','HCA','ORLY',
        'AEP','MCHP','SRE','CTAS','TRV','AIG','ECL','KMB','CMG','JCI','PAYX',
        'PCAR','GM','DXCM','CTSH','AFL','CARR','MSCI','F','EXC','SYY','GIS',
        'FTNT','WELL','A','D','TEL','PSX','AME','TFC','KMI','WMB','ROST','ON',
        'AMP','LHX','CNC','VLO','HSY','YUM','DOW','BK','ODFL','NEM','CTVA','RSG',
        'DLR','IDXX','MNST','VRSK','PPG','KEYS','EL','MLM','FAST','CHTR','STZ',
        'O','DHI','CEG','IR','PRU','ALL','WEC','BIIB','HPQ','GPN','XEL','ED',
        'HAL','CPRT','KHC','CMI','HLT','OTIS','ALB','GLW','EIX','GWW','CAH',
        'NUE','MTD','COF','CSGP','CDW','AJG','KDP','DG','IT','APTV','FANG','BKR',
        'RMD','SBAC','AVB','TROW','ARE','CINF','LUV','DLTR','AWK','FE','ES',
        'VICI','ILMN','PEG','TDY','PWR','LEN','MKC','TSCO','BBY','ZBH','WAT',
        'ROK','STE','HOLX','CHD','BAX','LYB','FTV','WTW','EFX','SWK','ACGL',
        'VMC','AXON','AMCR','TSN','ETSY','HIG','FITB','WRB','RJF','PPL','MTB'
    ],
    'SP400_MidCap': [
        'SAIA','WSC','RHP','RS','BWXT','KBR','AGCO','FLR','MTZ','EME','GVA',
        'DY','STRL','RUSHA','ARCB','GNRC','AIT','MOD','FUL','GGG','RBC','MIDD',
        'NDSN','DXPE','RRX','AYI','FOXF','LII','WTS','CFR','IPAR','RBA',
        'CWST','POOL','WSM','OXM','BOOT','SHOO','GES','TPR','DECK','CROX',
        'CAL','DBI','SCVL','HBI','SFM','GO','CHEF','POST','THS','INGR','JJSF',
        'CALM','WING','SHAK','DIN','TXRH','CAKE','EAT','BJRI','DENN',
        'PLAY','JACK','WEN','ARCO','EXLS','EPAM','GLOB','ACM','FTI','TTEK',
        'HUBB','J','CLH','EXPO','GHC','ATGE','SCI','CSV','MATX','PRIM','HTLD',
        'FND','BLDR','BLD','UFPI','WMS','TREX','AAON','TTC','FTV','ROK','ITT',
        'IEX','FELE','SNA','VIRT','SNEX','PIPR','EVR','HLI','MKTX','AVNT','HLIT',
        'INSP','LNTH','MED','MMSI','PRGO','SUPN','UTHR','XRAY','ABCB','BANF',
        'BANR','CASH','CBSH','CVBF','EFSC','FFBC','FFIN','FULT','GBCI','HOPE',
        'INDB','NBTB','NWBI','ONB','OZK','PEBO','SFBS','SFNC','TBBK','TOWN',
        'UBSI','WSBC','WSFS','WTFC','BKE','DKS','GCO','GIII','HVT','KTB','LZB',
        'PLCE','PVH','RVLV'
    ],
    'SP600_SmallCap': [
        # Financial Services
        'CUBI','HOMB','FIBK','SBCF','BUSE','PPBI','WAFD','TCBK','BY','HTBK',
        'HAFC','RNST','SBSI','EGBN','FCF','BHLB','UVSP','SASR','CFFN','FRME',
        'TRMK','CTBI','NBHC','HBT','GSBC','CNOB','MVBF','PFIS','OSBC','BRKL',
        # Industrials
        'GBX','ATKR','WERN','JBLU','ARCB','PATK','SNDR','RXO','HUBG','MATW',
        'TNC','APOG','AWI','GVA','NX','TILE','ROCK','IIIN','DOOR','ASTE',
        'KALU','SXI','POWL','AIMC','NPO','CXT','HNI','MLKN','KMT','SPXC',
        # Consumer
        'SHOO','SCVL','HIBB','CAL','CHS','BOOT','DBI','GIII','OXM','SCHL',
        'JILL','LE','DXLG','CATO','TLYS','EXPR','PRTY','CPRI','ZUMZ','BLMN',
        'LOCO','TXRH','CAKE','BJRI','DENN','EAT','DIN','JACK','PLAY','KRUS',
        # Technology
        'PLUS','CAMT','ICHR','ACLS','COHU','AEIS','LSCC','RMBS','AOSL','POWI',
        'DIOD','SMTC','SLAB','VSH','OSIS','SGH','CLS','SANM','BHE','NTCT',
        # Healthcare
        'ACHC','ADUS','AMED','ENSG','PNTG','USPH','SEM','NHC','CCRN','HCSG',
        'PDCO','HSIC','OMI','PRGO','SUPN','PAHC','PCRX','IRWD','XERS','AMPH',
        # Energy
        'PTEN','HP','LBRT','OII','WHD','VTOL','NR','AROC','TUSK','BOOM',
        'SOC','PUMP','REI','REPX','TTI','RES','SND','USAC','HESM','DK',
        # Materials
        'RYAM','CLW','SLVM','ITE','TROX','KWR','CBT','KOP','SXT','IOSP',
        'HAYN','IPAR','FUL','NGVT','GCP','WOR','KRA','HUN','MERC','BCPC',
        # Real Estate
        'UE','ALEX','GTY','ESRT','DEI','PGRE','BDN','JBGS','CLI','OLP',
        'GOOD','AIV','APLE','SHO','RLJ','DRH','PEB','INN','CLDT','AHH',
        # Utilities
        'MGEE','NWE','AVA','SJW','MSEX','CWT','ARTNA','UTL','CPK','BKH'
    ],
    'DOW30': [
        'AAPL','AMGN','AXP','BA','CAT','CRM','CSCO','CVX','DIS','DOW',
        'GS','HD','HON','IBM','INTC','JNJ','JPM','KO','MCD','MMM',
        'MRK','MSFT','NKE','PG','TRV','UNH','V','VZ','WBA','WMT'
    ]
}

# 섹터 매핑
SECTOR_MAP = {
    'NVDA': 'Semiconductor', 'AMD': 'Semiconductor', 'INTC': 'Semiconductor',
    'MU': 'Semiconductor', 'AVGO': 'Semiconductor', 'QCOM': 'Semiconductor',
    'AMAT': 'Semiconductor', 'LRCX': 'Semiconductor', 'KLAC': 'Semiconductor',
    'ASML': 'Semiconductor', 'MRVL': 'Semiconductor', 'NXPI': 'Semiconductor',
    'MCHP': 'Semiconductor', 'ADI': 'Semiconductor', 'TXN': 'Semiconductor',
    'AAPL': 'Tech', 'MSFT': 'Tech', 'GOOGL': 'Tech', 'GOOG': 'Tech',
    'META': 'Tech', 'AMZN': 'Tech', 'TSLA': 'Consumer', 'NFLX': 'Tech',
    'CRM': 'Tech', 'ADBE': 'Tech', 'NOW': 'Tech', 'INTU': 'Tech',
    'LLY': 'Healthcare', 'UNH': 'Healthcare', 'JNJ': 'Healthcare',
    'MRK': 'Healthcare', 'ABBV': 'Healthcare', 'PFE': 'Healthcare',
    'REGN': 'Healthcare', 'VRTX': 'Healthcare', 'GILD': 'Healthcare',
    'JPM': 'Financial', 'BAC': 'Financial', 'WFC': 'Financial',
    'GS': 'Financial', 'MS': 'Financial', 'BLK': 'Financial',
    'XOM': 'Energy', 'CVX': 'Energy', 'COP': 'Energy', 'SLB': 'Energy',
}


# ============================================================
# Track 2: 데이터 축적 (Point-in-Time) - 전 종목 저장
# ============================================================

def init_database():
    """SQLite 데이터베이스 초기화"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # EPS Trend 스냅샷 테이블 (v3: A/B 테스팅용 필드 추가)
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
            momentum_score REAL,
            score_321 REAL,
            score_slope REAL,
            eps_chg_60d REAL,
            passed_screen INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, ticker, period)
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON eps_snapshots(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON eps_snapshots(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_passed ON eps_snapshots(passed_screen)')

    conn.commit()
    conn.close()
    print(f'Database initialized: {DB_PATH}')


def collect_and_store_snapshot(index_filter=None):
    """
    전 종목 EPS 스냅샷 수집 및 저장 (생존편향 방지)
    - 스크리닝 통과 여부와 관계없이 모든 종목 저장
    """
    init_database()

    today = datetime.now().strftime('%Y-%m-%d')
    conn = sqlite3.connect(DB_PATH)

    # 전체 종목 수집 (중복 제거)
    all_tickers = {}
    for idx_name, tickers in INDICES.items():
        if index_filter and idx_name != index_filter:
            continue
        for ticker in tickers:
            if ticker not in all_tickers:
                all_tickers[ticker] = idx_name

    print(f'\n[Track 2] 전 종목 데이터 축적 - {today}')
    print(f'수집 대상: {len(all_tickers)}개 종목 (생존편향 방지)')
    print('-' * 50)

    collected = 0
    errors = 0

    for i, (ticker, idx_name) in enumerate(all_tickers.items()):
        try:
            stock = yf.Ticker(ticker)
            trend = stock.eps_trend
            info = stock.info

            # 가격/거래량 (1개월)
            hist = stock.history(period='1mo')
            if len(hist) < 5:
                errors += 1
                continue

            price = hist['Close'].iloc[-1]
            avg_volume = hist['Volume'].mean()
            dollar_volume = price * avg_volume

            # 20일 이동평균
            ma_20 = hist['Close'].tail(20).mean() if len(hist) >= 20 else hist['Close'].mean()
            above_ma20 = 1 if price > ma_20 else 0

            market_cap = info.get('marketCap', 0)
            sector = SECTOR_MAP.get(ticker, info.get('sector', 'Other'))

            # EPS Trend가 없어도 가격 데이터는 저장
            if trend is None or len(trend) == 0 or '+1y' not in trend.index:
                # EPS 없이 기본 데이터만 저장
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO eps_snapshots
                    (date, ticker, index_name, period, price, volume, dollar_volume,
                     market_cap, sector, ma_20, above_ma20, passed_screen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (today, ticker, idx_name, '+1y', price, avg_volume, dollar_volume,
                      market_cap, sector, ma_20, above_ma20, 0))
                collected += 1
                continue

            # +1y EPS 데이터
            eps_row = trend.loc['+1y']
            eps_current = eps_row.get('current')
            eps_7d = eps_row.get('7daysAgo')
            eps_30d = eps_row.get('30daysAgo')
            eps_60d = eps_row.get('60daysAgo')
            eps_90d = eps_row.get('90daysAgo')

            # A/B 테스팅: 두 가지 스코어링 방식 계산
            # Score_321: 가중치 기반 (기존 방식)
            score_321, eps_chg_60d, passed = calculate_momentum_score_v2(
                eps_current, eps_7d, eps_30d, eps_60d
            )

            # Score_Slope: 변화율 가중 평균 (Gemini 제안)
            score_slope = calculate_slope_score(eps_current, eps_7d, eps_30d, eps_60d)

            # 스크리닝 통과 여부 (Score_321 기준, 참고용)
            passed_screen = 0
            if passed and score_321 and score_321 >= 4.0:
                if dollar_volume >= MIN_DOLLAR_VOLUME and above_ma20:
                    passed_screen = 1

            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO eps_snapshots
                (date, ticker, index_name, period, eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                 price, volume, dollar_volume, market_cap, sector, ma_20, above_ma20,
                 momentum_score, score_321, score_slope, eps_chg_60d, passed_screen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, ticker, idx_name, '+1y',
                  eps_current, eps_7d, eps_30d, eps_60d, eps_90d,
                  price, avg_volume, dollar_volume, market_cap, sector,
                  ma_20, above_ma20, score_321, score_321, score_slope, eps_chg_60d, passed_screen))

            collected += 1

            if (i + 1) % 50 == 0:
                print(f'  진행: {i+1}/{len(all_tickers)} (수집: {collected})')
                conn.commit()

        except Exception as e:
            errors += 1
            continue

    conn.commit()
    conn.close()

    print(f'\n완료: {collected}개 수집, {errors}개 오류')
    print(f'저장: {DB_PATH}')

    return collected


def get_data_stats():
    """축적된 데이터 통계"""
    if not os.path.exists(DB_PATH):
        print('데이터베이스 없음')
        return

    conn = sqlite3.connect(DB_PATH)

    stats = pd.read_sql('''
        SELECT
            MIN(date) as first_date,
            MAX(date) as last_date,
            COUNT(DISTINCT date) as days,
            COUNT(DISTINCT ticker) as tickers,
            COUNT(*) as total_records,
            SUM(passed_screen) as passed_total
        FROM eps_snapshots
    ''', conn)

    print('\n[데이터 축적 현황]')
    print(f"기간: {stats['first_date'].iloc[0]} ~ {stats['last_date'].iloc[0]}")
    print(f"일수: {stats['days'].iloc[0]}일")
    print(f"종목: {stats['tickers'].iloc[0]}개")
    print(f"레코드: {stats['total_records'].iloc[0]}개")
    print(f"스크리닝 통과: {stats['passed_total'].iloc[0]}건")

    # 지수별 현황
    by_index = pd.read_sql('''
        SELECT index_name, COUNT(DISTINCT ticker) as tickers,
               SUM(passed_screen) as passed
        FROM eps_snapshots
        WHERE date = (SELECT MAX(date) FROM eps_snapshots)
        GROUP BY index_name
    ''', conn)

    print('\n[지수별 현황 (최신)]')
    for _, row in by_index.iterrows():
        print(f"  {row['index_name']}: {row['tickers']}개 (통과: {row['passed']}개)")

    conn.close()


# ============================================================
# Track 1: 실시간 스크리닝 (v2 개선)
# ============================================================

def calculate_slope_score(current, d7, d30, d60):
    """
    Score_Slope: 변화율 가중 평균 (Gemini 제안 방식)

    공식: Score = (W1 × Δ7d) + (W2 × Δ30d) + (W3 × Δ60d)
    - W1 = 0.5 (최신 변화에 50% 비중)
    - W2 = 0.3 (한 달 변화에 30% 비중)
    - W3 = 0.2 (두 달 변화에 20% 비중)

    "얼마나 가파르게 오르고 있는가(Acceleration)"를 수치화
    """
    if pd.isna(current) or pd.isna(d60) or d60 == 0:
        return None

    # 각 구간 변화율 계산
    delta_7d = 0
    delta_30d = 0
    delta_60d = 0

    # 7일 변화율: (Current - 7d) / 7d
    if pd.notna(d7) and d7 != 0:
        delta_7d = (current - d7) / abs(d7)

    # 30일 변화율: (Current - 30d) / 30d
    if pd.notna(d30) and d30 != 0:
        delta_30d = (current - d30) / abs(d30)

    # 60일 변화율: (Current - 60d) / 60d
    if pd.notna(d60) and d60 != 0:
        delta_60d = (current - d60) / abs(d60)

    # 가중 평균 (W1=0.5, W2=0.3, W3=0.2)
    score = (0.5 * delta_7d) + (0.3 * delta_30d) + (0.2 * delta_60d)

    return round(score, 4)


def calculate_momentum_score_v2(current, d7, d30, d60):
    """
    Score_321: 모멘텀 점수 계산 v2 (가중치 적용 + Kill Switch)

    가중치:
    - Current > 7d: +3점 (최신, 가장 중요)
    - 7d > 30d: +2점
    - 30d > 60d: +1점

    Kill Switch:
    - Current < 7d (최근 하향)이면 제외

    Returns:
    - momentum_score: 점수 (None이면 Kill Switch 발동)
    - eps_chg_60d: 60일 변화율
    - passed: Kill Switch 통과 여부
    """
    if pd.isna(current) or pd.isna(d60) or d60 == 0:
        return None, None, False

    # 60일 변화율 (핵심 지표)
    eps_chg_60d = (current - d60) / abs(d60) * 100

    # 이상치 필터
    if eps_chg_60d > 200 or eps_chg_60d < -80:
        return None, None, False

    # Kill Switch: Current < 7d면 제외 (최근 하향 조정)
    if pd.notna(d7) and current < d7:
        return None, eps_chg_60d, False  # Kill Switch 발동

    # 가중치 기반 점수 계산
    score = 0

    # Current > 7d: +3점 (최신)
    if pd.notna(d7) and d7 != 0:
        if current > d7:
            score += 3

    # 7d > 30d: +2점
    if pd.notna(d7) and pd.notna(d30) and d30 != 0:
        if d7 > d30:
            score += 2
        elif d7 < d30:
            score -= 1

    # 30d > 60d: +1점
    if pd.notna(d30) and pd.notna(d60) and d60 != 0:
        if d30 > d60:
            score += 1
        elif d30 < d60:
            score -= 1

    # 변화율 보너스 (5%당 1점)
    score += eps_chg_60d / 5

    return round(score, 2), round(eps_chg_60d, 2), True


def check_technical_filter(hist):
    """
    기술적 필터: 20일 이평선 위에 있을 것
    - 떨어지는 칼날 방지
    """
    if len(hist) < 20:
        return False, 0, 0

    price = hist['Close'].iloc[-1]
    ma_20 = hist['Close'].tail(20).mean()

    return price > ma_20, price, ma_20


def get_peg_ratio(info):
    """PEG Ratio 계산"""
    try:
        pe = info.get('forwardPE') or info.get('trailingPE')
        growth = info.get('earningsGrowth') or info.get('revenueGrowth')

        if pe and growth and growth > 0:
            peg = pe / (growth * 100)
            return round(peg, 2)
    except:
        pass
    return None


def run_screening(index_filter=None, min_score=4.0):
    """
    실시간 스크리닝 v2

    필터:
    1. Kill Switch: Current >= 7d (최근 하향이면 제외)
    2. 모멘텀 점수 >= min_score
    3. 거래대금 >= $20M
    4. 20일 이평선 위
    5. PEG < 3.0
    6. 섹터 분산 30%
    """
    today = datetime.now().strftime('%Y-%m-%d')

    print('=' * 70)
    print(f'[Track 1] 실시간 스크리닝 v2 - {today}')
    print('=' * 70)
    print(f'필터: 모멘텀>={min_score}, 거래대금>=$20M, MA20위, PEG<3.0')
    print(f'Kill Switch: Current < 7d면 제외')
    print('-' * 70)

    # 종목 수집
    all_tickers = {}
    for idx_name, tickers in INDICES.items():
        if index_filter and idx_name != index_filter:
            continue
        for ticker in tickers:
            if ticker not in all_tickers:
                all_tickers[ticker] = idx_name

    candidates = []
    killed = 0
    no_eps = 0
    low_volume = 0
    below_ma = 0
    high_peg = 0

    for i, (ticker, idx_name) in enumerate(all_tickers.items()):
        try:
            stock = yf.Ticker(ticker)
            trend = stock.eps_trend
            info = stock.info

            if trend is None or '+1y' not in trend.index:
                no_eps += 1
                continue

            eps_row = trend.loc['+1y']

            # 1. 모멘텀 점수 + Kill Switch (Score_321)
            score_321, eps_chg, passed = calculate_momentum_score_v2(
                eps_row.get('current'),
                eps_row.get('7daysAgo'),
                eps_row.get('30daysAgo'),
                eps_row.get('60daysAgo')
            )

            # Score_Slope 계산 (A/B 테스팅용)
            score_slope = calculate_slope_score(
                eps_row.get('current'),
                eps_row.get('7daysAgo'),
                eps_row.get('30daysAgo'),
                eps_row.get('60daysAgo')
            )

            if not passed:
                killed += 1
                continue

            if score_321 is None or score_321 < min_score:
                continue

            # 2. 가격/거래량
            hist = stock.history(period='1mo')
            if len(hist) < 5:
                continue

            price = hist['Close'].iloc[-1]
            avg_volume = hist['Volume'].mean()
            dollar_volume = price * avg_volume

            # 거래대금 필터
            if dollar_volume < MIN_DOLLAR_VOLUME:
                low_volume += 1
                continue

            # 3. 기술적 필터: 20일 이평선 위
            above_ma, current_price, ma_20 = check_technical_filter(hist)
            if not above_ma:
                below_ma += 1
                continue

            # 4. PEG 필터
            peg = get_peg_ratio(info)
            if peg and peg > MAX_PEG:
                high_peg += 1
                continue

            # 5. 섹터
            sector = SECTOR_MAP.get(ticker, info.get('sector', 'Other'))

            candidates.append({
                'ticker': ticker,
                'index': idx_name,
                'momentum': score_321,  # 현재 스크리닝 기준
                'score_321': score_321,
                'score_slope': score_slope,
                'eps_chg_60d': eps_chg,
                'peg': peg,
                'price': round(price, 2),
                'ma_20': round(ma_20, 2),
                'dollar_vol_M': round(dollar_volume / 1_000_000, 1),
                'sector': sector,
                'current': eps_row.get('current'),
                '7d': eps_row.get('7daysAgo'),
                '30d': eps_row.get('30daysAgo'),
                '60d': eps_row.get('60daysAgo'),
            })

            if (i + 1) % 50 == 0:
                print(f'  진행: {i+1}/{len(all_tickers)} (후보: {len(candidates)})')

        except Exception as e:
            continue

    # 필터링 통계
    print(f'\n[필터링 통계]')
    print(f'  Kill Switch (Current<7d): {killed}개 제외')
    print(f'  EPS 데이터 없음: {no_eps}개')
    print(f'  거래대금 부족: {low_volume}개')
    print(f'  MA20 하회: {below_ma}개')
    print(f'  PEG 초과: {high_peg}개')

    if not candidates:
        print('\n조건 충족 종목 없음')
        return pd.DataFrame()

    df = pd.DataFrame(candidates)
    df = df.sort_values('momentum', ascending=False)

    # 섹터 분산
    print(f'\n섹터 분산 전: {len(df)}개')

    final_picks = []
    sector_counts = {}
    max_per_sector = max(int(len(df) * MAX_SECTOR_PCT), 3)

    for _, row in df.iterrows():
        sector = row['sector']
        current_count = sector_counts.get(sector, 0)

        if current_count < max_per_sector:
            final_picks.append(row)
            sector_counts[sector] = current_count + 1

    result = pd.DataFrame(final_picks)
    print(f'섹터 분산 후: {len(result)}개')

    # 결과 출력
    print('\n' + '=' * 70)
    print('스크리닝 결과')
    print('=' * 70)

    # 지수별 분포
    print('\n[지수별 분포]')
    idx_dist = result['index'].value_counts()
    for idx, count in idx_dist.items():
        print(f'  {idx}: {count}개')

    # 섹터별 분포
    print('\n[섹터별 분포]')
    sector_dist = result['sector'].value_counts()
    for sector, count in sector_dist.items():
        pct = count / len(result) * 100
        print(f'  {sector}: {count}개 ({pct:.0f}%)')

    # 종목 리스트
    print('\n[추천 종목]')
    print(f"{'Ticker':<8} {'Index':<12} {'Score':>7} {'EPS%':>8} {'PEG':>6} {'$Vol(M)':>8} {'Price':>10}")
    print('-' * 75)

    for _, row in result.head(25).iterrows():
        peg_str = f"{row['peg']:.1f}" if row['peg'] else 'N/A'
        print(f"{row['ticker']:<8} {row['index']:<12} {row['momentum']:>+6.1f} {row['eps_chg_60d']:>+7.1f}% {peg_str:>6} {row['dollar_vol_M']:>7.1f}M ${row['price']:>9.2f}")

    # EPS Trend 상세
    print('\n[EPS Trend 상세 (Top 10)]')
    print(f"{'Ticker':<8} {'Current':>10} {'7d':>10} {'30d':>10} {'60d':>10} {'Flow':<15}")
    print('-' * 70)

    for _, row in result.head(10).iterrows():
        # 흐름 표시
        flow = ""
        if row['current'] > row['7d']:
            flow += "C>7d "
        if row['7d'] > row['30d']:
            flow += "7>30 "
        if row['30d'] > row['60d']:
            flow += "30>60"

        print(f"{row['ticker']:<8} {row['current']:>10.2f} {row['7d']:>10.2f} {row['30d']:>10.2f} {row['60d']:>10.2f} {flow:<15}")

    # CSV 저장
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    csv_path = os.path.join(DATA_DIR, f'screening_{today}.csv')
    result.to_csv(csv_path, index=False)
    print(f'\n저장: {csv_path}')

    return result


# ============================================================
# 지수별 효과 분석
# ============================================================

def analyze_by_index():
    """지수별 60일 EPS 모멘텀 효과 분석"""
    print('=' * 70)
    print('지수별 60일 EPS 모멘텀 효과 분석')
    print('=' * 70)

    results = {}

    for idx_name, tickers in INDICES.items():
        print(f'\n[{idx_name}] 분석 중...')
        data = []

        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                trend = stock.eps_trend

                if trend is None or '+1y' not in trend.index:
                    continue

                eps_row = trend.loc['+1y']
                current = eps_row.get('current')
                d60 = eps_row.get('60daysAgo')

                hist = stock.history(period='6mo')
                if len(hist) < 44:
                    continue

                ret_60d = (hist['Close'].iloc[-1] / hist['Close'].iloc[-44] - 1) * 100

                if pd.notna(current) and pd.notna(d60) and d60 != 0:
                    chg_60d = (current - d60) / abs(d60) * 100
                    if -80 < chg_60d < 200:
                        data.append({'chg_60d': chg_60d, 'ret_60d': ret_60d})
            except:
                continue

        if len(data) < 10:
            print(f'  데이터 부족: {len(data)}개')
            continue

        df = pd.DataFrame(data)
        corr = df['chg_60d'].corr(df['ret_60d'])

        # 임계값별 분석
        best_sharpe = 0
        best_thresh = 0

        for thresh in [3, 4, 5, 6, 7]:
            filtered = df[df['chg_60d'] >= thresh]
            if len(filtered) >= 3:
                avg = filtered['ret_60d'].mean()
                std = filtered['ret_60d'].std()
                sharpe = avg / std if std > 0 else 0
                if sharpe > best_sharpe:
                    best_sharpe = sharpe
                    best_thresh = thresh

        results[idx_name] = {
            'n': len(df),
            'corr': corr,
            'best_thresh': best_thresh,
            'sharpe': best_sharpe
        }

        print(f'  수집: {len(df)}개, 상관계수: {corr:.3f}')
        print(f'  Best: +{best_thresh}% (Sharpe={best_sharpe:.2f})')

    # 요약
    print('\n' + '=' * 70)
    print('지수별 비교 요약')
    print('=' * 70)
    print(f"{'Index':<15} {'N':>6} {'Corr':>8} {'BestThresh':>12} {'Sharpe':>8}")
    print('-' * 55)

    for idx, data in sorted(results.items(), key=lambda x: x[1]['sharpe'], reverse=True):
        print(f"{idx:<15} {data['n']:>6} {data['corr']:>+7.3f} {data['best_thresh']:>11}% {data['sharpe']:>8.2f}")

    return results


# ============================================================
# 메인
# ============================================================

def main():
    import sys

    if len(sys.argv) < 2:
        print('''
EPS Momentum System v2
======================
사용법:
  python eps_momentum_system.py screen              # 실시간 스크리닝
  python eps_momentum_system.py screen NASDAQ_100   # 특정 지수만
  python eps_momentum_system.py collect             # 전 종목 데이터 축적
  python eps_momentum_system.py stats               # 축적 현황
  python eps_momentum_system.py analyze             # 지수별 효과 분석
  python eps_momentum_system.py all                 # 스크리닝 + 축적

v2 개선사항:
  - 거래대금 필터 ($20M+)
  - Kill Switch (Current < 7d면 제외)
  - 가중치 기반 모멘텀 점수
  - 20일 이평선 기술적 필터
  - 전 종목 저장 (생존편향 방지)
        ''')
        return

    cmd = sys.argv[1].lower()
    index_filter = sys.argv[2] if len(sys.argv) > 2 else None

    if cmd == 'screen':
        run_screening(index_filter)
    elif cmd == 'collect':
        collect_and_store_snapshot(index_filter)
    elif cmd == 'stats':
        get_data_stats()
    elif cmd == 'analyze':
        analyze_by_index()
    elif cmd == 'all':
        run_screening(index_filter)
        collect_and_store_snapshot(index_filter)
    else:
        print(f'알 수 없는 명령: {cmd}')


if __name__ == '__main__':
    main()
