#!/usr/bin/env python3
"""
EPS Momentum Screening System - Korean Stocks (KR)
====================================================
KOSPI (.KS) + KOSDAQ (.KQ) 대형/중형주 ~200+ 종목
NTM EPS 기반 모멘텀 스크리닝 (US 시스템과 동일 방법론)

사용법: python eps_momentum_kr.py
"""

import sys
import os
import time
import sqlite3
import json
import statistics
import warnings
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf
import pandas as pd

warnings.filterwarnings('ignore')

# Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# ============================================================
# 경로 설정
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'eps_momentum_kr.db')
CACHE_PATH = os.path.join(BASE_DIR, 'ticker_info_cache_kr.json')

# ============================================================
# 한국 주식 유니버스 (~220 tickers)
# Cleaned & verified: removed delisted/invalid, added verified tickers
# ============================================================

KOSPI_TICKERS = [
    # === Top 50 by market cap ===
    '005930.KS',  # Samsung Electronics
    '000660.KS',  # SK Hynix
    '373220.KS',  # LG Energy Solution
    '005380.KS',  # Hyundai Motor
    '000270.KS',  # Kia
    '068270.KS',  # Celltrion
    '035420.KS',  # NAVER
    '035720.KS',  # Kakao
    '207940.KS',  # Samsung Biologics
    '006400.KS',  # Samsung SDI
    '051910.KS',  # LG Chem
    '028260.KS',  # Samsung C&T
    '003670.KS',  # POSCO Holdings
    '105560.KS',  # KB Financial
    '055550.KS',  # Shinhan Financial
    '086790.KS',  # Hana Financial
    '316140.KS',  # Woori Financial
    '066570.KS',  # LG Electronics
    '032830.KS',  # Samsung Life
    '034730.KS',  # SK Inc
    '030200.KS',  # KT
    '012330.KS',  # Hyundai Mobis
    '096770.KS',  # SK Innovation
    '003550.KS',  # LG
    '010130.KS',  # Korea Zinc
    '034020.KS',  # Doosan Enerbility
    '009150.KS',  # Samsung Electro-Mechanics
    '018260.KS',  # Samsung SDS
    '000810.KS',  # Samsung Fire
    '010950.KS',  # S-Oil
    '017670.KS',  # SK Telecom
    '024110.KS',  # Industrial Bank of Korea
    '259960.KS',  # Krafton
    '011200.KS',  # HMM
    '000720.KS',  # Hyundai E&C
    '033780.KS',  # KT&G
    '015760.KS',  # Korea Electric Power
    '036570.KS',  # NCsoft
    '011170.KS',  # Lotte Chemical
    '329180.KS',  # HD Hyundai Heavy Industries
    '042660.KS',  # Hanwha Ocean
    '010140.KS',  # Samsung Heavy Industries
    '267250.KS',  # HD Hyundai
    '138040.KS',  # Meritz Financial
    '302440.KS',  # SK Bioscience
    '047050.KS',  # POSCO International
    '003490.KS',  # Korean Air
    '180640.KS',  # Hanwha Solutions
    '352820.KS',  # HYBE
    '009540.KS',  # HD Hyundai Heavy

    # === 51-100: Large Cap ===
    '000100.KS',  # Yuhan
    '006800.KS',  # Mirae Asset Securities
    '088980.KS',  # Macquarie Korea Infra
    '090430.KS',  # Amorepacific
    '004020.KS',  # Hyundai Steel
    '097950.KS',  # CJ CheilJedang
    '326030.KS',  # SK Biopharm
    '161390.KS',  # Hankook Tire
    '011790.KS',  # SKC
    '021240.KS',  # Coway
    '078930.KS',  # GS
    '034220.KS',  # LG Display
    '004170.KS',  # Shinsegae
    '036460.KS',  # Korea Gas
    '251270.KS',  # Netmarble
    '128940.KS',  # Hanmi Pharm
    '005490.KS',  # POSCO (DPS)
    '402340.KS',  # SK Square
    '323410.KS',  # Kakao Bank
    '377300.KS',  # Kakao Pay
    '361610.KS',  # SK IE Technology
    '000880.KS',  # Hanwha
    '272210.KS',  # Hanwha Systems
    '042700.KS',  # Hanwha Aerospace
    '241560.KS',  # Doosan Bobcat
    '011070.KS',  # LG Innotek
    '001570.KS',  # KumYoung
    '271560.KS',  # Orion
    '006360.KS',  # GS Engineering
    '071050.KS',  # Korea Investment Holdings
    '139480.KS',  # E-Mart
    '016360.KS',  # Samsung Securities
    '004990.KS',  # Lotte Corp
    '000240.KS',  # Hankook Aerospace
    '003030.KS',  # NICE Rating
    '039490.KS',  # Kiwoom Securities

    # === 101-150: Mid-Large Cap ===
    '004370.KS',  # Nongshim
    '051900.KS',  # LG H&H
    '005940.KS',  # NH Investment
    '001040.KS',  # CJ
    '069960.KS',  # Hyundai Department Store
    '005830.KS',  # DB Insurance
    '002790.KS',  # Amore G
    '006280.KS',  # Green Cross
    '009240.KS',  # Hanssem
    '088350.KS',  # Hanwha Life
    '001450.KS',  # Hyundai Marine
    '008770.KS',  # Hotel Shilla
    '192820.KS',  # Cosmax
    '004800.KS',  # Hyosung
    '008930.KS',  # Hanmi Science
    '001800.KS',  # ORION Holdings
    # '005387.KS',  # Hyundai Motor 2prf — EXCLUDED (preferred share, distorts adj_gap)
    '009420.KS',  # Hankook Holdings
    '120110.KS',  # Kolon Industries
    '005850.KS',  # SL Corp (Auto Parts)
    '002380.KS',  # KCC
    '010120.KS',  # LS Electric
    '006260.KS',  # LS
    '103140.KS',  # Poongsan
    '298020.KS',  # Hyosung TNC
    '003000.KS',  # Bukwang Pharmaceutical
    '014680.KS',  # Hansol Chemical
    '007070.KS',  # GS Retail
    '002620.KS',  # Jeil Pharm
    '000150.KS',  # Doosan
    '000120.KS',  # CJ Logistics
    '111770.KS',  # Youngone
    '282330.KS',  # BGF Retail
    '079550.KS',  # LIG Nex1
    '028050.KS',  # Samsung Engineering
    '023530.KS',  # Lotte Shopping
    '017800.KS',  # Hyundai Elevator
    '267260.KS',  # HD Hyundai Construction Equipment
    '006120.KS',  # SK Discovery
    '285130.KS',  # SK Chemicals

    # === 151-175: Additional KOSPI (verified) ===
    '004000.KS',  # Lotte Fine Chemical
    '069620.KS',  # Dae Woong Pharm
    '011780.KS',  # Kumho Petro
    '034830.KS',  # HD Korea Shipbuilding
    '100840.KS',  # SNT Energy
    '007310.KS',  # Otoki (Packaged Foods)
    '047810.KS',  # Korea Aerospace Industries
    '016800.KS',  # Taihan Cable
    '005180.KS',  # Binggrae
    '009450.KS',  # Kyungdong Navien
    '081660.KS',  # Isu Chemical
    '383800.KS',  # LX Holdings
    '092780.KS',  # DYP (Auto Parts)
    '003240.KS',  # Taeyoung E&C
    '025540.KS',  # Korea Petrochemical
    '012450.KS',  # Hanwha Techwin
    '004150.KS',  # Hanwha Investment
    '024720.KS',  # Hansung Enterprise
    '078520.KS',  # Aekyung Industrial
    '013520.KS',  # CJ Chemical
    '214420.KS',  # Tonymoly
    '002960.KS',  # Hankuk Carbon
    '003620.KS',  # KG Chemical
    '006890.KS',  # Taekyung BK
    '000990.KS',  # DB HiTek

    # === 176-200+: Additional verified large/mid caps ===
    '000080.KS',  # Hite Jinro
    '004490.KS',  # Sejong Industrial
    '003850.KS',  # Kumho Electric
    '001120.KS',  # LX International
    # '008560.KS',  # Meritz Securities — EXCLUDED (delisted/404)
    '044820.KS',  # Cosmax BTI
    '002350.KS',  # NICE Information Service
    '001740.KS',  # SK Networks
    # '006110.KS',  # SamaAlum (Aluminum) — EXCLUDED (commodity, low cap)
    # '015540.KS',  # SPC Samlip — EXCLUDED (delisted/404)
    '007700.KS',  # F&F Holdings
    '004910.KS',  # Chorok Baem
    '014820.KS',  # DongWon Systems
    '004560.KS',  # Hyundai WIA
    '267270.KS',  # HD Hyundai Infracore
    '071840.KS',  # Lotte Himart
    '023590.KS',  # Daou Technology
    '020560.KS',  # Asia Paper
    '002840.KS',  # Miwon Specialty Chemical
    '014910.KS',  # Sungdo Engineering
    '008730.KS',  # Yuhan Yanghaeng
    # '010060.KS',  # OCI Holdings — EXCLUDED (dup of 007310.KS concept, Asset Mgmt holding co)
    '001680.KS',  # DaeShang
    '272450.KS',  # Jin Air
    '950210.KS',  # Prestige BioPharma
]

KOSDAQ_TICKERS = [
    # === Top 50 KOSDAQ by market cap (verified) ===
    '196170.KQ',  # Alteogen
    '028300.KQ',  # HLB
    '041510.KQ',  # SM Entertainment
    '263750.KQ',  # Pearl Abyss
    '293490.KQ',  # Cafe24
    '357780.KQ',  # Solus Advanced Materials
    '095340.KQ',  # ISC
    '394280.KQ',  # CLIO Cosmetics
    '039030.KQ',  # Ion Tech
    '060310.KQ',  # 3S
    '112040.KQ',  # Wemade
    '035760.KQ',  # CJ ENM
    '035900.KQ',  # JYP Entertainment
    '078600.KQ',  # Daejoo Electronic Materials
    '222080.KQ',  # Hyundai HT
    '089030.KQ',  # Technowings
    '067310.KQ',  # Hana Materials
    '141080.KQ',  # Legochem Bio
    '086900.KQ',  # MEDY-TOX
    '253450.KQ',  # Studio Dragon
    '240810.KQ',  # Won Ik QnC
    '098460.KQ',  # Koh Young Technology
    '078340.KQ',  # COM2US
    '069080.KQ',  # Webzen
    '215600.KQ',  # Shin Young Securities
    '131970.KQ',  # Tesna
    '067630.KQ',  # HLB Life Science
    '036620.KQ',  # Gamevil (COM2US Holdings)
    '054620.KQ',  # APS Holdings
    '033640.KQ',  # NHN
    '046890.KQ',  # Seoul Viosys
    '348210.KQ',  # Nextchip
    '036930.KQ',  # JoongAng Vaccine
    '214150.KQ',  # Clone Tech
    '348150.KQ',  # KBG
    '257720.KQ',  # Actoz Soft
    '039200.KQ',  # Osstem Implant
    '340570.KQ',  # Tiotek
    '950130.KQ',  # NICE Holdings
    '058610.KQ',  # ASTech
    # Additional verified KOSDAQ
    '036560.KQ',  # YOUNG POONG ELECTRONICS
    '056190.KQ',  # SFA Engineering
    '043150.KQ',  # ValueAdded Network
    '090710.KQ',  # Humax
    # '091990.KQ',  # Celltrion Pharm — EXCLUDED (delisted/404, merged into Celltrion)
    '041020.KQ',  # Polaris Office
    '237690.KQ',  # Seah Metals
    '220180.KQ',  # Handsome Hitech
    '352480.KQ',  # CUBEENT
    '058820.KQ',  # CMG Pharmaceutical
    '096530.KQ',  # Seegene
    '314930.KQ',  # BioNote
    '039340.KQ',  # Korea Pharma
    '330350.KQ',  # Cellid
    '226330.KQ',  # Shintekauto
]

# Remove duplicates
ALL_TICKERS = list(dict.fromkeys(KOSPI_TICKERS + KOSDAQ_TICKERS))

# ============================================================
# 원자재 업종 (한국 특화 - 영문 industry 기준)
# ============================================================
COMMODITY_INDUSTRIES = {
    'Gold', 'Other Precious Metals & Mining',
    'Other Industrial Metals & Mining', 'Copper', 'Steel', 'Aluminum',
    'Agricultural Inputs', 'Oil & Gas E&P', 'Oil & Gas Integrated',
    'Oil & Gas Refining & Marketing', 'Lumber & Wood Production',
    'Coking Coal',
}

# yfinance industry overrides (misclassifications + missing data)
INDUSTRY_OVERRIDES = {
    # Misclassifications
    '005930.KS': 'Semiconductors',       # Samsung Electronics: yf says Consumer Electronics
    '009150.KS': 'Electronic Components', # Samsung Electro-Mechanics: keep as components (MLCC, substrate)
    # Missing industry (yfinance returns N/A for these)
    '088980.KS': 'Asset Management',     # Macquarie Korea Infra Fund
    '095340.KQ': 'Semiconductor Equipment & Materials',  # ISC: test sockets
    '112040.KQ': 'Electronic Gaming & Multimedia',       # Wemade: game company
    '078600.KQ': 'Semiconductor Equipment & Materials',  # Daejoo Electronic Materials
    '067310.KQ': 'Semiconductors',       # Hana Micron: semiconductor packaging
    '039200.KQ': 'Medical Devices',      # Osstem Implant: dental implants
    '069080.KQ': 'Electronic Gaming & Multimedia',       # Webzen: game company
    '131970.KQ': 'Semiconductor Equipment & Materials',  # Doosan Tesna: wafer test
    '036620.KQ': 'Electronic Gaming & Multimedia',       # Com2uS Holdings (Gamsung)
    '043150.KQ': 'Medical Devices',      # Vatech: dental X-ray equipment
    '237690.KQ': 'Drug Manufacturers - Specialty & Generic',  # ST Pharm: pharma CDMO
    '036930.KQ': 'Drug Manufacturers - Specialty & Generic',  # JoongAng Vaccine (JEL)
}

# ============================================================
# 업종 한글 매핑
# ============================================================
INDUSTRY_MAP = {
    'Semiconductors': '반도체',
    'Semiconductor Equipment & Materials': '반도체장비',
    'Software - Application': '응용SW',
    'Software - Infrastructure': '인프라SW',
    'Information Technology Services': 'IT서비스',
    'Computer Hardware': '하드웨어',
    'Electronic Components': '전자부품',
    'Scientific & Technical Instruments': '계측기기',
    'Communication Equipment': '통신장비',
    'Consumer Electronics': '가전',
    'Electronic Gaming & Multimedia': '게임',
    'Internet Content & Information': '인터넷',
    'Internet Retail': '온라인유통',
    'Entertainment': '엔터',
    'Telecom Services': '통신',
    'Auto Parts': '자동차부품',
    'Auto Manufacturers': '자동차',
    'Banks - Regional': '지역은행',
    'Banks - Diversified': '대형은행',
    'Asset Management': '자산운용',
    'Capital Markets': '자본시장',
    'Credit Services': '신용서비스',
    'Financial Data & Stock Exchanges': '금융데이터',
    'Insurance - Property & Casualty': '손해보험',
    'Insurance - Life': '생명보험',
    'Insurance - Diversified': '종합보험',
    'Financial Conglomerates': '금융지주',
    'Medical Devices': '의료기기',
    'Medical Instruments & Supplies': '의료용품',
    'Diagnostics & Research': '진단연구',
    'Drug Manufacturers - General': '대형제약',
    'Drug Manufacturers - Specialty & Generic': '특수제약',
    'Biotechnology': '바이오',
    'Aerospace & Defense': '방산',
    'Specialty Industrial Machinery': '산업기계',
    'Farm & Heavy Construction Machinery': '중장비',
    'Engineering & Construction': '건설',
    'Building Products & Equipment': '건축자재',
    'Electrical Equipment & Parts': '전기장비',
    'Industrial Distribution': '산업유통',
    'Conglomerates': '복합기업',
    'Integrated Freight & Logistics': '물류',
    'Marine Shipping': '해운',
    'Airlines': '항공',
    'Specialty Chemicals': '특수화학',
    'Chemicals': '화학',
    'Steel': '철강',
    'Packaged Foods': '식품',
    'Beverages - Non-Alcoholic': '음료',
    'Household & Personal Products': '생활용품',
    'Luxury Goods': '명품',
    'Department Stores': '백화점',
    'Specialty Retail': '전문소매',
    'Discount Stores': '할인점',
    'Apparel Manufacturing': '의류제조',
    'Residential Construction': '주택건설',
    'Oil & Gas Refining & Marketing': '석유정제',
    'Utilities - Regulated Electric': '전력',
    'Utilities - Regulated Gas': '가스',
    'Renewable Energy': '신재생',
    'Solar': '태양광',
    'Shell Companies': '쉘컴퍼니',
    'Gambling': '도박',
    'Leisure': '레저',
    'Education & Training Services': '교육',
    'Tobacco': '담배',
    'Beverages - Wineries & Distilleries': '주류',
    'Confectioners': '제과',
    'Furnishings, Fixtures & Appliances': '가구가전',
    'Grocery Stores': '식료품점',
    'Health Information Services': '의료정보',
    'Metal Fabrication': '금속가공',
    'Other Industrial Metals & Mining': '비철금속',
    'Publishing': '출판',
    'REIT - Diversified': '리츠',
    'Textile Manufacturing': '섬유',
    'N/A': '기타',
}


# ============================================================
# Helpers
# ============================================================

def log(msg, level="INFO"):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {level}: {msg}")


def _safe(val, default=0.0):
    """Safe float conversion, NaN/None -> default."""
    if val is None:
        return default
    try:
        f = float(val)
        return default if f != f else f
    except (ValueError, TypeError):
        return default


def industry_kr(eng_industry):
    """English industry -> Korean abbreviation."""
    if not eng_industry:
        return '기타'
    return INDUSTRY_MAP.get(eng_industry, eng_industry[:8] if len(eng_industry) > 8 else eng_industry)


# ============================================================
# DB Setup
# ============================================================

def init_db():
    """Create tables if not exist."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS ntm_screening (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            score REAL,
            adj_score REAL,
            adj_gap REAL,
            price REAL,
            ma60 REAL,
            ma120 REAL,
            ntm_current REAL,
            ntm_7d REAL,
            ntm_30d REAL,
            ntm_60d REAL,
            ntm_90d REAL,
            rev_growth REAL,
            num_analysts INTEGER,
            rev_up30 INTEGER,
            rev_down30 INTEGER,
            operating_margin REAL,
            gross_margin REAL,
            market_cap REAL,
            fwd_pe REAL,
            industry TEXT,
            short_name TEXT,
            is_turnaround INTEGER DEFAULT 0,
            direction REAL,
            seg1 REAL, seg2 REAL, seg3 REAL, seg4 REAL,
            PRIMARY KEY (date, ticker)
        )
    ''')
    conn.commit()
    conn.close()


# ============================================================
# NTM EPS Calculation (same logic as US system)
# ============================================================

MIN_NTM_EPS = 100.0  # KRW basis: 100 won (instead of $1)

def calculate_ntm_eps(stock, today=None):
    """NTM EPS: endDate-based time-weighted blend of 0y/+1y.
    ALL Korean stocks use Dec 31 fiscal year (simpler than US).
    """
    if today is None:
        today = datetime.now()

    eps_trend = stock.eps_trend
    if eps_trend is None or len(eps_trend) == 0:
        return None

    if '0y' not in eps_trend.index or '+1y' not in eps_trend.index:
        return None

    try:
        raw_trend = stock._analysis._earnings_trend
    except (AttributeError, Exception):
        return None

    periods = {}
    for item in raw_trend:
        p = item.get('period')
        if p in ('0y', '+1y'):
            end_date_str = item.get('endDate')
            if end_date_str:
                periods[p] = datetime.strptime(end_date_str, '%Y-%m-%d')

    if '0y' not in periods or '+1y' not in periods:
        return None

    fy0_end = periods['0y']
    fy1_end = periods['+1y']
    fy0_start = datetime(fy0_end.year - 1, fy0_end.month, fy0_end.day) + timedelta(days=1)
    fy1_start = fy0_end + timedelta(days=1)

    snapshots = {
        'current': ('current', 0),
        '7d': ('7daysAgo', 7),
        '30d': ('30daysAgo', 30),
        '60d': ('60daysAgo', 60),
        '90d': ('90daysAgo', 90),
    }

    ntm = {}
    for key, (col, days_ago) in snapshots.items():
        ref = today - timedelta(days=days_ago)
        window_end = ref + timedelta(days=365)

        overlap_0y = max(0, (min(window_end, fy0_end) - max(ref, fy0_start)).days)
        overlap_1y = max(0, (min(window_end, fy1_end) - max(ref, fy1_start)).days)
        total_overlap = overlap_0y + overlap_1y

        if total_overlap == 0:
            return None

        v0 = eps_trend.loc['0y', col]
        v1 = eps_trend.loc['+1y', col]

        if pd.isna(v0) or pd.isna(v1):
            return None

        ntm[key] = (overlap_0y / total_overlap) * v0 + (overlap_1y / total_overlap) * v1

    return ntm


def calculate_ntm_score(ntm_values):
    """Score = seg1+seg2+seg3+seg4, adj_score with direction correction."""
    nc = ntm_values['current']
    n7 = ntm_values['7d']
    n30 = ntm_values['30d']
    n60 = ntm_values['60d']
    n90 = ntm_values['90d']

    is_turnaround = abs(nc) < MIN_NTM_EPS or abs(n90) < MIN_NTM_EPS

    SEG_CAP = 100
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100)) if n7 != 0 else 0
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100)) if n30 != 0 else 0
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100)) if n60 != 0 else 0
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100)) if n90 != 0 else 0

    score = seg1 + seg2 + seg3 + seg4

    DIRECTION_DIVISOR = 30
    DIRECTION_CAP = 0.3
    recent_avg = (seg1 + seg2) / 2
    old_avg = (seg3 + seg4) / 2
    direction = recent_avg - old_avg
    direction_mult = max(-DIRECTION_CAP, min(DIRECTION_CAP, direction / DIRECTION_DIVISOR))
    adj_score = score * (1 + direction_mult)

    return score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction


def get_trend_lights(seg1, seg2, seg3, seg4):
    """Trend lights: past->present order."""
    segs = [seg4, seg3, seg2, seg1]
    lights = []
    for s in segs:
        if s > 20:
            lights.append('🔥')
        elif s > 5:
            lights.append('☀️')
        elif s > 1:
            lights.append('🌤️')
        elif s >= -1:
            lights.append('☁️')
        else:
            lights.append('🌧️')
    return ''.join(lights)


# ============================================================
# Data Collection
# ============================================================

def prefetch_eps_data(ticker, today):
    """Worker: fetch NTM EPS + analyst data for one ticker."""
    try:
        stock = yf.Ticker(ticker)
        ntm = calculate_ntm_eps(stock, today)
        if ntm is None:
            return ticker, {'ntm': None}

        raw_trend = None
        try:
            raw_trend = stock._analysis._earnings_trend
        except Exception:
            pass

        return ticker, {'ntm': ntm, 'raw_trend': raw_trend}
    except Exception as e:
        return ticker, {'error': str(e)}


def fetch_info_worker(ticker):
    """Worker: fetch .info for revenue growth, margins, etc."""
    try:
        info = yf.Ticker(ticker).info
        return ticker, info
    except Exception:
        return ticker, None


def collect_data(tickers, today=None):
    """
    Main data collection pipeline:
    1. Batch download price history
    2. Parallel EPS data collection
    3. Parallel .info collection (revenue, margins, etc.)
    4. Score calculation and DB storage
    """
    if today is None:
        today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')

    log(f"=== EPS Momentum KR Data Collection ({today_str}) ===")
    log(f"Universe: {len(tickers)} tickers")

    # --- Step 1: Batch price download ---
    log("Step 1: Batch price download (1y)...")
    t0 = time.time()
    hist_all = None
    try:
        hist_all = yf.download(tickers, period='1y', threads=True, progress=False)
        log(f"  Price download done ({time.time()-t0:.0f}s)")
    except Exception as e:
        log(f"  Batch download failed: {e}", "WARN")

    # --- Step 2: Parallel EPS collection ---
    log("Step 2: EPS data collection (5 threads)...")
    t1 = time.time()
    eps_data = {}
    BATCH = 50
    for batch_start in range(0, len(tickers), BATCH):
        batch = tickers[batch_start:batch_start + BATCH]
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(prefetch_eps_data, t, today): t for t in batch}
            for f in as_completed(futures):
                try:
                    t, data = f.result(timeout=30)
                except Exception as e:
                    t = futures[f]
                    data = {'error': str(e)}
                eps_data[t] = data
        done = batch_start + len(batch)
        if done % 100 < BATCH:
            log(f"  EPS: {done}/{len(tickers)}")
        if batch_start + BATCH < len(tickers):
            time.sleep(0.3)

    # Retry errors with exponential backoff
    error_tickers = [t for t, d in eps_data.items() if 'error' in d]
    if error_tickers:
        log(f"  Retrying {len(error_tickers)} errors...")
        time.sleep(5)  # longer initial wait for rate-limit / crumb refresh
        for batch_start in range(0, len(error_tickers), BATCH):
            batch = error_tickers[batch_start:batch_start + BATCH]
            with ThreadPoolExecutor(max_workers=3) as ex:
                futures = {ex.submit(prefetch_eps_data, t, today): t for t in batch}
                for f in as_completed(futures):
                    try:
                        t, data = f.result(timeout=30)
                    except Exception as e:
                        t = futures[f]
                        data = {'error': str(e)}
                    if 'error' not in data:
                        eps_data[t] = data
            if batch_start + BATCH < len(error_tickers):
                time.sleep(1)

    eps_ok = sum(1 for d in eps_data.values() if d.get('ntm') is not None)
    log(f"  EPS done: {eps_ok}/{len(tickers)} have NTM data ({time.time()-t1:.0f}s)")

    # --- Step 3: .info collection for revenue/margins ---
    tickers_with_eps = [t for t in tickers if eps_data.get(t, {}).get('ntm') is not None]
    log(f"Step 3: Fetching .info for {len(tickers_with_eps)} tickers (5 threads)...")
    t2 = time.time()
    info_data = {}
    for batch_start in range(0, len(tickers_with_eps), BATCH):
        batch = tickers_with_eps[batch_start:batch_start + BATCH]
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(fetch_info_worker, t): t for t in batch}
            for f in as_completed(futures):
                try:
                    t, info = f.result(timeout=30)
                except Exception as e:
                    t = futures[f]
                    info = None
                info_data[t] = info
        done = batch_start + len(batch)
        if done % 100 < BATCH:
            log(f"  Info: {done}/{len(tickers_with_eps)}")
        if batch_start + BATCH < len(tickers_with_eps):
            time.sleep(0.3)
    log(f"  Info done ({time.time()-t2:.0f}s)")

    # --- Step 4: Process and store ---
    log("Step 4: Processing & DB storage...")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()

    results = []
    no_data = []
    errors = []

    # Load/init cache
    ticker_cache = {}
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, encoding='utf-8') as f:
                ticker_cache = json.load(f)
        except Exception:
            pass

    cache_updated = False

    for ticker in tickers:
        data = eps_data.get(ticker, {})
        if 'error' in data:
            errors.append(ticker)
            continue

        ntm = data.get('ntm')
        if ntm is None:
            no_data.append(ticker)
            continue

        try:
            score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction = calculate_ntm_score(ntm)
            trend_lights = get_trend_lights(seg1, seg2, seg3, seg4)

            # EPS revision & analyst count
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

            # Price & PE & adj_gap
            fwd_pe_now = None
            fwd_pe_chg = None
            current_price = None
            ma60_val = None
            ma120_val = None

            try:
                if hist_all is not None:
                    # Handle multi-ticker download structure
                    if isinstance(hist_all.columns, pd.MultiIndex):
                        if ticker in hist_all['Close'].columns:
                            hist = hist_all['Close'][ticker].dropna()
                        else:
                            hist = pd.Series(dtype=float)
                    else:
                        hist = hist_all['Close'].dropna()
                else:
                    hist = pd.Series(dtype=float)

                if len(hist) >= 60:
                    p_now = float(hist.iloc[-1])
                    current_price = p_now
                    ma60_val = float(hist.rolling(window=60).mean().iloc[-1])
                    if len(hist) >= 120:
                        ma120_val = float(hist.rolling(window=120).mean().iloc[-1])
                    hist_dt = hist.index.tz_localize(None) if hist.index.tz else hist.index

                    # Price at each snapshot
                    prices = {}
                    for days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                        target = today - timedelta(days=days)
                        idx = (hist_dt - target).map(lambda x: abs(x.days)).argmin()
                        prices[key] = float(hist.iloc[idx])

                    # Current fwd PE
                    nc = ntm['current']
                    if nc > 0:
                        fwd_pe_now = p_now / nc

                    # Weighted PE change (adj_gap basis)
                    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
                    weighted_sum = 0.0
                    total_weight = 0.0
                    for key, w in weights.items():
                        ntm_val = ntm[key]
                        if fwd_pe_now is not None and nc > 0 and ntm_val > 0 and prices[key] > 0:
                            fwd_pe_then = prices[key] / ntm_val
                            pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                            weighted_sum += w * pe_chg
                            total_weight += w
                    if total_weight > 0:
                        fwd_pe_chg = weighted_sum / total_weight

            except Exception as e:
                pass  # price data unavailable

            # adj_gap = fwd_pe_chg * (1 + dir_factor) * eps_quality
            adj_gap = None
            if fwd_pe_chg is not None and direction is not None:
                dir_factor = max(-0.3, min(0.3, direction / 30))
                min_seg = min(seg1 or 0, seg2 or 0, seg3 or 0, seg4 or 0)
                eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
                adj_gap = fwd_pe_chg * (1 + dir_factor) * eps_q

            # Info data (revenue, margins)
            info = info_data.get(ticker)
            rev_growth = None
            operating_margin = None
            gross_margin = None
            market_cap = None
            short_name = ticker

            if info:
                rev_growth = info.get('revenueGrowth')
                operating_margin = info.get('operatingMargins')
                gross_margin = info.get('grossMargins')
                market_cap = info.get('marketCap')
                sn = info.get('shortName') or info.get('longName') or ticker
                short_name = sn
                ind = info.get('industry', 'N/A')

                # Apply industry overrides (yfinance misclassifications)
                if ticker in INDUSTRY_OVERRIDES:
                    ind = INDUSTRY_OVERRIDES[ticker]

                # Update cache
                if ticker not in ticker_cache or ticker_cache[ticker].get('shortName') != short_name:
                    ticker_cache[ticker] = {
                        'shortName': short_name,
                        'industry': ind,
                        'industry_kr': industry_kr(ind),
                    }
                    cache_updated = True
            else:
                ind = ticker_cache.get(ticker, {}).get('industry', 'N/A')

            ind_kr = industry_kr(ind) if ind else '기타'

            # eps_change_90d
            eps_90d = None
            n90 = ntm['90d']
            nc_val = ntm['current']
            if n90 != 0:
                eps_90d = (nc_val - n90) / abs(n90) * 100

            row = {
                'ticker': ticker,
                'short_name': short_name,
                'industry': ind if ind else 'N/A',
                'industry_kr': ind_kr,
                'score': score,
                'adj_score': adj_score,
                'direction': direction,
                'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
                'min_seg': min(seg1, seg2, seg3, seg4),
                'ntm_current': ntm['current'],
                'ntm_90d': ntm['90d'],
                'eps_90d': eps_90d,
                'trend_lights': trend_lights,
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
                'rev_growth': rev_growth,
                'operating_margin': operating_margin,
                'gross_margin': gross_margin,
                'market_cap': market_cap,
            }

            # DB insert
            cursor.execute('''
                INSERT INTO ntm_screening
                (date, ticker, score, adj_score, adj_gap, price, ma60, ma120,
                 ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                 rev_growth, num_analysts, rev_up30, rev_down30,
                 operating_margin, gross_margin, market_cap, fwd_pe,
                 industry, short_name, is_turnaround, direction,
                 seg1, seg2, seg3, seg4)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    score=excluded.score, adj_score=excluded.adj_score,
                    adj_gap=excluded.adj_gap, price=excluded.price,
                    ma60=excluded.ma60, ma120=excluded.ma120,
                    ntm_current=excluded.ntm_current, ntm_7d=excluded.ntm_7d,
                    ntm_30d=excluded.ntm_30d, ntm_60d=excluded.ntm_60d,
                    ntm_90d=excluded.ntm_90d,
                    rev_growth=excluded.rev_growth,
                    num_analysts=excluded.num_analysts,
                    rev_up30=excluded.rev_up30, rev_down30=excluded.rev_down30,
                    operating_margin=excluded.operating_margin,
                    gross_margin=excluded.gross_margin,
                    market_cap=excluded.market_cap, fwd_pe=excluded.fwd_pe,
                    industry=excluded.industry, short_name=excluded.short_name,
                    is_turnaround=excluded.is_turnaround,
                    direction=excluded.direction,
                    seg1=excluded.seg1, seg2=excluded.seg2,
                    seg3=excluded.seg3, seg4=excluded.seg4
            ''', (today_str, ticker, score, adj_score, adj_gap,
                  current_price, ma60_val, ma120_val,
                  ntm['current'], ntm['7d'], ntm['30d'], ntm['60d'], ntm['90d'],
                  rev_growth, num_analysts, rev_up30, rev_down30,
                  operating_margin, gross_margin, market_cap, fwd_pe_now,
                  ind, short_name, 1 if is_turnaround else 0, direction,
                  seg1, seg2, seg3, seg4))

            results.append(row)

        except Exception as e:
            errors.append(ticker)
            continue

    conn.commit()
    conn.close()

    # Save cache
    if cache_updated:
        try:
            with open(CACHE_PATH, 'w', encoding='utf-8') as f:
                json.dump(ticker_cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    log(f"Collection complete: {len(results)} processed, {len(no_data)} no data, {len(errors)} errors")
    return results


# ============================================================
# Screening Filters (Part 2 adapted for KR)
# ============================================================

def screen_candidates(results):
    """Apply Part 2 filters adapted for Korean market.

    Returns: (passed_list, filter_stats)
    """
    if not results:
        return [], {}

    total = len(results)
    stats = {'total': total}

    candidates = list(results)  # copy

    # Filter 1: adj_score > 9
    before = len(candidates)
    candidates = [r for r in candidates if _safe(r.get('adj_score')) > 9]
    stats['adj_score_pass'] = len(candidates)
    stats['adj_score_cut'] = before - len(candidates)

    # Filter 2: fwd_pe > 0
    before = len(candidates)
    candidates = [r for r in candidates if _safe(r.get('fwd_pe')) > 0]
    stats['fwd_pe_pass'] = len(candidates)
    stats['fwd_pe_cut'] = before - len(candidates)

    # Filter 3: eps_90d > 0 (90-day EPS change positive)
    before = len(candidates)
    candidates = [r for r in candidates if _safe(r.get('eps_90d')) > 0]
    stats['eps_90d_pass'] = len(candidates)
    stats['eps_90d_cut'] = before - len(candidates)

    # Filter 4: price >= 5000 KRW
    before = len(candidates)
    candidates = [r for r in candidates if _safe(r.get('price')) >= 5000]
    stats['price_pass'] = len(candidates)
    stats['price_cut'] = before - len(candidates)

    # Filter 5: price > MA120 (fallback MA60)
    before = len(candidates)
    ma_passed = []
    for r in candidates:
        price = _safe(r.get('price'))
        ma120 = _safe(r.get('ma120'))
        ma60 = _safe(r.get('ma60'))
        ma = ma120 if ma120 > 0 else ma60
        if ma > 0 and price > ma:
            ma_passed.append(r)
        elif ma <= 0:
            ma_passed.append(r)  # no MA data -> pass
    candidates = ma_passed
    stats['ma_pass'] = len(candidates)
    stats['ma_cut'] = before - len(candidates)

    # Filter 6: rev_growth >= 10%
    before = len(candidates)
    rev_passed = []
    rev_none_count = 0
    for r in candidates:
        rg = r.get('rev_growth')
        if rg is not None and _safe(rg) >= 0.10:
            rev_passed.append(r)
        elif rg is None:
            rev_none_count += 1  # no data -> exclude
    candidates = rev_passed
    stats['rev_pass'] = len(candidates)
    stats['rev_cut'] = before - len(candidates)
    stats['rev_none'] = rev_none_count

    # Filter 7: num_analysts >= 5 (KR: stricter than US)
    before = len(candidates)
    candidates = [r for r in candidates if _safe(r.get('num_analysts'), 0) >= 5]
    stats['analyst_pass'] = len(candidates)
    stats['analyst_cut'] = before - len(candidates)

    # Filter 8: down_ratio <= 30%
    before = len(candidates)
    dr_passed = []
    for r in candidates:
        up = _safe(r.get('rev_up30'), 0)
        dn = _safe(r.get('rev_down30'), 0)
        total_rev = up + dn
        if total_rev > 0:
            if dn / total_rev <= 0.3:
                dr_passed.append(r)
        else:
            dr_passed.append(r)
    candidates = dr_passed
    stats['downratio_pass'] = len(candidates)
    stats['downratio_cut'] = before - len(candidates)

    # Filter 9: Low margin exclude (OM<10% & GM<30%)
    before = len(candidates)
    margin_passed = []
    for r in candidates:
        om = r.get('operating_margin')
        gm = r.get('gross_margin')
        if om is not None and gm is not None:
            if _safe(om) < 0.10 and _safe(gm) < 0.30:
                continue  # exclude
        margin_passed.append(r)
    candidates = margin_passed
    stats['margin_pass'] = len(candidates)
    stats['margin_cut'] = before - len(candidates)

    # Filter 10: OP < 5% exclude
    before = len(candidates)
    op_passed = []
    for r in candidates:
        om = r.get('operating_margin')
        if om is not None and _safe(om) < 0.05:
            continue
        op_passed.append(r)
    candidates = op_passed
    stats['op_pass'] = len(candidates)
    stats['op_cut'] = before - len(candidates)

    # Filter 11: Commodity industries exclude
    before = len(candidates)
    candidates = [r for r in candidates if r.get('industry', 'N/A') not in COMMODITY_INDUSTRIES]
    stats['commodity_pass'] = len(candidates)
    stats['commodity_cut'] = before - len(candidates)

    # Filter 12: adj_gap must exist
    before = len(candidates)
    candidates = [r for r in candidates if r.get('adj_gap') is not None]
    stats['adj_gap_pass'] = len(candidates)
    stats['adj_gap_cut'] = before - len(candidates)

    # Filter 13: Not turnaround (optional - include but flag)
    # We'll keep turnarounds but flag them

    # Sort by adj_gap ascending (most undervalued first)
    candidates.sort(key=lambda r: _safe(r.get('adj_gap')))

    stats['final'] = len(candidates)
    return candidates, stats


# ============================================================
# Report Output
# ============================================================

def print_report(results, candidates, stats):
    """Print clean screening report."""
    print()
    print("=" * 80)
    print("  EPS Momentum Screening Report - Korean Stocks (KR)")
    print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    # --- Universe Summary ---
    print(f"\n{'='*60}")
    print("  UNIVERSE SUMMARY")
    print(f"{'='*60}")
    print(f"  Total tickers in universe:  {len(ALL_TICKERS)}")
    print(f"  Successfully fetched:       {len(results)}")
    print(f"  No EPS data:                {len(ALL_TICKERS) - len(results)}")

    # --- Filter Pipeline ---
    print(f"\n{'='*60}")
    print("  FILTER PIPELINE")
    print(f"{'='*60}")
    print(f"  {'Filter':<30} {'Pass':>6} {'Cut':>6}")
    print(f"  {'-'*42}")
    print(f"  {'Total with data':<30} {stats.get('total',0):>6}")
    filters = [
        ('adj_score > 9', 'adj_score'),
        ('fwd_pe > 0', 'fwd_pe'),
        ('eps_90d > 0', 'eps_90d'),
        ('price >= 5000 KRW', 'price'),
        ('price > MA120/60', 'ma'),
        ('rev_growth >= 10%', 'rev'),
        ('num_analysts >= 5', 'analyst'),
        ('down_ratio <= 30%', 'downratio'),
        ('margin (OM/GM)', 'margin'),
        ('OP >= 5%', 'op'),
        ('not commodity', 'commodity'),
        ('adj_gap exists', 'adj_gap'),
    ]
    for label, key in filters:
        p = stats.get(f'{key}_pass', '?')
        c = stats.get(f'{key}_cut', '?')
        extra = ""
        if key == 'rev' and stats.get('rev_none', 0) > 0:
            extra = f"  ({stats['rev_none']} had no rev data)"
        print(f"  {label:<30} {p:>6} {f'-{c}':>6}{extra}")
    print(f"  {'-'*42}")
    print(f"  {'FINAL CANDIDATES':<30} {stats.get('final',0):>6}")

    # --- Top 20 Candidates ---
    print(f"\n{'='*80}")
    print("  TOP 20 CANDIDATES (adj_gap ascending = most undervalued)")
    print(f"{'='*80}")

    top20 = candidates[:20]
    if not top20:
        print("  No candidates passed all filters.")
        return

    print(f"  {'#':>2} {'Ticker':<12} {'Name':<20} {'Industry':<10} "
          f"{'Price':>10} {'FwdPE':>7} {'AdjGap':>8} {'Score':>7} "
          f"{'EPS90d':>7} {'Rev%':>6} {'Anlst':>5} {'Trend'}")
    print(f"  {'-'*120}")

    for i, r in enumerate(top20, 1):
        ticker = r['ticker']
        name = (r.get('short_name') or ticker)[:19]
        ind = (r.get('industry_kr') or '?')[:9]
        price = _safe(r.get('price'))
        fwd_pe = _safe(r.get('fwd_pe'))
        adj_gap = _safe(r.get('adj_gap'))
        score = _safe(r.get('adj_score'))
        eps_90d = _safe(r.get('eps_90d'))
        rev = _safe(r.get('rev_growth'))
        analysts = int(_safe(r.get('num_analysts'), 0))
        trend = r.get('trend_lights', '')

        price_str = f"{price:,.0f}" if price > 0 else "N/A"
        pe_str = f"{fwd_pe:.1f}" if fwd_pe and fwd_pe > 0 else "N/A"
        gap_str = f"{adj_gap:+.1f}%" if adj_gap is not None else "N/A"
        score_str = f"{score:.1f}"
        eps_str = f"{eps_90d:+.1f}%" if eps_90d else "N/A"
        rev_str = f"{min(rev*100, 999):.0f}%" if rev is not None else "N/A"

        print(f"  {i:>2} {ticker:<12} {name:<20} {ind:<10} "
              f"{price_str:>10} {pe_str:>7} {gap_str:>8} {score_str:>7} "
              f"{eps_str:>7} {rev_str:>6} {analysts:>5} {trend}")

    # --- Segment Detail ---
    print(f"\n{'='*80}")
    print("  SEGMENT DETAIL (seg4=oldest -> seg1=recent, min_seg)")
    print(f"{'='*80}")
    print(f"  {'#':>2} {'Ticker':<12} {'seg4':>8} {'seg3':>8} {'seg2':>8} {'seg1':>8} {'min_seg':>8} {'Turnaround'}")
    print(f"  {'-'*70}")

    for i, r in enumerate(top20, 1):
        ticker = r['ticker']
        s1, s2, s3, s4 = _safe(r.get('seg1')), _safe(r.get('seg2')), _safe(r.get('seg3')), _safe(r.get('seg4'))
        ms = _safe(r.get('min_seg'))
        ta = "Y" if r.get('is_turnaround') else ""
        print(f"  {i:>2} {ticker:<12} {s4:>+8.2f} {s3:>+8.2f} {s2:>+8.2f} {s1:>+8.2f} {ms:>+8.2f} {ta:>10}")

    # --- Sector Distribution ---
    print(f"\n{'='*60}")
    print("  SECTOR DISTRIBUTION (Top 20)")
    print(f"{'='*60}")
    sector_count = {}
    for r in top20:
        ind = r.get('industry_kr') or '기타'
        sector_count[ind] = sector_count.get(ind, 0) + 1
    for sect, cnt in sorted(sector_count.items(), key=lambda x: -x[1]):
        bar = '|' * cnt
        print(f"  {sect:<15} {cnt:>3} {bar}")

    # --- All Data Summary ---
    print(f"\n{'='*60}")
    print("  ALL DATA SUMMARY (all fetched tickers)")
    print(f"{'='*60}")
    # Score distribution
    scores = [_safe(r.get('adj_score')) for r in results if r.get('adj_score') is not None]
    if scores:
        print(f"  adj_score: mean={statistics.mean(scores):.1f}, "
              f"median={statistics.median(scores):.1f}, "
              f"min={min(scores):.1f}, max={max(scores):.1f}")

    adj_gaps = [_safe(r.get('adj_gap')) for r in results if r.get('adj_gap') is not None]
    if adj_gaps:
        print(f"  adj_gap:   mean={statistics.mean(adj_gaps):.1f}, "
              f"median={statistics.median(adj_gaps):.1f}, "
              f"min={min(adj_gaps):.1f}, max={max(adj_gaps):.1f}")

    analyst_counts = [int(_safe(r.get('num_analysts'), 0)) for r in results]
    if analyst_counts:
        print(f"  analysts:  mean={statistics.mean(analyst_counts):.1f}, "
              f"min={min(analyst_counts)}, max={max(analyst_counts)}")

    # Industry distribution of ALL fetched
    all_ind = {}
    for r in results:
        ind = r.get('industry_kr') or '기타'
        all_ind[ind] = all_ind.get(ind, 0) + 1
    print(f"\n  Industry distribution (top 15 of {len(all_ind)} industries):")
    for sect, cnt in sorted(all_ind.items(), key=lambda x: -x[1])[:15]:
        print(f"    {sect:<20} {cnt:>3}")

    print(f"\n{'='*80}")
    print(f"  DB saved to: {DB_PATH}")
    print(f"{'='*80}")
    print()


# ============================================================
# Main
# ============================================================

def main():
    t_start = time.time()

    init_db()

    # Collect data
    results = collect_data(ALL_TICKERS)

    # Screen
    candidates, stats = screen_candidates(results)

    # Report
    print_report(results, candidates, stats)

    elapsed = time.time() - t_start
    log(f"Total elapsed: {elapsed:.0f}s ({elapsed/60:.1f}m)")


if __name__ == '__main__':
    main()
