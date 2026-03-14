"""
EPS Momentum System v9.0 - NTM (Next Twelve Months) EPS 기반

핵심:
- NTM EPS: endDate 기반 시간 가중 블렌딩 (0y/+1y)
- Score = seg1+seg2+seg3+seg4 (4개 독립 구간, ±100% 캡)
- adj_score = score × (1 + clamp(direction/30, -0.3, +0.3))
- 트래픽 라이트: 12개 기본 패턴 + 🟩🟥 강도 수식어
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# 유니버스 정의
# ============================================================

# 지수별 티커
INDICES = {
    'NASDAQ_100': [
        'AAPL','ABNB','ADBE','ADI','ADP','ADSK','AEP','ALNY','AMAT','AMD',
        'AMGN','AMZN','APP','ARM','ASML','AVGO','AXON','BKNG','BKR','CCEP',
        'CDNS','CEG','CHTR','CMCSA','COST','CPRT','CRWD','CSCO','CSGP','CSX',
        'CTAS','CTSH','DASH','DDOG','DXCM','EA','EXC','FANG','FAST','FER',
        'FTNT','GEHC','GILD','GOOG','GOOGL','HON','IDXX','INSM','INTC','INTU',
        'ISRG','KDP','KHC','KLAC','LIN','LRCX','MAR','MCHP','MDLZ','MELI',
        'META','MNST','MPWR','MRVL','MSFT','MSTR','MU','NFLX','NVDA','NXPI',
        'ODFL','ORLY','PANW','PAYX','PCAR','PDD','PEP','PLTR','PYPL','QCOM',
        'REGN','ROP','ROST','SBUX','SHOP','SNPS','STX','TEAM','TMUS','TRI',
        'TSLA','TTWO','TXN','VRSK','VRTX','WBD','WDAY','WDC','WMT','XEL','ZS'
    ],
    'SP500': [
        'A','AAPL','ABBV','ABNB','ABT','ACGL','ACN','ADBE','ADI','ADM',
        'ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ','AJG','AKAM',
        'ALB','ALGN','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP',
        'AMT','AMZN','ANET','AON','AOS','APA','APD','APH','APO','APP',
        'APTV','ARE','ARES','ATO','AVB','AVGO','AVY','AWK','AXON','AXP',
        'AZO','BA','BAC','BALL','BAX','BBY','BDX','BEN','BF-B','BG',
        'BIIB','BK','BKNG','BKR','BLDR','BLK','BMY','BR','BRK-B','BRO',
        'BSX','BX','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE',
        'CBRE','CCI','CCL','CDNS','CDW','CEG','CF','CFG','CHD','CHRW',
        'CHTR','CI','CINF','CL','CLX','CMCSA','CME','CMG','CMI','CMS',
        'CNC','CNP','COF','COIN','COO','COP','COR','COST','CPAY','CPB',
        'CPRT','CPT','CRH','CRL','CRM','CRWD','CSCO','CSGP','CSX','CTAS',
        'CTRA','CTSH','CTVA','CVNA','CVS','CVX','D','DAL','DASH','DAY',
        'DD','DDOG','DE','DECK','DELL','DG','DGX','DHI','DHR','DIS',
        'DLR','DLTR','DOC','DOV','DOW','DPZ','DRI','DTE','DUK','DVA',
        'DVN','DXCM','EA','EBAY','ECL','ED','EFX','EG','EIX','EL',
        'ELV','EME','EMR','EOG','EPAM','EQIX','EQR','EQT','ERIE','ES',
        'ESS','ETN','ETR','EVRG','EW','EXC','EXE','EXPD','EXPE','EXR',
        'F','FANG','FAST','FCX','FDS','FDX','FE','FFIV','FICO','FIS',
        'FISV','FITB','FIX','FOXA','FRT','FSLR','FTNT','FTV','GD',
        'GDDY','GE','GEHC','GEN','GEV','GILD','GIS','GL','GLW','GM',
        'GNRC','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS',
        'HBAN','HCA','HD','HIG','HII','HLT','HOLX','HON','HOOD','HPE',
        'HPQ','HRL','HSIC','HST','HSY','HUBB','HUM','HWM','IBKR','IBM',
        'ICE','IDXX','IEX','IFF','INCY','INTC','INTU','INVH','IP','IQV',
        'IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JBL','JCI',
        'JKHY','JNJ','JPM','KDP','KEY','KEYS','KHC','KIM','KKR','KLAC',
        'KMB','KMI','KO','KR','KVUE','L','LDOS','LEN','LH','LHX',
        'LII','LIN','LLY','LMT','LNT','LOW','LRCX','LULU','LUV','LVS',
        'LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP','MCK',
        'MCO','MDLZ','MDT','MET','META','MGM','MKC','MLM','MMM','MNST',
        'MO','MOH','MOS','MPC','MPWR','MRK','MRNA','MRSH','MS','MSCI',
        'MSFT','MSI','MTB','MTCH','MTD','MU','NCLH','NDAQ','NDSN','NEE',
        'NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS',
        'NUE','NVDA','NVR','NWS','NWSA','NXPI','O','ODFL','OKE','OMC',
        'ON','ORCL','ORLY','OTIS','OXY','PANW','PAYC','PAYX','PCAR','PCG',
        'PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PLD',
        'PLTR','PM','PNC','PNR','PNW','PODD','POOL','PPG','PPL','PRU',
        'PSA','PSKY','PSX','PTC','PWR','PYPL','Q','QCOM','RCL','REG',
        'REGN','RF','RJF','RL','RMD','ROK','ROL','ROP','ROST','RSG',
        'RTX','RVTY','SBAC','SBUX','SCHW','SHW','SJM','SLB','SMCI','SNA',
        'SNDK','SNPS','SO','SOLV','SPG','SPGI','SRE','STE','STLD','STT',
        'STX','STZ','SW','SWK','SWKS','SYF','SYK','SYY','T','TAP',
        'TDG','TDY','TECH','TEL','TER','TFC','TGT','TJX','TKO','TMO',
        'TMUS','TPL','TPR','TRGP','TRMB','TROW','TRV','TSCO','TSLA','TSN',
        'TT','TTD','TTWO','TXN','TXT','TYL','UAL','UBER','UDR','UHS',
        'ULTA','UNH','UNP','UPS','URI','USB','V','VICI','VLO','VLTO',
        'VMC','VRSK','VRSN','VRTX','VST','VTR','VTRS','VZ','WAB','WAT',
        'WBD','WDAY','WDC','WEC','WELL','WFC','WM','WMB','WMT','WRB',
        'WSM','WST','WTW','WY','WYNN','XEL','XOM','XYL','XYZ','YUM',
        'ZBH','ZBRA','ZTS'
    ],
    'SP400_MidCap': [
        'AA','AAL','AAON','ACI','ACM','ADC','AEIS','AFG','AGCO','AHR',
        'AIT','ALGM','ALK','ALLY','ALV','AM','AMG','AMH','AMKR','AN',
        'ANF','APG','APPF','AR','ARMK','ARW','ASB','ASGN','ASH','ATI',
        'ATR','AVAV','AVNT','AVT','AVTR','AXTA','AYI','BAH','BBWI','BC',
        'BCO','BDC','BHF','BILL','BIO','BJ','BKH','BLD','BLKB','BMRN',
        'BRBR','BRKR','BROS','BRX','BSY','BURL','BWA','BWXT','BYD','CACI',
        'CAR','CART','CASY','CAVA','CBSH','CBT','CCK','CDP','CELH','CFR',
        'CG','CGNX','CHDN','CHE','CHH','CHRD','CHWY','CIEN','CLF','CLH',
        'CMC','CNH','CNM','CNO','CNX','CNXC','COHR','COKE','COLB','COLM',
        'COTY','CPRI','CR','CRBG','CROX','CRS','CRUS','CSL','CUBE','CUZ',
        'CVLT','CW','CXT','CYTK','DAR','DBX','DCI','DINO','DKS','DLB',
        'DOCS','DOCU','DT','DTM','DUOL','DY','EEFT','EGP','EHC','ELAN',
        'ELF','ELS','ENS','ENSG','ENTG','EPR','EQH','ESAB','ESNT','EVR',
        'EWBC','EXEL','EXLS','EXP','EXPO','FAF','FBIN','FCFS','FCN','FFIN',
        'FHI','FHN','FIVE','FLEX','FLG','FLO','FLR','FLS','FN','FNB',
        'FND','FNF','FOUR','FR','FTI','G','GAP','GATX','GBCI','GEF',
        'GGG','GHC','GLPI','GME','GMED','GNTX','GPK','GT','GTLS','GTM',
        'GWRE','GXO','H','HAE','HALO','HGV','HIMS','HL','HLI','HLNE',
        'HOG','HOMB','HQY','HR','HRB','HWC','HXL','IBOC','IDA','ILMN',
        'INGR','IPGP','IRT','ITT','JAZZ','JEF','JHG','JLL','KBH','KBR',
        'KD','KEX','KMPR','KNF','KNSL','KNX','KRC','KRG','KTOS','LAD',
        'LAMR','LEA','LECO','LFUS','LITE','LIVN','LNTH','LOPE','LPX','LSCC',
        'LSTR','M','MANH','MASI','MAT','MEDP','MIDD','MKSI','MLI','MMS',
        'MORN','MP','MSA','MSM','MTDR','MTG','MTN','MTSI','MTZ','MUR',
        'MUSA','MZTI','NBIX','NEU','NFG','NJR','NLY','NNN','NOV','NOVT',
        'NSA','NTNX','NVST','NVT','NWE','NXST','NXT','NYT','OC','OGE',
        'OGS','OHI','OKTA','OLED','OLLI','OLN','ONB','ONTO','OPCH','ORA',
        'ORI','OSK','OVV','OZK','PAG','PATH','PB','PBF','PCTY','PEGA',
        'PEN','PFGC','PII','PINS','PK','PLNT','PNFP','POR','POST','PPC',
        'PR','PRI','PSN','PSTG','PVH','QLYS','R','RBA','RBC','REXR',
        'RGA','RGEN','RGLD','RH','RLI','RMBS','RNR','ROIV','RPM','RRC',
        'RRX','RS','RYAN','RYN','SAIA','SAIC','SAM','SARO','SATS','SBRA',
        'SCI','SEIC','SF','SFM','SGI','SHC','SIGI','SLAB','SLGN','SLM',
        'SMG','SNX','SON','SPXC','SR','SSB','SSD','ST','STAG','STRL',
        'STWD','SWX','SYNA','TCBI','TEX','THC','THG','THO','TKR','TLN',
        'TMHC','TNL','TOL','TREX','TRU','TTC','TTEK','TTMI','TWLO','TXNM',
        'TXRH','UBSI','UFPI','UGI','ULS','UMBF','UNM','USFD','UTHR','VAL',
        'VC','VFC','VLY','VMI','VNO','VNOM','VNT','VOYA','VVV','WAL',
        'WBS','WCC','WEX','WFRD','WH','WHR','WING','WLK','WMG','WMS',
        'WPC','WSO','WTFC','WTRG','WTS','WWD','XPO','XRAY','YETI','ZION'
    ],
}

# 업종 한글 매핑 (yfinance industry → 한글 축약)
INDUSTRY_MAP = {
    # Technology
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
    'Electronics & Computer Distribution': '전자유통',
    'Electronic Gaming & Multimedia': '게임',
    'Solar': '태양광',
    # Internet & Media
    'Internet Content & Information': '인터넷',
    'Internet Retail': '온라인유통',
    'Entertainment': '엔터',
    'Broadcasting': '방송',
    'Publishing': '출판',
    'Advertising Agencies': '광고',
    'Telecom Services': '통신',
    # Financial
    'Banks - Regional': '지역은행',
    'Banks - Diversified': '대형은행',
    'Asset Management': '자산운용',
    'Capital Markets': '자본시장',
    'Credit Services': '신용서비스',
    'Financial Data & Stock Exchanges': '금융데이터',
    'Insurance - Property & Casualty': '손해보험',
    'Insurance - Life': '생명보험',
    'Insurance - Diversified': '종합보험',
    'Insurance - Specialty': '특수보험',
    'Insurance - Reinsurance': '재보험',
    'Insurance Brokers': '보험중개',
    'Financial Conglomerates': '금융지주',
    # Healthcare
    'Medical Devices': '의료기기',
    'Medical Instruments & Supplies': '의료용품',
    'Medical Care Facilities': '의료시설',
    'Medical Distribution': '의약유통',
    'Diagnostics & Research': '진단연구',
    'Drug Manufacturers - General': '대형제약',
    'Drug Manufacturers - Specialty & Generic': '특수제약',
    'Biotechnology': '바이오',
    'Healthcare Plans': '건강보험',
    'Health Information Services': '의료정보',
    # Industrials
    'Aerospace & Defense': '방산',
    'Specialty Industrial Machinery': '산업기계',
    'Farm & Heavy Construction Machinery': '중장비',
    'Engineering & Construction': '건설',
    'Building Products & Equipment': '건축자재',
    'Building Materials': '건자재',
    'Electrical Equipment & Parts': '전기장비',
    'Tools & Accessories': '공구',
    'Industrial Distribution': '산업유통',
    'Specialty Business Services': '비즈니스서비스',
    'Consulting Services': '컨설팅',
    'Security & Protection Services': '보안',
    'Waste Management': '폐기물',
    'Pollution & Treatment Controls': '환경',
    'Conglomerates': '복합기업',
    'Integrated Freight & Logistics': '물류',
    'Railroads': '철도',
    'Trucking': '트럭운송',
    'Airlines': '항공',
    'Marine Shipping': '해운',
    'Rental & Leasing Services': '렌탈리스',
    # Consumer Cyclical
    'Auto Parts': '자동차부품',
    'Auto Manufacturers': '자동차',
    'Auto & Truck Dealerships': '자동차딜러',
    'Restaurants': '외식',
    'Specialty Retail': '전문소매',
    'Discount Stores': '할인점',
    'Home Improvement Retail': '홈인테리어',
    'Apparel Retail': '의류소매',
    'Apparel Manufacturing': '의류제조',
    'Department Stores': '백화점',
    'Footwear & Accessories': '신발잡화',
    'Luxury Goods': '명품',
    'Residential Construction': '주택건설',
    'Furnishings, Fixtures & Appliances': '가구가전',
    'Resorts & Casinos': '리조트카지노',
    'Gambling': '도박',
    'Lodging': '숙박',
    'Travel Services': '여행',
    'Recreational Vehicles': '레저차량',
    'Leisure': '레저',
    'Personal Services': '생활서비스',
    # Consumer Defensive
    'Packaged Foods': '식품',
    'Beverages - Non-Alcoholic': '음료',
    'Beverages - Brewers': '맥주',
    'Beverages - Wineries & Distilleries': '주류',
    'Confectioners': '제과',
    'Household & Personal Products': '생활용품',
    'Tobacco': '담배',
    'Grocery Stores': '식료품점',
    'Food Distribution': '식품유통',
    'Education & Training Services': '교육',
    # Real Estate
    'REIT - Specialty': '리츠특수',
    'REIT - Residential': '리츠주거',
    'REIT - Retail': '리츠소매',
    'REIT - Industrial': '리츠산업',
    'REIT - Healthcare Facilities': '리츠의료',
    'REIT - Office': '리츠오피스',
    'REIT - Hotel & Motel': '리츠호텔',
    'REIT - Mortgage': '리츠모기지',
    'REIT - Diversified': '리츠복합',
    'Real Estate Services': '부동산서비스',
    # Energy
    'Oil & Gas E&P': '석유가스',
    'Oil & Gas Midstream': '석유미드스트림',
    'Oil & Gas Equipment & Services': '석유장비',
    'Oil & Gas Refining & Marketing': '석유정제',
    'Oil & Gas Integrated': '석유종합',
    # Utilities
    'Utilities - Regulated Electric': '전력',
    'Utilities - Regulated Gas': '가스',
    'Utilities - Regulated Water': '수도',
    'Utilities - Diversified': '유틸복합',
    'Utilities - Independent Power Producers': '독립발전',
    'Utilities - Renewable': '신재생',
    # Basic Materials
    'Specialty Chemicals': '특수화학',
    'Chemicals': '화학',
    'Agricultural Inputs': '농업',
    'Steel': '철강',
    'Aluminum': '알루미늄',
    'Copper': '구리',
    'Gold': '금',
    'Other Precious Metals & Mining': '귀금속',
    'Other Industrial Metals & Mining': '산업금속',
    'Lumber & Wood Production': '목재',
    'Metal Fabrication': '금속가공',
    'Packaging & Containers': '포장재',
    'Farm Products': '농산물',
    # Other
    'N/A': '기타',
}


# ============================================================
# NTM (Next Twelve Months) EPS 계산
# ============================================================

MIN_NTM_EPS = 1.0  # 턴어라운드 판별 기준 ($1.00)

def calculate_ntm_eps(stock, today=None):
    """NTM EPS 계산 - endDate 기반 시간 가중 블렌딩

    yfinance eps_trend의 5개 스냅샷(current, 7d, 30d, 60d, 90d)에 대해
    각각 forward 12개월 윈도우를 계산하고, 0y/+1y EPS를 시간 비례로 블렌딩한다.

    Args:
        stock: yf.Ticker 객체
        today: 기준일 (None이면 현재 날짜)

    Returns:
        dict {'current': float, '7d': float, '30d': float, '60d': float, '90d': float}
        or None if data unavailable
    """
    if today is None:
        today = datetime.now()

    eps_trend = stock.eps_trend
    if eps_trend is None or len(eps_trend) == 0:
        return None

    if '0y' not in eps_trend.index or '+1y' not in eps_trend.index:
        return None

    # endDate 추출 (raw _earnings_trend 데이터에서)
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

    # 5개 스냅샷에 대해 NTM 계산
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

        # 각 fiscal year와의 겹침 일수
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
    """NTM EPS 기반 모멘텀 스코어 계산

    Score = seg1 + seg2 + seg3 + seg4
    각 segment는 인접 스냅샷 간 변화율(%)

    방향 보정 (adj_score):
    recent = (seg1 + seg2) / 2, old = (seg3 + seg4) / 2
    direction = recent - old
    adj_score = score × (1 + clamp(direction/30, -0.3, +0.3))
    → 1σ(3.67) 가속 시 ~12% 보너스, 감속 시 ~12% 패널티

    Args:
        ntm_values: calculate_ntm_eps() 반환 dict

    Returns:
        tuple (score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction)
    """
    nc = ntm_values['current']
    n7 = ntm_values['7d']
    n30 = ntm_values['30d']
    n60 = ntm_values['60d']
    n90 = ntm_values['90d']

    # 턴어라운드 판별: 현재 또는 90일전 |EPS| < $1.00
    # (기저가 낮으면 변화율이 과대 계산되므로)
    is_turnaround = abs(nc) < MIN_NTM_EPS or abs(n90) < MIN_NTM_EPS

    # 각 segment 변화율 계산 (±100% 캡으로 이상치 방지)
    SEG_CAP = 100
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100)) if n7 != 0 else 0
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100)) if n30 != 0 else 0
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100)) if n60 != 0 else 0
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100)) if n90 != 0 else 0

    score = seg1 + seg2 + seg3 + seg4

    # 방향 보정: 최근 vs 과거 세그먼트 평균 차이
    DIRECTION_DIVISOR = 30  # 1σ(3.67) → ~12% 보정
    DIRECTION_CAP = 0.3     # 최대 ±30% 보정
    recent_avg = (seg1 + seg2) / 2
    old_avg = (seg3 + seg4) / 2
    direction = recent_avg - old_avg
    direction_mult = max(-DIRECTION_CAP, min(DIRECTION_CAP, direction / DIRECTION_DIVISOR))
    adj_score = score * (1 + direction_mult)

    return score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction


def calculate_eps_change_90d(ntm_values):
    """90일 이익변화율 계산 (고객 표시용)

    Args:
        ntm_values: calculate_ntm_eps() 반환 dict

    Returns:
        float (percentage) or None
    """
    nc = ntm_values['current']
    n90 = ntm_values['90d']

    if n90 == 0:
        return None

    return (nc - n90) / abs(n90) * 100


def get_trend_lights(seg1, seg2, seg3, seg4):
    """추세 신호등 생성 (90d/60d/30d/7d 순서 = 과거→현재)

    5단계 아이콘: 🔥(>20%) ☀️(5~20%) 🌤️(1~5%) ☁️(±1%) 🌧️(<-1%)
    12개 기본 패턴 + 🔥 강도 수식어

    Args:
        seg1-seg4: calculate_ntm_score()에서 반환된 segment 값 (%)

    Returns:
        tuple: (lights_str, description)
    """
    segs = [seg4, seg3, seg2, seg1]  # 과거→현재 순서

    # 5단계 아이콘: 🔥폭등 ☀️강한상승 🌤️상승 ☁️보합 🌧️하락
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

    lights_str = ''.join(lights)
    has_fire = '🔥' in lights
    has_rain = '🌧️' in lights

    # 구간 분류 (|s| > 0.5 = 유의미한 변화)
    pos_count = sum(1 for s in segs if s > 0.5)
    neg_count = sum(1 for s in segs if s < -0.5)
    flat_count = 4 - pos_count - neg_count

    recent_avg = (segs[2] + segs[3]) / 2  # seg2, seg1
    old_avg = (segs[0] + segs[1]) / 2     # seg4, seg3

    old_pos = sum(1 for s in segs[:2] if s > 0.5)
    old_neg = sum(1 for s in segs[:2] if s < -0.5)
    recent_pos = sum(1 for s in segs[2:] if s > 0.5)
    recent_neg = sum(1 for s in segs[2:] if s < -0.5)

    # --- 12개 기본 패턴 ---
    if flat_count >= 3:
        base = '횡보'
    elif neg_count >= 3:
        base = '하락'
    elif neg_count == 0:
        # 전구간 양수 (또는 보합) — 피크 위치 + 형태로 하위 패턴 분류
        total = sum(segs)
        max_seg = max(segs)
        spread = max_seg - min(segs)
        mean_val = total / 4

        if mean_val < 1.5:
            base = '전구간 상승'
        elif spread / max(mean_val, 0.01) < 0.8:
            base = '꾸준한 상승'
        else:
            # 진동 감지: 인접 구간 차이 부호가 교차 (high-low-high-low)
            diffs = [segs[i + 1] - segs[i] for i in range(3)]
            signs = [1 if d > 1 else (-1 if d < -1 else 0) for d in diffs]
            is_zigzag = (signs[0] * signs[1] < 0 and signs[1] * signs[2] < 0)
            min_amp = min(abs(d) for d in diffs)

            if is_zigzag and min_amp > 3:
                base = '상승 등락'
            else:
                # 동률 시 최근(오른쪽) 우선 — segs[3]=seg1이 가장 최근
                peak_idx = max(range(4), key=lambda i: (segs[i], i))
                if peak_idx == 3:  # seg1(최근)이 피크
                    others_avg = sum(segs[:3]) / 3
                    if max_seg > others_avg * 3:
                        base = '최근 급상향'
                    else:
                        base = '상향 가속'
                elif peak_idx == 2:  # seg2(중반)가 피크
                    if segs[3] < max_seg * 0.6:
                        base = '중반 강세'
                    else:
                        base = '상향 가속'
                else:  # seg3/seg4(초반)가 피크
                    # 단조감소 체크: 피크 이후 계속 하락
                    mono_decline = all(
                        segs[j] > segs[j + 1] + 0.5
                        for j in range(peak_idx, 3)
                    )
                    if mono_decline and segs[3] <= 5:
                        # 계속 하락 + 최근 ☀️ 미만 → 진짜 둔화
                        base = '상향 둔화'
                    elif min(segs[2:]) > 1:  # 최근 🌤️ 이상
                        base = '중반 강세'
                    else:
                        base = '상향 둔화'
    elif old_neg > old_pos and recent_pos > recent_neg and recent_avg > old_avg:
        base = '반등'
    elif old_pos > old_neg and recent_neg > recent_pos and old_avg > recent_avg:
        base = '추세 전환'
    else:
        base = '등락 반복'

    # --- 🔥 강도 수식어 ---
    if has_fire and has_rain:
        desc = {'반등': '급락 후 반등', '추세 전환': '급격한 전환'}.get(base, '급등락')
    elif has_fire:
        desc = {
            '전구간 상승': '폭발적 상승',
            '꾸준한 상승': '폭발적 상승',
            '상향 가속': '폭발적 가속',
            '최근 급상향': '폭발적 급상향',
            '중반 강세': '중반 급등',
            '상승 등락': '폭발적 등락',
            '상향 둔화': '급등 후 둔화',
            '반등': '폭발적 반등',
        }.get(base, base)
    elif has_rain:
        desc = {
            '하락': '급락',
            '추세 전환': '급격한 전환',
            '반등': '급락 후 반등',
            '등락 반복': '급등락',
        }.get(base, base)
    else:
        desc = base

    return lights_str, desc
