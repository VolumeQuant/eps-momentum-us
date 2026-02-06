"""
EPS Momentum System v8.0 - NTM (Next Twelve Months) EPS 기반

핵심 변경:
- +1y → NTM EPS 전환: endDate 기반 시간 가중 블렌딩
- Score = seg1+seg2+seg3+seg4: 4개 독립 구간 합산
- |NTM EPS| < $1.00 → 턴어라운드 카테고리 분리
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
    ]
}

# 업종 한글 매핑 (yfinance industry → 한글 축약)
INDUSTRY_MAP = {
    # Technology
    'Semiconductors': '반도체',
    'Semiconductor Equipment & Materials': '반도체장비',
    'Software - Application': '응용SW',
    'Software - Infrastructure': '인프라SW',
    'Information Technology Services': 'IT서비스',
    'Computer Hardware': 'HW',
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

    Args:
        ntm_values: calculate_ntm_eps() 반환 dict

    Returns:
        tuple (score, seg1, seg2, seg3, seg4, is_turnaround)
    """
    nc = ntm_values['current']
    n7 = ntm_values['7d']
    n30 = ntm_values['30d']
    n60 = ntm_values['60d']
    n90 = ntm_values['90d']

    # 턴어라운드 판별: 5개 값 중 하나라도 |EPS| < $1.00
    is_turnaround = any(abs(v) < MIN_NTM_EPS for v in [nc, n7, n30, n60, n90])

    # 각 segment 변화율 계산
    seg1 = (nc - n7) / abs(n7) * 100 if n7 != 0 else 0
    seg2 = (n7 - n30) / abs(n30) * 100 if n30 != 0 else 0
    seg3 = (n30 - n60) / abs(n60) * 100 if n60 != 0 else 0
    seg4 = (n60 - n90) / abs(n90) * 100 if n90 != 0 else 0

    score = seg1 + seg2 + seg3 + seg4

    return score, seg1, seg2, seg3, seg4, is_turnaround


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


def get_trend_arrows(seg1, seg2, seg3, seg4):
    """추세 화살표 생성 (90d/60d/30d/7d 순서 = 과거→현재)

    Args:
        seg1-seg4: calculate_ntm_score()에서 반환된 segment 값

    Returns:
        str: 예) "↑↑↑↓"
    """
    arrows = []
    for seg in [seg4, seg3, seg2, seg1]:  # 과거→현재 순서
        arrows.append('↑' if seg > 0 else '↓')
    return ''.join(arrows)
