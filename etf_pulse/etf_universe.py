"""미국 ETF universe — AUM 상위 약 250개

운용사별 main ETF + 카테고리별 핵심 ETF.
yfinance로 AUM 확인 후 추후 자동 갱신 가능.
"""

# 카테고리별 정리 (운용사 묶음)
ETF_UNIVERSE = {
    # ━━━ Core 지수 (S&P500, Nasdaq, Russell, Total) ━━━
    'core_us': [
        'VOO', 'IVV', 'SPY', 'SPLG',  # S&P 500
        'QQQ', 'QQQM',  # Nasdaq 100
        'VTI', 'ITOT', 'SCHB',  # Total Market
        'IWM', 'IJR', 'VB', 'SCHA',  # Small Cap
        'IJH', 'MDY', 'VO', 'SCHM',  # Mid Cap
        'RSP', 'EQAL',  # Equal Weight
        'DIA',  # Dow
        'SPHQ', 'QUAL', 'MTUM', 'USMV',  # Factor
        'IWF', 'IWD', 'VUG', 'VTV',  # Growth/Value
        'SCHD', 'VIG', 'VYM', 'DGRO', 'NOBL',  # Dividend
    ],

    # ━━━ 국제 / 신흥국 ━━━
    'international': [
        'VEA', 'IEFA', 'EFA', 'SCHF', 'IDEV',  # Developed
        'VWO', 'IEMG', 'EEM', 'SCHE',  # Emerging
        'VXUS', 'IXUS', 'CWI',  # ex-US Total
        'VT',  # Total World
        'EWJ', 'DXJ',  # Japan
        'FXI', 'MCHI', 'KWEB', 'ASHR', 'CWEB', 'YINN',  # China
        'EWZ', 'EWY', 'INDA', 'INDY',  # Other EM
        'VPL', 'VGK', 'EZU',  # Asia/EU
    ],

    # ━━━ 섹터 (Sector ETF — 전통 11 GICS 섹터 + sub) ━━━
    'sectors': [
        'SOXX', 'SMH', 'XSD',  # Semiconductors (sub-sector)
        'XLK', 'VGT', 'IYW', 'FTEC',  # Tech
        'XLF', 'VFH', 'IYF', 'KRE', 'KBE',  # Financials
        'XLV', 'VHT', 'IYH', 'IBB', 'XBI',  # Health
        'XLE', 'VDE', 'IYE', 'XOP', 'OIH', 'AMLP',  # Energy
        'XLI', 'VIS', 'IYJ', 'XAR',  # Industrials
        'XLY', 'VCR', 'IYC',  # Consumer Disc
        'XLP', 'VDC', 'IYK',  # Consumer Staples
        'XLU', 'VPU', 'IDU', 'FAN', 'ICLN',  # Utilities/Clean
        'XLB', 'VAW', 'IYM', 'GDX', 'GDXJ', 'XME',  # Materials/Mining
        'XLRE', 'VNQ', 'IYR', 'SCHH', 'USRT',  # REIT
        'XLC', 'VOX',  # Comm
    ],

    # ━━━ 테마 / 트렌드 (특정 산업 베팅) ━━━
    'themes': [
        'BOTZ', 'ROBO', 'IRBO', 'AIQ',  # AI/Robot
        'ARKK', 'ARKG', 'ARKW', 'ARKQ', 'ARKF', 'ARKX',  # ARK
        'TAN', 'PBW', 'QCLN',  # Clean Energy
        'IPAY', 'PEJ',  # Fintech/Travel
        'BLOK', 'BITQ', 'IBIT', 'FBTC', 'BITO',  # Crypto
        'JETS',  # Airlines
        'MOO', 'WEAT', 'CORN', 'SOYB',  # Agriculture
        'CIBR', 'HACK',  # Cybersecurity
        'COPX',  # Copper
        'URA', 'NLR',  # Uranium/Nuclear
        'DRIV', 'IDRV', 'KARS',  # EV
    ],

    # ━━━ 채권 ━━━
    'bonds': [
        'AGG', 'BND', 'BNDX', 'IUSB',  # Aggregate
        'TLT', 'EDV', 'VGLT', 'TLH',  # Long Treasury
        'IEF', 'VGIT',  # Mid Treasury
        'SHY', 'VGSH', 'BIL', 'SHV', 'JPST',  # Short Treasury
        'LQD', 'VCIT', 'VCLT', 'VCSH',  # Corp
        'HYG', 'JNK', 'USHY', 'SHYG',  # High Yield
        'MUB', 'VTEB',  # Muni
        'TIP', 'VTIP', 'SCHP',  # TIPS
        'EMB', 'PCY',  # EM bonds
        'USFR', 'BSV',  # FRN
    ],

    # ━━━ 원자재 / 헷지 ━━━
    'commodity_hedge': [
        'GLD', 'IAU', 'GLDM',  # Gold
        'SLV', 'SIVR',  # Silver
        'PPLT', 'PALL',  # Platinum/Palladium
        'USO', 'BNO', 'DBO',  # Oil
        'UNG', 'UNL',  # Natural Gas
        'DBC', 'GSG', 'PDBC', 'COMT',  # Commodity baskets
        'CPER',  # Copper
        'WOOD', 'CUT',  # Wood
        'VIXY', 'VXX',  # Vol hedge
    ],

    # ━━━ 인컴 / 옵션 / 레버리지 ━━━
    'income_lev': [
        'JEPI', 'JEPQ',  # Covered call income
        'QYLD', 'XYLD', 'RYLD',  # Buy-write
        'NUSI', 'DIVO',  # Income
        'TQQQ', 'SQQQ', 'UPRO', 'SPXU', 'SH', 'SDS',  # Lev/Inverse
        'SOXL', 'SOXS',  # Semi lev
        'FAS', 'FAZ', 'TNA', 'TZA',  # Sector lev
        'LABU', 'LABD',  # Bio lev
        'NVDL', 'NVDS', 'NVDX',  # NVDA single-stock lev
        'AAPU', 'AAPD',  # AAPL single-stock lev
        'TSLL', 'TSLZ',  # TSLA single-stock lev
        'MSTU', 'MSTZ',  # MSTR single-stock lev
        'CONL',  # Coinbase 2X lev
        'GOOL',  # GOOGL 2X lev
        'METU',  # META 2X lev
        'BITX',  # Bitcoin 2X lev
        'AMZU',  # AMZN 2X lev
        'PYPL',  # PayPal (이건 종목이긴 한데)
    ],

    # ━━━ ESG / 사회적 책임 ━━━
    'esg': [
        'ESGU', 'SUSL', 'SUSA',  # MSCI USA ESG
        'ESGD', 'ESGE',  # International ESG
        'ICLN',  # Clean energy (themes에도 있음)
        'NUGT',  # ESG growth
        'CRBN', 'KRMA',  # Low carbon, impact
    ],

    # ━━━ 신규/소형 액티브 ETF (2024-2026 인기) ━━━
    'active_new': [
        'BUFR',  # FT Cboe Vest US Equity Buffer (downside protection)
        'BUFB',  # Same buffer different month
        'PFFV',  # InfraCap Variable Rate Preferred (인컴 액티브)
        'PYZ',  # Invesco DWA Basic Materials Momentum
        'GLDM',  # Cheaper gold (이미 commodity에 있음)
        'IBHI', 'IBHJ',  # iShares iBonds Treasury target maturity
        'BSCT', 'BSCU',  # Invesco BulletShares (target maturity)
        'CGUS',  # Capital Group US Equity
        'CGGO',  # Capital Group Growth Opps
        'AVUV',  # Avantis US Small Cap Value
        'AVUS',  # Avantis US Equity
        'AVEM',  # Avantis EM
        'CALF',  # Pacer US Small Cap Cash Cows
        'COWZ',  # Pacer US Cash Cows
    ],
}


def get_all_etfs():
    """평탄화된 unique ETF list"""
    seen = set()
    out = []
    for cat, tickers in ETF_UNIVERSE.items():
        for tk in tickers:
            if tk not in seen:
                seen.add(tk)
                out.append((tk, cat))
    return out


def get_category(ticker):
    """ticker → 카테고리"""
    for cat, tickers in ETF_UNIVERSE.items():
        if ticker in tickers:
            return cat
    return None


if __name__ == '__main__':
    all_etfs = get_all_etfs()
    print(f'총 ETF 수: {len(all_etfs)}')
    print(f'카테고리:')
    for cat, tickers in ETF_UNIVERSE.items():
        print(f'  {cat}: {len(tickers)}개')
