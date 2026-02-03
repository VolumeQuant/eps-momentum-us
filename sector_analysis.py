"""섹터 분산 없이 스크리닝 - 주도 섹터/테마 파악 (Broad vs Narrow 분석)"""
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
from datetime import datetime
warnings.filterwarnings('ignore')

MIN_DOLLAR_VOLUME = 20_000_000
MIN_SCORE = 4.0

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
    ]
}

# 섹터별 레버리지 ETF (Broad 신호용)
SECTOR_ETF = {
    'Technology': {'1x': 'XLK', '2x': 'ROM', '3x': 'TECL'},
    'Semiconductor': {'1x': 'SMH', '2x': 'USD', '3x': 'SOXL'},  # v7.0 추가
    'Financial Services': {'1x': 'XLF', '2x': 'UYG', '3x': 'FAS'},
    'Healthcare': {'1x': 'XLV', '2x': 'RXL', '3x': 'CURE'},
    'Consumer Cyclical': {'1x': 'XLY', '2x': 'UCC', '3x': 'WANT'},
    'Industrials': {'1x': 'XLI', '2x': 'UXI', '3x': 'DUSL'},
    'Energy': {'1x': 'XLE', '2x': 'DIG', '3x': 'ERX'},
    'Basic Materials': {'1x': 'XLB', '2x': 'UYM', '3x': 'MATL'},
    'Consumer Defensive': {'1x': 'XLP', '2x': 'UGE', '3x': None},
    'Utilities': {'1x': 'XLU', '2x': 'UPW', '3x': 'UTSL'},
    'Real Estate': {'1x': 'XLRE', '2x': 'URE', '3x': 'DRN'},
    'Communication Services': {'1x': 'XLC', '2x': None, '3x': None},
}

# 테마별 ETF (Narrow 신호용 - industry 기반)
THEME_ETF = {
    # Technology 하위
    'Semiconductors': {'1x': 'SMH', '2x': 'USD', '3x': 'SOXL'},
    'Software—Infrastructure': {'1x': 'IGV', '2x': None, '3x': None},
    'Software—Application': {'1x': 'IGV', '2x': None, '3x': None},
    'Internet Content & Information': {'1x': 'ARKW', '2x': None, '3x': 'WEBL'},

    # Basic Materials 하위
    'Copper': {'1x': 'COPX', '2x': None, '3x': None},
    'Steel': {'1x': 'SLX', '2x': None, '3x': None},
    'Gold': {'1x': 'GDX', '2x': None, '3x': 'NUGT'},
    'Silver': {'1x': 'SIL', '2x': None, '3x': None},

    # Energy 하위
    'Oil & Gas E&P': {'1x': 'XOP', '2x': None, '3x': None},
    'Oil & Gas Equipment & Services': {'1x': 'OIH', '2x': None, '3x': None},

    # Healthcare 하위
    'Biotechnology': {'1x': 'XBI', '2x': 'BIB', '3x': 'LABU'},
    'Medical Devices': {'1x': 'IHI', '2x': None, '3x': None},

    # Financials 하위
    'Banks—Regional': {'1x': 'KRE', '2x': None, '3x': 'DPST'},
    'Insurance—Property & Casualty': {'1x': 'KIE', '2x': None, '3x': None},

    # Real Estate 하위
    'REIT—Residential': {'1x': 'REZ', '2x': None, '3x': None},
    'REIT—Industrial': {'1x': 'INDS', '2x': None, '3x': None},

    # Consumer 하위
    'Auto Manufacturers': {'1x': 'CARZ', '2x': None, '3x': None},
    'Restaurants': {'1x': 'EATZ', '2x': None, '3x': None},
    'Home Improvement Retail': {'1x': 'XHB', '2x': None, '3x': None},
}

# HHI 임계값 (이 값 이상이면 NARROW)
HHI_THRESHOLD = 0.25


def calc_momentum(current, d7, d30, d60):
    """모멘텀 점수 계산 (Kill Switch 포함)"""
    if pd.isna(current) or pd.isna(d60) or d60 == 0:
        return None, None, False

    eps_chg = (current - d60) / abs(d60) * 100
    if eps_chg > 200 or eps_chg < -80:
        return None, None, False

    # Kill Switch: 최근 하향이면 제외
    if pd.notna(d7) and current < d7:
        return None, eps_chg, False

    score = 0
    if pd.notna(d7) and current > d7:
        score += 3
    if pd.notna(d7) and pd.notna(d30) and d7 > d30:
        score += 2
    elif pd.notna(d7) and pd.notna(d30) and d7 < d30:
        score -= 1
    if pd.notna(d30) and pd.notna(d60) and d30 > d60:
        score += 1
    elif pd.notna(d30) and pd.notna(d60) and d30 < d60:
        score -= 1

    score += eps_chg / 5
    return round(score, 2), round(eps_chg, 2), True


def analyze_signal_breadth(df, sector):
    """
    섹터 내 신호 집중도 분석 (HHI 기반)

    Returns:
        signal_type: 'BROAD' or 'NARROW'
        dominant_industry: NARROW일 경우 주도 industry
        hhi: Herfindahl-Hirschman Index
    """
    sector_df = df[df['sector'] == sector]
    if len(sector_df) == 0:
        return 'BROAD', None, 0.0

    industry_dist = sector_df.groupby('industry').agg({
        'ticker': 'count',
        'score': 'sum'  # 점수 가중 집중도
    }).rename(columns={'ticker': 'count'})

    # 점수 기반 HHI (단순 종목수보다 점수 합계가 더 의미있음)
    total_score = industry_dist['score'].sum()
    if total_score == 0:
        return 'BROAD', None, 0.0

    shares = industry_dist['score'] / total_score
    hhi = (shares ** 2).sum()

    # 주도 industry 찾기
    dominant_industry = industry_dist['score'].idxmax()
    dominant_share = shares[dominant_industry]

    if hhi >= HHI_THRESHOLD or dominant_share >= 0.5:
        return 'NARROW', dominant_industry, round(hhi, 3)
    else:
        return 'BROAD', None, round(hhi, 3)


def get_etf_recommendation(sector, signal_type, dominant_industry):
    """신호 유형에 따른 ETF 추천"""
    if signal_type == 'NARROW' and dominant_industry:
        theme_etf = THEME_ETF.get(dominant_industry, {})
        if theme_etf.get('1x'):
            return theme_etf, dominant_industry

    # BROAD이거나 테마 ETF가 없으면 섹터 ETF
    return SECTOR_ETF.get(sector, {}), sector


def main():
    all_tickers = {}
    for idx, tickers in INDICES.items():
        for t in tickers:
            if t not in all_tickers:
                all_tickers[t] = idx

    candidates = []
    killed = 0

    print('='*80)
    print('EPS Momentum Sector Analysis - Broad vs Narrow Signal Detection')
    print(f'Date: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('='*80)

    for i, (ticker, idx) in enumerate(all_tickers.items()):
        try:
            stock = yf.Ticker(ticker)
            trend = stock.eps_trend
            info = stock.info

            if trend is None or '+1y' not in trend.index:
                continue

            eps = trend.loc['+1y']
            score, chg, passed = calc_momentum(
                eps.get('current'), eps.get('7daysAgo'),
                eps.get('30daysAgo'), eps.get('60daysAgo')
            )

            if not passed:
                killed += 1
                continue

            if score is None or score < MIN_SCORE:
                continue

            hist = stock.history(period='1mo')
            if len(hist) < 20:
                continue

            price = hist['Close'].iloc[-1]
            vol = hist['Volume'].mean()
            dollar_vol = price * vol

            if dollar_vol < MIN_DOLLAR_VOLUME:
                continue

            ma20 = hist['Close'].tail(20).mean()
            if price <= ma20:
                continue

            sector = info.get('sector', 'Other')
            industry = info.get('industry', 'Other')

            candidates.append({
                'ticker': ticker,
                'index': idx,
                'score': score,
                'eps_chg': chg,
                'sector': sector,
                'industry': industry,
                'price': round(price, 2),
                'dollar_vol_M': round(dollar_vol/1e6, 1)
            })

            if (i+1) % 100 == 0:
                print(f'  Progress: {i+1}/{len(all_tickers)}')
        except:
            continue

    print(f'\nKill Switch excluded: {killed}')
    print(f'Passed screening: {len(candidates)}')

    if len(candidates) == 0:
        print('\nNo candidates found.')
        return

    df = pd.DataFrame(candidates)
    df = df.sort_values('score', ascending=False)

    # =========================================================================
    # 섹터별 분포
    # =========================================================================
    print('\n' + '='*80)
    print('SECTOR DISTRIBUTION')
    print('='*80)

    sector_stats = df.groupby('sector').agg({
        'ticker': 'count',
        'score': 'mean',
        'eps_chg': 'mean'
    }).rename(columns={'ticker': 'count'})

    sector_stats = sector_stats.sort_values('count', ascending=False)
    sector_stats['pct'] = sector_stats['count'] / len(df) * 100

    print(f"{'Sector':<25} {'N':>5} {'%':>7} {'AvgScore':>10} {'AvgEPS%':>10}")
    print('-'*60)
    for sector, row in sector_stats.iterrows():
        print(f"{sector:<25} {int(row['count']):>5} {row['pct']:>6.1f}% {row['score']:>+9.1f} {row['eps_chg']:>+9.1f}%")

    # =========================================================================
    # Broad vs Narrow 분석 + ETF 추천
    # =========================================================================
    print('\n' + '='*80)
    print('SIGNAL BREADTH ANALYSIS & ETF RECOMMENDATIONS')
    print('='*80)
    print(f"{'Sector':<22} {'Type':<8} {'HHI':>6} {'Dominant Theme':<25} {'ETF Recommendation':<20}")
    print('-'*85)

    etf_recommendations = []

    for sector in sector_stats.index:
        signal_type, dominant_industry, hhi = analyze_signal_breadth(df, sector)
        etf_info, etf_basis = get_etf_recommendation(sector, signal_type, dominant_industry)

        etf_1x = etf_info.get('1x', '-')
        etf_3x = etf_info.get('3x', '-') or '-'
        etf_str = f"{etf_1x} / {etf_3x}"

        theme_display = dominant_industry if signal_type == 'NARROW' else '-'

        print(f"{sector:<22} {signal_type:<8} {hhi:>5.2f} {theme_display:<25} {etf_str:<20}")

        pct = sector_stats.loc[sector, 'pct']
        etf_recommendations.append({
            'sector': sector,
            'signal_type': signal_type,
            'hhi': hhi,
            'dominant_theme': dominant_industry,
            'etf_1x': etf_1x,
            'etf_3x': etf_3x,
            'weight_pct': pct
        })

    # =========================================================================
    # 상위 섹터 Industry 상세 분석
    # =========================================================================
    print('\n' + '='*80)
    print('TOP SECTORS - INDUSTRY BREAKDOWN')
    print('='*80)

    for sector in sector_stats.head(4).index:
        sector_df = df[df['sector'] == sector]
        print(f"\n[{sector}] - {len(sector_df)} stocks")
        print('-'*60)

        industry_breakdown = sector_df.groupby('industry').agg({
            'ticker': lambda x: list(x),
            'score': ['count', 'sum', 'mean']
        })
        industry_breakdown.columns = ['tickers', 'count', 'score_sum', 'avg_score']
        industry_breakdown = industry_breakdown.sort_values('score_sum', ascending=False)

        for industry, row in industry_breakdown.iterrows():
            tickers_str = ', '.join(row['tickers'][:5])
            if len(row['tickers']) > 5:
                tickers_str += f' +{len(row["tickers"])-5}'
            print(f"  {industry:<35} [{row['count']:>2}] {tickers_str}")

    # =========================================================================
    # Top 40 종목
    # =========================================================================
    print('\n' + '='*80)
    print('TOP 40 STOCKS (No Sector Diversification)')
    print('='*80)
    print(f"{'#':>3} {'Ticker':<7} {'Sector':<18} {'Industry':<28} {'Score':>7} {'EPS%':>8}")
    print('-'*80)

    for i, (_, row) in enumerate(df.head(40).iterrows()):
        sector_short = row['sector'][:17] if len(row['sector']) > 17 else row['sector']
        industry_short = row['industry'][:27] if len(row['industry']) > 27 else row['industry']
        print(f"{i+1:>3} {row['ticker']:<7} {sector_short:<18} {industry_short:<28} {row['score']:>+6.1f} {row['eps_chg']:>+7.1f}%")

    # =========================================================================
    # 최종 ETF 추천 요약
    # =========================================================================
    print('\n' + '='*80)
    print('FINAL ETF RECOMMENDATIONS')
    print('='*80)

    recs = pd.DataFrame(etf_recommendations)
    recs = recs[recs['weight_pct'] >= 10]  # 10% 이상만

    print("\n** High Conviction Plays (Sector Weight >= 10%) **\n")
    print(f"{'Sector':<22} {'Signal':<8} {'Theme':<25} {'1x ETF':>10} {'3x ETF':>10}")
    print('-'*80)

    for _, r in recs.iterrows():
        theme = r['dominant_theme'] if r['dominant_theme'] else r['sector']
        print(f"{r['sector']:<22} {r['signal_type']:<8} {theme:<25} {r['etf_1x']:>10} {r['etf_3x']:>10}")

    print("\n" + "="*80)
    print("KEY INSIGHT:")
    print("  - NARROW signal: Use THEME ETF (e.g., SOXL for Semiconductors)")
    print("  - BROAD signal:  Use SECTOR ETF (e.g., DUSL for Industrials)")
    print("="*80)


# ============================================================
# v7.0 신규 함수: Sector Booster (ETF 추천)
# ============================================================

def get_sector_etf_recommendation(screening_df, top_n=10, min_count=3, config=None):
    """
    Sector Booster: TOP N 종목 중 동일 섹터 집중 시 ETF 추천

    TOP 10 종목 중 동일 섹터가 3개 이상이면 관련 ETF 추천.
    특정 섹터에 쏠림을 기회로 활용.

    Args:
        screening_df: 스크리닝 결과 DataFrame (actionable_score 정렬됨)
        top_n: 상위 N개 종목 기준 (기본 10)
        min_count: 섹터당 최소 종목 수 (기본 3)
        config: 설정 딕셔너리

    Returns:
        list: [{'sector': str, 'count': int, 'pct': float,
                'etf_1x': str, 'etf_3x': str}]
    """
    if screening_df is None or screening_df.empty:
        return []

    # config에서 설정 로드
    if config and 'sector_booster' in config:
        sb_config = config['sector_booster']
        if not sb_config.get('enabled', True):
            return []
        top_n = sb_config.get('top_n', top_n)
        min_count = sb_config.get('min_sector_count', min_count)

    # TOP N 종목 추출
    top_df = screening_df.head(top_n)
    if 'sector' not in top_df.columns:
        return []

    # 섹터별 카운트
    sector_counts = top_df['sector'].value_counts()

    recommendations = []
    for sector, count in sector_counts.items():
        if count >= min_count:
            etf_info = SECTOR_ETF.get(sector, {})
            recommendations.append({
                'sector': sector,
                'count': count,
                'pct': round(count / top_n * 100, 1),
                'etf_1x': etf_info.get('1x'),
                'etf_2x': etf_info.get('2x'),
                'etf_3x': etf_info.get('3x'),
            })

    # count 내림차순 정렬
    recommendations.sort(key=lambda x: x['count'], reverse=True)

    return recommendations


def format_etf_recommendation_text(recommendations):
    """
    ETF 추천을 텔레그램 메시지 포맷으로 변환

    Args:
        recommendations: get_sector_etf_recommendation() 결과

    Returns:
        str: 포맷된 텍스트
    """
    if not recommendations:
        return ""

    lines = []
    for rec in recommendations:
        sector = rec['sector']
        count = rec['count']
        pct = rec['pct']
        etf_1x = rec.get('etf_1x', '-')
        etf_3x = rec.get('etf_3x', '-')

        line = f"{sector} {count}개({pct}%)"
        if etf_1x and etf_3x:
            line += f" → {etf_1x}/{etf_3x}"
        elif etf_1x:
            line += f" → {etf_1x}"

        lines.append(line)

    return "\n".join(lines)


if __name__ == '__main__':
    main()
