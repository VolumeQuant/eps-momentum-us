"""
EPS Revision Momentum Daily Screener
- 매일 실행하여 +7% 이상 EPS 상향 종목 스크리닝
- 편입/편출 종목 추적
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 설정
THRESHOLD = 7.0  # EPS 변화율 임계값 (%)
DATA_DIR = 'screener_data'
HISTORY_FILE = os.path.join(DATA_DIR, 'screening_history.json')

# 지수별 티커 리스트
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
        'FND','BLDR','BLD','UFPI','WMS','TREX','AAON','TTC',
        'FTV','ROK','ITT','IEX','FELE','SNA',
        'VIRT','SNEX','PIPR','EVR','HLI','MKTX','AVNT','HLIT',
        'INSP','LNTH','MED','MMSI','PRGO','SUPN','UTHR','XRAY','ABCB','BANF',
        'BANR','CASH','CBSH','CVBF','EFSC','FFBC','FFIN','FULT','GBCI','HOPE',
        'INDB','NBTB','NWBI','ONB','OZK','PEBO','SFBS','SFNC','TBBK',
        'TOWN','UBSI','WSBC','WSFS','WTFC','BKE',
        'DKS','GCO','GIII','HVT','KTB','LZB','PLCE','PVH','RVLV'
    ]
}


def ensure_data_dir():
    """데이터 디렉토리 생성"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)


def load_history():
    """이전 스크리닝 히스토리 로드"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            return json.load(f)
    return {'screenings': [], 'last_qualified': {}}


def save_history(history):
    """히스토리 저장"""
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2, default=str)


def get_eps_data(ticker):
    """개별 종목 EPS 데이터 수집"""
    try:
        stock = yf.Ticker(ticker)
        trend = stock.eps_trend

        if trend is None or len(trend) == 0 or '+1y' not in trend.index:
            return None

        eps_row = trend.loc['+1y']
        current_eps = eps_row.get('current', np.nan)
        eps_90d = eps_row.get('90daysAgo', np.nan)

        if pd.isna(current_eps) or pd.isna(eps_90d) or eps_90d == 0:
            return None

        eps_chg = (current_eps - eps_90d) / abs(eps_90d) * 100

        # 이상치 필터링
        if eps_chg > 200 or eps_chg < -80:
            return None

        # 현재가 및 수익률
        hist = stock.history(period='5d')
        if len(hist) == 0:
            return None

        current_price = hist['Close'].iloc[-1]

        return {
            'ticker': ticker,
            'eps_chg': round(eps_chg, 2),
            'current_eps': round(current_eps, 2),
            'eps_90d': round(eps_90d, 2),
            'price': round(current_price, 2)
        }
    except Exception as e:
        return None


def screen_index(index_name, tickers):
    """지수별 스크리닝"""
    print(f'\n[{index_name}] 스크리닝 중... ({len(tickers)}개)')

    qualified = []
    all_data = []

    for i, ticker in enumerate(tickers):
        data = get_eps_data(ticker)
        if data:
            all_data.append(data)
            if data['eps_chg'] >= THRESHOLD:
                qualified.append(data)

        if (i + 1) % 30 == 0:
            print(f'  진행: {i+1}/{len(tickers)} (통과: {len(qualified)}개)')

    print(f'  완료: {len(all_data)}개 수집, {len(qualified)}개 통과')

    return qualified, all_data


def compare_with_previous(current_qualified, previous_qualified):
    """이전 데이터와 비교하여 편입/편출 계산"""
    current_tickers = set(item['ticker'] for item in current_qualified)
    previous_tickers = set(previous_qualified) if previous_qualified else set()

    new_entries = current_tickers - previous_tickers  # 신규 편입
    exits = previous_tickers - current_tickers        # 편출
    maintained = current_tickers & previous_tickers   # 유지

    return {
        'new': list(new_entries),
        'exit': list(exits),
        'maintained': list(maintained)
    }


def run_screening():
    """전체 스크리닝 실행"""
    ensure_data_dir()
    history = load_history()

    today = datetime.now().strftime('%Y-%m-%d')
    print('='*70)
    print(f'EPS Revision Momentum Screener - {today}')
    print(f'임계값: +{THRESHOLD}% 이상')
    print('='*70)

    results = {}
    all_qualified = []

    for index_name, tickers in INDICES.items():
        qualified, all_data = screen_index(index_name, tickers)

        # 이전 데이터와 비교
        prev_qualified = history['last_qualified'].get(index_name, [])
        comparison = compare_with_previous(qualified, prev_qualified)

        results[index_name] = {
            'qualified': qualified,
            'comparison': comparison,
            'total_screened': len(all_data)
        }

        all_qualified.extend(qualified)

        # 히스토리 업데이트
        history['last_qualified'][index_name] = [item['ticker'] for item in qualified]

    # 결과 출력
    print('\n' + '='*70)
    print('스크리닝 결과')
    print('='*70)

    for index_name, data in results.items():
        qualified = data['qualified']
        comp = data['comparison']

        print(f'\n[{index_name}] - {len(qualified)}개 통과')
        print('-'*50)

        if comp['new']:
            print(f'  [NEW] 신규 편입: {", ".join(sorted(comp["new"]))}')
        if comp['exit']:
            print(f'  [OUT] 편출: {", ".join(sorted(comp["exit"]))}')
        if comp['maintained']:
            print(f'  [---] 유지: {", ".join(sorted(comp["maintained"]))}')

        if qualified:
            print(f'\n  {"티커":<8} {"EPS변화율":>10} {"현재EPS":>10} {"90일전EPS":>10} {"현재가":>10}')
            print('  ' + '-'*48)
            for item in sorted(qualified, key=lambda x: x['eps_chg'], reverse=True):
                print(f'  {item["ticker"]:<8} {item["eps_chg"]:>+9.1f}% {item["current_eps"]:>10.2f} {item["eps_90d"]:>10.2f} ${item["price"]:>9.2f}')

    # 전체 요약
    print('\n' + '='*70)
    print('전체 요약')
    print('='*70)

    total_new = sum(len(r['comparison']['new']) for r in results.values())
    total_exit = sum(len(r['comparison']['exit']) for r in results.values())
    total_qualified = len(set(item['ticker'] for item in all_qualified))

    print(f'총 통과 종목: {total_qualified}개 (중복 제외)')
    print(f'신규 편입: {total_new}개')
    print(f'편출: {total_exit}개')

    # 전체 통과 종목 리스트
    unique_qualified = {}
    for item in all_qualified:
        if item['ticker'] not in unique_qualified or item['eps_chg'] > unique_qualified[item['ticker']]['eps_chg']:
            unique_qualified[item['ticker']] = item

    print(f'\n전체 통과 종목 (EPS 변화율 순):')
    for item in sorted(unique_qualified.values(), key=lambda x: x['eps_chg'], reverse=True):
        print(f'  {item["ticker"]:<8} +{item["eps_chg"]:.1f}%')

    # 히스토리에 오늘 결과 추가
    history['screenings'].append({
        'date': today,
        'threshold': THRESHOLD,
        'results': {
            idx: {
                'qualified': [item['ticker'] for item in data['qualified']],
                'new': data['comparison']['new'],
                'exit': data['comparison']['exit']
            }
            for idx, data in results.items()
        }
    })

    # 최근 30일만 유지
    history['screenings'] = history['screenings'][-30:]

    save_history(history)

    # CSV로 오늘 결과 저장
    today_file = os.path.join(DATA_DIR, f'screening_{today}.csv')
    df = pd.DataFrame(all_qualified)
    if len(df) > 0:
        df.to_csv(today_file, index=False)
        print(f'\n결과 저장: {today_file}')

    return results


def show_history(days=7):
    """최근 히스토리 조회"""
    history = load_history()

    print('='*70)
    print(f'최근 {days}일 스크리닝 히스토리')
    print('='*70)

    for screening in history['screenings'][-days:]:
        print(f'\n[{screening["date"]}]')
        for idx, data in screening['results'].items():
            new_str = f'+{len(data["new"])}' if data['new'] else ''
            exit_str = f'-{len(data["exit"])}' if data['exit'] else ''
            print(f'  {idx}: {len(data["qualified"])}개 {new_str} {exit_str}')
            if data['new']:
                print(f'    편입: {", ".join(data["new"])}')
            if data['exit']:
                print(f'    편출: {", ".join(data["exit"])}')


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'history':
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        show_history(days)
    else:
        run_screening()
