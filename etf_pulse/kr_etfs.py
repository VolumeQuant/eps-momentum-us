"""ETF Pulse — 한국 ETF prototype (확장 아이디어)

한국거래소(KRX) ETF universe + yfinance로 가격 데이터.
한국 ETF 티커는 6자리 숫자 + .KS 또는 .KQ 접미사.

운용사 매핑:
- 삼성: KODEX (KOSPI 200, 미국 S&P500 등)
- 미래에셋: TIGER (반도체, AI, 미국 나스닥 등)
- KB자산운용: KBSTAR
- 한국투자: ACE
- 한화자산운용: ARIRANG
- 신한자산운용: SOL

특이점:
- 한국 ETF는 환헤지(H) vs 비헤지 구분 중요
- 액티브 ETF 비중 빠르게 증가
- 단일종목 추종 ETF 인기
"""
import sys
import yfinance as yf
import pandas as pd
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')


# 한국 주요 ETF (티커 + 카테고리)
KR_ETFS = {
    # ━━━ 한국 코어 (KOSPI200, 코스닥150) ━━━
    'kr_core': {
        '069500.KS': 'KODEX 200',
        '102110.KS': 'TIGER 200',
        '148020.KS': 'KBSTAR 200',
        '226490.KS': 'KODEX 코스피',
        '229200.KS': 'KODEX 코스닥150',
        '233740.KS': 'KODEX 코스닥150 레버리지',
    },
    # ━━━ 미국 지수 추종 ━━━
    'kr_us_index': {
        '360750.KS': 'TIGER 미국S&P500',
        '379800.KS': 'KODEX 미국S&P500TR',
        '381180.KS': 'TIGER 미국필라델피아반도체나스닥',
        '133690.KS': 'TIGER 미국나스닥100',
        '195930.KS': 'TIGER 미국MSCI리츠(합성)',
        '433330.KS': 'SOL 미국S&P500',
    },
    # ━━━ 한국 섹터 ━━━
    'kr_sectors': {
        '091160.KS': 'KODEX 반도체',
        '091170.KS': 'KODEX 은행',
        '266390.KS': 'KODEX 200ESG',
        '305720.KS': 'KODEX 2차전지산업',
        '329200.KS': 'KODEX 헬스케어',
        '364980.KS': 'TIGER 차이나전기차SOLACTIVE',
    },
    # ━━━ 액티브 (운용자 매매) ━━━
    'kr_active': {
        '396500.KS': 'TIGER AI코리아그로스액티브',
        '465330.KS': 'TIGER 미국AI빅테크10액티브',
        '474220.KS': 'TIGER 미국나스닥100타겟데일리커버드콜액티브',
    },
    # ━━━ 채권 ━━━
    'kr_bonds': {
        '114820.KS': 'TIGER 국채3년',
        '136340.KS': 'TIGER 단기채',
        '305080.KS': 'TIGER 미국채10년선물',
        '305540.KS': 'KODEX 종합채권(AA-이상)액티브',
    },
    # ━━━ 원자재 / 헷지 ━━━
    'kr_commodity': {
        '132030.KS': 'KODEX 골드선물(H)',
        '139320.KS': 'TIGER 200선물인버스',
        '252670.KS': 'KODEX 200선물인버스2X',
        '233160.KS': 'KODEX 미국달러선물',
    },
    # ━━━ 테마 / 신규 인기 ━━━
    'kr_theme': {
        '391590.KS': 'KODEX K-신재생에너지액티브',
        '441060.KS': 'KODEX 미국빅테크10(H)',
        '465670.KS': 'TIGER 미국S&P500타겟데일리커버드콜2X',
    },
}


def get_all_kr_etfs():
    out = []
    for cat, etfs in KR_ETFS.items():
        for tk, name in etfs.items():
            out.append((tk, cat, name))
    return out


def fetch_kr_etf(ticker, name, category):
    """단일 한국 ETF fetch"""
    t = yf.Ticker(ticker)
    try:
        info = t.info
        hist = t.history(period='40d')['Close'].dropna()
        if len(hist) < 2:
            return None
        price = float(hist.iloc[-1])
        prev = float(hist.iloc[-2])
        day_ret = (price - prev) / prev * 100 if prev > 0 else 0
        vol_hist = t.history(period='40d')['Volume'].dropna()
        vol_today = int(vol_hist.iloc[-1]) if len(vol_hist) > 0 else 0
        vol_avg = int(vol_hist.iloc[:-1].tail(30).mean()) if len(vol_hist) > 1 else 0
        return {
            'ticker': ticker, 'name': name, 'category': category,
            'price_krw': price, 'day_return': day_ret,
            'volume': vol_today, 'avg_volume_30d': vol_avg,
            'volume_spike': vol_today / vol_avg if vol_avg > 0 else 0,
            'aum_krw_b': (info.get('totalAssets', 0) or 0) / 1e9,  # 십억 원
            'currency': info.get('currency', 'KRW'),
        }
    except Exception as e:
        return {'ticker': ticker, 'error': str(e)[:80]}


def main():
    print('=== 한국 ETF 데이터 prototype (yfinance) ===\n')
    etfs = get_all_kr_etfs()
    print(f'대상 {len(etfs)}개')

    results = []
    errors = []
    for i, (tk, cat, name) in enumerate(etfs, 1):
        r = fetch_kr_etf(tk, name, cat)
        if r is None or 'error' in r:
            errors.append(r if r else {'ticker': tk, 'error': 'no data'})
            continue
        results.append(r)
        if i % 10 == 0:
            print(f'  {i}/{len(etfs)}')

    print(f'\n성공: {len(results)} / 실패: {len(errors)}')

    if errors:
        print('\n실패한 ETF:')
        for e in errors[:10]:
            print(f'  {e["ticker"]}: {e.get("error", "")[:60]}')

    if results:
        print('\n[수익률 Top 10]')
        results_sorted = sorted(results, key=lambda x: -x['day_return'])
        for r in results_sorted[:10]:
            print(f'  {r["ticker"]:<12} {r["name"][:30]:<30} {r["day_return"]:+6.2f}%  '
                  f'AUM ₩{r["aum_krw_b"]:.0f}B  vol_spike {r["volume_spike"]:.2f}x')

        print('\n[거래량 spike Top 5]')
        spikes = sorted([r for r in results if r['volume_spike'] > 1.5],
                       key=lambda x: -x['volume_spike'])
        for r in spikes[:5]:
            print(f'  {r["ticker"]:<12} {r["name"][:30]:<30} {r["volume_spike"]:.2f}x  '
                  f'ret {r["day_return"]:+.2f}%')


if __name__ == '__main__':
    main()
