"""KR 종목 yfinance EPS 모멘텀 데이터 가용성 단일 종목 probe.

목적: 171090.KQ (선익시스템) 기준으로 어떤 데이터가 가용한지 확인.
US 시스템의 핵심 입력:
  - stock.eps_trend (current/7d/30d/60d/90d 5개 스냅샷, 0y/+1y FY)
  - stock._analysis._earnings_trend (endDate 추출용)
  - stock.earnings_estimate (revUp30 / revDown30 / numAnalysts)
  - stock.info (rev_growth, marketCap, operatingMargins 등)
  - stock.calendar (earnings date)
"""
import sys
import yfinance as yf

sys.stdout.reconfigure(encoding='utf-8')

TICKER = '171090.KQ'  # 선익시스템


def probe(ticker):
    print(f'\n{"="*70}')
    print(f'KR yfinance probe: {ticker}')
    print(f'{"="*70}')

    t = yf.Ticker(ticker)

    # 1. eps_trend
    print('\n[1] eps_trend')
    try:
        et = t.eps_trend
        print(f'  type: {type(et)}')
        if et is None:
            print('  ❌ None')
        elif len(et) == 0:
            print('  ❌ empty')
        else:
            print(f'  index: {list(et.index)}')
            print(f'  columns: {list(et.columns)}')
            print(et)
    except Exception as e:
        print(f'  ❌ exception: {e}')

    # 2. raw _earnings_trend (endDate 추출용)
    print('\n[2] _analysis._earnings_trend (raw)')
    try:
        raw = t._analysis._earnings_trend
        if raw is None:
            print('  ❌ None')
        else:
            print(f'  len: {len(raw)}')
            for item in raw[:6]:
                p = item.get('period')
                ed = item.get('endDate')
                print(f'  period={p} endDate={ed}')
    except Exception as e:
        print(f'  ❌ exception: {e}')

    # 3. earnings_estimate (분석가 의견)
    print('\n[3] earnings_estimate (revUp30 / revDown30 / numAnalysts)')
    try:
        ee = t.earnings_estimate
        print(f'  type: {type(ee)}')
        if ee is None or len(ee) == 0:
            print('  ❌ empty')
        else:
            print(f'  index: {list(ee.index)}')
            print(f'  columns: {list(ee.columns)}')
            print(ee)
    except Exception as e:
        print(f'  ❌ exception: {e}')

    # 4. eps_revisions
    print('\n[4] eps_revisions (US 시스템 핵심 — rev_up30/rev_down30)')
    try:
        er = t.eps_revisions
        if er is None or len(er) == 0:
            print('  ❌ empty')
        else:
            print(f'  index: {list(er.index)}')
            print(f'  columns: {list(er.columns)}')
            print(er)
    except Exception as e:
        print(f'  ❌ exception: {e}')

    # 5. info — fundamentals
    print('\n[5] .info (선택 필드)')
    try:
        info = t.info
        keys = [
            'symbol', 'shortName', 'longName', 'currency',
            'marketCap', 'numberOfAnalystOpinions',
            'revenueGrowth', 'operatingMargins', 'grossMargins',
            'forwardEps', 'trailingEps', 'forwardPE', 'trailingPE',
            'earningsTimestamp', 'earningsDate',
        ]
        for k in keys:
            v = info.get(k)
            print(f'  {k}: {v}')
    except Exception as e:
        print(f'  ❌ exception: {e}')

    # 6. calendar
    print('\n[6] .calendar (어닝일)')
    try:
        cal = t.calendar
        print(f'  {cal}')
    except Exception as e:
        print(f'  ❌ exception: {e}')

    # 7. earnings_dates
    print('\n[7] .earnings_dates (최근 5)')
    try:
        ed = t.earnings_dates
        if ed is not None and len(ed) > 0:
            print(ed.head(5))
        else:
            print('  ❌ empty')
    except Exception as e:
        print(f'  ❌ exception: {e}')


if __name__ == '__main__':
    probe(TICKER)
