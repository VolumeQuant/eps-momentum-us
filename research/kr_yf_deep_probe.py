"""KR 시총 상위 ~500종목 yf EPS 모멘텀 데이터 심층 검증.

수집:
  - pykrx로 KOSPI/KOSDAQ 시총 정렬 → 상위 500
  - 병렬 yf probe (3 worker, 0.4s sleep)
  - eps_trend / eps_revisions / info / earnings_dates 풀 수집

저장:
  - research/kr_yf_deep_results.csv

분석 차원:
  - 시총 구간별 가용성 cliff
  - 분석가 커버리지 분포 (시장별, 시총별)
  - NaN 패턴 (5스냅샷 중 일부 / 0y vs +1y)
  - revenueGrowth 비정상 큰 값 비율
  - 어닝 점프 detection (90d→7d eps_trend 변화)
  - yf staleness 패턴 (7d 컬럼이 30d/60d와 같은지)
"""
import sys
import time
import csv
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
OUT_CSV = ROOT / 'research' / 'kr_yf_deep_results.csv'

TOP_N = 500
WORKERS = 3
SLEEP = 0.4


def get_universe():
    """KR 프로젝트 market_cap parquet에서 시총 상위 N종목.
    시장 분류(KS/KQ)는 yf probe 단계에서 자동 결정 (.KS 우선, 실패 시 .KQ)."""
    import glob
    files = sorted(glob.glob('C:/dev/claude code/quant_py-main/data_cache/market_cap_ALL_*.parquet'))
    if not files:
        raise RuntimeError('market_cap parquet 없음')
    latest = files[-1]
    print(f'시총 parquet 로드: {Path(latest).name}', flush=True)
    df = pd.read_parquet(latest)
    # 컬럼명 cp949 깨짐 — 위치로 접근: [종가, 시가총액, 거래량, 거래대금, 상장주식수]
    df.columns = ['close', 'mc', 'vol', 'val', 'shares']
    df = df.sort_values('mc', ascending=False).head(TOP_N)
    print(f'  top {TOP_N} 시총 추출 (대부분 우선주는 끝자리 ≠ 0)', flush=True)
    rows = []
    for tk, row in df.iterrows():
        # 우선주 제외 (끝자리 ≠ 0이면 우선주 가능성 높음)
        if not str(tk).endswith('0'):
            continue
        rows.append({
            'code': str(tk),
            'symbol': None,  # probe 단계에서 .KS/.KQ 결정
            'name': '',
            'market': '?',
            'mc_krw': float(row['mc']),
        })
    print(f'  보통주만 (우선주 제외): {len(rows)}종목', flush=True)
    return rows[:TOP_N]


def try_market(code):
    """code에 .KS 우선 시도 → eps_trend 없으면 .KQ. 시장 결정 + ticker 반환."""
    for mkt in ['KS', 'KQ']:
        sym = f'{code}.{mkt}'
        try:
            t = yf.Ticker(sym)
            et = t.eps_trend
            if et is not None and len(et) > 0:
                return sym, mkt, t
        except Exception:
            continue
        time.sleep(0.1)
    # 둘 다 실패 시 .KS 반환 (이후 단계에서 error 기록)
    return f'{code}.KS', '?', None


def probe(item):
    """단일 종목 yf 데이터 수집"""
    code = item['code']
    sym, mkt, t_probe = try_market(code)
    r = {
        'symbol': sym, 'name': item['name'], 'market': mkt,
        'mc_krw': item['mc_krw'],
        'eps_trend_ok': False, 'fy_complete_0y': False, 'fy_complete_1y': False,
        'snap_ok_count': 0,         # 5스냅샷 중 몇 개 not NaN (0y 기준)
        '0y_current': None, '0y_7d': None, '0y_30d': None, '0y_60d': None, '0y_90d': None,
        '1y_current': None, '1y_90d': None,
        'endDate_0y_ok': False, 'endDate_1y_ok': False,
        'rev_ok_0y': False,
        'up7': None, 'up30': None, 'dn30': None, 'dn7': None,
        'na': None, 'rev_growth': None, 'op_margin': None, 'gross_margin': None,
        'fwd_pe': None, 'fwd_eps': None,
        'earn_date_present': False,
        'error': None,
    }
    try:
        t = t_probe if t_probe is not None else yf.Ticker(sym)

        try:
            et = t.eps_trend
            if et is not None and len(et) > 0:
                r['eps_trend_ok'] = True
                cols = ['current', '7daysAgo', '30daysAgo', '60daysAgo', '90daysAgo']
                col_keys = ['current', '7d', '30d', '60d', '90d']
                if '0y' in et.index:
                    not_nan = 0
                    for c, k in zip(cols, col_keys):
                        if c in et.columns:
                            v = et.loc['0y', c]
                            if not pd.isna(v):
                                r[f'0y_{k}'] = float(v)
                                not_nan += 1
                    r['snap_ok_count'] = not_nan
                    r['fy_complete_0y'] = (not_nan == 5)
                if '+1y' in et.index:
                    v = et.loc['+1y', 'current'] if 'current' in et.columns else None
                    if not pd.isna(v):
                        r['1y_current'] = float(v)
                    v90 = et.loc['+1y', '90daysAgo'] if '90daysAgo' in et.columns else None
                    if not pd.isna(v90):
                        r['1y_90d'] = float(v90)
                    r['fy_complete_1y'] = r['1y_current'] is not None and r['1y_90d'] is not None
        except Exception:
            pass

        try:
            raw = t._analysis._earnings_trend
            if raw:
                for item_ in raw:
                    p = item_.get('period')
                    ed = item_.get('endDate')
                    if p == '0y' and ed:
                        r['endDate_0y_ok'] = True
                    if p == '+1y' and ed:
                        r['endDate_1y_ok'] = True
        except Exception:
            pass

        try:
            er = t.eps_revisions
            if er is not None and len(er) > 0 and '0y' in er.index:
                row = er.loc['0y']
                up7 = row.get('upLast7days')
                up30 = row.get('upLast30days')
                dn30 = row.get('downLast30days')
                dn7 = row.get('downLast7Days')
                if up30 is not None and not pd.isna(up30):
                    r['up7'] = int(up7) if not pd.isna(up7) else 0
                    r['up30'] = int(up30)
                    r['dn30'] = int(dn30) if not pd.isna(dn30) else 0
                    r['dn7'] = int(dn7) if not pd.isna(dn7) else 0
                    r['rev_ok_0y'] = True
        except Exception:
            pass

        try:
            info = t.info
            r['name'] = info.get('shortName') or info.get('longName') or item.get('name', '')
            r['na'] = info.get('numberOfAnalystOpinions')
            r['rev_growth'] = info.get('revenueGrowth')
            r['op_margin'] = info.get('operatingMargins')
            r['gross_margin'] = info.get('grossMargins')
            r['fwd_pe'] = info.get('forwardPE')
            r['fwd_eps'] = info.get('forwardEps')
        except Exception:
            pass

        try:
            cal = t.calendar
            if cal and cal.get('Earnings Date'):
                r['earn_date_present'] = True
        except Exception:
            pass

    except Exception as e:
        r['error'] = str(e)[:80]

    time.sleep(SLEEP)
    return r


def main():
    print('=' * 100)
    print(f'KR yf 심층 probe — top {TOP_N} by 시가총액')
    print('=' * 100)

    universe = get_universe()
    print(f'\n유니버스: {len(universe)}종목')
    print(f'  KOSPI: {sum(1 for u in universe if u["market"] == "KS")}')
    print(f'  KOSDAQ: {sum(1 for u in universe if u["market"] == "KQ")}')
    print(f'\nyf probe 시작 ({WORKERS} workers, {SLEEP}s sleep, ~{len(universe)*SLEEP/WORKERS/60:.1f}분 예상)')

    results = []
    completed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(probe, u): u for u in universe}
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            completed += 1
            if completed % 25 == 0:
                ok_count = sum(1 for x in results if x['fy_complete_0y'])
                print(f'  [{completed}/{len(universe)}] FY 가용: {ok_count}/{completed} ({ok_count/completed*100:.0f}%)',
                      flush=True)

    # CSV 저장
    if results:
        fields = list(results[0].keys())
        with open(OUT_CSV, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(results)
        print(f'\n✓ 저장: {OUT_CSV} ({len(results)} rows)')


if __name__ == '__main__':
    main()
