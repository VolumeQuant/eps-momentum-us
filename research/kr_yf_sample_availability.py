"""KR 종목 표본 ~30종목 yfinance EPS trend 가용성 통계.

목적: US 시스템의 데이터 입력(eps_trend, eps_revisions 등)이 KR 종목에서
얼마나 가용한지 비율 측정. KR 프로젝트 적용 가능성 판단.

표본 분포:
  - KOSPI 대형 (분석가 많은 대표 종목)
  - KOSPI 중형
  - KOSDAQ 대형
  - KOSDAQ 중형
  - KOSDAQ 소형 (선익시스템 포함)

확인 항목:
  - eps_trend 0y, +1y 둘 다 가용 (NaN 아님)
  - 5 스냅샷 (current/7d/30d/60d/90d) 모두 가용
  - _earnings_trend endDate 가용
  - eps_revisions upLast30days/downLast30days 가용
  - numberOfAnalysts ≥ 3 (의미 있는 커버리지)
  - marketCap, revenueGrowth, operatingMargins 가용
"""
import sys
import time
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

SAMPLE = [
    # KOSPI 대형 (시총 ~10조+)
    ('005930.KS', '삼성전자', 'KOSPI_대형'),
    ('000660.KS', 'SK하이닉스', 'KOSPI_대형'),
    ('035420.KS', 'NAVER', 'KOSPI_대형'),
    ('005380.KS', '현대차', 'KOSPI_대형'),
    ('207940.KS', '삼성바이오로직스', 'KOSPI_대형'),
    ('068270.KS', '셀트리온', 'KOSPI_대형'),

    # KOSPI 중형
    ('010130.KS', '고려아연', 'KOSPI_중형'),
    ('086790.KS', '하나금융지주', 'KOSPI_중형'),
    ('003490.KS', '대한항공', 'KOSPI_중형'),
    ('011170.KS', '롯데케미칼', 'KOSPI_중형'),
    ('028260.KS', '삼성물산', 'KOSPI_중형'),
    ('010120.KS', 'LS ELECTRIC', 'KOSPI_중형'),

    # KOSDAQ 대형
    ('086520.KQ', '에코프로', 'KOSDAQ_대형'),
    ('247540.KQ', '에코프로비엠', 'KOSDAQ_대형'),
    ('091990.KQ', '셀트리온헬스케어', 'KOSDAQ_대형'),
    ('068760.KQ', '셀트리온제약', 'KOSDAQ_대형'),
    ('058470.KQ', '리노공업', 'KOSDAQ_대형'),
    ('196170.KQ', '알테오젠', 'KOSDAQ_대형'),

    # KOSDAQ 중형
    ('357780.KQ', '솔브레인', 'KOSDAQ_중형'),
    ('263750.KQ', '펄어비스', 'KOSDAQ_중형'),
    ('240810.KQ', '원익IPS', 'KOSDAQ_중형'),
    ('293490.KQ', '카카오게임즈', 'KOSDAQ_중형'),
    ('095660.KQ', '네오위즈', 'KOSDAQ_중형'),
    ('042700.KQ', '한미반도체', 'KOSDAQ_중형'),

    # KOSDAQ 소형
    ('171090.KQ', '선익시스템', 'KOSDAQ_소형'),
    ('088130.KQ', '동아엘텍', 'KOSDAQ_소형'),
    ('089030.KQ', '테크윙', 'KOSDAQ_소형'),
    ('204620.KQ', '글로벌텍스프리', 'KOSDAQ_소형'),
    ('033100.KQ', '제룡전기', 'KOSDAQ_소형'),
    ('062040.KQ', '산일전기', 'KOSDAQ_소형'),
]


def check(symbol, name):
    """단일 종목 가용성 체크"""
    result = {
        'symbol': symbol, 'name': name,
        'eps_trend_ok': False,
        'fy_complete': False,         # 0y + +1y FY 둘 다 5컬럼 모두 가용
        'endDate_ok': False,
        'rev_ok': False,              # eps_revisions 가용 (FY 기준)
        'na': None,                   # numberOfAnalysts
        'mc': None,                   # marketCap
        'rev_growth': None,
        'op_margin': None,
        'fwd_pe': None,
        'error': None,
    }
    try:
        t = yf.Ticker(symbol)

        # 1. eps_trend
        try:
            et = t.eps_trend
            if et is not None and len(et) > 0:
                result['eps_trend_ok'] = True
                if '0y' in et.index and '+1y' in et.index:
                    cols = ['current', '7daysAgo', '30daysAgo', '60daysAgo', '90daysAgo']
                    fy0_ok = all(not pd.isna(et.loc['0y', c]) for c in cols if c in et.columns)
                    fy1_ok = all(not pd.isna(et.loc['+1y', c]) for c in cols if c in et.columns)
                    result['fy_complete'] = fy0_ok and fy1_ok
        except Exception:
            pass

        # 2. _earnings_trend (endDate)
        try:
            raw = t._analysis._earnings_trend
            if raw:
                have_0y = any(item.get('period') == '0y' and item.get('endDate') for item in raw)
                have_1y = any(item.get('period') == '+1y' and item.get('endDate') for item in raw)
                result['endDate_ok'] = have_0y and have_1y
        except Exception:
            pass

        # 3. eps_revisions
        try:
            er = t.eps_revisions
            if er is not None and len(er) > 0 and '0y' in er.index:
                up = er.loc['0y'].get('upLast30days')
                dn = er.loc['0y'].get('downLast30days')
                if up is not None and not pd.isna(up):
                    result['rev_ok'] = True
        except Exception:
            pass

        # 4. info fields
        try:
            info = t.info
            result['na'] = info.get('numberOfAnalystOpinions')
            result['mc'] = info.get('marketCap')
            result['rev_growth'] = info.get('revenueGrowth')
            result['op_margin'] = info.get('operatingMargins')
            result['fwd_pe'] = info.get('forwardPE')
        except Exception:
            pass

    except Exception as e:
        result['error'] = str(e)[:60]
    return result


def main():
    print(f'{"="*120}')
    print(f'KR 표본 {len(SAMPLE)}종목 yfinance EPS trend 가용성 통계')
    print(f'{"="*120}')

    results = []
    for i, (sym, name, cat) in enumerate(SAMPLE):
        print(f'  [{i+1}/{len(SAMPLE)}] {sym} {name} ({cat}) ...', end=' ', flush=True)
        r = check(sym, name)
        r['cat'] = cat
        results.append(r)
        print('✓' if r['fy_complete'] else '⚠️' if r['eps_trend_ok'] else '❌')
        time.sleep(0.4)  # rate limit

    # 상세 테이블
    print()
    print(f'{"="*120}')
    print(f'{"sym":<14} {"name":<22} {"cat":<14} {"eps_t":>5} {"fy":>4} {"end":>4} {"rev":>4} '
          f'{"na":>4} {"mc(B)":>7} {"rev_g":>7} {"opM":>6} {"fPE":>6}')
    print('-' * 120)
    for r in results:
        mc_b = f"{r['mc']/1e9:.1f}" if r['mc'] else "-"
        rg = f"{r['rev_growth']*100:.0f}%" if r['rev_growth'] is not None else "-"
        om = f"{r['op_margin']*100:.0f}%" if r['op_margin'] is not None else "-"
        fpe = f"{r['fwd_pe']:.1f}" if r['fwd_pe'] else "-"
        na = str(r['na']) if r['na'] is not None else "-"
        print(f'{r["symbol"]:<14} {r["name"][:20]:<22} {r["cat"]:<14} '
              f'{"✓" if r["eps_trend_ok"] else "✗":>5} '
              f'{"✓" if r["fy_complete"] else "✗":>4} '
              f'{"✓" if r["endDate_ok"] else "✗":>4} '
              f'{"✓" if r["rev_ok"] else "✗":>4} '
              f'{na:>4} {mc_b:>7} {rg:>7} {om:>6} {fpe:>6}')

    # 통계
    print()
    print(f'{"="*120}')
    print('가용성 통계')
    print(f'{"="*120}')
    n = len(results)

    def pct(field, condition=lambda x: x):
        cnt = sum(1 for r in results if condition(r.get(field)))
        return f'{cnt}/{n} ({cnt/n*100:.0f}%)'

    print(f'  eps_trend 가용         : {pct("eps_trend_ok")}')
    print(f'  FY 5스냅샷 완전        : {pct("fy_complete")}')
    print(f'  _earnings_trend endDate: {pct("endDate_ok")}')
    print(f'  eps_revisions 가용     : {pct("rev_ok")}')
    print(f'  numberOfAnalysts ≥ 3   : {pct("na", lambda x: x is not None and x >= 3)}')
    print(f'  numberOfAnalysts ≥ 5   : {pct("na", lambda x: x is not None and x >= 5)}')
    print(f'  marketCap 가용         : {pct("mc")}')
    print(f'  revenueGrowth 가용     : {pct("rev_growth", lambda x: x is not None)}')
    print(f'  operatingMargin 가용   : {pct("op_margin", lambda x: x is not None)}')

    # 카테고리별 가용성
    print()
    print('카테고리별 FY 5스냅샷 완전 비율 (시스템 사용 가능 비율):')
    cats = {}
    for r in results:
        cats.setdefault(r['cat'], []).append(r['fy_complete'])
    for c in ['KOSPI_대형', 'KOSPI_중형', 'KOSDAQ_대형', 'KOSDAQ_중형', 'KOSDAQ_소형']:
        v = cats.get(c, [])
        if v:
            ok = sum(v)
            print(f'  {c:<12}: {ok}/{len(v)} ({ok/len(v)*100:.0f}%)')

    # NA 분포 (커버리지)
    nas = [r['na'] for r in results if r['na'] is not None]
    if nas:
        print()
        print(f'numberOfAnalysts 분포: min={min(nas)}, max={max(nas)}, '
              f'avg={sum(nas)/len(nas):.1f}, median={sorted(nas)[len(nas)//2]}')


if __name__ == '__main__':
    main()
