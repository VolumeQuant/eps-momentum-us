"""kr_yf_deep_results.csv 심층 분석.

분석 차원:
  1. 시총 구간별 가용성 cliff
  2. 분석가 커버리지 분포 (시장별, 시총별)
  3. NaN 패턴 (5스냅샷, 0y vs +1y)
  4. revenueGrowth 비정상 큰 값 비율
  5. 어닝 점프 detection 가능성 (90d→7d eps_trend 변화)
  6. yf staleness 패턴 (7d/30d/60d 값이 같은지 - 컬럼 stale 여부)
  7. KR 시스템 적용 시 실효 종목 수 (필터 적용 후)
"""
import sys
import csv
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
CSV_PATH = ROOT / 'research' / 'kr_yf_deep_results.csv'


def f(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def main():
    rows = []
    with open(CSV_PATH, 'r', encoding='utf-8') as fp:
        reader = csv.DictReader(fp)
        for r in reader:
            # 타입 변환
            r['mc_krw'] = f(r['mc_krw']) or 0
            r['mc_b'] = r['mc_krw'] / 1e9
            r['mc_t'] = r['mc_krw'] / 1e12
            r['snap_ok_count'] = int(r['snap_ok_count']) if r['snap_ok_count'] else 0
            for k in ['eps_trend_ok', 'fy_complete_0y', 'fy_complete_1y',
                      'endDate_0y_ok', 'endDate_1y_ok', 'rev_ok_0y', 'earn_date_present']:
                r[k] = (r[k] == 'True')
            for k in ['0y_current', '0y_7d', '0y_30d', '0y_60d', '0y_90d',
                      '1y_current', '1y_90d', 'rev_growth', 'op_margin',
                      'gross_margin', 'fwd_pe', 'fwd_eps']:
                r[k] = f(r[k])
            for k in ['up7', 'up30', 'dn30', 'dn7', 'na']:
                r[k] = int(f(r[k])) if f(r[k]) is not None else None
            rows.append(r)

    n = len(rows)
    print(f'{"="*100}')
    print(f'KR yf 심층 분석 — {n}종목')
    print(f'{"="*100}')

    # ─── 1. 시장별 분포 ───
    ks = [r for r in rows if r['market'] == 'KS']
    kq = [r for r in rows if r['market'] == 'KQ']
    print(f'\n[1] 시장 분포')
    print(f'  KOSPI: {len(ks)}, KOSDAQ: {len(kq)}')

    # ─── 2. 전체 가용성 ───
    def pct(cond):
        cnt = sum(1 for r in rows if cond(r))
        return f'{cnt}/{n} ({cnt/n*100:.1f}%)'

    print(f'\n[2] 전체 가용성')
    print(f'  eps_trend 존재             : {pct(lambda r: r["eps_trend_ok"])}')
    print(f'  0y FY 5스냅샷 완전         : {pct(lambda r: r["fy_complete_0y"])}')
    print(f'  +1y FY 90d→current 완전    : {pct(lambda r: r["fy_complete_1y"])}')
    print(f'  endDate 0y                 : {pct(lambda r: r["endDate_0y_ok"])}')
    print(f'  endDate +1y                : {pct(lambda r: r["endDate_1y_ok"])}')
    print(f'  eps_revisions 0y           : {pct(lambda r: r["rev_ok_0y"])}')
    print(f'  na ≥ 3                     : {pct(lambda r: r["na"] is not None and r["na"] >= 3)}')
    print(f'  na ≥ 5                     : {pct(lambda r: r["na"] is not None and r["na"] >= 5)}')
    print(f'  rev_growth 존재            : {pct(lambda r: r["rev_growth"] is not None)}')
    print(f'  op_margin 존재             : {pct(lambda r: r["op_margin"] is not None)}')
    print(f'  Earnings Date 캘린더 가용  : {pct(lambda r: r["earn_date_present"])}')

    # ─── 3. 시총 구간별 가용성 cliff ───
    print(f'\n[3] 시총 구간별 가용성 (FY 5스냅샷 + na ≥ 3 기준 = 시스템 진입 가능)')
    BUCKETS = [
        ('10조+',     lambda mc: mc >= 10e12),
        ('5~10조',    lambda mc: 5e12 <= mc < 10e12),
        ('1~5조',     lambda mc: 1e12 <= mc < 5e12),
        ('5천억~1조', lambda mc: 5e11 <= mc < 1e12),
        ('1천억~5천', lambda mc: 1e11 <= mc < 5e11),
        ('1천억 미만', lambda mc: mc < 1e11),
    ]
    print(f'  {"구간":<14} {"전체":>5} {"FY완전":>8} {"na≥3":>8} {"FY+na≥3":>10}')
    for label, cond in BUCKETS:
        sub = [r for r in rows if cond(r['mc_krw'])]
        if not sub:
            continue
        fy = sum(1 for r in sub if r['fy_complete_0y'])
        na3 = sum(1 for r in sub if r['na'] is not None and r['na'] >= 3)
        both = sum(1 for r in sub if r['fy_complete_0y'] and r['na'] is not None and r['na'] >= 3)
        print(f'  {label:<14} {len(sub):>5} '
              f'{fy:>3}({fy/len(sub)*100:>3.0f}%) '
              f'{na3:>3}({na3/len(sub)*100:>3.0f}%) '
              f'{both:>3}({both/len(sub)*100:>3.0f}%)')

    # ─── 4. 분석가 커버리지 분포 ───
    print(f'\n[4] 분석가 커버리지 분포 (numberOfAnalystOpinions)')
    nas = [r['na'] for r in rows if r['na'] is not None]
    if nas:
        nas_sorted = sorted(nas)
        print(f'  존재 종목: {len(nas)}/{n}')
        print(f'  min={min(nas)}, max={max(nas)}, '
              f'p25={nas_sorted[len(nas)//4]}, '
              f'median={nas_sorted[len(nas)//2]}, '
              f'p75={nas_sorted[3*len(nas)//4]}, '
              f'avg={sum(nas)/len(nas):.1f}')
        print(f'  히스토그램:')
        for lo, hi in [(0, 0), (1, 2), (3, 5), (6, 10), (11, 20), (21, 99)]:
            cnt = sum(1 for x in nas if lo <= x <= hi)
            bar = '█' * int(cnt / max(nas) * 50) if nas else ''
            label = f'{lo}' if lo == hi else f'{lo}~{hi}'
            print(f'    {label:>6}: {cnt:>4} {bar}')

    # ─── 5. NaN 패턴 (5스냅샷 부분 결측) ───
    print(f'\n[5] NaN 패턴 (eps_trend 0y의 5스냅샷 결측 분포)')
    for k in range(6):
        cnt = sum(1 for r in rows if r['eps_trend_ok'] and r['snap_ok_count'] == k)
        print(f'  {k}/5 스냅샷 가용: {cnt}')
    # 90d만 NaN인 케이스 (신규 상장 가능성)
    only_90d_na = sum(1 for r in rows
                       if r['eps_trend_ok']
                       and r['0y_current'] is not None
                       and r['0y_7d'] is not None
                       and r['0y_30d'] is not None
                       and r['0y_60d'] is not None
                       and r['0y_90d'] is None)
    print(f'  90d만 NaN (신규 상장 가능성): {only_90d_na}')

    # ─── 6. revenueGrowth 비정상 ───
    print(f'\n[6] revenueGrowth 분포')
    rgs = [r['rev_growth'] for r in rows if r['rev_growth'] is not None]
    if rgs:
        rgs_sorted = sorted(rgs)
        big = sum(1 for x in rgs if abs(x) > 5)
        print(f'  존재 종목: {len(rgs)}/{n}')
        print(f'  min={min(rgs)*100:.0f}%, max={max(rgs)*100:.0f}%')
        print(f'  median={rgs_sorted[len(rgs)//2]*100:.0f}%, '
              f'p10={rgs_sorted[len(rgs)//10]*100:.0f}%, '
              f'p90={rgs_sorted[9*len(rgs)//10]*100:.0f}%')
        print(f'  |rev_growth| > 500% (비정상 큰 값): {big}/{len(rgs)} ({big/len(rgs)*100:.1f}%)')
        print(f'  → US 시스템에서 이런 값들 income_stmt 재검증 필요')

    # ─── 7. 어닝 점프 detection ───
    print(f'\n[7] 어닝 점프 detection (eps_trend 0y에서 |current - 90d| / |90d| 큰 종목)')
    jumps = []
    for r in rows:
        if r['0y_current'] is not None and r['0y_90d'] is not None and abs(r['0y_90d']) > 0.01:
            jump_pct = (r['0y_current'] - r['0y_90d']) / abs(r['0y_90d']) * 100
            jumps.append((r['symbol'], r['name'], jump_pct, r['na']))
    jumps_filt = [j for j in jumps if abs(j[2]) < 1000]  # outlier 제거
    jumps_filt.sort(key=lambda x: x[2], reverse=True)
    print(f'  90d→current 변화 detect 가능: {len(jumps)}종목')
    print(f'  +50% 이상 (큰 상향): {sum(1 for j in jumps if j[2] >= 50)}')
    print(f'  -50% 이하 (큰 하향): {sum(1 for j in jumps if j[2] <= -50)}')
    print(f'  Top 5 상향:')
    for sym, name, jp, na in jumps_filt[:5]:
        print(f'    {sym:<12} {name[:18]:<20} +{jp:>6.1f}% (na={na})')
    print(f'  Top 5 하향:')
    for sym, name, jp, na in jumps_filt[-5:]:
        print(f'    {sym:<12} {name[:18]:<20} {jp:>+7.1f}% (na={na})')

    # ─── 8. yf staleness 패턴 (US의 BE 사례 — 7d/30d/60d가 같은 값인 종목) ───
    print(f'\n[8] yf staleness 의심 (7d/30d/60d 동일 = 컬럼 stale)')
    stale_730 = sum(1 for r in rows
                     if r['0y_7d'] is not None and r['0y_30d'] is not None
                     and abs(r['0y_7d'] - r['0y_30d']) < 0.01)
    stale_3060 = sum(1 for r in rows
                      if r['0y_30d'] is not None and r['0y_60d'] is not None
                      and abs(r['0y_30d'] - r['0y_60d']) < 0.01)
    stale_all = sum(1 for r in rows
                     if r['0y_7d'] is not None and r['0y_30d'] is not None
                     and r['0y_60d'] is not None
                     and abs(r['0y_7d'] - r['0y_30d']) < 0.01
                     and abs(r['0y_30d'] - r['0y_60d']) < 0.01)
    valid_730 = sum(1 for r in rows if r['0y_7d'] is not None and r['0y_30d'] is not None)
    if valid_730:
        print(f'  7d ≈ 30d (값 동일): {stale_730}/{valid_730} ({stale_730/valid_730*100:.1f}%)')
        print(f'  30d ≈ 60d (값 동일): {stale_3060}/{valid_730} ({stale_3060/valid_730*100:.1f}%)')
        print(f'  7d=30d=60d 모두 동일: {stale_all}/{valid_730} ({stale_all/valid_730*100:.1f}%)')
        print(f'  → 선익시스템 패턴(0y: 8910/8773/8773/8773/5795)이 일반적인지 확인')

    # ─── 9. KR 시스템 적용 시 실효 종목 수 ───
    print(f'\n[9] KR 시스템 적용 시나리오별 실효 종목 수')
    # 시나리오 A: US 동일 (FY완전 + na≥3 + endDate + rev_revisions)
    a = sum(1 for r in rows if r['fy_complete_0y'] and r['endDate_0y_ok'] and r['rev_ok_0y']
            and r['na'] is not None and r['na'] >= 3)
    # 시나리오 B: 임계 완화 (FY완전 + na≥2)
    b = sum(1 for r in rows if r['fy_complete_0y']
            and r['na'] is not None and r['na'] >= 2)
    # 시나리오 C: 최소 (FY완전)
    c = sum(1 for r in rows if r['fy_complete_0y'])
    print(f'  A (US 기준: FY완전+endDate+revisions+na≥3): {a} ({a/n*100:.0f}%)')
    print(f'  B (완화: FY완전+na≥2): {b} ({b/n*100:.0f}%)')
    print(f'  C (최소: FY완전): {c} ({c/n*100:.0f}%)')

    # ─── 10. 시장별 통계 ───
    print(f'\n[10] KOSPI vs KOSDAQ')
    for name, sub in [('KOSPI', ks), ('KOSDAQ', kq)]:
        if not sub:
            continue
        fy = sum(1 for r in sub if r['fy_complete_0y'])
        nas_sub = [r['na'] for r in sub if r['na'] is not None]
        avg_na = sum(nas_sub) / len(nas_sub) if nas_sub else 0
        na3 = sum(1 for r in sub if r['na'] is not None and r['na'] >= 3)
        print(f'  {name:<8}: n={len(sub)}, FY완전 {fy} ({fy/len(sub)*100:.0f}%), '
              f'na≥3 {na3} ({na3/len(sub)*100:.0f}%), avg na={avg_na:.1f}')


if __name__ == '__main__':
    main()
