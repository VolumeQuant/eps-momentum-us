"""opt1 multistart 검증 — 시작일 효과 제거 후에도 우월한지.

기존 sign_aware DB(baseline/gamma/opt1) 활용 + backtest_s2_params.simulate
multistart로 시작일별 분포 측정.
"""
import sys
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
GRID_DIR = ROOT / 'research' / 'sign_aware_dbs'

DBS = {
    'baseline': GRID_DIR / 'baseline.db',
    'gamma': GRID_DIR / 'gamma.db',
    'beta1': GRID_DIR / 'beta1.db',
    'beta2': GRID_DIR / 'beta2.db',
    'opt1': GRID_DIR / 'opt1.db',
    'opt2': GRID_DIR / 'opt2.db',
    'b1_opt2': GRID_DIR / 'b1_opt2.db',
    'b2_opt2': GRID_DIR / 'b2_opt2.db',
}


def multistart(db_path, entry=3, exit_top=8, slots=3, n_starts=5):
    """올바른 multistart: 처음 N일을 시작일로, 모든 시작일이 시계열 끝까지 BT.

    시작일이 너무 늦으면 BT 기간 짧아져 통계 무의미. 처음 5일만 사용해
    모든 sample이 50일+ BT 보장.
    """
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    if len(dates) < n_starts + 10:
        return None
    start_dates = dates[:n_starts]  # 처음 N일만 시작일로
    rets = []
    mdds = []
    bt_lengths = []
    for sd in start_dates:
        r = bts2.simulate(dates, data, entry, exit_top, slots, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
        bt_lengths.append(len([d for d in dates if d >= sd]))
    n = len(rets)
    return {
        'n': n,
        'avg': sum(rets) / n,
        'med': sorted(rets)[n // 2],
        'std': statistics.pstdev(rets),
        'min': min(rets),
        'max': max(rets),
        'avg_mdd': sum(mdds) / n,
        'worst_mdd': min(mdds),
        'risk_adj': (sum(rets)/n) / abs(min(mdds)) if min(mdds) < 0 else 0,
        'rets': rets,
        'start_dates': start_dates,
        'bt_lengths': bt_lengths,
    }


def main():
    print('=' * 100)
    print('Multistart — 처음 5일을 시작일로 변경 (각 시작일이 시계열 끝까지 BT)')
    print('=' * 100)

    rows = []
    for name, db in DBS.items():
        if not db.exists():
            print(f'  {name:<10} DB 없음, skip')
            continue
        r = multistart(db, n_starts=5)
        if r is None:
            continue
        r['name'] = name
        rows.append(r)

    # 시작일별 raw return 출력
    if rows:
        sample = rows[0]
        print()
        print('각 시작일별 BT 기간:')
        for sd, bl in zip(sample['start_dates'], sample['bt_lengths']):
            print(f'  {sd}: {bl}일')
        print()
        print(f'{"Variant":<10}', end='')
        for sd in sample['start_dates']:
            print(f' {sd:>10}', end='')
        print(f' {"avg":>8} {"med":>7} {"std":>6} {"worstMDD":>9} {"risk_adj":>8}')
        print('-' * (12 + 11 * len(sample['start_dates']) + 50))
        for r in rows:
            print(f'  {r["name"]:<8}', end='')
            for ret in r['rets']:
                print(f' {ret:>9.2f}%', end='')
            print(f' {r["avg"]:+7.2f}% {r["med"]:+6.2f}% {r["std"]:>5.2f} '
                  f'{r["worst_mdd"]:+8.2f}% {r["risk_adj"]:>7.2f}')

    # 비교
    print()
    print('=' * 100)
    print('비교 (baseline 대비)')
    print('=' * 100)
    base = next((r for r in rows if r['name'] == 'baseline'), None)
    gamma = next((r for r in rows if r['name'] == 'gamma'), None)
    if not base or not gamma:
        return
    for r in rows:
        if r['name'] == 'baseline':
            continue
        d_base = r['avg'] - base['avg']
        d_gamma = r['avg'] - gamma['avg']
        d_mdd_base = r['worst_mdd'] - base['worst_mdd']
        d_ra_base = r['risk_adj'] - base['risk_adj']
        print(f'  {r["name"]:<10} vs base: ΔRet {d_base:+.2f}%p, '
              f'ΔworstMDD {d_mdd_base:+.2f}%p, Δrisk_adj {d_ra_base:+.2f}')
        print(f'  {r["name"]:<10} vs γ   : ΔRet {d_gamma:+.2f}%p')

    # opt1 robustness
    opt1 = next((r for r in rows if r['name'] == 'opt1'), None)
    if opt1:
        print()
        print('=' * 100)
        print('opt1 robustness 평가')
        print('=' * 100)
        positive = sum(1 for r in [opt1['min'], opt1['med'], opt1['max']] if r > 0)
        # 시작일별 양수 비율 정확히 계산
        bts2.DB_PATH = str(GRID_DIR / 'opt1.db')
        dates, data = bts2.load_data()
        start_dates = dates[3:len(dates)-5]
        rets = []
        for sd in start_dates:
            r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
            rets.append(r['total_return'])
        pos_count = sum(1 for r in rets if r > 0)
        neg_count = sum(1 for r in rets if r < 0)
        print(f'  시작일별 양수 수익: {pos_count}/{len(rets)}')
        print(f'  시작일별 음수 수익: {neg_count}/{len(rets)}')
        print(f'  최저 수익: {opt1["min"]:+.2f}% (worst case)')
        print(f'  표준편차: {opt1["std"]:.2f} (변동성)')

        # vs gamma 시작일별 차이
        bts2.DB_PATH = str(GRID_DIR / 'gamma.db')
        dates_g, data_g = bts2.load_data()
        gamma_rets = [bts2.simulate(dates_g, data_g, 3, 8, 3, start_date=sd)['total_return'] for sd in start_dates]
        diffs = [r1 - r2 for r1, r2 in zip(rets, gamma_rets)]
        better = sum(1 for d in diffs if d > 0)
        print()
        print(f'  opt1 vs γ 시작일별:')
        print(f'    opt1 우월: {better}/{len(diffs)}')
        print(f'    평균 차이: {sum(diffs)/len(diffs):+.2f}%p')
        print(f'    최대 차이: {max(diffs):+.2f}%p')
        print(f'    최소 차이: {min(diffs):+.2f}%p')


if __name__ == '__main__':
    main()
