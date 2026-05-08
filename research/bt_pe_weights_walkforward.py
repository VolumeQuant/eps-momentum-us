"""walk-forward 검증: train에서 best 찾고 test에서 진짜 작동하는지

split T ∈ {20, 25, 30, 35, 40}:
  train: dates[0:T]에서 13변형 비교 → best 선택
  test:  dates[T:]에서 best vs production 비교
  동시에 모든 변형의 test 결과도 출력 → train→test 순위 상관 확인
"""
import sys
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'pe_weight_dbs'

VARIANTS = [
    ('w7=0.10', 'w7_0p10.db'),
    ('w7=0.15', 'w7_0p15.db'),
    ('w7=0.20', 'w7_0p20.db'),
    ('w7=0.25', 'w7_0p25.db'),
    ('w7=0.30', 'w7_0p30.db'),
    ('w7=0.35', 'w7_0p35.db'),
    ('w7=0.40', 'w7_0p40.db'),
    ('w7=0.45', 'w7_0p45.db'),
    ('w7=0.50', 'w7_0p50.db'),
    ('w7=0.55', 'w7_0p55.db'),
    ('w7=0.60', 'w7_0p60.db'),
    ('w7=0.65', 'w7_0p65.db'),
    ('w7=0.70', 'w7_0p70.db'),
]


def simulate_range(db_path, dates_subset, data):
    """주어진 dates 부분집합에서 cold-start 시뮬레이션"""
    bts2.DB_PATH = str(db_path)
    # dates_subset를 dates_all로 넘기면 그 범위만 시뮬
    r = bts2.simulate(dates_subset, data, 3, 8, 3, start_date=None)
    return r['total_return'], r['max_dd'], r['n_trades']


def main():
    # 한 번 데이터 로드 (모든 변형이 같은 ntm 데이터 + 다른 part2_rank)
    bts2.DB_PATH = str(GRID / VARIANTS[0][1])
    dates_all, _ = bts2.load_data()
    n_total = len(dates_all)
    print(f'전체 거래일: {n_total}')
    print(f'데이터 범위: {dates_all[0]} ~ {dates_all[-1]}')

    SPLITS = [20, 25, 30, 35, 40]
    print(f'\nsplit points: {SPLITS}')
    for T in SPLITS:
        print(f'  T={T:>2}: train={T}일 ({dates_all[0]}~{dates_all[T-1]}), '
              f'test={n_total-T}일 ({dates_all[T]}~{dates_all[-1]})')

    # 각 split별 결과
    all_results = {}
    for T in SPLITS:
        train_dates = dates_all[:T]
        test_dates = dates_all[T:]

        variant_results = []
        for name, dbf in VARIANTS:
            db = GRID / dbf
            # 각 DB는 독립적이니 매번 load_data 필요
            bts2.DB_PATH = str(db)
            _, data = bts2.load_data()
            train_ret, train_mdd, train_nt = simulate_range(db, train_dates, data)
            test_ret, test_mdd, test_nt = simulate_range(db, test_dates, data)
            variant_results.append({
                'name': name,
                'train_ret': train_ret, 'train_mdd': train_mdd, 'train_nt': train_nt,
                'test_ret': test_ret, 'test_mdd': test_mdd, 'test_nt': test_nt,
            })
        all_results[T] = variant_results

    # split별 결과 출력
    for T in SPLITS:
        print()
        print('=' * 110)
        print(f'split T={T}: train {T}일 → test {n_total-T}일')
        print('=' * 110)
        results = all_results[T]
        # train_ret로 정렬
        sorted_train = sorted(results, key=lambda x: x['train_ret'], reverse=True)
        sorted_test = sorted(results, key=lambda x: x['test_ret'], reverse=True)
        train_rank = {r['name']: i + 1 for i, r in enumerate(sorted_train)}
        test_rank = {r['name']: i + 1 for i, r in enumerate(sorted_test)}

        print(f'{"variant":<10} {"train_ret":>10} {"train_rk":>9} {"test_ret":>10} '
              f'{"test_rk":>8} {"test_mdd":>9}')
        print('-' * 80)
        for r in sorted(results, key=lambda x: x['name']):
            tr_rk = train_rank[r['name']]
            te_rk = test_rank[r['name']]
            marker = ''
            if tr_rk == 1: marker += ' ←train_best'
            if te_rk == 1: marker += ' ←test_best'
            if r['name'] == 'w7=0.40': marker += ' ★prod'
            print(f'  {r["name"]:<8} {r["train_ret"]:+9.2f}% {tr_rk:>8} {r["test_ret"]:+9.2f}% '
                  f'{te_rk:>7} {r["test_mdd"]:+8.2f}%{marker}')

        # OOS 결과
        train_best = sorted_train[0]
        prod = next(r for r in results if r['name'] == 'w7=0.40')
        oos_lift = train_best['test_ret'] - prod['test_ret']
        print(f'\n  ▶ train_best={train_best["name"]} (train +{train_best["train_ret"]:.2f}%)')
        print(f'    OOS test return: {train_best["test_ret"]:+.2f}% (production {prod["test_ret"]:+.2f}%)')
        print(f'    OOS lift vs production: {oos_lift:+.2f}%p')
        # train→test 순위 상관 (Spearman)
        names = [r['name'] for r in results]
        tr_ranks = [train_rank[n] for n in names]
        te_ranks = [test_rank[n] for n in names]
        # Spearman (간이) — 동률 없다고 가정
        diffs = [(tr - te) ** 2 for tr, te in zip(tr_ranks, te_ranks)]
        n = len(names)
        spearman = 1 - 6 * sum(diffs) / (n * (n ** 2 - 1))
        print(f'    train→test 순위 상관 (Spearman ρ): {spearman:+.3f}')

    # 통합 요약
    print()
    print('=' * 110)
    print('통합 요약: train_best와 OOS 결과')
    print('=' * 110)
    print(f'{"split":<6} {"train_best":<10} {"test_ret":>10} {"prod_test":>11} {"OOS_lift":>10} {"Spearman":>10}')
    print('-' * 70)
    summary_lifts = []
    for T in SPLITS:
        results = all_results[T]
        sorted_train = sorted(results, key=lambda x: x['train_ret'], reverse=True)
        train_best = sorted_train[0]
        prod = next(r for r in results if r['name'] == 'w7=0.40')
        oos_lift = train_best['test_ret'] - prod['test_ret']
        summary_lifts.append(oos_lift)

        names = [r['name'] for r in results]
        train_rank = {r['name']: i + 1 for i, r in enumerate(sorted_train)}
        sorted_test = sorted(results, key=lambda x: x['test_ret'], reverse=True)
        test_rank = {r['name']: i + 1 for i, r in enumerate(sorted_test)}
        diffs = [(train_rank[nm] - test_rank[nm]) ** 2 for nm in names]
        n = len(names)
        spearman = 1 - 6 * sum(diffs) / (n * (n ** 2 - 1))

        print(f'  T={T:<3} {train_best["name"]:<8} {train_best["test_ret"]:+9.2f}% '
              f'{prod["test_ret"]:+10.2f}% {oos_lift:+9.2f}%p {spearman:+9.3f}')

    print()
    if summary_lifts:
        avg_lift = sum(summary_lifts) / len(summary_lifts)
        n_pos = sum(1 for x in summary_lifts if x > 0)
        n_neg = sum(1 for x in summary_lifts if x < 0)
        print(f'OOS lift 평균: {avg_lift:+.2f}%p ({n_pos}개 양수 / {n_neg}개 음수 / {len(summary_lifts)}개 split)')

    # train_best 일관성
    print()
    print('=' * 110)
    print('train_best 일관성')
    print('=' * 110)
    train_bests = []
    for T in SPLITS:
        results = all_results[T]
        sorted_train = sorted(results, key=lambda x: x['train_ret'], reverse=True)
        train_bests.append((T, sorted_train[0]['name']))
    for T, name in train_bests:
        print(f'  T={T:<3}: train_best = {name}')
    unique_bests = set(name for _, name in train_bests)
    print(f'\n고유 best: {len(unique_bests)}개 ({", ".join(sorted(unique_bests))})')
    if len(unique_bests) == 1:
        print('→ ✓ 모든 split에서 동일 variant 선택 (train signal 안정적)')
    else:
        print('→ ✗ split별로 best 달라짐 (train signal 불안정 → 과적합 의심)')


if __name__ == '__main__':
    main()
