"""4D grid Top 10 walk-forward 검증

캐시된 DB 그대로 사용 (research/pe_4d_dbs/)
splits: T ∈ {20, 25, 30, 35, 40}
production (w7=0.40, 30d=0.3, 60d=0.2, 90d=0.1) 대비 OOS lift 측정
"""
import sys
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'pe_4d_dbs'

# Top 10 from 4D grid (avg 기준)
TOP10 = [
    ('w_10_50_10_30', (0.10, 0.50, 0.10, 0.30), '30d-heavy'),
    ('w_10_10_30_50', (0.10, 0.10, 0.30, 0.50), 'long-tail'),
    ('w_20_10_20_50', (0.20, 0.10, 0.20, 0.50), 'long-tail'),
    ('w_10_10_20_60', (0.10, 0.10, 0.20, 0.60), 'long-tail'),
    ('w_20_10_30_40', (0.20, 0.10, 0.30, 0.40), 'long-tail'),
    ('w_10_20_10_60', (0.10, 0.20, 0.10, 0.60), 'long-tail'),
    ('w_60_10_10_20', (0.60, 0.10, 0.10, 0.20), 'high-w7'),
    ('w_10_10_50_30', (0.10, 0.10, 0.50, 0.30), 'mid-long'),
    ('w_10_20_20_50', (0.10, 0.20, 0.20, 0.50), 'long-tail'),
    ('w_30_10_10_50', (0.30, 0.10, 0.10, 0.50), 'long-tail'),
]
PRODUCTION = ('w_40_30_20_10', (0.40, 0.30, 0.20, 0.10), 'production')


def simulate_range(db_path, dates_subset, data):
    bts2.DB_PATH = str(db_path)
    r = bts2.simulate(dates_subset, data, 3, 8, 3, start_date=None)
    return r['total_return'], r['max_dd'], r['n_trades']


def main():
    bts2.DB_PATH = str(GRID / f'{PRODUCTION[0]}.db')
    dates_all, _ = bts2.load_data()
    n = len(dates_all)
    print(f'전체 거래일: {n} ({dates_all[0]} ~ {dates_all[-1]})')

    SPLITS = [20, 25, 30, 35, 40]
    print(f'splits: {SPLITS}\n')

    # 각 split, 각 변형의 train/test 결과
    all_results = {}
    for T in SPLITS:
        train_dates = dates_all[:T]
        test_dates = dates_all[T:]
        split_results = {}
        for name, w, pattern in TOP10 + [PRODUCTION]:
            db = GRID / f'{name}.db'
            bts2.DB_PATH = str(db)
            _, data = bts2.load_data()
            tr_ret, tr_mdd, _ = simulate_range(db, train_dates, data)
            te_ret, te_mdd, _ = simulate_range(db, test_dates, data)
            split_results[name] = {
                'w': w, 'pattern': pattern,
                'train_ret': tr_ret, 'train_mdd': tr_mdd,
                'test_ret': te_ret, 'test_mdd': te_mdd,
            }
        all_results[T] = split_results

    # split별 결과
    for T in SPLITS:
        print('=' * 110)
        print(f'split T={T}: train {T}일 → test {n-T}일')
        print('=' * 110)
        sr = all_results[T]
        prod_test = sr[PRODUCTION[0]]['test_ret']
        # train_ret 정렬
        sorted_train = sorted(sr.items(), key=lambda x: x[1]['train_ret'], reverse=True)
        train_rank = {k: i+1 for i, (k, _) in enumerate(sorted_train)}
        sorted_test = sorted(sr.items(), key=lambda x: x[1]['test_ret'], reverse=True)
        test_rank = {k: i+1 for i, (k, _) in enumerate(sorted_test)}

        print(f'{"variant":<16} {"weights":<22} {"pattern":<12} '
              f'{"train":>9} {"trk":>4} {"test":>9} {"terk":>4} {"OOS_lift":>10}')
        print('-' * 105)
        for name, _, pat in TOP10 + [PRODUCTION]:
            r = sr[name]
            wstr = f'({r["w"][0]:.2f},{r["w"][1]:.2f},{r["w"][2]:.2f},{r["w"][3]:.2f})'
            lift = r['test_ret'] - prod_test
            mark = ''
            if name == PRODUCTION[0]:
                mark = ' ★'
            elif lift > 0:
                mark = ' ✓'
            else:
                mark = ' ✗'
            print(f'  {name:<14} {wstr:<22} {pat:<12} '
                  f'{r["train_ret"]:+8.2f}% {train_rank[name]:>3} {r["test_ret"]:+8.2f}% '
                  f'{test_rank[name]:>3} {lift:+9.2f}%p{mark}')

    # 변형별 OOS lift 통합 (Top 10만)
    print()
    print('=' * 110)
    print('변형별 OOS lift 통합 (5개 split 전체)')
    print('=' * 110)
    print(f'{"variant":<16} {"weights":<22} {"pattern":<12} '
          f'{"avg":>9} {"min":>9} {"max":>9} {"#pos":>5} {"#neg":>5}')
    print('-' * 100)
    summary = []
    for name, w, pat in TOP10:
        lifts = []
        for T in SPLITS:
            sr = all_results[T]
            lift = sr[name]['test_ret'] - sr[PRODUCTION[0]]['test_ret']
            lifts.append(lift)
        avg = sum(lifts) / len(lifts)
        n_pos = sum(1 for l in lifts if l > 0)
        n_neg = sum(1 for l in lifts if l < 0)
        wstr = f'({w[0]:.2f},{w[1]:.2f},{w[2]:.2f},{w[3]:.2f})'
        summary.append((name, w, pat, avg, min(lifts), max(lifts), n_pos, n_neg, lifts))
    summary.sort(key=lambda x: x[3], reverse=True)
    for name, w, pat, avg, mn, mx, np_, nn, lifts in summary:
        wstr = f'({w[0]:.2f},{w[1]:.2f},{w[2]:.2f},{w[3]:.2f})'
        print(f'  {name:<14} {wstr:<22} {pat:<12} '
              f'{avg:+8.2f}%p {mn:+8.2f}%p {mx:+8.2f}%p {np_:>4} {nn:>4}')

    # 각 split별 raw lifts
    print()
    print('=' * 110)
    print('각 split별 OOS lift detail')
    print('=' * 110)
    header = f'{"variant":<16}'
    for T in SPLITS:
        header += f' {f"T={T}":>10}'
    header += f' {"avg":>10}'
    print(header)
    print('-' * 90)
    for name, w, pat, avg, mn, mx, np_, nn, lifts in summary:
        line = f'  {name:<14}'
        for l in lifts:
            line += f' {l:+9.2f}%p'
        line += f' {avg:+9.2f}%p'
        print(line)

    # Aggregate verdict
    print()
    print('=' * 110)
    print('최종 평가')
    print('=' * 110)
    robust = [s for s in summary if s[6] == 5]  # 5/5 split positive
    mostly = [s for s in summary if s[6] == 4]  # 4/5 split positive
    weak = [s for s in summary if s[6] <= 3]
    print(f'\n5/5 split outperform (robust): {len(robust)}개')
    for s in robust:
        print(f'  {s[0]:<14} {s[2]:<12} avg lift {s[3]:+.2f}%p (min {s[4]:+.2f}, max {s[5]:+.2f})')
    print(f'\n4/5 split outperform: {len(mostly)}개')
    for s in mostly:
        print(f'  {s[0]:<14} {s[2]:<12} avg lift {s[3]:+.2f}%p (min {s[4]:+.2f}, max {s[5]:+.2f})')
    print(f'\n≤3/5 split outperform (의심): {len(weak)}개')
    for s in weak:
        print(f'  {s[0]:<14} {s[2]:<12} avg lift {s[3]:+.2f}%p, #pos={s[6]}/5')

    # train→test 안정성: train Top 3가 test에서도 Top 5에 드는가
    print()
    print('=' * 110)
    print('train→test 일관성 (각 split에서 train Top 3 vs test 순위)')
    print('=' * 110)
    for T in SPLITS:
        sr = all_results[T]
        sorted_train = sorted(sr.items(), key=lambda x: x[1]['train_ret'], reverse=True)
        sorted_test = sorted(sr.items(), key=lambda x: x[1]['test_ret'], reverse=True)
        test_rank = {k: i+1 for i, (k, _) in enumerate(sorted_test)}
        print(f'\nT={T}:')
        for i, (name, _) in enumerate(sorted_train[:3]):
            te_rk = test_rank[name]
            ok = '✓' if te_rk <= 5 else '✗'
            print(f'  train#{i+1} {name:<14} → test#{te_rk:<2} {ok}')


if __name__ == '__main__':
    main()
