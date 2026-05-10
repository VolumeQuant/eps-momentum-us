"""전체 기간 random 3 시작일 × 100 seed BT
v80.9 vs v80.10 production (3, 8, 3) 비교

multistart는 첫 12일만 보지만 후반부 시작일도 검증 필요.
random sampling이 더 robust한 분포 estimation.
"""
import sys
import random
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_OLD = ROOT / 'eps_momentum_data.bak_pre_v80_10.db'  # v80.9
DB_NEW = ROOT / 'eps_momentum_data.db'                  # v80.10

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10  # 시작일 후 최소 10일 보유 가능해야 (끝 너무 가까우면 제외)


def main():
    print('=' * 100)
    print(f'Random start BT — N_SEEDS={N_SEEDS} × {SAMPLES_PER_SEED} random starts')
    print(f'Production rule: entry=3, exit=8, slots=3')
    print('=' * 100)

    all_results = {}
    for db, label in [(DB_OLD, 'v80.9'), (DB_NEW, 'v80.10')]:
        bts2.DB_PATH = str(db)
        dates, data = bts2.load_data()
        eligible_starts = dates[:-MIN_HOLD_DAYS]
        print(f'\n[{label}] sampling pool: {eligible_starts[0]} ~ {eligible_starts[-1]} ({len(eligible_starts)}일)')

        rets = []
        mdds = []
        seed_avg_rets = []
        for seed_i in range(N_SEEDS):
            random.seed(seed_i)
            chosen = random.sample(eligible_starts, SAMPLES_PER_SEED)
            seed_rets = []
            for sd in chosen:
                r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                seed_rets.append(r['total_return'])
            seed_avg_rets.append(sum(seed_rets) / len(seed_rets))

        all_results[label] = {
            'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avg_rets,
        }
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        print(f'  총 시뮬: {len(rets)}개 (= {N_SEEDS} seed × {SAMPLES_PER_SEED} samples)')
        print(f'  평균 수익률: {avg:+.2f}%')
        print(f'  중앙값: {med:+.2f}%')
        print(f'  최저: {min(rets):+.2f}%, 최고: {max(rets):+.2f}%')
        print(f'  표준편차: {statistics.pstdev(rets):.2f}%')
        print(f'  최악 MDD: {min(mdds):+.2f}%')
        print(f'  양수 비율: {sum(1 for r in rets if r > 0) / len(rets) * 100:.1f}%')

    # 비교
    print()
    print('=' * 100)
    print('v80.9 vs v80.10 비교')
    print('=' * 100)

    old = all_results['v80.9']
    new = all_results['v80.10']

    # seed별 비교 (동일 random sample이라 paired test)
    wins = sum(1 for a, b in zip(old['seed_avgs'], new['seed_avgs']) if b > a)
    losses = sum(1 for a, b in zip(old['seed_avgs'], new['seed_avgs']) if b < a)
    ties = N_SEEDS - wins - losses

    print(f'\n같은 random sample (same seeds)에서 paired 비교:')
    print(f'  v80.10이 v80.9보다 좋은 seed: {wins}/{N_SEEDS} ({wins/N_SEEDS*100:.1f}%)')
    print(f'  v80.9가 더 좋은 seed: {losses}/{N_SEEDS} ({losses/N_SEEDS*100:.1f}%)')
    print(f'  같음: {ties}')

    lifts = [b - a for a, b in zip(old['seed_avgs'], new['seed_avgs'])]
    avg_lift = sum(lifts) / len(lifts)
    print(f'\n  평균 lift (v80.10 - v80.9): {avg_lift:+.2f}%p')
    print(f'  최저 lift: {min(lifts):+.2f}%p')
    print(f'  최고 lift: {max(lifts):+.2f}%p')

    # 분포 비교
    print()
    print('=== 분포 (300개 시뮬 전체) ===')
    print(f'{"":>12} {"v80.9":>12} {"v80.10":>12} {"차이":>10}')
    print('-' * 50)
    metrics = [
        ('mean', lambda r: sum(r)/len(r)),
        ('median', lambda r: sorted(r)[len(r)//2]),
        ('std', lambda r: statistics.pstdev(r)),
        ('min', lambda r: min(r)),
        ('max', lambda r: max(r)),
        ('p25', lambda r: sorted(r)[len(r)//4]),
        ('p75', lambda r: sorted(r)[3*len(r)//4]),
    ]
    for name, fn in metrics:
        v_old = fn(old['rets'])
        v_new = fn(new['rets'])
        diff = v_new - v_old
        print(f'{name:>12} {v_old:+11.2f}% {v_new:+11.2f}% {diff:+9.2f}%p')

    print(f'\nMDD 비교:')
    print(f'  v80.9 worst MDD: {min(old["mdds"]):+.2f}%')
    print(f'  v80.10 worst MDD: {min(new["mdds"]):+.2f}%')


if __name__ == '__main__':
    main()
