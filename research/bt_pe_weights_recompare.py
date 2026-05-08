"""캐시된 DB 그대로 multistart 개수만 바꿔 재측정

n_starts ∈ {6, 10, 15, 20, 33}
random baseline은 33 seed (이미 다 다름이라 변경 불필요)
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


def multistart(db_path, n_starts):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    starts = dates[:n_starts]
    for sd in starts:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds, starts


def stats(rets, mdds):
    n = len(rets)
    avg = sum(rets) / n
    med = sorted(rets)[n // 2]
    std = statistics.pstdev(rets)
    worst_mdd = min(mdds)
    risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
    return avg, med, std, min(rets), max(rets), worst_mdd, risk_adj


def main():
    N_VALUES = [6, 8, 10, 12, 15, 20, 33]

    # 결과: results[n_starts][variant] = stats
    results = {n: {} for n in N_VALUES}
    for name, dbf in VARIANTS:
        db = GRID / dbf
        for n in N_VALUES:
            rets, mdds, _ = multistart(db, n)
            results[n][name] = stats(rets, mdds)

    # 변형별 비교 표 (다양한 n_starts)
    print('=' * 110)
    print('multistart 개수별 결과 (avg return)')
    print('=' * 110)
    header = f'{"variant":<10}'
    for n in N_VALUES:
        header += f' {f"n={n}":>10}'
    print(header)
    print('-' * 80)
    for name, _ in VARIANTS:
        line = f'  {name:<8}'
        for n in N_VALUES:
            avg = results[n][name][0]
            line += f' {avg:>+9.2f}%'
        if name == 'w7=0.40':
            line += '  ★ production'
        print(line)

    # 변형별 risk_adj
    print()
    print('=' * 110)
    print('risk_adj (avg / |worst_MDD|)')
    print('=' * 110)
    header = f'{"variant":<10}'
    for n in N_VALUES:
        header += f' {f"n={n}":>8}'
    print(header)
    print('-' * 80)
    for name, _ in VARIANTS:
        line = f'  {name:<8}'
        for n in N_VALUES:
            ra = results[n][name][6]
            line += f' {ra:>7.2f}'
        if name == 'w7=0.40':
            line += '  ★'
        print(line)

    # n별로 best variant 찾기
    print()
    print('=' * 110)
    print('각 n_starts에서의 best 변형')
    print('=' * 110)
    for n in N_VALUES:
        ranked = sorted(VARIANTS, key=lambda v: results[n][v[0]][0], reverse=True)
        prod_rank = next(i for i, (name, _) in enumerate(ranked) if name == 'w7=0.40') + 1
        prod_avg = results[n]['w7=0.40'][0]
        print(f'\nn={n}:')
        print(f'  production w7=0.40 → rank {prod_rank}/13, avg={prod_avg:+.2f}%')
        for i, (name, _) in enumerate(ranked[:5]):
            avg, med, std, mn, mx, mdd, ra = results[n][name]
            d_prod = avg - prod_avg
            print(f'    {i+1}위 {name:<10} avg={avg:+6.2f}% (Δprod={d_prod:+5.2f}%p) '
                  f'med={med:+6.2f}% std={std:>4.1f} MDD={mdd:+5.2f}% risk_adj={ra:.2f}')

    # 짧은 시작일 영향도 분리: 33 vs 6 차이
    print()
    print('=' * 110)
    print('33 vs 6: 짧은 시작일 영향도')
    print('=' * 110)
    print(f'{"variant":<10} {"avg(n=6)":>10} {"avg(n=33)":>11} {"Δ":>8} {"std(n=6)":>10} {"std(n=33)":>11}')
    print('-' * 80)
    for name, _ in VARIANTS:
        a6 = results[6][name][0]
        a33 = results[33][name][0]
        s6 = results[6][name][2]
        s33 = results[33][name][2]
        diff = a33 - a6
        line = f'  {name:<8} {a6:+9.2f}% {a33:+10.2f}% {diff:+7.2f}%p {s6:>9.1f} {s33:>10.1f}'
        if name == 'w7=0.40':
            line += '  ★'
        print(line)

    # 시작일별 변형 순위 변동성 (n=6일 때 vs n=33일 때 best가 같은가?)
    print()
    print('=' * 110)
    print('peak 위치 안정성')
    print('=' * 110)
    for n in N_VALUES:
        best = max(VARIANTS, key=lambda v: results[n][v[0]][0])
        best_name = best[0]
        best_avg = results[n][best_name][0]
        prod_avg = results[n]['w7=0.40'][0]
        gap = best_avg - prod_avg
        print(f'  n={n:>2}: best={best_name} (avg={best_avg:+.2f}%, Δprod={gap:+5.2f}%p)')


if __name__ == '__main__':
    main()
