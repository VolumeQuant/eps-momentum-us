"""4D 그리드: (w7, w30, w60, w90) 자유 탐색

각 weight ∈ {0.10, 0.20, ..., 0.70}, 단위 0.10, sum=1.0, 각 ≥ 0.10
84조합 (compositions of 10 into 4 with each ≥ 1)
n_starts=12 multistart (47일+ 보장)
NEW conviction (현재 production)
"""
import sys
import shutil
import sqlite3
import statistics
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_pe_weights as btpe
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'pe_4d_dbs'
GRID.mkdir(exist_ok=True)
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'


def gen_grid(step=0.10, min_w=0.10, total=1.0):
    """compositions of total into 4 parts with each ≥ min_w, step quantized"""
    n_steps = round(total / step)  # 10
    n_min = round(min_w / step)    # 1
    combos = []
    for a in range(n_min, n_steps + 1):
        for b in range(n_min, n_steps + 1):
            for c in range(n_min, n_steps + 1):
                d = n_steps - a - b - c
                if d < n_min:
                    continue
                combos.append((round(a * step, 2), round(b * step, 2), round(c * step, 2), round(d * step, 2)))
    return combos


def name_combo(w7, w30, w60, w90):
    return f'w_{int(w7*100):02d}_{int(w30*100):02d}_{int(w60*100):02d}_{int(w90*100):02d}'


def multistart(db_path, n_starts):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in dates[:n_starts]:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


def main():
    combos = gen_grid()
    print(f'총 조합: {len(combos)}개')
    print(f'예시: {combos[0]}, {combos[10]}, {combos[-1]}')

    # 표본 1개로 시간 측정
    print('\n[표본] 1조합 측정...')
    t0 = time.time()
    name = name_combo(*combos[0])
    db = GRID / f'{name}.db'
    shutil.copy(DB_ORIGINAL, db)
    weights = {'7d': combos[0][0], '30d': combos[0][1], '60d': combos[0][2], '90d': combos[0][3]}
    btpe.regenerate(db, weights)
    rets, mdds = multistart(db, n_starts=12)
    sample_time = time.time() - t0
    print(f'  1조합: {sample_time:.1f}s')
    print(f'  예상 총 소요: {sample_time * len(combos) / 60:.1f}분')

    # 본실행
    print(f'\n[본실행] {len(combos)}개 조합 × n_starts=12')
    print('-' * 80)
    t_start = time.time()
    results = []
    for i, (w7, w30, w60, w90) in enumerate(combos):
        name = name_combo(w7, w30, w60, w90)
        db = GRID / f'{name}.db'
        if not db.exists():
            shutil.copy(DB_ORIGINAL, db)
            weights = {'7d': w7, '30d': w30, '60d': w60, '90d': w90}
            btpe.regenerate(db, weights)
        rets, mdds = multistart(db, n_starts=12)
        n = len(rets)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        results.append({
            'name': name, 'w7': w7, 'w30': w30, 'w60': w60, 'w90': w90,
            'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })
        if (i + 1) % 10 == 0 or i == 0:
            elapsed = time.time() - t_start
            eta = elapsed / (i + 1) * (len(combos) - i - 1)
            print(f'  [{i+1:>3}/{len(combos)}] {name}: avg={avg:+6.2f}% '
                  f'risk_adj={risk_adj:.2f}  (경과 {elapsed:.0f}s, ETA {eta:.0f}s)')

    print(f'\n총 소요: {time.time()-t_start:.1f}s')

    # Top 20 (avg 기준)
    print()
    print('=' * 110)
    print('Top 20 (avg return 기준)')
    print('=' * 110)
    print(f'{"variant":<20} {"7d":>5} {"30d":>5} {"60d":>5} {"90d":>5} '
          f'{"avg":>8} {"med":>8} {"min":>8} {"max":>8} {"std":>5} {"MDD":>7} {"risk":>5}')
    print('-' * 110)
    sorted_results = sorted(results, key=lambda x: x['avg'], reverse=True)
    for r in sorted_results[:20]:
        marker = ''
        if (r['w7'], r['w30'], r['w60'], r['w90']) == (0.4, 0.3, 0.2, 0.1):
            marker = ' ★ production'
        print(f'  {r["name"]:<18} {r["w7"]:>5.2f} {r["w30"]:>5.2f} {r["w60"]:>5.2f} {r["w90"]:>5.2f} '
              f'{r["avg"]:+7.2f}% {r["med"]:+7.2f}% {r["min"]:+7.2f}% {r["max"]:+7.2f}% '
              f'{r["std"]:>4.1f} {r["worst_mdd"]:+6.2f}% {r["risk_adj"]:>4.2f}{marker}')

    # production 위치
    prod = next((r for r in results if (r['w7'], r['w30'], r['w60'], r['w90']) == (0.4, 0.3, 0.2, 0.1)), None)
    if prod:
        prod_rank = sorted_results.index(prod) + 1
        print(f'\nproduction (w7=0.4/30d=0.3/60d=0.2/90d=0.1): {prod_rank}/{len(results)}위, avg={prod["avg"]:+.2f}%')

    # Top 20 (risk_adj 기준)
    print()
    print('=' * 110)
    print('Top 20 (risk_adj 기준)')
    print('=' * 110)
    print(f'{"variant":<20} {"7d":>5} {"30d":>5} {"60d":>5} {"90d":>5} '
          f'{"avg":>8} {"MDD":>7} {"risk":>5}')
    print('-' * 80)
    sorted_ra = sorted(results, key=lambda x: x['risk_adj'], reverse=True)
    for r in sorted_ra[:20]:
        marker = ''
        if (r['w7'], r['w30'], r['w60'], r['w90']) == (0.4, 0.3, 0.2, 0.1):
            marker = ' ★ production'
        print(f'  {r["name"]:<18} {r["w7"]:>5.2f} {r["w30"]:>5.2f} {r["w60"]:>5.2f} {r["w90"]:>5.2f} '
              f'{r["avg"]:+7.2f}% {r["worst_mdd"]:+6.2f}% {r["risk_adj"]:>4.2f}{marker}')

    # 패턴 분석: 각 weight 차원별 평균 효과
    print()
    print('=' * 110)
    print('각 차원별 평균 효과 (한 weight 고정 시 다른 3개 평균)')
    print('=' * 110)
    for axis_name in ['w7', 'w30', 'w60', 'w90']:
        print(f'\n{axis_name}:')
        by_axis = {}
        for r in results:
            v = r[axis_name]
            by_axis.setdefault(v, []).append(r['avg'])
        for v in sorted(by_axis):
            vals = by_axis[v]
            print(f'  {axis_name}={v:.2f}: n={len(vals):>3}, avg={sum(vals)/len(vals):+6.2f}%, '
                  f'best={max(vals):+6.2f}%, worst={min(vals):+6.2f}%')

    # 같은 (w30, w60, w90) 분포에서 w7만 변화
    # 더 깔끔한 차원 효과 분석: 각 w7 값에서 best 조합
    print()
    print('=' * 110)
    print('w7 값별 best 조합 (해당 w7 값에서 가장 좋은 3개)')
    print('=' * 110)
    for w7_val in sorted(set(r['w7'] for r in results)):
        subset = [r for r in results if r['w7'] == w7_val]
        subset.sort(key=lambda x: x['avg'], reverse=True)
        print(f'\nw7={w7_val:.2f} (n={len(subset)}):')
        for r in subset[:3]:
            print(f'  {r["name"]:<18} avg={r["avg"]:+6.2f}% MDD={r["worst_mdd"]:+5.2f}% '
                  f'risk_adj={r["risk_adj"]:.2f}')


if __name__ == '__main__':
    main()
