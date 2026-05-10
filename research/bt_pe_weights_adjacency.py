"""인접 안정성 검증 — w_30_10_10_50 (A 후보) 주변 ±0.05 6변형 walk-forward

⚠️ PRODUCTION SAFETY:
- 원본 DB(eps_momentum_data.db)는 절대 수정하지 않음
- pe_adj_dbs/ 하위 복사본만 사용

목적: A 후보가 plateau 위에 있는지 (single-point luck 아닌지) 검증
  - 7개 변형 모두 walk-forward 5 splits OOS lift 양수 → plateau OK
  - 한 변형이라도 음수 lift → A는 단일 점, 폐기

변형:
  baseline   (0.30, 0.10, 0.10, 0.50)
  w7+5       (0.35, 0.10, 0.10, 0.45)
  w7-5       (0.25, 0.10, 0.10, 0.55)
  w30+5      (0.30, 0.15, 0.10, 0.45)
  w30-5      (0.30, 0.05, 0.10, 0.55)
  w60+5      (0.30, 0.10, 0.15, 0.45)
  w60-5      (0.30, 0.10, 0.05, 0.55)
  + production 비교 (0.40, 0.30, 0.20, 0.10)
"""
import sys
import shutil
import sqlite3
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_pe_weights as btpe
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'pe_adj_dbs'
GRID.mkdir(exist_ok=True)


VARIANTS = [
    ('baseline',  (0.30, 0.10, 0.10, 0.50)),
    ('w7+5',      (0.35, 0.10, 0.10, 0.45)),
    ('w7-5',      (0.25, 0.10, 0.10, 0.55)),
    ('w30+5',     (0.30, 0.15, 0.10, 0.45)),
    ('w30-5',     (0.30, 0.05, 0.10, 0.55)),
    ('w60+5',     (0.30, 0.10, 0.15, 0.45)),
    ('w60-5',     (0.30, 0.10, 0.05, 0.55)),
    ('production', (0.40, 0.30, 0.20, 0.10)),
]

SPLITS = [20, 25, 30, 35, 40]


def name_combo(w7, w30, w60, w90):
    return f'adj_{int(w7*100):02d}_{int(w30*100):02d}_{int(w60*100):02d}_{int(w90*100):02d}'


def simulate_split(db_path, T):
    """train T일 + test 나머지 cold-start"""
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    train_dates = dates[:T]
    test_start = dates[T] if T < len(dates) else None
    if test_start is None:
        return None, None
    train_r = bts2.simulate(dates, data, 3, 8, 3, start_date=train_dates[0])['total_return']
    test_r = bts2.simulate(dates, data, 3, 8, 3, start_date=test_start)['total_return']
    return train_r, test_r


def main():
    print('=' * 110)
    print('인접 안정성 검증 — A 후보 w_30_10_10_50 주변 ±0.05')
    print('=' * 110)

    # DB 생성 / regenerate
    db_paths = {}
    for name, w in VARIANTS:
        slug = name_combo(*w)
        db = GRID / f'{slug}.db'
        db_paths[name] = db
        if not db.exists():
            print(f'  생성 중: {name} {w}')
            shutil.copy(DB_ORIGINAL, db)
            weights = {'7d': w[0], '30d': w[1], '60d': w[2], '90d': w[3]}
            btpe.regenerate(db, weights)
            print(f'    완료')
    print()

    # Walk-forward 5 splits
    results = {name: {'train': [], 'test': [], 'lift': []} for name, _ in VARIANTS}
    prod_test = {}

    for T in SPLITS:
        print(f'\n--- Split T={T} ---')
        # production test return을 기준으로 lift 계산
        prod_train, prod_t = simulate_split(db_paths['production'], T)
        prod_test[T] = prod_t
        print(f'  production: train={prod_train:+.2f}% test={prod_t:+.2f}%')

        for name, w in VARIANTS:
            if name == 'production':
                results[name]['train'].append(prod_train)
                results[name]['test'].append(prod_t)
                results[name]['lift'].append(0.0)
                continue
            train_r, test_r = simulate_split(db_paths[name], T)
            lift = test_r - prod_t
            results[name]['train'].append(train_r)
            results[name]['test'].append(test_r)
            results[name]['lift'].append(lift)
            marker = '✓' if lift > 0 else '✗'
            print(f'  {name:<12} train={train_r:+7.2f}% test={test_r:+7.2f}% lift={lift:+7.2f}%p {marker}')

    # 통합
    print()
    print('=' * 110)
    print('OOS lift 통합 (5 splits 평균)')
    print('=' * 110)
    print(f'{"variant":<12} {"weights":<25} {"avg_lift":>10} {"min_lift":>10} {"max_lift":>10} {"#pos":>5}')
    print('-' * 110)
    for name, w in VARIANTS:
        lifts = results[name]['lift']
        avg = sum(lifts) / len(lifts)
        n_pos = sum(1 for v in lifts if v > 0)
        print(f'  {name:<10} {str(w):<25} {avg:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p {n_pos:>3}/5')

    # 최종 판정
    print()
    print('=' * 110)
    print('plateau 판정')
    print('=' * 110)
    target_variants = [v for v in VARIANTS if v[0] != 'production']
    all_pos = True
    for name, w in target_variants:
        n_pos = sum(1 for v in results[name]['lift'] if v > 0)
        if n_pos < 5:
            all_pos = False
            print(f'  ✗ {name}: {n_pos}/5 splits positive — single-point 의심')
            break
    if all_pos:
        print('  ✓ A 후보 + 6 인접 변형 모두 5/5 splits OOS lift 양수')
        print('  → plateau 확인. A (w_30_10_10_50) 채택 안전.')
    else:
        print('  → A 후보 단일 점 의심. 변경 보류 권장.')


if __name__ == '__main__':
    main()
