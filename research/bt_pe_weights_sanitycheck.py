"""sanity check: 이전 BT 5변형을 동일 framework로 재실행 → 결과 일치 확인

이전 commit adcb138에서 검증된 변형:
  current/uniform/long_heavy/midweight/short_lite (w7 ≤ 0.4)
  결과: midweight +5.72%p ret / -3.75%p MDD ('risk_adj 우위로 거부')

내 framework가 동일하게 작동하는지 확인 + 같은 grid로 0.55 추가 비교
"""
import sys
import shutil
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_pe_weights as btpe
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'pe_weight_dbs_sanity'
GRID.mkdir(exist_ok=True)
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'

# 이전 BT 5변형 그대로 + 새 grid에서 best였던 0.55 + 다른 후보
VARIANTS = [
    ('current',    {'7d': 0.4,  '30d': 0.3,  '60d': 0.2,  '90d': 0.1}),
    ('uniform',    {'7d': 0.25, '30d': 0.25, '60d': 0.25, '90d': 0.25}),
    ('long_heavy', {'7d': 0.1,  '30d': 0.2,  '60d': 0.3,  '90d': 0.4}),
    ('midweight',  {'7d': 0.3,  '30d': 0.3,  '60d': 0.2,  '90d': 0.2}),
    ('short_lite', {'7d': 0.25, '30d': 0.35, '60d': 0.25, '90d': 0.15}),
    # 새 후보 (이전 BT 안 한 것)
    ('w7_0p55_my', {'7d': 0.55, '30d': 0.225,'60d': 0.150,'90d': 0.075}),  # 내 grid의 best
    ('w7_0p55_alt',{'7d': 0.55, '30d': 0.25, '60d': 0.15, '90d': 0.05}),    # 다른 분배
    ('w7_0p55_eq', {'7d': 0.55, '30d': 0.15, '60d': 0.15, '90d': 0.15}),    # 30d/60d/90d 균등
]


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
    print('=' * 110)
    print('Sanity check: 이전 BT 5변형 재실행 + 새 grid 0.55 변형 비교')
    print('=' * 110)
    print()
    for name, w in VARIANTS:
        s = sum(w.values())
        print(f'  {name:<14}: 7d={w["7d"]:.3f} 30d={w["30d"]:.3f} 60d={w["60d"]:.3f} 90d={w["90d"]:.3f} (sum={s:.3f})')

    # 변형별 BT
    rows = []
    import time
    t_start = time.time()
    for name, weights in VARIANTS:
        t0 = time.time()
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        btpe.regenerate(db, weights)
        # 이전 BT는 33 multistart 사용 (commit 메시지 기준)
        rets, mdds = multistart(db, n_starts=33)
        n = len(rets)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })
        print(f'  [{time.time()-t0:>4.1f}s] {name:<14} avg={avg:+6.2f}% med={med:+6.2f}% '
              f'MDD={worst_mdd:+5.2f}% risk_adj={risk_adj:.2f}')

    print(f'\n총 소요: {time.time()-t_start:.1f}s')

    # 출력
    print()
    print('=' * 110)
    print('결과 (33 multistart)')
    print('=' * 110)
    print(f'{"variant":<14} {"avg":>8} {"med":>8} {"min":>8} {"max":>8} {"MDD":>7} {"risk_adj":>8}')
    print('-' * 80)
    for r in sorted(rows, key=lambda x: x['avg'], reverse=True):
        marker = ''
        if r['name'] == 'current': marker = ' ← production'
        if r['name'].startswith('w7_0p55'): marker = ' ← new'
        print(f'  {r["name"]:<12} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+6.2f}% '
              f'{r["risk_adj"]:>7.2f}{marker}')

    # current 기준 비교
    base = next(r for r in rows if r['name'] == 'current')
    print()
    print('=' * 110)
    print('current(production) 대비 차이 — 이전 commit adcb138 결과와 대조')
    print('=' * 110)
    print('이전 commit 메시지 기록:')
    print('  midweight: +5.72%p ret / -3.75%p MDD (risk_adj 우위로 거부)')
    print()
    print(f'{"variant":<14} {"ΔRet":>8} {"ΔMDD":>8} {"Δrisk":>7}')
    print('-' * 60)
    for r in rows:
        if r['name'] == 'current':
            continue
        d_ret = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        d_ra = r['risk_adj'] - base['risk_adj']
        print(f'  {r["name"]:<12} {d_ret:+7.2f}%p {d_mdd:+7.2f}%p {d_ra:+6.2f}')


if __name__ == '__main__':
    main()
