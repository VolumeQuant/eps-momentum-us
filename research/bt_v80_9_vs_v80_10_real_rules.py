"""실제 production 룰 paired 비교

v80.9 production: 가중치 0.4/0.3/0.2/0.1 + exit_top=8 + N=2 유예
v80.10b production: 가중치 0.3/0.1/0.1/0.5 + exit_top=10 + N=0 (유예 없음, BT 최적)

지금까지 비교한 것:
  - v80.9 + N별: N=2가 +6.21%p alpha
  - v80.10 + N별: N=0이 best, N=2는 -5.37%p

빠진 비교: v80.9 실제 운영 룰(N=2) vs v80.10b 실제 운영 룰(N=0).
즉 production 전체 변경의 진짜 alpha.
"""
import sys
import random
import statistics
import shutil
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_pe_weights as btpe
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'  # v80.10 가중치
DB_V80_9 = ROOT / 'research' / 'pe_4d_dbs' / 'w_40_30_20_10_60d.db'  # v80.9 가중치 60일

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def run_paired(db_path, exit_top, hold_days, seed_starts):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    rets = []
    seed_avg_rets = []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(dates, data, price_series, hold_days=hold_days,
                                  entry_top=3, exit_top=exit_top, max_slots=3,
                                  start_date=sd)
            rets.append(r['total_return'])
            seed_rets.append(r['total_return'])
        seed_avg_rets.append(sum(seed_rets) / len(seed_rets))
    return rets, seed_avg_rets


def main():
    print('=' * 100)
    print('실제 production 룰 paired 비교')
    print('  v80.9: 가중치 0.4/0.3/0.2/0.1 + exit_top=8 + N=2 유예')
    print('  v80.10b: 가중치 0.3/0.1/0.1/0.5 + exit_top=10 + N=0')
    print('=' * 100)

    if not DB_V80_9.exists():
        print('\nv80.9 가중치 DB 생성 중...')
        shutil.copy(DB_ORIGINAL, DB_V80_9)
        weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
        btpe.regenerate(DB_V80_9, weights)
    print(f'v80.9 DB: {DB_V80_9}')
    print(f'v80.10 DB: {DB_ORIGINAL}')

    # 같은 시작일 사용 (paired)
    bth.DB_PATH = DB_ORIGINAL
    dates, _, _ = bth.load_data_ext()
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    print(f'\n시작일 풀: {len(eligible_starts)}개 ({dates[0]} ~ {eligible_starts[-1]})')
    print(f'Random 100 seed × 3 starts paired\n')

    # v80.9 production rules
    print('[v80.9 production] 가중치 0.4/0.3/0.2/0.1 + exit=8 + N=2 유예')
    import time
    t0 = time.time()
    rets_v9, seed_v9 = run_paired(DB_V80_9, exit_top=8, hold_days=2,
                                  seed_starts=seed_starts)
    print(f'  [{time.time()-t0:.1f}s] avg={sum(rets_v9)/len(rets_v9):+.2f}% '
          f'min={min(rets_v9):+.2f}% max={max(rets_v9):+.2f}%')

    # v80.10b production rules
    print('\n[v80.10b production] 가중치 0.3/0.1/0.1/0.5 + exit=10 + N=0')
    t0 = time.time()
    rets_v10, seed_v10 = run_paired(DB_ORIGINAL, exit_top=10, hold_days=0,
                                    seed_starts=seed_starts)
    print(f'  [{time.time()-t0:.1f}s] avg={sum(rets_v10)/len(rets_v10):+.2f}% '
          f'min={min(rets_v10):+.2f}% max={max(rets_v10):+.2f}%')

    # 추가 비교: 운영 변경 전 후 단계별 분해
    print('\n[참조 — 단계 분해] 가중치만 바꿔서 효과 분리')
    print('  v80.9 가중치 + exit=8 + N=0 (가중치 효과만 제외)')
    t0 = time.time()
    rets_v9_n0, seed_v9_n0 = run_paired(DB_V80_9, exit_top=8, hold_days=0,
                                        seed_starts=seed_starts)
    print(f'  [{time.time()-t0:.1f}s] avg={sum(rets_v9_n0)/len(rets_v9_n0):+.2f}%')

    print('  v80.10 가중치 + exit=8 + N=0 (exit_top 변경 제외)')
    t0 = time.time()
    rets_v10_e8, seed_v10_e8 = run_paired(DB_ORIGINAL, exit_top=8, hold_days=0,
                                          seed_starts=seed_starts)
    print(f'  [{time.time()-t0:.1f}s] avg={sum(rets_v10_e8)/len(rets_v10_e8):+.2f}%')

    print('  v80.10 가중치 + exit=10 + N=2 (유예 룰 유지)')
    t0 = time.time()
    rets_v10_n2, seed_v10_n2 = run_paired(DB_ORIGINAL, exit_top=10, hold_days=2,
                                          seed_starts=seed_starts)
    print(f'  [{time.time()-t0:.1f}s] avg={sum(rets_v10_n2)/len(rets_v10_n2):+.2f}%')

    # paired lift v80.9 → v80.10b
    print()
    print('=' * 100)
    print('★ 진짜 비교: v80.9 production → v80.10b production')
    print('=' * 100)
    lifts = [b - a for a, b in zip(seed_v9, seed_v10)]
    avg_lift = sum(lifts) / len(lifts)
    wins = sum(1 for l in lifts if l > 0)
    losses = sum(1 for l in lifts if l < 0)
    print(f'\n  v80.9 평균: {sum(seed_v9)/len(seed_v9):+.2f}%')
    print(f'  v80.10b 평균: {sum(seed_v10)/len(seed_v10):+.2f}%')
    print(f'  paired avg lift: {avg_lift:+.2f}%p')
    print(f'  paired min lift: {min(lifts):+.2f}%p')
    print(f'  paired max lift: {max(lifts):+.2f}%p')
    print(f'  paired wins: {wins}/{N_SEEDS}')
    print(f'  paired losses: {losses}/{N_SEEDS}')

    # 단계별 분해
    print()
    print('=' * 100)
    print('변경 단계별 alpha 분해 (paired vs v80.9 production)')
    print('=' * 100)
    base = seed_v9
    print(f'\n  v80.9 production (가중 0.4 + exit=8 + N=2): baseline')

    def step_lift(label, seeds):
        lifts = [b - a for a, b in zip(base, seeds)]
        avg = sum(lifts) / len(lifts)
        wins = sum(1 for l in lifts if l > 0)
        print(f'  {label}: {avg:+7.2f}%p (paired {wins}/{N_SEEDS} wins)')

    step_lift('  → 가중치만 변경 (v80.10 + exit=8 + N=0)        ', seed_v10_e8)
    step_lift('  → 가중치 + exit 변경 (v80.10 + exit=10 + N=0)   ', seed_v10)
    step_lift('  → 가중치 + exit + 유예 유지 (v80.10 + exit=10 + N=2)', seed_v10_n2)

    # 유예 룰 제거의 진짜 효과 (v80.10 환경 내)
    print()
    print('=' * 100)
    print('v80.10 환경 내에서: 유예 룰 유지 vs 제거')
    print('=' * 100)
    lifts_hold = [b - a for a, b in zip(seed_v10_n2, seed_v10)]  # n2 → n0
    avg = sum(lifts_hold) / len(lifts_hold)
    wins = sum(1 for l in lifts_hold if l > 0)
    print(f'\n  v80.10 + N=2 유지: {sum(seed_v10_n2)/len(seed_v10_n2):+.2f}%')
    print(f'  v80.10 + N=0 제거: {sum(seed_v10)/len(seed_v10):+.2f}%')
    print(f'  유예 제거 lift: {avg:+.2f}%p ({wins}/{N_SEEDS} wins)')


if __name__ == '__main__':
    main()
