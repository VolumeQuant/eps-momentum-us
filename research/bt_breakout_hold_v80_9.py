"""v80.9 가중치 (0.4/0.3/0.2/0.1) 환경에서 N일 유예 BT

사용자 가설:
  단기 강조(0.4/0.3/0.2/0.1) → 신호 변동 큼 → ⏸️ 유예가 노이즈 흡수 알파
  장기 강조(0.3/0.1/0.1/0.5) → 신호 안정 → ⏸️ 유예 효과 사라짐 (이미 검증, N=0 best)

이 BT: v80.9 가중치 + 60일 데이터로 N=0/1/2/3/5/무제한 비교.
  N>0이 양수 lift → 가설 검증
  N=0이 또 best → ⏸️ 룰이 처음부터 임의 규칙
"""
import sys
import shutil
import random
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_pe_weights as btpe
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'pe_4d_dbs'
TEST_DB = GRID / 'w_40_30_20_10_60d.db'  # v80.9 가중치 + 60일 데이터

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def main():
    print('=' * 100)
    print('v80.9 가중치 (0.4/0.3/0.2/0.1) 환경 + 60일 데이터에서 N일 유예 BT')
    print('=' * 100)

    # Step 1: production DB 복사 후 v80.9 가중치로 재계산
    print(f'\n[Step 1] {DB_ORIGINAL} → {TEST_DB} 복사 후 가중치 0.4/0.3/0.2/0.1 적용')
    import time
    t0 = time.time()
    shutil.copy(DB_ORIGINAL, TEST_DB)
    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
    btpe.regenerate(TEST_DB, weights)
    print(f'  regenerate: {time.time()-t0:.1f}s')

    # Step 2: N별 BT
    print(f'\n[Step 2] N별 BT (entry=3, exit=8, slots=3, ⏸️ N일 유예)')
    bth.DB_PATH = TEST_DB
    dates, data, price_series = bth.load_data_ext()
    print(f'  데이터 로드: {len(dates)}일')

    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    HOLD_DAYS_LIST = [0, 1, 2, 3, 5, 999]
    EXIT_TOP = 8  # v80.9 production 룰

    all_results = {}
    for N in HOLD_DAYS_LIST:
        t_n = time.time()
        rets = []
        mdds = []
        seed_avg_rets = []
        hold_trades = 0
        rank_trades = 0
        for seed_i, chosen in enumerate(seed_starts):
            seed_rets = []
            for sd in chosen:
                r = bth.simulate_hold(dates, data, price_series, hold_days=N,
                                      entry_top=3, exit_top=EXIT_TOP, max_slots=3,
                                      start_date=sd)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                seed_rets.append(r['total_return'])
                for t in r['trades']:
                    if t['reason'] == 'hold_expired':
                        hold_trades += 1
                    elif t['reason'] == 'rank_exit':
                        rank_trades += 1
            seed_avg_rets.append(sum(seed_rets) / len(seed_rets))
        all_results[N] = {
            'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avg_rets,
            'hold_trades': hold_trades, 'rank_trades': rank_trades,
        }
        avg = sum(rets) / len(rets)
        n_label = '무제한' if N == 999 else f'{N}일'
        print(f'  [{time.time()-t_n:>5.1f}s] N={n_label:<6} avg={avg:+6.2f}% '
              f'min={min(rets):+6.2f}% max={max(rets):+6.2f}% '
              f'MDD={min(mdds):+6.2f}% hold_exit={hold_trades} rank_exit={rank_trades}')

    # 결과 분포
    print()
    print('=' * 100)
    print('v80.9 가중치 환경 N별 결과 분포 (300개 시뮬)')
    print('=' * 100)
    print(f'{"N":<8} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"min":>9} {"max":>9} {"MDD":>8}')
    print('-' * 80)
    for N in HOLD_DAYS_LIST:
        r = all_results[N]
        rets = r['rets']
        n = len(rets)
        rets_s = sorted(rets)
        avg = sum(rets) / n
        med = rets_s[n // 2]
        std = statistics.pstdev(rets)
        mdd = min(r['mdds'])
        n_label = '무제한' if N == 999 else f'{N}일'
        marker = ' ★ v80.9 baseline (이전 룰)' if N == 0 else ' ← 메시지 안내 룰' if N == 2 else ''
        print(f'  N={n_label:<6} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{min(rets):+8.2f}% {max(rets):+8.2f}% {mdd:+7.2f}%{marker}')

    # paired N=0 vs N>0
    print()
    print('=' * 100)
    print('N=0 (즉시 매도) 대비 paired 비교 — 사용자 가설 검증')
    print('=' * 100)
    base = all_results[0]['seed_avgs']
    print(f'{"vs":<8} {"avg lift":>10} {"min lift":>10} {"max lift":>10} '
          f'{"#wins":>7} {"#losses":>8}')
    print('-' * 65)
    for N in HOLD_DAYS_LIST:
        if N == 0:
            continue
        new = all_results[N]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        losses = sum(1 for l in lifts if l < 0)
        avg_lift = sum(lifts) / len(lifts)
        n_label = '무제한' if N == 999 else f'{N}일'
        print(f'  N={n_label:<5} {avg_lift:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p '
              f'{wins:>6} {losses:>7}')

    # 가설 verdict
    print()
    print('=' * 100)
    print('사용자 가설 검증')
    print('=' * 100)
    n2_lift = sum(b - a for a, b in zip(base, all_results[2]['seed_avgs'])) / N_SEEDS
    n3_lift = sum(b - a for a, b in zip(base, all_results[3]['seed_avgs'])) / N_SEEDS
    n_best_lift = max(
        sum(b - a for a, b in zip(base, all_results[N]['seed_avgs'])) / N_SEEDS
        for N in HOLD_DAYS_LIST
    )
    print(f'\nv80.9 (단기 강조) 환경에서:')
    print(f'  N=2일 vs N=0: {n2_lift:+.2f}%p')
    print(f'  N=3일 vs N=0: {n3_lift:+.2f}%p')
    print(f'  N별 최대 lift: {n_best_lift:+.2f}%p')

    print(f'\nv80.10 (장기 강조) 환경 (이전 결과):')
    print(f'  N=2일 vs N=0: -5.37%p')
    print(f'  N=3일 vs N=0: -5.47%p')

    if n2_lift > 0:
        print(f'\n✓ 가설 부분 검증 — v80.9에선 ⏸️ 유예가 알파 (단기 강조 노이즈 완충)')
        print(f'  v80.10 전환 후 유예 효과 사라짐 → 안내문 제거 합당')
    else:
        print(f'\n✗ 가설 미검증 — v80.9에서도 N=0이 best')
        print(f'  ⏸️ 룰은 가중치와 무관하게 임의 규칙. 메시지 안내문 제거 권장.')


if __name__ == '__main__':
    main()
