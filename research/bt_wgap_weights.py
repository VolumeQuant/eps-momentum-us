"""w_gap 3일 가중치 변형 BT — T-2/T-1/T0 비율 비교

현재 production: [0.2, 0.3, 0.5] = T-2:20% / T-1:30% / T0:50% (오래된순)
사용자 제안:     [0.1, 0.2, 0.7] = T-2:10% / T-1:20% / T0:70% — 오늘 강조

기타 비교 변형:
  current     [0.2, 0.3, 0.5]   = 20/30/50 (production)
  proposed    [0.1, 0.2, 0.7]   = 10/20/70 (사용자 제안)
  uniform     [0.33,0.33,0.34]  = 33/33/33 (균등)
  smooth      [0.25, 0.30, 0.45]= 25/30/45 (더 부드러움)
  today_50    [0.2, 0.3, 0.5]   (= current)
  today_60    [0.15, 0.25, 0.60]
  today_80    [0.05, 0.15, 0.80] = 더 극단적
  today_100   [0.0, 0.0, 1.0]    = T0만 (1일 z-score)

방법:
  - _compute_w_gap_map의 weights를 monkey-patch
  - DB 변형별 복제 + regenerate (MA filter는 current production 유지)
  - random 100 seed × 3 starts paired
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import numpy as np
import daily_runner as dr
import bt_breakout_hold as bth
import bt_ma_filter_extended as ext

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'wgap_weight_dbs'
GRID.mkdir(exist_ok=True)

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 3
HOLD_DAYS = 0


def make_wgap_with_weights(custom_weights_3):
    """custom_weights_3 = [T-2, T-1, T0] 비율로 _compute_w_gap_map 대체 함수 생성"""
    def _patched(cursor, today_str, tickers):
        dates = dr._get_recent_dates(cursor, 'composite_rank', today_str, 3)
        dates = sorted(dates)  # 오래된 순

        MISSING_PENALTY = 30

        score_by_date = {}
        for d in dates:
            rows = cursor.execute(
                'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, '
                'rev_growth FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
                (d,)
            ).fetchall()
            conv_gaps = {}
            for r in rows:
                tk = r[0]
                conv_gaps[tk] = dr._apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6])

            vals = list(conv_gaps.values())
            if len(vals) >= 2:
                mean_v = np.mean(vals)
                std_v = np.std(vals)
                if std_v > 0:
                    score_by_date[d] = {
                        tk: max(30.0, 65 + (-(v - mean_v) / std_v) * 15)
                        for tk, v in conv_gaps.items()
                    }
                else:
                    score_by_date[d] = {tk: 65 for tk in conv_gaps}
            else:
                score_by_date[d] = {tk: 65 for tk in conv_gaps}

        # 변형 weights — len(dates)에 따라 정규화
        full_w = custom_weights_3  # [T-2, T-1, T0]
        if len(dates) == 3:
            weights = full_w
        elif len(dates) == 2:
            # T-1, T0 사용 (정규화)
            sub = [full_w[1], full_w[2]]
            s = sum(sub)
            weights = [w / s for w in sub] if s > 0 else [0.5, 0.5]
        elif len(dates) == 1:
            weights = [1.0]
        else:
            weights = []

        p2_by_date = {}
        for d in dates:
            rows = cursor.execute(
                'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (d,)
            ).fetchall()
            p2_by_date[d] = {r[0] for r in rows}

        result = {}
        for tk in tickers:
            wg = 0
            for i, d in enumerate(dates):
                is_today = (d == today_str)
                if not is_today and tk not in p2_by_date.get(d, set()):
                    score = MISSING_PENALTY
                else:
                    score = score_by_date.get(d, {}).get(tk)
                    if score is None:
                        score = MISSING_PENALTY
                wg += score * weights[i]
            result[tk] = wg
        return result
    return _patched


def regenerate_with_custom_weights(test_db, weights):
    """현재 MA filter (production) 유지 + custom weights로 w_gap 재계산"""
    # _compute_w_gap_map monkey-patch
    original_fn = dr._compute_w_gap_map
    dr._compute_w_gap_map = make_wgap_with_weights(weights)
    try:
        # ext.regenerate는 'current' 변형 (MA120 + MA60 fallback) 사용
        ext.regenerate(test_db, 'current', ma_df=None)
    finally:
        dr._compute_w_gap_map = original_fn


def run_bt(db_path):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    if len(dates) <= MIN_HOLD_DAYS:
        return None
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=HOLD_DAYS,
                entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
                max_slots=MAX_SLOTS, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


VARIANTS = [
    ('current_20_30_50',  [0.20, 0.30, 0.50]),  # production
    ('proposed_10_20_70', [0.10, 0.20, 0.70]),  # user proposal
    ('uniform_33_33_33',  [0.33, 0.33, 0.34]),
    ('smooth_25_30_45',   [0.25, 0.30, 0.45]),
    ('today_60_15_25_60', [0.15, 0.25, 0.60]),
    ('today_80_05_15_80', [0.05, 0.15, 0.80]),
    ('today_only_0_0_100',[0.00, 0.00, 1.00]),  # 1일만
]


def main():
    print('=' * 110)
    print('w_gap 3일 가중치 변형 BT — production 대비 비교')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS}, hold={HOLD_DAYS}')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/변형')
    print('=' * 110)
    print()
    print('변형 (T-2 / T-1 / T0 비율):')
    for name, w in VARIANTS:
        ratio = '/'.join(f'{x*100:.0f}' for x in w)
        marker = ' ★' if name == 'current_20_30_50' else '  '
        marker += ' (제안)' if name == 'proposed_10_20_70' else ''
        print(f'  {marker} {name:<22}: {ratio:>12}%')

    all_results = {}
    for name, weights in VARIANTS:
        db = GRID / f'{name}.db'
        print(f'\n[{name}] DB 복제 + regenerate (weights = {weights})...')
        t0 = time.time()
        shutil.copy(DB_ORIGINAL, db)
        regenerate_with_custom_weights(db, weights)
        print(f'  regenerate: {time.time()-t0:.1f}s')

        t1 = time.time()
        res = run_bt(db)
        if res is None:
            print('  데이터 부족 — skip')
            continue
        all_results[name] = res
        avg = sum(res['rets']) / len(res['rets'])
        med = sorted(res['rets'])[len(res['rets']) // 2]
        mdd = min(res['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        print(f'  BT: {time.time()-t1:.1f}s | avg={avg:+6.2f}% med={med:+6.2f}% '
              f'mdd={mdd:+6.2f}% risk_adj={ra:+.2f}')

    print()
    print('=' * 110)
    print(f'결과 분포 ({N_SEEDS*SAMPLES_PER_SEED}개 시뮬/변형)')
    print('=' * 110)
    print(f'{"variant":<22} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"p25":>9} {"p75":>9} {"min":>9} {"max":>9} {"MDD":>8} {"ra":>7}')
    print('-' * 110)
    for name, _ in VARIANTS:
        if name not in all_results:
            continue
        r = all_results[name]
        rets = sorted(r['rets'])
        n = len(rets)
        avg = sum(rets) / n
        med = rets[n // 2]
        std = statistics.pstdev(rets)
        p25 = rets[n // 4]
        p75 = rets[3 * n // 4]
        mdd = min(r['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        marker = ' ★' if name == 'current_20_30_50' else '  '
        print(f'{marker}{name:<20} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{p25:+8.2f}% {p75:+8.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}% '
              f'{mdd:+7.2f}% {ra:+6.2f}')

    if 'current_20_30_50' in all_results:
        print()
        print('=' * 110)
        print('current (20/30/50 production) 대비 paired 비교')
        print('=' * 110)
        base = all_results['current_20_30_50']['seed_avgs']
        print(f'{"vs":<22} {"avg lift":>10} {"min lift":>10} {"max lift":>10} '
              f'{"#wins":>6} {"#losses":>8} {"#ties":>6}')
        print('-' * 90)
        for name, _ in VARIANTS:
            if name == 'current_20_30_50' or name not in all_results:
                continue
            new = all_results[name]['seed_avgs']
            lifts = [b - a for a, b in zip(base, new)]
            wins = sum(1 for l in lifts if l > 0)
            losses = sum(1 for l in lifts if l < 0)
            ties = sum(1 for l in lifts if l == 0)
            avg_lift = sum(lifts) / len(lifts)
            verdict = '✓ 우월' if wins >= 70 else '✗ 열세' if losses >= 70 else '~ 동등'
            mark = ' (제안)' if name == 'proposed_10_20_70' else ''
            print(f'  {name+mark:<20} {avg_lift:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p '
                  f'{wins:>5} {losses:>7} {ties:>5}  {verdict}')


if __name__ == '__main__':
    main()
