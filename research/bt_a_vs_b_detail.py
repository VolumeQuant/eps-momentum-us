"""A vs B 정밀 비교 + BT 재현성 검증

검증:
1. 같은 옵션 두 번 돌려서 결과 일치 (재현성)
2. 시작일별 ret/MDD 상세 출력
3. baseline ↔ production +57% 일치 확인
4. A/B 통계 비교
"""
import sqlite3
import shutil
import sys
import statistics
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import numpy as np
import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'a_vs_b_dbs'
GRID.mkdir(exist_ok=True)


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs) if segs else 0


def compute_wgap_with_t0(cursor, today_str, tickers, t0_weight=0.5):
    dates = dr._get_recent_dates(cursor, 'composite_rank', today_str, 3)
    dates = sorted(dates)
    MISSING_PENALTY = 30

    score_by_date = {}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth '
            'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (d,)
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

    if t0_weight == 0.5:
        weights = [0.2, 0.3, 0.5]
    elif t0_weight == 0.45:
        weights = [0.225, 0.325, 0.45]
    else:
        rest = 1 - t0_weight
        weights = [rest * 0.4, rest * 0.6, t0_weight]

    if len(dates) == 2:
        weights = [0.4, 0.6]
    elif len(dates) == 1:
        weights = [1.0]

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


def regenerate(test_db, rev_up_min=0, t0_weight=0.5):
    original_path = dr.DB_PATH
    dr.DB_PATH = str(test_db)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        for today in dates:
            rows = cur.execute('''
                SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_7d,
                       ntm_30d, ntm_60d, ntm_90d, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            elig_conv = []
            for r in rows:
                tk, ag, ru, na, nc, n7, n30, n60, n90, rg = r
                if ag is None: continue
                ms = calc_min_seg(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
                if ms < -2: continue
                if rev_up_min > 0 and (ru or 0) < rev_up_min: continue
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}
            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                            (cr, today, tk))
            tickers = list(new_cr.keys())
            wmap = compute_wgap_with_t0(cur, today, tickers, t0_weight)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                            (rk, today, tk))
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original_path


def run_per_start(db_path, start_dates):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    results = []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        results.append({
            'start': sd, 'ret': r['total_return'], 'mdd': r['max_dd'],
            'trades': r['n_trades'], 'win_rate': r['win_rate'],
        })
    return results


def main():
    print('=' * 100)
    print('A vs B 정밀 비교 + BT 재현성 검증')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]
    print(f'시작일: {start_dates}\n')

    # 4개 시뮬: baseline 2회 (재현성), A, B
    configs = [
        ('Baseline run #1',                  0,  0.5),
        ('Baseline run #2 (재현성)',          0,  0.5),
        ('A. rev_up30≥3',                    3,  0.5),
        ('B. rev_up30≥3 + T0=0.45',          3,  0.45),
    ]

    all_results = {}
    for name, ru, t0 in configs:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'avb_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, rev_up_min=ru, t0_weight=t0)
        results = run_per_start(db, start_dates)
        all_results[name] = results

    # 시작일별 상세
    print(f'{"시작일":<14}', end='')
    for name in [c[0] for c in configs]:
        print(f' {name[:18]:>20}', end='')
    print()
    print('-' * 100)
    for i, sd in enumerate(start_dates):
        print(f'{sd:<14}', end='')
        for name in [c[0] for c in configs]:
            r = all_results[name][i]
            print(f' ret={r["ret"]:+5.1f} mdd={r["mdd"]:+5.1f}', end='')
        print()
    print()

    # 통계 요약
    print(f'{"변형":<35} {"avg":>8} {"med":>8} {"std":>5} {"min":>8} {"max":>8} {"MDD":>8} {"risk_adj":>9}')
    print('-' * 100)
    summary = {}
    for name, _, _ in configs:
        rets = [r['ret'] for r in all_results[name]]
        mdds = [r['mdd'] for r in all_results[name]]
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        summary[name] = {'avg': avg, 'med': med, 'std': std,
                         'min': min(rets), 'max': max(rets),
                         'worst_mdd': worst_mdd, 'risk_adj': risk_adj}
        print(f'  {name:<33} {avg:+7.2f}% {med:+7.2f}% {std:>4.2f} '
              f'{min(rets):+7.2f}% {max(rets):+7.2f}% {worst_mdd:+7.2f}% {risk_adj:>8.2f}')

    # 재현성 검증
    print()
    print('=' * 100)
    print('재현성 검증: Baseline #1 vs #2')
    print('=' * 100)
    b1 = summary['Baseline run #1']
    b2 = summary['Baseline run #2 (재현성)']
    print(f'  avg 차이: {abs(b1["avg"] - b2["avg"]):.4f}%p (0이면 완벽 재현성)')
    print(f'  MDD 차이: {abs(b1["worst_mdd"] - b2["worst_mdd"]):.4f}%p')

    # A vs B 통계 비교
    print()
    print('=' * 100)
    print('A vs B 정밀 비교')
    print('=' * 100)
    A = summary['A. rev_up30≥3']
    B = summary['B. rev_up30≥3 + T0=0.45']
    base = summary['Baseline run #1']

    A_rets = [r['ret'] for r in all_results['A. rev_up30≥3']]
    B_rets = [r['ret'] for r in all_results['B. rev_up30≥3 + T0=0.45']]

    print(f'A: avg {A["avg"]:+.2f}%, MDD {A["worst_mdd"]:+.2f}%, std {A["std"]:.2f}, range [{A["min"]:+.1f}, {A["max"]:+.1f}]')
    print(f'B: avg {B["avg"]:+.2f}%, MDD {B["worst_mdd"]:+.2f}%, std {B["std"]:.2f}, range [{B["min"]:+.1f}, {B["max"]:+.1f}]')
    print()

    # 시작일별 A-B 차이
    print('시작일별 A vs B:')
    a_better = 0
    b_better = 0
    for i, sd in enumerate(start_dates):
        a_ret = A_rets[i]
        b_ret = B_rets[i]
        diff = a_ret - b_ret
        winner = 'A' if a_ret > b_ret else 'B' if b_ret > a_ret else '='
        if a_ret > b_ret: a_better += 1
        elif b_ret > a_ret: b_better += 1
        print(f'  {sd}: A={a_ret:+6.2f}%  B={b_ret:+6.2f}%  ΔRet={diff:+5.2f}%p  ({winner})')
    print(f'\n시작일별 우승: A {a_better}개 / B {b_better}개')

    # baseline 대비
    print()
    print('Baseline 대비:')
    print(f'  A: ΔRet {A["avg"]-base["avg"]:+5.2f}%p, ΔMDD {A["worst_mdd"]-base["worst_mdd"]:+5.2f}%p, Δrisk_adj {A["risk_adj"]-base["risk_adj"]:+5.2f}')
    print(f'  B: ΔRet {B["avg"]-base["avg"]:+5.2f}%p, ΔMDD {B["worst_mdd"]-base["worst_mdd"]:+5.2f}%p, Δrisk_adj {B["risk_adj"]-base["risk_adj"]:+5.2f}')


if __name__ == '__main__':
    main()
