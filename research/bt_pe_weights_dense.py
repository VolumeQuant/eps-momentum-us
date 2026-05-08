"""dense grid for fwd_pe_chg time-window weights + random baseline

7d 가중치 0.10~0.70 (13점) — 30d/60d/90d 비율은 3:2:1 고정 (production decay 유지)
multistart 33시작일
random_rank baseline: 33 seeds (각 날짜 part2_rank 랜덤 재할당)

표본 → 본실행 진행
"""
import sqlite3
import shutil
import sys
import statistics
import random
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'pe_weight_dbs'
GRID.mkdir(exist_ok=True)
SEG_CAP = 100


def fmt_segments(nc, n7, n30, n60, n90):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    return tuple(max(-SEG_CAP, min(SEG_CAP, v)) for v in (
        (nc - n7) / abs(n7) * 100,
        (n7 - n30) / abs(n30) * 100,
        (n30 - n60) / abs(n60) * 100,
        (n60 - n90) / abs(n90) * 100,
    ))


def calc_gamma_opt4(segs, fwd_pe_chg):
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw
        else:
            df = df_raw
    valid = [s for s in segs if abs(s) < SEG_CAP]
    min_seg = min(valid) if valid else 0
    eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    score = sum(segs)
    return score, df, eps_q


def regenerate(test_db, weights):
    """fwd_pe_chg를 새 weights로 재계산 + adj_gap/cr/p2 재정렬"""
    original = dr.DB_PATH
    dr.DB_PATH = test_db
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()

        rows = cur.execute(
            'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL AND price > 0'
        ).fetchall()
        price_history = {}
        for tk, d, p in rows:
            price_history.setdefault(tk, []).append((d, p))
        for tk in price_history:
            price_history[tk].sort(key=lambda x: x[0])

        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            today_d = datetime.strptime(today, '%Y-%m-%d').date()
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price,
                       rev_up30, num_analysts, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            if not rows:
                continue

            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, px_now, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or px_now is None or px_now <= 0 or nc is None or nc <= 0:
                    continue

                hist = price_history.get(tk, [])
                px_then = {}
                for n_days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                    target = (today_d - timedelta(days=n_days))
                    best, best_diff = None, None
                    for d, p in hist:
                        d_obj = datetime.strptime(d, '%Y-%m-%d').date()
                        if d_obj > today_d:
                            continue
                        diff = abs((d_obj - target).days)
                        if best_diff is None or diff < best_diff:
                            best_diff = diff
                            best = p
                    px_then[key] = best

                fwd_pe_now = px_now / nc
                ntm_map = {'7d': n7, '30d': n30, '60d': n60, '90d': n90}
                weighted_sum = 0.0
                total_weight = 0.0
                for key, w in weights.items():
                    pt = px_then.get(key)
                    nt = ntm_map.get(key)
                    if pt and pt > 0 and nt and nt > 0:
                        fwd_pe_then = pt / nt
                        pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                        weighted_sum += w * pe_chg
                        total_weight += w
                if total_weight <= 0:
                    continue
                fwd_pe_chg = weighted_sum / total_weight

                score_n, df_n, eq_n = calc_gamma_opt4(segs, fwd_pe_chg)
                asc_n = score_n * (1 + df_n)
                ag_n = fwd_pe_chg * (1 + df_n) * eq_n
                new_data.append((tk, score_n, asc_n, ag_n, ru, na, nc, n90, rg))

            for tk, sc, asc, ag, *_ in new_data:
                cur.execute(
                    'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE date=? AND ticker=?',
                    (sc, asc, ag, today, tk)
                )

            elig_conv = []
            for tk, _, _, ag, ru, na, nc, n90, rg in new_data:
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today, tk)
                )

            tickers = list(new_cr.keys())
            wmap = dr._compute_w_gap_map(cur, today, tickers)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute(
                    'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                    (rk, today, tk)
                )
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original


def randomize_p2(test_db, seed):
    """원본 DB 복사본의 part2_rank를 각 날짜마다 랜덤 셔플 (eligible 종목 한정)"""
    rng = random.Random(seed)
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    for today in dates:
        rows = cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (today,)
        ).fetchall()
        tickers = [r[0] for r in rows]
        rng.shuffle(tickers)
        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for rk, tk in enumerate(tickers[:30], 1):
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
    conn.commit()
    conn.close()


def multistart(db_path, n_starts=33):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    start_dates = dates[:n_starts]
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds, start_dates


def stats(rets, mdds):
    n = len(rets)
    avg = sum(rets) / n
    med = sorted(rets)[n // 2]
    std = statistics.pstdev(rets)
    worst_mdd = min(mdds)
    risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
    return {
        'n': n, 'avg': avg, 'med': med, 'std': std,
        'min': min(rets), 'max': max(rets),
        'p25': sorted(rets)[n // 4], 'p75': sorted(rets)[3 * n // 4],
        'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
    }


def make_grid():
    """7d 0.10~0.70 (13점), 30d:60d:90d = 3:2:1 비율 고정"""
    grid = []
    for w7 in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
        rest = 1 - w7
        w30 = rest * 0.5      # 3/(3+2+1) = 0.5
        w60 = rest * (1/3)    # 2/6
        w90 = rest * (1/6)    # 1/6
        grid.append((f'w7={w7:.2f}', {'7d': w7, '30d': w30, '60d': w60, '90d': w90}))
    return grid


def main():
    print('=' * 110)
    print('Dense PE-weight grid + multistart + random baseline')
    print('=' * 110)

    grid = make_grid()
    print(f'\n변형 ({len(grid)}개):')
    for name, w in grid:
        print(f'  {name}: 7d={w["7d"]:.3f} 30d={w["30d"]:.3f} 60d={w["60d"]:.3f} 90d={w["90d"]:.3f}')

    # 13 variants
    rows = []
    import time
    t_start = time.time()
    for name, weights in grid:
        t0 = time.time()
        db = GRID / f'{name.replace("=","_").replace(".","p")}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, weights)
        rets, mdds, sds = multistart(db, n_starts=33)
        s = stats(rets, mdds)
        s['name'] = name
        s['weights'] = weights
        rows.append(s)
        print(f'  [{time.time()-t0:>4.1f}s] {name}: avg={s["avg"]:+6.2f}% med={s["med"]:+6.2f}% '
              f'min={s["min"]:+6.2f}% max={s["max"]:+6.2f}% MDD={s["worst_mdd"]:+5.2f}% '
              f'risk_adj={s["risk_adj"]:.2f}')

    # random baseline (33 seeds)
    print(f'\n[random baseline] 33 seeds (multistart과 동등 수)')
    rand_rets, rand_mdds = [], []
    for seed in range(33):
        t0 = time.time()
        db = GRID / f'rand_{seed:02d}.db'
        shutil.copy(DB_ORIGINAL, db)
        randomize_p2(db, seed)
        # 시작일: 첫날 1개만 (셔플 자체가 randomness)
        bts2.DB_PATH = str(db)
        dates, data = bts2.load_data()
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=dates[0])
        rand_rets.append(r['total_return'])
        rand_mdds.append(r['max_dd'])
        if seed < 3 or seed % 8 == 0:
            print(f'  [{time.time()-t0:>4.1f}s] seed={seed:02d}: ret={r["total_return"]:+6.2f}% MDD={r["max_dd"]:+5.2f}%')
    rand_stats = stats(rand_rets, rand_mdds)
    rand_stats['name'] = 'random (33seeds)'

    print(f'\n총 소요: {time.time()-t_start:.1f}s')

    # summary
    print()
    print('=' * 110)
    print('Dense grid 결과')
    print('=' * 110)
    print(f'{"variant":<10} {"avg":>8} {"med":>8} {"min":>8} {"max":>8} {"std":>5} '
          f'{"MDD":>7} {"risk":>6}')
    print('-' * 80)
    for r in sorted(rows, key=lambda x: x['avg'], reverse=True):
        marker = ' ★' if r['name'] == 'w7=0.40' else '  '
        print(f'{marker}{r["name"]:<8} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["std"]:>4.1f} '
              f'{r["worst_mdd"]:+6.2f}% {r["risk_adj"]:>5.2f}')

    print()
    print(f'random baseline: avg={rand_stats["avg"]:+7.2f}% med={rand_stats["med"]:+7.2f}% '
          f'p25={rand_stats["p25"]:+7.2f}% p75={rand_stats["p75"]:+7.2f}% '
          f'min={rand_stats["min"]:+7.2f}% max={rand_stats["max"]:+7.2f}% '
          f'MDD={rand_stats["worst_mdd"]:+6.2f}%')

    # production 대비
    base = next((r for r in rows if r['name'] == 'w7=0.40'), None)
    if base:
        print()
        print('=' * 110)
        print(f'production (w7=0.40) 대비 차이 + random 대비 차이')
        print('=' * 110)
        print(f'{"variant":<10} {"ΔRet":>8} {"ΔMDD":>8} {"Δrisk":>7} {"vs_rand":>9}  {"verdict":<22}')
        print('-' * 80)
        for r in sorted(rows, key=lambda x: x['avg'], reverse=True):
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            vs_rand = r['avg'] - rand_stats['avg']
            if r['name'] == 'w7=0.40':
                verdict = 'baseline'
            elif d_ret >= 0 and d_mdd >= 0:
                verdict = '✓ 개선'
            elif d_ret >= 1 and d_mdd >= -2:
                verdict = '✓ 채택가능'
            elif d_ret < -3:
                verdict = '✗ 손실'
            else:
                verdict = '~ 무차이'
            print(f'  {r["name"]:<8} {d_ret:+7.2f}%p {d_mdd:+7.2f}%p {d_ra:+6.2f} '
                  f'{vs_rand:+8.2f}%p  {verdict:<22}')

    # plateau 분석
    print()
    print('=' * 110)
    print('plateau 분석 (인접 안정성)')
    print('=' * 110)
    for i in range(1, len(rows) - 1):
        r = next(x for x in rows if x['name'] == f'w7={0.10 + i*0.05:.2f}')
        prev_r = next(x for x in rows if x['name'] == f'w7={0.10 + (i-1)*0.05:.2f}')
        next_r = next(x for x in rows if x['name'] == f'w7={0.10 + (i+1)*0.05:.2f}')
        adj_avg = (prev_r['avg'] + r['avg'] + next_r['avg']) / 3
        adj_std = statistics.pstdev([prev_r['avg'], r['avg'], next_r['avg']])
        marker = ' ★' if r['name'] == 'w7=0.40' else '  '
        print(f'{marker}{r["name"]:<8} this={r["avg"]:+7.2f}% '
              f'3pt-avg={adj_avg:+7.2f}% 3pt-std={adj_std:>4.1f}')


if __name__ == '__main__':
    main()
