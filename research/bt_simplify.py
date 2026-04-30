"""단순화 BT — case별 보너스/너프 다 빼고 v80.4 baseline 대비 성과 측정.

변형:
  v80_4         — 현재 production (β1 + opt4 + Case1)            [control]
  no_case1      — β1 + opt4만 (Case 1 z-score 보너스 제거)
  no_capopt     — Case 1만 (β1 + opt4 제거 → 순수 baseline cap/C4)
  pure_baseline — 셋 다 제거 (가장 단순)

multistart 5시작일 + production 룰 3/8/3.
"""
import sqlite3
import shutil
import sys
import statistics
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import numpy as np
import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = Path('/tmp/db_pre_gamma.db') if Path('/tmp/db_pre_gamma.db').exists() else ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'simplify_dbs'
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


def calc_pure_baseline(segs, fwd_pe_chg):
    """β1 X, opt4 X — 순수 dir/30 그대로"""
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df = max(-0.3, min(0.3, direction / 30))
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq


def calc_v80_4(segs, fwd_pe_chg):
    """β1 (cap → dir=+0.3) + opt4 (C4 sign flip)"""
    score = sum(segs)
    if any(abs(s) >= SEG_CAP for s in segs):
        df = 0.3  # β1: cap 발동 → +0.3 max
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw  # opt4: C4 sign flip
        else:
            df = df_raw
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq


def make_w_gap_map_fn(case1_enabled):
    """custom _compute_w_gap_map — Case 1 보너스 on/off 가능"""
    def _compute(cursor, today_str, tickers):
        dates = dr._get_recent_dates(cursor, 'composite_rank', today_str, 3)
        dates = sorted(dates)
        MISSING_PENALTY = 30
        CASE1_PERIOD = 30
        CASE1_NTM_THR = 1.0
        CASE1_PX_THR = -1.0
        CASE1_SCORE_BONUS = 8

        score_by_date = {}
        for d in dates:
            rows = cursor.execute(
                'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, '
                'rev_growth, ntm_30d, price '
                'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
                (d,)
            ).fetchall()
            conv_gaps = {}
            ntm_px_data = {}
            for r in rows:
                tk = r[0]
                conv_gaps[tk] = dr._apply_conviction(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6])
                ntm_px_data[tk] = (r[4], r[7], r[8])

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

            if case1_enabled:
                target_30d = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=CASE1_PERIOD)).strftime('%Y-%m-%d')
                d_30ago = cursor.execute(
                    'SELECT MAX(date) FROM ntm_screening WHERE date <= ?', (target_30d,)
                ).fetchone()
                px_30d_map = {}
                if d_30ago and d_30ago[0]:
                    px_30d_rows = cursor.execute(
                        'SELECT ticker, price FROM ntm_screening WHERE date=? AND price > 0',
                        (d_30ago[0],)
                    ).fetchall()
                    px_30d_map = {r[0]: r[1] for r in px_30d_rows}

                for tk in list(score_by_date[d].keys()):
                    nd = ntm_px_data.get(tk)
                    if not nd: continue
                    ntm_cur, ntm_30d_val, price_now = nd
                    ntm_chg = ((ntm_cur - ntm_30d_val) / ntm_30d_val * 100) \
                        if ntm_30d_val and abs(ntm_30d_val) > 0.01 and ntm_cur else 0
                    px_30d = px_30d_map.get(tk)
                    px_chg = ((price_now - px_30d) / px_30d * 100) \
                        if px_30d and px_30d > 0 and price_now and price_now > 0 else 0
                    if ntm_chg > CASE1_NTM_THR and px_chg < CASE1_PX_THR:
                        score_by_date[d][tk] += CASE1_SCORE_BONUS

        weights = [0.2, 0.3, 0.5]
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
    return _compute


def regenerate(test_db, calc_fn, case1_enabled):
    """DB 전 일자 재계산 — calc_fn 적용 + Case 1 on/off"""
    original = dr.DB_PATH
    original_w_gap = dr._compute_w_gap_map
    dr.DB_PATH = test_db
    dr._compute_w_gap_map = make_w_gap_map_fn(case1_enabled)
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                       adj_gap, rev_up30, num_analysts, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            if not rows:
                continue

            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, ag_old, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or ag_old is None:
                    continue
                # back-derive fwd_pe_chg from old adj_gap using baseline logic
                # (/tmp/db_pre_gamma.db는 v80.2 baseline = 순수 dir/30)
                _, df_base, eq_base = calc_pure_baseline(segs, None)
                denom = (1 + df_base) * eq_base
                if abs(denom) < 1e-6:
                    continue
                fwd_pe_chg = ag_old / denom

                score_n, df_n, eq_n = calc_fn(segs, fwd_pe_chg)
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
        dr._compute_w_gap_map = original_w_gap


def multistart(db_path, n_starts=5):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    start_dates = dates[:n_starts]
    rets, mdds, trades_per_start = [], [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
        trades_per_start.append(r['trades'])
    return rets, mdds, start_dates, trades_per_start


VARIANTS = [
    ('v80_4', calc_v80_4, True),         # control: β1 + opt4 + Case1
    ('no_case1', calc_v80_4, False),     # β1 + opt4만
    ('no_capopt', calc_pure_baseline, True),   # Case1만
    ('pure_baseline', calc_pure_baseline, False),  # 다 빼기
]


def main():
    print('=' * 100)
    print('단순화 BT — case별 보너스/너프 영향 측정')
    print(f'DB 원본: {DB_ORIGINAL}')
    print('=' * 100)

    rows = []
    sds = None
    trades_by_variant = {}
    for name, fn, case1 in VARIANTS:
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, fn, case1)
        rets, mdds, sds, trades = multistart(db, n_starts=5)
        n = len(rets)
        avg = sum(rets) / n
        med = sorted(rets)[n // 2]
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj, 'rets': rets,
        })
        trades_by_variant[name] = trades

    print()
    print(f'{"Variant":<14}', end='')
    for sd in sds:
        print(f' {sd:>10}', end='')
    print(f' {"avg":>8} {"std":>5} {"worstMDD":>9} {"risk_adj":>8}')
    print('-' * (16 + 11 * len(sds) + 35))
    for r in rows:
        print(f'  {r["name"]:<12}', end='')
        for ret in r['rets']:
            print(f' {ret:>9.2f}%', end='')
        print(f' {r["avg"]:+7.2f}% {r["std"]:>4.2f} {r["worst_mdd"]:+8.2f}% {r["risk_adj"]:>7.2f}')

    base = next((r for r in rows if r['name'] == 'v80_4'), None)
    if base:
        print()
        print('=' * 100)
        print('v80_4 (현재 production) 대비 차이')
        print('=' * 100)
        for r in rows:
            if r['name'] == 'v80_4':
                continue
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            print(f'  {r["name"]:<14}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}')


if __name__ == '__main__':
    main()
