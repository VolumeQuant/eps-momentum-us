"""Case 1 보너스 적용 layer 캘리브레이션 BT

전제: β1 + opt4 롤백 (BT 확인)
변형: Case 1을 z-score(현행) vs adj_gap(이동) 비교

목표: adj_gap 단계 어떤 배수가 z-score +8 알파(+2.84%p)를 보존하는지 측정
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
GRID = ROOT / 'research' / 'case1_layer_dbs'
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


def calc_baseline(segs):
    """순수 baseline (β1, opt4 X)"""
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df = max(-0.3, min(0.3, direction / 30))
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq


def get_case1_tickers(cursor, today_str):
    """Case 1 (NTM 30d > +1% AND 가격 30d < -1%) 종목 set 반환"""
    target_30d = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
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

    rows = cursor.execute(
        'SELECT ticker, ntm_current, ntm_30d, price '
        'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
        (today_str,)
    ).fetchall()

    case1_set = set()
    for tk, ntm_cur, ntm_30d, price_now in rows:
        if not ntm_cur or not ntm_30d or abs(ntm_30d) < 0.01:
            continue
        ntm_chg = (ntm_cur - ntm_30d) / ntm_30d * 100
        px_30d = px_30d_map.get(tk)
        if not px_30d or px_30d <= 0 or not price_now or price_now <= 0:
            continue
        px_chg = (price_now - px_30d) / px_30d * 100
        if ntm_chg > 1.0 and px_chg < -1.0:
            case1_set.add(tk)
    return case1_set


def make_w_gap_map_fn(case1_zscore_enabled):
    """custom _compute_w_gap_map — z-score 단계 Case 1 보너스 on/off"""
    def _compute(cursor, today_str, tickers):
        dates = dr._get_recent_dates(cursor, 'composite_rank', today_str, 3)
        dates = sorted(dates)
        MISSING_PENALTY = 30
        CASE1_SCORE_BONUS = 8

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

            if case1_zscore_enabled:
                case1_set = get_case1_tickers(cursor, d)
                for tk in case1_set:
                    if tk in score_by_date[d]:
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


def regenerate(test_db, case1_adjgap_factor=None, case1_zscore=False):
    """DB 재계산
    case1_adjgap_factor: None=비활성, 0.85 등 = adj_gap × factor
    case1_zscore: True면 z-score +8 보너스
    """
    original = dr.DB_PATH
    original_w_gap = dr._compute_w_gap_map
    dr.DB_PATH = test_db
    dr._compute_w_gap_map = make_w_gap_map_fn(case1_zscore)
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

            # Case 1 종목 set (오늘 기준)
            case1_set = get_case1_tickers(cur, today) if case1_adjgap_factor is not None else set()

            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, ag_old, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or ag_old is None:
                    continue
                _, df_base, eq_base = calc_baseline(segs)
                denom = (1 + df_base) * eq_base
                if abs(denom) < 1e-6:
                    continue
                fwd_pe_chg = ag_old / denom

                score_n = sum(segs)
                df_n = df_base
                eq_n = eq_base
                ag_n = fwd_pe_chg * (1 + df_n) * eq_n
                # Case 1 adj_gap 단계 부스트
                if case1_adjgap_factor is not None and tk in case1_set:
                    ag_n *= case1_adjgap_factor
                asc_n = score_n * (1 + df_n)
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
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds, start_dates


VARIANTS = [
    ('zscore_v79', None, True),       # 현행: Case 1 z-score +8 (control)
    ('adjgap_1.05', 1.05, False),     # 약한 보너스
    ('adjgap_1.10', 1.10, False),
    ('adjgap_1.15', 1.15, False),
    ('adjgap_1.20', 1.20, False),
    ('adjgap_1.30', 1.30, False),     # 강한 보너스
    ('adjgap_1.50', 1.50, False),     # 매우 강한 보너스
    ('no_case1', None, False),        # Case 1 완전 제거 (참고)
]


def main():
    print('=' * 100)
    print('Case 1 보너스 layer 캘리브레이션 BT')
    print(f'DB 원본: {DB_ORIGINAL}')
    print(f'전제: β1 + opt4 롤백 (순수 baseline)')
    print('=' * 100)

    rows = []
    sds = None
    for name, factor, zscore in VARIANTS:
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, case1_adjgap_factor=factor, case1_zscore=zscore)
        rets, mdds, sds = multistart(db, n_starts=5)
        n = len(rets)
        avg = sum(rets) / n
        std = statistics.pstdev(rets)
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'std': std,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj, 'rets': rets,
        })

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

    base = next((r for r in rows if r['name'] == 'zscore_v79'), None)
    if base:
        print()
        print('=' * 100)
        print('zscore_v79 (현행 Case 1 z-score) 대비 차이')
        print('=' * 100)
        for r in rows:
            if r['name'] == 'zscore_v79':
                continue
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            print(f'  {r["name"]:<14}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}')


if __name__ == '__main__':
    main()
