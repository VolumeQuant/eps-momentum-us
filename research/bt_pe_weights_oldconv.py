"""OLD conviction (v75) 공식으로 BT — commit adcb138 시점 코드 재현

5/2 시점:
  eps_floor = min(|NTM변화|, 1.0)  (cap 1.0)
  rev_bonus = 0.3 if rev_growth >= 0.30 else 0  (binary cliff)

현재 (v80.9):
  eps_floor = min(|NTM변화|, 3.0)  (cap 3.0)
  rev_bonus = min(min(rg, 0.5) * 0.6, 0.3)  (smooth)

목적: commit "midweight +5.72%p"가 OLD conviction에서 재현되는지 확인
"""
import sys
import shutil
import sqlite3
import statistics
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'pe_weight_dbs_oldconv'
GRID.mkdir(exist_ok=True)
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
SEG_CAP = 100


# OLD conviction (5/2 시점, v75)
def _apply_conviction_OLD(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None,
                          rev_growth=None):
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)  # cap 1.0
    base_conviction = max(ratio, eps_floor)

    rev_bonus = 0.0
    if rev_growth is not None and rev_growth >= 0.30:  # binary cliff
        rev_bonus = 0.3

    conviction = base_conviction + rev_bonus
    return adj_gap * (1 + conviction)


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


def _compute_w_gap_map_OLD(cursor, today_str, tickers):
    """w_gap 계산 — OLD conviction 사용"""
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
            conv_gaps[r[0]] = _apply_conviction_OLD(r[1], r[2], r[3], r[4], r[5], rev_growth=r[6])
        if not conv_gaps:
            score_by_date[d] = {}
            continue
        sorted_t = sorted(conv_gaps.items(), key=lambda x: x[1])
        rank_score = {tk: i + 1 for i, (tk, _) in enumerate(sorted_t)}
        score_by_date[d] = rank_score

    weights = {0: 0.5, 1: 0.3, 2: 0.2}
    wmap = {}
    n_dates = len(dates)
    for tk in tickers:
        weighted_score = 0
        for di, d in enumerate(dates):
            if tk in score_by_date.get(d, {}):
                weighted_score += weights[n_dates - 1 - di] * score_by_date[d][tk]
            else:
                weighted_score += weights[n_dates - 1 - di] * MISSING_PENALTY
        wmap[tk] = -weighted_score  # higher = better (역순)
    return wmap


def regenerate_OLD(test_db, weights):
    """OLD conviction으로 재정렬"""
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
            cg = _apply_conviction_OLD(ag, ru, na, nc, n90, rev_growth=rg)
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
        wmap = _compute_w_gap_map_OLD(cur, today, tickers)
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


def multistart(db_path, n_starts=33):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in dates[:n_starts]:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


VARIANTS = [
    ('current',    {'7d': 0.4,  '30d': 0.3,  '60d': 0.2,  '90d': 0.1}),
    ('uniform',    {'7d': 0.25, '30d': 0.25, '60d': 0.25, '90d': 0.25}),
    ('long_heavy', {'7d': 0.1,  '30d': 0.2,  '60d': 0.3,  '90d': 0.4}),
    ('midweight',  {'7d': 0.3,  '30d': 0.3,  '60d': 0.2,  '90d': 0.2}),
    ('short_lite', {'7d': 0.25, '30d': 0.35, '60d': 0.25, '90d': 0.15}),
]


def main():
    print('=' * 110)
    print('OLD conviction (v75) 공식으로 BT — 5/2 코드 상태 재현')
    print('=' * 110)
    print('eps_floor cap 1.0 / rev_bonus binary cliff 0.30')
    print()

    import time
    t_start = time.time()
    rows = []
    for name, weights in VARIANTS:
        t0 = time.time()
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate_OLD(db, weights)
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

    print()
    print('=' * 110)
    print('OLD conviction 결과 (33 multistart, 현재 데이터)')
    print('=' * 110)
    print(f'{"variant":<12} {"avg":>8} {"med":>8} {"min":>8} {"max":>8} {"MDD":>7} {"risk_adj":>8}')
    print('-' * 80)
    for r in sorted(rows, key=lambda x: x['avg'], reverse=True):
        marker = ' ← production' if r['name'] == 'current' else ''
        print(f'  {r["name"]:<10} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["min"]:+7.2f}% {r["max"]:+7.2f}% {r["worst_mdd"]:+6.2f}% '
              f'{r["risk_adj"]:>7.2f}{marker}')

    base = next(r for r in rows if r['name'] == 'current')
    print()
    print('=' * 110)
    print('current 대비 — commit adcb138과 비교')
    print('=' * 110)
    print('이전 commit 메시지: midweight +5.72%p ret / -3.75%p MDD')
    print()
    print(f'{"variant":<12} {"ΔRet":>8} {"ΔMDD":>8}')
    print('-' * 50)
    for r in rows:
        if r['name'] == 'current':
            continue
        d_ret = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        marker = ''
        if r['name'] == 'midweight':
            if abs(d_ret - 5.72) < 3.0:
                marker = '  ✓ 재현 성공 (~5.72%p)'
            else:
                marker = f'  ✗ 재현 실패 (commit: +5.72%p, 실제: {d_ret:+.2f}%p)'
        print(f'  {r["name"]:<10} {d_ret:+7.2f}%p {d_mdd:+7.2f}%p{marker}')


if __name__ == '__main__':
    main()
