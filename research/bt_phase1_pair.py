"""Phase 1: B 옵션 페어 BT — rev_up30 ≥ 3 + T0=0.45 직접 검증"""
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
GRID = ROOT / 'research' / 'pair_dbs'
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

    # T0 weight 변경
    if t0_weight == 0.5:
        weights = [0.2, 0.3, 0.5]
    elif t0_weight == 0.4:
        weights = [0.25, 0.35, 0.4]
    elif t0_weight == 0.45:
        weights = [0.225, 0.325, 0.45]
    elif t0_weight == 0.55:
        weights = [0.175, 0.275, 0.55]
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


def run_multistart(db_path, start_dates):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


VARIANTS = [
    ('A. baseline (현재)',                      0,  0.5),
    ('B. rev_up30 ≥ 3 단독',                   3,  0.5),
    ('C. T0=0.45 단독',                        0,  0.45),
    ('D. T0=0.40 단독',                        0,  0.40),
    ('E. rev_up30≥3 + T0=0.45 (B옵션)',         3,  0.45),
    ('F. rev_up30≥3 + T0=0.40 (C옵션)',         3,  0.40),
    ('G. rev_up30≥3 + T0=0.55 (대조)',          3,  0.55),
    ('H. rev_up30≥3 + T0=0.50 (단독비교)',       3,  0.50),
]


def main():
    print('=' * 100)
    print('Phase 1: B 옵션 페어 BT — rev_up30 ≥ 3 + T0=0.45 직접 검증')
    print('=' * 100)

    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]
    print(f'시작일 6개, 모두 50거래일+\n')

    rows = []
    for name, ru, t0 in VARIANTS:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'pair_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, rev_up_min=ru, t0_weight=t0)
        rets, mdds = run_multistart(db, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({
            'name': name, 'avg': avg, 'med': med,
            'min': min(rets), 'max': max(rets),
            'worst_mdd': worst_mdd, 'risk_adj': risk_adj,
        })

    base = rows[0]
    print(f'{"변형":<35} {"avg":>8} {"med":>8} {"min":>8} {"MDD":>8} {"risk_adj":>9} {"ΔRet":>8}')
    print('-' * 100)
    for r in rows:
        marker = ' ★' if r is base else ('  ✓' if r['avg'] - base['avg'] >= 1 else '   ')
        d = r['avg'] - base['avg']
        d_mdd = r['worst_mdd'] - base['worst_mdd']
        print(f'{marker}{r["name"]:<33} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["min"]:+7.2f}% {r["worst_mdd"]:+7.2f}% {r["risk_adj"]:>8.2f} {d:+7.2f}%p')

    print()
    print('주요 비교:')
    A = base
    B = next(r for r in rows if 'B.' in r['name'])
    E = next(r for r in rows if 'E.' in r['name'])
    print(f'  A (현재) → B (rev_up30≥3 단독):  ΔRet {B["avg"]-A["avg"]:+.2f}%p, ΔMDD {B["worst_mdd"]-A["worst_mdd"]:+.2f}%p')
    print(f'  B → E (+ T0=0.45 추가):          ΔRet {E["avg"]-B["avg"]:+.2f}%p, ΔMDD {E["worst_mdd"]-B["worst_mdd"]:+.2f}%p')
    print(f'  A → E (B옵션 전체 효과):         ΔRet {E["avg"]-A["avg"]:+.2f}%p, ΔMDD {E["worst_mdd"]-A["worst_mdd"]:+.2f}%p')


if __name__ == '__main__':
    main()
