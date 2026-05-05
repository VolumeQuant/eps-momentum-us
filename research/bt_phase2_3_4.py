"""Phase 2/3/4 — 그룹 A (ratio) + D (rev_bonus) + H (T0 가중치)
Base = rev_up30 ≥ 3 + max + cap=1.0 + bonus=off
"""
import sqlite3
import shutil
import sys
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'phase234_dbs'
GRID.mkdir(exist_ok=True)


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs) if segs else 0


def conv_full(ag, ru, rd, na, nc, n90, rg, ratio_mode='baseline', rev_bonus_mode='cliff'):
    """ratio + rev_bonus 변형 통합"""
    ru = ru or 0
    rd = rd or 0
    na = na or 0
    rg = rg or 0

    # ratio 계산 (그룹 A 변형)
    if na > 0:
        up_r = ru / na
        down_r = rd / na
        if ratio_mode == 'baseline':
            ratio = up_r
        elif ratio_mode == 'confidence':
            ratio = up_r * min(na / 10, 1.0)
        elif ratio_mode == 'net':  # (up - down) / N
            ratio = max(0, (ru - rd) / na)
        elif ratio_mode == 'penalty03':
            ratio = max(0, up_r - 0.3 * down_r)
        elif ratio_mode == 'penalty05':
            ratio = max(0, up_r - 0.5 * down_r)
        else:
            ratio = up_r
    else:
        ratio = 0

    eps_floor = 0
    if nc is not None and n90 and abs(n90) > 0.01:
        eps_floor = min(abs((nc - n90) / n90), 1.0)
    base = max(ratio, eps_floor)

    # rev_bonus (그룹 D 변형)
    if rev_bonus_mode == 'cliff':
        rb = 0.3 if rg >= 0.30 else 0
    elif rev_bonus_mode == 'proportional':
        rb = min(min(rg, 0.5) * 0.6, 0.3)
    elif rev_bonus_mode == 'sigmoid':
        rb = 0.3 * min(rg / 0.5, 1.0) if rg else 0
    else:
        rb = 0.3 if rg >= 0.30 else 0

    return ag * (1 + base + rb)


def regenerate(test_db, ratio_mode='baseline', rev_bonus_mode='cliff', t0_weight=0.5):
    """rev_up30 ≥ 3 base + 변형"""
    original_path = dr.DB_PATH
    original_fn = dr._apply_conviction
    dr.DB_PATH = str(test_db)

    def patched_conv(ag, ru, na, nc, n90, rev_growth=None):
        return conv_full(ag, ru, 0, na, nc, n90, rev_growth, ratio_mode, rev_bonus_mode)
    dr._apply_conviction = patched_conv

    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]
        for today in dates:
            rows = cur.execute('''
                SELECT ticker, adj_gap, rev_up30, rev_down30, num_analysts, ntm_current, ntm_7d,
                       ntm_30d, ntm_60d, ntm_90d, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            elig_conv = []
            for r in rows:
                tk, ag, ru, rd, na, nc, n7, n30, n60, n90, rg = r
                if ag is None: continue
                ms = calc_min_seg(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
                if ms < -2: continue
                # base: rev_up30 ≥ 3
                if (ru or 0) < 3: continue
                cg = conv_full(ag, ru, rd, na, nc, n90, rg, ratio_mode, rev_bonus_mode)
                if cg is not None:
                    elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}
            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                            (cr, today, tk))

            # T0 가중치 변경 시: _compute_w_gap_map 직접 patch 어려우니 simulate 단계에서 wgap 계산
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
        dr._apply_conviction = original_fn


def compute_wgap_with_t0(cursor, today_str, tickers, t0_weight=0.5):
    """T0 가중치 customizable (그룹 H용)"""
    import numpy as np
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
        # custom: T0=t0, T1=t1, T2=t2 (1-t0 분배)
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


def run_multistart(db_path, start_dates):
    bts2.DB_PATH = str(db_path)
    dates, data = bts2.load_data()
    rets, mdds = [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    return rets, mdds


def run_phase(phase_name, variants):
    print()
    print('=' * 100)
    print(phase_name)
    print('=' * 100)
    bts2.DB_PATH = str(DB_ORIGINAL)
    dates, _ = bts2.load_data()
    start_dates = dates[2:8]

    rows = []
    for name, kw in variants:
        slug = ''.join(c if c.isalnum() else '_' for c in name)[:25]
        db = GRID / f'p_{slug}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, **kw)
        rets, mdds = run_multistart(db, start_dates)
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets) // 2]
        worst_mdd = min(mdds)
        risk_adj = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        rows.append({'name': name, 'avg': avg, 'med': med,
                     'worst_mdd': worst_mdd, 'risk_adj': risk_adj})

    base = rows[0]
    print(f'{"변형":<35} {"avg":>8} {"med":>8} {"MDD":>8} {"risk_adj":>9} {"ΔRet":>8}')
    print('-' * 100)
    for r in sorted(rows, key=lambda x: -x['avg']):
        marker = ' ★' if r is base else ('  ✓' if r['avg'] - base['avg'] >= 1 else '   ')
        d = r['avg'] - base['avg']
        print(f'{marker}{r["name"]:<33} {r["avg"]:+7.2f}% {r["med"]:+7.2f}% '
              f'{r["worst_mdd"]:+7.2f}% {r["risk_adj"]:>8.2f} {d:+7.2f}%p')
    return rows


def main():
    # Phase 2: 그룹 A (ratio 변형)
    phase2_variants = [
        ('A: baseline (현재)', {'ratio_mode': 'baseline'}),
        ('A: confidence N=10', {'ratio_mode': 'confidence'}),
        ('A: (up-down)/N',     {'ratio_mode': 'net'}),
        ('A: up/N - 0.3×down', {'ratio_mode': 'penalty03'}),
        ('A: up/N - 0.5×down', {'ratio_mode': 'penalty05'}),
    ]
    run_phase('Phase 2 — 그룹 A (ratio 변형)', phase2_variants)

    # Phase 3: 그룹 D (rev_bonus 변형)
    phase3_variants = [
        ('D: cliff (현재)',      {'rev_bonus_mode': 'cliff'}),
        ('D: proportional',     {'rev_bonus_mode': 'proportional'}),
        ('D: sigmoid',          {'rev_bonus_mode': 'sigmoid'}),
    ]
    run_phase('Phase 3 — 그룹 D (rev_bonus)', phase3_variants)

    # Phase 4: 그룹 H (T0 가중치)
    phase4_variants = [
        ('H: T0=0.50 (현재)',   {'t0_weight': 0.5}),
        ('H: T0=0.40',          {'t0_weight': 0.4}),
        ('H: T0=0.45',          {'t0_weight': 0.45}),
        ('H: T0=0.55',          {'t0_weight': 0.55}),
    ]
    run_phase('Phase 4 — 그룹 H (T0 가중치)', phase4_variants)


if __name__ == '__main__':
    main()
