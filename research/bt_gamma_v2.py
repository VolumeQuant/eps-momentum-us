"""γ_v2 변형 BT — 신규/만성 cap 분리 + opt2 결합.

변형:
  baseline    — control (γ 미적용)
  gamma       — 현 v80.3 (cap 시 무조건 dir=0)
  gamma_v2    — 신규 cap만 dir=0, 만성 cap은 그대로
  gv2_opt2    — γ_v2 + 정상 영역 sign flip (고평가+둔화 fix)
  opt2        — γ + 정상 영역 sign flip
  baseline_opt2 — baseline + 정상 영역 sign flip (γ 없이 sign flip만)

multistart: 처음 5일 시작일, production 룰(3/8/3).
"""
import sqlite3
import shutil
import sys
import statistics
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr
import backtest_s2_params as bts2

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = Path('/tmp/db_pre_gamma.db') if Path('/tmp/db_pre_gamma.db').exists() else ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'gv2_dbs'
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


def fmt_segments_yesterday(cur, today, ticker):
    """어제 segments 계산 — 신규 cap 판정용"""
    yest_row = cur.execute('''
        SELECT date FROM ntm_screening
        WHERE date < ? AND ticker = ? AND ntm_current IS NOT NULL
        ORDER BY date DESC LIMIT 1
    ''', (today, ticker)).fetchone()
    if not yest_row:
        return None
    yest = yest_row[0]
    row = cur.execute('''
        SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
        FROM ntm_screening WHERE date=? AND ticker=?
    ''', (yest, ticker)).fetchone()
    if not row:
        return None
    return fmt_segments(*row)


def calc_baseline(segs, fwd_pe_chg, yest_segs=None):
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df = max(-0.3, min(0.3, direction / 30))
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq, score * (1 + df)


def calc_gamma(segs, fwd_pe_chg, yest_segs=None):
    """γ: cap 시 무조건 dir=0"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.0
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df = max(-0.3, min(0.3, direction / 30))
        min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


def calc_gamma_v2(segs, fwd_pe_chg, yest_segs=None):
    """γ_v2: 신규 cap만 dir=0. 만성 cap(어제도 cap)은 dir 그대로"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)

    is_new_cap = False
    if cap_hit and yest_segs:
        yest_cap = any(abs(s) >= SEG_CAP for s in yest_segs)
        is_new_cap = not yest_cap
    elif cap_hit and not yest_segs:
        # 어제 데이터 없으면 신규로 간주 (안전)
        is_new_cap = True

    if cap_hit and is_new_cap:
        # 신규 cap → 노이즈 의심 → dir=0
        df = 0.0
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
    else:
        # 정상 OR 만성 cap → dir 그대로
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df = max(-0.3, min(0.3, direction / 30))
        min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


def calc_opt2(segs, fwd_pe_chg, yest_segs=None):
    """γ + 정상 영역 sign flip (fwd>0 시 dir 부호 반전)"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = 0.0
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        df = -df_raw if (fwd_pe_chg is not None and fwd_pe_chg > 0) else df_raw
        min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


def calc_gv2_opt2(segs, fwd_pe_chg, yest_segs=None):
    """γ_v2 + opt2 — 가장 정교한 결합"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)

    is_new_cap = False
    if cap_hit and yest_segs:
        yest_cap = any(abs(s) >= SEG_CAP for s in yest_segs)
        is_new_cap = not yest_cap
    elif cap_hit and not yest_segs:
        is_new_cap = True

    if cap_hit and is_new_cap:
        df = 0.0
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        # opt2 sign flip 적용
        df = -df_raw if (fwd_pe_chg is not None and fwd_pe_chg > 0) else df_raw
        min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


def calc_baseline_opt2(segs, fwd_pe_chg, yest_segs=None):
    """baseline + opt2 (γ 없이 sign flip만)"""
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df_raw = max(-0.3, min(0.3, direction / 30))
    df = -df_raw if (fwd_pe_chg is not None and fwd_pe_chg > 0) else df_raw
    min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


VARIANTS = {
    'baseline': calc_baseline,
    'gamma': calc_gamma,
    'gamma_v2': calc_gamma_v2,
    'opt2': calc_opt2,
    'gv2_opt2': calc_gv2_opt2,
    'base_opt2': calc_baseline_opt2,
}


def regenerate(test_db, calc_fn):
    original = dr.DB_PATH
    dr.DB_PATH = test_db
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
                # baseline 가정으로 fwd_pe_chg 역산
                _, df_base, eq_base, _ = calc_baseline(segs, None)
                denom = (1 + df_base) * eq_base
                if abs(denom) < 1e-6:
                    continue
                fwd_pe_chg = ag_old / denom

                yest_segs = fmt_segments_yesterday(cur, today, tk)

                score_n, df_n, eq_n, asc_n = calc_fn(segs, fwd_pe_chg, yest_segs)
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


def main():
    print('=' * 100)
    print('γ_v2 + opt2 변형 BT (multistart 5 시작일, 룰 3/8/3)')
    print(f'DB: {DB_ORIGINAL}')
    print('=' * 100)

    rows = []
    for name, fn in VARIANTS.items():
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, fn)
        rets, mdds, sds = multistart(db)
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

    # vs baseline
    base = next((r for r in rows if r['name'] == 'baseline'), None)
    gamma = next((r for r in rows if r['name'] == 'gamma'), None)
    if base:
        print()
        print('=' * 100)
        print('비교 (baseline 대비)')
        print('=' * 100)
        for r in rows:
            if r['name'] == 'baseline':
                continue
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            d_gamma = r['avg'] - gamma['avg'] if gamma else 0
            print(f'  {r["name"]:<12}: ΔRet vs base {d_ret:+6.2f}%p, '
                  f'ΔMDD {d_mdd:+5.2f}%p, Δrisk_adj {d_ra:+5.2f}, '
                  f'vs γ {d_gamma:+6.2f}%p')


if __name__ == '__main__':
    main()
