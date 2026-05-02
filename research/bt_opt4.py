"""opt4 (C4만 sign flip) BT — 메모리 v75 검증 인사이트 + 사용자 직관 통합.

opt4 = baseline + C4(고평가+둔화)만 dir 부호 flip
  - C1/C2/C3/cap: baseline 그대로 (양수 차별 알파 보존)
  - C4: sign flip → 1.3× → 매도 강조 (사용자 지적 fix)

비교 변형:
  baseline   — control
  opt4       — C4만 fix (사용자 의도 정확)
  opt2       — 모든 고평가 sign flip (참고: B/D 변형과 비슷)
  γ          — cap 시 dir=0 (현 v80.3, 비교용)

multistart 처음 5일 시작일 + production 룰 3/8/3.
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
GRID = ROOT / 'research' / 'opt4_dbs'
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


def calc_baseline(segs, fwd_pe_chg):
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df = max(-0.3, min(0.3, direction / 30))
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq, score * (1 + df)


def calc_gamma(segs, fwd_pe_chg):
    score = sum(segs)
    if any(abs(s) >= SEG_CAP for s in segs):
        df = 0.0
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df = max(-0.3, min(0.3, direction / 30))
        min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


def calc_opt2(segs, fwd_pe_chg):
    """모든 고평가(fwd>0) sign flip — 참고 비교용"""
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df_raw = max(-0.3, min(0.3, direction / 30))
    if fwd_pe_chg is not None and fwd_pe_chg > 0:
        df = -df_raw
    else:
        df = df_raw
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq, score * (1 + df)


def calc_opt4(segs, fwd_pe_chg):
    """opt4: C4 (고평가+둔화)만 sign flip — 핵심 변형"""
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df_raw = max(-0.3, min(0.3, direction / 30))
    # C4: fwd>0 AND dir<0
    if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
        df = -df_raw  # sign flip: -0.3 → +0.3, 0.7× → 1.3× 매도 강조
    else:
        df = df_raw  # baseline 그대로 (C1/C2/C3, cap)
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq, score * (1 + df)


VARIANTS = {
    'baseline': calc_baseline,
    'gamma': calc_gamma,
    'opt2': calc_opt2,
    'opt4': calc_opt4,
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
                _, df_base, eq_base, _ = calc_baseline(segs, None)
                denom = (1 + df_base) * eq_base
                if abs(denom) < 1e-6:
                    continue
                fwd_pe_chg = ag_old / denom
                score_n, df_n, eq_n, asc_n = calc_fn(segs, fwd_pe_chg)
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
    rets, mdds, trades_per_start = [], [], []
    for sd in start_dates:
        r = bts2.simulate(dates, data, 3, 8, 3, start_date=sd)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
        trades_per_start.append(r['trades'])
    return rets, mdds, start_dates, trades_per_start


def main():
    print('=' * 100)
    print('opt4 (C4만 sign flip) BT — 메모리 v75 인사이트 + 사용자 직관 통합')
    print(f'DB: {DB_ORIGINAL}')
    print('=' * 100)

    rows = []
    trades_by_variant = {}
    for name, fn in VARIANTS.items():
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, fn)
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
    print(f'{"Variant":<12}', end='')
    for sd in sds:
        print(f' {sd:>10}', end='')
    print(f' {"avg":>8} {"std":>5} {"worstMDD":>9} {"risk_adj":>8}')
    print('-' * (14 + 11 * len(sds) + 35))
    for r in rows:
        print(f'  {r["name"]:<10}', end='')
        for ret in r['rets']:
            print(f' {ret:>9.2f}%', end='')
        print(f' {r["avg"]:+7.2f}% {r["std"]:>4.2f} {r["worst_mdd"]:+8.2f}% {r["risk_adj"]:>7.2f}')

    base = next((r for r in rows if r['name'] == 'baseline'), None)
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
            print(f'  {r["name"]:<10}: ΔRet {d_ret:+6.2f}%p, ΔMDD {d_mdd:+5.2f}%p, '
                  f'Δrisk_adj {d_ra:+5.2f}')

    # opt4 vs baseline trade-by-trade 차이
    print()
    print('=' * 100)
    print('opt4 vs baseline trade 차이 (시작일 2/10)')
    print('=' * 100)
    base_trades = trades_by_variant['baseline'][0]
    opt4_trades = trades_by_variant['opt4'][0]
    base_set = {(t['ticker'], t['entry_date']) for t in base_trades}
    opt4_set = {(t['ticker'], t['entry_date']) for t in opt4_trades}
    print(f'  baseline trades: {len(base_trades)}')
    print(f'  opt4 trades: {len(opt4_trades)}')
    only_base = base_set - opt4_set
    only_opt4 = opt4_set - base_set
    print(f'  baseline만: {len(only_base)} 건 — {only_base}')
    print(f'  opt4만: {len(only_opt4)} 건 — {only_opt4}')


if __name__ == '__main__':
    main()
