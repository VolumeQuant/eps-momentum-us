"""SNDK 없는 시장 + 변형 BT 종합.

사용자 의도:
  1. SNDK 의존성 제거 후 진짜 알파 측정
  2. β1: cap 발동 시 무조건 +0.3 (어닝 비트 호재 보너스)
  3. opt4: C4 sign flip (사용자 직관 fix)

비교 변형:
  baseline    — control
  gamma       — cap 시 dir=0 (현 v80.3)
  beta1       — cap 시 dir=+0.3 (사용자 의도, 어닝 비트 보너스)
  opt4        — C4 (고평가+둔화) sign flip
  beta1_opt4  — β1 + opt4 (사용자 두 의도 결합)

multistart 처음 5일 + SNDK 제거 후.
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
GRID = ROOT / 'research' / 'no_sndk_full_dbs'
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
    """γ: cap 시 dir=0"""
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


def calc_beta1(segs, fwd_pe_chg):
    """β1: cap 시 dir=+0.3 무조건 보너스"""
    score = sum(segs)
    if any(abs(s) >= SEG_CAP for s in segs):
        df = +0.3
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df = max(-0.3, min(0.3, direction / 30))
        min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


def calc_opt4(segs, fwd_pe_chg):
    """opt4: C4 sign flip"""
    score = sum(segs)
    direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
    df_raw = max(-0.3, min(0.3, direction / 30))
    if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
        df = -df_raw
    else:
        df = df_raw
    eq = 1.0 + 0.3 * max(-1, min(1, min(segs) / 2))
    return score, df, eq, score * (1 + df)


def calc_beta1_opt4(segs, fwd_pe_chg):
    """β1 + opt4: cap 보너스 + C4 sign flip"""
    score = sum(segs)
    cap_hit = any(abs(s) >= SEG_CAP for s in segs)
    if cap_hit:
        df = +0.3
        valid = [s for s in segs if abs(s) < SEG_CAP]
        min_seg = min(valid) if valid else 0
    else:
        direction = (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2
        df_raw = max(-0.3, min(0.3, direction / 30))
        if fwd_pe_chg is not None and fwd_pe_chg > 0 and direction < 0:
            df = -df_raw
        else:
            df = df_raw
        min_seg = min(segs)
    eq = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
    return score, df, eq, score * (1 + df)


VARIANTS = {
    'baseline':   calc_baseline,
    'gamma':      calc_gamma,
    'beta1':      calc_beta1,
    'opt4':       calc_opt4,
    'b1_opt4':    calc_beta1_opt4,
}


def regenerate(test_db, calc_fn, exclude_ticker='SNDK'):
    """변형 적용 + SNDK 제거"""
    original = dr.DB_PATH
    dr.DB_PATH = test_db
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            # SNDK 제외하고 종목 가져옴
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                       adj_gap, rev_up30, num_analysts, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL AND ticker != ?
            ''', (today, exclude_ticker)).fetchall()
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

            # SNDK row composite_rank/part2_rank NULL
            cur.execute(
                'UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL WHERE date=? AND ticker=?',
                (today, exclude_ticker)
            )

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
    print('SNDK 없는 시장 + 변형 BT (multistart 5시작일, 룰 3/8/3)')
    print(f'DB: {DB_ORIGINAL}')
    print('=' * 100)

    rows = []
    for name, fn in VARIANTS.items():
        db = GRID / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, fn, exclude_ticker='SNDK')
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
        print('비교 (baseline 대비, SNDK 없는 시장)')
        print('=' * 100)
        for r in rows:
            d_ret = r['avg'] - base['avg']
            d_mdd = r['worst_mdd'] - base['worst_mdd']
            d_ra = r['risk_adj'] - base['risk_adj']
            marker = ''
            if r['name'] == 'baseline':
                marker = ' (control)'
            elif d_ret > 0:
                marker = ' ← 우월'
            elif d_ret < -0.5:
                marker = ' ← 손실'
            print(f'  {r["name"]:<10}: avg {r["avg"]:+6.2f}%, MDD {r["worst_mdd"]:+6.2f}%, '
                  f'risk_adj {r["risk_adj"]:.2f} | ΔRet {d_ret:+5.2f}%p, ΔMDD {d_mdd:+5.2f}%p{marker}')


if __name__ == '__main__':
    main()
