"""광범위 신호 보너스 변형 — 11개

각 신호별 단독 + 결합:
  W1: baseline
  W2: rev_growth만 (V9h)
  W3: EPS 90일 변화
  W4: EPS 30일 변화 (단기 모멘텀)
  W5: OP margin (영업이익률 절대값)
  W6: GP margin (매출총이익률 절대값)
  W7: ROE
  W8: rev + EPS_90d 결합
  W9: rev + OP 결합
  W10: EPS_90d + OP 결합
  W11: rev + EPS_90d + OP (3중)
"""
import sys
import os
import shutil
sys.path.insert(0, '.')
from bonus_test import make_test_db, multistart, regenerate_with_rev
from bt_engine import load_data
import sqlite3
import numpy as np


# 단일 신호 변형
def conv_baseline(adj_gap, rev_up=None, num_analysts=None,
                   ntm_current=None, ntm_90d=None, rev_growth=None,
                   op_margin=None, gp_margin=None, ntm_30d=None, roe=None):
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    return adj_gap * (1 + max(ratio, eps_floor))


def conv_rev_only(adj_gap, rev_growth=None, **kwargs):
    """W2: V9h 재현"""
    base = conv_baseline(adj_gap, **kwargs) / adj_gap - 1 if adj_gap else 0
    bonus = 0.3 if (rev_growth is not None and rev_growth >= 0.30) else 0
    return adj_gap * (1 + base + bonus)


def conv_eps90d(adj_gap, ntm_current=None, ntm_90d=None, **kwargs):
    """W3: EPS 90일 변화 add 보너스 (>=30%이면 +0.3)"""
    base = conv_baseline(adj_gap, ntm_current=ntm_current, ntm_90d=ntm_90d, **kwargs) / adj_gap - 1 if adj_gap else 0
    eps_chg = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_chg = (ntm_current - ntm_90d) / abs(ntm_90d)
    bonus = 0.3 if eps_chg >= 0.30 else 0
    return adj_gap * (1 + base + bonus)


def conv_eps30d(adj_gap, ntm_current=None, ntm_30d=None, ntm_90d=None, **kwargs):
    """W4: EPS 30일 변화 add 보너스 (>=10%이면 +0.3)"""
    base = conv_baseline(adj_gap, ntm_current=ntm_current, ntm_90d=ntm_90d, **kwargs) / adj_gap - 1 if adj_gap else 0
    eps_chg = 0
    if ntm_current is not None and ntm_30d and abs(ntm_30d) > 0.01:
        eps_chg = (ntm_current - ntm_30d) / abs(ntm_30d)
    bonus = 0.3 if eps_chg >= 0.10 else 0
    return adj_gap * (1 + base + bonus)


def conv_op(adj_gap, op_margin=None, **kwargs):
    """W5: OP margin >= 20%면 +0.3"""
    base = conv_baseline(adj_gap, **kwargs) / adj_gap - 1 if adj_gap else 0
    bonus = 0.3 if (op_margin is not None and op_margin >= 0.20) else 0
    return adj_gap * (1 + base + bonus)


def conv_gp(adj_gap, gp_margin=None, **kwargs):
    """W6: GP margin >= 50%면 +0.3"""
    base = conv_baseline(adj_gap, **kwargs) / adj_gap - 1 if adj_gap else 0
    bonus = 0.3 if (gp_margin is not None and gp_margin >= 0.50) else 0
    return adj_gap * (1 + base + bonus)


def conv_roe(adj_gap, roe=None, **kwargs):
    """W7: ROE >= 20%면 +0.3"""
    base = conv_baseline(adj_gap, **kwargs) / adj_gap - 1 if adj_gap else 0
    bonus = 0.3 if (roe is not None and roe >= 0.20) else 0
    return adj_gap * (1 + base + bonus)


def conv_rev_eps90(adj_gap, rev_growth=None, ntm_current=None, ntm_90d=None, **kwargs):
    """W8: rev>=30% OR EPS 90일>=30% (둘 중 하나라도)"""
    base = conv_baseline(adj_gap, ntm_current=ntm_current, ntm_90d=ntm_90d, **kwargs) / adj_gap - 1 if adj_gap else 0
    eps_chg = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_chg = (ntm_current - ntm_90d) / abs(ntm_90d)
    bonus = 0
    if (rev_growth is not None and rev_growth >= 0.30) or eps_chg >= 0.30:
        bonus = 0.3
    return adj_gap * (1 + base + bonus)


def conv_rev_op(adj_gap, rev_growth=None, op_margin=None, **kwargs):
    """W9: rev>=30% AND OP>=15% (둘 다)"""
    base = conv_baseline(adj_gap, **kwargs) / adj_gap - 1 if adj_gap else 0
    bonus = 0
    if rev_growth is not None and rev_growth >= 0.30 and op_margin is not None and op_margin >= 0.15:
        bonus = 0.3
    return adj_gap * (1 + base + bonus)


def conv_eps90_op(adj_gap, ntm_current=None, ntm_90d=None, op_margin=None, **kwargs):
    """W10: EPS 90일>=30% AND OP>=15%"""
    base = conv_baseline(adj_gap, ntm_current=ntm_current, ntm_90d=ntm_90d, **kwargs) / adj_gap - 1 if adj_gap else 0
    eps_chg = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_chg = (ntm_current - ntm_90d) / abs(ntm_90d)
    bonus = 0
    if eps_chg >= 0.30 and op_margin is not None and op_margin >= 0.15:
        bonus = 0.3
    return adj_gap * (1 + base + bonus)


def conv_three(adj_gap, rev_growth=None, ntm_current=None, ntm_90d=None, op_margin=None, **kwargs):
    """W11: rev + EPS_90d + OP 셋 다 (각각 +0.1)"""
    base = conv_baseline(adj_gap, ntm_current=ntm_current, ntm_90d=ntm_90d, **kwargs) / adj_gap - 1 if adj_gap else 0
    eps_chg = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_chg = (ntm_current - ntm_90d) / abs(ntm_90d)
    bonus = 0
    if rev_growth is not None and rev_growth >= 0.30:
        bonus += 0.1
    if eps_chg >= 0.30:
        bonus += 0.1
    if op_margin is not None and op_margin >= 0.15:
        bonus += 0.1
    return adj_gap * (1 + base + bonus)


def regenerate_full(test_db_path, conv_fn):
    """모든 컬럼 fetch + conv_fn 호출"""
    import daily_runner as dr
    original_path = dr.DB_PATH
    dr.DB_PATH = test_db_path

    try:
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()
        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today_str in dates:
            rows = cursor.execute('''
                SELECT ticker, adj_gap, rev_up30, num_analysts,
                       ntm_current, ntm_30d, ntm_90d, rev_growth,
                       operating_margin, gross_margin, roe
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today_str,)).fetchall()

            if not rows:
                continue

            eligible = []
            for r in rows:
                tk, ag, ru, na, nc, n30, n90, rg, om, gm, ro = r
                cg = conv_fn(
                    ag, rev_up=ru, num_analysts=na,
                    ntm_current=nc, ntm_90d=n90, ntm_30d=n30,
                    rev_growth=rg, op_margin=om, gp_margin=gm, roe=ro
                )
                if cg is not None:
                    eligible.append((tk, cg))

            if not eligible:
                continue

            eligible.sort(key=lambda x: x[1])
            new_comp = {tk: i + 1 for i, (tk, _) in enumerate(eligible)}

            cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today_str,))
            for tk, cr in new_comp.items():
                cursor.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                                (cr, today_str, tk))

            # part2_rank: simple z-score 가중
            cursor.execute(
                'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL AND date <= ? ORDER BY date DESC LIMIT 3',
                (today_str,)
            )
            recent = sorted([r[0] for r in cursor.fetchall()])
            weights = [0.2, 0.3, 0.5]
            if len(recent) == 2:
                weights = [0.4, 0.6]
            elif len(recent) == 1:
                weights = [1.0]

            score_by_date = {}
            for d in recent:
                rs = cursor.execute('''
                    SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_30d, ntm_90d, rev_growth, operating_margin, gross_margin, roe
                    FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
                ''', (d,)).fetchall()
                cgs = {}
                for rr in rs:
                    cg = conv_fn(
                        rr[1], rev_up=rr[2], num_analysts=rr[3],
                        ntm_current=rr[4], ntm_30d=rr[5], ntm_90d=rr[6],
                        rev_growth=rr[7], op_margin=rr[8], gp_margin=rr[9], roe=rr[10]
                    )
                    if cg is not None:
                        cgs[rr[0]] = cg
                vals = list(cgs.values())
                if len(vals) >= 2:
                    m = np.mean(vals)
                    s = np.std(vals)
                    if s > 0:
                        score_by_date[d] = {
                            tk: min(100.0, max(30.0, 65 + (-(v - m) / s) * 15))
                            for tk, v in cgs.items()
                        }
                    else:
                        score_by_date[d] = {tk: 65 for tk in cgs}

            def carry(tk, idx):
                for j in range(idx - 1, -1, -1):
                    prev = score_by_date.get(recent[j], {}).get(tk)
                    if prev is not None:
                        return prev
                return 30

            wgap = {}
            for tk in new_comp:
                ws = 0
                for i, d in enumerate(recent):
                    sc = score_by_date.get(d, {}).get(tk)
                    if sc is None:
                        sc = carry(tk, i)
                    ws += sc * weights[i]
                wgap[tk] = ws

            sorted_tks = sorted(new_comp.keys(), key=lambda tk: -wgap.get(tk, 0))
            top30 = sorted_tks[:30]
            cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today_str,))
            for rank, tk in enumerate(top30, 1):
                cursor.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                                (rank, today_str, tk))
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original_path


def make_test_db_full(suffix, conv_fn):
    test_db = f'eps_test_{suffix}.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    shutil.copy('eps_momentum_data.db.v74_backup_20260411_194906', test_db)
    regenerate_full(test_db, conv_fn)
    return test_db


def main():
    print("=" * 110)
    print("광범위 신호 보너스 변형 (W1~W11)")
    print("=" * 110)

    variants = {
        'W1_baseline': conv_baseline,
        'W2_rev30 (V9h)': conv_rev_only,
        'W3_eps_90d>=30%': conv_eps90d,
        'W4_eps_30d>=10%': conv_eps30d,
        'W5_op_margin>=20%': conv_op,
        'W6_gp_margin>=50%': conv_gp,
        'W7_roe>=20%': conv_roe,
        'W8_rev OR eps90 (>=30%)': conv_rev_eps90,
        'W9_rev AND op (30%&15%)': conv_rev_op,
        'W10_eps90 AND op (30%&15%)': conv_eps90_op,
        'W11_three (rev+eps90+op,각0.1)': conv_three,
    }

    print(f"\n{'변형':<32s} {'avg':>7s} {'med':>7s} {'min':>7s} {'max':>7s} "
          f"{'std':>5s} {'MDD avg':>8s} {'MDD worst':>10s} {'risk_adj':>9s}")
    print("-" * 105)

    results = {}
    for name, conv_fn in variants.items():
        suffix = 'wide_' + name.split('_')[0].lower()
        db = make_test_db_full(suffix, conv_fn)
        ds, data = load_data(db)
        m = multistart(ds, data)
        results[name] = m
        print(f"{name:<32s} {m['avg']:+6.1f}% {m['med']:+6.1f}% {m['min']:+6.1f}% "
              f"{m['max']:+6.1f}% {m['std']:>4.1f} {m['mdd_avg']:+7.1f}% "
              f"{m['mdd_worst']:+9.1f}% {m['risk_adj']:>8.2f}")

    # 차분 vs baseline
    print("\n[차분 vs W1 baseline]")
    base = results['W1_baseline']
    sorted_r = sorted(results.items(), key=lambda x: -x[1]['avg'])
    for name, m in sorted_r:
        if name == 'W1_baseline':
            continue
        ret_diff = m['avg'] - base['avg']
        risk_diff = m['risk_adj'] - base['risk_adj']
        marker = ' ⭐⭐' if ret_diff > 1.5 else (' ⭐' if ret_diff > 0.5 else (' ⚠️' if ret_diff < -0.5 else ''))
        print(f"  {name:<32s} ret {ret_diff:+5.2f}%p, risk_adj {risk_diff:+5.2f}{marker}")


if __name__ == '__main__':
    main()
