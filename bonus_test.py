"""EPS/매출 전망 보너스 변형 백테스트

현재: conviction = max(ratio, eps_floor) — rev_growth 미사용
변형:
  V1 (baseline): 현재 v74
  V2: + rev_growth 보너스 (>=30%부터 점진)
  V3: + 강한 rev_growth 보너스 (>=50%부터)
  V4: + EPS+rev 시너지 (둘 다 강하면 추가)
  V5: max에 rev_floor 추가
"""
import sys
import os
import shutil
sys.path.insert(0, '.')
import daily_runner as dr
from backtest_v3 import regenerate_part2_with_conviction
from bt_engine import load_data, simulate

DB_ORIGINAL = 'eps_momentum_data.db'
HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
               'check_ma60': True, 'max_grace': 2}


def conv_baseline(adj_gap, rev_up=None, num_analysts=None,
                   ntm_current=None, ntm_90d=None, rev_growth=None):
    """V1: 현재 v74 (rev_growth 미사용)"""
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def make_conv_with_rev(rev_threshold, max_bonus, scale):
    """rev_growth 보너스 함수 팩토리

    rev_growth가 threshold 이상일 때 점진 보너스
    bonus = min(scale * (rev_growth - threshold), max_bonus)
    """
    def conv_fn(adj_gap, rev_up=None, num_analysts=None,
                ntm_current=None, ntm_90d=None,
                rev_growth=None):
        if adj_gap is None:
            return None
        ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
        eps_floor = 0
        if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
            eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
        rev_floor = 0
        if rev_growth is not None and rev_growth > rev_threshold:
            rev_floor = min(scale * (rev_growth - rev_threshold), max_bonus)
        conviction = max(ratio, eps_floor, rev_floor)
        return adj_gap * (1 + conviction)
    return conv_fn


def make_conv_synergy(rev_threshold, ratio_threshold, bonus):
    """시너지 보너스: ratio와 rev_growth 둘 다 강할 때만"""
    def conv_fn(adj_gap, rev_up=None, num_analysts=None,
                ntm_current=None, ntm_90d=None,
                rev_growth=None):
        if adj_gap is None:
            return None
        ratio = (rev_up / num_analysts) if (num_analysts and rev_up is not None) else 0
        eps_floor = 0
        if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
            eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
        base_conv = max(ratio, eps_floor)
        # 시너지 조건: ratio >= threshold AND rev_growth >= threshold
        synergy = 0
        if rev_growth is not None and ratio >= ratio_threshold and rev_growth >= rev_threshold:
            synergy = bonus
        conviction = base_conv + synergy
        return adj_gap * (1 + conviction)
    return conv_fn


# regenerate_part2_with_conviction은 rev_growth 안 받음 → 우리가 직접 처리
# 임시 monkey-patch 방식으로 _apply_conviction 교체
def patch_with_rev_aware(conv_fn):
    """rev_growth aware conviction 함수를 monkey-patch
    daily_runner._apply_conviction는 rev_growth 안 받지만, 우리가 cursor에서 추가 fetch
    """
    import sqlite3 as _sqlite3

    def patched(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None):
        # 이 호출 자체는 rev_growth 모름 → 0으로 처리 (현재 동작과 동일)
        # 진짜 rev_growth 활용은 별도 함수에서 (composite_rank 재계산 시)
        return conv_fn(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d, rev_growth=None)

    return patched


# 더 깔끔한 방법: 직접 DB에서 part2_rank 재생성 (rev_growth 활용)
def regenerate_with_rev(test_db_path, conv_fn_with_rev):
    """rev_growth 사용하는 conviction으로 재생성"""
    original_path = dr.DB_PATH
    dr.DB_PATH = test_db_path
    try:
        import sqlite3 as sql
        conn = sql.connect(test_db_path)
        cursor = conn.cursor()
        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today_str in dates:
            rows = cursor.execute('''
                SELECT ticker, adj_gap, rev_up30, num_analysts,
                       ntm_current, ntm_90d, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today_str,)).fetchall()

            if not rows:
                continue

            eligible = []
            for r in rows:
                tk, ag, ru, na, nc, n90, rg = r
                cg = conv_fn_with_rev(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    eligible.append((tk, cg))

            if not eligible:
                continue

            eligible.sort(key=lambda x: x[1])
            new_comp = {tk: i + 1 for i, (tk, _) in enumerate(eligible)}

            cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today_str,))
            for tk, cr in new_comp.items():
                cursor.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today_str, tk)
                )

            # _compute_w_gap_map은 monkey-patch된 _apply_conviction을 사용
            # 우리 conv_fn_with_rev는 5개 인자라 직접 사용 못 함
            # → 단순 z-score 가중 직접 계산
            import numpy as np

            # 최근 3일
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
                    SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth
                    FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
                ''', (d,)).fetchall()
                cgs = {}
                for rr in rs:
                    cg = conv_fn_with_rev(rr[1], rr[2], rr[3], rr[4], rr[5], rev_growth=rr[6])
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
                cursor.execute(
                    'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                    (rank, today_str, tk)
                )
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original_path


def make_test_db(suffix, conv_fn):
    test_db = f'eps_test_{suffix}.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    shutil.copy(DB_ORIGINAL, test_db)
    regenerate_with_rev(test_db, conv_fn)
    return test_db


def multistart(dates, data, e=3, x=11, s=3, hold_params=HOLD_STRICT):
    starts = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    rets, mdds = [], []
    for sd in starts:
        r = simulate(dates, data, e, x, s, start_date=sd, hold_params=hold_params)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    n = len(rets)
    avg = sum(rets) / n
    sorted_r = sorted(rets)
    std = (sum((r - avg) ** 2 for r in rets) / n) ** 0.5
    return {
        'avg': round(avg, 2),
        'med': round(sorted_r[n//2], 2),
        'min': round(min(rets), 2),
        'max': round(max(rets), 2),
        'std': round(std, 2),
        'mdd_avg': round(sum(mdds)/n, 2),
        'mdd_worst': round(min(mdds), 2),
        'risk_adj': round(avg / abs(min(mdds)), 2) if min(mdds) != 0 else 0,
    }


def main():
    print("=" * 100)
    print("EPS/매출 전망 보너스 변형 백테스트 (multistart)")
    print("=" * 100)

    # 변형 5종
    variants = {
        'V1_baseline (현재 v74)': conv_baseline,
        'V2_rev30 (>=30%, +0.3 max)': make_conv_with_rev(rev_threshold=0.30, max_bonus=0.3, scale=1.0),
        'V3_rev50 (>=50%, +0.5 max)': make_conv_with_rev(rev_threshold=0.50, max_bonus=0.5, scale=1.0),
        'V4_synergy (ratio≥0.5 & rev≥30%)': make_conv_synergy(rev_threshold=0.30, ratio_threshold=0.5, bonus=0.3),
        'V5_rev_only_strong (>=50%, +0.3)': make_conv_with_rev(rev_threshold=0.50, max_bonus=0.3, scale=0.6),
    }

    print("\n[1] 5 변형 DB 생성 + multistart 백테스트\n")
    print(f"{'변형':<36s} {'avg':>7s} {'med':>7s} {'min':>7s} {'max':>7s} "
          f"{'std':>5s} {'MDD avg':>8s} {'MDD worst':>10s} {'risk_adj':>9s}")
    print("-" * 105)

    results = {}
    for name, conv_fn in variants.items():
        suffix = name.split('_')[0].lower()
        db = make_test_db(suffix, conv_fn)
        ds, data = load_data(db)
        m = multistart(ds, data)
        results[name] = m
        print(f"{name:<36s} {m['avg']:+6.1f}% {m['med']:+6.1f}% {m['min']:+6.1f}% "
              f"{m['max']:+6.1f}% {m['std']:>4.1f} {m['mdd_avg']:+7.1f}% "
              f"{m['mdd_worst']:+9.1f}% {m['risk_adj']:>8.2f}")

    # 차분 측정
    print("\n[2] V1 baseline 대비 차분")
    base = results['V1_baseline (현재 v74)']
    for name, m in results.items():
        if name == 'V1_baseline (현재 v74)':
            continue
        ret_diff = m['avg'] - base['avg']
        mdd_diff = m['mdd_worst'] - base['mdd_worst']
        risk_diff = m['risk_adj'] - base['risk_adj']
        print(f"  {name:<36s} ret {ret_diff:+5.2f}%p, MDD {mdd_diff:+5.2f}%p, "
              f"risk_adj {risk_diff:+5.2f}")


if __name__ == '__main__':
    main()
