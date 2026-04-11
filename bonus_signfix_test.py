"""V75 부호 결함 + N<3 필터 종합 검증

5가지 conviction 변형 비교 (composite_rank 부여된 종목 한정):
- V74:    baseline (보너스 없음)
- V75:    현재 production (multiplier 보너스, 부호 결함 있음)
- V75-A:  Signed Magnitude (음수 영역 V75 동치, 양수 영역 페널티 완화)
- V75-B:  Sign-Split Discount (음수 V75 동치, 양수 30% 할인)
- V75-D:  Absolute Discount with floor

수학적 검증 (SNDK 4/10: adj_gap=+5.04, base=1.0):
- V75:   5.04 × 2.3 = +11.58  (보너스가 페널티로)
- V75-A: 5.04 × 2.0 - 5.04 × 0.3 = +8.57
- V75-B: 5.04 × 2.0 × 0.7 = +7.06
- V75-D: 5.04 × 2.0 - max(5.04, 1.0) × 0.3 = +8.57

음수 케이스 (MU adj_gap=-36.28, base=1.0):
- V75/V75-A/V75-B/V75-D 모두 -83.44 (V75와 동일)

→ 음수 종목은 V75와 100% 동일, 양수 종목만 차이.
"""
import sys
import os
import shutil
import sqlite3
import json

sys.path.insert(0, '.')
import daily_runner as dr
import pandas as pd
import numpy as np

V74_BACKUP = sorted([f for f in os.listdir('.') if f.startswith('eps_momentum_data.db.v74_backup_')])[-1]
print(f"V74 backup: {V74_BACKUP}")


# ===== Conviction 변형 5가지 =====

def conv_v74(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None, rev_growth=None):
    """V74: 보너스 없음 (baseline)"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    return adj_gap * (1 + max(ratio, eps_floor))


def conv_v75(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None, rev_growth=None):
    """V75 현재: multiplier 보너스 (부호 결함)"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    bonus = 0.3 if (rev_growth is not None and rev_growth >= 0.30) else 0
    return adj_gap * (1 + base + bonus)


def conv_v75A(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None, rev_growth=None):
    """V75-A: Signed Magnitude — 항상 음수 방향으로 |adj_gap|×0.3 차감"""
    base_conv = conv_v74(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d, rev_growth)
    if base_conv is None:
        return None
    if rev_growth is not None and rev_growth >= 0.30:
        return base_conv - abs(adj_gap) * 0.3
    return base_conv


def conv_v75B(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None, rev_growth=None):
    """V75-B: Sign-Split Discount — 음수면 V75 동치, 양수면 30% 할인"""
    base_conv = conv_v74(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d, rev_growth)
    if base_conv is None:
        return None
    if rev_growth is not None and rev_growth >= 0.30:
        if adj_gap < 0:
            # 음수 영역: V75와 동치 (base_conv + adj_gap × 0.3 = adj_gap × (1+base+0.3))
            return base_conv + adj_gap * 0.3
        else:
            # 양수 영역: 30% 할인
            return base_conv * 0.7
    return base_conv


def conv_v75D(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None, rev_growth=None):
    """V75-D: Absolute Discount with floor — V75-A에 최소값 1.0 보장"""
    base_conv = conv_v74(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d, rev_growth)
    if base_conv is None:
        return None
    if rev_growth is not None and rev_growth >= 0.30:
        return base_conv - max(abs(adj_gap), 1.0) * 0.3
    return base_conv


# ===== 재계산 함수 (fair_v74_vs_v75.py 패턴) =====

def regenerate_with_conv(test_db_path, conv_fn):
    """모든 날짜에 conv_fn 적용해서 composite_rank/part2_rank 재저장"""
    with open('ticker_info_cache.json', encoding='utf-8') as f:
        ticker_cache = json.load(f)

    original_path = dr.DB_PATH
    original_conv = dr._apply_conviction
    dr.DB_PATH = test_db_path
    dr._apply_conviction = conv_fn

    try:
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()
        dates = [r[0] for r in cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
        ).fetchall()]

        for today_str in dates:
            rows = cursor.execute('''
                SELECT ticker, score, adj_score, adj_gap,
                       ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                       price, ma60, ma120,
                       rev_up30, rev_down30, num_analysts, rev_growth,
                       operating_margin, gross_margin, is_turnaround
                FROM ntm_screening WHERE date=?
            ''', (today_str,)).fetchall()

            records = []
            for r in rows:
                (tk, score, adj_score, adj_gap, nc, n7, n30, n60, n90,
                 price, ma60, ma120, ru, rd, na, rg, om, gm, is_to) = r

                fwd_pe = (price / nc) if (price and nc and nc > 0) else None
                eps_change_90d = ((nc - n90) / abs(n90) * 100) if (n90 and abs(n90) > 0.01) else None

                nc_, n7_, n30_, n60_, n90_ = (float(x) if x else 0 for x in [nc, n7, n30, n60, n90])
                segs = []
                for a, b in [(nc_, n7_), (n7_, n30_), (n30_, n60_), (n60_, n90_)]:
                    if b and abs(b) > 0.01:
                        segs.append((a - b) / abs(b) * 100)
                    else:
                        segs.append(0)
                segs = [max(-100.0, min(100.0, s)) for s in segs]
                seg1, seg2, seg3, seg4 = segs
                direction = (seg1 - seg4)
                industry = ticker_cache.get(tk, {}).get('industry', '기타')

                records.append({
                    'ticker': tk, 'score': score, 'adj_score': adj_score,
                    'adj_gap': adj_gap, 'direction': direction,
                    'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
                    'ntm_current': nc, 'ntm_7d': n7, 'ntm_30d': n30,
                    'ntm_60d': n60, 'ntm_90d': n90,
                    'eps_change_90d': eps_change_90d, 'fwd_pe': fwd_pe,
                    'price': price, 'ma60': ma60, 'ma120': ma120,
                    'rev_up30': ru, 'rev_down30': rd,
                    'num_analysts': na, 'rev_growth': rg,
                    'operating_margin': om, 'gross_margin': gm,
                    'industry': industry, 'is_turnaround': is_to,
                })

            df = pd.DataFrame(records)
            if df.empty:
                continue

            try:
                dr.save_part2_ranks(df, today_str)
            except Exception as e:
                print(f"  ERROR {today_str}: {e}")

        conn.close()
    finally:
        dr.DB_PATH = original_path
        dr._apply_conviction = original_conv


def make_test_db(suffix, conv_fn):
    db = f'eps_test_signfix_{suffix}.db'
    if os.path.exists(db):
        os.remove(db)
    shutil.copy(V74_BACKUP, db)
    print(f"  [{suffix}] 재계산 중...")
    regenerate_with_conv(db, conv_fn)
    return db


def multistart(db_path):
    from bt_engine import load_data, simulate
    HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
                   'check_ma60': True, 'max_grace': 2}
    dates, data = load_data(db_path)
    starts = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    rets, mdds = [], []
    for sd in starts:
        r = simulate(dates, data, 3, 11, 3, start_date=sd, hold_params=HOLD_STRICT)
        rets.append(r['total_return'])
        mdds.append(r['max_dd'])
    n = len(rets)
    avg = sum(rets) / n
    sorted_r = sorted(rets)
    std = (sum((r - avg) ** 2 for r in rets) / n) ** 0.5
    return {
        'n': n, 'avg': round(avg, 2),
        'med': round(sorted_r[n // 2], 2),
        'min': round(min(rets), 2), 'max': round(max(rets), 2),
        'std': round(std, 2),
        'mdd_avg': round(sum(mdds) / n, 2),
        'mdd_worst': round(min(mdds), 2),
        'risk_adj': round(avg / abs(min(mdds)), 2) if min(mdds) != 0 else 0,
    }


def main():
    print("=" * 100)
    print("V75 부호 결함 검증 — 5개 conviction 변형 multistart 비교")
    print("=" * 100)

    variants = [
        ('V74_baseline',         conv_v74),
        ('V75_current',          conv_v75),
        ('V75A_signed_mag',      conv_v75A),
        ('V75B_split_discount',  conv_v75B),
        ('V75D_abs_floor',       conv_v75D),
    ]

    print(f"\n[1] 5개 변형 일관 재계산 + multistart")
    results = {}
    for name, fn in variants:
        db = make_test_db(name.lower(), fn)
        m = multistart(db)
        results[name] = m
        print(f"  [{name}] avg {m['avg']:+.2f}% | min {m['min']:+.2f}% | "
              f"std {m['std']} | MDD worst {m['mdd_worst']:+.2f}% | risk_adj {m['risk_adj']}")

    # 결과 표
    print()
    print("=" * 100)
    print(f"{'변형':<24s} {'avg':>8s} {'med':>8s} {'min':>8s} {'max':>8s} "
          f"{'std':>6s} {'MDD avg':>9s} {'MDD worst':>10s} {'risk_adj':>9s}")
    print("-" * 100)
    for name, _ in variants:
        m = results[name]
        print(f"{name:<24s} {m['avg']:+7.2f}% {m['med']:+7.2f}% {m['min']:+7.2f}% "
              f"{m['max']:+7.2f}% {m['std']:>5.2f} {m['mdd_avg']:+8.2f}% "
              f"{m['mdd_worst']:+9.2f}% {m['risk_adj']:>8.2f}")

    # vs V75 차분
    print()
    print("[차분 vs V75_current (production)]")
    base = results['V75_current']
    for name, _ in variants:
        if name == 'V75_current':
            continue
        m = results[name]
        ret_diff = m['avg'] - base['avg']
        risk_diff = m['risk_adj'] - base['risk_adj']
        mdd_diff = m['mdd_worst'] - base['mdd_worst']
        marker = ''
        if ret_diff > 1.0: marker = ' ⭐⭐'
        elif ret_diff > 0.3: marker = ' ⭐'
        elif ret_diff < -0.3: marker = ' ⚠️'
        print(f"  {name:<24s} ret {ret_diff:+5.2f}%p, MDD {mdd_diff:+5.2f}%p, "
              f"risk_adj {risk_diff:+5.2f}{marker}")

    # vs V74 차분 (baseline 대비 보너스 효과)
    print()
    print("[차분 vs V74_baseline (보너스 없음)]")
    base = results['V74_baseline']
    for name, _ in variants:
        if name == 'V74_baseline':
            continue
        m = results[name]
        ret_diff = m['avg'] - base['avg']
        risk_diff = m['risk_adj'] - base['risk_adj']
        mdd_diff = m['mdd_worst'] - base['mdd_worst']
        print(f"  {name:<24s} ret {ret_diff:+5.2f}%p, MDD {mdd_diff:+5.2f}%p, "
              f"risk_adj {risk_diff:+5.2f}")


if __name__ == '__main__':
    main()
