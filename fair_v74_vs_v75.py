"""Fair 비교: v74 vs v75 (둘 다 모든 일자 일관 재계산)

이전 비교의 문제:
- v74 backup: 시점별 v71/v72 mix (production 상태)
- v75 migration: 전체 v75 일관
- → 일관성 차이로 fair 아님

Fair 비교:
- v74 일관 재계산 (rev_growth 보너스 없음)
- v75 일관 재계산 (rev_growth 보너스 있음)
- 둘 다 같은 sim 환경
"""
import sys
import os
import shutil
sys.path.insert(0, '.')
import daily_runner as dr

V74_BACKUP = sorted([f for f in os.listdir('.') if f.startswith('eps_momentum_data.db.v74_backup_')])[-1]
print(f"V74 backup 파일: {V74_BACKUP}")


def conv_v74(adj_gap, rev_up, num_analysts, ntm_current=None, ntm_90d=None, rev_growth=None):
    """v74 conviction (rev_growth 무시)"""
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
    """v75 conviction (rev_growth 보너스)"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    base = max(ratio, eps_floor)
    rev_bonus = 0.3 if (rev_growth is not None and rev_growth >= 0.30) else 0
    return adj_gap * (1 + base + rev_bonus)


def regenerate_with_conv(test_db_path, conv_fn):
    """모든 날짜에 대해 conv_fn 적용해서 part2_rank 재계산"""
    import sqlite3
    import json
    import pandas as pd

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


def main():
    print("=" * 80)
    print("Fair v74 vs v75 비교 (둘 다 일관 재계산)")
    print("=" * 80)

    # 1. v74 일관 DB 생성 (backup에서 시작 → v74 conv로 재계산)
    db_v74 = 'eps_test_v74_consistent.db'
    if os.path.exists(db_v74):
        os.remove(db_v74)
    shutil.copy(V74_BACKUP, db_v74)
    print(f"\n[1] v74 일관 재계산 시작...")
    regenerate_with_conv(db_v74, conv_v74)
    print("    완료")

    # 2. v75 일관 DB 생성 (backup에서 시작 → v75 conv로 재계산)
    db_v75 = 'eps_test_v75_consistent.db'
    if os.path.exists(db_v75):
        os.remove(db_v75)
    shutil.copy(V74_BACKUP, db_v75)
    print(f"\n[2] v75 일관 재계산 시작...")
    regenerate_with_conv(db_v75, conv_v75)
    print("    완료")

    # 3. multistart 백테스트 비교
    print("\n[3] Multistart 백테스트 비교")
    from bt_engine import load_data, simulate

    HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
                   'check_ma60': True, 'max_grace': 2}

    def multistart(db_path):
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
            'med': round(sorted_r[n//2], 2),
            'min': round(min(rets), 2), 'max': round(max(rets), 2),
            'std': round(std, 2),
            'mdd_avg': round(sum(mdds)/n, 2),
            'mdd_worst': round(min(mdds), 2),
            'risk_adj': round(avg / abs(min(mdds)), 2) if min(mdds) != 0 else 0,
        }

    print()
    print("[v74 일관 재계산]")
    m74 = multistart(db_v74)
    print(f"  평균: {m74['avg']:+.2f}% (med {m74['med']:+.2f}, min {m74['min']:+.2f}, max {m74['max']:+.2f})")
    print(f"  std {m74['std']}, MDD avg {m74['mdd_avg']:+.2f}% / worst {m74['mdd_worst']:+.2f}%")
    print(f"  위험조정: {m74['risk_adj']}")

    print()
    print("[v75 일관 재계산]")
    m75 = multistart(db_v75)
    print(f"  평균: {m75['avg']:+.2f}% (med {m75['med']:+.2f}, min {m75['min']:+.2f}, max {m75['max']:+.2f})")
    print(f"  std {m75['std']}, MDD avg {m75['mdd_avg']:+.2f}% / worst {m75['mdd_worst']:+.2f}%")
    print(f"  위험조정: {m75['risk_adj']}")

    print()
    print("[차이 (v75 - v74) Fair 비교]")
    print(f"  평균: {m75['avg'] - m74['avg']:+.2f}%p")
    print(f"  중앙: {m75['med'] - m74['med']:+.2f}%p")
    print(f"  최저: {m75['min'] - m74['min']:+.2f}%p")
    print(f"  MDD worst: {m75['mdd_worst'] - m74['mdd_worst']:+.2f}%p")
    print(f"  위험조정: {m75['risk_adj'] - m74['risk_adj']:+.2f}")


if __name__ == '__main__':
    main()
