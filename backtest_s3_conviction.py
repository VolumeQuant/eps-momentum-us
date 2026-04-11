"""S3: Conviction 변형 비교 (차분 측정)

핵심: sim 재계산이 100% 정확 안 됨 (78% Top 3 일치)
     → 같은 sim 안에서 base/strong/none 비교 → 차이는 정확
방법: regenerate_part2_with_conviction 사용
검증: G (E3/X10/S2) 위에서 비교
"""
import sqlite3
import shutil
import os
import sys
from collections import defaultdict

DB_ORIGINAL = 'eps_momentum_data.db'

sys.path.insert(0, '.')
import daily_runner as dr
from backtest_v3 import (
    conv_none, conv_base, conv_strong,
    regenerate_part2_with_conviction,
)
from backtest_s2_params import simulate, load_data


# 새로운 conviction 변형들 (S3 인사이트 기반)
def conv_amplify_15(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None):
    """conviction max 1.5 (1~2.5x), 임계값 없음"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = min(rev_up / num_analysts, 1.5)
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.5)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def conv_softer_07(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None):
    """conviction max 0.7 (1~1.7x)"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = min(rev_up / num_analysts, 0.7)
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 0.7)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def conv_strong_or(adj_gap, rev_up=None, num_analysts=None, ntm_current=None, ntm_90d=None):
    """OR 조건 strong: ratio>=0.5 OR eps_floor>=0.5 일 때 tail bonus"""
    if adj_gap is None:
        return None
    ratio = 0
    if num_analysts and num_analysts > 0 and rev_up is not None:
        ratio = rev_up / num_analysts
    eps_floor = 0
    if ntm_current is not None and ntm_90d is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 2.0)
    base = max(ratio, min(eps_floor, 1.0))
    tail_bonus = 0
    if ratio >= 0.5 or eps_floor >= 0.5:
        tail_bonus = min(max(ratio, eps_floor) * 0.3, 0.5)
    conviction = base + tail_bonus
    return adj_gap * (1 + conviction)


def make_test_db(suffix, conviction_fn):
    """특정 conviction으로 part2_rank 재계산된 DB 생성"""
    test_db = f'eps_test_{suffix}.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    shutil.copy(DB_ORIGINAL, test_db)
    print(f"  생성: {test_db}")
    regenerate_part2_with_conviction(test_db, conviction_fn)
    return test_db


def load_data_from(db_path):
    """특정 DB에서 data 로드 (load_data와 유사하지만 DB 경로 파라미터)"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'comp_rank': r[3], 'min_seg': min_seg,
            }
    conn.close()
    return dates, data


def sample_test(test_db_base):
    """표본 테스트: regen_base가 DB 원본과 얼마나 다른지 확인"""
    print("\n표본 테스트: sim_base = DB 원본과의 일치율")

    conn1 = sqlite3.connect(test_db_base)
    conn2 = sqlite3.connect(DB_ORIGINAL)
    c1, c2 = conn1.cursor(), conn2.cursor()

    dates = [r[0] for r in c1.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    matches = {'top3': 0, 'top5': 0, 'top12': 0}
    for d in dates:
        t1 = [r[0] for r in c1.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
            (d,)).fetchall()]
        t2 = [r[0] for r in c2.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
            (d,)).fetchall()]
        if t1[:3] == t2[:3]:
            matches['top3'] += 1
        if t1[:5] == t2[:5]:
            matches['top5'] += 1
        if t1[:12] == t2[:12]:
            matches['top12'] += 1

    conn1.close()
    conn2.close()
    n = len(dates)
    print(f"  Top 3: {matches['top3']}/{n} ({matches['top3']/n*100:.0f}%)")
    print(f"  Top 5: {matches['top5']}/{n} ({matches['top5']/n*100:.0f}%)")
    print(f"  Top 12: {matches['top12']}/{n} ({matches['top12']/n*100:.0f}%)")
    return n, matches


def multistart_compare(dates, data_dict, entry, exit_th, slots, label):
    """multistart 비교: 3 conviction 변형"""
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]
    samples = start_dates[::2]

    results_by_conv = {'none': [], 'base': [], 'strong': []}
    for sd in samples:
        for conv_name, data in data_dict.items():
            r = simulate(dates, data, entry, exit_th, slots, start_date=sd)
            results_by_conv[conv_name].append(r['total_return'])

    print(f"\n=== {label} : {entry}/X{exit_th}/S{slots} (multistart {len(samples)}개 시작일) ===")
    print(f"{'Conviction':<12s} {'평균':>7s} {'중앙값':>7s} {'표준편차':>8s} {'최저':>7s} {'최고':>7s}")
    print("-" * 60)
    for conv in ['none', 'base', 'strong']:
        rets = sorted(results_by_conv[conv])
        avg = sum(rets) / len(rets)
        median = rets[len(rets) // 2]
        std = (sum((r - avg) ** 2 for r in rets) / len(rets)) ** 0.5
        print(f"{conv:<12s} {avg:+6.2f}% {median:+6.2f}% {std:>7.2f} "
              f"{min(rets):+6.1f}% {max(rets):+6.1f}%")

    # 차분 측정
    base_avg = sum(results_by_conv['base']) / len(results_by_conv['base'])
    none_avg = sum(results_by_conv['none']) / len(results_by_conv['none'])
    strong_avg = sum(results_by_conv['strong']) / len(results_by_conv['strong'])

    print(f"\n  차분 (base 대비):")
    print(f"    none   - base = {none_avg - base_avg:+5.2f}%p")
    print(f"    strong - base = {strong_avg - base_avg:+5.2f}%p")

    return results_by_conv


def main():
    print("=" * 70)
    print("S3: Conviction 변형 비교 (차분 측정)")
    print("=" * 70)

    # Step 1: 3가지 conviction으로 part2_rank 재계산
    print("\n[1] 3가지 DB 생성:")
    db_none = make_test_db('none', conv_none)
    db_base = make_test_db('base', conv_base)
    db_strong = make_test_db('strong', conv_strong)

    # Step 2: 표본 검증 - sim_base와 DB 원본 차이
    print("\n[2] 표본 검증")
    sample_test(db_base)
    print("  ↑ sim_base가 DB 원본과 차이 있음 — 차분 측정으로 우회")

    # Step 3: 데이터 로드
    print("\n[3] 데이터 로드")
    dates_n, data_n = load_data_from(db_none)
    dates_b, data_b = load_data_from(db_base)
    dates_s, data_s = load_data_from(db_strong)

    data_dict = {'none': data_n, 'base': data_b, 'strong': data_s}
    dates = dates_b  # 모두 같음

    # Step 4: 여러 파라미터 조합에서 비교 (S2.5/S2.6 인사이트 반영)
    print("\n[4] Multistart 비교 (3 후보 baseline)")

    # 현재
    multistart_compare(dates, data_dict, 5, 12, 3, "A: 현재 E5/X12/S3")

    # 3 후보
    multistart_compare(dates, data_dict, 3, 9, 2, "E3/X9/S2 (Sharpe 최고)")
    multistart_compare(dates, data_dict, 3, 10, 2, "G: E3/X10/S2 (균형)")
    multistart_compare(dates, data_dict, 3, 10, 1, "E3/X10/S1 (공격적)")


if __name__ == '__main__':
    main()
