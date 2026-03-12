# -*- coding: utf-8 -*-
"""v52 순위 재계산 — adj_gap 오름차순으로 composite_rank 전환

기존: composite score = (-z_gap)*0.7 + z_rev*0.3 (z-score 기반)
신규: adj_gap 오름차순 (가장 음수 = 가장 저평가 = rank 1)

1단계: 모든 날짜의 composite_rank → adj_gap 오름차순으로 재계산
2단계: part2_rank 재계산 (가중순위: T0×0.5 + T1×0.3 + T2×0.2, PENALTY=50)
3단계: 최근 3일 before/after 비교 출력

사용법:
  python migrate_v52_ranks.py          # dry run (변경 사항만 표시)
  python migrate_v52_ranks.py --apply  # 실제 적용
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / 'eps_momentum_data.db'
PENALTY = 50


def get_dates_with_composite(conn):
    """composite_rank가 있는 모든 날짜를 오름차순으로 반환"""
    cursor = conn.cursor()
    cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening '
        'WHERE composite_rank IS NOT NULL ORDER BY date'
    )
    return [r[0] for r in cursor.fetchall()]


def get_composite_data(conn, date):
    """특정 날짜의 composite_rank가 있는 종목들의 ticker, adj_gap, composite_rank 조회"""
    cursor = conn.cursor()
    cursor.execute(
        'SELECT ticker, adj_gap, composite_rank FROM ntm_screening '
        'WHERE date=? AND composite_rank IS NOT NULL',
        (date,)
    )
    return cursor.fetchall()


def get_part2_data(conn, date):
    """특정 날짜의 part2_rank 조회"""
    cursor = conn.cursor()
    cursor.execute(
        'SELECT ticker, part2_rank FROM ntm_screening '
        'WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
        (date,)
    )
    return cursor.fetchall()


def compute_new_composite_ranks(rows):
    """adj_gap 오름차순으로 새 composite_rank 계산

    Args:
        rows: [(ticker, adj_gap, old_composite_rank), ...]

    Returns:
        dict: {ticker: new_rank}
        list: 스킵된 ticker (adj_gap이 NULL)
    """
    # adj_gap이 NULL인 종목 분리
    valid = [(t, gap, old) for t, gap, old in rows if gap is not None]
    skipped = [t for t, gap, old in rows if gap is None]

    # adj_gap 오름차순 정렬 (가장 음수 = rank 1)
    valid.sort(key=lambda x: x[1])

    new_ranks = {}
    for i, (ticker, gap, old) in enumerate(valid):
        new_ranks[ticker] = i + 1

    # NULL adj_gap 종목은 맨 뒤에 배치
    next_rank = len(valid) + 1
    for ticker in skipped:
        new_ranks[ticker] = next_rank
        next_rank += 1

    return new_ranks, skipped


def compute_new_part2_ranks(conn, date, all_composite_ranks):
    """가중순위 기반 part2_rank 재계산

    Args:
        conn: DB connection
        date: 현재 날짜
        all_composite_ranks: {date: {ticker: rank}} — 이미 업데이트된 composite_rank

    Returns:
        dict: {ticker: new_part2_rank} (Top 30)
    """
    today_ranks = all_composite_ranks.get(date, {})
    if not today_ranks:
        return {}

    # 이전 2일 날짜 조회 (all_composite_ranks에서)
    all_dates = sorted(all_composite_ranks.keys())
    prev_dates = [d for d in all_dates if d < date]
    t1 = prev_dates[-1] if len(prev_dates) >= 1 else None
    t2 = prev_dates[-2] if len(prev_dates) >= 2 else None

    t1_ranks = all_composite_ranks.get(t1, {}) if t1 else {}
    t2_ranks = all_composite_ranks.get(t2, {}) if t2 else {}

    # 가중순위 = T0×0.5 + T1×0.3 + T2×0.2
    weighted = {}
    for ticker, r0 in today_ranks.items():
        r1 = t1_ranks.get(ticker, PENALTY)
        r2 = t2_ranks.get(ticker, PENALTY)
        weighted[ticker] = r0 * 0.5 + r1 * 0.3 + r2 * 0.2

    # 가중순위 오름차순 정렬 → Top 30
    sorted_tickers = sorted(weighted.items(), key=lambda x: x[1])
    top30 = sorted_tickers[:30]

    return {ticker: rank for rank, (ticker, _) in enumerate(top30, 1)}


def main():
    apply_mode = '--apply' in sys.argv

    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # ==========================================================
    # 0. 모든 날짜 수집
    # ==========================================================
    dates = get_dates_with_composite(conn)
    print(f"=== v52 순위 재계산 ({'APPLY' if apply_mode else 'DRY RUN'}) ===")
    print(f"대상 날짜: {len(dates)}개 ({dates[0]} ~ {dates[-1]})")
    print()

    # ==========================================================
    # 1. composite_rank 재계산 (adj_gap 오름차순)
    # ==========================================================
    print("[1] composite_rank 재계산 (adj_gap 오름차순)")

    # 기존 데이터 저장 (비교용)
    old_composite = {}  # {date: {ticker: old_rank}}
    old_part2 = {}      # {date: {ticker: old_rank}}

    # 새 데이터
    new_composite = {}  # {date: {ticker: new_rank}}

    total_composite_changed = 0
    total_composite_tickers = 0

    for date in dates:
        rows = get_composite_data(conn, date)
        if not rows:
            continue

        # 기존 값 저장
        old_composite[date] = {t: old for t, gap, old in rows}

        # 새 composite_rank 계산
        new_ranks, skipped = compute_new_composite_ranks(rows)
        new_composite[date] = new_ranks

        # 변경 카운트
        changed = sum(
            1 for t in new_ranks
            if new_ranks[t] != old_composite[date].get(t)
        )
        total_composite_changed += changed
        total_composite_tickers += len(new_ranks)

        if skipped:
            print(f"  {date}: {len(new_ranks)}개 종목, {changed}개 변경, "
                  f"NULL adj_gap 스킵: {', '.join(skipped)}")
        else:
            print(f"  {date}: {len(new_ranks)}개 종목, {changed}개 변경")

    print(f"\n  composite_rank 총 변경: {total_composite_changed}/{total_composite_tickers}")
    print()

    # ==========================================================
    # 2. part2_rank 재계산 (가중순위 Top 30)
    # ==========================================================
    print("[2] part2_rank 재계산 (가중순위 Top 30)")

    new_part2 = {}  # {date: {ticker: new_rank}}
    total_part2_changed = 0
    total_part2_tickers = 0

    for date in dates:
        # 기존 part2 저장
        p2_rows = get_part2_data(conn, date)
        old_part2[date] = {t: r for t, r in p2_rows}

        # 새 part2_rank 계산
        new_p2 = compute_new_part2_ranks(conn, date, new_composite)
        new_part2[date] = new_p2

        # 변경 카운트
        old_set = set(old_part2[date].keys())
        new_set = set(new_p2.keys())
        rank_changed = sum(
            1 for t in old_set & new_set
            if old_part2[date][t] != new_p2.get(t)
        )
        members_changed = len(old_set ^ new_set)
        total_changes = rank_changed + members_changed
        total_part2_changed += total_changes
        total_part2_tickers += max(len(old_set), len(new_set))

        # 이전 날짜 정보
        all_d = sorted(new_composite.keys())
        prev_d = [d for d in all_d if d < date]
        t1 = prev_d[-1] if len(prev_d) >= 1 else None
        t2 = prev_d[-2] if len(prev_d) >= 2 else None

        entered = new_set - old_set
        exited = old_set - new_set
        extra = ""
        if entered:
            extra += f" 진입: {','.join(sorted(entered)[:5])}"
            if len(entered) > 5:
                extra += f"외 {len(entered)-5}"
        if exited:
            extra += f" 이탈: {','.join(sorted(exited)[:5])}"
            if len(exited) > 5:
                extra += f"외 {len(exited)-5}"

        print(f"  {date}: Top 30 (t1={t1}, t2={t2}), {total_changes}개 변경{extra}")

    print(f"\n  part2_rank 총 변경: {total_part2_changed}/{total_part2_tickers}")
    print()

    # ==========================================================
    # 3. 최근 3일 before/after 비교
    # ==========================================================
    print("[3] 최근 3일 before/after 비교")
    recent_dates = dates[-3:] if len(dates) >= 3 else dates

    for date in recent_dates:
        print(f"\n  === {date} ===")

        # composite_rank 비교 (Top 10)
        old_c = old_composite.get(date, {})
        new_c = new_composite.get(date, {})

        # adj_gap 조회 (표시용)
        rows = get_composite_data(conn, date)
        gap_map = {t: gap for t, gap, _ in rows}

        old_top10 = sorted(old_c.items(), key=lambda x: x[1])[:10]
        new_top10 = sorted(new_c.items(), key=lambda x: x[1])[:10]

        print(f"  composite_rank Top 10:")
        print(f"    {'OLD':<35s} {'NEW':<35s}")
        print(f"    {'---':<35s} {'---':<35s}")
        for i in range(10):
            old_str = ""
            new_str = ""
            if i < len(old_top10):
                t, r = old_top10[i]
                g = gap_map.get(t, 0) or 0
                old_str = f"#{r:>2d} {t:<6s} (gap={g:+.1f}%)"
            if i < len(new_top10):
                t, r = new_top10[i]
                g = gap_map.get(t, 0) or 0
                new_str = f"#{r:>2d} {t:<6s} (gap={g:+.1f}%)"
            print(f"    {old_str:<35s} {new_str:<35s}")

        # part2_rank 비교 (Top 10)
        old_p = old_part2.get(date, {})
        new_p = new_part2.get(date, {})

        old_p_top10 = sorted(old_p.items(), key=lambda x: x[1])[:10]
        new_p_top10 = sorted(new_p.items(), key=lambda x: x[1])[:10]

        print(f"\n  part2_rank Top 10:")
        print(f"    {'OLD':<25s} {'NEW':<25s}")
        print(f"    {'---':<25s} {'---':<25s}")
        for i in range(10):
            old_str = ""
            new_str = ""
            if i < len(old_p_top10):
                t, r = old_p_top10[i]
                old_str = f"#{r:>2d} {t:<6s}"
            if i < len(new_p_top10):
                t, r = new_p_top10[i]
                new_str = f"#{r:>2d} {t:<6s}"
            print(f"    {old_str:<25s} {new_str:<25s}")

    # ==========================================================
    # 4. Summary
    # ==========================================================
    print("\n" + "=" * 60)
    print(f"총 {len(dates)}개 날짜 처리")
    print(f"composite_rank 변경: {total_composite_changed}/{total_composite_tickers}")
    print(f"part2_rank 변경: {total_part2_changed}/{total_part2_tickers}")

    # ==========================================================
    # 5. 적용 or 종료
    # ==========================================================
    if not apply_mode:
        print("\n위 변경 사항을 적용하려면:")
        print("  python migrate_v52_ranks.py --apply")
        conn.close()
        return

    # 확인 프롬프트
    print()
    answer = input("변경 사항을 DB에 적용하시겠습니까? (yes/no): ").strip().lower()
    if answer != 'yes':
        print("취소되었습니다.")
        conn.close()
        return

    # DB 적용
    print("\n[적용] DB 업데이트 시작...")
    cursor = conn.cursor()

    # composite_rank 업데이트
    for date in dates:
        new_c = new_composite.get(date, {})
        if not new_c:
            continue

        # 해당 날짜의 모든 composite_rank NULL로 초기화
        cursor.execute(
            'UPDATE ntm_screening SET composite_rank=NULL WHERE date=?',
            (date,)
        )

        for ticker, rank in new_c.items():
            cursor.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (rank, date, ticker)
            )

    # part2_rank 업데이트
    for date in dates:
        new_p = new_part2.get(date, {})

        # 해당 날짜의 모든 part2_rank NULL로 초기화
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank=NULL WHERE date=?',
            (date,)
        )

        for ticker, rank in new_p.items():
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date, ticker)
            )

    conn.commit()
    conn.close()
    print("[적용] 완료!")

    # 검증: DB 재접속 후 확인
    conn2 = sqlite3.connect(DB_PATH)
    cursor2 = conn2.cursor()
    print("\n[검증] 최근 3일 Top 5:")
    for date in recent_dates:
        cursor2.execute(
            'SELECT part2_rank, ticker, adj_gap FROM ntm_screening '
            'WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 5',
            (date,)
        )
        top5 = cursor2.fetchall()
        top5_str = ', '.join(f"#{r[0]} {r[1]}(gap={r[2]:+.1f}%)" if r[2] else f"#{r[0]} {r[1]}" for r in top5)
        cursor2.execute(
            'SELECT COUNT(*) FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (date,)
        )
        comp_count = cursor2.fetchone()[0]
        print(f"  {date}: composite={comp_count}개, Top5=[{top5_str}]")
    conn2.close()


if __name__ == '__main__':
    main()
