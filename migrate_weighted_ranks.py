"""과거 날짜 composite_rank + part2_rank 재계산

핵심: composite_rank = 당일 순수 점수 순위 (DB에 저장)
      part2_rank = 가중순위(composite × 0.5 + T1_composite × 0.3 + T2_composite × 0.2) Top 30

가중순위는 항상 composite_rank에서 계산 → 누적(cascading) 방지
"""
import sqlite3
import sys
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'
PENALTY = 50
TOP_N = 30


def get_eligible_tickers_with_ranks(cursor, date_str):
    """해당 날짜의 eligible 종목을 composite 순위로 반환."""
    cursor.execute('''
        SELECT ticker, adj_gap, rev_growth, num_analysts, rev_up30, rev_down30
        FROM ntm_screening
        WHERE date=? AND adj_score > 9 AND adj_gap IS NOT NULL
        AND price IS NOT NULL AND price >= 10
        AND ma60 IS NOT NULL AND price > ma60
    ''', (date_str,))
    rows = cursor.fetchall()

    if not rows:
        return {}

    filtered = []
    for ticker, adj_gap, rev_growth, num_analysts, rev_up, rev_down in rows:
        if num_analysts is not None and num_analysts < 3:
            continue
        if rev_up is not None and rev_down is not None:
            total = (rev_up or 0) + (rev_down or 0)
            if total > 0 and (rev_down or 0) / total > 0.3:
                continue
        filtered.append((ticker, adj_gap, rev_growth))

    if not filtered:
        return {}

    rev_count = sum(1 for _, _, rg in filtered if rg is not None)
    use_composite = rev_count >= 10

    if use_composite:
        filtered = [(t, g, r) for t, g, r in filtered if r is not None and r >= 0.10]
        if not filtered:
            return {}

        gaps = [g for _, g, _ in filtered]
        revs = [r for _, _, r in filtered]
        gap_mean, gap_std = np.mean(gaps), np.std(gaps)
        rev_mean, rev_std = np.mean(revs), np.std(revs)

        if gap_std > 0 and rev_std > 0:
            scored = []
            for t, g, r in filtered:
                z_gap = (g - gap_mean) / gap_std
                z_rev = (r - rev_mean) / rev_std
                composite = (-z_gap) * 0.7 + z_rev * 0.3
                scored.append((t, composite))
            scored.sort(key=lambda x: x[1], reverse=True)
        else:
            scored = sorted(filtered, key=lambda x: x[1])
            scored = [(t, -i) for i, (t, _, _) in enumerate(scored)]
    else:
        scored = sorted(filtered, key=lambda x: x[1])
        scored = [(t, -i) for i, (t, _, _) in enumerate(scored)]

    return {t: rank + 1 for rank, (t, _) in enumerate(scored)}


def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # composite_rank 컬럼 추가
    try:
        cursor.execute('ALTER TABLE ntm_screening ADD COLUMN composite_rank INTEGER')
        print("composite_rank 컬럼 추가됨")
    except sqlite3.OperationalError:
        print("composite_rank 컬럼 이미 존재")

    # 모든 날짜
    cursor.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')
    dates = [r[0] for r in cursor.fetchall()]

    print(f"\n총 {len(dates)}개 날짜 재계산")
    print(f"composite_rank = 당일 순수 순위 (DB 저장)")
    print(f"part2_rank = 가중순위(composite 기반) Top 30")
    print(f"PENALTY={PENALTY}")
    print()

    # 이전 날짜의 composite_rank 저장 (가중순위 계산용)
    prev_composites = {}  # {date: {ticker: composite_rank}}

    for date_str in dates:
        # 1. composite 순위 계산
        composite_ranks = get_eligible_tickers_with_ranks(cursor, date_str)
        if not composite_ranks:
            print(f"  {date_str}: eligible 0개 — 스킵")
            continue

        # 2. composite_rank DB 저장 (모든 eligible)
        cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (date_str,))
        for ticker, crank in composite_ranks.items():
            cursor.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (crank, date_str, ticker)
            )

        # 3. 이전 날짜의 composite_rank로 가중순위 계산 (누적 없음!)
        prev_dates = sorted([d for d in prev_composites.keys() if d < date_str])
        t1 = prev_dates[-1] if len(prev_dates) >= 1 else None
        t2 = prev_dates[-2] if len(prev_dates) >= 2 else None

        weighted = {}
        for ticker, r0 in composite_ranks.items():
            r1 = prev_composites.get(t1, {}).get(ticker, PENALTY) if t1 else PENALTY
            r2 = prev_composites.get(t2, {}).get(ticker, PENALTY) if t2 else PENALTY
            weighted[ticker] = r0 * 0.5 + r1 * 0.3 + r2 * 0.2

        # 4. 가중순위 Top 30 → part2_rank
        sorted_tickers = sorted(weighted.items(), key=lambda x: x[1])
        top30 = sorted_tickers[:TOP_N]

        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date_str,))
        for rank, (ticker, w) in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, ticker)
            )

        # 5. 이 날짜의 composite_rank 저장 (다음 날짜 참조용)
        prev_composites[date_str] = dict(composite_ranks)

        # 리포트
        cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL AND part2_rank <= 5 ORDER BY part2_rank',
            (date_str,)
        )
        top5 = [f"{r[1]}.{r[0]}" for r in cursor.fetchall()]
        has_history = "✅" if t1 else "🆕"
        print(f"  {date_str}: eligible={len(composite_ranks)}, Top5=[{', '.join(top5)}] {has_history}")

    conn.commit()
    conn.close()
    print(f"\n완료 — {len(dates)}개 날짜 composite_rank + part2_rank 재계산")


if __name__ == '__main__':
    migrate()
