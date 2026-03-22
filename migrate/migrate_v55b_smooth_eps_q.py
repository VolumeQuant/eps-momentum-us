"""v55b 마이그레이션: eps_quality 3단계 → 연속함수 전환

기존 (v55):  3단계 계단식
  min_seg >= 2%  → 1.3
  min_seg >= 0%  → 1.0
  min_seg < 0%   → 0.7

변경 (v55b): 연속함수
  eps_q = 1.0 + 0.3 × clamp(min_seg / 2, -1, 1)
  min_seg = -2% → 0.7, 0% → 1.0, 2% → 1.3 (동일)
  경계 근처 cliff effect 제거

마이그레이션: adj_gap = adj_gap / old_eps_q × new_eps_q
이후 composite_rank + part2_rank 재계산
"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = Path(__file__).parent.parent / 'eps_momentum_data.db'


def compute_segs(ntm_cur, ntm_7d, ntm_30d, ntm_60d, ntm_90d):
    segs = []
    for a, b in [(ntm_cur, ntm_7d), (ntm_7d, ntm_30d),
                 (ntm_30d, ntm_60d), (ntm_60d, ntm_90d)]:
        if b is not None and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return segs


def old_eps_q(min_seg):
    """v55 3단계"""
    if min_seg >= 2:
        return 1.3
    elif min_seg >= 0:
        return 1.0
    else:
        return 0.7


def new_eps_q(min_seg):
    """v55b 연속함수"""
    return 1.0 + 0.3 * max(-1, min(1, min_seg / 2))


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'처리 대상: {len(dates)}개 날짜 ({dates[0]} ~ {dates[-1]})')

    # Step 1: adj_gap 변환 (old_eps_q → new_eps_q)
    print('\n=== Step 1: adj_gap 변환 (3단계 → 연속함수) ===')
    total_changed = 0
    total_unchanged = 0

    for date_str in dates:
        rows = cursor.execute('''
            SELECT rowid, ticker, adj_gap, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL
        ''', (date_str,)).fetchall()

        changed = 0
        for rowid, ticker, old_ag, nc, n7, n30, n60, n90 in rows:
            if nc is None:
                continue
            segs = compute_segs(nc, n7, n30, n60, n90)
            ms = min(segs)
            old_q = old_eps_q(ms)
            new_q = new_eps_q(ms)

            if abs(old_q - new_q) < 0.001:
                total_unchanged += 1
                continue

            # Reverse old, apply new: adj_gap = (adj_gap / old_q) * new_q
            base_ag = old_ag / old_q
            new_ag = base_ag * new_q

            cursor.execute(
                'UPDATE ntm_screening SET adj_gap=? WHERE rowid=?',
                (new_ag, rowid)
            )
            changed += 1

        total_changed += changed
        if changed > 0:
            sample = cursor.execute('''
                SELECT ticker, adj_gap FROM ntm_screening
                WHERE date=? AND adj_gap IS NOT NULL
                ORDER BY adj_gap LIMIT 3
            ''', (date_str,)).fetchall()
            sample_str = ', '.join(f'{t}({ag:.1f}%)' for t, ag in sample)
            print(f'  {date_str}: {changed}행 변경 | Top3: {sample_str}')

    conn.commit()
    print(f'\nadj_gap 변환 완료: {total_changed}행 변경, {total_unchanged}행 동일')

    # Step 2: composite_rank 재계산
    print('\n=== Step 2: composite_rank 재계산 ===')
    rank_dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    for date_str in rank_dates:
        rows = cursor.execute('''
            SELECT ticker, adj_gap
            FROM ntm_screening
            WHERE date=? AND composite_rank IS NOT NULL AND adj_gap IS NOT NULL
            ORDER BY adj_gap ASC
        ''', (date_str,)).fetchall()

        for rank, (ticker, adj_gap) in enumerate(rows, 1):
            cursor.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, ticker)
            )

    conn.commit()

    # Step 3: part2_rank 재계산
    print('\n=== Step 3: part2_rank 재계산 ===')
    for i, date_str in enumerate(rank_dates):
        prev_dates = rank_dates[max(0, i-2):i]
        t1 = prev_dates[-1] if len(prev_dates) >= 1 else None
        t2 = prev_dates[-2] if len(prev_dates) >= 2 else None

        today_rows = cursor.execute('''
            SELECT ticker, adj_gap
            FROM ntm_screening
            WHERE date=? AND composite_rank IS NOT NULL AND adj_gap IS NOT NULL
        ''', (date_str,)).fetchall()
        today_gaps = {tk: ag for tk, ag in today_rows}

        gap_t1, gap_t2 = {}, {}
        if t1:
            gap_t1 = {tk: ag for tk, ag in cursor.execute(
                'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
                (t1,)
            ).fetchall()}
        if t2:
            gap_t2 = {tk: ag for tk, ag in cursor.execute(
                'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
                (t2,)
            ).fetchall()}

        weighted = {}
        for ticker in today_gaps:
            g0 = today_gaps.get(ticker, 0)
            g1 = gap_t1.get(ticker, 0)
            g2 = gap_t2.get(ticker, 0)
            weighted[ticker] = g0 * 0.5 + g1 * 0.3 + g2 * 0.2

        sorted_tickers = sorted(weighted.items(), key=lambda x: x[1])
        top30 = sorted_tickers[:30]

        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date_str,))
        for rank, (ticker, w) in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, ticker)
            )

    conn.commit()
    conn.close()
    print(f'\n완료: adj_gap(연속함수) + composite_rank + part2_rank 전체 재계산')


if __name__ == '__main__':
    main()
