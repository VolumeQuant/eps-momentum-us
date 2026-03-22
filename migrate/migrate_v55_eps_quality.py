"""v55 마이그레이션: 과거 adj_gap에 eps_quality(min_seg 기반) 적용 + 순위 재계산

기존 (v52/v53): adj_gap = fwd_pe_chg × (1 + dir_factor)
변경 (v55):     adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_q

eps_q (min_seg 기반, 3단계):
  min_seg >= 2%  → 1.3 (전 구간 고른 상향)
  min_seg >= 0%  → 1.0 (중립)
  min_seg < 0%   → 0.7 (한 구간이라도 꺾임)

min_seg = min(seg1, seg2, seg3, seg4)
  seg1 = (ntm_current - ntm_7d) / |ntm_7d| × 100
  seg2 = (ntm_7d - ntm_30d) / |ntm_30d| × 100
  seg3 = (ntm_30d - ntm_60d) / |ntm_60d| × 100
  seg4 = (ntm_60d - ntm_90d) / |ntm_90d| × 100

마이그레이션: new_adj_gap = old_adj_gap × eps_q
이후 composite_rank + part2_rank 재계산
"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = Path(__file__).parent.parent / 'eps_momentum_data.db'


def compute_segs(ntm_cur, ntm_7d, ntm_30d, ntm_60d, ntm_90d):
    """seg1~seg4 계산"""
    segs = []
    for a, b in [(ntm_cur, ntm_7d), (ntm_7d, ntm_30d),
                 (ntm_30d, ntm_60d), (ntm_60d, ntm_90d)]:
        if b is not None and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return segs


def compute_eps_q(segs):
    """v55 eps_quality: min_seg 기반 3단계"""
    min_seg = min(segs)
    if min_seg >= 2:
        return 1.3
    elif min_seg >= 0:
        return 1.0
    else:
        return 0.7


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 모든 날짜
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'처리 대상: {len(dates)}개 날짜 ({dates[0]} ~ {dates[-1]})')

    # ── Step 1: adj_gap에 eps_q 적용 ──
    print('\n=== Step 1: adj_gap × eps_q 적용 ===')
    total_updated = 0
    eq_dist = {0.7: 0, 1.0: 0, 1.3: 0}

    for date_str in dates:
        rows = cursor.execute('''
            SELECT rowid, ticker, adj_gap, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL
        ''', (date_str,)).fetchall()

        updated = 0
        for rowid, ticker, old_ag, nc, n7, n30, n60, n90 in rows:
            if nc is None:
                continue
            segs = compute_segs(nc, n7, n30, n60, n90)
            eq = compute_eps_q(segs)
            new_ag = old_ag * eq

            cursor.execute(
                'UPDATE ntm_screening SET adj_gap=? WHERE rowid=?',
                (new_ag, rowid)
            )
            eq_dist[eq] += 1
            updated += 1

        total_updated += updated

        # 샘플 출력
        sample = cursor.execute('''
            SELECT ticker, adj_gap FROM ntm_screening
            WHERE date=? AND adj_gap IS NOT NULL
            ORDER BY adj_gap LIMIT 3
        ''', (date_str,)).fetchall()
        sample_str = ', '.join(f'{t}({ag:.1f}%)' for t, ag in sample)
        print(f'  {date_str}: {updated}행 | Top3: {sample_str}')

    conn.commit()
    print(f'\nadj_gap 업데이트 완료: {total_updated}행')
    print(f'  eps_q 분포: 0.7={eq_dist[0.7]}, 1.0={eq_dist[1.0]}, 1.3={eq_dist[1.3]}')

    # ── Step 2: composite_rank 재계산 ──
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

        print(f'  {date_str}: {len(rows)}종목 composite_rank 재정렬')

    conn.commit()

    # ── Step 3: part2_rank 재계산 (w_gap 기준) ──
    print('\n=== Step 3: part2_rank 재계산 ===')
    for i, date_str in enumerate(rank_dates):
        prev_dates = rank_dates[max(0, i-2):i]
        t1 = prev_dates[-1] if len(prev_dates) >= 1 else None
        t2 = prev_dates[-2] if len(prev_dates) >= 2 else None

        # 오늘 eligible 종목의 adj_gap
        today_rows = cursor.execute('''
            SELECT ticker, adj_gap
            FROM ntm_screening
            WHERE date=? AND composite_rank IS NOT NULL AND adj_gap IS NOT NULL
        ''', (date_str,)).fetchall()
        today_gaps = {tk: ag for tk, ag in today_rows}

        # T-1, T-2 adj_gap
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

        # w_gap 계산
        weighted = {}
        for ticker in today_gaps:
            g0 = today_gaps.get(ticker, 0)
            g1 = gap_t1.get(ticker, 0)
            g2 = gap_t2.get(ticker, 0)
            weighted[ticker] = g0 * 0.5 + g1 * 0.3 + g2 * 0.2

        # w_gap 오름차순 → Top 30
        sorted_tickers = sorted(weighted.items(), key=lambda x: x[1])
        top30 = sorted_tickers[:30]

        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date_str,))
        for rank, (ticker, w) in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, ticker)
            )

        top3 = [(t, f'{w:.1f}%') for t, w in top30[:3]]
        print(f'  {date_str}: {len(today_gaps)}종목 → Top 30 | Top3: {top3}')

    conn.commit()
    conn.close()
    print(f'\n완료: adj_gap + composite_rank + part2_rank 전체 재계산')


if __name__ == '__main__':
    main()
