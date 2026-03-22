"""v54 마이그레이션 2단계: composite_rank + part2_rank 재계산

adj_gap에 eps_quality가 반영된 후, downstream 순위를 재계산:
1. composite_rank: 각 날짜별 eligible 종목을 adj_gap 오름차순 재정렬
2. part2_rank: w_gap(3일 가중 adj_gap) 기준 Top 30 재선정

eligible 판단: 기존 composite_rank IS NOT NULL (필터 조건 변경 없음)
"""
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 모든 날짜 조회 (composite_rank 있는 날짜만)
    dates = cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()
    dates = [d[0] for d in dates]
    print(f'처리 대상: {len(dates)}개 날짜 ({dates[0]} ~ {dates[-1]})')

    # ── Step 1: composite_rank 재계산 ──
    print('\n=== Step 1: composite_rank 재계산 ===')
    for date_str in dates:
        # 기존 eligible 종목 (composite_rank IS NOT NULL) → adj_gap 오름차순 재정렬
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

    # ── Step 2: part2_rank 재계산 ──
    print('\n=== Step 2: part2_rank 재계산 ===')
    for i, date_str in enumerate(dates):
        # 이전 2일 날짜 (composite_rank 있는 날짜 중)
        prev_dates = dates[max(0, i-2):i]  # 최대 2개

        t1 = prev_dates[-1] if len(prev_dates) >= 1 else None
        t2 = prev_dates[-2] if len(prev_dates) >= 2 else None

        # 오늘 eligible 종목의 adj_gap
        today_rows = cursor.execute('''
            SELECT ticker, adj_gap
            FROM ntm_screening
            WHERE date=? AND composite_rank IS NOT NULL AND adj_gap IS NOT NULL
        ''', (date_str,)).fetchall()
        today_gaps = {ticker: ag for ticker, ag in today_rows}

        # T-1, T-2 adj_gap
        gap_t1 = {}
        gap_t2 = {}
        if t1:
            for ticker, ag in cursor.execute(
                'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
                (t1,)
            ).fetchall():
                gap_t1[ticker] = ag
        if t2:
            for ticker, ag in cursor.execute(
                'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
                (t2,)
            ).fetchall():
                gap_t2[ticker] = ag

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

        # part2_rank 초기화 후 재설정
        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date_str,))
        for rank, (ticker, w) in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, ticker)
            )

        top3 = [(t, f'{w:.1f}%') for t, w in top30[:3]]
        print(f'  {date_str}: eligible {len(today_gaps)}종목 → Top 30 | Top3: {top3}')

    conn.commit()
    conn.close()

    print(f'\n완료: {len(dates)}개 날짜 composite_rank + part2_rank 재계산')


if __name__ == '__main__':
    main()
