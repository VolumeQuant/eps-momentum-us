"""v58 마이그레이션: part2_rank를 w_gap(3일 가중 adj_gap) 기준으로 재계산

기존 (v57b): 당일 raw adj_gap 오름차순 → part2_rank
변경 (v58): w_gap(T0×0.5 + T1×0.3 + T2×0.2) 오름차순 → part2_rank

composite_rank는 변경 없음 (당일 adj_gap 순위 유지).
과거 데이터를 재수집하지 않음 — 기존 adj_gap 값으로 w_gap만 재계산.
"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = Path(__file__).parent.parent / 'eps_momentum_data.db'


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # part2_rank 있는 날짜
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    # adj_gap 있는 모든 날짜 (lookback용)
    all_dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()]

    print(f'거래일: {len(dates)}개 ({dates[0]} ~ {dates[-1]})')
    print(f'전체 날짜(lookback): {len(all_dates)}개 ({all_dates[0]} ~ {all_dates[-1]})')

    # 날짜별 adj_gap 로드
    gap_by_date = {}
    for d in all_dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
            (d,)
        ).fetchall()
        gap_by_date[d] = {r[0]: r[1] for r in rows}

    total_changed = 0

    for date_str in dates:
        # 기존 part2_rank
        old_ranks = {r[0]: r[1] for r in cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (date_str,)
        ).fetchall()}

        # 이 날짜에 part2_rank 있는 종목들
        eligible_tickers = list(old_ranks.keys())

        # w_gap 계산
        di = all_dates.index(date_str)
        d0 = all_dates[di]
        d1 = all_dates[di - 1] if di >= 1 else None
        d2 = all_dates[di - 2] if di >= 2 else None

        wgaps = {}
        for tk in eligible_tickers:
            g0 = gap_by_date.get(d0, {}).get(tk, 0)
            g1 = gap_by_date.get(d1, {}).get(tk, 0) if d1 else 0
            g2 = gap_by_date.get(d2, {}).get(tk, 0) if d2 else 0
            wgaps[tk] = g0 * 0.5 + g1 * 0.3 + g2 * 0.2

        # w_gap 오름차순 정렬
        sorted_tickers = sorted(eligible_tickers, key=lambda tk: wgaps.get(tk, 0))

        # part2_rank 초기화
        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date_str,))

        # 상위 30개에 part2_rank 1~30 할당
        top30 = sorted_tickers[:30]
        new_ranks = {}
        for rank, ticker in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, ticker)
            )
            new_ranks[ticker] = rank

        # 변경 카운트
        changed = 0
        for tk, new_r in new_ranks.items():
            if old_ranks.get(tk) != new_r:
                changed += 1
        for tk in old_ranks:
            if tk not in new_ranks:
                changed += 1

        if changed > 0:
            total_changed += changed
            sample = ', '.join(f'{tk}(wg{wgaps[tk]:+.1f})' for tk in top30[:3])
            print(f'  {date_str}: {changed}건 변경 | Top3: {sample}')

    conn.commit()
    conn.close()
    print(f'\n완료: {total_changed}건 part2_rank 변경 (w_gap 기준)')


if __name__ == '__main__':
    main()
