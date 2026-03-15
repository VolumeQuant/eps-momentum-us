"""v57b 마이그레이션: part2_rank를 raw adj_gap 기준으로 재계산

기존 (v55b): 3일 가중 adj_gap(w_gap = T0×0.5 + T1×0.3 + T2×0.2) 정렬 → part2_rank
변경 (v57b): 당일 raw adj_gap(= composite_rank 순서) 그대로 → part2_rank

composite_rank는 이미 당일 adj_gap 오름차순이므로,
part2_rank = composite_rank 상위 30개 그대로 재할당.
"""
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # composite_rank가 있는 날짜 목록
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'처리 대상: {len(dates)}개 날짜 ({dates[0]} ~ {dates[-1]})')

    total_changed = 0

    for date_str in dates:
        # 당일 composite_rank 정렬 (이미 adj_gap 오름차순)
        rows = cursor.execute('''
            SELECT ticker, composite_rank
            FROM ntm_screening
            WHERE date=? AND composite_rank IS NOT NULL
            ORDER BY composite_rank ASC
        ''', (date_str,)).fetchall()

        # 기존 part2_rank 조회
        old_ranks = {r[0]: r[1] for r in cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (date_str,)
        ).fetchall()}

        # part2_rank 초기화
        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date_str,))

        # 상위 30개에 part2_rank 1~30 할당
        top30 = rows[:30]
        new_ranks = {}
        for rank, (ticker, comp_rank) in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, ticker)
            )
            new_ranks[ticker] = rank

        # 변경 사항 카운트
        changed = 0
        for tk, new_r in new_ranks.items():
            old_r = old_ranks.get(tk)
            if old_r != new_r:
                changed += 1
        # 기존에 있었는데 없어진 것
        for tk in old_ranks:
            if tk not in new_ranks:
                changed += 1

        if changed > 0:
            total_changed += changed
            # 상위 3개 샘플
            sample = ', '.join(f'{tk}({cr})' for tk, cr in top30[:3])
            print(f'  {date_str}: {changed}건 변경 | Top3: {sample}')

    conn.commit()
    conn.close()
    print(f'\n완료: {total_changed}건 part2_rank 변경 (raw adj_gap 기준)')


if __name__ == '__main__':
    main()
