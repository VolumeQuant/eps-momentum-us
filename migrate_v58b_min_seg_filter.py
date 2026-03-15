"""v58b 마이그레이션: min_seg < -2% 종목을 순위에서 제외

기존: 모든 Part 2 eligible 종목에 composite_rank 부여 후 min_seg 필터
변경: min_seg < -2% 종목은 composite_rank/part2_rank 부여 전에 제외

과거 21거래일 composite_rank + part2_rank(w_gap 기반) 재계산.
"""
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'


def calc_min_seg(nc, n7, n30, n60, n90):
    segs = []
    for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
        if b and abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0)
    return min(segs)


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # composite_rank 있는 날짜
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    # adj_gap 있는 모든 날짜 (w_gap lookback용)
    all_dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE adj_gap IS NOT NULL ORDER BY date'
    ).fetchall()]

    print(f'거래일: {len(dates)}개 ({dates[0]} ~ {dates[-1]})')

    # 날짜별 adj_gap 로드 (w_gap 계산용)
    gap_by_date = {}
    for d in all_dates:
        rows = cursor.execute(
            'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL',
            (d,)
        ).fetchall()
        gap_by_date[d] = {r[0]: r[1] for r in rows}

    total_comp_changed = 0
    total_p2_changed = 0
    total_excluded = 0

    for date_str in dates:
        # 기존 순위
        old_comp = {r[0]: r[1] for r in cursor.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (date_str,)
        ).fetchall()}
        old_p2 = {r[0]: r[1] for r in cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (date_str,)
        ).fetchall()}

        # NTM 데이터로 min_seg 계산
        ntm_rows = cursor.execute('''
            SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, adj_gap
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (date_str,)).fetchall()

        # min_seg >= -2% 만 남기기
        eligible = []
        excluded = []
        for tk, nc, n7, n30, n60, n90, ag in ntm_rows:
            ms = calc_min_seg(nc, n7, n30, n60, n90)
            if ms >= -2:
                eligible.append((tk, ag))
            else:
                excluded.append((tk, ms))

        # adj_gap 오름차순 → composite_rank
        eligible.sort(key=lambda x: x[1] if x[1] is not None else 999)
        new_comp = {tk: i + 1 for i, (tk, _) in enumerate(eligible)}

        # composite_rank 초기화 + 재저장
        cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (date_str,))
        for tk, crank in new_comp.items():
            cursor.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (crank, date_str, tk)
            )

        # w_gap 계산 (eligible 종목만)
        di = all_dates.index(date_str)
        d0 = all_dates[di]
        d1 = all_dates[di - 1] if di >= 1 else None
        d2 = all_dates[di - 2] if di >= 2 else None

        wgaps = {}
        for tk, _ in eligible:
            g0 = gap_by_date.get(d0, {}).get(tk, 0)
            g1 = gap_by_date.get(d1, {}).get(tk, 0) if d1 else 0
            g2 = gap_by_date.get(d2, {}).get(tk, 0) if d2 else 0
            wgaps[tk] = g0 * 0.5 + g1 * 0.3 + g2 * 0.2

        # w_gap 오름차순 Top 30 → part2_rank
        sorted_tickers = sorted(eligible, key=lambda x: wgaps.get(x[0], 0))
        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date_str,))
        top30 = sorted_tickers[:30]
        new_p2 = {}
        for rank, (tk, _) in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date_str, tk)
            )
            new_p2[tk] = rank

        # 변경 카운트
        comp_changed = sum(1 for tk in set(list(old_comp.keys()) + list(new_comp.keys()))
                          if old_comp.get(tk) != new_comp.get(tk))
        p2_changed = sum(1 for tk in set(list(old_p2.keys()) + list(new_p2.keys()))
                         if old_p2.get(tk) != new_p2.get(tk))

        total_comp_changed += comp_changed
        total_p2_changed += p2_changed
        total_excluded += len(excluded)

        if excluded:
            exc_str = ', '.join(f'{tk}(ms{ms:.1f})' for tk, ms in excluded[:5])
            print(f'  {date_str}: 제외 {len(excluded)}개 [{exc_str}] | '
                  f'comp변경 {comp_changed} | p2변경 {p2_changed}')
        else:
            print(f'  {date_str}: 제외 없음 | comp변경 {comp_changed} | p2변경 {p2_changed}')

    conn.commit()
    conn.close()
    print(f'\n완료: 제외 {total_excluded}건, composite 변경 {total_comp_changed}건, '
          f'part2_rank 변경 {total_p2_changed}건')


if __name__ == '__main__':
    main()
