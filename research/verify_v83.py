"""v83 검증 — 5/22 (가장 최근) part2_rank 변화 + 시스템 성과 비교"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

ROOT = Path(__file__).parent.parent
DB_V83 = ROOT / 'eps_momentum_data.db'
DB_V82 = ROOT / 'eps_momentum_data.db.bak_pre_v83'


def get_top10(db, date):
    conn = sqlite3.connect(db)
    rows = conn.execute(
        'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL '
        'AND part2_rank <= 10 ORDER BY part2_rank',
        (date,)
    ).fetchall()
    conn.close()
    return rows


def get_c2_info(date):
    """현재 v83 DB에서 5/22 종목별 C2 여부 표시"""
    import daily_runner as dr
    dr.DB_PATH = str(DB_V83)
    conn = sqlite3.connect(DB_V83)
    cur = conn.cursor()
    rows = cur.execute(
        'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL AND part2_rank <= 15 ORDER BY part2_rank',
        (date,)
    ).fetchall()
    result = []
    for (tk,) in rows:
        is_c2 = dr._is_c2_for_v83(cur, date, tk)
        # eps_chg_weighted + 가격 30d
        eps_row = cur.execute(
            'SELECT eps_chg_weighted, price FROM ntm_screening WHERE date=? AND ticker=?',
            (date, tk)
        ).fetchone()
        eps_w = eps_row[0] if eps_row else 0
        cur_p = eps_row[1] if eps_row else None
        past_p = cur.execute(
            'SELECT price FROM ntm_screening WHERE ticker=? AND date<? AND price IS NOT NULL '
            'ORDER BY date DESC LIMIT 1 OFFSET 29',
            (tk, date)
        ).fetchone()
        p30 = None
        if past_p and past_p[0] and cur_p:
            p30 = (cur_p - past_p[0]) / past_p[0] * 100
        result.append((tk, eps_w, p30, is_c2))
    conn.close()
    return result


def main():
    print('=' * 100)
    print('v83 검증 — 5/22 (최근) part2_rank 변화')
    print('=' * 100)

    DATE = '2026-05-22'
    v82_top = get_top10(DB_V82, DATE)
    v83_top = get_top10(DB_V83, DATE)

    print(f'\n[5/22 Top 10 비교]')
    print(f'{"rank":<6} {"v82 (이전)":<10} {"v83 (현재)":<10} {"변화":<10}')
    print('-' * 50)
    v82_map = {tk: r for tk, r in v82_top}
    v83_map = {tk: r for tk, r in v83_top}
    for rank in range(1, 11):
        v82_tk = next((tk for tk, r in v82_top if r == rank), '-')
        v83_tk = next((tk for tk, r in v83_top if r == rank), '-')
        marker = ''
        if v82_tk != v83_tk:
            marker = '★ 변경'
        print(f'  {rank:<4} {v82_tk:<10} {v83_tk:<10} {marker}')

    # C2 여부 표시
    print(f'\n[5/22 v83 Top 15 — C2 여부]')
    print(f'{"rank":<6} {"ticker":<8} {"eps_w":>8} {"price 30d":>10} {"C2?":>5}')
    print('-' * 50)
    info = get_c2_info(DATE)
    for i, (tk, eps_w, p30, is_c2) in enumerate(info, 1):
        c2_mark = '★C2' if is_c2 else ''
        p30_str = f'{p30:+.2f}%' if p30 is not None else '-'
        print(f'  {i:<4} {tk:<8} {eps_w:>+7.2f} {p30_str:>10} {c2_mark:>5}')

    # 시스템 성과
    print(f'\n[시스템 누적 성과 — v83 적용 후]')
    sys.path.insert(0, '.')
    import daily_runner as dr
    dr.DB_PATH = str(DB_V83)
    perf = dr._get_system_performance()
    if perf:
        print(f'  시스템 누적: {perf["sys_cum"]:+.2f}%')
        print(f'  SPY 누적:    {perf["spy_cum"]:+.2f}%')
        print(f'  알파:        {perf["alpha"]:+.2f}%p')
        print(f'  기간:        {perf["start_date"]} ~ {perf["end_date"]} ({perf["n_days"]}일)')
        print(f'  승/패:       {perf["wins"]}/{perf["losses"]}')

    # 백업과 비교
    dr.DB_PATH = str(DB_V82)
    perf_v82 = dr._get_system_performance()
    if perf_v82:
        print(f'\n[v82 (백업) 동일 기간 성과]')
        print(f'  시스템 누적: {perf_v82["sys_cum"]:+.2f}%')
        print(f'  알파:        {perf_v82["alpha"]:+.2f}%p')
        if perf:
            print(f'\n[v82 → v83 차이]')
            print(f'  시스템 누적: {perf["sys_cum"] - perf_v82["sys_cum"]:+.2f}%p')
            print(f'  알파:        {perf["alpha"] - perf_v82["alpha"]:+.2f}%p')


if __name__ == '__main__':
    main()
