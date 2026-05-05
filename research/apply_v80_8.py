"""v80.8 마이그레이션 — rev_up30 ≥ 3 합의 강도 필터 추가

전제: rev_up30 < 3 종목을 cr/p2 부여 단계에서 제외
영향: 모든 일자 cr/p2 재계산 (60일)

방법:
1. 백업 (bak_pre_v80_8)
2. 모든 일자에서 rev_up30 < 3 종목의 cr/p2를 NULL 처리
3. 남은 종목으로 cr (conviction adj_gap ascending) 재정렬
4. w_gap 기반 part2_rank Top 30 재계산
5. 검증

근거: 6시작일 multistart +8.51%p, 12시작일 +7.16%p, MDD +3.47%p 개선
"""
import sqlite3
import shutil
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_TARGET = ROOT / 'eps_momentum_data.db'
DB_BACKUP = ROOT / 'eps_momentum_data.bak_pre_v80_8.db'


def main():
    print('=' * 80)
    print('v80.8 마이그레이션 — rev_up30 ≥ 3 합의 강도 필터')
    print(f'DB: {DB_TARGET}')
    print('=' * 80)

    if not DB_TARGET.exists():
        print(f'❌ DB 없음: {DB_TARGET}')
        return

    print(f'\n[1] 백업: {DB_BACKUP}')
    shutil.copy(DB_TARGET, DB_BACKUP)

    dr.DB_PATH = str(DB_TARGET)
    conn = sqlite3.connect(DB_TARGET)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'\n[2] 대상 일자: {len(dates)}일')

    excluded_total = 0
    cr_changed = 0
    p2_changed = 0

    for today in dates:
        # rev_up30 < 3 종목 식별
        rows = cur.execute('''
            SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (today,)).fetchall()

        elig_conv = []
        excluded_today = 0
        for r in rows:
            tk, ag, ru, na, nc, n90, rg = r
            if ag is None:
                continue
            # v80.8 컷오프
            if (ru or 0) < 3:
                excluded_today += 1
                continue
            cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
            if cg is not None:
                elig_conv.append((tk, cg))

        excluded_total += excluded_today

        elig_conv.sort(key=lambda x: x[1])
        new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

        old_cr = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (today,)
        ).fetchall()}
        if old_cr != new_cr:
            cr_changed += 1

        cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
        for tk, cr in new_cr.items():
            cur.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (cr, today, tk)
            )

        # part2_rank 재계산
        tickers = list(new_cr.keys())
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]

        old_p2 = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (today,)
        ).fetchall()}
        new_p2 = {tk: rk for rk, tk in enumerate(top30, 1)}
        if old_p2 != new_p2:
            p2_changed += 1

        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for tk, rk in new_p2.items():
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()

    print(f'  rev_up30 < 3 제외 누적: {excluded_total}건')
    print(f'  composite_rank 변경: {cr_changed}/{len(dates)}일')
    print(f'  part2_rank 변경: {p2_changed}/{len(dates)}일')

    # 검증
    print('\n[3] 검증')
    cur.execute('SELECT MIN(part2_rank), MAX(part2_rank), COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    mn, mx, cnt = cur.fetchone()
    print(f'  part2_rank 범위: {mn}~{mx}, 총 {cnt}건')

    # rev_up30 < 3인데 cr 있는 케이스 (있으면 안 됨)
    cur.execute('SELECT COUNT(*) FROM ntm_screening WHERE composite_rank IS NOT NULL AND rev_up30 < 3')
    bad = cur.fetchone()[0]
    print(f'  ❌ rev_up30<3인데 cr 있는 종목: {bad}건 (0이어야 정상)')

    # 최근 5일 매수 후보 변화 확인
    print('\n[4] 최근 5일 매수 후보 Top 5')
    last_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 5'
    ).fetchall()][::-1]
    for d in last_dates:
        rows = cur.execute(
            'SELECT ticker, part2_rank, rev_up30 FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL AND part2_rank <= 5 ORDER BY part2_rank',
            (d,)
        ).fetchall()
        info = ', '.join(f'{r[0]}(p2={r[1]}, ↑{r[2]})' for r in rows)
        print(f'  {d}: {info}')

    conn.close()
    print('\n✓ v80.8 마이그레이션 완료')


if __name__ == '__main__':
    main()
