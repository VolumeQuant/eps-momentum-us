"""v80.9 마이그레이션 — X2 적용 (eps_cap 1.0→3.0 + rev_bonus 비례)

전제: rev_up30 ≥ 3 + min_seg < -2 컷오프 유지 (v80.8)
변경:
  1. eps_floor cap 1.0 → 3.0 (정보 보존)
  2. rev_bonus binary (30% cliff +0.3) → 비례 (rg × 0.6, cap 0.3)

영향: 모든 일자 cr/p2 재계산
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
DB_BACKUP = ROOT / 'eps_momentum_data.bak_pre_v80_9.db'


def main():
    print('=' * 80)
    print('v80.9 마이그레이션 — X2 (eps_cap 1.0→3.0 + rev_bonus 비례)')
    print(f'DB: {DB_TARGET}')
    print('=' * 80)

    if not DB_TARGET.exists():
        print(f'❌ DB 없음')
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

    cr_changed = 0
    p2_changed = 0

    for today in dates:
        rows = cur.execute('''
            SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d, rev_growth
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (today,)).fetchall()

        elig_conv = []
        for r in rows:
            tk, ag, ru, na, nc, n90, rg = r
            if ag is None: continue
            cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
            if cg is not None:
                elig_conv.append((tk, cg))

        elig_conv.sort(key=lambda x: x[1])
        new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

        old_cr = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (today,)
        ).fetchall()}
        if old_cr != new_cr: cr_changed += 1

        cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
        for tk, cr in new_cr.items():
            cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?', (cr, today, tk))

        tickers = list(new_cr.keys())
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]

        old_p2 = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (today,)
        ).fetchall()}
        new_p2 = {tk: rk for rk, tk in enumerate(top30, 1)}
        if old_p2 != new_p2: p2_changed += 1

        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for tk, rk in new_p2.items():
            cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?', (rk, today, tk))
        conn.commit()

    print(f'  composite_rank 변경: {cr_changed}/{len(dates)}일')
    print(f'  part2_rank 변경: {p2_changed}/{len(dates)}일')

    print('\n[3] 검증')
    print('  최근 5일 매수 후보:')
    last_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 5'
    ).fetchall()][::-1]
    for d in last_dates:
        rows = cur.execute('SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank<=3 ORDER BY part2_rank', (d,)).fetchall()
        info = ', '.join(f'{r[0]}(p2={r[1]})' for r in rows)
        print(f'    {d}: {info}')

    conn.close()
    print('\n✓ v80.9 마이그레이션 완료')


if __name__ == '__main__':
    main()
