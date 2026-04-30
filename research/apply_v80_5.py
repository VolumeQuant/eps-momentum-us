"""v80.5 마이그레이션 — Case 1 z-score 보너스 제거.

전제: β1 + opt4 유지 (변경 없음), Case 1만 제거.
효과: score/adj_score/adj_gap/composite_rank는 변경 없음. part2_rank만 재계산.

방법:
  1. 현재 DB 백업 (bak_pre_v80_5)
  2. 모든 일자 part2_rank 재계산 (Case 1 제거된 _compute_w_gap_map 사용)
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
DB_BACKUP = ROOT / 'eps_momentum_data.bak_pre_v80_5.db'


def main():
    print('=' * 80)
    print('v80.5 마이그레이션 — Case 1 z-score 보너스 제거')
    print(f'DB: {DB_TARGET}')
    print('=' * 80)

    if not DB_TARGET.exists():
        print(f'❌ DB 없음: {DB_TARGET}')
        return

    print(f'\n[1] 백업: {DB_BACKUP}')
    shutil.copy(DB_TARGET, DB_BACKUP)

    print('\n[2] part2_rank 재계산 (Case 1 제거된 w_gap 사용)...')
    dr.DB_PATH = str(DB_TARGET)
    conn = sqlite3.connect(DB_TARGET)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    print(f'  대상 일자: {len(dates)}일')

    changed_count = 0
    for today in dates:
        # 해당 일자 eligible 종목들
        tickers = [r[0] for r in cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (today,)
        ).fetchall()]

        # 새 w_gap 맵 계산 (Case 1 제거됨)
        wmap = dr._compute_w_gap_map(cur, today, tickers)

        # 기존 part2_rank
        old_p2 = {r[0]: r[1] for r in cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (today,)
        ).fetchall()}

        # w_gap descending Top 30
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        new_p2 = {tk: rk for rk, tk in enumerate(top30, 1)}

        # 변경 감지
        if old_p2 != new_p2:
            changed_count += 1

        # 적용
        cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
        for tk, rk in new_p2.items():
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()

    print(f'  완료. {changed_count}/{len(dates)}일 part2_rank 변경됨.')

    # 검증
    print('\n[3] 검증')
    cur.execute('SELECT MIN(part2_rank), MAX(part2_rank), COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    mn, mx, cnt = cur.fetchone()
    print(f'  part2_rank 범위: {mn}~{mx}, 총 {cnt}건')

    cur.execute(
        '''SELECT date, COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL
        GROUP BY date HAVING COUNT(*) != 30 ORDER BY date'''
    )
    issues = cur.fetchall()
    if issues:
        print(f'  ⚠️ 일자별 30개 아닌 경우 {len(issues)}건')
        for d, c in issues[:5]:
            print(f'    {d}: {c}건')
    else:
        print(f'  ✓ 모든 일자 30종목')

    # 최근 5일 LNG / MU / LRCX 확인
    print('\n[4] 최근 5일 주요 종목')
    last_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 5'
    ).fetchall()][::-1]
    for tk in ['LNG', 'MU', 'LRCX', 'ASML', 'LITE']:
        print(f'  {tk}:')
        for d in last_dates:
            row = cur.execute(
                'SELECT part2_rank, composite_rank FROM ntm_screening WHERE ticker=? AND date=?',
                (tk, d)
            ).fetchone()
            if row:
                p2, cr = row
                print(f'    {d}: p2={p2 if p2 else "-"}, cr={cr if cr else "-"}')

    conn.close()
    print('\n✓ 마이그레이션 완료')


if __name__ == '__main__':
    main()
