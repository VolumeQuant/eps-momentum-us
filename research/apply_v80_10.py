"""v80.10 마이그레이션 — Forward PE long-tail 가중치 적용

변경: weights = {'7d': 0.40, '30d': 0.30, '60d': 0.20, '90d': 0.10}
            → {'7d': 0.30, '30d': 0.10, '60d': 0.10, '90d': 0.50}

근거:
  - 4D 그리드 84조합 BT: production 80/84위
  - Walk-forward 5/5 splits: production OOS 항상 11/11위
  - A 후보 (w_30_10_10_50) 인접 안정성 7변형 모두 5/5 lift 양수 (+46~65%p)
  - 두 전문가 (퀀트 + 리스크) 모두 A 후보 권장
  - seg-style 비교: cumulative + long-tail이 모든 형태 중 best

영향: 모든 일자(60일) adj_gap / cr / p2 재계산
"""
import sqlite3
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_pe_weights as btpe
import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_TARGET = ROOT / 'eps_momentum_data.db'
DB_BACKUP = ROOT / 'eps_momentum_data.bak_pre_v80_10.db'

NEW_WEIGHTS = {'7d': 0.30, '30d': 0.10, '60d': 0.10, '90d': 0.50}


def main():
    print('=' * 80)
    print('v80.10 마이그레이션 — PE long-tail 가중치')
    print(f'DB: {DB_TARGET}')
    print(f'백업: {DB_BACKUP}')
    print(f'새 가중치: {NEW_WEIGHTS}')
    print('=' * 80)

    if not DB_TARGET.exists():
        print('❌ DB 없음')
        return

    if not DB_BACKUP.exists():
        print(f'\n[1] 백업 생성: {DB_BACKUP}')
        shutil.copy(DB_TARGET, DB_BACKUP)
    else:
        print(f'\n[1] 백업 이미 존재 (skip): {DB_BACKUP}')

    print('\n[2] regenerate 실행 (모든 일자 adj_gap/cr/p2 재계산)')
    print('  ⚠️ 시간 ~3분 예상')
    btpe.regenerate(str(DB_TARGET), NEW_WEIGHTS)

    print('\n[3] 검증')
    conn = sqlite3.connect(DB_TARGET)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date DESC LIMIT 5'
    ).fetchall()][::-1]
    print(f'\n  최근 5일 매수 후보 (cr=1~3):')
    for d in dates:
        rows = cur.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank<=3 ORDER BY composite_rank',
            (d,)
        ).fetchall()
        info = ', '.join(f'{r[0]}(cr={r[1]})' for r in rows)
        print(f'    {d}: {info}')

    print(f'\n  최근 5일 part2_rank Top 5:')
    for d in dates:
        rows = cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank<=5 ORDER BY part2_rank',
            (d,)
        ).fetchall()
        info = ', '.join(f'{r[0]}(p2={r[1]})' for r in rows)
        print(f'    {d}: {info}')

    conn.close()
    print('\n✓ v80.10 마이그레이션 완료')


if __name__ == '__main__':
    main()
