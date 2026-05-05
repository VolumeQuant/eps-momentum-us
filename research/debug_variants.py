"""변형 적용이 실제로 part2_rank를 바꾸는지 진단"""
import sqlite3
import shutil
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'v80_9_extras_dbs'
SEG_CAP = 100


def calc_segs(nc, n7, n30, n60, n90):
    if not all(x and abs(x) > 0.01 for x in (n7, n30, n60, n90)):
        return [0, 0, 0, 0]
    s1 = max(-SEG_CAP, min(SEG_CAP, (nc-n7)/abs(n7)*100))
    s2 = max(-SEG_CAP, min(SEG_CAP, (n7-n30)/abs(n30)*100))
    s3 = max(-SEG_CAP, min(SEG_CAP, (n30-n60)/abs(n60)*100))
    s4 = max(-SEG_CAP, min(SEG_CAP, (n60-n90)/abs(n90)*100))
    return [s1, s2, s3, s4]


def get_p2_top(db, date, n=20):
    conn = sqlite3.connect(db)
    rows = conn.execute(
        'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank<=? ORDER BY part2_rank',
        (date, n)
    ).fetchall()
    conn.close()
    return rows


# 1) 원본 DB의 어느 일자에서, baseline vs avg/median 변형 시 elig pool이 달라지는지
print('=' * 80)
print('샘플 일자 테스트: 4/30')
print('=' * 80)

conn = sqlite3.connect(DB_ORIGINAL)
date = '2026-04-30'
rows = conn.execute('''SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, rev_up30, adj_gap
                       FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL''', (date,)).fetchall()
conn.close()

baseline_pool = []
avg_pool = []
median_pool = []
for r in rows:
    tk, nc, n7, n30, n60, n90, ru, ag = r
    if ag is None or (ru or 0) < 3: continue
    segs = calc_segs(nc or 0, n7 or 0, n30 or 0, n60 or 0, n90 or 0)
    ms_min = min(segs)
    ms_avg = sum(segs)/4
    ms_med = sorted(segs)[1:3]
    ms_med = (ms_med[0]+ms_med[1])/2
    if ms_min >= -2: baseline_pool.append(tk)
    if ms_avg >= -2: avg_pool.append(tk)
    if ms_med >= -2: median_pool.append(tk)

print(f'Eligible (rev_up30>=3 후):')
print(f'  baseline (min>=-2): {len(baseline_pool)}')
print(f'  avg (avg>=-2):      {len(avg_pool)}')
print(f'  median:             {len(median_pool)}')
print(f'  baseline에 있고 avg에 없음 (avg가 더 엄격): {len(set(baseline_pool)-set(avg_pool))}')
print(f'  avg에 있고 baseline에 없음 (avg가 더 관대): {len(set(avg_pool)-set(baseline_pool))}')


# 2) 실제 생성된 db들의 part2_rank가 다른지 비교
print('\n' + '=' * 80)
print('생성된 DB part2_rank 비교 (4/30 Top 10)')
print('=' * 80)

for slug in ['__X2_base', 'B1__min_seg___avg', 'B2__min_seg___median', 'B4__min_seg___min_s1', 'E2__rev_bonus_cap_0_2']:
    db = GRID / f'{slug}.db'
    if db.exists():
        top = get_p2_top(db, date, 10)
        print(f'\n{slug}:')
        for tk, p2 in top:
            print(f'  p2={p2}: {tk}')
    else:
        print(f'\n{slug}: DB 없음')
