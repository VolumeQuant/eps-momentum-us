"""v81 마이그레이션: ma20 컬럼 추가 + 과거 65일 backfill + part2_rank 재계산

배경:
  v81 = price > MA120 (MA60 fallback) → price > MA20 변경
  매수 진입 필터를 단기 모멘텀으로 강화. BT 검증:
    - random 100seed×3, 12시작일 multistart 모두 100% 우월
    - 6/6 파라미터 조합에서 lift > 0
    - MDD 평균 +8.46%p 개선

  핵심 직관: MA20 이탈 시 part2_rank NULL → rank>10 트리거 → 즉시 매도
    → 알파는 "단기 모멘텀 잃은 종목 빠른 cut"에서 옴 (매수 필터 자체는 alpha 0)

마이그레이션:
  1. ALTER TABLE: ma20 컬럼 추가
  2. research/price_history_for_ma_bt.parquet에서 ma20 backfill
  3. 매 일자 part2_rank 재계산 (MA20 필터 적용)

사용법: python research/apply_v81.py
"""
import sys
import sqlite3
import shutil
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import pandas as pd
import daily_runner as dr
import bt_ma_filter_extended as ext

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
BACKUP = ROOT / 'eps_momentum_data.db.bak_pre_v81'
PRICE_PARQUET = Path(__file__).parent / 'price_history_for_ma_bt.parquet'


def main():
    print('=' * 100)
    print('v81 마이그레이션: ma20 컬럼 추가 + backfill + part2_rank 재계산')
    print('=' * 100)

    # 백업 확인
    if not BACKUP.exists():
        print(f'\nERROR: 백업 파일 없음 ({BACKUP})')
        print('cp eps_momentum_data.db eps_momentum_data.db.bak_pre_v81 먼저 실행')
        return
    print(f'\n백업 확인: {BACKUP} ({BACKUP.stat().st_size/1e6:.1f}MB)')

    # Parquet 확인
    if not PRICE_PARQUET.exists():
        print(f'\nERROR: price parquet 없음 ({PRICE_PARQUET})')
        return
    close_df = pd.read_parquet(PRICE_PARQUET)
    print(f'Parquet: {close_df.shape}, dates {close_df.index.min()} ~ {close_df.index.max()}')

    # ma20 계산
    ma20_df = close_df.rolling(window=20, min_periods=20).mean()
    print(f'MA20 계산 완료')

    # DB 연결
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1) ma20 컬럼 추가
    print('\n[1] ALTER TABLE: ma20 컬럼 추가')
    try:
        cur.execute('ALTER TABLE ntm_screening ADD COLUMN ma20 REAL')
        conn.commit()
        print('  ✓ ma20 컬럼 추가됨')
    except sqlite3.OperationalError as e:
        if 'duplicate column' in str(e).lower():
            print('  · ma20 컬럼 이미 존재 (skip)')
        else:
            raise

    # 2) ma20 backfill
    print('\n[2] ma20 backfill (parquet에서)')
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
    ).fetchall()]
    print(f'  DB dates: {len(dates)}일 ({dates[0]} ~ {dates[-1]})')

    parquet_dates = ma20_df.index.tolist()
    total_updated = 0
    for db_date in dates:
        # parquet date <= db_date 중 가장 가까운 거래일
        eligible = [d for d in parquet_dates if d <= db_date]
        if not eligible:
            print(f'  {db_date}: parquet에 이전 거래일 없음, skip')
            continue
        ref_date = eligible[-1]
        row = ma20_df.loc[ref_date]

        # 해당 날짜의 모든 ticker에 ma20 UPDATE
        date_tickers = [r[0] for r in cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=?', (db_date,)
        ).fetchall()]
        updates = []
        for tk in date_tickers:
            if tk in row.index:
                val = row[tk]
                if pd.notna(val):
                    updates.append((float(val), db_date, tk))
        cur.executemany(
            'UPDATE ntm_screening SET ma20=? WHERE date=? AND ticker=?',
            updates
        )
        total_updated += len(updates)
    conn.commit()
    print(f'  ✓ {total_updated}개 row 업데이트 (DB {len(dates)}일)')

    # 3) part2_rank 재계산 (MA20 필터 적용)
    print('\n[3] part2_rank 재계산 (MA20 필터 적용)')
    print('  매 일자별로 MA20 위 종목 → composite_rank → w_gap → part2_rank Top 30 재계산')
    print('  ※ research/ma_filter_dbs/ext_ma20.db와 동일 로직')
    conn.close()  # ext.regenerate가 자체 connection 사용

    t0 = time.time()
    ext.regenerate(DB_PATH, 'ma20', ma20_df)
    print(f'  ✓ regenerate: {time.time()-t0:.1f}s')

    # 4) 검증
    print('\n[4] 검증')
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    print('  최근 5일 part2_rank 분포:')
    for d, n in cur.execute('''
        SELECT date, COUNT(*) FROM ntm_screening
        WHERE part2_rank IS NOT NULL GROUP BY date
        ORDER BY date DESC LIMIT 5
    ''').fetchall():
        print(f'    {d}: {n}개')

    print('  최근 5일 composite_rank 분포 (eligible 종목 수):')
    for d, n in cur.execute('''
        SELECT date, COUNT(*) FROM ntm_screening
        WHERE composite_rank IS NOT NULL GROUP BY date
        ORDER BY date DESC LIMIT 5
    ''').fetchall():
        print(f'    {d}: {n}개')

    print('  ma20 coverage 최근 5일:')
    for d, n_total, n_ma20 in cur.execute('''
        SELECT date, COUNT(*), SUM(CASE WHEN ma20 IS NOT NULL THEN 1 ELSE 0 END)
        FROM ntm_screening GROUP BY date ORDER BY date DESC LIMIT 5
    ''').fetchall():
        print(f'    {d}: {n_ma20}/{n_total} ({n_ma20/n_total*100:.0f}%)')

    conn.close()
    print('\n완료. daily_runner.py 수정 후 운영 가능.')


if __name__ == '__main__':
    main()
