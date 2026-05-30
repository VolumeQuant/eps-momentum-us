"""v84 마이그레이션 — 기존 DB에 high30 컬럼 채우기

- yfinance 200일 history로 각 종목/일자별 30일 high 정확 계산
- ntm_screening.high30 컬럼 채우기

진입 시 dd_30_25 필터 작동하려면 high30 필요.
"""
import sys
import sqlite3
import time
from pathlib import Path
from collections import defaultdict
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
CACHE = ROOT / 'research' / 'yfinance_200d_cache.pkl'


def main():
    print('=' * 80)
    print('v84 마이그레이션 — high30 컬럼 채우기')
    print('=' * 80)

    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()

    # 1. high30 컬럼 추가 (이미 있으면 skip)
    try:
        cur.execute('ALTER TABLE ntm_screening ADD COLUMN high30 REAL')
        print('high30 컬럼 추가됨')
    except sqlite3.OperationalError:
        print('high30 컬럼 이미 존재')

    # 2. unique tickers + dates
    tickers = [r[0] for r in cur.execute('SELECT DISTINCT ticker FROM ntm_screening').fetchall()]
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    print(f'unique tickers: {len(tickers)}, dates: {len(dates)}')

    # 3. yfinance 200d history (캐시 사용)
    if CACHE.exists():
        print(f'캐시 hit: {CACHE}')
        closes = pd.read_pickle(CACHE)
    else:
        print(f'yfinance 200d fetch...')
        t0 = time.time()
        data = yf.download(' '.join(tickers), period='200d', group_by='ticker',
                          auto_adjust=True, threads=True, progress=False)
        closes = {}
        for tk in tickers:
            try:
                s = data[tk]['Close'].dropna()
                if len(s) > 0:
                    closes[tk] = s
            except (KeyError, AttributeError):
                pass
        pd.to_pickle(closes, CACHE)
        print(f'  done {time.time()-t0:.1f}s ({len(closes)} valid)')

    # 4. tz 통일 (date index)
    closes_naive = {}
    for tk, s in closes.items():
        s2 = s.copy()
        if s2.index.tz is not None:
            s2.index = s2.index.tz_localize(None)
        s2.index = pd.to_datetime(s2.index).date
        closes_naive[tk] = s2

    # 5. 각 (date, ticker)에 대해 30일 high 계산 + UPDATE
    print('\nhigh30 계산 + UPDATE...')
    t0 = time.time()
    updated = 0
    skipped = 0
    for d in dates:
        d_pd = pd.Timestamp(d).date()
        for tk in tickers:
            s = closes_naive.get(tk)
            if s is None:
                skipped += 1
                continue
            mask = s.index <= d_pd
            s_filtered = s[mask]
            if len(s_filtered) >= 30:
                high30 = float(s_filtered.tail(30).max())
                cur.execute('UPDATE ntm_screening SET high30=? WHERE date=? AND ticker=?',
                           (high30, d, tk))
                updated += 1
            elif len(s_filtered) >= 10:
                high30 = float(s_filtered.max())  # 부분 max (cold start 보호)
                cur.execute('UPDATE ntm_screening SET high30=? WHERE date=? AND ticker=?',
                           (high30, d, tk))
                updated += 1
            else:
                skipped += 1
    conn.commit()
    print(f'  done {time.time()-t0:.1f}s')
    print(f'  updated: {updated}, skipped (data 부족): {skipped}')

    # 6. 검증 — AEIS 최근 high30 확인
    print('\nAEIS 최근 high30 spot check:')
    rows = cur.execute('''
        SELECT date, price, high30
        FROM ntm_screening WHERE ticker='AEIS' AND date >= '2026-05-19'
        ORDER BY date
    ''').fetchall()
    for d, p, h in rows:
        dd = (p - h) / h * 100 if h else 0
        cut = '✗ 컷' if dd <= -25 else '✓ 통과'
        print(f'  {d}: price ${p:.2f}, high30 ${h:.2f}, dd {dd:+.2f}% [{cut}]')

    # 7. 검증 — high30 NULL 비율
    n_total = cur.execute('SELECT COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL').fetchone()[0]
    n_null = cur.execute("SELECT COUNT(*) FROM ntm_screening WHERE part2_rank IS NOT NULL AND high30 IS NULL").fetchone()[0]
    print(f'\npart2_rank 종목 중 high30 NULL: {n_null}/{n_total} ({n_null/n_total*100:.1f}%)')

    conn.close()
    print('\n마이그레이션 완료')


if __name__ == '__main__':
    main()
