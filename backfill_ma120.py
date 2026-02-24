"""MA120 backfill — 기존 DB 데이터에 ma120 값 추가

init_ntm_database()로 컬럼 생성 후,
최근 3일 데이터의 모든 종목에 대해 1y 히스토리를 가져와 ma120 계산.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, '.')
from daily_runner import DB_PATH, init_ntm_database, log

import yfinance as yf


def backfill_ma120():
    # 1. Migration — ma120 컬럼 추가
    init_ntm_database()
    print("DB migration 완료 (ma120 컬럼)")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 2. 최근 3일 날짜 + 종목 목록
    dates = [d[0] for d in c.execute(
        'SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC LIMIT 3'
    ).fetchall()]
    print(f"대상 날짜: {dates}")

    # ma120이 NULL인 레코드만
    rows = c.execute('''
        SELECT DISTINCT ticker FROM ntm_screening
        WHERE date IN ({}) AND (ma120 IS NULL)
    '''.format(','.join('?' * len(dates))), dates).fetchall()
    tickers = [r[0] for r in rows]
    print(f"MA120 미수집 종목: {len(tickers)}개")

    if not tickers:
        print("모두 수집 완료 — 종료")
        conn.close()
        return

    # 3. yfinance로 1y 히스토리 → ma120 계산
    def fetch_ma120(ticker):
        try:
            hist = yf.Ticker(ticker).history(period='1y', auto_adjust=True)
            if hist.empty or len(hist) < 120:
                return ticker, None, len(hist) if not hist.empty else 0
            ma120 = float(hist['Close'].rolling(window=120).mean().iloc[-1])
            return ticker, ma120, len(hist)
        except Exception as e:
            return ticker, None, str(e)

    results = {}
    start = time.time()
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch_ma120, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            done += 1
            ticker, ma120, info = f.result()
            results[ticker] = ma120
            if done % 100 == 0:
                elapsed = time.time() - start
                print(f"  {done}/{len(tickers)} ({elapsed:.0f}s)")

    elapsed = time.time() - start
    print(f"수집 완료: {len(results)}종목, {elapsed:.1f}s")

    # 4. DB 업데이트
    updated = 0
    skipped = 0
    for ticker, ma120 in results.items():
        if ma120 is not None:
            c.execute('''
                UPDATE ntm_screening SET ma120 = ?
                WHERE ticker = ? AND date IN ({})
            '''.format(','.join('?' * len(dates))), [ma120, ticker] + dates)
            updated += c.rowcount
        else:
            skipped += 1

    conn.commit()
    conn.close()
    print(f"업데이트: {updated}행, 스킵: {skipped}종목 (데이터 부족)")
    print("Done!")


if __name__ == '__main__':
    backfill_ma120()
