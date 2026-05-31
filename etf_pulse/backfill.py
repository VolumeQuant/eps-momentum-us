"""ETF Pulse Backfill — 과거 N일 데이터 채우기

yfinance.history로 N일치 가격/거래량 → DB 저장
holdings는 현재 snapshot만 가능 (과거 holdings 못 받음)
"""
import sys
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from etf_universe import get_all_etfs

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def backfill_etf(ticker, category, days=30):
    """N일 가격/거래량 history → 일별 snapshot 생성"""
    t = yf.Ticker(ticker)
    try:
        hist = t.history(period=f'{days+40}d')  # 30일 추가 (avg 계산용)
        if hist.empty:
            return []
        close = hist['Close'].dropna()
        vol = hist['Volume'].dropna()
        if len(close) < 2:
            return []

        # 현재 정보 (AUM 등)는 snapshot으로 한 번만
        info = t.info
        aum_now = info.get('totalAssets', 0) or 0
        expense = (info.get('netExpenseRatio') or info.get('annualReportExpenseRatio') or 0) / 100
        divy = info.get('yield') or 0  # yfinance yield는 이미 decimal
        beta = info.get('beta3Year') or info.get('beta') or 0

        out = []
        # 최근 N일만 저장 (앞 40일은 avg 계산용)
        recent = close.tail(days)
        for i, (dt, price) in enumerate(recent.items()):
            date_str = dt.strftime('%Y-%m-%d')
            # 인덱스 = close 전체에서 이 날짜의 위치
            idx = close.index.get_loc(dt)
            if idx < 1:
                continue
            prev = close.iloc[idx - 1]
            day_ret = (price - prev) / prev * 100 if prev > 0 else 0
            vol_today = int(vol.iloc[idx]) if idx < len(vol) else 0
            vol_avg = int(vol.iloc[max(0, idx-30):idx].mean()) if idx > 0 else 0
            vol_spike = vol_today / vol_avg if vol_avg > 0 else 0
            # AUM은 매일 다른 값 없음 → 가격수익률로 추정 (과거 AUM ≈ now × 가격비)
            aum_est = aum_now * (price / recent.iloc[-1]) if recent.iloc[-1] > 0 else aum_now
            out.append({
                'date_str': date_str,
                'ticker': ticker,
                'category': category,
                'price': float(price),
                'volume': vol_today,
                'avg_volume_30d': vol_avg,
                'volume_spike': vol_spike,
                'aum': aum_est,
                'day_return': day_ret,
                'expense_ratio': expense,
                'dividend_yield': divy,
                'beta': beta,
            })
        return out
    except Exception as e:
        return []


def main():
    days = 30
    print(f'=== Backfill {days}일 ===')

    etfs = get_all_etfs()
    print(f'대상 ETF: {len(etfs)}')

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    t0 = time.time()
    total = 0
    for i, (tk, cat) in enumerate(etfs, 1):
        snaps = backfill_etf(tk, cat, days)
        for s in snaps:
            cur.execute('''
                INSERT INTO etf_daily (date, ticker, category, price, volume, avg_volume_30d,
                                        volume_spike, aum, day_return, expense_ratio, dividend_yield, beta)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    category=excluded.category, price=excluded.price, volume=excluded.volume,
                    avg_volume_30d=excluded.avg_volume_30d, volume_spike=excluded.volume_spike,
                    aum=excluded.aum, day_return=excluded.day_return,
                    expense_ratio=excluded.expense_ratio, dividend_yield=excluded.dividend_yield, beta=excluded.beta
            ''', (s['date_str'], s['ticker'], s['category'], s['price'], s['volume'], s['avg_volume_30d'],
                  s['volume_spike'], s['aum'], s['day_return'], s['expense_ratio'], s['dividend_yield'], s['beta']))
            total += 1
        if i % 50 == 0:
            conn.commit()
            print(f'  {i}/{len(etfs)} ({time.time()-t0:.0f}s, {total} rows)')

    conn.commit()
    print(f'\nbackfill 완료: {total} rows, {time.time()-t0:.0f}s')

    # 결과 확인
    n_dates = cur.execute('SELECT COUNT(DISTINCT date) FROM etf_daily').fetchone()[0]
    dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM etf_daily ORDER BY date').fetchall()]
    print(f'\nDB 일자: {n_dates}일 ({dates[0]} ~ {dates[-1]})')

    # fund flow 추정 (간단 — AUM diff 사용)
    # 단 backfill의 AUM은 가격으로 추정한 거라 정확한 flow X.
    # 실제 fund flow는 매일 cron에서 진짜 AUM 누적 후 계산해야.
    print('\n[참고] backfill의 AUM은 가격수익률로 추정 → flow 부정확')
    print('  진짜 flow는 매일 cron에서 yfinance 실시간 AUM 누적 후 가능')

    conn.close()


if __name__ == '__main__':
    main()
