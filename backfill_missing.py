"""2/6(금), 2/9(월) 데이터 보충 — DB의 NTM 스냅샷 + yfinance 과거 종가로 계산

- DB date 2/7 → 미국 2/6(금) 데이터. 날짜 2/6으로 수정 + price/ma60/adj_score/adj_gap/part2_rank 계산
- DB date 2/8 → 주말 중복. 삭제
- DB date 2/9 → 미국 2/9(월) 데이터. price/ma60/adj_score/adj_gap/part2_rank 계산
"""
import sys, io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from eps_momentum_system import calculate_ntm_score, calculate_eps_change_90d

DB_PATH = Path(__file__).parent / 'eps_momentum_data.db'


def backfill_date(db_date, market_date, conn):
    """특정 날짜의 데이터 보충"""
    print(f"\n{'='*60}")
    print(f"  DB date {db_date} → 미국 영업일 {market_date}")
    print(f"{'='*60}")

    # 1) DB에서 NTM 데이터 로드
    rows = conn.execute('''
        SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, score
        FROM ntm_screening WHERE date=? AND ntm_current IS NOT NULL
    ''', (db_date,)).fetchall()
    print(f"  NTM 데이터: {len(rows)}개 종목")

    if not rows:
        print("  데이터 없음 — 스킵")
        return

    tickers = [r[0] for r in rows]
    ntm_map = {}
    for r in rows:
        ntm_map[r[0]] = {
            'current': r[1], '7d': r[2], '30d': r[3], '60d': r[4], '90d': r[5]
        }

    # 2) yfinance에서 과거 가격 다운로드 (market_date 기준 90일 전부터)
    market_dt = datetime.strptime(market_date, '%Y-%m-%d')
    start = (market_dt - timedelta(days=120)).strftime('%Y-%m-%d')
    end = (market_dt + timedelta(days=5)).strftime('%Y-%m-%d')

    print(f"  가격 다운로드: {len(tickers)}종목, {start} ~ {end}")
    price_data = yf.download(tickers, start=start, end=end, progress=False)

    if price_data.empty:
        print("  가격 데이터 없음 — 스킵")
        return

    if isinstance(price_data.columns, pd.MultiIndex):
        closes = price_data['Close']
    else:
        closes = price_data[['Close']]
        closes.columns = [tickers[0]]

    # 날짜 인덱스 tz 제거
    closes.index = closes.index.tz_localize(None) if closes.index.tz else closes.index

    # market_date 또는 가장 가까운 이전 거래일 찾기
    target = pd.Timestamp(market_date)
    available_dates = closes.index[closes.index <= target]
    if len(available_dates) == 0:
        print(f"  {market_date} 이전 거래일 없음 — 스킵")
        return
    actual_date = available_dates[-1]
    print(f"  실제 거래일: {actual_date.strftime('%Y-%m-%d')}")

    # 3) 각 종목별 계산
    cursor = conn.cursor()
    updated = 0
    part2_candidates = []

    for ticker in tickers:
        if ticker not in closes.columns:
            continue

        ticker_prices = closes[ticker].dropna()
        if len(ticker_prices) < 60:
            continue

        # 현재가 (market_date 기준)
        prices_up_to = ticker_prices[ticker_prices.index <= target]
        if len(prices_up_to) < 60:
            continue

        price = float(prices_up_to.iloc[-1])
        ma60 = float(prices_up_to.tail(60).mean())

        if price <= 0:
            continue

        ntm = ntm_map.get(ticker)
        if not ntm or ntm['current'] is None:
            continue

        # adj_score, direction 계산
        try:
            score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction = calculate_ntm_score(ntm)
            eps_change_90d = calculate_eps_change_90d(ntm)
        except Exception:
            continue

        # fwd_pe & adj_gap 계산
        nc = ntm['current']
        fwd_pe = price / nc if nc > 0 else None

        adj_gap = None
        if nc > 0:
            fwd_pe_now = price / nc

            # 과거 시점 가격 찾기
            hist_prices = {}
            for days, key in [(7, '7d'), (30, '30d'), (60, '60d'), (90, '90d')]:
                ref = target - timedelta(days=days)
                past = ticker_prices[ticker_prices.index <= ref]
                if len(past) > 0:
                    hist_prices[key] = float(past.iloc[-1])

            # 가중평균 PE 변화
            weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
            w_sum = 0.0
            w_total = 0.0
            for key, w in weights.items():
                ntm_val = ntm.get(key, 0)
                hp = hist_prices.get(key, 0)
                if nc > 0 and ntm_val > 0 and hp > 0:
                    fwd_pe_then = hp / ntm_val
                    pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                    w_sum += w * pe_chg
                    w_total += w

            if w_total > 0:
                fwd_pe_chg = w_sum / w_total
                dir_factor = max(-0.3, min(0.3, direction / 30))
                adj_gap = fwd_pe_chg * (1 + dir_factor)

        # DB 업데이트
        cursor.execute('''
            UPDATE ntm_screening
            SET score=?, adj_score=?, adj_gap=?, price=?, ma60=?
            WHERE date=? AND ticker=?
        ''', (score, adj_score, adj_gap, price, ma60, db_date, ticker))
        updated += 1

        # Part 2 후보 판별
        if (adj_score and adj_score > 9
                and adj_gap is not None and adj_gap <= 0
                and fwd_pe and fwd_pe > 0
                and eps_change_90d and eps_change_90d > 0
                and price >= 10
                and price > ma60):
            part2_candidates.append((ticker, adj_gap, adj_score))

    conn.commit()
    print(f"  업데이트: {updated}개 종목")

    # 4) Part 2 rank 저장 (adj_gap 오름차순 Top 30)
    part2_candidates.sort(key=lambda x: x[1])
    top30 = part2_candidates[:30]

    for i, (ticker, gap, ascore) in enumerate(top30):
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
            (i + 1, db_date, ticker)
        )

    conn.commit()
    print(f"  Part 2 rank: {len(top30)}개 종목")
    if top30:
        print(f"  Top 5: {', '.join(f'{t[0]}({t[1]:+.1f})' for t in top30[:5])}")


def main():
    conn = sqlite3.connect(DB_PATH)

    # 0) 날짜 수정: 2/7 → 2/6
    print("DB date 2/7 → 2/6 으로 변경 (미국 금요일)")
    conn.execute("UPDATE ntm_screening SET date='2026-02-06' WHERE date='2026-02-07'")
    conn.commit()

    # 1) 2/8 삭제 (주말 중복)
    print("DB date 2/8 삭제 (주말)")
    conn.execute("DELETE FROM ntm_screening WHERE date='2026-02-08'")
    conn.commit()

    # 2) 2/6 데이터 보충
    backfill_date('2026-02-06', '2026-02-06', conn)

    # 3) 2/9 데이터 보충
    backfill_date('2026-02-09', '2026-02-09', conn)

    # 4) 최종 확인
    print(f"\n{'='*60}")
    print("  최종 DB 현황")
    print(f"{'='*60}")
    rows = conn.execute('''
        SELECT date, COUNT(*) as total,
            SUM(CASE WHEN adj_score IS NOT NULL THEN 1 ELSE 0 END) as has_adj,
            SUM(CASE WHEN part2_rank IS NOT NULL THEN 1 ELSE 0 END) as has_rank
        FROM ntm_screening GROUP BY date ORDER BY date
    ''').fetchall()
    for r in rows:
        print(f"  {r[0]}: total={r[1]}, adj_score={r[2]}, part2_rank={r[3]}")

    # cold start 체크
    rank_days = conn.execute(
        'SELECT COUNT(DISTINCT date) FROM ntm_screening WHERE part2_rank IS NOT NULL'
    ).fetchone()[0]
    print(f"\n  part2_rank 날짜 수: {rank_days}일")
    print(f"  cold start: {'YES' if rank_days < 3 else 'NO — 채널 활성화 가능!'}")

    conn.close()


if __name__ == '__main__':
    main()
