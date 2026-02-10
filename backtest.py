"""
EPS Momentum Backtest Framework
검증 일수(2/3/5/7) × 보유 기간(5/10/15/20일) × 퇴장 조건(고정/Death List) 매트릭스 테스트

사용법: python backtest.py
- DB에 part2_rank 데이터가 30일 이상 필요
- 부족하면 현황만 출력하고 종료
"""

import sqlite3
import sys
import io
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

import pandas as pd
import yfinance as yf

# Windows UTF-8 지원
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / 'eps_momentum_data.db'

# 테스트 변수
VERIFY_DAYS_LIST = [2, 3, 5, 7]
HOLD_DAYS_LIST = [5, 10, 15, 20]
MIN_DATA_DAYS = 30  # 최소 데이터 일수


def load_backtest_data():
    """DB에서 part2_rank 있는 전체 데이터 로드"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """SELECT date, ticker, part2_rank, price, adj_score, adj_gap
        FROM ntm_screening
        WHERE part2_rank IS NOT NULL
        ORDER BY date, part2_rank""",
        conn
    )
    conn.close()
    return df


def get_data_summary():
    """DB 데이터 현황 요약"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(DISTINCT date) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    rank_days = cursor.fetchone()[0]

    cursor.execute('SELECT MIN(date), MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL')
    row = cursor.fetchone()
    min_date, max_date = row[0], row[1]

    cursor.execute("""SELECT date, COUNT(*) as cnt
        FROM ntm_screening WHERE part2_rank IS NOT NULL
        GROUP BY date ORDER BY date""")
    daily = cursor.fetchall()

    conn.close()
    return rank_days, min_date, max_date, daily


def find_entry_signals(df, verify_days):
    """N일 연속 Part 2 종목 찾기 → [(ticker, entry_date, entry_price, adj_gap), ...]"""
    dates = sorted(df['date'].unique())
    if len(dates) < verify_days:
        return []

    # 날짜별 Part 2 종목 set
    date_tickers = {}
    date_prices = {}
    date_gaps = {}
    for d in dates:
        day_df = df[df['date'] == d]
        date_tickers[d] = set(day_df['ticker'])
        date_prices[d] = dict(zip(day_df['ticker'], day_df['price']))
        date_gaps[d] = dict(zip(day_df['ticker'], day_df['adj_gap']))

    entries = []
    seen = set()  # (ticker, entry_date) 중복 방지

    for i in range(verify_days - 1, len(dates)):
        window = dates[i - verify_days + 1:i + 1]
        # N일 모두 Part 2에 있는 종목
        common = set.intersection(*[date_tickers[d] for d in window])

        entry_date = dates[i]  # 검증 완료일 = 진입일
        for ticker in common:
            key = (ticker, entry_date)
            if key not in seen:
                seen.add(key)
                price = date_prices[entry_date].get(ticker)
                gap = date_gaps[entry_date].get(ticker)
                if price and price > 0:
                    entries.append({
                        'ticker': ticker,
                        'entry_date': entry_date,
                        'entry_price': price,
                        'adj_gap': gap,
                    })

    return entries


def get_exit_prices(entries, hold_days_list):
    """yfinance에서 퇴장 가격 일괄 조회 (캐싱)"""
    if not entries:
        return {}

    # 필요한 ticker + 날짜 범위 파악
    tickers = list(set(e['ticker'] for e in entries))
    min_date = min(e['entry_date'] for e in entries)
    max_hold = max(hold_days_list)
    # 충분한 여유 두고 다운로드 (거래일 고려 1.5배)
    end_date = (datetime.strptime(max(e['entry_date'] for e in entries), '%Y-%m-%d')
                + timedelta(days=int(max_hold * 1.5) + 5)).strftime('%Y-%m-%d')

    print(f"  가격 데이터 다운로드: {len(tickers)}종목, {min_date} ~ {end_date}")
    price_data = yf.download(tickers, start=min_date, end=end_date, progress=False)

    if price_data.empty:
        return {}

    # Close 가격 추출
    if isinstance(price_data.columns, pd.MultiIndex):
        closes = price_data['Close']
    else:
        closes = price_data[['Close']]
        closes.columns = [tickers[0]] if len(tickers) == 1 else closes.columns

    return closes


def find_death_exit_date(ticker, entry_date, df):
    """Part 2에서 탈락한 첫 날짜 찾기 (Death List 퇴장)"""
    dates = sorted(df['date'].unique())
    try:
        start_idx = dates.index(entry_date)
    except ValueError:
        return None

    for i in range(start_idx + 1, len(dates)):
        day_tickers = set(df[df['date'] == dates[i]]['ticker'])
        if ticker not in day_tickers:
            return dates[i]

    return None  # 아직 탈락 안 함


def get_price_on_date(closes, ticker, target_date, max_search_days=5):
    """특정 날짜의 종가 (거래일 아니면 다음 거래일)"""
    if closes is None or closes.empty:
        return None

    if ticker not in closes.columns:
        return None

    target = pd.Timestamp(target_date)
    for delta in range(max_search_days):
        check = target + pd.Timedelta(days=delta)
        if check in closes.index:
            price = closes.loc[check, ticker]
            if pd.notna(price) and price > 0:
                return float(price)

    return None


def run_backtest(df, entries, closes, hold_days, use_death_exit=False):
    """단일 조합 백테스트 실행"""
    results = []

    for entry in entries:
        ticker = entry['ticker']
        entry_date = entry['entry_date']
        entry_price = entry['entry_price']

        if use_death_exit:
            # Death List 퇴장: Part 2 탈락일에 매도
            death_date = find_death_exit_date(ticker, entry_date, df)
            if death_date is None:
                continue  # 아직 탈락 안 함 → 미확정
            exit_price = get_price_on_date(closes, ticker, death_date)
            actual_hold = len([d for d in sorted(df['date'].unique())
                              if entry_date < d <= death_date])
        else:
            # 고정 보유: entry_date + hold_days 거래일 후 매도
            target_date = (datetime.strptime(entry_date, '%Y-%m-%d')
                          + timedelta(days=int(hold_days * 1.5))).strftime('%Y-%m-%d')
            exit_price = get_price_on_date(closes, ticker, target_date)
            actual_hold = hold_days

        if exit_price is None or entry_price <= 0:
            continue

        ret = (exit_price - entry_price) / entry_price * 100
        results.append({
            'ticker': ticker,
            'entry_date': entry_date,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'return_pct': ret,
            'hold_days': actual_hold,
            'adj_gap': entry.get('adj_gap', 0),
        })

    return results


def run_all_combinations():
    """전체 매트릭스 실행"""
    df = load_backtest_data()
    rank_days, min_date, max_date, daily = get_data_summary()

    print("=" * 60)
    print("  EPS Momentum Backtest Framework")
    print("=" * 60)
    print(f"  DB 데이터: {rank_days}일 ({min_date} ~ {max_date})")
    for d, cnt in daily:
        print(f"    {d}: {cnt}종목")
    print()

    if rank_days < MIN_DATA_DAYS:
        print(f"  ⚠️  데이터 부족: {rank_days}일 / 최소 {MIN_DATA_DAYS}일 필요")
        print(f"  📅 예상 준비 완료: ~{MIN_DATA_DAYS - rank_days} 거래일 후")
        print()
        print("  데이터가 축적되면 다시 실행하세요: python backtest.py")
        return

    # 전체 entries에 필요한 가격 데이터 한 번에 다운로드
    all_entries = []
    for vd in VERIFY_DAYS_LIST:
        entries = find_entry_signals(df, vd)
        all_entries.extend(entries)

    if not all_entries:
        print("  진입 신호 없음")
        return

    closes = get_exit_prices(all_entries, HOLD_DAYS_LIST)

    # 매트릭스 결과 수집
    matrix = {}  # (verify_days, hold_days_or_death) → results summary

    for vd in VERIFY_DAYS_LIST:
        entries = find_entry_signals(df, vd)
        if not entries:
            continue

        print(f"\n검증 {vd}일: 진입 신호 {len(entries)}건")

        # 고정 보유
        for hd in HOLD_DAYS_LIST:
            results = run_backtest(df, entries, closes, hd, use_death_exit=False)
            if results:
                avg_ret = sum(r['return_pct'] for r in results) / len(results)
                win_rate = sum(1 for r in results if r['return_pct'] > 0) / len(results) * 100
                max_loss = min(r['return_pct'] for r in results)
                matrix[(vd, f'{hd}d')] = {
                    'avg': avg_ret, 'win': win_rate, 'n': len(results), 'max_loss': max_loss
                }

        # Death List 퇴장
        results = run_backtest(df, entries, closes, 0, use_death_exit=True)
        if results:
            avg_ret = sum(r['return_pct'] for r in results) / len(results)
            win_rate = sum(1 for r in results if r['return_pct'] > 0) / len(results) * 100
            max_loss = min(r['return_pct'] for r in results)
            avg_hold = sum(r['hold_days'] for r in results) / len(results)
            matrix[(vd, 'death')] = {
                'avg': avg_ret, 'win': win_rate, 'n': len(results),
                'max_loss': max_loss, 'avg_hold': avg_hold
            }

    # 결과 출력
    print_results(matrix)


def print_results(matrix):
    """매트릭스 결과 출력"""
    if not matrix:
        print("\n  결과 없음")
        return

    cols = [f'{hd}d' for hd in HOLD_DAYS_LIST] + ['death']
    col_labels = [f'{hd}일' for hd in HOLD_DAYS_LIST] + ['탈락퇴장']

    print("\n" + "=" * 70)
    print("  평균 수익률 매트릭스")
    print("=" * 70)

    # 헤더
    header = f"{'검증':>6s} |"
    for cl in col_labels:
        header += f" {cl:>8s} |"
    print(header)
    print("-" * 70)

    # 데이터
    for vd in VERIFY_DAYS_LIST:
        row = f"{vd:>4d}일 |"
        for col in cols:
            key = (vd, col)
            if key in matrix:
                m = matrix[key]
                row += f" {m['avg']:>+7.1f}% |"
            else:
                row += f" {'N/A':>7s} |"
        print(row)

    # 상세 통계
    print("\n" + "=" * 70)
    print("  상세 통계")
    print("=" * 70)
    print(f"{'검증':>6s} {'보유':>8s} | {'거래수':>6s} {'승률':>7s} {'최대손실':>9s}")
    print("-" * 50)

    for vd in VERIFY_DAYS_LIST:
        for col in cols:
            key = (vd, col)
            if key in matrix:
                m = matrix[key]
                hold_label = col if col != 'death' else f"탈락({m.get('avg_hold', 0):.0f}d)"
                print(f"{vd:>4d}일 {hold_label:>8s} | {m['n']:>5d}건 {m['win']:>6.1f}% {m['max_loss']:>+8.1f}%")


if __name__ == '__main__':
    run_all_combinations()
