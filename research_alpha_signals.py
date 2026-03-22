"""추가 알파 시그널 연구 — 현재 Top30 종목 대상
yfinance에서 4가지 추가 데이터를 수집하여 종목별 알파 시그널을 분석.

시그널:
1. 내부자 매수/매도 비율 (Insider Net Buy)
2. 어닝 서프라이즈 연속 비트 (Earnings Beat Streak)
3. 공매도 비율 및 변화 (Short Interest)
4. 매출 가속도 (Revenue Acceleration)

Usage:
    python research_alpha_signals.py
"""
import sqlite3
import sys
import time
from pathlib import Path

import yfinance as yf

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'eps_momentum_data.db'


def get_top30_tickers():
    """DB에서 최신 날짜 part2_rank Top30 종목 가져오기"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    latest = c.execute(
        'SELECT MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL'
    ).fetchone()[0]
    rows = c.execute(
        'SELECT ticker, part2_rank, price, adj_gap FROM ntm_screening '
        'WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
        (latest,)
    ).fetchall()
    conn.close()
    print(f'기준일: {latest}, Top30: {len(rows)}종목\n')
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def analyze_insider(stock, ticker):
    """내부자 매매 분석"""
    try:
        purchases = stock.insider_purchases
        if purchases is not None and len(purchases) > 0:
            # insider_purchases: Purchases, Sales, Net Shares Purchased 등
            data = {}
            for _, row in purchases.iterrows():
                idx = row.iloc[0] if len(row) > 0 else ''
                val = row.iloc[1] if len(row) > 1 else None
                data[idx] = val
            net_shares = data.get('Net Shares Purchased (Sold)', 0)
            total = data.get('Total Insider Shares Held', 1)
            pct = data.get('% Net Shares Purchased (Sold)', 0)
            return {
                'net_shares': net_shares,
                'pct_change': pct,
                'signal': 'BUY' if (pct and float(str(pct).replace('%', '')) > 0) else 'SELL' if pct else 'NEUTRAL'
            }
    except Exception:
        pass
    return {'net_shares': 0, 'pct_change': 0, 'signal': 'N/A'}


def analyze_earnings_surprise(stock, ticker):
    """어닝 서프라이즈 분석 — 연속 비트 횟수"""
    try:
        eh = stock.earnings_history
        if eh is not None and len(eh) > 0:
            surprises = eh['surprisePercent'].dropna().tolist()
            # 최근부터 연속 비트 카운트
            streak = 0
            for s in surprises:
                if s > 0:
                    streak += 1
                else:
                    break
            avg_surprise = sum(surprises) / len(surprises) if surprises else 0
            return {
                'beat_streak': streak,
                'avg_surprise_pct': avg_surprise,
                'last_4q': surprises[:4],
                'signal': 'STRONG' if streak >= 4 else 'GOOD' if streak >= 2 else 'WEAK'
            }
    except Exception:
        pass
    return {'beat_streak': 0, 'avg_surprise_pct': 0, 'last_4q': [], 'signal': 'N/A'}


def analyze_short_interest(info, ticker):
    """공매도 비율 분석"""
    try:
        short_pct = info.get('shortPercentOfFloat', 0) or 0
        short_ratio = info.get('shortRatio', 0) or 0  # days to cover
        short_prior = info.get('sharesShortPriorMonth', 0) or 0
        short_now = info.get('sharesShort', 0) or 0
        mom = 0
        if short_prior > 0:
            mom = (short_now - short_prior) / short_prior * 100

        # 공매도 많은데 감소 중 = 숏커버 (긍정)
        # 공매도 많고 증가 중 = 부정
        if short_pct > 0.10 and mom < -10:
            signal = 'SQUEEZE'
        elif short_pct > 0.15:
            signal = 'HIGH_SHORT'
        elif short_pct < 0.03:
            signal = 'LOW'
        else:
            signal = 'NORMAL'

        return {
            'short_pct_float': short_pct * 100,
            'short_ratio_days': short_ratio,
            'mom_pct': mom,
            'signal': signal
        }
    except Exception:
        pass
    return {'short_pct_float': 0, 'short_ratio_days': 0, 'mom_pct': 0, 'signal': 'N/A'}


def analyze_revenue_accel(stock, ticker):
    """매출 가속도 분석 — QoQ YoY 성장률의 변화"""
    try:
        qi = stock.quarterly_income_stmt
        if qi is not None and qi.shape[1] >= 4:
            rev_row = None
            for label in ['Total Revenue', 'Operating Revenue', 'Revenue']:
                if label in qi.index:
                    rev_row = qi.loc[label]
                    break
            if rev_row is not None:
                vals = rev_row.dropna().sort_index().tolist()
                if len(vals) >= 4:
                    # YoY 성장률 계산 (최근 2개 분기의 YoY)
                    # vals[-1] = 최근, vals[-5] = 4분기 전 (1년 전)
                    recent_yoy = (vals[-1] - vals[-4]) / abs(vals[-4]) * 100 if vals[-4] != 0 else 0
                    prev_yoy = None
                    if len(vals) >= 5:
                        prev_yoy = (vals[-2] - vals[-5]) / abs(vals[-5]) * 100 if vals[-5] != 0 else 0

                    accel = None
                    if prev_yoy is not None:
                        accel = recent_yoy - prev_yoy

                    return {
                        'recent_yoy': recent_yoy,
                        'prev_yoy': prev_yoy,
                        'acceleration': accel,
                        'signal': 'ACCEL' if accel and accel > 5 else 'DECEL' if accel and accel < -5 else 'STABLE'
                    }
    except Exception:
        pass
    return {'recent_yoy': 0, 'prev_yoy': None, 'acceleration': None, 'signal': 'N/A'}


def main():
    tickers_data = get_top30_tickers()

    print('=' * 100)
    print('  추가 알파 시그널 분석 — Top30 종목')
    print('=' * 100)
    print()

    results = []
    for ticker, rank, price, adj_gap in tickers_data:
        sys.stdout.write(f'  {ticker:6s} (rank {rank:2d}) 수집중...')
        sys.stdout.flush()
        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}

            insider = analyze_insider(stock, ticker)
            earnings = analyze_earnings_surprise(stock, ticker)
            short = analyze_short_interest(info, ticker)
            revenue = analyze_revenue_accel(stock, ticker)

            results.append({
                'ticker': ticker, 'rank': rank, 'price': price, 'adj_gap': adj_gap,
                'insider': insider, 'earnings': earnings,
                'short': short, 'revenue': revenue
            })
            print(f' OK')
            time.sleep(0.3)
        except Exception as e:
            print(f' ERROR: {e}')
            results.append({
                'ticker': ticker, 'rank': rank, 'price': price, 'adj_gap': adj_gap,
                'insider': {'signal': 'ERR'}, 'earnings': {'signal': 'ERR'},
                'short': {'signal': 'ERR'}, 'revenue': {'signal': 'ERR'}
            })

    # 결과 테이블 출력
    print()
    print('=' * 110)
    print(f'{"종목":>6} {"순위":>4} {"adj_gap":>8} '
          f'{"내부자":>8} {"어닝비트":>8} {"서프%":>7} '
          f'{"공매도%":>7} {"공매도MoM":>9} '
          f'{"매출YoY":>8} {"가속도":>7}')
    print('-' * 110)

    for r in results:
        ei = r['earnings']
        si = r['short']
        rv = r['revenue']
        ins = r['insider']

        beat = f'{ei.get("beat_streak", 0)}연속' if ei.get('beat_streak', 0) > 0 else '-'
        surp = f'{ei.get("avg_surprise_pct", 0):+.1f}' if ei.get('avg_surprise_pct') else '-'
        sp = f'{si.get("short_pct_float", 0):.1f}%' if si.get('short_pct_float') else '-'
        sm = f'{si.get("mom_pct", 0):+.0f}%' if si.get('mom_pct') else '-'
        ry = f'{rv.get("recent_yoy", 0):+.0f}%' if rv.get('recent_yoy') else '-'
        ra = f'{rv.get("acceleration", 0):+.0f}' if rv.get('acceleration') is not None else '-'

        print(f'{r["ticker"]:>6} {r["rank"]:>4} {r["adj_gap"]:>+8.2f} '
              f'{ins.get("signal", "N/A"):>8} {beat:>8} {surp:>7} '
              f'{sp:>7} {sm:>9} '
              f'{ry:>8} {ra:>7}')

    # 알파 시그널 강도별 종목 분류
    print()
    print('=' * 80)
    print('  시그널 강도별 분류')
    print('=' * 80)

    # 4연속 어닝 비트
    strong_earnings = [r for r in results if r['earnings'].get('beat_streak', 0) >= 4]
    if strong_earnings:
        print(f'\n  어닝 4연속 비트: {", ".join(r["ticker"] for r in strong_earnings)}')

    # 숏스퀴즈 후보
    squeeze = [r for r in results if r['short'].get('signal') == 'SQUEEZE']
    if squeeze:
        print(f'  숏스퀴즈 후보: {", ".join(r["ticker"] for r in squeeze)}')

    # 공매도 높은 종목
    high_short = [r for r in results if r['short'].get('short_pct_float', 0) > 10]
    if high_short:
        print(f'  공매도 >10%: {", ".join(f"{r["ticker"]}({r["short"]["short_pct_float"]:.1f}%)" for r in high_short)}')

    # 매출 가속
    accel = [r for r in results if r['revenue'].get('signal') == 'ACCEL']
    if accel:
        print(f'  매출 가속 중: {", ".join(r["ticker"] for r in accel)}')

    # 내부자 매수
    insider_buy = [r for r in results if r['insider'].get('signal') == 'BUY']
    if insider_buy:
        print(f'  내부자 매수: {", ".join(r["ticker"] for r in insider_buy)}')

    # 복합 강도 (2개 이상 긍정 시그널)
    print(f'\n  복합 시그널 (2개+ 긍정):')
    for r in results:
        score = 0
        reasons = []
        if r['earnings'].get('beat_streak', 0) >= 3:
            score += 1
            reasons.append(f'어닝{r["earnings"]["beat_streak"]}연속비트')
        if r['short'].get('signal') in ('SQUEEZE', 'LOW'):
            score += 1
            reasons.append(f'공매도{r["short"]["signal"]}')
        if r['revenue'].get('signal') == 'ACCEL':
            score += 1
            reasons.append('매출가속')
        if r['insider'].get('signal') == 'BUY':
            score += 1
            reasons.append('내부자매수')
        if score >= 2:
            print(f'    {r["ticker"]:6s} (rank {r["rank"]:2d}): {" + ".join(reasons)}')

    print()


if __name__ == '__main__':
    main()
