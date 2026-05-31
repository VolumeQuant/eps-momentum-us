"""ETF Pulse Daily fetch — yfinance 일괄 수집

매일 미국 시장 마감 후 (한국 새벽) cron 실행:
1. 가격, 거래량, AUM, 수익률 → etf_daily
2. Top holdings (10개) → etf_holdings_daily
3. 어제 vs 오늘 holdings diff → etf_holdings_changes
4. fund flow 추정 (AUM diff - 가격수익률) → etf_daily.estimated_flow
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


def fetch_etf_basic(ticker, category):
    """단일 ETF basic 데이터 fetch"""
    t = yf.Ticker(ticker)
    try:
        info = t.info
        hist = t.history(period='40d')['Close'].dropna()
        vol_hist = t.history(period='40d')['Volume'].dropna()
        if len(hist) < 2:
            return None
        price = float(hist.iloc[-1])
        prev = float(hist.iloc[-2])
        day_ret = (price - prev) / prev * 100 if prev > 0 else 0
        vol_today = int(vol_hist.iloc[-1]) if len(vol_hist) > 0 else 0
        vol_avg = int(vol_hist.iloc[:-1].tail(30).mean()) if len(vol_hist) > 1 else 0
        vol_spike = vol_today / vol_avg if vol_avg > 0 else 0
        return {
            'ticker': ticker,
            'category': category,
            'price': price,
            'volume': vol_today,
            'avg_volume_30d': vol_avg,
            'volume_spike': vol_spike,
            'aum': info.get('totalAssets', 0) or 0,
            'day_return': day_ret,
            'expense_ratio': (info.get('netExpenseRatio') or info.get('annualReportExpenseRatio') or 0) / 100,  # yfinance가 %로 줌 → 0~1 범위
            'dividend_yield': (info.get('yield') or 0) / 100 if info.get('yield') else 0,
            'beta': info.get('beta3Year') or info.get('beta') or 0,
            'date_str': hist.index[-1].strftime('%Y-%m-%d'),
        }
    except Exception as e:
        return {'ticker': ticker, 'error': str(e)[:100]}


def fetch_holdings(ticker):
    """단일 ETF top holdings fetch"""
    t = yf.Ticker(ticker)
    try:
        fd = t.funds_data
        top = fd.top_holdings
        if top is None or len(top) == 0:
            return []
        out = []
        for i, (sym, row) in enumerate(top.head(10).iterrows(), 1):
            out.append({
                'holding_ticker': sym,
                'holding_name': row.get('Name', ''),
                'weight': float(row.get('Holding Percent', 0)),
                'rank': i,
            })
        return out
    except Exception:
        return []


def save_daily(conn, date_str, snapshots):
    """etf_daily에 저장"""
    cur = conn.cursor()
    for s in snapshots:
        if 'error' in s:
            continue
        cur.execute('''
            INSERT INTO etf_daily (date, ticker, category, price, volume, avg_volume_30d,
                                    volume_spike, aum, day_return, expense_ratio, dividend_yield, beta)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, ticker) DO UPDATE SET
                category=excluded.category, price=excluded.price, volume=excluded.volume,
                avg_volume_30d=excluded.avg_volume_30d, volume_spike=excluded.volume_spike,
                aum=excluded.aum, day_return=excluded.day_return,
                expense_ratio=excluded.expense_ratio, dividend_yield=excluded.dividend_yield, beta=excluded.beta
        ''', (date_str, s['ticker'], s['category'], s['price'], s['volume'], s['avg_volume_30d'],
              s['volume_spike'], s['aum'], s['day_return'], s['expense_ratio'], s['dividend_yield'], s['beta']))
    conn.commit()


def save_holdings(conn, date_str, etf_ticker, holdings):
    """etf_holdings_daily에 저장"""
    cur = conn.cursor()
    # 기존 삭제
    cur.execute('DELETE FROM etf_holdings_daily WHERE date=? AND etf_ticker=?', (date_str, etf_ticker))
    for h in holdings:
        cur.execute('''
            INSERT INTO etf_holdings_daily (date, etf_ticker, holding_ticker, holding_name, weight, rank)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (date_str, etf_ticker, h['holding_ticker'], h['holding_name'], h['weight'], h['rank']))
    conn.commit()


def compute_diffs(conn, today_str):
    """어제 vs 오늘 holdings diff 계산 + etf_holdings_changes 저장
       fund flow (AUM diff - 가격수익률) 계산 + etf_daily 업데이트"""
    cur = conn.cursor()

    # 1. 어제 날짜
    yest = cur.execute('SELECT MAX(date) FROM etf_daily WHERE date < ?', (today_str,)).fetchone()[0]
    if not yest:
        print(f'  diff 계산 skip (어제 데이터 없음)')
        return 0, 0

    # 2. fund flow 추정 (AUM diff에서 가격수익률 효과 제거)
    flow_count = 0
    rows = cur.execute('''
        SELECT t.ticker, t.aum, t.day_return, y.aum
        FROM etf_daily t JOIN etf_daily y ON t.ticker=y.ticker
        WHERE t.date=? AND y.date=?
    ''', (today_str, yest)).fetchall()
    for tk, aum_today, ret_today, aum_yest in rows:
        if aum_yest > 0 and aum_today > 0:
            expected = aum_yest * (1 + (ret_today or 0)/100)
            flow = aum_today - expected
            cur.execute('UPDATE etf_daily SET estimated_flow=? WHERE date=? AND ticker=?',
                       (flow, today_str, tk))
            flow_count += 1

    # 3. holdings diff
    diff_count = 0
    etfs_today = [r[0] for r in cur.execute(
        'SELECT DISTINCT etf_ticker FROM etf_holdings_daily WHERE date=?', (today_str,)).fetchall()]
    for etf_tk in etfs_today:
        today_h = {r[0]: r[1] for r in cur.execute(
            'SELECT holding_ticker, weight FROM etf_holdings_daily WHERE date=? AND etf_ticker=?',
            (today_str, etf_tk)).fetchall()}
        yest_h = {r[0]: r[1] for r in cur.execute(
            'SELECT holding_ticker, weight FROM etf_holdings_daily WHERE date=? AND etf_ticker=?',
            (yest, etf_tk)).fetchall()}
        if not yest_h:
            continue
        # NEW (오늘만 있음)
        for tk, w in today_h.items():
            if tk not in yest_h:
                cur.execute('''INSERT OR REPLACE INTO etf_holdings_changes
                               (date, etf_ticker, holding_ticker, change_type, old_weight, new_weight, weight_delta)
                               VALUES (?, ?, ?, 'NEW', NULL, ?, ?)''',
                           (today_str, etf_tk, tk, w, w))
                diff_count += 1
        # EXIT (어제만 있음)
        for tk, w in yest_h.items():
            if tk not in today_h:
                cur.execute('''INSERT OR REPLACE INTO etf_holdings_changes
                               (date, etf_ticker, holding_ticker, change_type, old_weight, new_weight, weight_delta)
                               VALUES (?, ?, ?, 'EXIT', ?, NULL, ?)''',
                           (today_str, etf_tk, tk, w, -w))
                diff_count += 1
        # INCREASE / DECREASE (둘 다 있음)
        for tk, w_today in today_h.items():
            w_yest = yest_h.get(tk)
            if w_yest is not None:
                delta = w_today - w_yest
                if abs(delta) > 0.0005:  # 0.05%p 이상 변동만
                    ct = 'INCREASE' if delta > 0 else 'DECREASE'
                    cur.execute('''INSERT OR REPLACE INTO etf_holdings_changes
                                   (date, etf_ticker, holding_ticker, change_type, old_weight, new_weight, weight_delta)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)''',
                               (today_str, etf_tk, tk, ct, w_yest, w_today, delta))
                    diff_count += 1
    conn.commit()
    return flow_count, diff_count


def main():
    print('=' * 80)
    print('ETF Pulse Daily Fetch')
    print('=' * 80)

    etfs = get_all_etfs()
    print(f'\n대상 ETF: {len(etfs)}개')

    conn = sqlite3.connect(DB_PATH)
    snapshots = []
    holdings_data = {}
    errors = []

    t0 = time.time()
    for i, (tk, cat) in enumerate(etfs, 1):
        snap = fetch_etf_basic(tk, cat)
        if snap is None:
            errors.append(tk)
            continue
        snapshots.append(snap)
        if 'error' in snap:
            errors.append(f"{tk}: {snap['error'][:50]}")
            continue
        # holdings 별도 fetch
        h = fetch_holdings(tk)
        if h:
            holdings_data[tk] = h
        if i % 30 == 0:
            print(f'  진행 {i}/{len(etfs)} ({time.time()-t0:.0f}s)')

    print(f'\nfetch 완료: {time.time()-t0:.0f}s')
    print(f'  성공: {len([s for s in snapshots if "error" not in s])}')
    print(f'  실패: {len(errors)}')
    print(f'  holdings 확보: {len(holdings_data)}')

    # 가장 최근 거래일 (실제 yfinance가 반환한 날짜)
    today_str = None
    for s in snapshots:
        if 'error' not in s:
            today_str = s.get('date_str')
            break
    if not today_str:
        print('  유효한 데이터 0 → 종료')
        return
    print(f'\n저장 일자: {today_str}')

    # 저장
    save_daily(conn, today_str, snapshots)
    for etf_tk, h in holdings_data.items():
        save_holdings(conn, today_str, etf_tk, h)

    # diff 계산 (어제 데이터 있을 때만)
    flow_n, diff_n = compute_diffs(conn, today_str)
    print(f'fund flow 계산: {flow_n}건')
    print(f'holdings diff: {diff_n}건')

    # 요약
    cur = conn.cursor()
    n = cur.execute('SELECT COUNT(*) FROM etf_daily WHERE date=?', (today_str,)).fetchone()[0]
    print(f'\n[etf_daily {today_str}]: {n}개')

    # Top 5 거래량 spike
    print('\n[거래량 spike Top 5]:')
    for r in cur.execute('''
        SELECT ticker, category, volume_spike, day_return, aum
        FROM etf_daily WHERE date=? AND volume_spike > 0
        ORDER BY volume_spike DESC LIMIT 5
    ''', (today_str,)).fetchall():
        print(f'  {r[0]:<8} ({r[1]:<15}) spike {r[2]:.2f}x  ret {r[3]:+6.2f}%  AUM ${r[4]/1e9:.1f}B')

    # Top 5 day_return
    print('\n[수익률 Top 5]:')
    for r in cur.execute('''
        SELECT ticker, category, day_return, aum FROM etf_daily WHERE date=?
        ORDER BY day_return DESC LIMIT 5
    ''', (today_str,)).fetchall():
        print(f'  {r[0]:<8} ({r[1]:<15}) {r[2]:+6.2f}%  AUM ${r[3]/1e9:.1f}B')

    conn.close()
    print(f'\n총 소요: {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
