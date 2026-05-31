"""ETF Pulse 신호 분석 — 매일 핵심 신호 추출

신호 종류:
  1. 거래량 spike (vs 30일 평균)
  2. 가격 모멘텀 (1일 / 5일 / 30일)
  3. AUM diff → fund flow (cron 누적 후)
  4. 카테고리별 강도 (카테고리 평균 수익률)
  5. 카테고리 회전 (어제 vs 오늘 카테고리 순위 변동)
  6. 신고가/신저가 (30일)
"""
import sys
import sqlite3
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def get_signals(date_str=None, top_n=5):
    """date_str의 핵심 신호 dict 반환"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if date_str is None:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    # 어제
    prev_date = cur.execute('SELECT MAX(date) FROM etf_daily WHERE date < ?', (date_str,)).fetchone()[0]

    # ━━━ 1. 거래량 spike Top N ━━━
    volume_spikes = []
    for r in cur.execute('''
        SELECT ticker, category, volume_spike, day_return, aum, price
        FROM etf_daily WHERE date=? AND volume_spike > 1.5
        ORDER BY volume_spike DESC LIMIT ?
    ''', (date_str, top_n)).fetchall():
        volume_spikes.append({
            'ticker': r[0], 'category': r[1], 'spike': r[2],
            'day_return': r[3], 'aum_b': r[4]/1e9, 'price': r[5],
        })

    # ━━━ 2. 수익률 Top N + Bottom N ━━━
    top_returns = []
    for r in cur.execute('''
        SELECT ticker, category, day_return, volume_spike, aum
        FROM etf_daily WHERE date=? AND aum > 1e8
        ORDER BY day_return DESC LIMIT ?
    ''', (date_str, top_n)).fetchall():
        top_returns.append({
            'ticker': r[0], 'category': r[1], 'day_return': r[2],
            'spike': r[3], 'aum_b': r[4]/1e9,
        })
    bottom_returns = []
    for r in cur.execute('''
        SELECT ticker, category, day_return, volume_spike, aum
        FROM etf_daily WHERE date=? AND aum > 1e8
        ORDER BY day_return ASC LIMIT ?
    ''', (date_str, top_n)).fetchall():
        bottom_returns.append({
            'ticker': r[0], 'category': r[1], 'day_return': r[2],
            'spike': r[3], 'aum_b': r[4]/1e9,
        })

    # ━━━ 3. 카테고리별 평균 수익률 (강한/약한 테마) ━━━
    cat_stats = defaultdict(lambda: {'returns': [], 'spikes': []})
    for r in cur.execute('''
        SELECT category, day_return, volume_spike FROM etf_daily WHERE date=?
    ''', (date_str,)).fetchall():
        cat_stats[r[0]]['returns'].append(r[1] or 0)
        cat_stats[r[0]]['spikes'].append(r[2] or 0)
    category_strength = []
    for cat, data in cat_stats.items():
        if len(data['returns']) >= 3:
            avg_ret = sum(data['returns']) / len(data['returns'])
            avg_spike = sum(data['spikes']) / len(data['spikes'])
            category_strength.append({
                'category': cat,
                'n_etfs': len(data['returns']),
                'avg_return': avg_ret,
                'avg_spike': avg_spike,
            })
    category_strength.sort(key=lambda x: -x['avg_return'])

    # ━━━ 4. 신고가 / 신저가 (30일) ━━━
    new_highs = []
    new_lows = []
    rows = cur.execute('''
        SELECT ticker, MAX(price) as max_p, MIN(price) as min_p
        FROM etf_daily WHERE date >= date(?, '-30 days')
        GROUP BY ticker
    ''', (date_str,)).fetchall()
    max_p_map = {r[0]: r[1] for r in rows}
    min_p_map = {r[0]: r[2] for r in rows}
    for r in cur.execute('SELECT ticker, category, price, aum FROM etf_daily WHERE date=? AND aum > 5e8', (date_str,)).fetchall():
        tk, cat, p, aum = r
        if max_p_map.get(tk) and p >= max_p_map[tk] * 0.999:
            new_highs.append({'ticker': tk, 'category': cat, 'price': p, 'aum_b': aum/1e9})
        if min_p_map.get(tk) and p <= min_p_map[tk] * 1.001:
            new_lows.append({'ticker': tk, 'category': cat, 'price': p, 'aum_b': aum/1e9})
    new_highs = new_highs[:top_n]
    new_lows = new_lows[:top_n]

    # ━━━ 5. 5일 모멘텀 Top N ━━━
    momentum_5d = []
    if prev_date:
        # 5거래일 전 가격
        dates_5d = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM etf_daily WHERE date <= ? ORDER BY date DESC LIMIT 6',
            (date_str,)).fetchall()]
        if len(dates_5d) >= 6:
            d_5d_ago = dates_5d[-1]
            rows = cur.execute('''
                SELECT t.ticker, t.category, t.price, p.price, t.aum
                FROM etf_daily t JOIN etf_daily p ON t.ticker=p.ticker
                WHERE t.date=? AND p.date=? AND t.aum > 5e8
            ''', (date_str, d_5d_ago)).fetchall()
            for r in rows:
                tk, cat, p_now, p_5d, aum = r
                if p_5d > 0:
                    ret_5d = (p_now - p_5d) / p_5d * 100
                    momentum_5d.append({
                        'ticker': tk, 'category': cat, 'return_5d': ret_5d, 'aum_b': aum/1e9
                    })
            momentum_5d.sort(key=lambda x: -x['return_5d'])
            momentum_5d = momentum_5d[:top_n]

    # ━━━ 6. fund flow (AUM diff, 가능한 경우) ━━━
    fund_flows = []
    rows = cur.execute('''
        SELECT ticker, category, estimated_flow, day_return, aum
        FROM etf_daily WHERE date=? AND estimated_flow IS NOT NULL
        ORDER BY ABS(estimated_flow) DESC LIMIT ?
    ''', (date_str, top_n*2)).fetchall()
    for r in rows:
        fund_flows.append({
            'ticker': r[0], 'category': r[1], 'flow_m': r[2]/1e6,
            'day_return': r[3], 'aum_b': r[4]/1e9,
        })

    conn.close()

    return {
        'date': date_str,
        'prev_date': prev_date,
        'volume_spikes': volume_spikes,
        'top_returns': top_returns,
        'bottom_returns': bottom_returns,
        'category_strength': category_strength,
        'new_highs': new_highs,
        'new_lows': new_lows,
        'momentum_5d': momentum_5d,
        'fund_flows': fund_flows,
    }


def print_signals(signals):
    """signals를 보기 좋게 출력"""
    print(f'\n{"="*80}')
    print(f'★ ETF Pulse 신호 — {signals["date"]}')
    print(f'{"="*80}')

    print('\n📈 수익률 Top 5')
    for s in signals['top_returns']:
        print(f'  {s["ticker"]:<8} ({s["category"]:<15}) {s["day_return"]:+6.2f}%  spike {s["spike"]:.2f}x  AUM ${s["aum_b"]:.1f}B')

    print('\n📉 수익률 Bottom 5')
    for s in signals['bottom_returns']:
        print(f'  {s["ticker"]:<8} ({s["category"]:<15}) {s["day_return"]:+6.2f}%  spike {s["spike"]:.2f}x  AUM ${s["aum_b"]:.1f}B')

    print('\n🔥 거래량 spike Top 5')
    for s in signals['volume_spikes']:
        print(f'  {s["ticker"]:<8} ({s["category"]:<15}) {s["spike"]:>5.2f}x  ret {s["day_return"]:+6.2f}%  AUM ${s["aum_b"]:.1f}B')

    print('\n🚀 5일 모멘텀 Top 5')
    for s in signals['momentum_5d']:
        print(f'  {s["ticker"]:<8} ({s["category"]:<15}) {s["return_5d"]:+6.2f}% (5d)  AUM ${s["aum_b"]:.1f}B')

    print('\n🎯 카테고리 강도 (어제 평균 수익률 순)')
    for c in signals['category_strength']:
        bar = '★' * max(1, int(abs(c['avg_return']) * 3))
        sign = '+' if c['avg_return'] >= 0 else ''
        print(f'  {c["category"]:<18} n={c["n_etfs"]:>2}  {sign}{c["avg_return"]:.2f}%  spike {c["avg_spike"]:.2f}x  {bar}')

    if signals['new_highs']:
        print('\n🏔️ 30일 신고가')
        for h in signals['new_highs'][:5]:
            print(f'  {h["ticker"]:<8} ({h["category"]:<15}) ${h["price"]:.2f}  AUM ${h["aum_b"]:.1f}B')

    if signals['new_lows']:
        print('\n🏞️ 30일 신저가')
        for h in signals['new_lows'][:5]:
            print(f'  {h["ticker"]:<8} ({h["category"]:<15}) ${h["price"]:.2f}  AUM ${h["aum_b"]:.1f}B')

    if signals['fund_flows']:
        print('\n💰 자금 흐름 Top 10 (AUM diff)')
        for f in signals['fund_flows']:
            print(f'  {f["ticker"]:<8} ({f["category"]:<15}) ${f["flow_m"]:+.1f}M  ret {f["day_return"]:+6.2f}%')
    else:
        print('\n💰 자금 흐름: backfill 데이터라 X (실제 cron 누적 후 가능)')


if __name__ == '__main__':
    signals = get_signals()
    print_signals(signals)
