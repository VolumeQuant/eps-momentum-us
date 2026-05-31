"""ETF Pulse 포트폴리오 추적 — paid 기능 prototype

사용자가 보유 ETF 등록 → 매일 변동 + 알림 + 추천 alternatives
"""
import sys
import sqlite3
import json
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def init_portfolio_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS user_portfolios (
            user_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            shares REAL,
            entry_price REAL,
            entry_date TEXT,
            note TEXT,
            PRIMARY KEY (user_id, ticker)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS user_alerts (
            user_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            alert_type TEXT,         -- 'price_drop', 'new_high', 'volume_spike', etc
            threshold REAL,
            triggered_at TEXT,
            PRIMARY KEY (user_id, ticker, alert_type)
        )
    ''')
    conn.commit()
    conn.close()


def add_holding(user_id, ticker, shares, entry_price=None, entry_date=None, note=''):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if entry_date is None:
        entry_date = datetime.now().strftime('%Y-%m-%d')
    if entry_price is None:
        # 최근 가격 사용
        r = cur.execute('SELECT price FROM etf_daily WHERE ticker=? ORDER BY date DESC LIMIT 1', (ticker,)).fetchone()
        entry_price = r[0] if r else 0
    cur.execute('''
        INSERT OR REPLACE INTO user_portfolios (user_id, ticker, shares, entry_price, entry_date, note)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (user_id, ticker, shares, entry_price, entry_date, note))
    conn.commit()
    conn.close()


def get_portfolio_pulse(user_id, date_str=None):
    """포트폴리오 일일 펄스 — 보유 종목별 변동 + 알림"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if date_str is None:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    # 보유 종목 + 어제/오늘 가격
    holdings = cur.execute('''
        SELECT p.ticker, p.shares, p.entry_price, p.entry_date, p.note,
               d.price, d.day_return, d.volume_spike, d.category
        FROM user_portfolios p
        LEFT JOIN etf_daily d ON p.ticker=d.ticker AND d.date=?
        WHERE p.user_id=?
    ''', (date_str, user_id)).fetchall()

    pulse = {'date': date_str, 'user_id': user_id, 'holdings': [], 'total_value': 0, 'total_pnl_pct': 0}
    total_value_today = 0
    total_value_entry = 0

    for h in holdings:
        tk, shares, ep, ed, note, p_today, ret_today, spike, cat = h
        if p_today is None:
            p_today = ep  # fallback
            ret_today = 0
        value_today = shares * p_today
        value_entry = shares * ep
        total_value_today += value_today
        total_value_entry += value_entry
        pnl_pct = (p_today - ep) / ep * 100 if ep > 0 else 0
        pulse['holdings'].append({
            'ticker': tk, 'category': cat,
            'shares': shares, 'entry_price': ep, 'entry_date': ed,
            'current_price': p_today, 'day_return': ret_today or 0,
            'pnl_pct': pnl_pct, 'value_today': value_today,
            'volume_spike': spike or 0, 'note': note,
        })

    pulse['total_value'] = total_value_today
    pulse['total_pnl_pct'] = (total_value_today - total_value_entry) / total_value_entry * 100 if total_value_entry > 0 else 0

    # 카테고리별 노출 분석
    cat_exposure = {}
    for h in pulse['holdings']:
        cat = h['category'] or 'unknown'
        cat_exposure[cat] = cat_exposure.get(cat, 0) + h['value_today']
    pulse['category_exposure'] = {
        k: (v, v/total_value_today*100 if total_value_today > 0 else 0)
        for k, v in cat_exposure.items()
    }

    # alternatives 추천 (같은 카테고리 better 점수)
    pulse['alternatives'] = {}
    for h in pulse['holdings']:
        cat = h['category']
        if not cat: continue
        # 같은 카테고리에서 AUM/expense 좋은 ETF
        alts = cur.execute('''
            SELECT ticker, aum, expense_ratio, day_return
            FROM etf_daily WHERE date=? AND category=? AND ticker != ?
            ORDER BY aum DESC LIMIT 3
        ''', (date_str, cat, h['ticker'])).fetchall()
        if alts:
            pulse['alternatives'][h['ticker']] = [
                {'ticker': a[0], 'aum_b': (a[1] or 0)/1e9, 'expense': a[2] or 0, 'day_return': a[3] or 0}
                for a in alts
            ]

    conn.close()
    return pulse


def gen_pulse_message(pulse):
    """포트폴리오 펄스 → Markdown"""
    lines = []
    lines.append(f'# 💼 내 포트폴리오 펄스 — {pulse["date"]}')
    lines.append('')
    lines.append(f'**총 가치**: ${pulse["total_value"]:,.0f}')
    lines.append(f'**누적 수익률**: {pulse["total_pnl_pct"]:+.2f}%')
    lines.append('')

    lines.append('## 📊 보유 종목 (변동순)')
    lines.append('')
    sorted_h = sorted(pulse['holdings'], key=lambda x: -abs(x['day_return']))
    for h in sorted_h:
        emoji = '🟢' if h['day_return'] > 0.5 else '🔴' if h['day_return'] < -0.5 else '⚪'
        spike_note = f' 거래량 {h["volume_spike"]:.1f}x' if h['volume_spike'] > 1.5 else ''
        lines.append(f'- {emoji} **{h["ticker"]}** ({h["category"]}): '
                     f'{h["day_return"]:+.2f}% (어제), 누적 {h["pnl_pct"]:+.2f}%{spike_note}')
        lines.append(f'  └ {h["shares"]:.2f}주 × ${h["current_price"]:.2f} = ${h["value_today"]:,.0f}')

    lines.append('')
    lines.append('## 🎯 카테고리 노출')
    lines.append('')
    cat_sorted = sorted(pulse['category_exposure'].items(), key=lambda x: -x[1][1])
    for cat, (val, pct) in cat_sorted:
        bar = '█' * int(pct / 5)
        lines.append(f'- {cat}: {pct:.1f}%  {bar}')

    if pulse['alternatives']:
        lines.append('')
        lines.append('## 🔍 alternatives (참고)')
        lines.append('')
        lines.append('같은 카테고리 AUM 상위 ETF — 수수료/유동성 더 좋을 수 있음:')
        lines.append('')
        for tk, alts in pulse['alternatives'].items():
            alt_str = ', '.join([f'{a["ticker"]} (AUM ${a["aum_b"]:.1f}B)' for a in alts])
            lines.append(f'- **{tk}**: {alt_str}')

    lines.append('')
    lines.append('---')
    lines.append('')
    lines.append('_ETF Pulse 포트폴리오 — 자동 추적. 투자 추천 아님._')

    return '\n'.join(lines)


def demo():
    """샘플 포트폴리오 — 사용자의 KR/US 시스템과 자연스럽게 결합"""
    init_portfolio_tables()
    user = 'demo_user'

    # 샘플 포트폴리오 (사용자 상황 가정)
    add_holding(user, 'VOO', 100, entry_date='2026-04-01')
    add_holding(user, 'QQQ', 50, entry_date='2026-04-15')
    add_holding(user, 'SOXX', 30, entry_date='2026-05-01')
    add_holding(user, 'GLD', 20, entry_date='2026-04-01')
    add_holding(user, 'JEPI', 40, entry_date='2026-03-01')

    pulse = get_portfolio_pulse(user)
    msg = gen_pulse_message(pulse)
    print(msg)

    # 저장
    out_dir = Path(__file__).parent / 'content'
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f'portfolio_demo_{pulse["date"]}.md'
    out_file.write_text(msg, encoding='utf-8')
    print(f'\n저장: {out_file}')


if __name__ == '__main__':
    demo()
