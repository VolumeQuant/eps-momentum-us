"""ETF Pulse 차트 — 콘텐츠에 시각 자료 추가

matplotlib로 간단 차트 생성 + PNG 저장.
이메일/Substack에 첨부.
"""
import sys
import sqlite3
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'
CHART_DIR = Path(__file__).parent / 'content' / 'charts'


def chart_category_strength(date_str=None):
    """카테고리별 평균 수익률 차트"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    rows = cur.execute('''
        SELECT category, AVG(day_return) as avg_ret, COUNT(*) as n
        FROM etf_daily WHERE date=?
        GROUP BY category ORDER BY avg_ret DESC
    ''', (date_str,)).fetchall()
    conn.close()

    if not rows: return None

    cats = [r[0] for r in rows]
    rets = [r[1] for r in rows]
    colors = ['#2ecc71' if r >= 0 else '#e74c3c' for r in rets]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(cats, rets, color=colors)
    ax.axvline(0, color='black', linewidth=0.5)
    ax.set_xlabel('Average Daily Return (%)')
    ax.set_title(f'ETF Category Strength — {date_str}', fontsize=14, fontweight='bold')
    for bar, ret in zip(bars, rets):
        ax.text(ret + (0.02 if ret >= 0 else -0.02), bar.get_y() + bar.get_height()/2,
                f'{ret:+.2f}%', va='center', ha='left' if ret >= 0 else 'right', fontsize=9)
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    out = CHART_DIR / f'category_strength_{date_str}.png'
    plt.savefig(out, dpi=100, bbox_inches='tight')
    plt.close()
    return out


def chart_top_returns(date_str=None, top_n=10):
    """수익률 Top N 차트"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    rows = cur.execute('''
        SELECT ticker, day_return, category
        FROM etf_daily WHERE date=? AND aum > 1e8
        ORDER BY day_return DESC LIMIT ?
    ''', (date_str, top_n)).fetchall()
    conn.close()

    if not rows: return None

    tickers = [r[0] for r in rows][::-1]  # 가장 큰 위쪽
    rets = [r[1] for r in rows][::-1]
    colors = ['#2ecc71' if r >= 0 else '#e74c3c' for r in rets]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(tickers, rets, color=colors)
    ax.set_xlabel('Daily Return (%)')
    ax.set_title(f'Top {top_n} ETFs by Daily Return — {date_str}', fontsize=14, fontweight='bold')
    for bar, ret in zip(bars, rets):
        ax.text(ret + 0.1, bar.get_y() + bar.get_height()/2, f'{ret:+.2f}%', va='center', fontsize=9)
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()

    out = CHART_DIR / f'top_returns_{date_str}.png'
    plt.savefig(out, dpi=100, bbox_inches='tight')
    plt.close()
    return out


def chart_momentum_5d(date_str=None, top_n=10):
    """5일 모멘텀 Top N"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    dates_5d = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM etf_daily WHERE date <= ? ORDER BY date DESC LIMIT 6', (date_str,)).fetchall()]
    if len(dates_5d) < 6:
        conn.close(); return None

    d_5d_ago = dates_5d[-1]
    rows = cur.execute('''
        SELECT t.ticker, t.price, p.price, t.aum
        FROM etf_daily t JOIN etf_daily p ON t.ticker=p.ticker
        WHERE t.date=? AND p.date=? AND t.aum > 5e8
    ''', (date_str, d_5d_ago)).fetchall()
    conn.close()

    moms = []
    for r in rows:
        tk, p_now, p_5d, aum = r
        if p_5d > 0:
            moms.append((tk, (p_now - p_5d) / p_5d * 100))
    moms.sort(key=lambda x: -x[1])
    moms = moms[:top_n]
    if not moms: return None

    tickers = [m[0] for m in moms][::-1]
    rets = [m[1] for m in moms][::-1]
    colors = ['#3498db' for _ in rets]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(tickers, rets, color=colors)
    ax.set_xlabel('5-day Return (%)')
    ax.set_title(f'Top {top_n} ETFs by 5-day Momentum — {date_str}', fontsize=14, fontweight='bold')
    for i, r in enumerate(rets):
        ax.text(r + 0.2, i, f'{r:+.2f}%', va='center', fontsize=9)
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()

    out = CHART_DIR / f'momentum_5d_{date_str}.png'
    plt.savefig(out, dpi=100, bbox_inches='tight')
    plt.close()
    return out


def chart_all(date_str=None):
    """모든 차트 생성"""
    out = []
    c1 = chart_category_strength(date_str)
    if c1: out.append(c1)
    c2 = chart_top_returns(date_str)
    if c2: out.append(c2)
    c3 = chart_momentum_5d(date_str)
    if c3: out.append(c3)
    return out


if __name__ == '__main__':
    out = chart_all()
    print(f'생성된 차트 {len(out)}개:')
    for f in out:
        print(f'  {f}')
