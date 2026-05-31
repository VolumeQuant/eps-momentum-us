"""ETF Pulse 고급 시각화 — sector pie, performance line, heatmap"""
import sys
import sqlite3
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'
CHART_DIR = Path(__file__).parent / 'content' / 'charts'


def chart_category_pie(date_str=None):
    """카테고리별 AUM 분포 pie chart"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    rows = cur.execute('''
        SELECT category, SUM(aum) FROM etf_daily WHERE date=? GROUP BY category ORDER BY SUM(aum) DESC
    ''', (date_str,)).fetchall()
    conn.close()
    if not rows: return None

    cats = [r[0] for r in rows]
    aums = [r[1]/1e9 for r in rows]
    colors = plt.cm.tab10(range(len(cats)))

    fig, ax = plt.subplots(figsize=(8, 8))
    wedges, texts, autotexts = ax.pie(aums, labels=cats, autopct='%1.1f%%',
                                       colors=colors, startangle=90)
    for t in autotexts: t.set_fontsize(10)
    ax.set_title(f'ETF AUM by Category — {date_str}\n(Total: ${sum(aums):.0f}B)',
                 fontsize=14, fontweight='bold')
    plt.tight_layout()
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    out = CHART_DIR / f'category_pie_{date_str}.png'
    plt.savefig(out, dpi=100, bbox_inches='tight')
    plt.close()
    return out


def chart_etf_performance_line(tickers=['SPY', 'QQQ', 'SOXX', 'GLD', 'TLT'], lookback=30):
    """ETF 누적 수익률 라인 차트"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

    for i, tk in enumerate(tickers):
        rows = cur.execute('''
            SELECT date, day_return FROM etf_daily
            WHERE ticker=? ORDER BY date DESC LIMIT ?
        ''', (tk, lookback)).fetchall()
        if not rows: continue
        rows.reverse()
        dates = [datetime.strptime(r[0], '%Y-%m-%d') for r in rows]
        rets = [r[1] or 0 for r in rows]
        cum = [0]
        for r in rets:
            cum.append((1 + cum[-1]/100) * (1 + r/100) * 100 - 100)
        ax.plot(dates, cum[1:], label=tk, linewidth=2, color=colors[i % len(colors)])

    ax.axhline(0, color='gray', linewidth=0.5)
    ax.set_title(f'ETF {lookback}-day Cumulative Return Comparison', fontsize=14, fontweight='bold')
    ax.set_ylabel('Cumulative Return (%)')
    ax.legend(loc='best')
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    plt.xticks(rotation=45)
    plt.tight_layout()

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    out = CHART_DIR / 'performance_line.png'
    plt.savefig(out, dpi=100, bbox_inches='tight')
    plt.close()
    conn.close()
    return out


def chart_volume_heatmap(date_str=None, top_n=20):
    """거래량 spike heatmap (카테고리 × ETF)"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    # 최근 7일 거래량 spike 변동
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM etf_daily WHERE date <= ? ORDER BY date DESC LIMIT 7',
        (date_str,)).fetchall()]
    dates.reverse()
    if len(dates) < 3: return None

    # 어제 거래량 spike Top N ETF만
    top_tks = [r[0] for r in cur.execute('''
        SELECT ticker FROM etf_daily WHERE date=? AND volume_spike > 0
        ORDER BY volume_spike DESC LIMIT ?
    ''', (date_str, top_n)).fetchall()]
    if not top_tks: return None

    # 데이터 매트릭스
    matrix = []
    for tk in top_tks:
        row = []
        for d in dates:
            r = cur.execute('SELECT volume_spike FROM etf_daily WHERE ticker=? AND date=?',
                           (tk, d)).fetchone()
            row.append(r[0] if r and r[0] else 0)
        matrix.append(row)
    conn.close()

    matrix = np.array(matrix)
    fig, ax = plt.subplots(figsize=(10, top_n*0.4))
    im = ax.imshow(matrix, cmap='YlOrRd', aspect='auto', vmin=0, vmax=3)
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels([d[5:] for d in dates], rotation=45)  # MM-DD
    ax.set_yticks(range(top_n))
    ax.set_yticklabels(top_tks)
    ax.set_title(f'Volume Spike Heatmap (Top {top_n} ETFs) — {date_str}', fontsize=12, fontweight='bold')
    plt.colorbar(im, label='Volume Spike (x)')
    # 값 표시
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            if matrix[i, j] > 1.5:
                color = 'white' if matrix[i, j] > 2 else 'black'
                ax.text(j, i, f'{matrix[i,j]:.1f}', ha='center', va='center',
                       fontsize=7, color=color)
    plt.tight_layout()
    out = CHART_DIR / f'volume_heatmap_{date_str}.png'
    plt.savefig(out, dpi=100, bbox_inches='tight')
    plt.close()
    return out


def chart_all_advanced(date_str=None):
    """모든 고급 차트 생성"""
    out = []
    for fn in [chart_category_pie, chart_etf_performance_line, chart_volume_heatmap]:
        try:
            r = fn(date_str) if 'date_str' in fn.__code__.co_varnames else fn()
            if r: out.append(r)
        except Exception as e:
            print(f'  {fn.__name__} 실패: {e}')
    return out


if __name__ == '__main__':
    out = chart_all_advanced()
    print(f'생성된 고급 차트 {len(out)}개:')
    for f in out:
        print(f'  {f}')
