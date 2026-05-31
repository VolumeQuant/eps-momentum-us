"""ETF Pulse Intent-Based Best — 사용자 의도별 personalized best 추천

같은 카테고리도 사용자 의도에 따라 다른 best:
- long_hold: 운용보수 + AUM 안정성 (장기 holding)
- short_trade: 거래량 + 유동성 (단기 트레이딩)
- dividend: 배당률 + 안정성 (배당 우선)
- options: 옵션 거래량 + 유동성 (옵션 거래)
- small_account: 가격 낮음 (소액 적립)
- momentum: 최근 5-30일 모멘텀 (트렌드 추종)
"""
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from compare import COMPARE_GROUPS

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def get_etfs_with_momentum(tickers, date_str, lookback=5):
    """ETF 데이터 + N일 모멘텀 추가"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # N일 전 날짜
    dates_back = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM etf_daily WHERE date <= ? ORDER BY date DESC LIMIT ?',
        (date_str, lookback + 1)).fetchall()]
    if len(dates_back) < lookback + 1:
        conn.close(); return []
    d_back = dates_back[-1]

    out = []
    for tk in tickers:
        r = cur.execute('''
            SELECT t.price, t.aum, t.volume, t.avg_volume_30d, t.day_return,
                   t.expense_ratio, t.dividend_yield, p.price
            FROM etf_daily t LEFT JOIN etf_daily p ON t.ticker=p.ticker AND p.date=?
            WHERE t.ticker=? AND t.date=?
        ''', (d_back, tk, date_str)).fetchone()
        if r and r[0]:
            p_now = r[0]
            p_back = r[7] or p_now
            mom_n = (p_now - p_back) / p_back * 100 if p_back else 0
            out.append({
                'ticker': tk, 'price': p_now, 'aum': r[1] or 0,
                'volume': r[2] or 0, 'avg_volume_30d': r[3] or 0,
                'day_return': r[4] or 0,
                'expense_ratio': r[5] or 0,
                'dividend_yield': r[6] or 0,
                'momentum_n': mom_n,
            })
    conn.close()
    return out


def score_for_intent(etfs, intent='long_hold'):
    """의도별 점수 계산"""
    if not etfs: return []

    for e in etfs:
        if intent == 'long_hold':
            # 운용보수 (40%) + AUM (40%) + 안정성 (20%)
            exp_score = max(0, 100 - e['expense_ratio'] * 10000) if e['expense_ratio'] > 0 else 50
            aum_score = min(100, (e['aum'] / 1e9) ** 0.5 * 20)
            stable_score = 50  # 향후 변동성 추가
            e['score'] = exp_score * 0.4 + aum_score * 0.4 + stable_score * 0.2
        elif intent == 'short_trade':
            # 거래량 (60%) + 유동성 spike potential (40%)
            vol_score = min(100, (e['avg_volume_30d'] / 1e6) ** 0.5 * 10)
            aum_score = min(100, (e['aum'] / 1e9) ** 0.5 * 15)
            e['score'] = vol_score * 0.6 + aum_score * 0.4
        elif intent == 'dividend':
            # 배당률 (60%) + AUM 안정성 (40%)
            div_score = min(100, e['dividend_yield'] * 1000)
            aum_score = min(100, (e['aum'] / 1e9) ** 0.5 * 20)
            e['score'] = div_score * 0.6 + aum_score * 0.4
        elif intent == 'small_account':
            # 가격 낮음 (50%) + AUM (30%) + 운용보수 낮음 (20%)
            price_score = max(0, 100 - e['price'] / 5)  # $100=80, $500=0
            aum_score = min(100, (e['aum'] / 1e9) ** 0.5 * 15)
            exp_score = max(0, 100 - e['expense_ratio'] * 10000) if e['expense_ratio'] > 0 else 50
            e['score'] = price_score * 0.5 + aum_score * 0.3 + exp_score * 0.2
        elif intent == 'momentum':
            # 모멘텀 (50%) + 거래량 (30%) + AUM (20%)
            mom_score = min(100, max(0, e['momentum_n'] * 5 + 50))
            vol_score = min(100, (e['avg_volume_30d'] / 1e6) ** 0.5 * 10)
            aum_score = min(100, (e['aum'] / 1e9) ** 0.5 * 15)
            e['score'] = mom_score * 0.5 + vol_score * 0.3 + aum_score * 0.2
        else:
            e['score'] = 50

    etfs.sort(key=lambda x: -x['score'])
    return etfs


INTENT_LABELS = {
    'long_hold': '🏦 장기 holding (수수료/안정성)',
    'short_trade': '⚡ 단기 트레이딩 (거래량/유동성)',
    'dividend': '💰 배당 우선 (배당률/안정성)',
    'small_account': '🐤 소액 적립 (낮은 가격)',
    'momentum': '🚀 모멘텀 추종 (최근 5일)',
}


def gen_intent_best_md(date_str=None):
    """모든 카테고리 × 의도별 best"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    conn.close()

    lines = [f'# 🎯 ETF 의도별 Best — {date_str}', '']
    lines.append('같은 카테고리도 사용자 의도에 따라 다른 best.')
    lines.append('')

    # 주요 카테고리만 (지나치게 많으면 가독성 X)
    key_groups = ['S&P 500', 'Nasdaq 100', 'Total Market', 'Semiconductor',
                  'AI/Robot', 'Dividend Growth', 'Gold', 'Long Treasury']

    for group in key_groups:
        tks = COMPARE_GROUPS.get(group)
        if not tks: continue
        etfs_base = get_etfs_with_momentum(tks, date_str)
        if not etfs_base: continue
        lines.append(f'## {group}')
        lines.append('')
        for intent, label in INTENT_LABELS.items():
            etfs = [dict(e) for e in etfs_base]  # copy
            score_for_intent(etfs, intent)
            if etfs:
                best = etfs[0]
                lines.append(f'- {label}: **{best["ticker"]}** '
                             f'(점수 {best["score"]:.0f}, 가격 ${best["price"]:.0f}, '
                             f'AUM ${best["aum"]/1e9:.1f}B)')
        lines.append('')

    lines.append('---')
    lines.append('')
    lines.append('_의도 선택으로 진짜 본인에게 맞는 ETF를 빠르게 찾으세요._')
    return '\n'.join(lines)


if __name__ == '__main__':
    md = gen_intent_best_md()
    out = Path(__file__).parent / 'content' / 'intent_best.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(f'저장: {out}')
    print(f'\n{md}')
