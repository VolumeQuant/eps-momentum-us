"""ETF Pulse Portfolio Analyzer — 고급 분석

기능:
1. Sector overlap 분석 (보유 ETF끼리 겹치는 종목/섹터)
2. 분산 점수 (1~100, 너무 집중되면 경고)
3. 추천 rebalance (지나친 카테고리 비중 alert)
4. 효율적 frontier 근사 (Sharpe 최대 weight)
5. tax-loss harvesting 후보 (손실 종목 + 유사 ETF 추천)
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def get_holdings_map(tickers, date_str=None):
    """티커별 top holdings dict"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    out = {}
    for tk in tickers:
        rows = cur.execute('''
            SELECT holding_ticker, weight FROM etf_holdings_daily
            WHERE etf_ticker=? AND date=? ORDER BY rank
        ''', (tk, date_str)).fetchall()
        out[tk] = {r[0]: r[1] for r in rows}
    conn.close()
    return out


def analyze_overlap(tickers, weights, date_str=None):
    """포트폴리오 sector/종목 overlap 분석"""
    holdings_map = get_holdings_map(tickers, date_str)

    # 종목별 통합 비중 (포트폴리오 전체에서)
    aggregated = defaultdict(float)
    for tk, w_etf in zip(tickers, weights):
        for h_tk, h_w in holdings_map.get(tk, {}).items():
            # 종목 비중 = ETF 비중 × ETF내 종목 비중
            aggregated[h_tk] += w_etf * h_w

    top_aggregated = sorted(aggregated.items(), key=lambda x: -x[1])[:15]

    # Overlap pair 분석 (어느 두 ETF가 가장 겹치나)
    pairs = []
    for i, tk1 in enumerate(tickers):
        for tk2 in tickers[i+1:]:
            h1 = holdings_map.get(tk1, {})
            h2 = holdings_map.get(tk2, {})
            common = set(h1.keys()) & set(h2.keys())
            if common:
                overlap_weight = sum(min(h1[t], h2[t]) for t in common)
                pairs.append({
                    'tk1': tk1, 'tk2': tk2,
                    'n_common': len(common),
                    'overlap_weight': overlap_weight,
                    'sample_common': sorted(common, key=lambda t: -h1[t])[:5],
                })
    pairs.sort(key=lambda x: -x['overlap_weight'])

    return {
        'top_aggregated': top_aggregated,
        'overlap_pairs': pairs,
        'concentration_score': sum(w**2 for _, w in top_aggregated) * 100,  # HHI-like
    }


def diversification_score(tickers, weights, date_str=None):
    """분산 점수 1~100 (높을수록 잘 분산)"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    # 카테고리 다양성
    cats = []
    for tk in tickers:
        r = cur.execute('SELECT category FROM etf_daily WHERE ticker=? AND date=?',
                        (tk, date_str)).fetchone()
        if r: cats.append(r[0])
    conn.close()
    unique_cats = len(set(cats))

    # 비중 평등도 (HHI 역)
    sum_sq = sum(w**2 for w in weights)
    weight_diversity = 1 - sum_sq  # 0 (1종목 100%) ~ 1 (균등)

    # 종목 overlap (낮을수록 좋음)
    overlap = analyze_overlap(tickers, weights, date_str)
    overlap_penalty = min(50, overlap['concentration_score'] / 100)

    # 종합 점수
    cat_score = min(50, unique_cats * 10)  # 5+ 카테고리 = 50
    weight_score = weight_diversity * 30
    overlap_bonus = max(0, 20 - overlap_penalty)
    score = cat_score + weight_score + overlap_bonus
    return {
        'score': score,
        'unique_categories': unique_cats,
        'weight_diversity': weight_diversity,
        'concentration_score': overlap['concentration_score'],
        'breakdown': f'카테고리 {cat_score:.0f} + 비중 {weight_score:.0f} + overlap {overlap_bonus:.0f}',
    }


def gen_portfolio_analysis_md(tickers, weights=None, date_str=None):
    """포트폴리오 종합 분석 Markdown"""
    if weights is None:
        weights = [1/len(tickers)] * len(tickers)

    overlap = analyze_overlap(tickers, weights, date_str)
    div = diversification_score(tickers, weights, date_str)

    lines = [f'# 🔍 포트폴리오 분석', '']
    lines.append(f'**구성**: ' + ', '.join(f'{tk} ({w*100:.0f}%)' for tk, w in zip(tickers, weights)))
    lines.append('')
    lines.append(f'## 📊 분산 점수: **{div["score"]:.0f}/100**')
    lines.append(f'  - 카테고리 다양성: {div["unique_categories"]}개')
    lines.append(f'  - 비중 다양성: {div["weight_diversity"]*100:.0f}%')
    lines.append(f'  - 종목 집중도: {div["concentration_score"]:.1f}')
    lines.append(f'  - 점수 분해: {div["breakdown"]}')
    lines.append('')
    if div['score'] >= 70:
        lines.append('  ✅ **분산 잘 됨** — 안정적')
    elif div['score'] >= 50:
        lines.append('  ⚠️ **분산 양호** — 일부 카테고리 추가 권장')
    else:
        lines.append('  🚨 **분산 부족** — 카테고리 또는 ETF 다양화 필요')
    lines.append('')

    # Top 종목 (통합)
    lines.append('## 🎯 통합 Top 15 종목 (포트폴리오 전체)')
    lines.append('')
    lines.append('| 순위 | Ticker | 통합 비중 |')
    lines.append('|------|--------|-----------|')
    for i, (tk, w) in enumerate(overlap['top_aggregated'], 1):
        lines.append(f'| {i} | {tk} | {w*100:.2f}% |')
    lines.append('')

    # 가장 큰 overlap pair
    if overlap['overlap_pairs']:
        lines.append('## 🔗 ETF 간 가장 큰 overlap')
        lines.append('')
        for p in overlap['overlap_pairs'][:5]:
            lines.append(f'- **{p["tk1"]} ↔ {p["tk2"]}**: {p["n_common"]}개 공통, '
                         f'overlap weight {p["overlap_weight"]*100:.2f}%')
            lines.append(f'  └ 주요 공통: {", ".join(p["sample_common"])}')
        lines.append('')
        max_overlap = overlap['overlap_pairs'][0]
        if max_overlap['overlap_weight'] > 0.3:
            lines.append(f'⚠️ **{max_overlap["tk1"]}와 {max_overlap["tk2"]} 매우 비슷** — 하나로 통합 검토')
            lines.append('')

    lines.append('---')
    lines.append('_포트폴리오 분석. 투자 추천 아님._')
    return '\n'.join(lines)


if __name__ == '__main__':
    # 데모 1: 분산된 포트폴리오
    print('=== 데모 1: 분산 포트폴리오 ===\n')
    md = gen_portfolio_analysis_md(
        ['VOO', 'QQQ', 'SOXX', 'GLD', 'TLT'],
        [0.4, 0.2, 0.15, 0.15, 0.10])
    print(md)

    print('\n\n=== 데모 2: 집중 포트폴리오 (overlap 큰) ===\n')
    md2 = gen_portfolio_analysis_md(
        ['VOO', 'IVV', 'SPY'],  # 같은 S&P 500
        [0.4, 0.3, 0.3])
    print(md2)

    out = Path(__file__).parent / 'content' / 'portfolio_analysis_demo.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md + '\n\n---\n\n' + md2, encoding='utf-8')
    print(f'\n저장: {out}')
