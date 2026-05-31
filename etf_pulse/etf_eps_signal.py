"""ETF Pulse — ETF에 EPS Momentum 신호 적용

사용자의 KR/US EPS Momentum 시스템을 ETF에 확장:
- ETF가 보유한 종목들의 NTM EPS revision 평균
- ETF별 "EPS-weighted gap" 계산
- 가격 vs EPS 괴리 큰 ETF = 매수 후보

데이터: eps_momentum_data.db의 ntm_screening + etf_pulse의 holdings
"""
import sys
import sqlite3
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ETF_DB = Path(__file__).parent / 'etf_pulse.db'
EPS_DB = Path(__file__).parent.parent / 'eps_momentum_data.db'


def get_etf_eps_signal(etf_ticker, date_str=None):
    """ETF의 EPS-weighted 신호 계산
    각 보유 종목의 adj_gap (괴리율) × ETF 내 비중 → 가중 합
    """
    # 1) ETF 보유종목 + 비중
    conn_e = sqlite3.connect(ETF_DB); cur_e = conn_e.cursor()
    if not date_str:
        date_str = cur_e.execute('SELECT MAX(date) FROM etf_holdings_daily').fetchone()[0]
    holdings = cur_e.execute('''
        SELECT holding_ticker, weight FROM etf_holdings_daily
        WHERE etf_ticker=? AND date=? ORDER BY weight DESC
    ''', (etf_ticker, date_str)).fetchall()
    conn_e.close()

    if not holdings or not EPS_DB.exists():
        return None

    # 2) 각 보유 종목의 adj_gap (EPS DB에서)
    conn_p = sqlite3.connect(EPS_DB); cur_p = conn_p.cursor()
    eps_latest = cur_p.execute(
        'SELECT MAX(date) FROM ntm_screening WHERE adj_gap IS NOT NULL'
    ).fetchone()[0]

    weighted_gap = 0
    matched_weight = 0
    matched_stocks = []
    for hold_tk, w in holdings:
        r = cur_p.execute('''
            SELECT adj_gap, eps_chg_weighted FROM ntm_screening
            WHERE ticker=? AND date=?
        ''', (hold_tk, eps_latest)).fetchone()
        if r and r[0] is not None:
            weighted_gap += w * r[0]
            matched_weight += w
            matched_stocks.append({'ticker': hold_tk, 'weight': w,
                                    'adj_gap': r[0], 'eps_chg': r[1]})

    conn_p.close()

    if matched_weight == 0:
        return None

    return {
        'etf': etf_ticker,
        'date': date_str,
        'weighted_gap': weighted_gap / matched_weight if matched_weight else 0,  # 가중 평균
        'matched_weight': matched_weight,
        'n_matched': len(matched_stocks),
        'top_matched': matched_stocks[:10],
    }


def rank_etfs_by_eps_signal(date_str=None):
    """모든 ETF를 EPS-weighted signal로 ranking"""
    conn_e = sqlite3.connect(ETF_DB); cur_e = conn_e.cursor()
    if not date_str:
        date_str = cur_e.execute('SELECT MAX(date) FROM etf_holdings_daily').fetchone()[0]
    etfs = [r[0] for r in cur_e.execute(
        'SELECT DISTINCT etf_ticker FROM etf_holdings_daily WHERE date=?', (date_str,)).fetchall()]
    conn_e.close()

    results = []
    for tk in etfs:
        s = get_etf_eps_signal(tk, date_str)
        if s and s['matched_weight'] > 0.1:  # 최소 10% 매칭
            results.append(s)
    results.sort(key=lambda x: x['weighted_gap'])  # 음수 (저평가) 먼저
    return results


def gen_eps_etf_signal_md(date_str=None):
    """ETF EPS signal 콘텐츠"""
    rankings = rank_etfs_by_eps_signal(date_str)
    if not rankings:
        return '# EPS Momentum DB 없음 또는 데이터 부족'

    lines = ['# 🎯 ETF × EPS Momentum 신호', '']
    lines.append('각 ETF가 보유한 종목들의 EPS revision 가중 평균.')
    lines.append('**음수 = 저평가** (EPS 상승 대비 가격 못 따라감, mean reversion 매수 후보)')
    lines.append('')
    lines.append('## 🥇 저평가 ETF Top 10 (음수 큰 순)')
    lines.append('')
    lines.append('| 순위 | ETF | 가중 adj_gap | 매칭률 | 핵심 종목 |')
    lines.append('|------|-----|--------------|--------|-----------|')
    for i, r in enumerate(rankings[:10], 1):
        top_tks = ', '.join(s['ticker'] for s in r['top_matched'][:3])
        lines.append(f'| {i} | **{r["etf"]}** | {r["weighted_gap"]:+.2f} | '
                     f'{r["matched_weight"]*100:.0f}% | {top_tks} |')
    lines.append('')

    lines.append('## 📈 고평가 ETF Top 10 (양수 큰 순)')
    lines.append('')
    lines.append('| 순위 | ETF | 가중 adj_gap | 매칭률 | 핵심 종목 |')
    lines.append('|------|-----|--------------|--------|-----------|')
    for i, r in enumerate(rankings[-10:][::-1], 1):
        top_tks = ', '.join(s['ticker'] for s in r['top_matched'][:3])
        lines.append(f'| {i} | **{r["etf"]}** | {r["weighted_gap"]:+.2f} | '
                     f'{r["matched_weight"]*100:.0f}% | {top_tks} |')
    lines.append('')

    lines.append('---')
    lines.append('_각 ETF 보유종목의 EPS revision 가중 평균. mean reversion 신호._')
    lines.append('_매칭률 = ETF 보유종목 중 EPS DB에 있는 비율. 100%에 가까울수록 신호 신뢰도 높음._')
    return '\n'.join(lines)


if __name__ == '__main__':
    print('=== ETF × EPS Momentum 신호 계산 ===\n')
    rankings = rank_etfs_by_eps_signal()
    print(f'분석 ETF: {len(rankings)}개\n')
    print('Top 10 저평가:')
    for r in rankings[:10]:
        print(f'  {r["etf"]:<8} adj_gap {r["weighted_gap"]:+6.2f}  '
              f'매칭률 {r["matched_weight"]*100:.0f}%  Top: '
              f'{", ".join(s["ticker"] for s in r["top_matched"][:3])}')
    print('\nTop 5 고평가:')
    for r in rankings[-5:]:
        print(f'  {r["etf"]:<8} adj_gap {r["weighted_gap"]:+6.2f}  '
              f'매칭률 {r["matched_weight"]*100:.0f}%')

    md = gen_eps_etf_signal_md()
    out = Path(__file__).parent / 'content' / 'etf_eps_signal.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(f'\n저장: {out}')
