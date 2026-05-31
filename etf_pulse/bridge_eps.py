"""ETF Pulse Bridge — KR/US EPS Momentum 시스템과 통합

사용자가 이미 보유한 KR/US EPS Momentum 시스템(eps_momentum_data.db) 결과를
ETF Pulse 콘텐츠에 통합.

기능:
1. 오늘 시스템 추천 종목 (AEIS, KEYS 등) → 그 종목 비중 큰 ETF 찾기
2. "내 종목 + 관련 ETF" 통합 추천
3. ETF로 분산 매수 옵션 제시
"""
import sys
import sqlite3
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))

ETF_DB = Path(__file__).parent / 'etf_pulse.db'
EPS_DB = Path(__file__).parent.parent / 'eps_momentum_data.db'


def get_eps_recommendations(date_str=None):
    """KR/US EPS Momentum 시스템의 오늘 추천 종목"""
    if not EPS_DB.exists():
        return None
    conn = sqlite3.connect(EPS_DB); cur = conn.cursor()
    if not date_str:
        date_str = cur.execute(
            'SELECT MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL'
        ).fetchone()[0]
    rows = cur.execute('''
        SELECT ticker, part2_rank, adj_gap, ntm_current, eps_chg_weighted
        FROM ntm_screening WHERE date=? AND part2_rank <= 5
        ORDER BY part2_rank
    ''', (date_str,)).fetchall()
    conn.close()
    return {
        'date': date_str,
        'recommendations': [
            {'ticker': r[0], 'rank': r[1], 'adj_gap': r[2],
             'ntm_current': r[3], 'eps_chg_weighted': r[4]}
            for r in rows
        ]
    }


def find_etfs_containing(ticker, min_weight=0.005):
    """그 종목을 보유한 ETF 찾기 (weight 0.5% 이상)"""
    if not ETF_DB.exists():
        return []
    conn = sqlite3.connect(ETF_DB); cur = conn.cursor()
    latest = cur.execute('SELECT MAX(date) FROM etf_holdings_daily').fetchone()[0]
    rows = cur.execute('''
        SELECT h.etf_ticker, h.weight, d.category, d.aum
        FROM etf_holdings_daily h
        LEFT JOIN etf_daily d ON h.etf_ticker=d.ticker AND d.date=h.date
        WHERE h.holding_ticker=? AND h.date=? AND h.weight >= ?
        ORDER BY h.weight DESC
    ''', (ticker.upper(), latest, min_weight)).fetchall()
    conn.close()
    return [
        {'etf': r[0], 'weight': r[1], 'category': r[2], 'aum_b': (r[3] or 0)/1e9}
        for r in rows
    ]


def gen_bridge_content():
    """통합 콘텐츠 — 종목 추천 + 관련 ETF"""
    eps = get_eps_recommendations()
    if not eps:
        return '# EPS Momentum DB 없음 — Bridge 비활성'

    lines = [f'# 🌉 EPS Momentum + ETF Pulse 통합 — {eps["date"]}', '']
    lines.append('당신의 EPS Momentum 시스템 추천 종목 + 그 종목을 가장 많이 보유한 ETF.')
    lines.append('개별 종목 부담스러우면 ETF로 분산 매수 옵션.')
    lines.append('')

    for rec in eps['recommendations']:
        tk = rec['ticker']
        lines.append(f'## {rec["rank"]}위: **{tk}**')
        if rec.get("adj_gap") is not None:
            lines.append(f'  - adj_gap: {rec["adj_gap"]:+.2f} (괴리율)')
        if rec.get("ntm_current") is not None:
            lines.append(f'  - NTM EPS: ${rec["ntm_current"]:.2f}')
        if rec.get("eps_chg_weighted") is not None:
            lines.append(f'  - EPS 가중 변화: {rec["eps_chg_weighted"]:+.2f}%')
        lines.append('')
        # 관련 ETF
        etfs = find_etfs_containing(tk)
        if etfs:
            lines.append(f'  **{tk} 보유 ETF (비중 큰 순)**:')
            for e in etfs[:5]:
                lines.append(f'  - {e["etf"]} ({e["category"]}): {e["weight"]*100:.2f}% 비중, '
                             f'AUM ${e["aum_b"]:.1f}B')
            lines.append('')
            lines.append(f'  💡 **{tk} 직접 매수 vs ETF 매수**: 단일 종목 변동성 부담 시 '
                         f'{etfs[0]["etf"]}로 일부 노출 (비중 {etfs[0]["weight"]*100:.2f}%) — '
                         f'분산 효과 + 비슷한 알파 일부 확보.')
        else:
            lines.append(f'  *{tk} 비중 큰 ETF 없음 (또는 데이터 부족)*')
        lines.append('')

    lines.append('---')
    lines.append('_EPS Momentum 시스템 결과 + ETF Pulse 통합. 투자 추천 아님._')
    return '\n'.join(lines)


if __name__ == '__main__':
    content = gen_bridge_content()
    out = Path(__file__).parent / 'content' / 'bridge_eps_etf.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(content, encoding='utf-8')
    print(content)
    print(f'\n저장: {out}')
