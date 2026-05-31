"""ETF Pulse Hedge Diagnose — 시장 리스크 자동 진단

매일 시장 환경 자동 평가 + 헤지 추천:
- VIX (변동성) 추이
- HY 스프레드 (신용 리스크)
- 채권 vs 주식 강도
- 금/달러 강도 (안전자산 선호)
- ARK 같은 high-beta growth ETF 동향
- 자동 결론: risk-on / risk-off / 중립
"""
import sys
import sqlite3
import statistics
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def get_recent_returns(ticker, days=5):
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, day_return, price FROM etf_daily
        WHERE ticker=? ORDER BY date DESC LIMIT ?
    ''', (ticker, days)).fetchall()
    conn.close()
    return list(reversed(rows))


def diagnose_market():
    """시장 환경 자동 진단"""
    # 핵심 지표 ETF
    indicators = {
        'risk_on': {
            'SPY': 'S&P 500', 'QQQ': 'Nasdaq', 'SOXX': 'Semis', 'ARKK': 'ARK Innovation',
            'KWEB': 'China Tech', 'IBIT': 'Bitcoin',
        },
        'risk_off': {
            'TLT': 'Long Treasury', 'SHY': 'Short Treasury', 'GLD': 'Gold',
            'JEPI': 'Covered Call', 'VIXY': 'VIX',
        },
        'cyclical': {
            'XLE': 'Energy', 'XLF': 'Financials', 'XLI': 'Industrials',
        },
        'defensive': {
            'XLP': 'Staples', 'XLU': 'Utilities', 'XLV': 'Healthcare',
        },
    }

    analysis = {}
    for category, etfs in indicators.items():
        cat_data = []
        for tk, name in etfs.items():
            rets = get_recent_returns(tk, 5)
            if len(rets) >= 2:
                ret_1d = rets[-1][1] if rets[-1][1] else 0
                rets_5d_pcts = [r[1] or 0 for r in rets[-5:]]
                ret_5d = sum(rets_5d_pcts)
                vol_5d = statistics.pstdev(rets_5d_pcts) if len(rets_5d_pcts) > 1 else 0
                cat_data.append({
                    'ticker': tk, 'name': name,
                    'ret_1d': ret_1d, 'ret_5d': ret_5d, 'vol_5d': vol_5d
                })
        analysis[category] = cat_data

    # 종합 점수
    risk_on_avg = statistics.mean([d['ret_5d'] for d in analysis['risk_on']]) if analysis['risk_on'] else 0
    risk_off_avg = statistics.mean([d['ret_5d'] for d in analysis['risk_off']]) if analysis['risk_off'] else 0
    cyclical_avg = statistics.mean([d['ret_5d'] for d in analysis['cyclical']]) if analysis['cyclical'] else 0
    defensive_avg = statistics.mean([d['ret_5d'] for d in analysis['defensive']]) if analysis['defensive'] else 0

    # 진단
    risk_on_score = risk_on_avg - risk_off_avg + (cyclical_avg - defensive_avg) * 0.5
    if risk_on_score > 3:
        regime = '🟢 Risk-On (강세장)'
    elif risk_on_score > 1:
        regime = '🟡 Mild Risk-On (약한 강세)'
    elif risk_on_score > -1:
        regime = '⚪ Neutral'
    elif risk_on_score > -3:
        regime = '🟠 Mild Risk-Off (약한 약세)'
    else:
        regime = '🔴 Risk-Off (방어 모드)'

    return {
        'regime': regime,
        'risk_on_score': risk_on_score,
        'risk_on_avg_5d': risk_on_avg,
        'risk_off_avg_5d': risk_off_avg,
        'cyclical_vs_defensive': cyclical_avg - defensive_avg,
        'details': analysis,
    }


def gen_hedge_md():
    """헤지 진단 콘텐츠"""
    d = diagnose_market()
    lines = [f'# 🛡️ Market Regime 진단 — {datetime.now().strftime("%Y-%m-%d")}', '']
    lines.append(f'## {d["regime"]}')
    lines.append(f'  - Risk-On 5일: {d["risk_on_avg_5d"]:+.2f}%')
    lines.append(f'  - Risk-Off 5일: {d["risk_off_avg_5d"]:+.2f}%')
    lines.append(f'  - Cyclical - Defensive: {d["cyclical_vs_defensive"]:+.2f}%')
    lines.append(f'  - 종합 점수: {d["risk_on_score"]:+.2f}')
    lines.append('')

    # 지표별 상세
    sections = [
        ('🚀 Risk-On 지표', 'risk_on'),
        ('🛡️ Risk-Off 지표', 'risk_off'),
        ('🏭 Cyclical (경기민감)', 'cyclical'),
        ('🏥 Defensive (방어주)', 'defensive'),
    ]
    for title, key in sections:
        lines.append(f'## {title}')
        lines.append('')
        lines.append('| ETF | 5일 수익률 | 5일 변동성 |')
        lines.append('|-----|-----------|-----------|')
        for d_etf in d['details'].get(key, []):
            lines.append(f'| {d_etf["ticker"]} ({d_etf["name"]}) | {d_etf["ret_5d"]:+.2f}% | {d_etf["vol_5d"]:.2f}% |')
        lines.append('')

    # 헤지 추천
    lines.append('## 💡 자동 추천')
    lines.append('')
    if d['risk_on_score'] > 2:
        lines.append('- 강세장 분위기 → **공격적 포트폴리오 적정** (QQQ/SOXX/ARK 등 비중 ↑)')
        lines.append('- 단, 너무 한쪽 쏠리면 변동성 폭증 주의 → 일부 GLD/TLT 헤지 권장')
    elif d['risk_on_score'] > 0:
        lines.append('- 약한 강세 → **균형 포트폴리오 권장** (코어 + 일부 성장)')
    elif d['risk_on_score'] > -2:
        lines.append('- 중립 → **분산 유지** + 채권/금 일부 헤지 검토')
    else:
        lines.append('- ⚠️ 약세 분위기 → **방어 ETF 비중 ↑** (TLT/GLD/JEPI 등)')
        lines.append('- 인버스 ETF (SH/SDS) 헤지 검토 (단, 장기 보유 X — decay)')
    lines.append('')

    lines.append('---')
    lines.append('_시장 진단 — 자동 데이터 분석. 투자 추천 아님._')
    return '\n'.join(lines)


if __name__ == '__main__':
    md = gen_hedge_md()
    out = Path(__file__).parent / 'content' / 'market_regime.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(md)
    print(f'\n저장: {out}')
