"""ETF Pulse Dual Market — 한미 통합 daily 신호

한국 + 미국 ETF를 하나의 콘텐츠로 발행.
양쪽 다 투자하는 retail에 특화 (다른 서비스 못 함).
"""
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from signals import get_signals
from kr_etfs import get_all_kr_etfs, fetch_kr_etf


def fetch_kr_signals():
    """한국 ETF 신호 fetch (즉시, DB 미사용)"""
    etfs = get_all_kr_etfs()
    results = []
    for tk, cat, name in etfs:
        r = fetch_kr_etf(tk, name, cat)
        if r and 'error' not in r:
            results.append(r)
    return results


def gen_dual_market_md():
    """한미 통합 daily 콘텐츠"""
    us = get_signals()
    kr = fetch_kr_signals()

    today = datetime.now().strftime('%Y-%m-%d')
    lines = [f'# 🌐 Daily Dual Market ETF Pulse — {today}', '']
    lines.append('한국 + 미국 ETF 통합 신호. 양 시장 다 투자하는 retail 전용.')
    lines.append('')

    # ━━━ 미국 시장 (어제 마감) ━━━
    lines.append('## 🇺🇸 미국 (어제 마감)')
    lines.append('')
    if us['top_returns']:
        lines.append('**수익률 Top 3**')
        for s in us['top_returns'][:3]:
            lines.append(f'- {s["ticker"]} ({s["category"]}): {s["day_return"]:+.2f}%')
    if us['volume_spikes']:
        lines.append('')
        lines.append('**거래량 spike Top 3**')
        for s in us['volume_spikes'][:3]:
            lines.append(f'- {s["ticker"]}: {s["spike"]:.2f}x, ret {s["day_return"]:+.2f}%')
    if us['category_strength']:
        strong = us['category_strength'][0]
        weak = us['category_strength'][-1]
        lines.append('')
        lines.append(f'**카테고리 강도**: 강 {strong["category"]} {strong["avg_return"]:+.2f}% / '
                     f'약 {weak["category"]} {weak["avg_return"]:+.2f}%')
    lines.append('')

    # ━━━ 한국 시장 ━━━
    lines.append('## 🇰🇷 한국 (어제 마감)')
    lines.append('')
    if kr:
        # 수익률 Top 3
        kr_top = sorted(kr, key=lambda x: -x['day_return'])[:5]
        lines.append('**수익률 Top 5**')
        for r in kr_top:
            lines.append(f'- {r["ticker"]} {r["name"][:25]}: {r["day_return"]:+.2f}%')
        # 거래량 spike
        kr_spikes = sorted([r for r in kr if r['volume_spike'] > 1.5],
                          key=lambda x: -x['volume_spike'])[:3]
        if kr_spikes:
            lines.append('')
            lines.append('**거래량 spike Top 3**')
            for r in kr_spikes:
                lines.append(f'- {r["ticker"]} {r["name"][:25]}: {r["volume_spike"]:.2f}x, '
                             f'ret {r["day_return"]:+.2f}%')
    else:
        lines.append('데이터 수집 실패')
    lines.append('')

    # ━━━ 한미 비교 자동 인사이트 ━━━
    lines.append('## 🧠 한미 통합 인사이트')
    lines.append('')
    # 동일 테마 한미 양쪽 강세
    insights = []
    us_top_tks = {s['ticker'] for s in us['top_returns']}
    if 'SOXX' in us_top_tks or 'SMH' in us_top_tks:
        kr_semi = [r for r in kr if '반도체' in r['name'] or 'SOXX' in r['name']]
        if kr_semi and any(r['day_return'] > 0 for r in kr_semi):
            insights.append('🔥 **한미 반도체 ETF 동시 강세** — 글로벌 반도체 테마 가속')
    if 'KWEB' in us_top_tks or 'FXI' in us_top_tks:
        kr_china = [r for r in kr if '차이나' in r['name'] or '중국' in r['name']]
        if kr_china and any(r['day_return'] > 0 for r in kr_china):
            insights.append('🇨🇳 **한미 중국 ETF 동시 강세** — 중국 자금 유입 신호')
    # 안전자산 vs 위험자산
    if 'GLD' in us_top_tks:
        kr_gold = [r for r in kr if '골드' in r['name'] or '금' in r['name']]
        if kr_gold and any(r['day_return'] > 0 for r in kr_gold):
            insights.append('🪙 **한미 금 ETF 동시 강세** — 안전자산 선호')
    # KODEX 200 vs SPY 비교
    kospi_etfs = [r for r in kr if 'KODEX 200' in r['name'] or 'TIGER 200' in r['name']]
    if kospi_etfs:
        kospi_ret = sum(r['day_return'] for r in kospi_etfs) / len(kospi_etfs)
        spy = next((s for s in us['top_returns'] + us['bottom_returns'] if s['ticker'] == 'SPY'), None)
        if spy:
            diff = kospi_ret - spy['day_return']
            if abs(diff) > 1:
                stronger = '한국 KOSPI' if diff > 0 else '미국 S&P500'
                insights.append(f'📊 **{stronger} 우위**: KOSPI {kospi_ret:+.2f}% vs SPY {spy["day_return"]:+.2f}% '
                                f'(차이 {diff:+.2f}%p)')

    if insights:
        for i in insights:
            lines.append(f'- {i}')
    else:
        lines.append('- 특별 패턴 없음')
    lines.append('')

    lines.append('---')
    lines.append('_한미 ETF 통합 분석 — 자동 데이터. 투자 추천 아님._')
    return '\n'.join(lines)


if __name__ == '__main__':
    md = gen_dual_market_md()
    out = Path(__file__).parent / 'content' / 'dual_market.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(md)
    print(f'\n저장: {out}')
