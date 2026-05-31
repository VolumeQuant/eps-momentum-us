"""ETF Pulse Premium Daily — 통합 daily 리포트

모든 핵심 신호를 한 페이지에 통합:
- 시장 regime 진단
- 신호 (수익률/거래량/모멘텀/카테고리)
- 카테고리 best
- 의도별 best (top 카테고리만)
- ETF EPS signal (가능 시)
- 포트폴리오 펄스 (사용자 등록 시)
"""
import sys
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))


def gen_premium_md(user_id=None):
    """Premium daily report 통합"""
    from signals import get_signals
    from narrative import gen_narrative_kr
    from hedge_diagnose import gen_hedge_md
    from category_best import gen_weekly_best_md
    from intent_best import gen_intent_best_md

    signals = get_signals()
    date = signals['date']

    lines = []
    lines.append(f'# 🌟 ETF Pulse Premium Daily — {date}')
    lines.append('')
    lines.append('미국 ETF 257개 종합 분석 + 시장 regime + 카테고리/의도별 best')
    lines.append('')
    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append('')

    # 1. 시장 regime
    try:
        hedge_md = gen_hedge_md()
        # H1 제거하고 추가
        hedge_lines = hedge_md.split('\n')
        if hedge_lines[0].startswith('#'):
            hedge_lines[0] = '## 🛡️ Market Regime'
        lines.extend(hedge_lines[:30])  # 적절한 길이
        lines.append('')
    except Exception as e:
        lines.append(f'_market regime 실패: {e}_')

    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append('')

    # 2. 핵심 신호 (수익률 Top 5)
    lines.append('## 📈 어제 수익률 Top 5')
    lines.append('')
    for s in signals['top_returns'][:5]:
        spike = f' (vol {s["spike"]:.1f}x)' if s['spike'] > 1.5 else ''
        lines.append(f'- **{s["ticker"]}** ({s["category"]}): {s["day_return"]:+.2f}%{spike}')
    lines.append('')

    lines.append('## 🚀 5일 모멘텀 Top 5')
    lines.append('')
    for s in signals['momentum_5d'][:5]:
        lines.append(f'- **{s["ticker"]}**: {s["return_5d"]:+.2f}% (5d)')
    lines.append('')

    # 3. 자동 인사이트 (narrative에서)
    from narrative import detect_patterns
    insights = detect_patterns(signals)
    if insights:
        lines.append('## 🧠 자동 인사이트')
        lines.append('')
        for ins in insights:
            lines.append(f'- {ins}')
        lines.append('')

    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append('')

    # 4. 카테고리 best (S&P 500, AI, Semi, Gold만 highlight)
    lines.append('## 🏆 주요 카테고리 Best')
    lines.append('')
    from compare import COMPARE_GROUPS, compare_group
    key_groups = ['S&P 500', 'Nasdaq 100', 'Semiconductor', 'AI/Robot',
                  'Gold', 'Long Treasury', 'Dividend Growth']
    for g in key_groups:
        tks = COMPARE_GROUPS.get(g)
        if not tks: continue
        r = compare_group(g, tks, date)
        if not r: continue
        b = r['best']
        rec = r['recommendation'] or f'점수 {b["total_score"]:.1f}'
        lines.append(f'- **{g}**: {b["ticker"]} ({rec})')
    lines.append('')

    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append('')

    # 5. ETF EPS signal (가능 시)
    try:
        from etf_eps_signal import rank_etfs_by_eps_signal
        rankings = rank_etfs_by_eps_signal(date)
        if rankings:
            lines.append('## 🎯 ETF × EPS Momentum Top 5 저평가')
            lines.append('')
            lines.append('각 ETF 보유종목 EPS revision 가중 평균 (음수 = 저평가)')
            lines.append('')
            for r in rankings[:5]:
                top_tks = ', '.join(s['ticker'] for s in r['top_matched'][:3])
                lines.append(f'- **{r["etf"]}**: 가중 adj_gap {r["weighted_gap"]:+.2f} '
                             f'(매칭 {r["matched_weight"]*100:.0f}%, Top: {top_tks})')
            lines.append('')
    except Exception:
        pass

    # 6. 포트폴리오 펄스 (user_id 있을 때)
    if user_id:
        try:
            from portfolio import get_portfolio_pulse, gen_pulse_message
            p_pulse = get_portfolio_pulse(user_id, date)
            if p_pulse and p_pulse['holdings']:
                lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
                lines.append('')
                lines.append('## 💼 내 포트폴리오 펄스')
                lines.append('')
                lines.append(f'총 가치: ${p_pulse["total_value"]:,.0f} / 누적 {p_pulse["total_pnl_pct"]:+.2f}%')
                lines.append('')
                for h in sorted(p_pulse['holdings'], key=lambda x: -abs(x['day_return']))[:5]:
                    emoji = '🟢' if h['day_return'] > 0.5 else '🔴' if h['day_return'] < -0.5 else '⚪'
                    lines.append(f'- {emoji} {h["ticker"]}: 어제 {h["day_return"]:+.2f}%, 누적 {h["pnl_pct"]:+.2f}%')
                lines.append('')
        except Exception:
            pass

    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append('')
    lines.append('_ETF Pulse Premium Daily — 자동 데이터 분석. 투자 추천 아님._')
    lines.append(f'_데이터: yfinance · 분석: 257 ETF · 갱신: 매일_')

    return '\n'.join(lines)


if __name__ == '__main__':
    md = gen_premium_md()
    out = Path(__file__).parent / 'content' / f'premium_daily_{datetime.now().strftime("%Y-%m-%d")}.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(f'저장: {out}')
    print(f'\n길이: {len(md)} chars\n')
    print(md[:3000])
    print('...')
