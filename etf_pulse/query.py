"""ETF Pulse Query — CLI 인터페이스 (사용자가 즉시 쿼리)

사용 예:
  python query.py compare "S&P 500"
  python query.py info VOO
  python query.py portfolio VOO QQQ GLD --weights 50 30 20
  python query.py signals  # 오늘 신호 요약
  python query.py best AI  # 카테고리 best
  python query.py intent semiconductor short_trade
"""
import sys
import argparse
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))


def cmd_info(args):
    from mcp_server import tool_get_etf_info
    info = tool_get_etf_info(args.ticker)
    if 'error' in info:
        print(f'✗ {info["error"]}')
        return
    print(f'📊 {info["ticker"]} ({info["category"]}) — {info["date"]}')
    print(f'   가격 ${info["price"]:.2f}, 어제 {info["day_return"]:+.2f}%')
    print(f'   AUM ${info["aum"]/1e9:.1f}B, 거래량 {info["volume"]:,}')
    if info.get('expense_ratio'):
        print(f'   수수료 {info["expense_ratio"]*100:.3f}%, 배당 {info["dividend_yield"]*100:.2f}%')
    if info.get('top_holdings'):
        print(f'\n   Top 5 보유종목:')
        for h in info['top_holdings'][:5]:
            print(f'   - {h["ticker"]:<6} {h["name"][:30]:<30} {h["weight"]*100:.2f}%')


def cmd_compare(args):
    from compare import COMPARE_GROUPS, compare_group, gen_compare_markdown
    # 그룹 이름 fuzzy 매칭
    matched = [k for k in COMPARE_GROUPS if args.group.lower() in k.lower()]
    if not matched:
        print(f'✗ "{args.group}" 매칭 그룹 없음.')
        print(f'사용 가능: {list(COMPARE_GROUPS.keys())[:15]}...')
        return
    group = matched[0]
    print(gen_compare_markdown(group, COMPARE_GROUPS[group]))


def cmd_portfolio(args):
    from portfolio_analyzer import gen_portfolio_analysis_md
    weights = [w/sum(args.weights) for w in args.weights] if args.weights else None
    print(gen_portfolio_analysis_md(args.tickers, weights))


def cmd_signals(args):
    from signals import get_signals, print_signals
    print_signals(get_signals())


def cmd_best(args):
    from compare import COMPARE_GROUPS, compare_group
    matched = [k for k in COMPARE_GROUPS if args.category.lower() in k.lower()]
    if not matched:
        print(f'✗ "{args.category}" 매칭 카테고리 없음.')
        return
    for g in matched[:3]:
        r = compare_group(g, COMPARE_GROUPS[g])
        if r:
            print(f'\n📍 {g}:')
            for i, e in enumerate(r['etfs'][:5], 1):
                print(f'   {i}. {e["ticker"]:<6} 점수 {e["total_score"]:5.1f}  '
                      f'AUM ${e["aum"]/1e9:6.1f}B  ret {e["day_return"]:+5.2f}%')


def cmd_intent(args):
    from intent_best import get_etfs_with_momentum, score_for_intent, INTENT_LABELS
    from compare import COMPARE_GROUPS
    matched = [k for k in COMPARE_GROUPS if args.category.lower() in k.lower()]
    if not matched:
        print(f'✗ "{args.category}" 매칭 없음')
        return
    group = matched[0]
    tks = COMPARE_GROUPS[group]
    from signals import get_signals
    date = get_signals()['date']
    etfs = get_etfs_with_momentum(tks, date)
    intents = args.intents if args.intents else list(INTENT_LABELS.keys())
    print(f'\n🎯 {group} — 의도별 best:\n')
    for intent in intents:
        if intent not in INTENT_LABELS:
            print(f'  ✗ unknown intent: {intent}')
            continue
        etfs_copy = [dict(e) for e in etfs]
        score_for_intent(etfs_copy, intent)
        if etfs_copy:
            b = etfs_copy[0]
            print(f'  {INTENT_LABELS[intent]}')
            print(f'    → {b["ticker"]} (점수 {b["score"]:.0f}, '
                  f'가격 ${b["price"]:.0f}, AUM ${b["aum"]/1e9:.1f}B)')


def cmd_regime(args):
    from hedge_diagnose import diagnose_market
    d = diagnose_market()
    print(f'\n🛡️ Market Regime: {d["regime"]}')
    print(f'   Risk-On 5일: {d["risk_on_avg_5d"]:+.2f}%')
    print(f'   Risk-Off 5일: {d["risk_off_avg_5d"]:+.2f}%')
    print(f'   Cyclical - Defensive: {d["cyclical_vs_defensive"]:+.2f}%')
    print(f'   종합 점수: {d["risk_on_score"]:+.2f}')


def cmd_eps_signal(args):
    from etf_eps_signal import rank_etfs_by_eps_signal
    rankings = rank_etfs_by_eps_signal()
    print(f'\n🎯 ETF × EPS Momentum 저평가 Top 10:\n')
    for i, r in enumerate(rankings[:10], 1):
        top_tks = ', '.join(s['ticker'] for s in r['top_matched'][:3])
        print(f'  {i:>2}. {r["etf"]:<6} adj_gap {r["weighted_gap"]:+6.2f}  '
              f'매칭 {r["matched_weight"]*100:.0f}%  Top: {top_tks}')


def cmd_help(args):
    print('''ETF Pulse Query — CLI 사용법:

  info <ticker>                ETF 단일 정보
  compare <group>              그룹 비교 (S&P 500, AI, Gold 등)
  best <category>              카테고리 best 5
  intent <category> [intents]  의도별 best
  portfolio <ticker...> --weights <w...>  포트폴리오 분석
  signals                      오늘 시장 신호 종합
  regime                       시장 regime 진단
  eps                          ETF EPS Momentum 저평가 ranking

예시:
  python query.py info SOXX
  python query.py compare "S&P 500"
  python query.py best AI
  python query.py intent semiconductor long_hold short_trade
  python query.py portfolio VOO QQQ GLD --weights 50 30 20
  python query.py signals
  python query.py regime
  python query.py eps
''')


COMMANDS = {
    'info': cmd_info,
    'compare': cmd_compare,
    'portfolio': cmd_portfolio,
    'signals': cmd_signals,
    'best': cmd_best,
    'intent': cmd_intent,
    'regime': cmd_regime,
    'eps': cmd_eps_signal,
    'help': cmd_help,
}


def main():
    parser = argparse.ArgumentParser(description='ETF Pulse CLI')
    parser.add_argument('command', choices=list(COMMANDS.keys()))
    parser.add_argument('positional', nargs='*', default=[])
    parser.add_argument('--weights', nargs='+', type=float)

    args = parser.parse_args()
    cmd = args.command
    pos = args.positional

    # 명령별 mapping
    if cmd == 'info':
        args.ticker = pos[0] if pos else 'VOO'
    elif cmd in ['compare', 'best']:
        args.group = ' '.join(pos) if pos else 'S&P 500'
        args.category = args.group
    elif cmd == 'intent':
        args.category = pos[0] if pos else 'AI'
        args.intents = pos[1:] if len(pos) > 1 else []
    elif cmd == 'portfolio':
        args.tickers = pos
    # 기타 명령은 인자 없음

    cmd_fn = COMMANDS.get(cmd)
    if cmd_fn:
        cmd_fn(args)
    else:
        cmd_help(None)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        cmd_help(None)
    else:
        main()
