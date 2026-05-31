"""ETF Pulse MCP Server — Claude Desktop / Claude Code 통합

사용자가 Claude에서 직접 ETF 정보 쿼리:
  - "오늘 ETF 시장 어때?"
  - "AI 관련 ETF best 3개 알려줘"
  - "내 포트폴리오 (VOO, QQQ, SOXX) 어떻게?"

MCP server stdio protocol — Claude Desktop / Claude Code에 등록.
"""
import sys
import json
import sqlite3
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def tool_get_etf_info(ticker: str) -> dict:
    """단일 ETF 정보 조회"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    latest = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    r = cur.execute('''
        SELECT category, price, volume, avg_volume_30d, aum, day_return,
               expense_ratio, dividend_yield, beta, volume_spike
        FROM etf_daily WHERE ticker=? AND date=?
    ''', (ticker.upper(), latest)).fetchone()
    if not r:
        conn.close()
        return {'error': f'{ticker} 데이터 없음'}
    holdings = cur.execute('''
        SELECT holding_ticker, holding_name, weight
        FROM etf_holdings_daily WHERE etf_ticker=? AND date=?
        ORDER BY rank LIMIT 10
    ''', (ticker.upper(), latest)).fetchall()
    conn.close()
    return {
        'ticker': ticker.upper(),
        'date': latest,
        'category': r[0], 'price': r[1], 'volume': r[2],
        'avg_volume_30d': r[3], 'aum': r[4], 'day_return': r[5],
        'expense_ratio': r[6], 'dividend_yield': r[7], 'beta': r[8],
        'volume_spike': r[9],
        'top_holdings': [{'ticker': h[0], 'name': h[1], 'weight': h[2]} for h in holdings],
    }


def tool_get_today_signals() -> dict:
    """오늘의 시장 신호 종합"""
    from signals import get_signals
    return get_signals()


def tool_compare_etfs(group_name: str) -> dict:
    """ETF 그룹 비교 (예: 'S&P 500', 'AI/Robot')"""
    from compare import COMPARE_GROUPS, compare_group
    tks = COMPARE_GROUPS.get(group_name)
    if not tks:
        return {'error': f'그룹 "{group_name}" 없음. 사용 가능: {list(COMPARE_GROUPS.keys())[:10]}...'}
    result = compare_group(group_name, tks)
    if not result:
        return {'error': '데이터 없음'}
    return {
        'group': group_name,
        'date': result['date'],
        'best': result['best']['ticker'],
        'recommendation': result['recommendation'],
        'etfs': [
            {
                'ticker': e['ticker'], 'score': e['total_score'],
                'aum': e['aum'], 'expense_ratio': e['expense_ratio'],
                'day_return': e['day_return'],
            }
            for e in result['etfs']
        ],
    }


def tool_portfolio_pulse(tickers: list, shares: list = None) -> dict:
    """임시 포트폴리오 pulse — DB 등록 없이 즉시 분석"""
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    latest = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]

    if shares is None:
        shares = [1] * len(tickers)

    holdings = []
    total = 0
    for tk, sh in zip(tickers, shares):
        r = cur.execute('''
            SELECT category, price, day_return, volume_spike, aum
            FROM etf_daily WHERE ticker=? AND date=?
        ''', (tk.upper(), latest)).fetchone()
        if r:
            val = sh * r[1]
            total += val
            holdings.append({
                'ticker': tk.upper(), 'category': r[0],
                'price': r[1], 'day_return': r[2],
                'volume_spike': r[3], 'value': val,
            })
    conn.close()

    return {
        'date': latest,
        'total_value': total,
        'holdings': holdings,
        'weighted_day_return': sum(h['day_return'] * h['value'] / total for h in holdings) if total > 0 else 0,
    }


# ━━━ MCP protocol handlers ━━━

TOOLS = [
    {
        'name': 'get_etf_info',
        'description': 'ETF 단일 정보 조회 (가격, AUM, 거래량, 보유종목, 카테고리)',
        'inputSchema': {
            'type': 'object',
            'properties': {'ticker': {'type': 'string', 'description': 'ETF 티커 (예: VOO, SPY)'}},
            'required': ['ticker'],
        },
    },
    {
        'name': 'get_today_signals',
        'description': '오늘의 ETF 시장 신호 종합 (거래량 spike, 수익률 Top, 카테고리 강도)',
        'inputSchema': {'type': 'object', 'properties': {}},
    },
    {
        'name': 'compare_etfs',
        'description': 'ETF 그룹 비교 + best 추천 (예: "S&P 500", "AI/Robot", "Gold")',
        'inputSchema': {
            'type': 'object',
            'properties': {'group_name': {'type': 'string'}},
            'required': ['group_name'],
        },
    },
    {
        'name': 'portfolio_pulse',
        'description': '임시 포트폴리오 분석 (보유 ETF + 수량)',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'tickers': {'type': 'array', 'items': {'type': 'string'}},
                'shares': {'type': 'array', 'items': {'type': 'number'}},
            },
            'required': ['tickers'],
        },
    },
]


def handle_request(req):
    """MCP request handler"""
    method = req.get('method')
    if method == 'initialize':
        return {
            'protocolVersion': '2024-11-05',
            'capabilities': {'tools': {}},
            'serverInfo': {'name': 'etf-pulse', 'version': '0.1.0'},
        }
    if method == 'tools/list':
        return {'tools': TOOLS}
    if method == 'tools/call':
        params = req.get('params', {})
        name = params.get('name')
        args = params.get('arguments', {})
        if name == 'get_etf_info':
            result = tool_get_etf_info(args['ticker'])
        elif name == 'get_today_signals':
            result = tool_get_today_signals()
        elif name == 'compare_etfs':
            result = tool_compare_etfs(args['group_name'])
        elif name == 'portfolio_pulse':
            result = tool_portfolio_pulse(args['tickers'], args.get('shares'))
        else:
            return {'error': {'code': -32601, 'message': f'Unknown tool: {name}'}}
        return {'content': [{'type': 'text', 'text': json.dumps(result, ensure_ascii=False, indent=2)}]}
    return {'error': {'code': -32601, 'message': f'Unknown method: {method}'}}


def main():
    """stdio JSON-RPC loop"""
    while True:
        line = sys.stdin.readline()
        if not line: break
        try:
            req = json.loads(line)
            result = handle_request(req)
            response = {'jsonrpc': '2.0', 'id': req.get('id')}
            if 'error' in result:
                response['error'] = result['error']
            else:
                response['result'] = result
            print(json.dumps(response, ensure_ascii=False), flush=True)
        except Exception as e:
            print(json.dumps({'jsonrpc': '2.0', 'error': {'code': -32700, 'message': str(e)}}), flush=True)


if __name__ == '__main__':
    # CLI 테스트 (MCP 없이)
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--tool', help='get_etf_info / get_today_signals / compare_etfs')
    parser.add_argument('--arg', help='JSON 인자')
    parser.add_argument('--mcp', action='store_true', help='MCP stdio mode')
    args = parser.parse_args()

    if args.mcp:
        main()
    elif args.tool:
        argd = json.loads(args.arg) if args.arg else {}
        if args.tool == 'get_etf_info':
            print(json.dumps(tool_get_etf_info(argd['ticker']), ensure_ascii=False, indent=2))
        elif args.tool == 'get_today_signals':
            r = tool_get_today_signals()
            print(json.dumps({
                'date': r['date'],
                'top_returns': r['top_returns'][:3],
                'volume_spikes': r['volume_spikes'][:3],
            }, ensure_ascii=False, indent=2))
        elif args.tool == 'compare_etfs':
            print(json.dumps(tool_compare_etfs(argd['group_name']), ensure_ascii=False, indent=2))
        elif args.tool == 'portfolio_pulse':
            print(json.dumps(tool_portfolio_pulse(argd['tickers'], argd.get('shares')), ensure_ascii=False, indent=2))
    else:
        # 데모
        print('=== ETF Pulse MCP Server demo ===')
        print('\n[1] get_etf_info("VOO")')
        print(json.dumps(tool_get_etf_info('VOO'), ensure_ascii=False, indent=2)[:500])
        print('\n[2] compare_etfs("S&P 500")')
        print(json.dumps(tool_compare_etfs('S&P 500'), ensure_ascii=False, indent=2))
        print('\n[3] portfolio_pulse(["VOO", "QQQ", "GLD"], [100, 50, 20])')
        print(json.dumps(tool_portfolio_pulse(['VOO', 'QQQ', 'GLD'], [100, 50, 20]), ensure_ascii=False, indent=2))
