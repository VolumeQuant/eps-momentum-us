"""ETF Pulse 단위 테스트 — 기본 무결성 검증"""
import sys
import sqlite3
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def test_db_exists():
    assert DB_PATH.exists(), 'DB 파일 없음'
    print('  ✓ DB 파일 존재')


def test_db_tables():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    expected = {'etf_daily', 'etf_holdings_daily', 'etf_holdings_changes', 'etf_news', 'daily_content'}
    assert expected.issubset(tables), f'테이블 누락: {expected - tables}'
    conn.close()
    print(f'  ✓ 모든 테이블 존재 ({len(tables)}개)')


def test_data_loaded():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    n = cur.execute('SELECT COUNT(*) FROM etf_daily').fetchone()[0]
    assert n > 0, 'etf_daily 데이터 없음'
    n_dates = cur.execute('SELECT COUNT(DISTINCT date) FROM etf_daily').fetchone()[0]
    assert n_dates > 0, '날짜 데이터 없음'
    n_tickers = cur.execute('SELECT COUNT(DISTINCT ticker) FROM etf_daily').fetchone()[0]
    assert n_tickers > 100, f'ETF 수 부족: {n_tickers}'
    conn.close()
    print(f'  ✓ 데이터 적재: {n} rows, {n_dates}일, {n_tickers} ETF')


def test_universe_consistent():
    from etf_universe import get_all_etfs, get_category
    etfs = get_all_etfs()
    assert len(etfs) > 200, f'universe 너무 작음: {len(etfs)}'
    # 카테고리 매핑
    for tk, cat in etfs[:5]:
        assert get_category(tk) == cat
    print(f'  ✓ universe {len(etfs)}개 + 카테고리 매핑 OK')


def test_signals_runs():
    from signals import get_signals
    s = get_signals()
    assert 'date' in s
    assert 'top_returns' in s
    assert 'volume_spikes' in s
    assert 'category_strength' in s
    print(f'  ✓ signals 모듈 작동 (date={s["date"]})')


def test_narrative_runs():
    from signals import get_signals
    from narrative import gen_narrative_kr
    md = gen_narrative_kr(get_signals())
    assert len(md) > 500, 'narrative 너무 짧음'
    assert 'ETF Pulse' in md
    print(f'  ✓ narrative 생성 OK ({len(md)} chars)')


def test_compare_runs():
    from compare import compare_group, COMPARE_GROUPS
    r = compare_group('S&P 500', COMPARE_GROUPS['S&P 500'])
    assert r is not None
    assert 'best' in r
    assert r['best']['ticker'] in COMPARE_GROUPS['S&P 500']
    print(f'  ✓ compare 작동: S&P 500 best = {r["best"]["ticker"]}')


def test_portfolio_analyzer():
    from portfolio_analyzer import gen_portfolio_analysis_md
    md = gen_portfolio_analysis_md(['VOO', 'QQQ', 'GLD'], [0.5, 0.3, 0.2])
    assert '분산 점수' in md
    print(f'  ✓ portfolio_analyzer 작동')


def test_advanced_signals():
    from advanced_signals import etf_metrics, portfolio_bt
    m = etf_metrics('VOO')
    assert m and m['ticker'] == 'VOO'
    p = portfolio_bt(['VOO', 'QQQ', 'GLD'])
    assert p and 'sharpe' in p
    print(f'  ✓ advanced_signals 작동 (VOO Sharpe {m["sharpe"]:.2f})')


def test_mcp_tools():
    from mcp_server import tool_get_etf_info, tool_compare_etfs
    info = tool_get_etf_info('SPY')
    assert info and info['ticker'] == 'SPY'
    cmp_ = tool_compare_etfs('Gold')
    assert cmp_ and 'best' in cmp_
    print(f'  ✓ MCP tools OK')


def test_publisher_safe():
    from publisher import md_to_telegram_html
    html = md_to_telegram_html('# Test\n\n**bold** text\n- list')
    assert '<b>' in html
    print(f'  ✓ publisher format conversion OK')


def main():
    tests = [
        ('DB 파일', test_db_exists),
        ('DB 테이블', test_db_tables),
        ('데이터 적재', test_data_loaded),
        ('universe', test_universe_consistent),
        ('signals', test_signals_runs),
        ('narrative', test_narrative_runs),
        ('compare', test_compare_runs),
        ('portfolio_analyzer', test_portfolio_analyzer),
        ('advanced_signals', test_advanced_signals),
        ('MCP tools', test_mcp_tools),
        ('publisher', test_publisher_safe),
    ]
    print('=' * 60)
    print('ETF Pulse 단위 테스트')
    print('=' * 60)
    passed = 0; failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            print(f'  ✗ {name}: {e}')
            failed += 1
        except Exception as e:
            print(f'  ✗ {name}: {type(e).__name__}: {e}')
            failed += 1
    print('=' * 60)
    print(f'결과: {passed}/{len(tests)} 통과')
    return failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
