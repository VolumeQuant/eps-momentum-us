"""ETF Pulse Dividend Tracker — 배당 ETF 일정 + 수익률 추적

배당 ETF (JEPI, JEPQ, SCHD 등)의 ex-dividend date + 배당금 추적.
사용자가 ex-date 알림 받음 + 누적 배당 수익률 추적.
"""
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = Path(__file__).parent / 'etf_pulse.db'

DIVIDEND_ETFS = {
    # 핵심 인컴 ETF
    'JEPI': '월배당, S&P 커버드콜',
    'JEPQ': '월배당, Nasdaq 커버드콜',
    'QYLD': '월배당, Nasdaq 100 buy-write',
    'XYLD': '월배당, S&P 500 buy-write',
    'DIVO': '월배당, 액티브 인컴',
    # 배당 성장
    'SCHD': '분기배당, 배당 성장',
    'VIG': '분기배당, 배당 성장 5년+',
    'NOBL': '분기배당, 배당 25년+ Aristocrats',
    'DGRO': '분기배당, 배당 성장',
    # 고배당
    'VYM': '분기배당, 고배당',
    'HDV': '분기배당, 고배당',
    'SCHY': '분기배당, 국제 고배당',
    # REIT (높은 배당)
    'VNQ': '분기배당, REIT',
    'XLRE': '분기배당, REIT 섹터',
    # 채권 (월 분배)
    'TLT': '월분배, 장기 국채',
    'HYG': '월분배, 하이일드',
    'LQD': '월분배, IG 회사채',
}


def get_dividend_info(ticker):
    """yfinance에서 배당 정보 fetch"""
    t = yf.Ticker(ticker)
    try:
        info = t.info
        divs = t.dividends.tail(12) if hasattr(t, 'dividends') else None  # 최근 12회
        cal = t.calendar  # 다음 ex-date
        return {
            'ticker': ticker,
            'description': DIVIDEND_ETFS.get(ticker, ''),
            'yield': info.get('yield') or 0,  # decimal (0.011 = 1.1%)
            'ttm_dividend': sum(divs.values) if divs is not None and len(divs) > 0 else 0,
            'last_dividend': float(divs.iloc[-1]) if divs is not None and len(divs) > 0 else 0,
            'last_div_date': str(divs.index[-1].date()) if divs is not None and len(divs) > 0 else None,
            'div_frequency': len(divs) if divs is not None else 0,  # 12회면 월배당, 4회면 분기
            'ex_div_date': str(cal.get('Ex-Dividend Date')) if cal and 'Ex-Dividend Date' in cal else None,
            'currency': info.get('currency', 'USD'),
        }
    except Exception as e:
        return {'ticker': ticker, 'error': str(e)[:80]}


def gen_dividend_calendar_md():
    """배당 ETF 일정 + 수익률 콘텐츠"""
    lines = [f'# 💰 배당 ETF 일정 + 수익률 — {datetime.now().strftime("%Y-%m-%d")}', '']
    lines.append('월배당 / 분기배당 ETF 핵심 정보. 다음 ex-dividend date 알림.')
    lines.append('')

    results = []
    for tk in DIVIDEND_ETFS.keys():
        info = get_dividend_info(tk)
        if 'error' not in info:
            results.append(info)

    # 월배당 그룹
    monthly = [r for r in results if r['div_frequency'] >= 8]  # 월배당 (대략)
    quarterly = [r for r in results if r['div_frequency'] < 8]

    lines.append('## 📅 월배당 ETF')
    lines.append('')
    lines.append('| Ticker | 설명 | 배당률 | 최근 배당금 | 최근 ex-date |')
    lines.append('|--------|------|--------|------------|---------------|')
    for r in sorted(monthly, key=lambda x: -x['yield']):
        yield_pct = r['yield'] * 100
        lines.append(f'| {r["ticker"]} | {r["description"]} | {yield_pct:.2f}% | '
                     f'${r["last_dividend"]:.4f} | {r["last_div_date"] or "-"} |')
    lines.append('')

    lines.append('## 📅 분기/연 배당 ETF')
    lines.append('')
    lines.append('| Ticker | 설명 | 배당률 | 최근 배당금 | 최근 ex-date |')
    lines.append('|--------|------|--------|------------|---------------|')
    for r in sorted(quarterly, key=lambda x: -x['yield']):
        yield_pct = r['yield'] * 100
        lines.append(f'| {r["ticker"]} | {r["description"]} | {yield_pct:.2f}% | '
                     f'${r["last_dividend"]:.4f} | {r["last_div_date"] or "-"} |')
    lines.append('')

    # Top yield
    top_yield = sorted(results, key=lambda x: -x['yield'])[:5]
    lines.append('## 🏆 배당률 Top 5')
    lines.append('')
    for r in top_yield:
        lines.append(f'- **{r["ticker"]}** ({r["description"]}): {r["yield"]*100:.2f}%')
    lines.append('')

    lines.append('---')
    lines.append('_배당 정보 자동 수집 — yfinance. ex-date 다음 매수 시 배당 미지급._')
    lines.append('_월배당 ETF는 매월 ex-date 1~2일 전 매수해야 받음._')
    return '\n'.join(lines)


if __name__ == '__main__':
    md = gen_dividend_calendar_md()
    out = Path(__file__).parent / 'content' / 'dividend_calendar.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(md)
    print(f'\n저장: {out}')
