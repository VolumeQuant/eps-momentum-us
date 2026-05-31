"""ETF Pulse Compare — 동일 카테고리 ETF 자동 비교 도구

retail의 가장 큰 pain: "VOO vs IVV vs SPY 뭐가 좋아?"
→ 한 검색으로 alternatives 자동 비교 + 한 줄 추천
"""
import sys
import sqlite3
from pathlib import Path
import statistics

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'


# 비교 그룹 — 같은 지수/테마 추적
COMPARE_GROUPS = {
    'S&P 500': ['VOO', 'IVV', 'SPY', 'SPLG'],
    'Nasdaq 100': ['QQQ', 'QQQM'],
    'Total Market': ['VTI', 'ITOT', 'SCHB'],
    'Small Cap': ['IWM', 'IJR', 'VB', 'SCHA'],
    'Mid Cap': ['IJH', 'MDY', 'VO', 'SCHM'],
    'Semiconductor': ['SOXX', 'SMH', 'XSD'],
    'AI/Robot': ['BOTZ', 'ROBO', 'IRBO', 'AIQ'],
    'Clean Energy': ['TAN', 'PBW', 'QCLN', 'ICLN'],
    'Cybersecurity': ['CIBR', 'HACK'],
    'Bitcoin Spot': ['IBIT', 'FBTC'],
    'Gold': ['GLD', 'IAU', 'GLDM'],
    'Silver': ['SLV', 'SIVR'],
    'Long Treasury': ['TLT', 'EDV', 'VGLT', 'TLH'],
    'Short Treasury': ['SHY', 'VGSH', 'BIL', 'SHV'],
    'IG Corp Bond': ['LQD', 'VCIT', 'VCLT', 'VCSH'],
    'High Yield': ['HYG', 'JNK', 'USHY'],
    'Dividend Growth': ['SCHD', 'VIG', 'DGRO', 'NOBL'],
    'High Dividend': ['VYM', 'HDV', 'SCHY'],
    'Covered Call Income': ['JEPI', 'JEPQ', 'QYLD', 'XYLD'],
    'Emerging Markets': ['VWO', 'IEMG', 'EEM', 'SCHE'],
    'China': ['FXI', 'MCHI', 'KWEB', 'ASHR'],
    'Energy': ['XLE', 'VDE', 'IYE'],
    'Technology': ['XLK', 'VGT', 'IYW', 'FTEC'],
    'Financials': ['XLF', 'VFH', 'IYF'],
    'Healthcare': ['XLV', 'VHT', 'IYH'],
    'REIT': ['VNQ', 'XLRE', 'IYR', 'SCHH'],
    'Uranium/Nuclear': ['URA', 'NLR'],
    'EV / Auto': ['DRIV', 'IDRV', 'KARS'],
    'NVDA Leverage': ['NVDL', 'NVDX', 'NVDS'],
    'TSLA Leverage': ['TSLL', 'TSLZ'],
}


def get_etf_compare_data(ticker, date_str):
    """단일 ETF 비교 데이터"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    r = cur.execute('''
        SELECT price, volume, avg_volume_30d, aum, day_return,
               expense_ratio, dividend_yield, beta
        FROM etf_daily WHERE ticker=? AND date=?
    ''', (ticker, date_str)).fetchone()
    conn.close()
    if not r:
        return None
    return {
        'ticker': ticker,
        'price': r[0],
        'volume': r[1],
        'avg_volume_30d': r[2],
        'aum': r[3] or 0,
        'day_return': r[4] or 0,
        'expense_ratio': r[5] or 0,
        'dividend_yield': r[6] or 0,
        'beta': r[7] or 0,
    }


def compare_group(group_name, tickers, date_str=None):
    """그룹 내 ETF 비교 + 한 줄 추천"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if not date_str:
        date_str = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    conn.close()

    etfs = []
    for tk in tickers:
        data = get_etf_compare_data(tk, date_str)
        if data:
            etfs.append(data)
    if not etfs:
        return None

    # 점수 계산 — AUM(유동성), 운용보수(낮을수록), 거래량
    for e in etfs:
        # AUM 점수 (log scale로 정규화)
        aum_score = min(100, (e['aum'] / 1e9) ** 0.5 * 20) if e['aum'] > 0 else 0
        # 운용보수 점수 (낮을수록 좋음, 0~100)
        exp_score = max(0, 100 - e['expense_ratio'] * 10000) if e['expense_ratio'] > 0 else 50
        # 거래량 점수 (avg_volume 기준)
        vol_score = min(100, (e['avg_volume_30d'] / 1e6) ** 0.5 * 10) if e['avg_volume_30d'] > 0 else 0
        # 종합
        e['total_score'] = (aum_score * 0.5 + exp_score * 0.3 + vol_score * 0.2)

    etfs.sort(key=lambda x: -x['total_score'])

    # 한 줄 추천
    best = etfs[0]
    reasons = []
    if best['aum'] == max(e['aum'] for e in etfs):
        reasons.append(f'AUM 1위 ${best["aum"]/1e9:.1f}B')
    valid_exp = [e['expense_ratio'] for e in etfs if e['expense_ratio'] > 0]
    if valid_exp and best['expense_ratio'] == min(valid_exp) and best['expense_ratio'] > 0:
        reasons.append(f'수수료 최저 {best["expense_ratio"]*100:.3f}%')
    if best['avg_volume_30d'] == max(e['avg_volume_30d'] for e in etfs):
        reasons.append('유동성 1위')

    return {
        'group': group_name,
        'date': date_str,
        'etfs': etfs,
        'best': best,
        'recommendation': ', '.join(reasons),
    }


def gen_compare_markdown(group_name, tickers, date_str=None):
    """비교 결과를 Markdown으로"""
    result = compare_group(group_name, tickers, date_str)
    if not result:
        return f'# {group_name}\n\n데이터 없음'

    lines = [f'# {group_name} 비교 — {result["date"]}', '']
    lines.append(f'**추천: {result["best"]["ticker"]}** — {result["recommendation"]}')
    lines.append('')
    lines.append('| 순위 | Ticker | 종합 점수 | AUM | 운용보수 | 30일 평균거래량 | 어제 수익률 | 배당률 |')
    lines.append('|------|--------|-----------|-----|----------|----------------|-------------|--------|')
    for i, e in enumerate(result['etfs'], 1):
        divy_str = f'{e["dividend_yield"]*100:.2f}%' if e["dividend_yield"] else '-'
        exp_str = f'{e["expense_ratio"]*100:.3f}%' if e["expense_ratio"] else '-'
        lines.append(f'| {i} | **{e["ticker"]}** | {e["total_score"]:.1f} | '
                     f'${e["aum"]/1e9:.1f}B | {exp_str} | '
                     f'{e["avg_volume_30d"]/1e6:.1f}M | {e["day_return"]:+.2f}% | {divy_str} |')
    lines.append('')
    lines.append('## 분석')
    lines.append('')
    # 차이점 자동 분석
    if len(result['etfs']) >= 2:
        max_aum = max(e['aum'] for e in result['etfs'])
        min_aum = min(e['aum'] for e in result['etfs'] if e['aum'] > 0)
        if max_aum > min_aum * 3 and min_aum > 0:
            lines.append(f'- AUM 격차 큼: {max_aum/min_aum:.1f}x (유동성/안정성 차이 존재)')
        valid_e = [e['expense_ratio'] for e in result['etfs'] if e['expense_ratio'] > 0]
        if valid_e:
            max_exp = max(valid_e); min_exp = min(valid_e)
            if max_exp > min_exp * 1.5 and min_exp > 0:
                lines.append(f'- 수수료 격차: {min_exp*100:.3f}% ~ {max_exp*100:.3f}% (저비용 ETF 선호 권장)')
        if all(abs(e['day_return']) < 0.1 for e in result['etfs']):
            lines.append('- 모든 ETF 수익률 거의 동일 → 추적오차 작음 (정상)')
    lines.append('')

    return '\n'.join(lines)


def all_groups_summary(date_str=None):
    """모든 비교 그룹 best ETF 요약"""
    lines = ['# ETF 카테고리별 Best (Michelin-style)', '']
    for group, tks in COMPARE_GROUPS.items():
        result = compare_group(group, tks, date_str)
        if result:
            b = result['best']
            lines.append(f'## {group}')
            lines.append(f'**1위: {b["ticker"]}** (점수 {b["total_score"]:.1f}) — {result["recommendation"]}')
            if len(result['etfs']) >= 2:
                others = ', '.join([e['ticker'] for e in result['etfs'][1:4]])
                lines.append(f'  대안: {others}')
            lines.append('')
    return '\n'.join(lines)


if __name__ == '__main__':
    # 샘플 비교 (S&P 500)
    print(gen_compare_markdown('S&P 500', COMPARE_GROUPS['S&P 500']))
    print('\n' + '='*80 + '\n')
    # 카테고리별 요약 일부
    summary = all_groups_summary()
    out = Path(__file__).parent / 'content' / 'compare_all_groups.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(summary, encoding='utf-8')
    print(f'전체 그룹 요약 저장: {out}')
    # 5개만 미리보기
    print('\n[미리보기: 첫 5 그룹]')
    print('\n'.join(summary.split('\n')[:50]))
