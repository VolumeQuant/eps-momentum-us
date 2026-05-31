"""ETF Pulse English Narrative — for global audience (X/Twitter, English Substack)"""
import sys
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from signals import get_signals
from narrative import detect_patterns as _detect_patterns_kr


CATEGORY_EN = {
    'core_us': 'US Core Index',
    'international': 'International / EM',
    'sectors': 'Sector',
    'themes': 'Theme',
    'bonds': 'Bonds',
    'commodity_hedge': 'Commodity / Hedge',
    'income_lev': 'Income / Leverage',
}


ETF_DESC_EN = {
    'SPY': 'S&P 500', 'VOO': 'S&P 500', 'IVV': 'S&P 500',
    'QQQ': 'Nasdaq 100', 'QQQM': 'Nasdaq 100 Mini',
    'SOXX': 'Semiconductors', 'SMH': 'Semiconductors', 'XSD': 'Semis (equal-weight)',
    'SOXL': 'Semi 3X Leveraged', 'TQQQ': 'Nasdaq 3X Leveraged',
    'ARKK': 'ARK Innovation', 'ARKG': 'ARK Genomics', 'ARKW': 'ARK Next Internet',
    'ARKF': 'ARK Fintech', 'ARKQ': 'ARK Autonomous/Robotics', 'ARKX': 'ARK Space',
    'IBIT': 'Bitcoin (BlackRock)', 'FBTC': 'Bitcoin (Fidelity)',
    'MSTU': 'MSTR 2X Leveraged', 'MSTZ': 'MSTR 2X Inverse',
    'NVDL': 'NVDA 2X Leveraged', 'NVDS': 'NVDA Inverse',
    'GLD': 'Gold', 'IAU': 'Gold (cheaper)', 'SLV': 'Silver',
    'TLT': 'Long Treasury', 'IEF': 'Intermediate Treasury', 'SHY': 'Short Treasury',
    'HYG': 'High Yield Bonds', 'LQD': 'Investment Grade Corp',
    'JEPI': 'Covered Call Income (large)', 'JEPQ': 'Covered Call Income (Nasdaq)',
    'CIBR': 'Cybersecurity', 'HACK': 'Cybersecurity',
    'TAN': 'Solar', 'ICLN': 'Clean Energy',
    'GDX': 'Gold Miners', 'GDXJ': 'Junior Gold Miners',
    'URA': 'Uranium', 'NLR': 'Nuclear',
    'KWEB': 'China Internet',
    'EWY': 'South Korea', 'EWJ': 'Japan',
    'XLE': 'Energy', 'XLF': 'Financials', 'XLV': 'Healthcare', 'XLK': 'Technology',
}


def describe_en(tk):
    desc = ETF_DESC_EN.get(tk)
    return f'**{tk}** ({desc})' if desc else f'**{tk}**'


def detect_patterns_en(signals):
    insights = []
    top = signals['top_returns']
    cat_count = {}
    for s in top:
        cat_count[s['category']] = cat_count.get(s['category'], 0) + 1
    for cat, n in cat_count.items():
        if n >= 2:
            insights.append(f'{n} ETFs in **{CATEGORY_EN.get(cat, cat)}** category in top-5 returns → theme rotation signal')

    spike_tks = [s['ticker'] for s in signals['volume_spikes']]
    theme_clusters = {
        'Cybersecurity': ['CIBR', 'HACK'],
        'Semis': ['SOXX', 'SMH', 'SOXL', 'XSD'],
        'ARK': ['ARKK', 'ARKG', 'ARKW', 'ARKF', 'ARKQ', 'ARKX'],
        'Clean Energy': ['TAN', 'ICLN', 'QCLN', 'PBW'],
        'Gold': ['GLD', 'IAU', 'GDX', 'GDXJ'],
        'China': ['FXI', 'KWEB', 'MCHI', 'CWEB'],
        'Bitcoin': ['IBIT', 'FBTC', 'BITO', 'BITQ'],
    }
    for theme, tks in theme_clusters.items():
        match = sum(1 for tk in tks if tk in spike_tks)
        if match >= 2:
            insights.append(f'**{theme}** ETFs ({match}) had simultaneous volume spikes')
        ret_match = sum(1 for tk in tks if tk in [s['ticker'] for s in signals['top_returns']])
        if ret_match >= 2:
            insights.append(f'**{theme}** ETFs ({ret_match}) up simultaneously')

    new_high_tks = [h['ticker'] for h in signals['new_highs']]
    ark_highs = [tk for tk in new_high_tks if tk.startswith('ARK')]
    if len(ark_highs) >= 3:
        insights.append(f'**ARK series** {len(ark_highs)} ETFs at 30-day highs ({", ".join(ark_highs)}) → growth strong')

    bond_spikes = [s for s in signals['volume_spikes'] if s['category'] == 'bonds']
    if bond_spikes:
        insights.append(f'**Bond ETFs** ({len(bond_spikes)}) volume spike → safe-haven flow')

    cs = signals['category_strength']
    if cs and cs[-1]['category'] == 'commodity_hedge' and cs[-1]['avg_return'] < -0.3:
        insights.append(f'**Commodity/hedge** weak ({cs[-1]["avg_return"]:+.2f}%) → risk-on mood')

    return insights


def gen_narrative_en(signals):
    date = signals['date']
    lines = []
    lines.append(f'# 🌅 ETF Pulse — {date}')
    lines.append('')
    lines.append(f'US ETF market signals (228 ETFs by AUM).')
    lines.append('')

    cs = signals['category_strength']
    if cs:
        strong = cs[0]
        weak = cs[-1]
        lines.append('## 🎯 Market Mood')
        lines.append('')
        lines.append(f'**Strong**: {CATEGORY_EN.get(strong["category"], strong["category"])} (avg {strong["avg_return"]:+.2f}%)')
        lines.append(f'**Weak**: {CATEGORY_EN.get(weak["category"], weak["category"])} (avg {weak["avg_return"]:+.2f}%)')
        lines.append('')
        for c in cs:
            sign = '🟢' if c['avg_return'] >= 0.1 else '🔴' if c['avg_return'] <= -0.1 else '⚪'
            lines.append(f'- {sign} {CATEGORY_EN.get(c["category"], c["category"])}: {c["avg_return"]:+.2f}% ({c["n_etfs"]} ETFs)')
        lines.append('')

    if signals['top_returns']:
        lines.append('## 📈 Top 5 Returns')
        lines.append('')
        for s in signals['top_returns']:
            spike = f' (vol {s["spike"]:.1f}x)' if s['spike'] > 1.5 else ''
            lines.append(f'- {describe_en(s["ticker"])}: {s["day_return"]:+.2f}%{spike} — AUM ${s["aum_b"]:.1f}B')
        lines.append('')

    if signals['bottom_returns']:
        lines.append('## 📉 Bottom 5 Returns')
        lines.append('')
        for s in signals['bottom_returns']:
            lines.append(f'- {describe_en(s["ticker"])}: {s["day_return"]:+.2f}% — AUM ${s["aum_b"]:.1f}B')
        lines.append('')

    if signals['volume_spikes']:
        lines.append('## 🔥 Volume Spikes (>1.5x 30-day avg)')
        lines.append('')
        for s in signals['volume_spikes']:
            ret = f' ({s["day_return"]:+.2f}%)' if abs(s['day_return']) > 0.5 else ''
            lines.append(f'- {describe_en(s["ticker"])}: {s["spike"]:.2f}x{ret}')
        lines.append('')

    if signals['momentum_5d']:
        lines.append('## 🚀 5-Day Momentum Top 5')
        lines.append('')
        for s in signals['momentum_5d']:
            lines.append(f'- {describe_en(s["ticker"])}: {s["return_5d"]:+.2f}% (5d)')
        lines.append('')

    if signals['new_highs']:
        lines.append('## 🏔️ 30-Day New Highs')
        lines.append('')
        for h in signals['new_highs'][:5]:
            lines.append(f'- {describe_en(h["ticker"])} — ${h["price"]:.2f}')
        lines.append('')

    if signals['new_lows']:
        lines.append('## 🏞️ 30-Day New Lows')
        lines.append('')
        for h in signals['new_lows'][:5]:
            lines.append(f'- {describe_en(h["ticker"])} — ${h["price"]:.2f}')
        lines.append('')

    insights = detect_patterns_en(signals)
    if insights:
        lines.append('## 🧠 Auto Insights')
        lines.append('')
        for ins in insights:
            lines.append(f'- {ins}')
        lines.append('')

    lines.append('---')
    lines.append('')
    lines.append('_ETF Pulse — automated data analysis. Not investment advice._')
    lines.append('')
    lines.append('Data: yfinance | Universe: 228 US ETFs by AUM | Updated daily')

    return '\n'.join(lines)


if __name__ == '__main__':
    signals = get_signals()
    md = gen_narrative_en(signals)
    out = Path(__file__).parent / 'content' / f'pulse_{signals["date"]}_en.md'
    out.parent.mkdir(exist_ok=True)
    out.write_text(md, encoding='utf-8')
    print(md)
    print(f'\nSaved: {out}')
