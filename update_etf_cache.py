"""ETF 전체 홀딩 캐시 갱신 — etf-scraper 기반 (SPDR + iShares)

Usage: python update_etf_cache.py
"""
import json
import warnings
import sys
from pathlib import Path

warnings.filterwarnings('ignore')

ETF_NAMES = {
    # Semiconductor
    'SOXX': 'iShares Semiconductor ETF',
    'XSD': 'SPDR S&P Semiconductor ETF',
    # Bank
    'KBE': 'SPDR S&P Bank ETF',
    'KRE': 'SPDR S&P Regional Banking ETF',
    # Healthcare
    'XLV': 'Health Care Select Sector SPDR',
    'IHI': 'iShares U.S. Medical Devices ETF',
    'XBI': 'SPDR S&P Biotech ETF',
    'XHE': 'SPDR S&P Health Care Equipment ETF',
    # Aerospace/Defense
    'ITA': 'iShares U.S. Aerospace & Defense ETF',
    'XAR': 'SPDR S&P Aerospace & Defense ETF',
    # Industrial
    'XLI': 'Industrial Select Sector SPDR',
    # Consumer
    'XLY': 'Consumer Discretionary Select Sector SPDR',
    'XLP': 'Consumer Staples Select Sector SPDR',
    'XRT': 'SPDR S&P Retail ETF',
    # Financial
    'XLF': 'Financial Select Sector SPDR',
    # Energy
    'XLE': 'Energy Select Sector SPDR',
    'XOP': 'SPDR S&P Oil & Gas Exploration ETF',
    # Technology
    'XLK': 'Technology Select Sector SPDR',
    # Communication
    'XLC': 'Communication Services Select Sector SPDR',
    # Materials
    'XLB': 'Materials Select Sector SPDR',
    'XME': 'SPDR S&P Metals & Mining ETF',
    # Real Estate
    'XLRE': 'Real Estate Select Sector SPDR',
    # Utilities
    'XLU': 'Utilities Select Sector SPDR',
}


def main():
    from etf_scraper import ETFScraper
    scraper = ETFScraper()

    cache = {}
    failed = []

    for ticker, name in ETF_NAMES.items():
        try:
            df = scraper.query_holdings(ticker)
            holdings = {}
            for _, row in df.iterrows():
                t = row.get('ticker', '')
                w = row.get('weight', 0)
                if t and isinstance(t, str) and len(t) <= 6 and w > 0:
                    holdings[t] = round(w / 100, 6)  # percent → decimal
            cache[ticker] = {
                'name': name,
                'holdings': holdings,
            }
            print(f'  {ticker:6s} OK  {len(holdings):4d} holdings')
        except Exception as e:
            failed.append(ticker)
            print(f'  {ticker:6s} FAIL {str(e)[:50]}')

    out_path = Path(__file__).parent / 'etf_holdings_cache_v2.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

    print(f'\nSaved {len(cache)} ETFs → {out_path.name}')
    if failed:
        print(f'Failed: {failed}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
