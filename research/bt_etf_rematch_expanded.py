"""확장 유니버스 + 개선 scoring 으로 top-20 관련 ETF 재매칭

기존 23 ETF + 신규 SPDR(XSW/XNTK/XITK/MDY) merge.
scoring 2가지:
  - count: top-20 중 ETF가 보유한 개수 (→ 광범위 ETF MDY 편향, 의미 약함)
  - weighted: ETF 비중합 (ETF가 우리 top-20으로 얼마나 채워졌나, 의미 있음)
각 scoring 동적매칭 백테스트 → Phase3(23개 count, SOXX/XLK +70%) 대비.
"""
import sys
import json
import warnings
from pathlib import Path
from collections import Counter
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')
pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from regime_eda_market import fetch_close  # noqa
import sqlite3

EXPANDED = ROOT / 'research' / 'etf_holdings_expanded.json'


def build_expanded():
    cache = json.load(open(ROOT / 'etf_holdings_cache_v2.json', encoding='utf-8'))
    if EXPANDED.exists():
        return json.load(open(EXPANDED, encoding='utf-8'))
    from etf_scraper import ETFScraper
    s = ETFScraper()
    for tk, nm in [('XSW', 'SPDR S&P Software & Services'), ('XNTK', 'SPDR NYSE Technology'),
                   ('XITK', 'SPDR FactSet Innovative Tech'), ('MDY', 'SPDR S&P MidCap 400')]:
        try:
            df = s.query_holdings(tk)
            h = {}
            for _, r in df.iterrows():
                t, w = r.get('ticker', ''), r.get('weight', 0)
                if isinstance(t, str) and len(t) <= 6 and w and w > 0:
                    h[t] = w / 100
            cache[tk] = {'name': nm, 'holdings': h}
            print(f'  +{tk}: {len(h)} holdings')
        except Exception as e:
            print(f'  {tk} FAIL {str(e)[:40]}')
    json.dump(cache, open(EXPANDED, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    return cache


def main():
    etf = build_expanded()
    print(f'유니버스: {len(etf)} ETF')
    con = sqlite3.connect(ROOT / 'eps_momentum_data.db')
    dates = [r[0] for r in con.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]

    def top20(d):
        return set(r[0] for r in con.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank<=20', (d,)))

    def best(d, mode):
        ts = top20(d)
        bb, bs = None, -1
        for sym, info in etf.items():
            h = info.get('holdings', {})
            ov = ts & set(h)
            score = len(ov) if mode == 'count' else sum(h[t] for t in ov)
            if score > bs:
                bb, bs = sym, score
        return bb

    for mode in ('count', 'weighted'):
        match = {d: best(d, mode) for d in dates}
        dist = Counter(match.values())
        print(f'\n[{mode}] 매칭 분포: {dict(dist.most_common(6))}')
        syms = set(match.values())
        px = {s: fetch_close(s) for s in syms}
        rets = []
        for i in range(1, len(dates)):
            sym = match[dates[i - 1]]
            p = px.get(sym)
            cp = p.get(pd.Timestamp(dates[i])) if p is not None else None
            pp = p.get(pd.Timestamp(dates[i - 1])) if p is not None else None
            rets.append((cp - pp) / pp if (cp and pp and pp > 0) else 0.0)
        nav = (1 + pd.Series(rets)).cumprod()
        tot = (nav.iloc[-1] - 1) * 100
        mdd = ((nav - nav.cummax()) / nav.cummax()).min() * 100
        print(f'[{mode}] 동적매칭: 총수익 {tot:+.1f}% / MDD {mdd:+.1f}%')

    print('\n참고: Phase3(23개,count) SOXX/XLK +70.1%/-10.3% | 2종목 +223.6% | Top-20바스켓 +53.9%')
    print('해석: weighted가 ETF를 우리 종목으로 실제 채워진 것 기준 = 의미있는 매칭. '
          'count는 MDY 같은 광범위ETF로 샐 수 있음(개수만 많고 비중 미미).')


if __name__ == '__main__':
    main()
