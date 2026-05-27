"""Phase 3 — top-20 holdings 동적 매칭 ETF 매매 (사용자 literal 아이디어)

매일 top-20 종목과 가장 많이 겹치는 ETF를 골라 그걸 보유 (개별종목 대신).
overlap = ETF holdings에 든 top-20 종목 수 (동률 시 ETF 내 비중합 큰 쪽).
비교: 동적매칭ETF / 고정 SMH·SOXX / Top-20 바스켓 / 2종목.
"""
import sys
import json
from pathlib import Path
from collections import Counter
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from regime_eda_market import fetch_close  # noqa
import sqlite3


def main():
    etf = json.load(open(ROOT / 'etf_holdings_cache_v2.json', encoding='utf-8'))
    con = sqlite3.connect(ROOT / 'eps_momentum_data.db')
    dates = [r[0] for r in con.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]

    def top20(d):
        return [r[0] for r in con.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL AND part2_rank<=20 ORDER BY part2_rank', (d,))]

    def best_etf(tickers):
        ts = set(tickers)
        best, best_n, best_w = None, 0, 0
        for sym, info in etf.items():
            h = info.get('holdings', {})
            ov = ts & set(h.keys())
            n = len(ov)
            w = sum(h[t] for t in ov)
            if n > best_n or (n == best_n and w > best_w):
                best, best_n, best_w = sym, n, w
        return best, best_n

    # 일별 best-match ETF
    matches = {}
    for d in dates:
        sym, n = best_etf(top20(d))
        matches[d] = sym
    dist = Counter(matches.values())
    print('일별 best-match ETF 분포:', dict(dist.most_common()))

    # 매칭된 ETF + 비교군 가격
    syms = sorted(set(matches.values()) | {'SMH', 'SOXX', 'QQQ'})
    px = {}
    for s in syms:
        p = fetch_close(s)
        if p is not None:
            px[s] = p

    w0, w1 = dates[0], dates[-1]

    def etf_buyhold(sym):
        seg = px[sym].loc[w0:w1]
        nav = seg / seg.iloc[0]
        return (nav.iloc[-1] - 1) * 100, ((nav - nav.cummax()) / nav.cummax()).min() * 100

    def dynamic_match():
        rets = []
        for i in range(1, len(dates)):
            d, pd_ = dates[i], dates[i - 1]
            sym = matches[pd_]  # 전일 top-20 기준 ETF 선택
            if sym in px:
                cp, pp = px[sym].get(pd.Timestamp(d)), px[sym].get(pd.Timestamp(pd_))
                if cp and pp and pp > 0:
                    rets.append((cp - pp) / pp)
                    continue
            rets.append(0.0)
        nav = (1 + pd.Series(rets)).cumprod()
        return (nav.iloc[-1] - 1) * 100, ((nav - nav.cummax()) / nav.cummax()).min() * 100

    print(f'\nwindow {w0}~{w1}')
    print(f'{"방식":<24}{"총수익":>9}{"MDD":>9}')
    print('-' * 44)
    dt, dm = dynamic_match()
    print(f'{"동적매칭 ETF":<24}{dt:>+8.1f}%{dm:>+8.1f}%')
    for s in ('SMH', 'SOXX', 'QQQ'):
        if s in px:
            t, m = etf_buyhold(s)
            print(f'{("고정 "+s):<24}{t:>+8.1f}%{m:>+8.1f}%')
    print('\n참고: 2종목 +223.6% / Top-20 바스켓 +53.9% (Phase 1)')
    print('해석: 동적매칭이 고정 SMH보다 나으면 top-20 추적 가치. '
          '대부분 반도체ETF로 수렴하면 사실상 섹터ETF = Top-20 바스켓 근사.')


if __name__ == '__main__':
    main()
