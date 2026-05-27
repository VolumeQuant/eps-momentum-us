"""Phase 5 — 위험조정 종합 + 2종목/ETF 블렌드

일별 수익 시리즈로 Sharpe + 블렌드(2종목 × w + 동적ETF × (1-w)) 비교.
질문: 블렌드가 순수 2종목보다 위험조정(Sharpe/Calmar) 개선?
"""
import sys
import json
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from bt_c1_and_weights_robust import load_raw, build_data, rerank  # noqa
from regime_eda_market import fetch_close  # noqa
import sqlite3


def stock2_daily(dates, data, price_full, weights=(80, 20), entry=30, exit_=10):
    """2종목 80/20 일별 수익 시리즈 (full window)."""
    slots = len(weights)
    portfolio, consec, rets = {}, defaultdict(int), []
    for di, today in enumerate(dates):
        td = data.get(today, {})
        nr = rerank(td, None, 0)
        nc = defaultdict(int)
        for tk, r in nr.items():
            if r <= 30:
                nc[tk] = consec.get(tk, 0) + 1
        consec = nc
        dr_ = 0
        if portfolio and di > 0:
            pv = dates[di - 1]
            for tk, pi in portfolio.items():
                cp = td.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pp = data.get(pv, {}).get(tk, {}).get('price') or price_full.get(pv, {}).get(tk)
                if cp and pp and pp > 0:
                    dr_ += (pi['weight'] / 100) * (cp - pp) / pp
        rets.append(dr_)
        for tk in list(portfolio):
            if td.get(tk, {}).get('min_seg', 0) < -2 or nr.get(tk) is None or nr.get(tk) > exit_:
                del portfolio[tk]
        used = {p['slot_idx'] for p in portfolio.values()}
        free = sorted(i for i in range(slots) if i not in used)
        cand = []
        for tk, r in sorted(nr.items(), key=lambda x: x[1]):
            if r > entry:
                break
            if tk in portfolio or consec.get(tk, 0) < 3:
                continue
            info = td.get(tk, {})
            if info.get('min_seg', 0) < 0:
                continue
            if info.get('price'):
                cand.append(tk)
        for si in free:
            if not cand:
                break
            tk = cand.pop(0)
            portfolio[tk] = {'slot_idx': si, 'weight': weights[si]}
    return pd.Series(rets, index=dates)


def metrics(daily):
    nav = (1 + daily).cumprod()
    tot = (nav.iloc[-1] - 1) * 100
    mdd = ((nav - nav.cummax()) / nav.cummax()).min() * 100
    sharpe = daily.mean() / daily.std() * np.sqrt(252) if daily.std() > 0 else 0
    cal = (tot) / abs(mdd) if mdd < 0 else float('nan')
    return tot, mdd, sharpe, cal


def main():
    dates, raw, price_full = load_raw()
    data = build_data(raw)
    etf = json.load(open(ROOT / 'etf_holdings_cache_v2.json', encoding='utf-8'))
    con = sqlite3.connect(ROOT / 'eps_momentum_data.db')

    def best_etf(d):
        ts = set(r[0] for r in con.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank<=20', (d,)))
        best, bn, bw = None, 0, 0
        for sym, info in etf.items():
            ov = ts & set(info.get('holdings', {}))
            n, w = len(ov), sum(info['holdings'][t] for t in ov)
            if n > bn or (n == bn and w > bw):
                best, bn, bw = sym, n, w
        return best
    match = {d: best_etf(d) for d in dates}
    pxc = {s: fetch_close(s) for s in set(match.values())}

    s2 = stock2_daily(dates, data, price_full)
    etf_d = []
    for i, d in enumerate(dates):
        if i == 0:
            etf_d.append(0.0); continue
        sym = match[dates[i - 1]]
        p = pxc.get(sym)
        cp = p.get(pd.Timestamp(d)) if p is not None else None
        pp = p.get(pd.Timestamp(dates[i - 1])) if p is not None else None
        etf_d.append((cp - pp) / pp if (cp and pp and pp > 0) else 0.0)
    etf_s = pd.Series(etf_d, index=dates)

    print(f'{"방식":<22}{"총수익":>9}{"MDD":>8}{"Sharpe":>8}{"Calmar":>8}')
    print('-' * 56)
    for name, dy in [('2종목 80/20', s2), ('동적매칭 ETF', etf_s)]:
        t, m, sh, c = metrics(dy)
        print(f'{name:<22}{t:>+8.1f}%{m:>+7.1f}%{sh:>8.2f}{c:>8.2f}')
    print('-- 블렌드 (2종목 × w + ETF × (1-w)) --')
    for w in (0.7, 0.5, 0.3):
        blend = w * s2 + (1 - w) * etf_s
        t, m, sh, c = metrics(blend)
        print(f'{f"  {int(w*100)}/{int((1-w)*100)} 주식/ETF":<22}{t:>+8.1f}%{m:>+7.1f}%{sh:>8.2f}{c:>8.2f}')

    print('\n해석: Sharpe/Calmar 최고가 어디인지. 블렌드가 순수 2종목보다 위험조정 개선되면 '
          '"개별+ETF 혼합"이 분산 가치. 강세장 단일표본 한계는 동일.')


if __name__ == '__main__':
    main()
