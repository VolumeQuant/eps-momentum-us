"""블렌드(70/30 주식+ETF) robustness 검증 — 배포 전 마지막 관문

질문: 블렌드의 위험조정 우위가 진짜냐, 강세장 운빨이냐?
 A) leave-winner-out: MU/SNDK 제외해도 블렌드가 순수 2종목 대비 우위 유지?
 B) 랜덤시작 500: 블렌드 Sharpe/Calmar가 일관되게 ≥ 순수 2종목? worst-case 개선?
블렌드 = 0.7×(2종목 80/20) + 0.3×(동적매칭 ETF SOXX/XLK). ETF는 외부(제외 무관).
"""
import sys
import json
import random
import warnings
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')
pd.set_option('future.no_silent_downcasting', True)
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from bt_c1_and_weights_robust import load_raw, build_data, rerank  # noqa
from regime_eda_market import fetch_close  # noqa
import sqlite3

W_STOCK = 0.7
N_SEEDS = 500
MIN_DAYS = 20


def stock2_daily(dates, data, price_full, weights=(80, 20), entry=30, exit_=10):
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
            if td.get(tk, {}).get('min_seg', 0) < 0:
                continue
            if td.get(tk, {}).get('price'):
                cand.append(tk)
        for si in free:
            if not cand:
                break
            portfolio[cand.pop(0)] = {'slot_idx': si, 'weight': weights[si]}
    return pd.Series(rets, index=dates)


def etf_daily(dates):
    etf = json.load(open(ROOT / 'etf_holdings_cache_v2.json', encoding='utf-8'))
    con = sqlite3.connect(ROOT / 'eps_momentum_data.db')

    def best(d):
        ts = set(r[0] for r in con.execute('SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank<=20', (d,)))
        bb, bn, bw = None, 0, 0
        for sym, info in etf.items():
            ov = ts & set(info.get('holdings', {}))
            n, w = len(ov), sum(info['holdings'][t] for t in ov)
            if n > bn or (n == bn and w > bw):
                bb, bn, bw = sym, n, w
        return bb
    match = {d: best(d) for d in dates}
    px = {s: fetch_close(s) for s in set(match.values())}
    rets = [0.0]
    for i in range(1, len(dates)):
        p = px.get(match[dates[i - 1]])
        cp = p.get(pd.Timestamp(dates[i])) if p is not None else None
        pp = p.get(pd.Timestamp(dates[i - 1])) if p is not None else None
        rets.append((cp - pp) / pp if (cp and pp and pp > 0) else 0.0)
    return pd.Series(rets, index=dates)


def metrics(daily):
    nav = (1 + daily).cumprod()
    tot = (nav.iloc[-1] - 1) * 100
    mdd = ((nav - nav.cummax()) / nav.cummax()).min() * 100
    sh = daily.mean() / daily.std() * np.sqrt(252) if daily.std() > 0 else 0
    return tot, mdd, sh, (tot / abs(mdd) if mdd < 0 else float('nan'))


def main():
    dates, raw, price_full = load_raw()
    etf_s = etf_daily(dates)

    print('=' * 76)
    print('A) leave-winner-out — 순수 2종목 vs 70/30 블렌드 (total / MDD / Sharpe / Cal)')
    print('-' * 76)
    for label, ex in [('전체', frozenset()), ('MU 제외', frozenset({'MU'})),
                      ('SNDK 제외', frozenset({'SNDK'})), ('둘다 제외', frozenset({'MU', 'SNDK'}))]:
        data = build_data(raw, ex)
        s = stock2_daily(dates, data, price_full)
        b = W_STOCK * s + (1 - W_STOCK) * etf_s
        st, sm, ss, sc = metrics(s)
        bt, bm, bs, bc = metrics(b)
        print(f'  [{label:<8}] 2종목 {st:>+7.0f}%/{sm:>+6.1f}%/Sh{ss:.2f}/Cal{sc:>5.1f}  |  '
              f'블렌드 {bt:>+7.0f}%/{bm:>+6.1f}%/Sh{bs:.2f}/Cal{bc:>5.1f}')

    print('\n' + '=' * 76)
    print('B) 랜덤시작 500 — 순수 2종목 vs 70/30 블렌드 (전체 우주)')
    data = build_data(raw, frozenset())
    s_full = stock2_daily(dates, data, price_full)
    eligible = dates[:-MIN_DAYS]
    s_mdd, b_mdd, s_sh, b_sh = [], [], [], []
    blend_better_mdd = blend_better_sh = 0
    for seed in range(N_SEEDS):
        random.seed(seed)
        sd = random.choice(eligible)
        si = dates.index(sd)
        ss_ = s_full.iloc[si:]
        es_ = etf_s.iloc[si:]
        bb_ = W_STOCK * ss_ + (1 - W_STOCK) * es_
        _, sm, ssh, _ = metrics(ss_)
        _, bm, bsh, _ = metrics(bb_)
        s_mdd.append(sm); b_mdd.append(bm); s_sh.append(ssh); b_sh.append(bsh)
        if bm > sm:
            blend_better_mdd += 1   # 블렌드 MDD 덜 깊음 (mdd는 음수, 큰 게 좋음)
        if bsh > ssh:
            blend_better_sh += 1
    print(f'  평균 MDD:   2종목 {np.mean(s_mdd):+.1f}% / 블렌드 {np.mean(b_mdd):+.1f}%  (블렌드 더 얕음 {blend_better_mdd}/{N_SEEDS})')
    print(f'  최악 MDD:   2종목 {min(s_mdd):+.1f}% / 블렌드 {min(b_mdd):+.1f}%')
    print(f'  평균 Sharpe: 2종목 {np.mean(s_sh):.2f} / 블렌드 {np.mean(b_sh):.2f}  (블렌드 더 높음 {blend_better_sh}/{N_SEEDS})')

    print('\n해석: 둘다제외에도 블렌드가 2종목 대비 MDD↓·Sharpe 유지/개선 + 랜덤시작서 '
          '블렌드가 일관되게 MDD 얕고 Sharpe 높으면 = 분산우위 진짜(강세장 운빨 아님).')


if __name__ == '__main__':
    main()
