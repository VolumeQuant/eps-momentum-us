# -*- coding: utf-8 -*-
"""US 본질형 vs Catalyst형 분리 BT
정의:
- 본질형 (Fundamental growth): AI/Tech sub-cycle 노출 (반도체/장비/계측/하드웨어/통신장비/전자부품/전기장비)
- Catalyst형: 외 sector (어닝 surprise·M&A·계약 등 catalyst-driven)

분류 후:
- 시스템 picks 중 각 type 비율
- 각 type의 누적 수익률
- production 룰 (rank≤3 entry, rank>10 exit, 2step_t15) 그대로
"""
import sys, sqlite3, json
from pathlib import Path
import pandas as pd
import numpy as np
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
TI_PATH = ROOT / 'ticker_info_cache.json'

# 본질형 (AI/Tech sub-cycle 노출) industry
FUNDAMENTAL_INDUSTRIES = {
    '반도체', '반도체장비', '계측기기', '하드웨어', '통신장비',
    '전자부품', '전기장비', '응용SW', '인프라SW',
}

# Catalyst-only로 처리 (entertainment, retail, finance 등)
# 나머지 industry는 자동으로 catalyst형


def load_data():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, score, eps_chg_weighted, adj_gap,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   rev_growth, operating_margin, num_analysts, rev_up30
            FROM ntm_screening WHERE date=?''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[6:11])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'score': r[3] or 0,
                'eps_w': r[4] or 0, 'adj_gap': r[5] or 0,
                'min_seg': min(segs) if segs else 0,
                'rev_growth': r[11], 'op_margin': r[12],
                'num_analysts': r[13], 'rev_up30': r[14],
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    with open(TI_PATH, encoding='utf-8') as f:
        ti = json.load(f)
    return dates, data, price_full, ti


def get_type(ticker, ti_cache):
    info = ti_cache.get(ticker, {})
    industry = info.get('industry', '기타')
    if industry in FUNDAMENTAL_INDUSTRIES:
        return 'Fundamental'
    return 'Catalyst'


def simulate_with_filter(dates, data, price_full, ti, allow_types, slots=2, entry=3, exit_=10):
    """allow_types 종목만 매수 허용"""
    held = {}
    trades = []
    value = 1.0
    cash = 1.0
    prev_held = None

    for i, d in enumerate(dates):
        dd = data[d]
        # Carry
        if prev_held and i > 0:
            d_prev = dates[i-1]
            n = len(prev_held)
            if n > 0:
                ret = 0
                # equal weight for simplicity (slot 2면 50/50)
                w_each = 1.0 / n
                for tk in prev_held:
                    pp = price_full[d_prev].get(tk); pn = price_full[d].get(tk, pp)
                    if pp and pn: ret += w_each * (pn/pp - 1)
                value *= (1 + ret)

        # Exits
        for tk in list(held):
            info = dd.get(tk)
            if info is None or info['p2'] is None or info['p2'] > exit_ or (info['min_seg'] is not None and info['min_seg'] < -2):
                sell_p = price_full[d].get(tk, held[tk][1])
                ret = (sell_p / held[tk][1] - 1) * 100
                trades.append({
                    'ticker': tk, 'buy_date': held[tk][0], 'sell_date': d,
                    'buy_p': held[tk][1], 'sell_p': sell_p, 'ret_pct': ret,
                    'type': get_type(tk, ti)
                })
                del held[tk]

        # Entries
        if len(held) < slots:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > entry: continue
                if tk in held: continue
                if info['min_seg'] is not None and info['min_seg'] < 0: continue
                if not info['price']: continue
                t_type = get_type(tk, ti)
                if t_type not in allow_types: continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            for _, _, tk in cands[:slots - len(held)]:
                held[tk] = (d, dd[tk]['price'])

        prev_held = dict(held)

    cum = (value - 1) * 100
    return cum, trades


def main():
    print('=' * 100)
    print('US 본질형 (AI/Tech) vs Catalyst형 분리 BT')
    print('=' * 100)
    dates, data, price_full, ti = load_data()
    print(f'data: {len(dates)} dates ({dates[0]} ~ {dates[-1]})\n')

    # 1. 시스템 picks 전체 종목별 type 분류
    print('--- 1. 74일 BT 시스템 picks (part2_rank ≤ 3 등장 종목) ---')
    pick_count = defaultdict(int)
    for d in dates:
        for tk, info in data[d].items():
            if info['p2'] is not None and info['p2'] <= 3:
                pick_count[tk] += 1

    fund_picks = []
    cata_picks = []
    for tk, count in sorted(pick_count.items(), key=lambda x: -x[1]):
        info = ti.get(tk, {})
        industry = info.get('industry', '기타')
        t = get_type(tk, ti)
        if t == 'Fundamental':
            fund_picks.append((tk, count, industry))
        else:
            cata_picks.append((tk, count, industry))

    print(f'\n  본질형 (AI/Tech): {len(fund_picks)}종목')
    for tk, c, ind in fund_picks[:15]:
        print(f'    {tk:<7}{c}회  industry: {ind}')
    print(f'\n  Catalyst형: {len(cata_picks)}종목')
    for tk, c, ind in cata_picks[:15]:
        print(f'    {tk:<7}{c}회  industry: {ind}')

    # 2. 각 type의 거래 P&L
    print('\n--- 2. 종합 BT (slot 2, entry 3, exit 10) ---')
    print(f'{"variant":<28}{"return":>10}{"trades":>9}{"avg ret%":>10}{"win%":>8}')
    for name, allow in [
        ('Baseline (둘 다 매수)', ['Fundamental', 'Catalyst']),
        ('Fundamental만', ['Fundamental']),
        ('Catalyst만', ['Catalyst']),
    ]:
        cum, trades = simulate_with_filter(dates, data, price_full, ti, allow)
        if trades:
            wins = sum(1 for t in trades if t['ret_pct'] > 0)
            avg = np.mean([t['ret_pct'] for t in trades])
            print(f'  {name:<28}{cum:>+9.1f}%{len(trades):>9}{avg:>+9.1f}%{wins/len(trades)*100:>7.0f}%')

    # 3. Type별 상세 trade 분석 (baseline에서)
    print('\n--- 3. Baseline BT의 trade type별 분해 ---')
    _, all_trades = simulate_with_filter(dates, data, price_full, ti, ['Fundamental', 'Catalyst'])
    by_type = defaultdict(list)
    for tr in all_trades:
        by_type[tr['type']].append(tr)

    print(f'{"type":<15}{"trades":>9}{"wins":>10}{"win%":>8}{"avg ret%":>10}{"median%":>10}{"max%":>10}{"min%":>10}')
    for t in ['Fundamental', 'Catalyst']:
        trs = by_type.get(t, [])
        if not trs: continue
        wins = sum(1 for x in trs if x['ret_pct'] > 0)
        avg = np.mean([x['ret_pct'] for x in trs])
        med = np.median([x['ret_pct'] for x in trs])
        mx = max([x['ret_pct'] for x in trs])
        mn = min([x['ret_pct'] for x in trs])
        print(f'  {t:<13}{len(trs):>9}{wins:>10}{wins/len(trs)*100:>7.0f}%{avg:>+9.1f}%{med:>+9.1f}%{mx:>+9.1f}%{mn:>+9.1f}%')

    # 4. Type별 개별 trade
    print(f'\n--- 4. Baseline 거래 종목별 ({len(all_trades)}건) ---')
    print(f'{"종목":<7}{"type":<13}{"sector":<15}{"buy_d":<12}{"sell_d":<12}{"buy$":>9}{"sell$":>9}{"ret%":>8}')
    for tr in sorted(all_trades, key=lambda x: x['buy_date']):
        ind = ti.get(tr['ticker'], {}).get('industry', '기타')[:14]
        flag = '✅' if tr['ret_pct'] > 0 else '❌'
        print(f'  {tr["ticker"]:<7}{tr["type"]:<13}{ind:<15}{tr["buy_date"]:<12}{tr["sell_date"]:<12}'
              f'{tr["buy_p"]:>9.2f}{tr["sell_p"]:>9.2f}{tr["ret_pct"]:>+7.1f}% {flag}')

    # 5. 종목별 누적 P&L
    print('\n--- 5. 종목별 누적 P&L (type 표시) ---')
    by_ticker = defaultdict(lambda: {'trades': 0, 'wins': 0, 'cum_pl': 0, 'type': '', 'industry': ''})
    for tr in all_trades:
        t = tr['ticker']
        by_ticker[t]['trades'] += 1
        if tr['ret_pct'] > 0: by_ticker[t]['wins'] += 1
        by_ticker[t]['cum_pl'] += tr['ret_pct']
        by_ticker[t]['type'] = tr['type']
        by_ticker[t]['industry'] = ti.get(t, {}).get('industry', '기타')

    sorted_t = sorted(by_ticker.items(), key=lambda x: -x[1]['cum_pl'])
    print(f'{"종목":<7}{"type":<13}{"industry":<15}{"trades":>8}{"wins":>8}{"cum ret%":>12}')
    for tk, d in sorted_t:
        wr = f'{d["wins"]}/{d["trades"]}'
        print(f'  {tk:<7}{d["type"]:<13}{d["industry"][:14]:<15}{d["trades"]:>8}{wr:>8}{d["cum_pl"]:>+11.1f}%')


if __name__ == '__main__':
    main()
