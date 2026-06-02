# -*- coding: utf-8 -*-
"""US 본질형 vs Catalyst형 paired BT — 정량 분류 + v84 production simulator
정의 (정량):
- 본질형: rev_growth ≥ 25% (지속 성장)
- Catalyst형: rev_growth < 25%

Simulator: v84 entry_fixed sticky slot (production 정합)
- slot 2, entry≤3, exit>10, min_seg ≥ 0, 3-day ✅ verified
- dd_30_25 진입필터, 2step_t15 dynamic weight
- 100 seeds × 3 samples paired BT
"""
import sys, sqlite3, random, statistics, math, time
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10

REV_GROWTH_THRESHOLD = 0.25  # 본질형 = rev_growth ≥ 25%


def load_data():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    data = {}
    rev_growth_map = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, composite_rank, price, score, adj_gap,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   rev_growth, num_analysts, high30
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
                'p2': r[1], 'cr': r[2], 'price': r[3], 'score': r[4] or 0,
                'adj_gap': r[5] or 0,
                'min_seg': min(segs) if segs else 0,
                'rev_growth': r[11], 'num_analysts': r[12], 'high30': r[13],
            }
            # rev_growth는 종목별 거의 변동 없음. 최근값으로 종목별 record
            if r[11] is not None:
                rev_growth_map[tk] = r[11]
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full, rev_growth_map


def get_type(ticker, rev_growth_map):
    rg = rev_growth_map.get(ticker)
    if rg is None: return 'Unknown'
    if rg >= REV_GROWTH_THRESHOLD: return 'Fundamental'
    return 'Catalyst'


def verified_cr(t, i, dates, data):
    """cr Top 30 for i, i-1, i-2 (3-day ✅)"""
    for j in (i, i-1, i-2):
        if j < 0: return False
        info = data[dates[j]].get(t)
        if not info or info.get('cr') is None or info['cr'] > 30: return False
    return True


def simulate(dates, data, price_full, rev_growth_map,
             allow_types=('Fundamental', 'Catalyst'),
             slots=2, entry=3, exit_=10,
             start_idx=0, use_dd_30_25=True, use_2step=True):
    """v84 entry_fixed sticky simulator with type filter"""
    held = {}  # ticker -> (entry_date, entry_price, slot_idx, weight)
    prev_held = None
    value = 1.0; peak = 1.0; mdd = 0.0
    daily_rets = []
    trades_log = []

    for i in range(start_idx, len(dates)):
        d = dates[i]
        # Carry portfolio with sticky weights
        if prev_held and i > start_idx:
            d_prev = dates[i-1]
            ret = 0
            for tk, (ed, ep, sidx, w) in prev_held.items():
                pp = price_full[d_prev].get(tk); pn = price_full[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            value *= (1 + ret)
            daily_rets.append(ret)
            peak = max(peak, value); mdd = max(mdd, (peak-value)/peak)

        dd = data[d]
        # Exits
        for tk in list(held):
            info = dd.get(tk)
            ep = held[tk][1]
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > exit_:
                sell_p = (info.get('price') if info else None) or price_full[d].get(tk, ep)
                ret_pct = (sell_p / ep - 1) * 100
                reason = 'NULL' if (info is None or p2 is None) else 'rank>{}'.format(exit_)
                trades_log.append({
                    'ticker': tk, 'buy_date': held[tk][0], 'sell_date': d,
                    'buy_p': ep, 'sell_p': sell_p, 'ret_pct': ret_pct,
                    'type': get_type(tk, rev_growth_map),
                    'reason': reason
                })
                del held[tk]
            elif info.get('min_seg') is not None and info['min_seg'] < -2:
                sell_p = info['price'] or ep
                ret_pct = (sell_p / ep - 1) * 100
                trades_log.append({
                    'ticker': tk, 'buy_date': held[tk][0], 'sell_date': d,
                    'buy_p': ep, 'sell_p': sell_p, 'ret_pct': ret_pct,
                    'type': get_type(tk, rev_growth_map), 'reason': 'min_seg<-2'
                })
                del held[tk]

        # Entries
        if len(held) < slots:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > entry: continue
                if tk in held: continue
                if info.get('min_seg') is not None and info['min_seg'] < 0: continue
                if not info['price']: continue
                if not verified_cr(tk, i, dates, data): continue
                # dd_30_25
                if use_dd_30_25 and info.get('high30') and info['price']:
                    if info['price'] / info['high30'] - 1 < -0.25: continue
                # type filter
                t_type = get_type(tk, rev_growth_map)
                if t_type not in allow_types: continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            picked = cands[:slots]
            if len(picked) == 1:
                _, _, tk = picked[0]
                held[tk] = (d, dd[tk]['price'], 0, 1.0)
            elif len(picked) >= 2:
                if use_2step:
                    s1, s2 = picked[0][1], picked[1][1]
                    gap = s1 - s2
                    weights = [1.0, 0.0] if gap >= 15 else [0.5, 0.5]
                else:
                    weights = [0.5, 0.5]
                for idx_s, (_, _, tk) in enumerate(picked[:2]):
                    if weights[idx_s] > 0:
                        held[tk] = (d, dd[tk]['price'], idx_s, weights[idx_s])
        prev_held = dict(held)

    cum = (value - 1) * 100
    return {'cum': cum, 'mdd': mdd*100, 'trades': trades_log,
            'daily_rets': daily_rets}


def metrics_from_result(r):
    if not r['daily_rets'] or len(r['daily_rets']) < 2:
        return r['cum'], r['mdd'], 0, 0
    mu = statistics.mean(r['daily_rets'])
    sd = statistics.stdev(r['daily_rets']) if len(r['daily_rets']) > 1 else 1e-9
    sh = (mu*252)/(sd*math.sqrt(252)) if sd > 0 else 0
    cal = r['cum']/r['mdd'] if r['mdd'] > 0 else 0
    return r['cum'], r['mdd'], sh, cal


def main():
    print('=' * 100)
    print('US 본질형 vs Catalyst형 paired BT (정량 정의: rev_growth ≥ 25%)')
    print(f'Simulator: v84 entry_fixed sticky + dd_30_25 + 2step_t15')
    print(f'Paired: {N_SEEDS} seeds × {SAMPLES_PER_SEED} samples')
    print('=' * 100)
    dates, data, price_full, rev_growth_map = load_data()
    print(f'data: {len(dates)} dates ({dates[0]} ~ {dates[-1]})')

    # Type breakdown
    fund_tickers = [tk for tk, rg in rev_growth_map.items() if rg >= REV_GROWTH_THRESHOLD]
    cata_tickers = [tk for tk, rg in rev_growth_map.items() if rg < REV_GROWTH_THRESHOLD]
    print(f'\nUniverse 분류 (rev_growth ≥ 25% = 본질형):')
    print(f'  본질형: {len(fund_tickers)} 종목')
    print(f'  Catalyst형: {len(cata_tickers)} 종목')

    # Paired BT
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(range(len(eligible)), SAMPLES_PER_SEED))

    print('\n--- Paired BT 결과 (100 × 3 = 300 runs) ---')
    print(f'{"variant":<28}{"avg":>10}{"med":>9}{"mdd":>9}{"sharpe":>9}{"cal":>8}  vs baseline')
    print('-' * 90)

    variants = [
        ('Baseline (둘 다)', ['Fundamental', 'Catalyst']),
        ('본질형만', ['Fundamental']),
        ('Catalyst형만', ['Catalyst']),
    ]
    all_results = {}
    for name, allow in variants:
        all_rets, all_mdds, seed_avgs = [], [], []
        for chosen in seed_starts:
            sr = []
            for s_idx in chosen:
                r = simulate(dates, data, price_full, rev_growth_map,
                             allow_types=allow, start_idx=s_idx)
                all_rets.append(r['cum']); all_mdds.append(r['mdd'])
                sr.append(r['cum'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[name] = {'rets': all_rets, 'mdds': all_mdds, 'seed_avgs': seed_avgs}
        avg = sum(all_rets)/len(all_rets)
        med = sorted(all_rets)[len(all_rets)//2]
        mdd = max(all_mdds)
        std = statistics.pstdev(all_rets)
        sh = avg/std if std > 0 else 0
        cal = avg/mdd if mdd > 0 else 0
        marker = ' ★' if name == 'Baseline (둘 다)' else '  '
        print(f'{marker}{name:<26}{avg:>+9.1f}%{med:>+8.1f}%{mdd:>+8.2f}%{sh:>8.2f}{cal:>8.2f}')

    # Paired lift
    base = all_results['Baseline (둘 다)']['seed_avgs']
    print('\n--- Paired (vs baseline) ---')
    print(f'  {"variant":<26}{"avg_lift":>11}{"med_lift":>11}{"min":>10}{"max":>10}{"wins":>10}  verdict')
    print('  ' + '-' * 90)
    for name, _ in variants:
        if name == 'Baseline (둘 다)': continue
        new = all_results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_lift = sum(lifts)/len(lifts)
        med_lift = statistics.median(lifts)
        verdict = ('✓✓ 우월' if wins >= 70 else
                   '✓ 우월' if wins >= 60 else
                   '~ 동등' if wins >= 40 else
                   '✗ 열세')
        print(f'  {name:<26}{avg_lift:>+9.2f}%p{med_lift:>+9.2f}%p{min(lifts):>+9.2f}%p{max(lifts):>+9.2f}%p{wins:>6}/{N_SEEDS}  {verdict}')

    # Production picks 분포 (74일 기준)
    print('\n--- 시스템 picks (rank ≤ 3 등장 종목) 분류 ---')
    pick_count = defaultdict(int)
    for d in dates:
        for tk, info in data[d].items():
            if info['p2'] is not None and info['p2'] <= 3:
                pick_count[tk] += 1
    fund_picks = []; cata_picks = []
    for tk, c in sorted(pick_count.items(), key=lambda x: -x[1]):
        t = get_type(tk, rev_growth_map)
        rg = rev_growth_map.get(tk)
        rg_str = f'{rg*100:.0f}%' if rg is not None else 'N/A'
        if t == 'Fundamental': fund_picks.append((tk, c, rg_str))
        elif t == 'Catalyst': cata_picks.append((tk, c, rg_str))

    print(f'\n  본질형 (rev_growth ≥ 25%): {len(fund_picks)} 종목')
    for tk, c, rg in fund_picks[:15]:
        print(f'    {tk:<7}{c:>3}회   rev_g {rg}')
    print(f'\n  Catalyst형 (rev_growth < 25%): {len(cata_picks)} 종목')
    for tk, c, rg in cata_picks[:15]:
        print(f'    {tk:<7}{c:>3}회   rev_g {rg}')

    # Trade-level type breakdown (baseline)
    print('\n--- Baseline의 trade-level 종합 (start_idx=0, 즉 full 74일) ---')
    r_full = simulate(dates, data, price_full, rev_growth_map,
                      allow_types=['Fundamental', 'Catalyst'], start_idx=0)
    by_type = defaultdict(list)
    for tr in r_full['trades']:
        by_type[tr['type']].append(tr)

    print(f'  {"type":<15}{"trades":>8}{"wins":>8}{"win%":>8}{"avg%":>10}{"max%":>10}{"min%":>10}')
    for t in ['Fundamental', 'Catalyst', 'Unknown']:
        trs = by_type.get(t, [])
        if not trs: continue
        wins = sum(1 for x in trs if x['ret_pct'] > 0)
        avg = statistics.mean([x['ret_pct'] for x in trs])
        mx = max(x['ret_pct'] for x in trs)
        mn = min(x['ret_pct'] for x in trs)
        print(f'  {t:<15}{len(trs):>8}{wins:>8}{wins/len(trs)*100:>7.0f}%{avg:>+9.1f}%{mx:>+9.1f}%{mn:>+9.1f}%')

    print(f'\n  Full BT (start_idx=0):  cum {r_full["cum"]:+.1f}%, MDD {r_full["mdd"]:.2f}%, 거래 {len(r_full["trades"])}건')


if __name__ == '__main__':
    main()
