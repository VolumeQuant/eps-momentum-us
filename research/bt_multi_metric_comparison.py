"""다양한 지표로 70/30 b=0 vs 80/20 + C1 b=5 비교

측정 지표:
1. 다양한 multistart 개수 (6, 12, 18, 24, 30)
2. Sharpe ratio (위험조정)
3. Sortino ratio (하방 위험만)
4. Trade-level win rate
5. Profit factor
6. Average holding period
7. Max consecutive losses
8. Drawdown duration (MDD 회복 시간)
9. Day return 분포 (skewness, kurtosis)
10. Calmar ratio (수익/MDD)
"""
import sys
import sqlite3
import random
import statistics
from pathlib import Path
from collections import defaultdict
import math

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

MIN_HOLD_DAYS = 10
LOOKBACK = 30

sys.path.insert(0, str(ROOT))


def load_all_with_score(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    import daily_runner as dr
    score_cache = {}
    for d in dates:
        try:
            _, sm = dr._build_score_100_map(d)
            score_cache[d] = sm
        except Exception:
            score_cache[d] = {}
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'price': r[2], 'eps_w': r[3],
                'min_seg': min(segs) if segs else 0,
                'score_100': score_cache.get(d, {}).get(tk, 0),
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def get_price_30d(tk, today, dates_list, price_full):
    if today not in dates_list: return None
    di = dates_list.index(today)
    if di < LOOKBACK: return None
    past_d = dates_list[di - LOOKBACK]
    past_p = price_full.get(past_d, {}).get(tk)
    cur_p = price_full.get(today, {}).get(tk)
    if past_p and cur_p and past_p > 0:
        return (cur_p - past_p) / past_p * 100
    return None


def is_c1(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None or eps_w <= 0: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return p30 > 0


def rerank(today, today_data, c1_boost, dates_list, price_full):
    if c1_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c1_now = is_c1(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c1_boost if is_c1_now else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate_full(dates_all, data, price_full, weights, entry, exit_, c1_boost, start_date=None):
    """실제 운용 BT + trade history 기록"""
    slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    trades = []  # 매도 시 기록
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c1_boost, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c1_boost, dates_all, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    w = info['weight'] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if min_seg < -2 or rank is None or rank > exit_:
                if cur_p:
                    info = portfolio[tk]
                    ret = (cur_p - info['entry_price']) / info['entry_price'] * 100
                    days = di - info['entry_di']
                    trades.append({'tk': tk, 'ret': ret, 'days': days, 'weight': info['weight']})
                exited.append(tk)
        for tk in exited: del portfolio[tk]
        used_slots = {info['slot_idx'] for info in portfolio.values()}
        free_slots = sorted([i for i in range(slots) if i not in used_slots])
        cands = []
        for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
            if new_r > entry: break
            if tk in portfolio: continue
            if consecutive.get(tk, 0) < 3: continue
            info = today_data.get(tk, {})
            min_seg = info.get('min_seg', 0)
            if min_seg < 0: continue
            price = info.get('price')
            if price and price > 0:
                cands.append((tk, price))
        for slot_idx in free_slots:
            if not cands: break
            tk, price = cands.pop(0)
            portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx, 'weight': weights[slot_idx], 'entry_di': di}
    # Open positions
    last_d = dates[-1]
    for tk, info in portfolio.items():
        final_p = data[last_d].get(tk, {}).get('price') or price_full.get(last_d, {}).get(tk)
        if final_p:
            ret = (final_p - info['entry_price']) / info['entry_price'] * 100
            days = len(dates) - info['entry_di'] - 1
            trades.append({'tk': tk, 'ret': ret, 'days': days, 'weight': info['weight'], 'open': True})

    cum = 1.0; peak = 1.0; max_dd = 0; mdd_start = None; mdd_end = None; in_dd = False; cur_dd_start = None
    drawdown_durations = []
    for i, r in enumerate(daily_returns):
        cum *= (1 + r/100)
        if cum >= peak:
            peak = cum
            if in_dd:
                drawdown_durations.append(i - cur_dd_start)
                in_dd = False
        else:
            if not in_dd:
                cur_dd_start = i
                in_dd = True
            dd = (cum - peak) / peak * 100
            if dd < max_dd:
                max_dd = dd
    return {
        'total_return': (cum-1)*100, 'max_dd': max_dd,
        'daily_returns': daily_returns, 'trades': trades,
        'drawdown_durations': drawdown_durations,
    }


def compute_metrics(daily_returns, trades, max_dd, drawdown_durations):
    if not daily_returns: return {}
    n = len(daily_returns)
    avg = sum(daily_returns)/n
    std = statistics.pstdev(daily_returns) if n > 1 else 0
    downside = [r for r in daily_returns if r < 0]
    downside_std = statistics.pstdev(downside) if len(downside) > 1 else 0
    sharpe = (avg/std * math.sqrt(252)) if std > 0 else 0  # annualized
    sortino = (avg/downside_std * math.sqrt(252)) if downside_std > 0 else 0
    # Trade level
    n_trades = len(trades)
    wins = [t for t in trades if t['ret'] > 0]
    losses = [t for t in trades if t['ret'] <= 0]
    win_rate = len(wins)/n_trades * 100 if n_trades > 0 else 0
    avg_win = sum(t['ret'] for t in wins)/len(wins) if wins else 0
    avg_loss = sum(t['ret'] for t in losses)/len(losses) if losses else 0
    profit_factor = abs(sum(t['ret'] for t in wins)/sum(t['ret'] for t in losses)) if losses and sum(t['ret'] for t in losses) != 0 else float('inf')
    avg_hold = sum(t['days'] for t in trades)/n_trades if n_trades > 0 else 0
    # Max consecutive losses
    consec_loss = 0
    max_consec_loss = 0
    for t in trades:
        if t['ret'] <= 0:
            consec_loss += 1
            max_consec_loss = max(max_consec_loss, consec_loss)
        else:
            consec_loss = 0
    # Drawdown duration
    avg_dd_duration = sum(drawdown_durations)/len(drawdown_durations) if drawdown_durations else 0
    max_dd_duration = max(drawdown_durations) if drawdown_durations else 0
    # Calmar (annualized)
    cum = 1.0
    for r in daily_returns: cum *= (1+r/100)
    total_ret = (cum-1)*100
    days_per_year = 252
    annualized = ((cum) ** (days_per_year/n) - 1) * 100 if n > 0 else 0
    calmar = annualized/abs(max_dd) if max_dd < 0 else 0
    return {
        'sharpe': sharpe, 'sortino': sortino, 'win_rate': win_rate,
        'profit_factor': profit_factor, 'avg_hold': avg_hold,
        'max_consec_loss': max_consec_loss, 'avg_dd_duration': avg_dd_duration,
        'max_dd_duration': max_dd_duration, 'annualized': annualized,
        'calmar': calmar, 'n_trades': n_trades, 'avg_win': avg_win, 'avg_loss': avg_loss,
        'total_return': total_ret, 'max_dd': max_dd,
    }


def main():
    print('=' * 110)
    print('70/30 b=0 vs 80/20 + C1 b=5 — 다양한 지표 종합 비교')
    print('=' * 110)
    dates, data, price_full = load_all_with_score(DB_PATH)
    eligible = dates[:-MIN_HOLD_DAYS]

    BASE = ([33, 34, 33], 30, 10, 0)
    OPT_A = ([70, 30], 30, 10, 0)  # 70/30 b=0
    OPT_B = ([80, 20], 30, 10, 5)  # 80/20 + C1 boost=5

    # ============================
    # 1. 다양한 multistart 개수
    # ============================
    print('\n[1] 다양한 multistart 개수별 비교 (lift = config - baseline)')
    print('-' * 100)
    multistart_counts = [6, 12, 18, 24, 30, 36]
    print(f'{"N starts":<10} {"A lift avg":>12} {"A wins":>10} {"A min":>10} {"B lift avg":>12} {"B wins":>10} {"B min":>10}')
    for n in multistart_counts:
        step = max(1, len(eligible) // (n + 1))
        starts = [eligible[step * i] for i in range(1, n + 1)]
        base_rets = [simulate_full(dates, data, price_full, *BASE, start_date=sd)['total_return'] for sd in starts]
        a_rets = [simulate_full(dates, data, price_full, *OPT_A, start_date=sd)['total_return'] for sd in starts]
        b_rets = [simulate_full(dates, data, price_full, *OPT_B, start_date=sd)['total_return'] for sd in starts]
        a_lifts = [a - b_ for a, b_ in zip(a_rets, base_rets)]
        b_lifts = [a - b_ for a, b_ in zip(b_rets, base_rets)]
        a_avg = sum(a_lifts)/len(a_lifts); a_wins = sum(1 for l in a_lifts if l > 0); a_min = min(a_lifts)
        b_avg = sum(b_lifts)/len(b_lifts); b_wins = sum(1 for l in b_lifts if l > 0); b_min = min(b_lifts)
        print(f'  N={n:<3} {a_avg:+10.2f}%p {a_wins:>5}/{n:<3} {a_min:+8.2f}%p {b_avg:+10.2f}%p {b_wins:>5}/{n:<3} {b_min:+8.2f}%p')

    # ============================
    # 2. 다양한 random seed (50, 100, 500, 1000)
    # ============================
    print('\n[2] 다양한 random seed 수별 lift')
    print('-' * 100)
    print(f'{"N seeds":<10} {"A R lift":>12} {"A wins%":>10} {"B R lift":>12} {"B wins%":>10}')
    for n_seeds in [50, 100, 500, 1000]:
        a_lifts = []
        b_lifts = []
        for seed_i in range(n_seeds):
            random.seed(seed_i)
            chosen = random.sample(eligible, 3)
            base_avg = sum(simulate_full(dates, data, price_full, *BASE, start_date=sd)['total_return'] for sd in chosen)/3
            a_avg = sum(simulate_full(dates, data, price_full, *OPT_A, start_date=sd)['total_return'] for sd in chosen)/3
            b_avg = sum(simulate_full(dates, data, price_full, *OPT_B, start_date=sd)['total_return'] for sd in chosen)/3
            a_lifts.append(a_avg - base_avg)
            b_lifts.append(b_avg - base_avg)
        a_l = sum(a_lifts)/len(a_lifts); a_w = sum(1 for l in a_lifts if l > 0)
        b_l = sum(b_lifts)/len(b_lifts); b_w = sum(1 for l in b_lifts if l > 0)
        print(f'  N={n_seeds:<5} {a_l:+10.2f}%p {a_w/n_seeds*100:>9.1f}% {b_l:+10.2f}%p {b_w/n_seeds*100:>9.1f}%')

    # ============================
    # 3. 시작일=처음 단일 시뮬 + 풍부한 metric
    # ============================
    print('\n[3] 시작일=처음 단일 시뮬 — 풍부한 metric')
    print('-' * 100)
    res_base = simulate_full(dates, data, price_full, *BASE)
    res_a = simulate_full(dates, data, price_full, *OPT_A)
    res_b = simulate_full(dates, data, price_full, *OPT_B)
    m_base = compute_metrics(res_base['daily_returns'], res_base['trades'], res_base['max_dd'], res_base['drawdown_durations'])
    m_a = compute_metrics(res_a['daily_returns'], res_a['trades'], res_a['max_dd'], res_a['drawdown_durations'])
    m_b = compute_metrics(res_b['daily_returns'], res_b['trades'], res_b['max_dd'], res_b['drawdown_durations'])

    metric_names = [
        ('total_return', '누적 수익률', '%', 1),
        ('annualized', '연환산 수익률', '%', 1),
        ('max_dd', 'Worst MDD', '%', 1),
        ('sharpe', 'Sharpe ratio', '', 2),
        ('sortino', 'Sortino ratio', '', 2),
        ('calmar', 'Calmar ratio', '', 2),
        ('n_trades', 'Trade 수', '', 0),
        ('win_rate', 'Trade 승률', '%', 1),
        ('avg_win', '평균 승 수익', '%', 1),
        ('avg_loss', '평균 패 손실', '%', 1),
        ('profit_factor', 'Profit factor', '', 2),
        ('avg_hold', '평균 보유 일수', 'd', 1),
        ('max_consec_loss', '최대 연속 손실', '', 0),
        ('avg_dd_duration', '평균 DD 회복 (거래일)', 'd', 1),
        ('max_dd_duration', '최대 DD 회복 (거래일)', 'd', 0),
    ]
    print(f'{"지표":<25} {"baseline":>15} {"A (70/30 b=0)":>18} {"B (80/20+C1=5)":>18} {"A vs base":>15} {"B vs base":>15}')
    for key, name, unit, prec in metric_names:
        b_v = m_base.get(key, 0); a_v = m_a.get(key, 0); b2_v = m_b.get(key, 0)
        a_diff = a_v - b_v
        b_diff = b2_v - b_v
        fmt = f'{{:+.{prec}f}}{unit}' if prec > 0 else f'{{:.{prec}f}}{unit}'
        if 'rate' in key or '%' in unit:
            fmt_v = f'{{:+.{prec}f}}%' if prec > 0 else f'{{:.{prec}f}}%'
        print(f'  {name:<23} {fmt.format(b_v):>15} {fmt.format(a_v):>18} {fmt.format(b2_v):>18} {fmt.format(a_diff):>15} {fmt.format(b_diff):>15}')

    # ============================
    # 4. 두 후보 trade-by-trade 상세
    # ============================
    print('\n[4] Trade-by-trade 상세 — 시작일=처음')
    print('-' * 100)
    print('\n[OPT A] 70/30 b=0 trades:')
    print(f'{"tk":<7} {"ret":>9} {"days":>5} {"weight":>7}')
    for t in res_a['trades']:
        marker = ' (open)' if t.get('open') else ''
        print(f'  {t["tk"]:<7} {t["ret"]:+8.2f}% {t["days"]:>4} {t["weight"]:>5}% {marker}')
    print('\n[OPT B] 80/20 + C1 b=5 trades:')
    for t in res_b['trades']:
        marker = ' (open)' if t.get('open') else ''
        print(f'  {t["tk"]:<7} {t["ret"]:+8.2f}% {t["days"]:>4} {t["weight"]:>5}% {marker}')

    # ============================
    # 5. Day return 분포 (skewness, kurtosis 같이)
    # ============================
    print('\n[5] Day return 분포 통계')
    print('-' * 100)
    def skew_kurt(rets):
        n = len(rets)
        if n < 3: return 0, 0
        avg = sum(rets)/n
        var = sum((r-avg)**2 for r in rets)/n
        std = math.sqrt(var) if var > 0 else 0
        if std == 0: return 0, 0
        skew = sum((r-avg)**3 for r in rets)/(n*std**3)
        kurt = sum((r-avg)**4 for r in rets)/(n*std**4) - 3
        return skew, kurt
    print(f'{"":<20} {"baseline":>12} {"A":>12} {"B":>12}')
    for label, rets in [('baseline', res_base['daily_returns']), ('A 70/30 b=0', res_a['daily_returns']), ('B 80/20+C1=5', res_b['daily_returns'])]:
        avg = sum(rets)/len(rets)
        std = statistics.pstdev(rets)
        sk, kt = skew_kurt(rets)
        max_r = max(rets); min_r = min(rets)
        print(f'  {label:<18}', end='')
        print(f' avg={avg:+.3f}% std={std:.3f}% skew={sk:+.2f} kurt={kt:+.2f} max={max_r:+.2f}% min={min_r:+.2f}%')


if __name__ == '__main__':
    main()
