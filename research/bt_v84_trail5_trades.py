"""v84 + trail_5 정밀 sweet spot + trade history

검증 1: trail anchor 미세 grid (4.0, 4.5, 5.0, 5.5, 6.0)
검증 2: trail_5 trade history (단일 시작일 + 10개 시작일)
"""
import sys
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr
import bt_dynamic_sweetspot as sw
import bt_v84_comprehensive as v84c

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def simulate_with_trades_trail(dates, data, price_full, scores, trailing_pct,
                                max_slots=2, entry=2, exit_=10):
    """v84 + trailing + trade history"""
    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_cash = [0.0] * max_slots
    slot_holding = [None] * max_slots
    current_weights = None
    daily_returns = []
    consecutive = defaultdict(int)
    trades = []
    prev_pv = INIT_CAP

    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # PV + max_price 업데이트
        pv = total_cash
        for i in range(max_slots):
            if slot_holding[i] is None:
                pv += slot_cash[i]
            else:
                tk, shares, ep, ed, w, max_p = slot_holding[i]
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep
                if p > max_p:
                    slot_holding[i] = (tk, shares, ep, ed, w, p)
                pv += shares * p
        if prev_pv > 0:
            daily_returns.append((pv - prev_pv) / prev_pv * 100)
        prev_pv = pv

        # 이탈
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w, max_p = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep
            reason = None
            if min_seg < -2:
                reason = 'min_seg'
            elif rank is None or rank > exit_:
                reason = 'rank_exit'
            elif trailing_pct > 0 and max_p > 0:
                dd = (p - max_p) / max_p * 100
                if dd <= -trailing_pct:
                    reason = f'trail_{trailing_pct}'
            if reason:
                ret = (p - ep) / ep * 100
                trades.append({
                    'ticker': tk, 'weight': w,
                    'entry_date': ed, 'entry_price': ep,
                    'exit_date': today, 'exit_price': p,
                    'max_price': max_p,
                    'return': ret, 'reason': reason,
                    'hold_days': dates.index(today) - dates.index(ed),
                    'peak_to_exit': (p - max_p) / max_p * 100,
                })
                slot_cash[i] = shares * p
                slot_holding[i] = None

        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        cands = []
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0: continue
            high30 = today_data.get(tk, {}).get('high30')
            if high30 is not None:
                dd = (price - high30) / high30 * 100
                if dd <= -25: continue
            cands.append((tk, price))

        if cands and current_weights is None and total_cash > 0:
            s1, s2, gap = scores.get(today, (100, 0, 100))
            ws = [100, 0] if gap >= 15 else [50, 50]
            current_weights = ws
            slot_cash = [w / 100 * total_cash for w in ws]
            total_cash = 0

        for slot_idx in range(max_slots):
            if slot_holding[slot_idx] is not None: continue
            if not cands: break
            if slot_cash[slot_idx] <= 0:
                cands.pop(0); continue
            tk, price = cands.pop(0)
            shares = slot_cash[slot_idx] / price
            w = current_weights[slot_idx] if current_weights else 0
            slot_holding[slot_idx] = (tk, shares, price, today, w, price)
            slot_cash[slot_idx] = 0

    # open
    open_positions = []
    if dates:
        last = dates[-1]
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w, max_p = slot_holding[i]
            p = data.get(last, {}).get(tk, {}).get('price') or price_full.get(last, {}).get(tk) or ep
            ret = (p - ep) / ep * 100
            open_positions.append({
                'ticker': tk, 'weight': w,
                'entry_date': ed, 'entry_price': ep,
                'current_date': last, 'current_price': p,
                'max_price': max_p,
                'unrealized_return': ret,
                'hold_days': dates.index(last) - dates.index(ed),
                'peak_to_current': (p - max_p) / max_p * 100 if max_p else 0,
            })

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    return {'trades': trades, 'open_positions': open_positions,
            'cum_return': (cum-1)*100, 'max_dd': max_dd,
            'n_days': len(daily_returns)}


def run_bt_paired(db_path, trailing_pct):
    scores = sw.precompute_scores(db_path)
    dates_, data, price_full = sw.load_data(db_path)
    eligible_starts = dates_[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = v84c.simulate_full(dates_, data, price_full, scores, sd, trailing_pct=trailing_pct)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': sum(rets)/len(rets), 'mdd': min(mdds)}


def main():
    print('=' * 110)
    print('★ v84 + trail anchor 정밀 sweet spot + trade history')
    print('=' * 110)

    db_incl = GRID / 'syn25_incl_dd_30_25.db'
    db_excl = GRID / 'syn25_excl_dd_30_25.db'

    # ============================================================
    # 검증 1: 정밀 anchor sensitivity (4.0~6.0, 0.5 step)
    # ============================================================
    print('\n[검증 1] trail anchor 정밀 (4.0, 4.5, 5.0, 5.5, 6.0)')
    print(f'  {"trail":<8} {"i_avg":>9} {"i_mdd":>8} {"i_lift":>8} {"i_win":>7} | {"e_avg":>9} {"e_mdd":>8} {"e_lift":>8} {"e_win":>7} | {"평균":>8}')
    base_i = run_bt_paired(db_incl, 0)['seed_avgs']
    base_e = run_bt_paired(db_excl, 0)['seed_avgs']

    for tp in [0, 4.0, 4.5, 5.0, 5.5, 6.0]:
        ri = run_bt_paired(db_incl, tp)
        re = run_bt_paired(db_excl, tp)
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        marker = ' ★' if tp == 5.0 else '  '
        name = 'baseline' if tp == 0 else f'trail_{tp}'
        print(f'{marker}{name:<8} {ri["avg"]:+8.2f}% {ri["mdd"]:+7.2f}% {avgi:+7.2f}%p {wi:>4}/100 | '
              f'{re["avg"]:+8.2f}% {re["mdd"]:+7.2f}% {avge:+7.2f}%p {we:>4}/100 | {(avgi+avge)/2:+7.2f}%p')

    # ============================================================
    # 검증 2: trail_5 trade history (단일 시작일 2/12)
    # ============================================================
    print('\n' + '=' * 110)
    print('[검증 2] trail_5 trade history (시작일 2026-02-12)')
    print('=' * 110)
    scores = sw.precompute_scores(db_incl)
    dates, data, price_full = sw.load_data(db_incl)

    res = simulate_with_trades_trail(dates, data, price_full, scores, 5)
    all_events = []
    for t in res['trades']:
        all_events.append({**t, 'status': 'CLOSED'})
    for p in res['open_positions']:
        all_events.append({**p, 'status': 'OPEN', 'return': p['unrealized_return'],
                           'exit_date': p['current_date'], 'exit_price': p['current_price'],
                           'reason': 'open',
                           'peak_to_exit': p.get('peak_to_current', 0)})
    all_events.sort(key=lambda x: x['entry_date'])

    print(f'\ntrade {len(all_events)}건 / 누적 {res["cum_return"]:+.2f}%')
    print(f'{"#":>3} {"ticker":<8} {"비중":>5} {"entry":<12} {"exit":<12} {"hold":>5} {"진입가":>9} {"고점":>9} {"청산가":>9} {"수익":>10} {"고점대비":>9} {"사유":<12}')
    print('-' * 130)
    for i, e in enumerate(all_events, 1):
        ret_str = f'{e["return"]:+7.2f}%'
        if e['status'] == 'OPEN':
            ret_str += ' ★'
        max_p = e.get('max_price', e['entry_price'])
        peak_diff = e.get('peak_to_exit', 0)
        print(f'{i:>3} {e["ticker"]:<8} {e["weight"]:>4}% {e["entry_date"]:<12} {e["exit_date"]:<12} '
              f'{e["hold_days"]:>4}일 ${e["entry_price"]:>8.2f} ${max_p:>8.2f} ${e["exit_price"]:>8.2f} '
              f'{ret_str:>11} {peak_diff:+8.2f}% {e["reason"]:<12}')

    # 종합 통계
    print()
    closed = [e for e in all_events if e['status'] == 'CLOSED']
    opens = [e for e in all_events if e['status'] == 'OPEN']
    wins = sum(1 for t in closed if t['return'] > 0)
    losses = sum(1 for t in closed if t['return'] <= 0)
    trail_exits = sum(1 for t in closed if 'trail' in t['reason'])
    rank_exits = sum(1 for t in closed if t['reason'] == 'rank_exit')
    minseg_exits = sum(1 for t in closed if t['reason'] == 'min_seg')

    print(f'완료 trade: {len(closed)}건 ({wins}승 {losses}패)')
    print(f'  - trail 매도: {trail_exits}건')
    print(f'  - rank 매도: {rank_exits}건')
    print(f'  - min_seg 매도: {minseg_exits}건')
    if closed:
        print(f'  - 평균 수익: {sum(t["return"] for t in closed)/len(closed):+.2f}%')
        print(f'  - 최고: {max(t["return"] for t in closed):+.2f}% ({max(closed, key=lambda x: x["return"])["ticker"]})')
        print(f'  - 최저: {min(t["return"] for t in closed):+.2f}% ({min(closed, key=lambda x: x["return"])["ticker"]})')
    print(f'\n현재 보유 (OPEN): {len(opens)}건')
    for p in opens:
        print(f'  {p["ticker"]} ({p["weight"]}%): 진입 {p["entry_date"]} @ ${p["entry_price"]:.2f}, '
              f'고점 ${p["max_price"]:.2f}, 현재 ${p["current_price"]:.2f}, 미실현 {p["unrealized_return"]:+.2f}%, '
              f'고점대비 {p["peak_to_current"]:+.2f}%')


if __name__ == '__main__':
    main()
