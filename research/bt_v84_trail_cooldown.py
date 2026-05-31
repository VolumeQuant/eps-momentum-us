"""v84 + trail_5 + cooldown — 쿨다운 효과 검증

trail_5 매도 후 N일 그 종목 매수 금지 (whipsaw 방지)
거래비용 0%, 0.25%, 0.5% 시나리오 모두 검증
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

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def simulate_trail_cooldown(dates_all, data, price_full, scores, start_date,
                            trailing_pct, cooldown_days, transaction_cost=0,
                            max_slots=2, entry=2, exit_=10):
    """v84 + trail + cooldown"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_cash = [0.0] * max_slots
    slot_holding = [None] * max_slots
    current_weights = None
    daily_returns = []
    consecutive = defaultdict(int)
    # cooldown: {ticker: exit_date_di}
    cooldown_until = {}  # {ticker: di까지 매수 금지}
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1
    prev_pv = INIT_CAP

    for di, today in enumerate(dates):
        if today not in data:
            daily_returns.append(0); continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

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
            should_exit = False
            trail_triggered = False
            if min_seg < -2: should_exit = True
            elif rank is None or rank > exit_: should_exit = True
            elif trailing_pct > 0 and max_p > 0:
                if (p - max_p) / max_p * 100 <= -trailing_pct:
                    should_exit = True
                    trail_triggered = True
            if should_exit:
                proceeds = shares * p * (1 - transaction_cost/100)
                slot_cash[i] = proceeds
                slot_holding[i] = None
                # cooldown 적용 (trail trigger 시에만)
                if trail_triggered and cooldown_days > 0:
                    cooldown_until[tk] = di + cooldown_days

        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        # 진입 후보 (cooldown 적용)
        cands = []
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            # cooldown 체크
            if tk in cooldown_until and di < cooldown_until[tk]:
                continue
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
            shares = shares * (1 - transaction_cost/100)
            w = current_weights[slot_idx] if current_weights else 0
            slot_holding[slot_idx] = (tk, shares, price, today, w, price)
            slot_cash[slot_idx] = 0

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_bt(db_path, trail_pct, cooldown, cost):
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
            r = simulate_trail_cooldown(dates_, data, price_full, scores, sd,
                                        trail_pct, cooldown, cost)
            rets.append(r['total_return']); mdds.append(r['max_dd']); sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': sum(rets)/len(rets), 'mdd': min(mdds)}


def main():
    print('=' * 110)
    print('★ v84 + trail_5 + cooldown 그리드 (거래비용 0%, 0.25%, 0.5% 시나리오)')
    print('=' * 110)

    db_incl = GRID / 'syn25_incl_dd_30_25.db'
    db_excl = GRID / 'syn25_excl_dd_30_25.db'

    # baseline (no trail, no cooldown, no cost)
    base_i = run_bt(db_incl, 0, 0, 0)['seed_avgs']
    base_e = run_bt(db_excl, 0, 0, 0)['seed_avgs']

    configs = [
        # (이름, trail, cooldown, cost)
        ('baseline (no trail)',      0, 0, 0),
        ('trail_5, no cool, cost=0', 5, 0, 0),
        ('trail_5, cool=1, cost=0',  5, 1, 0),
        ('trail_5, cool=2, cost=0',  5, 2, 0),
        ('trail_5, cool=3, cost=0',  5, 3, 0),
        ('trail_5, cool=5, cost=0',  5, 5, 0),
        ('trail_5, cool=7, cost=0',  5, 7, 0),
        ('---', None, None, None),
        ('trail_5, no cool, cost=0.25%', 5, 0, 0.25),
        ('trail_5, cool=1, cost=0.25%',  5, 1, 0.25),
        ('trail_5, cool=3, cost=0.25%',  5, 3, 0.25),
        ('trail_5, cool=5, cost=0.25%',  5, 5, 0.25),
        ('---', None, None, None),
        ('trail_5, no cool, cost=0.5%',  5, 0, 0.5),
        ('trail_5, cool=3, cost=0.5%',   5, 3, 0.5),
        ('trail_5, cool=5, cost=0.5%',   5, 5, 0.5),
        ('---', None, None, None),
        ('baseline cost=0.25%', 0, 0, 0.25),
        ('baseline cost=0.5%',  0, 0, 0.5),
    ]

    print(f'\n{"config":<35} | {"i_avg":>9} {"i_lift":>9} {"i_win":>7} | {"e_avg":>9} {"e_lift":>9} {"e_win":>7} | {"평균":>9}')
    print('-' * 110)
    for label, tp, cd, cs in configs:
        if label == '---':
            print('-' * 110)
            continue
        ri = run_bt(db_incl, tp, cd, cs)
        re = run_bt(db_excl, tp, cd, cs)
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        print(f'  {label:<35} | {ri["avg"]:+8.2f}% {avgi:+7.2f}%p {wi:>4}/100 | '
              f'{re["avg"]:+8.2f}% {avge:+7.2f}%p {we:>4}/100 | {(avgi+avge)/2:+7.2f}%p')


if __name__ == '__main__':
    main()
