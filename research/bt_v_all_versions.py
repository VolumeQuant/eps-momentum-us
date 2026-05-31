"""모든 버전 비교 — v80.10c → v82 → v83 → v83.3 → v84 → v84+trail_5

동일 simulator (entry_fixed), 동일 BT 환경 (production 정확 entry=2 or 3)
weight rule + filter 변화 비교.

버전:
  v80.10c    : slot=3, entry=3, weights [33.3, 33.3, 33.3], current 필터
  v82        : slot=2, entry=2, weights [70, 30], current 필터
  v83        : slot=2, entry=2, weights [80, 20], current 필터 (C2 boost 무시)
  v83.3      : slot=2, entry=2, weights [90, 10], current 필터 (현행)
  v84        : slot=2, entry=2, 2step_t15 dynamic, dd_30_25 진입필터
  v84+trail5 : v84 + trailing stop 5%
"""
import sys
import shutil
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
import bt_dd30_real as bdr

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def simulate_version(dates_all, data, price_full, scores, start_date,
                     weights_static=None, weight_rule_dynamic=None,
                     trailing_pct=0, max_slots=2, entry=2, exit_=10):
    """동일 simulator (entry_fixed)에서 다양한 버전 BT.

    weights_static: list (정적 weight, e.g., [90, 10] or [33.3, 33.3, 33.3])
    weight_rule_dynamic: '2step_t15' (gap 기반)
    trailing_pct: 트레일링 스탑 (0 = 비활성)
    """
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

        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w, max_p = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep
            should_exit = False
            if min_seg < -2: should_exit = True
            elif rank is None or rank > exit_: should_exit = True
            elif trailing_pct > 0 and max_p > 0:
                if (p - max_p) / max_p * 100 <= -trailing_pct:
                    should_exit = True
            if should_exit:
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
            cands.append((tk, price))

        if cands and current_weights is None and total_cash > 0:
            if weights_static is not None:
                ws = list(weights_static)
            elif weight_rule_dynamic == '2step_t15':
                s1, s2, gap = scores.get(today, (100, 0, 100))
                ws = [100, 0] if gap >= 15 else [50, 50]
            else:
                ws = [100/max_slots] * max_slots
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

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run_bt(db_path, sim_args):
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
            r = simulate_version(dates_, data, price_full, scores, sd, **sim_args)
            rets.append(r['total_return']); mdds.append(r['max_dd']); sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


def main():
    print('=' * 110)
    print('★ 역대 버전 종합 비교 — 동일 entry_fixed simulator')
    print('=' * 110)

    db_v83 = DB_ORIGINAL  # current 필터 (production)
    db_v84 = GRID / 'syn25_incl_dd_30_25.db'  # dd_30_25 적용
    db_v83_excl = GRID / 'syn25_excl_current.db'
    db_v84_excl = GRID / 'syn25_excl_dd_30_25.db'

    versions = [
        # (이름, db_incl, db_excl, sim_args)
        ('v80.10c (slot3, 33/33/33)',   db_v83,  db_v83_excl, {'max_slots': 3, 'entry': 3,
                                                                'weights_static': [33.3, 33.3, 33.3]}),
        ('v82 (slot2, 70/30)',          db_v83,  db_v83_excl, {'max_slots': 2, 'entry': 2,
                                                                'weights_static': [70, 30]}),
        ('v83 (slot2, 80/20)',          db_v83,  db_v83_excl, {'max_slots': 2, 'entry': 2,
                                                                'weights_static': [80, 20]}),
        ('v83.3 (slot2, 90/10)',        db_v83,  db_v83_excl, {'max_slots': 2, 'entry': 2,
                                                                'weights_static': [90, 10]}),
        ('v84 (dd_30_25 + 2step_t15)',  db_v84,  db_v84_excl, {'max_slots': 2, 'entry': 2,
                                                                'weight_rule_dynamic': '2step_t15'}),
        ('v84 + trail_5',               db_v84,  db_v84_excl, {'max_slots': 2, 'entry': 2,
                                                                'weight_rule_dynamic': '2step_t15',
                                                                'trailing_pct': 5}),
    ]

    print(f'\n{"버전":<32} | {"i_avg":>9} {"i_mdd":>8} {"i_sharpe":>9} | {"e_avg":>9} {"e_mdd":>8} {"e_sharpe":>9}')
    print('-' * 110)

    results = []
    for name, dbi, dbe, args in versions:
        t0 = time.time()
        ri = run_bt(dbi, args)
        re = run_bt(dbe, args)
        results.append((name, ri, re))
        print(f'  {name:<30} | {ri["avg"]:+8.2f}% {ri["mdd"]:+7.2f}% {ri["sharpe"]:+8.2f} | '
              f'{re["avg"]:+8.2f}% {re["mdd"]:+7.2f}% {re["sharpe"]:+8.2f} [{time.time()-t0:.1f}s]')

    # paired vs v83.3 (현행 직전 production)
    print()
    print('=' * 110)
    print('paired lift vs v83.3 (현행)')
    print('=' * 110)
    base_idx = next(i for i, (n, _, _) in enumerate(results) if 'v83.3' in n)
    base_i = results[base_idx][1]['seed_avgs']
    base_e = results[base_idx][2]['seed_avgs']
    print(f'{"버전":<32} | {"i_lift":>9} {"i_win":>7} | {"e_lift":>9} {"e_win":>7} | {"평균":>9}')
    print('-' * 100)
    for name, ri, re in results:
        if 'v83.3' in name:
            print(f'☆ {name:<30} | baseline                                  |')
            continue
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        print(f'  {name:<30} | {avgi:+7.2f}%p {wi:>4}/100 | {avge:+7.2f}%p {we:>4}/100 | {(avgi+avge)/2:+7.2f}%p')

    # 종합 (수익률, MDD, Sharpe)
    print()
    print('=' * 110)
    print('★ 종합 (incl/excl 평균)')
    print('=' * 110)
    print(f'{"버전":<32} | {"평균 수익":>10} {"평균 MDD":>10} {"평균 Sharpe":>12}')
    print('-' * 75)
    for name, ri, re in results:
        avg_r = (ri['avg'] + re['avg']) / 2
        avg_m = (ri['mdd'] + re['mdd']) / 2
        avg_s = (ri['sharpe'] + re['sharpe']) / 2
        print(f'  {name:<30} | {avg_r:+9.2f}% {avg_m:+9.2f}% {avg_s:+11.2f}')


if __name__ == '__main__':
    main()
