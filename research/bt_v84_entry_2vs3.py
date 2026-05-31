"""v84 환경에서 entry=2 vs entry=3 비교 (LITE/TTMI 진입 효과)

환경:
- 공통: slot=2, exit=10, v84 (dd_30_25 진입필터 + 2step_t15 dynamic weight)
- 변수: entry=2 (현재) vs entry=3 (이전 v80.10c 시절)

검증:
1. 100×3 paired BT (수익률 비교)
2. trade history (LITE/TTMI 진입 여부 + 수익률)
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
import pandas as pd
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr
import bt_dynamic_sweetspot as sw
import bt_verify_blindspots as vb
import bt_dd30_real as bdr

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def run_bt(db_path, weight_fn, entry_top):
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
            r = vb.simulate_hybrid(dates_, data, price_full, scores, weight_fn, sd,
                                  max_slots=2, entry=entry_top, exit_=10)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


def simulate_with_trades_v84(dates, data, price_full, scores, entry_top, max_slots=2):
    """v84 entry_fixed + trade history + dd_30_25 진입필터"""
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

        pv = total_cash
        for i in range(max_slots):
            if slot_holding[i] is None:
                pv += slot_cash[i]
            else:
                tk, shares, ep, ed, w = slot_holding[i]
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep
                pv += shares * p
        if prev_pv > 0:
            daily_returns.append((pv - prev_pv) / prev_pv * 100)
        prev_pv = pv

        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            reason = None
            if min_seg < -2: reason = 'min_seg'
            elif rank is None or rank > 10: reason = 'rank_exit'
            if reason:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep
                ret = (p - ep) / ep * 100
                trades.append({
                    'ticker': tk, 'weight': w,
                    'entry_date': ed, 'entry_price': ep,
                    'exit_date': today, 'exit_price': p,
                    'return': ret, 'reason': reason,
                    'hold_days': dates.index(today) - dates.index(ed),
                })
                slot_cash[i] = shares * p
                slot_holding[i] = None

        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        # 진입 후보 (dd_30_25 적용)
        cands = []
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry_top: break
            if any(h is not None and h[0] == tk for h in slot_holding): continue
            if consecutive.get(tk, 0) < 3: continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0: continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0: continue
            # dd_30_25
            high30 = today_data.get(tk, {}).get('high30')
            if high30 is not None:
                dd = (price - high30) / high30 * 100
                if dd <= -25: continue
            cands.append((tk, price))

        # weight 결정 (2step_t15) — entry zone 안의 1-2위 score gap 기반
        if cands and current_weights is None and total_cash > 0:
            # cands[0]는 가장 높은 rank (p2 작은 거 = 1위)
            # 그러나 score gap은 part2_rank Top 1-2 기준
            top1 = cands[0][0]
            top2 = cands[1][0] if len(cands) >= 2 else None
            # score_100 격차
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
            slot_holding[slot_idx] = (tk, shares, price, today, w)
            slot_cash[slot_idx] = 0

    # 미실현
    open_positions = []
    if dates:
        last = dates[-1]
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w = slot_holding[i]
            p = data.get(last, {}).get(tk, {}).get('price') or price_full.get(last, {}).get(tk) or ep
            ret = (p - ep) / ep * 100
            open_positions.append({
                'ticker': tk, 'weight': w,
                'entry_date': ed, 'entry_price': ep,
                'current_date': last, 'current_price': p,
                'unrealized_return': ret,
                'hold_days': dates.index(last) - dates.index(ed),
            })

    cum = 1.0
    for r in daily_returns:
        cum *= (1 + r/100)
    return {'trades': trades, 'open_positions': open_positions,
            'cum_return': (cum-1)*100, 'n_days': len(daily_returns)}


def main():
    print('=' * 110)
    print('★ v84 entry=2 vs entry=3 비교 (LITE/TTMI 진입 효과)')
    print('=' * 110)

    closes = bdr.fetch_yfinance_200d()
    conn = sqlite3.connect(DB_ORIGINAL); cur = conn.cursor()
    db_dates_all = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    conn.close()
    high30 = bdr.compute_high30_real(closes, db_dates_all)

    # dd_30_25 DB 사용 (이전에 syn25_*.db로 만든 것)
    db = GRID / 'syn25_incl_dd_30_25.db'
    if not db.exists():
        # 만들기
        shutil.copy(DB_ORIGINAL, db)
        bdr.regenerate(db, 25, set(), high30)
    db_excl = GRID / 'syn25_excl_dd_30_25.db'

    step_2 = sw.make_weight_fn({'type': 'step_2', 'lo': 0, 'hi': 15, 'wH': 1.0, 'wM': 0.5, 'wL': 0.5})

    # ============================================================
    # 검증 1: paired BT
    # ============================================================
    print('\n[검증 1] paired BT — entry=2 vs entry=3')
    results = {}
    for entry_top in [2, 3]:
        for env_name, db_path in [('incl', db), ('excl', db_excl)]:
            t0 = time.time()
            res = run_bt(db_path, step_2, entry_top)
            results[f'e{entry_top}_{env_name}'] = res
            marker = ' ★' if entry_top == 2 else '  '
            print(f'{marker}entry={entry_top} {env_name}: avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    print()
    print('=' * 110)
    print('paired lift entry=3 vs entry=2')
    print('=' * 110)
    for env in ['incl', 'excl']:
        base = results[f'e2_{env}']['seed_avgs']
        new = results[f'e3_{env}']['seed_avgs']
        lifts = [b-a for a,b in zip(base, new)]
        avg_l = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0.01)
        print(f'  {env}: entry=2 avg={results[f"e2_{env}"]["avg"]:+.2f}%, entry=3 avg={results[f"e3_{env}"]["avg"]:+.2f}%, lift={avg_l:+.2f}%p ({wins}/100)')

    # ============================================================
    # 검증 2: trade history (entry=3에서 LITE/TTMI 들어오는지)
    # ============================================================
    print()
    print('=' * 110)
    print('[검증 2] trade history (incl 환경, entry=3)')
    print('=' * 110)

    # incl DB, 전체 기간으로 trade history
    scores_full = sw.precompute_scores(db)
    dates_full, data_full, price_full_full = sw.load_data(db)
    for entry_top in [2, 3]:
        print(f'\n--- entry={entry_top} ---')
        res_t = simulate_with_trades_v84(dates_full, data_full, price_full_full, scores_full, entry_top)
        all_events = []
        for t in res_t['trades']:
            all_events.append({**t, 'status': 'CLOSED'})
        for p in res_t['open_positions']:
            all_events.append({**p, 'status': 'OPEN', 'return': p['unrealized_return'],
                               'exit_date': p['current_date'], 'exit_price': p['current_price'],
                               'reason': 'open'})
        all_events.sort(key=lambda x: x['entry_date'])

        print(f'  trade {len(all_events)}건 / 누적 {res_t["cum_return"]:+.2f}%')
        print(f'  {"#":>2} {"ticker":<8} {"비중":>5} {"entry":<12} {"exit":<12} {"hold":>5} {"수익":>9} {"상태":<8}')
        for i, e in enumerate(all_events, 1):
            ret_str = f'{e["return"]:+7.2f}%'
            if e['status'] == 'OPEN':
                ret_str += ' ★'
            print(f'  {i:>2} {e["ticker"]:<8} {e["weight"]:>4}% {e["entry_date"]:<12} {e["exit_date"]:<12} {e["hold_days"]:>4}일 {ret_str:>10} {e["status"]:<8}')


if __name__ == '__main__':
    main()
