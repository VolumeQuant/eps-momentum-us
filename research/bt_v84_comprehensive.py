"""v84 종합 검증 — trail anchor + stop loss + 거래비용 + 버전 비교

검증 1: trail anchor sensitivity (3, 4, 5, 6, 7, 8%)
검증 2: stop loss anchor (5, 10, 15, 20%)
검증 3: trail_5 + 거래비용 stress (0.25%, 0.5%, 1%)
검증 4: 버전 비교 (v83.3 vs v84 vs v84+trail_5) — 역대 최강 확인
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
ENTRY_TOP = 2
EXIT_TOP = 10


def simulate_full(dates_all, data, price_full, scores, start_date,
                  trailing_pct=0, stop_loss_pct=0, transaction_cost_pct=0,
                  weight_rule='2step_t15', max_slots=2, entry=2, exit_=10):
    """v84 + trail + stop loss + cost"""
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

            should_exit = False
            if min_seg < -2:
                should_exit = True
            elif rank is None or rank > exit_:
                should_exit = True
            elif trailing_pct > 0 and max_p > 0:
                dd_from_max = (p - max_p) / max_p * 100
                if dd_from_max <= -trailing_pct:
                    should_exit = True
            elif stop_loss_pct > 0 and ep > 0:
                dd_from_entry = (p - ep) / ep * 100
                if dd_from_entry <= -stop_loss_pct:
                    should_exit = True

            if should_exit:
                proceeds = shares * p * (1 - transaction_cost_pct/100)  # 매도 비용
                slot_cash[i] = proceeds
                slot_holding[i] = None

        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        # 진입 (v84 dd_30_25)
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

        # weight 결정
        if cands and current_weights is None and total_cash > 0:
            s1, s2, gap = scores.get(today, (100, 0, 100))
            if weight_rule == 'fixed_90_10':
                ws = [90, 10]
            elif weight_rule == '2step_t15':
                ws = [100, 0] if gap >= 15 else [50, 50]
            else:
                ws = [50, 50]
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
            shares = shares * (1 - transaction_cost_pct/100)  # 매수 비용
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
            r = simulate_full(dates_, data, price_full, scores, sd, **sim_args)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


def main():
    print('=' * 110)
    print('★ v84 종합 검증 — trail anchor + SL + cost + 버전 비교')
    print('=' * 110)

    # v84 DB
    db_v84 = GRID / 'syn25_incl_dd_30_25.db'
    db_v84_excl = GRID / 'syn25_excl_dd_30_25.db'

    # v83.3 DB (current 필터, dd_30_25 없음)
    # part2_rank 그대로 사용 가능 (production DB)
    db_v83 = DB_ORIGINAL
    db_v83_excl = GRID / 'syn25_excl_current.db'
    if not db_v83_excl.exists():
        print('v83.3 excl DB 생성...')
        closes = bdr.fetch_yfinance_200d()
        conn = sqlite3.connect(DB_ORIGINAL); cur = conn.cursor()
        db_dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
        conn.close()
        high30 = bdr.compute_high30_real(closes, db_dates)
        shutil.copy(DB_ORIGINAL, db_v83_excl)
        bdr.regenerate(db_v83_excl, 0, {'MU', 'SNDK'}, high30)  # th=0 = current filter

    # ============================================================
    # 검증 1: trail anchor sensitivity (3~8)
    # ============================================================
    print('\n' + '=' * 110)
    print('★ 검증 1: trail anchor (3, 4, 5, 6, 7, 8%) — v84 환경')
    print('=' * 110)
    print(f'{"trail":<8} {"i_avg":>8} {"i_mdd":>7} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_mdd":>7} {"e_lift":>8} {"e_win":>7} | {"avg_lift":>9}')
    print('-' * 110)
    base_i = run_bt(db_v84, {'trailing_pct': 0})['seed_avgs']
    base_e = run_bt(db_v84_excl, {'trailing_pct': 0})['seed_avgs']
    trail_results = {}
    for tp in [0, 3, 4, 5, 6, 7, 8]:
        ri = run_bt(db_v84, {'trailing_pct': tp})
        re = run_bt(db_v84_excl, {'trailing_pct': tp})
        trail_results[tp] = (ri, re)
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        marker = ' ★' if tp == 5 else '  '
        name = 'baseline' if tp == 0 else f'trail_{tp}'
        print(f'{marker}{name:<6} {ri["avg"]:+7.2f}% {ri["mdd"]:+6.2f}% {avgi:+7.2f}%p {wi:>4}/100 | '
              f'{re["avg"]:+7.2f}% {re["mdd"]:+6.2f}% {avge:+7.2f}%p {we:>4}/100 | {(avgi+avge)/2:+7.2f}%p')

    # ============================================================
    # 검증 2: stop loss (no trailing, fixed -X% from entry)
    # ============================================================
    print('\n' + '=' * 110)
    print('★ 검증 2: stop loss (5, 10, 15, 20%) — entry 대비 -X% 매도')
    print('=' * 110)
    print(f'{"SL":<8} {"i_avg":>8} {"i_mdd":>7} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_mdd":>7} {"e_lift":>8} {"e_win":>7} | {"avg_lift":>9}')
    print('-' * 110)
    sl_results = {}
    for sl in [0, 5, 10, 15, 20]:
        ri = run_bt(db_v84, {'stop_loss_pct': sl})
        re = run_bt(db_v84_excl, {'stop_loss_pct': sl})
        sl_results[sl] = (ri, re)
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        name = 'baseline' if sl == 0 else f'SL_{sl}'
        print(f'  {name:<6} {ri["avg"]:+7.2f}% {ri["mdd"]:+6.2f}% {avgi:+7.2f}%p {wi:>4}/100 | '
              f'{re["avg"]:+7.2f}% {re["mdd"]:+6.2f}% {avge:+7.2f}%p {we:>4}/100 | {(avgi+avge)/2:+7.2f}%p')

    # ============================================================
    # 검증 3: trail_5 + 거래비용 stress
    # ============================================================
    print('\n' + '=' * 110)
    print('★ 검증 3: trail_5 + 거래비용 stress (0%, 0.25%, 0.5%, 1%)')
    print('=' * 110)
    print(f'{"variant":<22} {"i_avg":>8} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_lift":>8} {"e_win":>7}')
    print('-' * 90)
    for cost in [0, 0.25, 0.5, 1.0]:
        ri = run_bt(db_v84, {'trailing_pct': 5, 'transaction_cost_pct': cost})
        re = run_bt(db_v84_excl, {'trailing_pct': 5, 'transaction_cost_pct': cost})
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        name = f'trail_5 cost={cost}%'
        print(f'  {name:<22} {ri["avg"]:+7.2f}% {avgi:+7.2f}%p {wi:>4}/100 | {re["avg"]:+7.2f}% {avge:+7.2f}%p {we:>4}/100')
    # baseline + cost 비교
    print('---')
    for cost in [0, 1.0]:
        ri = run_bt(db_v84, {'transaction_cost_pct': cost})
        re = run_bt(db_v84_excl, {'transaction_cost_pct': cost})
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        name = f'baseline cost={cost}%'
        print(f'  {name:<22} {ri["avg"]:+7.2f}% {avgi:+7.2f}%p {wi:>4}/100 | {re["avg"]:+7.2f}% {avge:+7.2f}%p {we:>4}/100')

    # ============================================================
    # 검증 4: 역대 버전 비교 (v83.3 vs v84 vs v84+trail_5)
    # ============================================================
    print('\n' + '=' * 110)
    print('★ 검증 4: 역대 버전 비교 (entry_fixed, production 정확 환경)')
    print('=' * 110)
    print(f'{"버전":<32} {"i_avg":>9} {"i_mdd":>8} {"i_sharpe":>9} | {"e_avg":>9} {"e_mdd":>8} {"e_sharpe":>9}')
    print('-' * 110)

    versions = [
        ('v83.3 (90/10, current)',          db_v83,     db_v83_excl, {'weight_rule': 'fixed_90_10'}),
        ('v84 (dd_30_25 + 2step_t15)',      db_v84,     db_v84_excl, {}),
        ('v84 + trail_5',                   db_v84,     db_v84_excl, {'trailing_pct': 5}),
        ('v84 + trail_5 + cost=1%',         db_v84,     db_v84_excl, {'trailing_pct': 5, 'transaction_cost_pct': 1}),
    ]
    for name, dbi, dbe, args in versions:
        ri = run_bt(dbi, args)
        re = run_bt(dbe, args)
        marker = ' ★' if 'v84 + trail_5' == name else '  '
        print(f'{marker}{name:<30} {ri["avg"]:+8.2f}% {ri["mdd"]:+7.2f}% {ri["sharpe"]:+7.2f} | '
              f'{re["avg"]:+8.2f}% {re["mdd"]:+7.2f}% {re["sharpe"]:+7.2f}')


if __name__ == '__main__':
    main()
