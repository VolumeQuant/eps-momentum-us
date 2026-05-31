"""v84 + 트레일링 스탑 검증

환경:
  v84 (dd_30_25 진입필터 + 2step_t15 dynamic weight)
  + 트레일링 스탑 N% (보유 후 max 대비 -N%면 매도)

후보:
  baseline (no trailing) — v84 그대로
  trailing_5, 10, 15, 20, 25

paired BT 100×3, 두 환경 (incl/excl).
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


def simulate_with_trailing(dates_all, data, price_full, scores, weight_fn,
                           start_date, trailing_pct, max_slots=2,
                           entry=2, exit_=10):
    """v84 + trailing stop N%"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_cash = [0.0] * max_slots
    # slot_holding[i] = (ticker, shares, entry_price, entry_date, weight, max_price)
    slot_holding = [None] * max_slots
    current_weights = None
    daily_returns = []
    consecutive = defaultdict(int)
    n_trail_exits = 0  # trailing stop으로 매도된 횟수

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
                # max_p 갱신
                if p > max_p:
                    slot_holding[i] = (tk, shares, ep, ed, w, p)
                    max_p = p
                pv += shares * p
        if prev_pv > 0:
            daily_returns.append((pv - prev_pv) / prev_pv * 100)
        prev_pv = pv

        # 이탈 (v84 룰 + trailing)
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, ep, ed, w, max_p = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk) or ep

            # 매도 조건 결정
            should_exit = False
            if min_seg < -2:
                should_exit = True
            elif rank is None or rank > exit_:
                should_exit = True
            elif trailing_pct > 0 and max_p > 0:
                # trailing stop
                drawdown_from_max = (p - max_p) / max_p * 100
                if drawdown_from_max <= -trailing_pct:
                    should_exit = True
                    n_trail_exits += 1

            if should_exit:
                slot_cash[i] = shares * p
                slot_holding[i] = None

        if all(h is None for h in slot_holding):
            total_cash += sum(slot_cash)
            slot_cash = [0.0] * max_slots
            current_weights = None

        # 진입 (v84 dd_30_25 진입필터)
        cands = []
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry: break
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

        # weight 결정 (2step_t15)
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
            slot_holding[slot_idx] = (tk, shares, price, today, w, price)  # max_price=entry_price

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd,
            'n_trail_exits': n_trail_exits}


def run_bt(db_path, trailing_pct):
    scores = sw.precompute_scores(db_path)
    dates_, data, price_full = sw.load_data(db_path)
    eligible_starts = dates_[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, n_trails, seed_avgs = [], [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate_with_trailing(dates_, data, price_full, scores, None,
                                       sd, trailing_pct, max_slots=2,
                                       entry=ENTRY_TOP, exit_=EXIT_TOP)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            n_trails.append(r['n_trail_exits'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'n_trails': n_trails,
            'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


def main():
    print('=' * 110)
    print('★ v84 + 트레일링 스탑 BT (5, 10, 15, 20, 25%)')
    print(f'  환경: entry={ENTRY_TOP}, exit={EXIT_TOP}, slot=2 + v84 (dd_30_25 + 2step_t15)')
    print('=' * 110)

    # v84 DB 사용 (dd_30_25 적용된 part2_rank)
    db_incl = GRID / 'syn25_incl_dd_30_25.db'
    db_excl = GRID / 'syn25_excl_dd_30_25.db'

    # 캐시 확인
    if not db_incl.exists() or not db_excl.exists():
        print('DB 재생성 필요...')
        closes = bdr.fetch_yfinance_200d()
        conn = sqlite3.connect(DB_ORIGINAL); cur = conn.cursor()
        db_dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
        conn.close()
        high30 = bdr.compute_high30_real(closes, db_dates)
        if not db_incl.exists():
            shutil.copy(DB_ORIGINAL, db_incl)
            bdr.regenerate(db_incl, 25, set(), high30)
        if not db_excl.exists():
            shutil.copy(DB_ORIGINAL, db_excl)
            bdr.regenerate(db_excl, 25, {'MU', 'SNDK'}, high30)

    trailing_pcts = [0, 5, 10, 15, 20, 25, 30]
    results = {'incl': {}, 'excl': {}}
    for env_name, db_path in [('incl', db_incl), ('excl', db_excl)]:
        print(f'\n[{env_name}]')
        for tp in trailing_pcts:
            name = 'baseline' if tp == 0 else f'trail_{tp}'
            t0 = time.time()
            res = run_bt(db_path, tp)
            results[env_name][name] = res
            avg_trails = sum(res['n_trails']) / len(res['n_trails'])
            marker = ' ★' if tp == 0 else '  '
            print(f'{marker}{name:<12} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} avg_trail_exits={avg_trails:.1f} [{time.time()-t0:.1f}s]')

    # 종합
    print()
    print('=' * 110)
    print('★ 종합 (lift vs baseline v84 no trailing, 양 환경 robust 평가)')
    print('=' * 110)
    base_i = results['incl']['baseline']['seed_avgs']
    base_e = results['excl']['baseline']['seed_avgs']
    rows = []
    print(f'  {"variant":<14} {"i_avg":>8} {"i_mdd":>7} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_mdd":>7} {"e_lift":>8} {"e_win":>7} | {"avg_lift":>9}')
    print('  ' + '-' * 110)
    for tp in trailing_pcts:
        name = 'baseline' if tp == 0 else f'trail_{tp}'
        ri = results['incl'][name]; re = results['excl'][name]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        avg_both = (avgi + avge) / 2
        rows.append((name, ri['avg'], ri['mdd'], avgi, wi, re['avg'], re['mdd'], avge, we, avg_both))
    rows.sort(key=lambda x: -x[9])
    for r in rows:
        marker = ' ☆' if r[0] == 'baseline' else '  '
        print(f'{marker}{r[0]:<12} {r[1]:+7.2f}% {r[2]:+6.2f}% {r[3]:+7.2f}%p {r[4]:>4}/100 | '
              f'{r[5]:+7.2f}% {r[6]:+6.2f}% {r[7]:+7.2f}%p {r[8]:>4}/100 | {r[9]:+7.2f}%p')

    print()
    print('=' * 110)
    print('★ robust 우월 (양 환경 wins ≥ 60)')
    print('=' * 110)
    found = False
    for r in rows:
        if r[4] >= 60 and r[8] >= 60:
            print(f'  ✓✓ {r[0]:<14} incl +{r[3]:.2f}%p ({r[4]}/100), excl +{r[7]:.2f}%p ({r[8]}/100)')
            found = True
    if not found:
        print('  (없음 — 트레일링 스탑이 v84 환경에서 robust 우월 X)')


if __name__ == '__main__':
    main()
