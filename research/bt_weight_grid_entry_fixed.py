"""최적 비중 BT — 진입 시점 weight 고정 (production 실거래 가정)

이전 BT (bt_weight_grid.py)는 매일 part2_rank 기준 rebalance 가정.
사용자 지적: 실거래는 처음 봤을 때 1위면 90% 사고 그대로 보유.

구현:
  슬롯별 독립 sub-account.
  - 시작 capital 100. 슬롯 i = weights[i] × 100.
  - 빈 슬롯 채울 때 그 슬롯의 cash로 진입 종목 매수.
  - 슬롯 매도 → cash로 환원, 다음 진입에 사용.
  - 매일 PV = sum(slot 가치). daily_return = ΔPV/PV.

100×3 paired vs v83.3 baseline.
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

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 3
EXIT_TOP = 10


def load_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=?
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[3:8])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            data[d][tk] = {'p2': r[1], 'price': r[2],
                          'min_seg': min(segs) if segs else 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate_entry_fixed(dates_all, data, price_full, weights, start_date,
                         entry=3, exit_=10):
    """진입 시점 weight 고정 simulator"""
    max_slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    INIT_CAP = 100.0
    slot_cash = [w * INIT_CAP for w in weights]  # 슬롯별 가용 현금
    slot_holding = [None] * max_slots  # 슬롯별 (ticker, shares, entry_price, entry_date)
    daily_returns = []
    consecutive = defaultdict(int)
    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    prev_pv = INIT_CAP

    for di, today in enumerate(dates):
        if today not in data:
            daily_returns.append(0)
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # 1. 오늘 PV 계산
        pv_today = 0
        for i in range(max_slots):
            if slot_holding[i] is None:
                pv_today += slot_cash[i]
            else:
                tk, shares, _, _ = slot_holding[i]
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                if p is None:
                    # 가격 unavailable → 어제 가치 유지 (drift 무시)
                    p = slot_holding[i][2]  # entry_price (fallback)
                pv_today += shares * p

        if prev_pv > 0:
            daily_returns.append((pv_today - prev_pv) / prev_pv * 100)
        else:
            daily_returns.append(0)
        prev_pv = pv_today

        # 2. 이탈 체크 (각 슬롯 독립)
        for i in range(max_slots):
            if slot_holding[i] is None:
                continue
            tk, shares, entry_price, entry_date = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            should_exit = False
            if min_seg < -2:
                should_exit = True
            elif rank is None or rank > exit_:
                should_exit = True
            if should_exit:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                if p:
                    slot_cash[i] = shares * p
                else:
                    slot_cash[i] = shares * entry_price  # fallback
                slot_holding[i] = None

        # 3. 진입 (빈 슬롯 채움, 낮은 index부터)
        for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
            if rank > entry:
                break
            # 이미 보유 중?
            if any(h is not None and h[0] == tk for h in slot_holding):
                continue
            if consecutive.get(tk, 0) < 3:
                continue
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < 0:
                continue
            price = today_data.get(tk, {}).get('price')
            if not price or price <= 0:
                continue
            # 빈 슬롯 찾기 (낮은 index 우선)
            free = next((i for i in range(max_slots) if slot_holding[i] is None), None)
            if free is None:
                break
            shares = slot_cash[free] / price
            slot_holding[free] = (tk, shares, price, today)
            slot_cash[free] = 0

    # MDD 계산
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100
        max_dd = min(max_dd, dd)
    max_day_loss = min(daily_returns) if daily_returns else 0
    return {
        'total_return': (cum-1)*100, 'max_dd': max_dd,
        'max_day_loss': max_day_loss,
    }


VARIANTS = [
    ('slot1_100',         [1.0]),
    ('slot2_100_0',       [1.0, 0.0]),
    ('slot2_90_10',       [0.9, 0.1]),  # v83.3
    ('slot2_80_20',       [0.8, 0.2]),
    ('slot2_70_30',       [0.7, 0.3]),
    ('slot2_60_40',       [0.6, 0.4]),
    ('slot2_50_50',       [0.5, 0.5]),
    ('slot3_50_30_20',    [0.5, 0.3, 0.2]),
    ('slot3_40_35_25',    [0.4, 0.35, 0.25]),
    ('slot3_equal',       [1/3, 1/3, 1/3]),
]


def main():
    print('=' * 110)
    print('최적 비중 BT — 진입 시점 weight 고정 (production 실거래 가정)')
    print(f'simulator: 슬롯별 독립 sub-account, 진입 시 cash로 매수, 매도 시 cash 환원')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} paired')
    print('=' * 110)

    dates, data, price_full = load_data()
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    for name, w in VARIANTS:
        t0 = time.time()
        rets, mdds, mdls, seed_avgs = [], [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate_entry_fixed(dates, data, price_full, w, sd,
                                        entry=ENTRY_TOP, exit_=EXIT_TOP)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                mdls.append(r['max_day_loss'])
                sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[name] = {
            'rets': rets, 'mdds': mdds, 'mdls': mdls, 'seed_avgs': seed_avgs,
            'weights': w,
        }
        avg = sum(rets)/len(rets)
        med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets)
        mdd = min(mdds)
        mdl = min(mdls)
        sharpe = avg/std if std > 0 else 0
        marker = ' ★' if name == 'slot2_90_10' else '  '
        print(f'{marker}{name:<18} {str(w):<25} avg={avg:+6.2f}% med={med:+6.2f}% '
              f'std={std:5.1f} mdd={mdd:+6.2f}% maxday={mdl:+5.2f}% '
              f'sharpe={sharpe:+.2f} [{time.time()-t0:.1f}s]')

    print()
    print('=' * 110)
    print('paired vs slot2_90_10 (v83.3 production)')
    print('=' * 110)
    base = all_results['slot2_90_10']['seed_avgs']
    print(f'  {"variant":<18} {"avg lift":>10} {"med lift":>10} {"min":>10} {"max":>10} '
          f'{"wins":>10} {"verdict":>10}')
    print('  ' + '-' * 95)
    for name, _ in VARIANTS:
        if name == 'slot2_90_10':
            continue
        new = all_results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts) / len(lifts)
        med_l = statistics.median(lifts)
        verdict = ('✓✓ 우월' if wins >= 70
                   else '✓ 우월' if wins >= 60
                   else '~ 동등' if wins >= 40
                   else '✗ 열세' if wins >= 30
                   else '✗✗ 열세')
        print(f'  {name:<18} {avg_l:+8.2f}%p {med_l:+8.2f}%p {min(lifts):+8.2f}%p '
              f'{max(lifts):+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')

    # 변동성 비교
    print()
    print('=' * 110)
    print('변동성 비교 (잠 자기 좋은 비중)')
    print('=' * 110)
    print(f'  {"variant":<18} {"avg":>8} {"std":>6} {"MDD":>8} {"max_day":>8} {"sharpe":>7}')
    print('  ' + '-' * 70)
    for name, _ in VARIANTS:
        r = all_results[name]
        avg = sum(r['rets'])/len(r['rets'])
        std = statistics.pstdev(r['rets'])
        mdd = min(r['mdds'])
        mdl = min(r['mdls'])
        sharpe = avg/std if std > 0 else 0
        marker = ' ★' if name == 'slot2_90_10' else '  '
        print(f'{marker}{name:<16} {avg:+7.2f}% {std:>5.1f} {mdd:+7.2f}% '
              f'{mdl:+7.2f}% {sharpe:+6.2f}')


if __name__ == '__main__':
    main()
