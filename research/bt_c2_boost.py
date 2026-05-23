"""C2 (buy-the-dip) boost BT — case-based 진입 우대

설계:
  - 매일 part2_rank Top 10 후보 종목 평가
  - 각 후보의 case 분류 (C1: EPS↑가격↑, C2: EPS↑가격↓)
  - C2 종목은 effective_rank = original_rank - boost
  - effective_rank 기준으로 Top 3 선택

C2 정의: D1 (eps_chg_weighted > 0 + price 30d < 0)
  (D2와 동일 결과, D3은 sample 너무 작아서 분리 어려움)

boost 변형:
  0: baseline (현재 production, 변화 없음)
  1: C2 rank -1 (예: rank 4 → effective 3)
  2: C2 rank -2
  3: C2 rank -3
  5: C2 rank -5 (강한 우대)
  10: C2 만 진입 (사실상 C1 제외)

NEW simulator + production rules + 5/22 DB.
"""
import sys
import sqlite3
import random
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 500
SAMPLES = 3
MIN_HOLD_DAYS = 10
LOOKBACK = 30  # price lookback for case classification


def load_all(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
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
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def get_price_30d(tk, today, dates, price_full):
    if today not in dates: return None
    di = dates.index(today)
    if di < LOOKBACK: return None
    past_d = dates[di - LOOKBACK]
    past_p = price_full.get(past_d, {}).get(tk)
    cur_p = price_full.get(today, {}).get(tk)
    if past_p and cur_p and past_p > 0:
        return (cur_p - past_p) / past_p * 100
    return None


def classify_c2(info, tk, today, dates, price_full):
    """C2 = EPS↑(eps_w > 0) AND 가격↓ (30일 가격 변화 < 0)"""
    eps_w = info.get('eps_w')
    if eps_w is None: return False
    p30 = get_price_30d(tk, today, dates, price_full)
    if p30 is None: return False
    return eps_w > 0 and p30 < 0


def simulate(dates_all, data, price_full, c2_boost, start_date=None,
             entry=3, exit_=10, slots=3):
    """c2_boost: C2 종목의 rank에서 차감할 값.
       boost=0: baseline, boost=10: C2만 진입 (effective rank 매우 낮음)"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            new_c = defaultdict(int)
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c

    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # Day return
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]; n = 0
            for tk in portfolio:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pr = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if p and pr and pr > 0:
                    day_ret += (p - pr) / pr * 100; n += 1
            if n > 0: day_ret /= n
        daily_returns.append(day_ret)

        # Exit (original part2_rank 기준)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]

        # Entry (case-adjusted ranking)
        vacancies = slots - len(portfolio)
        if vacancies > 0:
            # Top 10 후보 모음, case별 effective rank 계산
            candidates = []
            for tk, rank in rank_map.items():
                if rank > 10: continue  # Top 10만 고려 (boost 5까지 흡수)
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                min_seg = info.get('min_seg', 0)
                if min_seg < 0: continue
                price = info.get('price')
                if not (price and price > 0): continue
                # case 분류
                is_c2 = classify_c2(info, tk, today, dates, price_full)
                effective_rank = rank - (c2_boost if is_c2 else 0)
                candidates.append((effective_rank, rank, tk, price))
            # effective rank 정렬 (오름차순 = 낮을수록 좋음)
            candidates.sort()
            # Top 3 (entry threshold만큼) — original rank entry 이하만 진입 허용
            # (즉 baseline의 진입 후보 풀은 유지하되, 우선순위만 case 가중)
            for eff_r, orig_r, tk, price in candidates:
                if vacancies <= 0: break
                # entry 임계값 적용: original rank가 너무 멀면 안 됨
                # boost로 effective는 좋아도 original rank 5 초과면 entry 제한
                # → entry 변수가 baseline 진입선이지만, boost로 효율적 entry 확장됨
                # 단순화: effective_rank ≤ entry이면 진입
                if eff_r > entry: continue
                portfolio[tk] = {'entry_price': price}
                vacancies -= 1

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run(c2_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, c2_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print(f'C2 (buy-the-dip) boost BT — case-based rank 우대')
    print(f'N_SEEDS={N_SEEDS} × {SAMPLES} = {N_SEEDS*SAMPLES} sim/boost')
    print('=' * 100)
    dates, data, price_full = load_all(DB_PATH)
    print(f'[Load] {len(dates)} 거래일')
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    boosts = [0, 1, 2, 3, 5, 10]
    print()
    print(f'{"C2 boost":<10} {"avg":>9} {"med":>9} {"worst MDD":>10} {"sharpe":>7}')
    print('-' * 60)
    results = {}
    for b in boosts:
        res = run(b, dates, data, price_full, seed_starts)
        results[b] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if b == 0 else '  '
        print(f'{marker} boost={b:<3} {avg:+8.2f}% {med:+8.2f}% {worst_mdd:+9.2f}% {sharpe:+6.2f}')

    print()
    print('=' * 100)
    print('paired vs boost=0 (baseline)')
    print('=' * 100)
    print(f'{"boost":<8} {"avg lift":>10} {"worst":>10} {"best":>10} {"wins":>10} {"verdict":>15}')
    print('-' * 70)
    base = results[0]['seed_avgs']
    for b in boosts:
        if b == 0: continue
        new_ = results[b]['seed_avgs']
        lifts = [b_ - a for a, b_ in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        worst = min(lifts); best = max(lifts)
        rate = wins / N_SEEDS * 100
        verdict = '✓✓ 우월' if rate >= 90 else '✓ 우월' if rate >= 75 else '◐ 약함' if rate >= 60 else '~ 동등' if rate >= 40 else '✗ 열세'
        print(f'  boost={b:<3} {avg_l:+8.2f}%p {worst:+8.2f}%p {best:+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')


if __name__ == '__main__':
    main()
