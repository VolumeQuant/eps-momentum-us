"""매도 유예 N일 룰 BT — N ∈ {0, 1, 2, 3, 5, 999} 비교

⏸️ 4조건 (check_breakout_hold):
  1. 20일 가격 +25%
  2. ntm_90d → ntm_current 순방향
  3. rev_up/num_an ≥ 0.4
  4. price > MA60

룰 비교:
  - N=0: ⏸️ 무시, rank > exit_top 즉시 매도 (현재 BT 동작)
  - N=1: 1일 유예 (오늘 ⏸️ → 내일 다시 ⏸️면 매도)
  - N=2: 2일 유예 (production 메시지 안내문)
  - N=3, 5, 999(무제한)

방법: random 100 seed × 3 starts paired (v80.10 검증과 동일)
"""
import sys
import random
import statistics
import sqlite3
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def load_data_ext():
    """⏸️ 체크용 컬럼 추가 로드"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   ma60, rev_up30, num_analysts
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0
            data[d][tk] = {
                'p2': r[1], 'price': r[2],
                'comp_rank': r[3], 'min_seg': min_seg,
                'ntm_current': nc, 'ntm_90d': n90,
                'ma60': r[9], 'rev_up30': r[10], 'num_analysts': r[11],
            }
    conn.close()

    # 20일 전 price를 위해 ticker별 (date_idx → price) 맵
    # 또한 part2_rank NULL인 날 (Top 30 밖) 가격도 필요할 수 있어 전체 OHLCV 로드
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    price_series = defaultdict(dict)  # {ticker: {date: price}}
    for tk, dt, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_series[tk][dt] = px
    conn.close()

    return dates, data, price_series


def check_breakout_hold(today, ticker, data, price_series, dates_all):
    """4조건 체크 — production의 check_breakout_hold와 동일 로직"""
    td = data.get(today, {}).get(ticker)
    if td is None:
        return False
    # 1. 20일 전 가격 (거래일 20일 전)
    try:
        idx = dates_all.index(today)
    except ValueError:
        return False
    if idx < 20:
        return False
    past_date = dates_all[idx - 20]
    past_price = price_series.get(ticker, {}).get(past_date)
    today_price = td.get('price')
    if not today_price or not past_price or past_price <= 0:
        return False
    if (today_price - past_price) / past_price * 100 < 25:
        return False
    # 2. ntm 순방향
    nc = td.get('ntm_current')
    n90 = td.get('ntm_90d')
    if not nc or not n90 or n90 <= 0 or nc <= n90:
        return False
    # 3. rev_up 비율
    rev_up = td.get('rev_up30') or 0
    num_an = td.get('num_analysts') or 0
    if num_an < 1 or (rev_up / num_an) < 0.4:
        return False
    # 4. MA60
    ma60 = td.get('ma60')
    if not ma60 or today_price <= ma60:
        return False
    return True


def simulate_hold(dates_all, data, price_series, hold_days,
                  entry_top=3, exit_top=10, max_slots=3, start_date=None):
    """simulate + N일 유예 룰"""
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    portfolio = {}  # {ticker: {entry_price, entry_date, paused_days}}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    if start_date:
        for d in dates_all:
            if d >= start_date:
                break
            for tk, v in data.get(d, {}).items():
                if v.get('p2') and v['p2'] <= 30:
                    consecutive[tk] = consecutive.get(tk, 0) + 1

    for di, today in enumerate(dates):
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items() if v.get('p2') is not None}

        new_consecutive = defaultdict(int)
        for tk in rank_map:
            new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        # day_ret 계산 (어제 portfolio 기준)
        day_ret = 0
        if portfolio:
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price')
                if price and di > 0:
                    prev = data.get(dates[di - 1], {}).get(tk, {}).get('price')
                    if prev and prev > 0:
                        day_ret += (price - prev) / prev * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

        # 이탈 체크 (유예 룰 적용)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            price = today_data.get(tk, {}).get('price')

            # min_seg < -2 이면 무조건 매도 (유예 없음)
            if min_seg < -2:
                if price:
                    entry_price = portfolio[tk]['entry_price']
                    ret = (price - entry_price) / entry_price * 100
                    trades.append({'ticker': tk, 'return': ret, 'reason': 'min_seg'})
                exited.append(tk)
                continue

            # rank 조건
            out_top = (rank is None) or (rank > exit_top)
            if not out_top:
                # rank 안에 들어옴 → paused 리셋
                portfolio[tk]['paused'] = 0
                continue

            # rank > exit_top: 유예 가능성 체크
            hold_ok = check_breakout_hold(today, tk, data, price_series, dates_all)
            if hold_ok and portfolio[tk]['paused'] < hold_days:
                portfolio[tk]['paused'] += 1
                continue
            # 유예 만료 또는 4조건 깨짐 → 매도
            if price:
                entry_price = portfolio[tk]['entry_price']
                ret = (price - entry_price) / entry_price * 100
                reason = 'hold_expired' if hold_ok else 'rank_exit'
                trades.append({'ticker': tk, 'return': ret, 'reason': reason})
            exited.append(tk)

        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry_top or vacancies <= 0:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0)
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = {'entry_price': price, 'entry_date': today,
                                     'paused': 0}
                    vacancies -= 1

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr_ in daily_returns:
        cum_ret *= (1 + dr_ / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    return {
        'total_return': round((cum_ret - 1) * 100, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': len(trades),
        'trades': trades,
    }


def main():
    print('=' * 100)
    print('매도 유예 N일 룰 BT — Random 100 seed × 3 starts paired')
    print('=' * 100)

    dates, data, price_series = load_data_ext()
    print(f'\n총 거래일: {len(dates)}, 종목 데이터 로드 완료')

    # 표본 측정
    import time
    t0 = time.time()
    r = simulate_hold(dates, data, price_series, hold_days=0,
                      entry_top=3, exit_top=10, max_slots=3, start_date=dates[0])
    print(f'\n[표본] N=0 1회 시뮬: {time.time()-t0:.2f}s, ret={r["total_return"]:+.2f}%')
    print(f'예상 총 소요: ~{(time.time()-t0)*6*N_SEEDS*SAMPLES_PER_SEED:.0f}s')

    # N별 random paired
    HOLD_DAYS_LIST = [0, 1, 2, 3, 5, 999]
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    print(f'시작일 풀: {len(eligible_starts)}개\n')

    # seed별로 같은 시작일 사용 (paired)
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    for N in HOLD_DAYS_LIST:
        t_n = time.time()
        rets = []
        mdds = []
        seed_avg_rets = []
        hold_trades = 0
        rank_trades = 0
        for seed_i, chosen in enumerate(seed_starts):
            seed_rets = []
            for sd in chosen:
                r = simulate_hold(dates, data, price_series, hold_days=N,
                                  entry_top=3, exit_top=10, max_slots=3,
                                  start_date=sd)
                rets.append(r['total_return'])
                mdds.append(r['max_dd'])
                seed_rets.append(r['total_return'])
                for t in r['trades']:
                    if t['reason'] == 'hold_expired':
                        hold_trades += 1
                    elif t['reason'] == 'rank_exit':
                        rank_trades += 1
            seed_avg_rets.append(sum(seed_rets) / len(seed_rets))
        all_results[N] = {
            'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avg_rets,
            'hold_trades': hold_trades, 'rank_trades': rank_trades,
        }
        avg = sum(rets) / len(rets)
        n_label = '무제한' if N == 999 else f'{N}일'
        print(f'  [{time.time()-t_n:>5.1f}s] N={n_label:<6} avg={avg:+6.2f}% '
              f'min={min(rets):+6.2f}% max={max(rets):+6.2f}% '
              f'MDD={min(mdds):+6.2f}% hold_exit={hold_trades} rank_exit={rank_trades}')

    # 종합
    print()
    print('=' * 100)
    print('결과 분포 (300개 시뮬 전체)')
    print('=' * 100)
    print(f'{"N":<8} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"p25":>9} {"p75":>9} {"min":>9} {"max":>9} {"MDD":>8}')
    print('-' * 90)
    for N in HOLD_DAYS_LIST:
        r = all_results[N]
        rets = r['rets']
        n = len(rets)
        rets_s = sorted(rets)
        avg = sum(rets) / n
        med = rets_s[n // 2]
        std = statistics.pstdev(rets)
        p25 = rets_s[n // 4]
        p75 = rets_s[3 * n // 4]
        mdd = min(r['mdds'])
        n_label = '무제한' if N == 999 else f'{N}일'
        marker = ' ★ current' if N == 0 else ' ← message' if N == 2 else ''
        print(f'  N={n_label:<6} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{p25:+8.2f}% {p75:+8.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}% '
              f'{mdd:+7.2f}%{marker}')

    # paired N=0 vs other
    print()
    print('=' * 100)
    print('N=0 (현재 baseline) 대비 paired 비교 (seed별 동일 시작일)')
    print('=' * 100)
    base = all_results[0]['seed_avgs']
    print(f'{"vs":<8} {"avg lift":>10} {"min lift":>10} {"max lift":>10} '
          f'{"#wins":>7} {"#losses":>8} {"#ties":>6}')
    print('-' * 70)
    for N in HOLD_DAYS_LIST:
        if N == 0:
            continue
        new = all_results[N]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        losses = sum(1 for l in lifts if l < 0)
        ties = sum(1 for l in lifts if l == 0)
        avg_lift = sum(lifts) / len(lifts)
        n_label = '무제한' if N == 999 else f'{N}일'
        print(f'  N={n_label:<5} {avg_lift:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p '
              f'{wins:>6} {losses:>7} {ties:>5}')


if __name__ == '__main__':
    main()
