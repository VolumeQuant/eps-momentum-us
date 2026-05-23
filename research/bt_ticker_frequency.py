"""종목별 진입 빈도 + 5/22 메시지 시뮬레이션

(3,10,2) 80/20 boost=3 시나리오에서:
1. 종목별 진입 빈도 카운팅 (12 multistart)
2. 5/22 현재 시점 메시지 예시 작성

baseline vs 새 후보의 종목 set 차이 가시화.
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict, Counter

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
LOOKBACK = 30


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
            data[d][tk] = {'p2': r[1], 'price': r[2], 'eps_w': r[3], 'min_seg': min(segs) if segs else 0}
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


def classify_c2(info, tk, today, dates_list, price_full):
    eps_w = info.get('eps_w')
    if eps_w is None: return False
    p30 = get_price_30d(tk, today, dates_list, price_full)
    if p30 is None: return False
    return eps_w > 0 and p30 < 0


def rerank(today, today_data, c2_boost, dates_list, price_full):
    if c2_boost == 0:
        return {tk: info.get('p2') for tk, info in today_data.items() if info.get('p2') is not None}
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def simulate_with_tickers(dates_all, data, price_full, weights, entry, exit_, c2_boost, start_date=None):
    slots = len(weights)
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    entries = []  # 진입 종목 리스트
    if start_date:
        for d in dates_all:
            if d >= start_date: break
            today_data = data.get(d, {})
            new_ranks = rerank(d, today_data, c2_boost, dates_all, price_full)
            new_c = defaultdict(int)
            for tk, r in new_ranks.items():
                if r <= 30:
                    new_c[tk] = consecutive.get(tk, 0) + 1
            consecutive = new_c
    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c2_boost, dates_all, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2 or rank is None or rank > exit_:
                exited.append(tk)
        for tk in exited: del portfolio[tk]
        if len(portfolio) < slots:
            used_idx = {info['slot_idx'] for info in portfolio.values()}
            free_idx = sorted([i for i in range(slots) if i not in used_idx])
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
            for slot_idx in free_idx:
                if not cands: break
                tk, price = cands.pop(0)
                portfolio[tk] = {'entry_price': price, 'slot_idx': slot_idx}
                entries.append((tk, slot_idx))
    return entries


def main():
    dates, data, price_full = load_all(DB_PATH)
    print('=' * 100)
    print('종목별 진입 빈도 + 5/22 메시지 시뮬레이션')
    print('=' * 100)

    # 12 multistart 진입 통계
    eligible = dates[:-10]
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    base_ticker_count = Counter()
    new_ticker_count = Counter()
    base_slot1 = Counter()
    new_slot1 = Counter()
    for sd in fixed_starts:
        b_entries = simulate_with_tickers(dates, data, price_full, [33,33,34], 3, 10, 0, start_date=sd)
        n_entries = simulate_with_tickers(dates, data, price_full, [80,20], 3, 10, 3, start_date=sd)
        for tk, si in b_entries:
            base_ticker_count[tk] += 1
            if si == 0: base_slot1[tk] += 1
        for tk, si in n_entries:
            new_ticker_count[tk] += 1
            if si == 0: new_slot1[tk] += 1

    print(f'\n[baseline (3,10,3)] 12 multistart 진입 종목 Top 20')
    print(f'{"ticker":<10} {"total":>6} {"slot1":>7}')
    for tk, cnt in base_ticker_count.most_common(20):
        s1 = base_slot1.get(tk, 0)
        print(f'  {tk:<8} {cnt:>5} {s1:>7}')

    print(f'\n[(3,10,2) 80/20 b=3] 12 multistart 진입 종목 Top 20')
    print(f'{"ticker":<10} {"total":>6} {"slot1":>7}')
    for tk, cnt in new_ticker_count.most_common(20):
        s1 = new_slot1.get(tk, 0)
        print(f'  {tk:<8} {cnt:>5} {s1:>7}')

    # 차이 분석
    print(f'\n[차이] 새 후보 - baseline')
    all_tks = set(base_ticker_count.keys()) | set(new_ticker_count.keys())
    diffs = [(tk, new_ticker_count.get(tk, 0) - base_ticker_count.get(tk, 0)) for tk in all_tks]
    diffs.sort(key=lambda x: -x[1])
    print(f'{"ticker":<10} {"diff":>5} {"baseline":>8} {"new":>5}')
    for tk, d in diffs:
        if d == 0: continue
        b = base_ticker_count.get(tk, 0)
        n = new_ticker_count.get(tk, 0)
        marker = '+' if d > 0 else '-'
        print(f'  {tk:<8} {marker}{abs(d):>4} {b:>7} {n:>5}')

    # 5/22 (오늘) 메시지 시뮬레이션
    print()
    print('=' * 100)
    print(f'5/22 (마지막 거래일) 메시지 시뮬레이션')
    print('=' * 100)
    last_d = dates[-1]
    today_data = data[last_d]

    # baseline ranking
    base_ranks = sorted(today_data.items(), key=lambda x: x[1].get('p2', 999))
    # new ranking
    new_ranks_d = rerank(last_d, today_data, 3, dates, price_full)
    new_sorted = sorted(today_data.items(), key=lambda x: new_ranks_d.get(x[0], 999))

    print(f'\n[baseline] Top 5 종목:')
    for i, (tk, info) in enumerate(base_ranks[:5], 1):
        p30 = get_price_30d(tk, last_d, dates, price_full)
        c2 = '★' if classify_c2(info, tk, last_d, dates, price_full) else ' '
        print(f'  {i}. {tk:<7} p2={info.get("p2")} {c2} eps_w={info.get("eps_w"):+.1f} p30d={p30:+.1f if p30 else "?"}%')

    print(f'\n[(3,10,2) 80/20 b=3] new rank Top 5 (C2 boost 적용):')
    for i, (tk, info) in enumerate(new_sorted[:5], 1):
        old_p2 = info.get('p2')
        new_r = new_ranks_d.get(tk)
        p30 = get_price_30d(tk, last_d, dates, price_full)
        c2 = '★' if classify_c2(info, tk, last_d, dates, price_full) else ' '
        change = ''
        if old_p2 and new_r and old_p2 != new_r:
            change = f' (was rank {old_p2})'
        print(f'  {i}. {tk:<7} new_rank={new_r} {c2}{change} eps_w={info.get("eps_w"):+.1f if info.get("eps_w") is not None else 0} p30d={p30:+.1f if p30 else "?"}%')


if __name__ == '__main__':
    main()
