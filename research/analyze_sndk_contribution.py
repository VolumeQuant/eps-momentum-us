"""SNDK 누적 기여도 분석 — baseline vs 새 후보 비교

질문: SNDK 같은 strong trend continuation을 놓치는 게 정말 맞나?
시스템 시작 (2/12) ~ 5/22 SNDK 알파 기여 정확 측정.
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
LOOKBACK = 30

sys.path.insert(0, str(ROOT))


def load_all_with_score(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    import daily_runner as dr
    score_cache = {}
    for d in dates:
        try:
            _, sm = dr._build_score_100_map(d)
            score_cache[d] = sm
        except Exception:
            score_cache[d] = {}
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
                'score_100': score_cache.get(d, {}).get(tk, 0),
            }
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
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    return {tk: i+1 for i, (_, tk) in enumerate(candidates)}


def calc_weights(strategy, scores, slots):
    n = len(scores)
    if n == 0: return [0] * slots
    if n == 1: return [100] + [0] * (slots-1)
    if strategy == 'baseline_equal_3':
        base = 100 // slots
        rem = 100 - base * slots
        return [base + (1 if i < rem else 0) for i in range(slots)]
    elif strategy == 'A2_3slot':
        return [50, 30, 20]
    elif strategy == 'fixed_80_20':
        return [80, 20]
    elif strategy == 'dynamic_5':
        if n >= 2 and scores[0] - scores[1] > 5:
            return [80, 20]
        return [50, 50]
    return [50, 50]


def simulate_trace_sndk(dates, data, price_full, strategy, slots, entry, exit_, c2_boost):
    """SNDK 관련 진입/이탈 추적"""
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    sndk_log = []  # SNDK 관련 이벤트
    last_d = dates[-1]

    for di, today in enumerate(dates):
        if today not in data: continue
        today_data = data[today]
        new_ranks = rerank(today, today_data, c2_boost, dates, price_full)
        new_consec = defaultdict(int)
        for tk, r in new_ranks.items():
            if r <= 30:
                new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            for tk, info in portfolio.items():
                cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                prev_p = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if cur_p and prev_p and prev_p > 0:
                    w = info['weight'] / 100.0
                    day_ret += w * (cur_p - prev_p) / prev_p * 100
        daily_returns.append(day_ret)

        # SNDK 보유 중이면 매일 NAV에 기여 측정
        if 'SNDK' in portfolio:
            sndk = portfolio['SNDK']
            cur_p = today_data.get('SNDK', {}).get('price') or price_full.get(today, {}).get('SNDK')
            if cur_p:
                cumul_ret = (cur_p - sndk['entry_price']) / sndk['entry_price'] * 100
                # 매일 contribution 도 따로 기록 안 함, 종합만

        # Exit
        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2:
                if tk == 'SNDK':
                    cur_p = today_data.get(tk, {}).get('price')
                    if cur_p:
                        ret = (cur_p - portfolio[tk]['entry_price']) / portfolio[tk]['entry_price'] * 100
                        sndk_log.append(f'{today} SNDK 매도 (min_seg) ret {ret:+.2f}%, 보유 {di - portfolio[tk]["entry_di"]}일, weight {portfolio[tk]["weight"]:.0f}%')
                exited.append(tk); continue
            if rank is None or rank > exit_:
                if tk == 'SNDK':
                    cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                    if cur_p:
                        ret = (cur_p - portfolio[tk]['entry_price']) / portfolio[tk]['entry_price'] * 100
                        reason = 'rank_NULL' if rank is None else f'rank>{exit_}'
                        sndk_log.append(f'{today} SNDK 매도 ({reason}) ret {ret:+.2f}%, 보유 {di - portfolio[tk]["entry_di"]}일, weight {portfolio[tk]["weight"]:.0f}%')
                exited.append(tk); continue
        for tk in exited: del portfolio[tk]

        # Entry
        if len(portfolio) < slots:
            cands = []
            for tk, new_r in sorted(new_ranks.items(), key=lambda x: x[1]):
                if new_r > entry: break
                if tk in portfolio: continue
                if consecutive.get(tk, 0) < 3: continue
                info = today_data.get(tk, {})
                min_seg = info.get('min_seg', 0)
                if min_seg < 0: continue
                price = info.get('price')
                score = info.get('score_100', 0)
                if price and price > 0:
                    cands.append((tk, price, score))
            free_slots = slots - len(portfolio)
            new_entries = cands[:free_slots]
            if new_entries:
                existing = [(tk, today_data.get(tk, {}).get('score_100', 0), 'old', info['entry_price'])
                            for tk, info in portfolio.items()]
                all_in_pf = existing + [(tk, sc, 'new', price) for tk, price, sc in new_entries]
                all_in_pf.sort(key=lambda x: -x[1])
                scores = [sc for _, sc, _, _ in all_in_pf]
                weights = calc_weights(strategy, scores, slots)
                new_portfolio = {}
                for i, (tk, sc, typ, price) in enumerate(all_in_pf):
                    w = weights[i] if i < len(weights) else 0
                    ep = portfolio[tk]['entry_price'] if typ == 'old' else price
                    entry_di = portfolio[tk]['entry_di'] if typ == 'old' else di
                    new_portfolio[tk] = {'entry_price': ep, 'weight': w, 'score': sc, 'entry_di': entry_di}
                portfolio = new_portfolio
                # SNDK 새 진입 또는 weight 변경
                for tk, sc, typ, price in all_in_pf:
                    if tk == 'SNDK':
                        if typ == 'new':
                            sndk_log.append(f'{today} SNDK 진입 (rank {new_ranks.get(tk)}, score {sc:.1f}) at ${price:.2f}, weight {new_portfolio[tk]["weight"]:.0f}%')
                        elif portfolio.get('SNDK') and 'old_weight' not in portfolio.get('SNDK', {}):
                            pass  # weight 변경

    # SNDK 보유 중 종료 (open position)
    if 'SNDK' in portfolio:
        sndk = portfolio['SNDK']
        final_p = data.get(last_d, {}).get('SNDK', {}).get('price') or price_full.get(last_d, {}).get('SNDK')
        if final_p:
            ret = (final_p - sndk['entry_price']) / sndk['entry_price'] * 100
            sndk_log.append(f'{last_d} SNDK 보유 중 (open) ret {ret:+.2f}%, weight {sndk["weight"]:.0f}%')

    # 총 cum
    cum = 1.0
    for r in daily_returns:
        cum *= (1 + r/100)
    return {'total_return': (cum-1)*100, 'sndk_log': sndk_log}


def main():
    dates, data, price_full = load_all_with_score(DB_PATH)
    print('=' * 110)
    print(f'SNDK 누적 기여도 분석 — {dates[0]} ~ {dates[-1]}')
    print('=' * 110)

    scenarios = [
        ('baseline (3,10,3) 균등',          'baseline_equal_3', 3, 30, 10, 0),
        ('A2 (3,10,3) 50/30/20',           'A2_3slot',        3, 30, 10, 0),
        ('(2,10,2) 80/20 b=3',             'fixed_80_20',     2, 30, 10, 3),
        ('(2,10,2) dynamic_5 b=3',         'dynamic_5',       2, 30, 10, 3),
    ]

    print(f'\n각 시나리오의 SNDK 진입/이탈 history:\n')
    for label, strat, slots, entry, exit_, b in scenarios:
        res = simulate_trace_sndk(dates, data, price_full, strat, slots, entry, exit_, b)
        print(f'[{label}] — 누적 {res["total_return"]:+.2f}%')
        if res['sndk_log']:
            for line in res['sndk_log']:
                print(f'  {line}')
        else:
            print(f'  SNDK 진입 0건')
        print()

    # SNDK가 매일 part2_rank 어디 있었는지
    print('=' * 110)
    print('SNDK의 daily part2_rank + score_100 + case (전체 기간)')
    print('=' * 110)
    print(f'{"date":<12} {"p2":>4} {"price":>8} {"score":>7} {"eps_w":>7} {"p30d":>9} {"case":<5}')
    for d in dates:
        info = data.get(d, {}).get('SNDK')
        if not info: continue
        p2 = info.get('p2')
        if p2 is None: continue
        price = info.get('price')
        eps_w = info.get('eps_w')
        score = info.get('score_100', 0)
        p30 = get_price_30d('SNDK', d, dates, price_full)
        case = ' '
        if eps_w is not None and p30 is not None:
            if eps_w > 0 and p30 < 0: case = 'C2'
            elif eps_w > 0 and p30 >= 0: case = 'C1'
        p30_str = f'{p30:+.1f}%' if p30 is not None else '?'
        eps_w_str = f'{eps_w:+.1f}' if eps_w is not None else '?'
        print(f'{d:<12} {p2:>4} {price:>7.2f} {score:>7.1f} {eps_w_str:>7} {p30_str:>9} {case:<5}')


if __name__ == '__main__':
    main()
