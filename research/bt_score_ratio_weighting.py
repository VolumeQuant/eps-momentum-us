"""Score-ratio weighting BT — 점수 차이 기반 비중 결정

사용자 질문: "점수 100/94/93.9 같이 차이 미세하면 80/20이 너무 1위 치우침"

설계:
1. production w_gap 기반 score_100 계산 (1위=100, 다른 종목 비율)
2. C2 boost 후 새 rank 적용
3. 새 score (boost 후) 기반 weighting:
   - Fixed 80/20 (현재 후보)
   - Fixed 50/50 (보수)
   - Score-ratio (점수 비율)
   - Dynamic (점수 차이 임계값별)

비중 측정 (BT 전 분석): 1/2위 score 차이 분포
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
LOOKBACK = 30

# production score_100 활용
sys.path.insert(0, str(ROOT))


def load_all_with_score(db_path):
    """일자별 part2_rank, price, score_100, eps_w 한번에 로드"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    # part2_rank의 base 점수를 (31 - p2)로 시작, w_gap 정보 없으므로 단순 모델
    # 실제 production score_100 cache 만들기 (daily_runner._build_score_100_map 호출)
    print('[Init] _build_score_100_map 캐싱...')
    import daily_runner as dr
    score_cache = {}  # {date: {ticker: score_100}}
    for d in dates:
        try:
            _, score_map = dr._build_score_100_map(d)
            score_cache[d] = score_map
        except Exception as e:
            print(f'  {d}: 캐시 실패 {e}')
            score_cache[d] = {}
    print(f'  완료: {len(score_cache)}일')

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
    """rerank + score 보존 (production score 그대로, rank만 변경)"""
    candidates = []
    for tk, info in today_data.items():
        p2 = info.get('p2')
        if p2 is None: continue
        is_c2 = classify_c2(info, tk, today, dates_list, price_full)
        score = (31 - p2) + (c2_boost if is_c2 else 0)
        candidates.append((-score, tk))
    candidates.sort()
    new_ranks = {tk: i+1 for i, (_, tk) in enumerate(candidates)}
    return new_ranks


def determine_weights(strategy, scores):
    """strategy별 weights 계산.
    scores: [score_1, score_2] (선택된 종목 점수)
    Returns: weights list
    """
    if strategy == 'fixed_80_20':
        return [80, 20]
    elif strategy == 'fixed_70_30':
        return [70, 30]
    elif strategy == 'fixed_50_50':
        return [50, 50]
    elif strategy == 'score_ratio':
        s1, s2 = scores[0], scores[1]
        total = s1 + s2
        if total <= 0: return [50, 50]
        return [s1/total*100, s2/total*100]
    elif strategy == 'dynamic_5':
        # 점수 차이 > 5: 80/20, ≤ 5: 50/50
        diff = scores[0] - scores[1]
        return [80, 20] if diff > 5 else [50, 50]
    elif strategy == 'dynamic_10':
        diff = scores[0] - scores[1]
        return [80, 20] if diff > 10 else [50, 50]
    elif strategy == 'power_2':
        # weight = score^2 / sum(score^2)
        s1, s2 = scores[0], scores[1]
        ss1, ss2 = s1**2, s2**2
        total = ss1 + ss2
        if total <= 0: return [50, 50]
        return [ss1/total*100, ss2/total*100]
    return [50, 50]


def simulate(dates_all, data, price_full, strategy, entry, exit_, c2_boost,
             start_date=None, log_score_diffs=False):
    slots = 2
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    consecutive = defaultdict(int)
    daily_returns = []
    score_diffs = []
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

        exited = []
        for tk in list(portfolio.keys()):
            rank = new_ranks.get(tk); min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2: exited.append(tk); continue
            if rank is None or rank > exit_: exited.append(tk); continue
        for tk in exited: del portfolio[tk]

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
            # 슬롯 빈 자리 채우기
            free_slots = slots - len(portfolio)
            new_entries = cands[:free_slots]
            if new_entries:
                # weights 계산 (현재 슬롯에 들어가는 종목들)
                # 기존 보유 종목 + 신규 종목 score 종합
                existing = [(tk, info.get('score_100', 0)) for tk, info in portfolio.items()]
                # 점수 순으로 정렬
                all_in_portfolio = existing + [(tk, sc) for tk, _, sc in new_entries]
                all_in_portfolio.sort(key=lambda x: -x[1])  # 점수 내림차순
                scores = [sc for _, sc in all_in_portfolio]
                if len(scores) == 2 and log_score_diffs:
                    score_diffs.append((today, scores[0], scores[1], scores[0]-scores[1]))
                # 가중치
                if len(scores) >= 2:
                    new_weights = determine_weights(strategy, scores[:2])
                elif len(scores) == 1:
                    new_weights = [100]
                else:
                    new_weights = []
                # 기존 종목 weight 재배정
                for i, (tk, sc) in enumerate(all_in_portfolio):
                    if tk in portfolio:
                        portfolio[tk]['weight'] = new_weights[i] if i < len(new_weights) else 0
                # 신규 진입
                for j, (tk, price, sc) in enumerate(new_entries):
                    rank_in_pf = next((i for i, (t, _) in enumerate(all_in_portfolio) if t == tk), 0)
                    weight = new_weights[rank_in_pf] if rank_in_pf < len(new_weights) else 0
                    portfolio[tk] = {'entry_price': price, 'weight': weight, 'score_100': sc}

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd, 'score_diffs': score_diffs}


def run(strategy, entry, exit_, c2_boost, dates, data, price_full, seed_starts):
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, strategy, entry, exit_, c2_boost, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 100)
    print('Score-ratio weighting + dynamic BT')
    print('=' * 100)
    dates, data, price_full = load_all_with_score(DB_PATH)

    # Phase 1: 점수 차이 분포 분석
    print()
    print('=' * 100)
    print('Phase 1: 1/2위 점수 차이 분포 (production score_100 기준, 전체 기간)')
    print('=' * 100)
    # 매일 production part2_rank 1, 2위 score
    daily_diffs = []
    for d in dates:
        scored = [(info.get('score_100', 0), tk) for tk, info in data[d].items()
                  if info.get('p2') and info['p2'] <= 30]
        scored.sort(key=lambda x: -x[0])  # 높은 점수 먼저
        if len(scored) >= 2:
            s1, _ = scored[0]
            s2, _ = scored[1]
            diff = s1 - s2
            daily_diffs.append((d, s1, s2, diff))
    diffs_only = [x[3] for x in daily_diffs]
    print(f'분석 일자 수: {len(daily_diffs)}')
    if diffs_only:
        print(f'  1/2위 점수 차이 평균: {sum(diffs_only)/len(diffs_only):+.2f}')
        print(f'  중앙값: {sorted(diffs_only)[len(diffs_only)//2]:+.2f}')
        print(f'  최소: {min(diffs_only):+.2f}, 최대: {max(diffs_only):+.2f}')
        # 분포
        bins = [(0,2), (2,5), (5,10), (10,20), (20,100)]
        print(f'\n  차이 구간별 발생 빈도:')
        for lo, hi in bins:
            cnt = sum(1 for d in diffs_only if lo <= d < hi)
            print(f'    {lo:>3}~{hi:>3}: {cnt:>3}일 ({cnt/len(diffs_only)*100:.1f}%)')

    print()
    print('  예시 (최근 10일):')
    print(f'  {"date":<12} {"1위":>5} {"2위":>5} {"차이":>6}')
    for d, s1, s2, diff in daily_diffs[-10:]:
        print(f'  {d:<12} {s1:>5.1f} {s2:>5.1f} {diff:>+5.1f}')

    # Phase 2: weighting 변형 BT
    print()
    print('=' * 100)
    print('Phase 2: Weighting 변형 BT — (2,10,2) + C2 boost=3')
    print('=' * 100)
    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))
    step = max(1, len(eligible) // 13)
    fixed_starts = [eligible[step * i] for i in range(1, 13)]

    strategies = ['fixed_80_20', 'fixed_70_30', 'fixed_50_50', 'score_ratio',
                  'dynamic_5', 'dynamic_10', 'power_2']
    print()
    print(f'{"strategy":<20} {"R avg":>9} {"M avg":>9} {"worst MDD":>10} {"M min":>10}')
    print('-' * 70)
    results = {}
    for strat in strategies:
        r_random = run(strat, 30, 10, 3, dates, data, price_full, seed_starts)  # entry=30 production-aligned
        multi_rets = []
        multi_mdds = []
        for sd in fixed_starts:
            r = simulate(dates, data, price_full, strat, 30, 10, 3, start_date=sd)
            multi_rets.append(r['total_return'])
            multi_mdds.append(r['max_dd'])
        results[strat] = {'random': r_random, 'multi': multi_rets, 'multi_mdds': multi_mdds}
        r_avg = sum(r_random['rets'])/len(r_random['rets'])
        m_avg = sum(multi_rets)/12
        worst_mdd = min(r_random['mdds'])
        m_min = min(multi_rets) - sum(multi_rets)/12  # 평균 대비 worst lift...
        # 차라리 absolute worst
        m_worst = min(multi_rets)
        marker = '★' if strat == 'fixed_80_20' else '  '
        print(f'{marker} {strat:<18} {r_avg:+7.2f}% {m_avg:+7.2f}% {worst_mdd:+9.2f}% {m_worst:+9.2f}%')

    # Paired vs fixed_80_20
    print()
    print('=' * 100)
    print('paired vs fixed_80_20 (현재 최강 후보)')
    print('=' * 100)
    print(f'{"strategy":<20} {"R lift":>10} {"R wins":>10} {"M lift":>10} {"M wins":>8}')
    print('-' * 65)
    base = results['fixed_80_20']['random']['seed_avgs']
    base_m = results['fixed_80_20']['multi']
    for strat in strategies:
        if strat == 'fixed_80_20': continue
        new_ = results[strat]['random']['seed_avgs']
        lifts = [b - a for a, b in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        new_m = results[strat]['multi']
        m_lifts = [b - a for a, b in zip(base_m, new_m)]
        m_wins = sum(1 for l in m_lifts if l > 0)
        m_lift = sum(m_lifts)/12
        print(f'  {strat:<18} {avg_l:+8.2f}%p {wins:>5}/{N_SEEDS} {m_lift:+8.2f}%p {m_wins:>5}/12')


if __name__ == '__main__':
    main()
