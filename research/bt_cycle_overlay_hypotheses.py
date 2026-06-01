# -*- coding: utf-8 -*-
"""사이클 overlay 가설 검증 BT v2 (simulator fix)
total_cash 단일 pool 모델 + 2step_t15 동적 weight + entry_fixed.
"""
import sys, sqlite3, random, statistics, json, time
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
TI_PATH = ROOT / 'ticker_info_cache.json'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10

INDUSTRY_TO_SECTOR = {
    '반도체': 'Tech', '반도체장비': 'Tech', '응용SW': 'Tech', '인프라SW': 'Tech',
    'IT서비스': 'Tech', '하드웨어': 'Tech', '통신장비': 'Tech', '계측기기': 'Tech',
    '전자부품': 'Tech', '금융데이터': 'Tech',
    '산업기계': 'Industrials', '방산': 'Industrials', '건설': 'Industrials',
    '건축자재': 'Industrials', '자동차부품': 'Industrials', '산업유통': 'Industrials',
    '비즈니스서비스': 'Industrials', '전기장비': 'Industrials', '여행': 'Industrials',
    '바이오': 'Healthcare', '의료기기': 'Healthcare', '의료용품': 'Healthcare',
    '진단연구': 'Healthcare', '대형제약': 'Healthcare', '특수제약': 'Healthcare',
    '의료시설': 'Healthcare',
    '지역은행': 'Financials', '자산운용': 'Financials', '자본시장': 'Financials',
    '대형은행': 'Financials', '손해보험': 'Financials', '생명보험': 'Financials',
    '신용서비스': 'Financials', '특수보험': 'Financials',
    '석유미드스트림': 'Energy', '석유가스': 'Energy', '석유종합': 'Energy', '석유장비': 'Energy',
    '특수화학': 'Materials', '금': 'Materials', '포장재': 'Materials',
    '리츠주거': 'RealEstate', '리츠특수': 'RealEstate', '리츠산업': 'RealEstate',
    '리츠소매': 'RealEstate',
    '전력': 'Utilities',
    '엔터': 'CommServ', '외식': 'ConsDisc', '전문소매': 'ConsDisc',
    '식품': 'ConsStap',
    '통신': 'CommServ',
    '기타': 'Other',
}

def get_sector(ticker, ti_cache):
    info = ti_cache.get(ticker, {})
    ind = info.get('industry', '기타')
    return INDUSTRY_TO_SECTOR.get(ind, 'Other')


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
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, score
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
                          'min_seg': min(segs) if segs else 0,
                          'adj_gap': r[8] or 0, 'score': r[9] or 0}
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    with open(TI_PATH, encoding='utf-8') as f:
        ti = json.load(f)
    return dates, data, price_full, ti


def simulate(dates_all, data, price_full, ti_cache,
             weights_mode='2step_t15',
             entry=3, exit_=10,
             stop_loss=None,
             sector_cap=None,
             start_date=None):
    """v84 entry_fixed — total_cash 단일 pool 모델"""
    max_slots = 2
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all

    INIT_CAP = 100.0
    total_cash = INIT_CAP
    slot_holding = [None, None]  # (ticker, shares, entry_price, entry_date)
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
        score_map = {tk: v['score'] for tk, v in today_data.items() if v.get('p2') is not None}
        new_consec = defaultdict(int)
        for tk in rank_map: new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec

        # PV
        pv_today = total_cash
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, _ = slot_holding[i]
            p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            if p is None: p = entry_price
            pv_today += shares * p

        if prev_pv > 0:
            daily_returns.append((pv_today - prev_pv) / prev_pv * 100)
        else:
            daily_returns.append(0)
        prev_pv = pv_today

        # 이탈
        for i in range(max_slots):
            if slot_holding[i] is None: continue
            tk, shares, entry_price, entry_date = slot_holding[i]
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            cur_p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
            should_exit = False
            if stop_loss is not None and cur_p is not None:
                if (cur_p / entry_price - 1) <= stop_loss:
                    should_exit = True
            if not should_exit:
                if min_seg < -2:
                    should_exit = True
                elif rank is None or rank > exit_:
                    should_exit = True
            if should_exit:
                exit_p = cur_p if cur_p else entry_price
                total_cash += shares * exit_p
                slot_holding[i] = None

        # 진입
        if slot_holding[0] is None and slot_holding[1] is None:
            # 모든 슬롯 비었음 → cash 풀 사용
            cands = sorted(
                [(rank_map[tk], score_map.get(tk, 0), tk) for tk in rank_map
                 if rank_map[tk] <= entry
                 and (today_data[tk].get('min_seg') is None or today_data[tk]['min_seg'] >= 0)
                 and consecutive.get(tk, 0) >= 3
                 and today_data[tk].get('price')],
                key=lambda x: x[0]
            )
            # Sector cap
            picked = []
            sector_count = defaultdict(int)
            for p2_val, sc, tk in cands:
                if len(picked) >= max_slots: break
                sec = get_sector(tk, ti_cache)
                if sector_cap and sec in sector_cap:
                    cap = sector_cap[sec]
                    if sector_count[sec] + 1 > int(cap * max_slots + 0.5):
                        continue
                picked.append((p2_val, sc, tk))
                sector_count[sec] += 1
            # weight
            if len(picked) == 1:
                _, _, tk = picked[0]
                price = today_data[tk]['price']
                shares = total_cash / price
                slot_holding[0] = (tk, shares, price, today)
                total_cash = 0
            elif len(picked) >= 2:
                if weights_mode == 'static_90_10':
                    w = [0.9, 0.1]
                else:  # 2step_t15
                    s1, s2 = picked[0][1], picked[1][1]
                    gap = s1 - s2
                    if gap >= 15:
                        w = [1.0, 0.0]
                    else:
                        w = [0.5, 0.5]
                for i, (_, _, tk) in enumerate(picked[:2]):
                    if w[i] > 0:
                        price = today_data[tk]['price']
                        allocated = total_cash * w[i]
                        shares = allocated / price
                        slot_holding[i] = (tk, shares, price, today)
                # 정확한 cash 차감
                used = sum(w[:2]) * total_cash
                total_cash = total_cash - used
        else:
            # 일부 슬롯만 비었음 → 그 슬롯만 채움 (cash 남은 만큼)
            for i in range(max_slots):
                if slot_holding[i] is not None: continue
                if total_cash <= 0: continue
                cands = sorted(
                    [(rank_map[tk], tk) for tk in rank_map
                     if rank_map[tk] <= entry
                     and not any(h is not None and h[0] == tk for h in slot_holding)
                     and (today_data[tk].get('min_seg') is None or today_data[tk]['min_seg'] >= 0)
                     and consecutive.get(tk, 0) >= 3
                     and today_data[tk].get('price')],
                    key=lambda x: x[0]
                )
                for p2_val, tk in cands:
                    sec = get_sector(tk, ti_cache)
                    if sector_cap and sec in sector_cap:
                        cap = sector_cap[sec]
                        existing_secs = [get_sector(h[0], ti_cache) for h in slot_holding if h]
                        if sec in existing_secs and (existing_secs.count(sec) + 1) > int(cap * max_slots + 0.5):
                            continue
                    price = today_data[tk]['price']
                    shares = total_cash / price
                    slot_holding[i] = (tk, shares, price, today)
                    total_cash = 0
                    break

    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum - peak)/peak*100
        max_dd = min(max_dd, dd)
    max_day_loss = min(daily_returns) if daily_returns else 0
    return {
        'total_return': (cum-1)*100, 'max_dd': max_dd,
        'max_day_loss': max_day_loss,
    }


VARIANTS = [
    ('baseline_v84',          {'weights_mode': '2step_t15'}),
    ('SL_-10',                {'weights_mode': '2step_t15', 'stop_loss': -0.10}),
    ('SL_-15',                {'weights_mode': '2step_t15', 'stop_loss': -0.15}),
    ('SL_-20',                {'weights_mode': '2step_t15', 'stop_loss': -0.20}),
    ('TechCap_50',            {'weights_mode': '2step_t15', 'sector_cap': {'Tech': 0.5}}),
    ('TechCap_50+SL-15',      {'weights_mode': '2step_t15', 'sector_cap': {'Tech': 0.5}, 'stop_loss': -0.15}),
    ('static_90_10',          {'weights_mode': 'static_90_10'}),
    ('SL_-15+90_10',          {'weights_mode': 'static_90_10', 'stop_loss': -0.15}),
]


def main():
    print('=' * 110)
    print('Cycle overlay 가설 BT v2 — entry_fixed (total_cash pool)')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} paired')
    print('=' * 110)
    dates, data, price_full, ti = load_data()
    print(f'dates: {len(dates)} ({dates[0]} ~ {dates[-1]})')
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))

    all_results = {}
    print(f'\n{"variant":<22}{"avg":>10}{"med":>10}{"std":>8}{"mdd":>10}{"maxday":>10}{"sharpe":>10}')
    for name, kwargs in VARIANTS:
        t0 = time.time()
        rets, mdds, mdls, seed_avgs = [], [], [], []
        for chosen in seed_starts:
            sr = []
            for sd in chosen:
                r = simulate(dates, data, price_full, ti, start_date=sd, **kwargs)
                rets.append(r['total_return']); mdds.append(r['max_dd'])
                mdls.append(r['max_day_loss']); sr.append(r['total_return'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[name] = {'rets': rets, 'mdds': mdds, 'mdls': mdls, 'seed_avgs': seed_avgs}
        avg = sum(rets)/len(rets)
        med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets)
        mdd = min(mdds); mdl = min(mdls)
        sharpe = avg/std if std > 0 else 0
        marker = ' ★' if name == 'baseline_v84' else '  '
        print(f'{marker}{name:<20}{avg:>+8.2f}%{med:>+8.2f}%{std:>8.1f}{mdd:>+8.2f}%{mdl:>+8.2f}%{sharpe:>+8.2f} [{time.time()-t0:.1f}s]')

    print()
    print('=' * 110)
    print('paired vs baseline_v84')
    print('=' * 110)
    base = all_results['baseline_v84']['seed_avgs']
    print(f'  {"variant":<22}{"avg_lift":>10}{"med_lift":>10}{"min":>10}{"max":>10}{"wins":>10}  verdict')
    print('  ' + '-'*95)
    for name, _ in VARIANTS:
        if name == 'baseline_v84': continue
        new = all_results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        verdict = '✓✓ 우월' if wins >= 70 else '✓ 우월' if wins >= 60 else '~ 동등' if wins >= 40 else '✗ 열세'
        avg_l = sum(lifts)/len(lifts)
        med_l = statistics.median(lifts)
        print(f'  {name:<22}{avg_l:>+8.2f}%p{med_l:>+8.2f}%p{min(lifts):>+8.2f}%p{max(lifts):>+8.2f}%p{wins:>6}/{N_SEEDS}  {verdict}')


if __name__ == '__main__':
    main()
