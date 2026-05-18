"""MA 변형 dense grid — NEW simulator로 sweet spot 정확히 탐색

기존: ma20/50/100/120/150/200 (sparse)
신규: ma40/60/80/90/110/130 (dense, ma120 주변 확인)

방법:
  1. price parquet에서 MA_N 계산
  2. ext_current.db 복제 → regenerate (ma_pass_dynamic) → 신규 DB
  3. NEW simulator로 BT
  4. paired vs current

100 seed × 3 starts paired (기존 NEW 재검증과 동일 조건)
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

import pandas as pd
import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'
PRICE_PARQUET = Path(__file__).parent / 'price_history_for_ma_bt.parquet'

N_SEEDS = 100
SAMPLES = 3
MIN_HOLD_DAYS = 10

# 신규 추가 — dense grid
NEW_PERIODS = [40, 60, 80, 90, 110, 130]


def ma_pass_dynamic(ticker, today_str, price, ma_df):
    if price is None or price <= 0 or ma_df is None:
        return False
    if ticker not in ma_df.columns:
        return False
    col = ma_df[ticker]
    eligible = col.index[col.index <= today_str]
    if len(eligible) == 0:
        return False
    ma_val = col.loc[eligible[-1]]
    if pd.isna(ma_val):
        return False
    return price > ma_val


def _load_ticker_industries():
    import json
    cache_path = ROOT / 'ticker_info_cache.json'
    try:
        with open(cache_path, encoding='utf-8') as f:
            cache = json.load(f)
        return {tk: info.get('industry', '') for tk, info in cache.items()}
    except Exception:
        return {}


def regenerate(test_db, ma_df):
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
    ).fetchall()]
    industry_map = _load_ticker_industries()
    commodity_industries = dr.COMMODITY_INDUSTRIES
    commodity_tickers = dr.COMMODITY_TICKERS

    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL')
    conn.commit()

    for today in dates:
        rows = cur.execute('''
            SELECT ticker, adj_score, adj_gap, eps_chg_weighted, price, ma60, ma120,
                   ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30,
                   operating_margin, gross_margin, free_cashflow, roe,
                   ntm_7d, ntm_30d, ntm_60d
            FROM ntm_screening WHERE date=?
        ''', (today,)).fetchall()
        if not rows:
            continue
        eligible = []
        for r in rows:
            (tk, asc, ag, eps_w, px, m60, m120, nc, n90, rg, na, ru, rd,
             om, gm, fcf, roe, n7, n30, n60) = r
            if asc is None or asc <= 9: continue
            if ag is None: continue
            if px is None or px < 10: continue
            if nc is None or nc <= 0: continue
            if eps_w is None or eps_w <= 0: continue
            if not ma_pass_dynamic(tk, today, px, ma_df): continue
            if rg is None or rg < 0.10: continue
            if na is None or na < 3: continue
            if ru is None or ru < 3: continue
            total = (ru or 0) + (rd or 0)
            if total > 0 and (rd or 0) / total > 0.3: continue
            if om is not None and gm is not None and om < 0.10 and gm < 0.30: continue
            if om is not None and om < 0.05: continue
            if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
            if industry_map.get(tk, '') in commodity_industries: continue
            if tk in commodity_tickers: continue
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b is not None and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else:
                    segs.append(0)
            if min(segs) < -2:
                continue
            eligible.append({
                'ticker': tk, 'adj_gap': ag, 'rev_up30': ru, 'num_analysts': na,
                'ntm_current': nc, 'ntm_90d': n90, 'rev_growth': rg,
            })
        if not eligible:
            continue
        for e in eligible:
            e['_conv_gap'] = dr._apply_conviction(
                e['adj_gap'], e['rev_up30'], e['num_analysts'],
                e['ntm_current'], e['ntm_90d'], rev_growth=e['rev_growth']
            )
        eligible.sort(key=lambda e: e['_conv_gap'])
        for i, e in enumerate(eligible, 1):
            cur.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (i, today, e['ticker'])
            )
        conn.commit()
        tickers = [e['ticker'] for e in eligible]
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        for rk, tk in enumerate(sorted_w[:30], 1):
            cur.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rk, today, tk)
            )
        conn.commit()
    conn.close()


# NEW simulator (price_full fallback)
def load_all(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
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
            data[d][tk] = {
                'p2': r[1], 'price': r[2],
                'min_seg': min(segs) if segs else 0,
            }
    price_full = defaultdict(dict)
    for tk, d, px in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'
    ):
        price_full[d][tk] = px
    conn.close()
    return dates, data, price_full


def simulate(dates_all, data, price_full, start_date=None,
             entry=3, exit_=10, slots=3):
    if start_date:
        dates = [d for d in dates_all if d >= start_date]
    else:
        dates = dates_all
    portfolio = {}
    daily_returns = []
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
        new_consec = defaultdict(int)
        for tk in rank_map:
            new_consec[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consec
        day_ret = 0
        if portfolio and di > 0:
            prev_d = dates[di-1]
            n = 0
            for tk in portfolio:
                p = today_data.get(tk, {}).get('price') or price_full.get(today, {}).get(tk)
                pr = data.get(prev_d, {}).get(tk, {}).get('price') or price_full.get(prev_d, {}).get(tk)
                if p and pr and pr > 0:
                    day_ret += (p - pr) / pr * 100
                    n += 1
            if n > 0:
                day_ret /= n
        daily_returns.append(day_ret)
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0)
            if min_seg < -2:
                exited.append(tk); continue
            if rank is None or rank > exit_:
                exited.append(tk); continue
        for tk in exited:
            del portfolio[tk]
        vacancies = slots - len(portfolio)
        if vacancies > 0:
            for tk, rank in sorted(rank_map.items(), key=lambda x: x[1]):
                if rank > entry or vacancies <= 0:
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
                    portfolio[tk] = {'entry_price': price}
                    vacancies -= 1
    cum = 1.0; peak = 1.0; max_dd = 0
    for r in daily_returns:
        cum *= (1 + r/100); peak = max(peak, cum)
        dd = (cum-peak)/peak*100; max_dd = min(max_dd, dd)
    return {'total_return': (cum-1)*100, 'max_dd': max_dd}


def run(db_path, seed_starts):
    dates, data, price_full = load_all(db_path)
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = simulate(dates, data, price_full, start_date=sd)
            rets.append(r['total_return']); mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


def main():
    print('=' * 110)
    print(f'MA dense grid — NEW simulator (ma120 주변 sweet spot 탐색)')
    print(f'추가 periods: {NEW_PERIODS}')
    print('=' * 110)

    t0 = time.time()
    print('\n[Load price parquet]')
    close_df = pd.read_parquet(PRICE_PARQUET)
    print(f'  Shape: {close_df.shape}')

    # 신규 DB 생성
    for period in NEW_PERIODS:
        db = GRID / f'ext_ma{period}_dense.db'
        if db.exists():
            print(f'  ma{period}: 이미 존재, regenerate 스킵')
            continue
        t1 = time.time()
        shutil.copy(DB_ORIGINAL, db)
        ma_df = close_df.rolling(window=period, min_periods=period).mean()
        regenerate(db, ma_df)
        print(f'  ma{period}: regenerate {time.time()-t1:.1f}s')

    # seed_starts (current 기준)
    dates_cur, _, _ = load_all(GRID / 'ext_current.db')
    eligible = dates_cur[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible, SAMPLES))

    # 비교 후보 (기존 + 신규)
    candidates = [
        ('current(ma120+ma60 fb)', GRID / 'ext_current.db'),
        ('ma40',  GRID / 'ext_ma40_dense.db'),
        ('ma50',  GRID / 'ext_ma50.db'),       # 기존
        ('ma60',  GRID / 'ext_ma60_dense.db'),
        ('ma80',  GRID / 'ext_ma80_dense.db'),
        ('ma90',  GRID / 'ext_ma90_dense.db'),
        ('ma100', GRID / 'ext_ma100.db'),      # 기존
        ('ma110', GRID / 'ext_ma110_dense.db'),
        ('ma120', GRID / 'ext_ma120.db'),      # 기존
        ('ma130', GRID / 'ext_ma130_dense.db'),
        ('ma150', GRID / 'ext_ma150.db'),      # 기존
    ]

    print()
    print('=' * 110)
    print('NEW simulator BT')
    print('=' * 110)
    print(f'  {"variant":<25} {"avg":>9} {"med":>9} {"worst MDD":>10} {"sharpe":>7}')
    print('  ' + '-' * 70)

    results = {}
    for name, db in candidates:
        if not db.exists():
            print(f'  {name:<25}  DB 없음 — skip')
            continue
        res = run(db, seed_starts)
        results[name] = res
        avg = sum(res['rets'])/len(res['rets'])
        med = sorted(res['rets'])[len(res['rets'])//2]
        worst_mdd = min(res['mdds'])
        std = statistics.pstdev(res['rets'])
        sharpe = avg/std if std > 0 else 0
        marker = '★' if 'current' in name else ' '
        print(f'  {marker} {name:<23} {avg:+8.2f}% {med:+8.2f}% {worst_mdd:+9.2f}% {sharpe:+6.2f}')

    print()
    print('=' * 110)
    print('paired vs current(ma120+ma60 fb)')
    print('=' * 110)
    print(f'  {"variant":<25} {"avg lift":>10} {"worst":>10} {"best":>10} {"wins":>10} {"verdict":>15}')
    print('  ' + '-' * 90)
    base = results['current(ma120+ma60 fb)']['seed_avgs']
    for name, db in candidates:
        if 'current' in name or name not in results:
            continue
        new_ = results[name]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new_)]
        wins = sum(1 for l in lifts if l > 0)
        avg_l = sum(lifts)/len(lifts)
        worst = min(lifts); best = max(lifts)
        rate = wins / N_SEEDS * 100
        verdict = '✓✓ 명확 우월' if rate >= 90 else '✓ 우월' if rate >= 75 else '◐ 약함' if rate >= 60 else '~ 동등' if rate >= 40 else '✗ 열세'
        print(f'  {name:<25} {avg_l:+8.2f}%p {worst:+8.2f}%p {best:+8.2f}%p {wins:>5}/{N_SEEDS}  {verdict}')

    print(f'\n총 소요: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
