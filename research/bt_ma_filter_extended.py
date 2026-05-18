"""MA 필터 확장 BT — MA20/MA50/MA100/MA120/MA150/MA200 변형 비교

전제: research/fetch_price_history.py로 14개월 가격 이력 parquet 생성 완료

변형:
  current        — production: MA120 with MA60 fallback (DB 저장값)
  ma20  / ma50 / ma100 / ma120 / ma150 / ma200 — yfinance 가격에서 재계산
  no_ma          — MA 필터 제거 (컨트롤)

방법:
  1. parquet에서 종목 × 일자 Close 로드
  2. 각 BT 일자별 종목별 MA_N 계산 (지난 N 거래일 평균)
  3. regenerate_for_variant_extended()로 part2_rank 재계산
  4. bt_breakout_hold.simulate_hold (entry=3, exit=10, slots=3, hold=0, v80.10c)
  5. 100 seed × 3 starts paired
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import pandas as pd
import daily_runner as dr
import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'
GRID.mkdir(exist_ok=True)
PRICE_PARQUET = Path(__file__).parent / 'price_history_for_ma_bt.parquet'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10

ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 3
HOLD_DAYS = 0


def build_ma_lookup(close_df, ma_period):
    """close_df (date × ticker) → {(date, ticker): MA_N}

    각 BT 일자에서 가장 가까운 (≤) 거래일의 MA를 반환.
    DB 일자 ≠ yfinance 거래일이 같진 않을 수 있으므로 forward-fill 사용.
    """
    ma_df = close_df.rolling(window=ma_period, min_periods=ma_period).mean()
    return ma_df  # date index (str YYYY-MM-DD), ticker columns


def ma_pass_dynamic(ticker, today_str, price, ma_df):
    """ma_df에서 today 시점의 MA 조회 → price > MA 여부"""
    if price is None or price <= 0 or ma_df is None:
        return False
    if ticker not in ma_df.columns:
        return False
    # today 이전 (포함) 가장 가까운 거래일의 MA
    col = ma_df[ticker]
    eligible_dates = col.index[col.index <= today_str]
    if len(eligible_dates) == 0:
        return False
    last_dt = eligible_dates[-1]
    ma_val = col.loc[last_dt]
    if pd.isna(ma_val):
        return False
    return price > ma_val


def ma_pass_db(price, ma60, ma120, variant):
    """DB 저장 ma60/ma120 사용 변형 (current, no_ma)"""
    if price is None or price <= 0:
        return False
    if variant == 'no_ma':
        return True
    if variant == 'current':
        if ma120 is not None:
            return price > ma120
        return ma60 is not None and price > ma60
    raise ValueError(variant)


def regenerate(test_db, variant, ma_df=None):
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening ORDER BY date'
    ).fetchall()]

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

            if variant in ('current', 'no_ma'):
                if not ma_pass_db(px, m60, m120, variant): continue
            else:
                if not ma_pass_dynamic(tk, today, px, ma_df): continue

            if rg is None or rg < 0.10: continue
            if na is None or na < 3: continue
            if ru is None or ru < 3: continue
            total = (ru or 0) + (rd or 0)
            if total > 0 and (rd or 0) / total > 0.3: continue
            if om is not None and gm is not None and om < 0.10 and gm < 0.30: continue
            if om is not None and om < 0.05: continue
            if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue

            # min_seg
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b is not None and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
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


def run_bt(db_path):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    if len(dates) <= MIN_HOLD_DAYS:
        return None
    eligible_starts = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        seed_rets = []
        for sd in chosen:
            r = bth.simulate_hold(
                dates, data, price_series, hold_days=HOLD_DAYS,
                entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
                max_slots=MAX_SLOTS, start_date=sd
            )
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            seed_rets.append(r['total_return'])
        seed_avgs.append(sum(seed_rets) / len(seed_rets))
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs}


# 변형 정의: (name, ma_period or None)
# None: DB ma60/ma120 사용 (current, no_ma)
VARIANTS = [
    ('current', None),
    ('no_ma', None),
    ('ma20', 20),
    ('ma50', 50),
    ('ma100', 100),
    ('ma120', 120),
    ('ma150', 150),
    ('ma200', 200),
]


def main():
    print('=' * 110)
    print('MA 필터 확장 BT — MA20~MA200 yfinance 기반 재계산')
    print(f'DB: {DB_ORIGINAL}')
    print(f'Price parquet: {PRICE_PARQUET}')
    print(f'params: entry={ENTRY_TOP}, exit={EXIT_TOP}, slots={MAX_SLOTS}, hold={HOLD_DAYS} (v80.10c)')
    print(f'seeds: {N_SEEDS} × {SAMPLES_PER_SEED} = {N_SEEDS*SAMPLES_PER_SEED} 시뮬/변형')
    print('=' * 110)

    if not PRICE_PARQUET.exists():
        print(f'ERROR: {PRICE_PARQUET} 없음. fetch_price_history.py 먼저 실행.')
        return

    print('\n[Load] Price parquet 로드 중...')
    close_df = pd.read_parquet(PRICE_PARQUET)
    print(f'  Shape: {close_df.shape}, dates: {close_df.index.min()} ~ {close_df.index.max()}')

    # 변형별 MA dataframe 사전 계산
    ma_dfs = {}
    for name, period in VARIANTS:
        if period is None:
            continue
        if period in ma_dfs:
            continue
        print(f'  Compute MA{period}...')
        ma_dfs[period] = close_df.rolling(window=period, min_periods=period).mean()

    all_results = {}
    for name, period in VARIANTS:
        db = GRID / f'ext_{name}.db'
        print(f'\n[{name}] DB 복제 + regenerate...')
        t0 = time.time()
        shutil.copy(DB_ORIGINAL, db)
        ma_df = ma_dfs.get(period) if period is not None else None
        regenerate(db, name, ma_df)
        print(f'  regenerate: {time.time()-t0:.1f}s')

        t1 = time.time()
        res = run_bt(db)
        if res is None:
            print('  데이터 부족 — skip')
            continue
        all_results[name] = res
        avg = sum(res['rets']) / len(res['rets'])
        med = sorted(res['rets'])[len(res['rets']) // 2]
        worst_mdd = min(res['mdds'])
        ra = avg / abs(worst_mdd) if worst_mdd < 0 else 0
        print(f'  BT: {time.time()-t1:.1f}s | avg={avg:+6.2f}% med={med:+6.2f}% '
              f'mdd={worst_mdd:+6.2f}% risk_adj={ra:+.2f}')

    print()
    print('=' * 110)
    print(f'결과 분포 ({N_SEEDS*SAMPLES_PER_SEED}개 시뮬/변형)')
    print('=' * 110)
    print(f'{"variant":<14} {"avg":>9} {"median":>9} {"std":>6} '
          f'{"p25":>9} {"p75":>9} {"min":>9} {"max":>9} {"MDD":>8} {"risk_adj":>9}')
    print('-' * 110)
    for name, _ in VARIANTS:
        if name not in all_results:
            continue
        r = all_results[name]
        rets = sorted(r['rets'])
        n = len(rets)
        avg = sum(rets) / n
        med = rets[n // 2]
        std = statistics.pstdev(rets)
        p25 = rets[n // 4]
        p75 = rets[3 * n // 4]
        mdd = min(r['mdds'])
        ra = avg / abs(mdd) if mdd < 0 else 0
        marker = ' ★' if name == 'current' else '  '
        print(f'{marker}{name:<12} {avg:+8.2f}% {med:+8.2f}% {std:>5.2f} '
              f'{p25:+8.2f}% {p75:+8.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}% '
              f'{mdd:+7.2f}% {ra:+8.2f}')

    if 'current' in all_results:
        print()
        print('=' * 110)
        print('current (production) 대비 paired 비교')
        print('=' * 110)
        base = all_results['current']['seed_avgs']
        print(f'{"vs":<14} {"avg lift":>10} {"min lift":>10} {"max lift":>10} '
              f'{"#wins":>7} {"#losses":>8} {"#ties":>6}')
        print('-' * 80)
        for name, _ in VARIANTS:
            if name == 'current' or name not in all_results:
                continue
            new = all_results[name]['seed_avgs']
            lifts = [b - a for a, b in zip(base, new)]
            wins = sum(1 for l in lifts if l > 0)
            losses = sum(1 for l in lifts if l < 0)
            ties = sum(1 for l in lifts if l == 0)
            avg_lift = sum(lifts) / len(lifts)
            verdict = '✓ 우월' if wins >= 70 else '✗ 열세' if losses >= 70 else '~ 동등'
            print(f'  {name:<12} {avg_lift:+9.2f}%p {min(lifts):+9.2f}%p {max(lifts):+9.2f}%p '
                  f'{wins:>6} {losses:>7} {ties:>5}  {verdict}')


if __name__ == '__main__':
    main()
