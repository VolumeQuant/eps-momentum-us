"""dd_30 진짜 BT — yfinance 200일 history fetch (cold start 없음)

이전 BT는 DB 73일치만 사용 → 첫 30일 cold start (데이터 부족)
이번 BT는 yfinance 200일 fetch → 모든 BT 일자에 30일 high 정확 보장

검증:
  - dd_30_15, 18, 20, 22, 25, 30 anchor sensitivity
  - cold start 없는 환경에서 진짜 robust한지 확인
"""
import sys
import shutil
import sqlite3
import random
import statistics
import time
from pathlib import Path
from collections import defaultdict
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')
import daily_runner as dr
import bt_dynamic_sweetspot as sw
import bt_verify_blindspots as vb

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = ROOT / 'eps_momentum_data.db'
GRID = ROOT / 'research' / 'ma_filter_dbs'
CACHE = ROOT / 'research' / 'yfinance_200d_cache.pkl'

N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10
ENTRY_TOP = 2
EXIT_TOP = 10


def fetch_yfinance_200d():
    """모든 unique ticker 200일 history fetch (캐시 사용)"""
    if CACHE.exists():
        print(f'  cache hit: {CACHE}')
        return pd.read_pickle(CACHE)

    conn = sqlite3.connect(DB_ORIGINAL); cur = conn.cursor()
    tickers = [r[0] for r in cur.execute('SELECT DISTINCT ticker FROM ntm_screening').fetchall()]
    conn.close()
    print(f'  fetching {len(tickers)} tickers, 200d history...')
    t0 = time.time()

    # batch download
    data = yf.download(' '.join(tickers), period='200d', group_by='ticker',
                       auto_adjust=True, threads=True, progress=False)
    print(f'  fetch done {time.time()-t0:.1f}s')

    # 종목별 Close 시리즈 추출
    closes = {}
    for tk in tickers:
        try:
            s = data[tk]['Close'].dropna()
            if len(s) > 0:
                closes[tk] = s
        except (KeyError, AttributeError):
            pass
    print(f'  {len(closes)} tickers have valid close data')
    pd.to_pickle(closes, CACHE)
    return closes


def compute_high30_real(closes, db_dates):
    """yfinance 200d 데이터로 각 DB 일자에 대해 30일 high 정확 계산"""
    # 모든 series를 tz-naive date index로 통일
    closes_naive = {}
    for tk, s in closes.items():
        s2 = s.copy()
        if s2.index.tz is not None:
            s2.index = s2.index.tz_localize(None)
        s2.index = pd.to_datetime(s2.index).date
        closes_naive[tk] = s2

    high30 = {}
    for d in db_dates:
        d_pd = pd.Timestamp(d).date()
        for tk, s in closes_naive.items():
            # d 이전 30거래일 max
            mask = s.index <= d_pd
            s_filtered = s[mask]
            if len(s_filtered) >= 30:
                high30[(d, tk)] = float(s_filtered.tail(30).max())
            elif len(s_filtered) >= 10:
                high30[(d, tk)] = float(s_filtered.max())
    return high30


def regenerate(db, threshold, exclude_set, high30):
    """dd_30_threshold 필터 적용"""
    conn = sqlite3.connect(db); cur = conn.cursor()
    dates_ = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    cur.execute('UPDATE ntm_screening SET composite_rank=NULL, part2_rank=NULL'); conn.commit()
    for today in dates_:
        rows = cur.execute('SELECT ticker, adj_score, adj_gap, eps_chg_weighted, price, ma60, ma120, ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30, operating_margin, gross_margin, free_cashflow, roe FROM ntm_screening WHERE date=?', (today,)).fetchall()
        if not rows: continue
        eligible = []
        for r in rows:
            (tk, asc, ag, eps_w, px, m60, m120, nc, n90, rg, na, ru, rd, om, gm, fcf, roe) = r
            if tk in exclude_set: continue
            if asc is None or asc <= 9: continue
            if ag is None: continue
            if px is None or px < 10: continue
            if nc is None or nc <= 0: continue
            if eps_w is None or eps_w <= 0: continue
            # current 기본
            if m120 is not None:
                if not (px > m120): continue
            else:
                if not (m60 is not None and px > m60): continue
            # dd_30 필터 (threshold > 0이면 적용, 0이면 baseline)
            if threshold > 0:
                high = high30.get((today, tk))
                if high is not None:
                    dd = (px - high) / high * 100
                    if dd <= -threshold: continue
            if rg is None or rg < 0.10: continue
            if na is None or na < 3: continue
            if ru is None or ru < 3: continue
            total = (ru or 0) + (rd or 0)
            if total > 0 and (rd or 0) / total > 0.3: continue
            if om is not None and gm is not None and om < 0.10 and gm < 0.30: continue
            if om is not None and om < 0.05: continue
            if fcf is not None and roe is not None and fcf < 0 and roe < 0: continue
            eligible.append({'ticker': tk, 'adj_gap': ag, 'rev_up30': ru, 'num_analysts': na,
                            'ntm_current': nc, 'ntm_90d': n90, 'rev_growth': rg, 'price': px})
        def _min_seg(tr):
            r2 = cur.execute('SELECT ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d FROM ntm_screening WHERE date=? AND ticker=?', (today, tr['ticker'])).fetchone()
            if not r2 or any(x is None for x in r2): return 0
            nc, n7, n30, n60, n90 = r2
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b is not None and abs(b) > 0.01: segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            return min(segs)
        eligible = [e for e in eligible if _min_seg(e) >= -2]
        if not eligible: continue
        for e in eligible:
            e['_conv_gap'] = dr._apply_conviction(e['adj_gap'], e['rev_up30'], e['num_analysts'], e['ntm_current'], e['ntm_90d'], rev_growth=e['rev_growth'])
        eligible.sort(key=lambda e: e['_conv_gap'])
        for i, e in enumerate(eligible, 1):
            cur.execute('UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?', (i, today, e['ticker']))
        conn.commit()
        tickers = [e['ticker'] for e in eligible]
        wmap = dr._compute_w_gap_map(cur, today, tickers)
        sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
        top30 = sorted_w[:30]
        for rk, tk in enumerate(top30, 1):
            cur.execute('UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?', (rk, today, tk))
        conn.commit()
    conn.close()


def run_bt(db_path, weight_fn):
    scores = sw.precompute_scores(db_path)
    dates_, data, price_full = sw.load_data(db_path)
    eligible_starts = dates_[:-MIN_HOLD_DAYS]
    seed_starts = []
    for seed_i in range(N_SEEDS):
        random.seed(seed_i)
        seed_starts.append(random.sample(eligible_starts, SAMPLES_PER_SEED))
    rets, mdds, seed_avgs = [], [], []
    for chosen in seed_starts:
        sr = []
        for sd in chosen:
            r = vb.simulate_hybrid(dates_, data, price_full, scores, weight_fn, sd,
                                  max_slots=2, entry=ENTRY_TOP, exit_=EXIT_TOP)
            rets.append(r['total_return'])
            mdds.append(r['max_dd'])
            sr.append(r['total_return'])
        seed_avgs.append(sum(sr)/len(sr))
    avg = sum(rets)/len(rets); med = sorted(rets)[len(rets)//2]
    std = statistics.pstdev(rets); mdd = min(mdds); sharpe = avg/std if std > 0 else 0
    return {'rets': rets, 'mdds': mdds, 'seed_avgs': seed_avgs,
            'avg': avg, 'med': med, 'std': std, 'mdd': mdd, 'sharpe': sharpe}


def main():
    print('=' * 110)
    print('★ dd_30 진짜 BT — yfinance 200d history (cold start 없음)')
    print(f'  entry={ENTRY_TOP}, exit={EXIT_TOP}, slot=2, weight=fixed_90_10')
    print('=' * 110)

    print('\n[step 0] yfinance 200d fetch...')
    closes = fetch_yfinance_200d()

    # DB 일자 추출
    conn = sqlite3.connect(DB_ORIGINAL); cur = conn.cursor()
    db_dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date').fetchall()]
    conn.close()

    print(f'\n[step 1] 30일 high 정확 계산 (cold start 없음 검증)...')
    t0 = time.time()
    high30 = compute_high30_real(closes, db_dates)
    print(f'  done {time.time()-t0:.1f}s (entries: {len(high30)})')

    # cold start 확인
    cold = sum(1 for d in db_dates for tk in closes if (d, tk) not in high30)
    available = sum(1 for d in db_dates for tk in closes if (d, tk) in high30)
    print(f'  cold start: {cold}, available: {available} ({available/(cold+available)*100:.1f}%)')

    fixed_90 = sw.make_weight_fn({'type': 'fixed', 'weights': [0.9, 0.1]})

    # 진짜 dd_30 BT
    thresholds = [0, 15, 18, 20, 22, 25, 30]  # 0 = baseline (current)
    results = {'incl': {}, 'excl': {}}
    for env_name, excl in [('incl', set()), ('excl', {'MU', 'SNDK'})]:
        print(f'\n[{env_name}]')
        for th in thresholds:
            name = 'current' if th == 0 else f'dd_30_{th}'
            db = GRID / f'real_{env_name}_{name}.db'
            t0 = time.time()
            shutil.copy(DB_ORIGINAL, db)
            regenerate(db, th, excl, high30)
            res = run_bt(db, fixed_90)
            results[env_name][name] = res
            marker = ' ★' if th == 0 else '  '
            print(f'{marker}{name:<14} avg={res["avg"]:+6.2f}% mdd={res["mdd"]:+6.2f}% sharpe={res["sharpe"]:+.2f} [{time.time()-t0:.1f}s]')

    # 종합
    print()
    print('=' * 110)
    print('★ 진짜 dd_30 anchor sensitivity (cold start 없음)')
    print('=' * 110)
    base_i = results['incl']['current']['seed_avgs']
    base_e = results['excl']['current']['seed_avgs']
    rows = []
    for th in thresholds:
        name = 'current' if th == 0 else f'dd_30_{th}'
        ri = results['incl'][name]; re = results['excl'][name]
        li = [b-a for a,b in zip(base_i, ri['seed_avgs'])]
        le = [b-a for a,b in zip(base_e, re['seed_avgs'])]
        avgi = sum(li)/len(li); avge = sum(le)/len(le)
        wi = sum(1 for x in li if x > 0.01); we = sum(1 for x in le if x > 0.01)
        avg_both = (avgi + avge) / 2
        rows.append((name, ri['avg'], ri['mdd'], avgi, wi, re['avg'], re['mdd'], avge, we, avg_both))
    rows.sort(key=lambda x: -x[9])
    print(f'  {"variant":<14} {"i_avg":>8} {"i_mdd":>7} {"i_lift":>8} {"i_win":>7} | {"e_avg":>8} {"e_mdd":>7} {"e_lift":>8} {"e_win":>7} | {"avg_lift":>9}')
    print('  ' + '-' * 110)
    for r in rows:
        marker = ' ☆' if r[0] == 'current' else '  '
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
        print('  (없음 — 모든 변형이 한 환경에서 60 미달)')


if __name__ == '__main__':
    main()
