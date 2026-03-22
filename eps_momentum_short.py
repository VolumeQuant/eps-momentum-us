#!/usr/bin/env python3
"""
EPS Momentum Short Screening System
====================================
Finds stocks where EPS is deteriorating but price hasn't dropped enough (overvalued).
This is the INVERSE of the long system which finds improving EPS with lagging price.

Short Candidate Criteria:
  - EPS Declining: ntm_current < ntm_90d
  - Score Negative: score < 0 or adj_score < -5
  - Overvalued: adj_gap > 0 (price hasn't dropped as much as EPS declined)
  - Downtrend: price < MA120 (fallback: price < MA60)
  - min_seg very negative: min(seg1..seg4) < -2%
  - Recent decline: recent segments (seg1, seg2) should not be strongly positive
  - Liquid: price >= $10

Ranking: Short Score descending (highest conviction first)
"""

import sqlite3
import json
import os
import sys
from datetime import datetime
from collections import defaultdict

# Fix Windows console encoding for Korean text
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'eps_momentum_data.db')
CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ticker_info_cache.json')

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _safe(val, default=0.0):
    """Return float, treating None/NaN as default."""
    if val is None:
        return default
    try:
        f = float(val)
        return default if f != f else f  # NaN check
    except (ValueError, TypeError):
        return default


def calc_segments(nc, n7, n30, n60, n90):
    """Calculate 4 segment change rates and return (seg1, seg2, seg3, seg4, min_seg).
    seg1 = (current - 7d) / |7d|    (most recent)
    seg2 = (7d - 30d) / |30d|
    seg3 = (30d - 60d) / |60d|
    seg4 = (60d - 90d) / |90d|      (oldest)
    """
    pairs = [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]
    segs = []
    for a, b in pairs:
        a, b = _safe(a), _safe(b)
        if abs(b) > 0.01:
            segs.append((a - b) / abs(b) * 100)
        else:
            segs.append(0.0)
    return segs[0], segs[1], segs[2], segs[3], min(segs)


def calc_recent_seg(seg1, seg2, seg3, seg4):
    """Recency-weighted segment score for shorts.
    Weights: seg1(40%) + seg2(30%) + seg3(20%) + seg4(10%)
    More negative = worse EPS trend = better short.
    Returns a value where negative means EPS is deteriorating recently.
    """
    return seg1 * 0.4 + seg2 * 0.3 + seg3 * 0.2 + seg4 * 0.1


def is_recovering(seg1, seg2):
    """Check if recent segments indicate EPS recovery (bad for shorts).
    Returns True if both seg1 and seg2 are positive (EPS improving).
    """
    return seg1 > 1.0 and seg2 > 1.0


def load_industry_cache():
    """Load ticker -> industry mapping from cache file."""
    if not os.path.exists(CACHE_PATH):
        return {}
    try:
        with open(CACHE_PATH, encoding='utf-8') as f:
            cache = json.load(f)
        return {k: v.get('industry', '?') for k, v in cache.items()}
    except Exception:
        return {}


def load_industry_cache_full():
    """Load ticker -> {shortName, industry} mapping."""
    if not os.path.exists(CACHE_PATH):
        return {}
    try:
        with open(CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


# ──────────────────────────────────────────────────────────────
# Data Loading
# ──────────────────────────────────────────────────────────────

def get_latest_dates(conn, n=3):
    """Get the latest n distinct dates from the DB."""
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC LIMIT ?', (n,))
    return [r[0] for r in cur.fetchall()]


def load_date_data(conn, date_str):
    """Load all screening data for a given date."""
    cur = conn.cursor()
    cur.execute('''
        SELECT ticker, score, adj_score, adj_gap, price, ma60, ma120,
               ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
               rev_growth, num_analysts, operating_margin, gross_margin,
               market_cap, beta
        FROM ntm_screening
        WHERE date = ?
    ''', (date_str,))

    rows = []
    for r in cur.fetchall():
        ticker = r[0]
        nc, n7, n30, n60, n90 = _safe(r[7]), _safe(r[8]), _safe(r[9]), _safe(r[10]), _safe(r[11])
        seg1, seg2, seg3, seg4, min_seg = calc_segments(nc, n7, n30, n60, n90)

        rows.append({
            'ticker': ticker,
            'score': _safe(r[1]),
            'adj_score': _safe(r[2]),
            'adj_gap': _safe(r[3]),
            'price': _safe(r[4]),
            'ma60': _safe(r[5]),
            'ma120': _safe(r[6]),
            'ntm_current': nc,
            'ntm_7d': n7,
            'ntm_30d': n30,
            'ntm_60d': n60,
            'ntm_90d': n90,
            'rev_growth': _safe(r[12]),
            'num_analysts': _safe(r[13], 0),
            'operating_margin': _safe(r[14]),
            'gross_margin': _safe(r[15]),
            'market_cap': _safe(r[16]),
            'beta': _safe(r[17]),
            'seg1': seg1,
            'seg2': seg2,
            'seg3': seg3,
            'seg4': seg4,
            'min_seg': min_seg,
            'recent_seg': calc_recent_seg(seg1, seg2, seg3, seg4),
        })
    return rows


def calc_w_gap(adj_gap_t0, adj_gap_t1, adj_gap_t2):
    """3-day weighted adj_gap: T0*0.5 + T1*0.3 + T2*0.2."""
    return adj_gap_t0 * 0.5 + adj_gap_t1 * 0.3 + adj_gap_t2 * 0.2


# ──────────────────────────────────────────────────────────────
# Short Screening Filters
# ──────────────────────────────────────────────────────────────

def apply_short_filters(row):
    """Apply strict short candidate filters. Returns (pass, reasons_failed)."""
    reasons = []

    # 1. Price >= $10 (liquidity for shorting)
    if row['price'] < 10:
        reasons.append('price<$10')

    # 2. EPS Declining: ntm_current < ntm_90d
    if row['ntm_90d'] > 0.01 and row['ntm_current'] >= row['ntm_90d']:
        reasons.append('EPS not declining')
    if row['ntm_90d'] <= 0.01:
        reasons.append('ntm_90d too small')

    # 3. Score negative: score < 0 or adj_score < -5
    if not (row['score'] < 0 or row['adj_score'] < -5):
        reasons.append('score not negative')

    # 4. Overvalued: adj_gap > 0
    if row['adj_gap'] <= 0:
        reasons.append('adj_gap<=0 (not overvalued)')

    # 5. Downtrend: price < MA120, fallback price < MA60
    ma = row['ma120'] if row['ma120'] > 0 else row['ma60']
    if ma > 0 and row['price'] >= ma:
        reasons.append('price>=MA (uptrend)')

    # 6. min_seg very negative (at least one segment with significant decline)
    if row['min_seg'] >= -2:
        reasons.append(f"min_seg={row['min_seg']:.1f}% (not declining enough)")

    return len(reasons) == 0, reasons


def apply_relaxed_short_filters(row):
    """Relaxed filters: drop MA and min_seg thresholds for broader universe."""
    reasons = []

    if row['price'] < 10:
        reasons.append('price<$10')
    if row['ntm_90d'] > 0.01 and row['ntm_current'] >= row['ntm_90d']:
        reasons.append('EPS not declining')
    if row['ntm_90d'] <= 0.01:
        reasons.append('ntm_90d too small')
    if not (row['score'] < 0 or row['adj_score'] < -5):
        reasons.append('score not negative')
    if row['adj_gap'] <= 0:
        reasons.append('adj_gap<=0')

    return len(reasons) == 0, reasons


def calc_short_score(row):
    """Composite short conviction score (0-100, higher = stronger short case).

    Components:
      - w_gap overvaluation  (0-35 pts): how much price exceeds EPS-justified level
      - EPS decline severity (0-25 pts): magnitude of 90-day NTM EPS drop
      - Recent trend penalty (0-20 pts): recency-weighted segment decline
      - min_seg severity     (0-10 pts): worst single segment (historical depth)
      - MA confirmation      (0-10 pts): distance below MA120/MA60

    Key change from v1: recent segments are weighted more heavily.
    A stock with seg3=-57% but seg1=+3%, seg2=+17% (recovering) gets penalized.
    """
    score = 0.0

    # 1. w_gap overvaluation (0-35 pts): how overvalued vs EPS decline
    gap = max(row.get('w_gap', row['adj_gap']), 0)  # floor at 0
    gap = min(gap, 100)  # cap at 100
    score += gap * 0.35  # max 35

    # 2. EPS decline severity (0-25 pts)
    if abs(row['ntm_90d']) > 0.01:
        eps_decline = (row['ntm_90d'] - row['ntm_current']) / abs(row['ntm_90d']) * 100
        eps_decline = min(max(eps_decline, 0), 100)
        score += eps_decline * 0.25  # max 25

    # 3. Recent trend contribution (0-20 pts)
    # recent_seg is negative when EPS is declining recently = good for shorts
    # Use recency-weighted average: seg1*0.4 + seg2*0.3 + seg3*0.2 + seg4*0.1
    rs = row.get('recent_seg', 0)
    # Flip sign: more negative recent_seg = higher score
    recent_pts = min(max(-rs, 0), 50) * 0.4  # max 20 (at recent_seg = -50)
    score += recent_pts

    # 4. min_seg severity (0-10 pts) -- historical depth, lower weight than before
    ms = -row['min_seg']  # flip sign: more negative min_seg = higher score
    ms = min(max(ms, 0), 100)
    score += ms * 0.1  # max 10

    # 5. MA confirmation (0-10 pts)
    ma = row['ma120'] if row['ma120'] > 0 else row['ma60']
    if ma > 0 and row['price'] < ma:
        below_pct = (ma - row['price']) / ma * 100
        score += min(below_pct, 30) * (10 / 30)  # max 10

    # Penalty: if recent segments are strongly positive (EPS recovering), reduce score
    if is_recovering(row['seg1'], row['seg2']):
        score *= 0.7  # 30% penalty for recovering stocks

    return round(score, 1)


# ──────────────────────────────────────────────────────────────
# Main Analysis
# ──────────────────────────────────────────────────────────────

def run_short_screening():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    # Get last 3 dates for w_gap
    try:
        dates = get_latest_dates(conn, 3)
    except Exception as e:
        print(f"ERROR: Failed to query dates: {e}")
        conn.close()
        return
    if len(dates) < 1:
        print("ERROR: No data found in DB.")
        conn.close()
        return

    print("=" * 90)
    print("  EPS MOMENTUM SHORT SCREENING SYSTEM")
    print("=" * 90)
    print(f"  Data dates: {dates[-1]} ~ {dates[0]}  (w_gap uses {len(dates)} days)")
    print(f"  Run time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)

    # Load data for each date
    data_by_date = {}
    for d in dates:
        data_by_date[d] = {r['ticker']: r for r in load_date_data(conn, d)}

    latest_date = dates[0]
    latest_data = data_by_date[latest_date]

    total = len(latest_data)
    print(f"\n  Universe size (latest): {total} stocks\n")

    if total == 0:
        print("  No data for latest date.")
        conn.close()
        return


    # ── Universe Statistics ──
    eps_declining = sum(1 for r in latest_data.values()
                        if r['ntm_90d'] > 0.01 and r['ntm_current'] < r['ntm_90d'])
    score_neg = sum(1 for r in latest_data.values() if r['score'] < 0)
    adj_gap_pos = sum(1 for r in latest_data.values() if r['adj_gap'] > 0)
    below_ma120 = sum(1 for r in latest_data.values()
                      if (r['ma120'] > 0 and r['price'] < r['ma120']) or
                         (r['ma120'] <= 0 and r['ma60'] > 0 and r['price'] < r['ma60']))
    min_seg_neg = sum(1 for r in latest_data.values() if r['min_seg'] < -2)

    print("  UNIVERSE BREAKDOWN")
    print("  " + "-" * 50)
    print(f"  {'EPS Declining (current < 90d):':<40} {eps_declining:>5} ({eps_declining/total*100:.1f}%)")
    print(f"  {'Score Negative (score < 0):':<40} {score_neg:>5} ({score_neg/total*100:.1f}%)")
    print(f"  {'Overvalued (adj_gap > 0):':<40} {adj_gap_pos:>5} ({adj_gap_pos/total*100:.1f}%)")
    print(f"  {'Below MA120/MA60:':<40} {below_ma120:>5} ({below_ma120/total*100:.1f}%)")
    print(f"  {'min_seg < -2%:':<40} {min_seg_neg:>5} ({min_seg_neg/total*100:.1f}%)")

    # ── Calculate w_gap for all tickers ──
    w_gap_map = {}
    for ticker, row in latest_data.items():
        gaps = [row['adj_gap']]
        for d in dates[1:]:
            if ticker in data_by_date[d]:
                gaps.append(data_by_date[d][ticker]['adj_gap'])
            else:
                gaps.append(row['adj_gap'])  # fallback to latest

        # Pad to 3 if fewer dates available
        while len(gaps) < 3:
            gaps.append(gaps[-1])

        w_gap_map[ticker] = calc_w_gap(gaps[0], gaps[1], gaps[2])

    # ── Attach w_gap and short_score to all rows ──
    for ticker, row in latest_data.items():
        row['w_gap'] = w_gap_map.get(ticker, row['adj_gap'])
        row['short_score'] = calc_short_score(row)

    # ── Apply Strict Short Filters ──
    strict_candidates = []
    for ticker, row in latest_data.items():
        passed, _ = apply_short_filters(row)
        if passed:
            strict_candidates.append(row)

    strict_candidates.sort(key=lambda x: x['short_score'], reverse=True)

    # ── Apply Relaxed Filters (for broader view) ──
    relaxed_candidates = []
    for ticker, row in latest_data.items():
        passed, _ = apply_relaxed_short_filters(row)
        if passed:
            relaxed_candidates.append(row)

    relaxed_candidates.sort(key=lambda x: x['short_score'], reverse=True)

    # ── Load industry cache ──
    ind_cache = load_industry_cache()
    full_cache = load_industry_cache_full()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STRICT SHORT CANDIDATES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'=' * 110}")
    print(f"  TOP SHORT CANDIDATES (STRICT) -- Sorted by Short Score")
    print(f"  Criteria: EPS declining + score<0 + adj_gap>0 + price<MA + min_seg<-2% + price>=$10")
    print(f"  Found: {len(strict_candidates)} stocks")
    print(f"{'=' * 110}")

    if strict_candidates:
        _print_candidates_table(strict_candidates[:20], ind_cache, show_segments=True)
    else:
        print("\n  No stocks passed all strict filters.")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RELAXED SHORT CANDIDATES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Only show if materially different from strict
    strict_tickers = {c['ticker'] for c in strict_candidates}
    relaxed_only = [c for c in relaxed_candidates if c['ticker'] not in strict_tickers]
    if relaxed_only:
        print(f"\n{'=' * 110}")
        print(f"  ADDITIONAL SHORT CANDIDATES (RELAXED) -- Not in strict list")
        print(f"  Criteria: EPS declining + score<0 + adj_gap>0 + price>=$10 (no MA/min_seg requirement)")
        print(f"  Found: {len(relaxed_candidates)} total ({len(relaxed_only)} additional)")
        print(f"{'=' * 110}")

        _print_candidates_table(relaxed_only[:15], ind_cache, show_segments=False)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SECTOR DISTRIBUTION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'=' * 90}")
    print(f"  SECTOR DISTRIBUTION OF SHORT CANDIDATES (All)")
    print(f"{'=' * 90}")

    if relaxed_candidates:
        sector_counts = defaultdict(int)
        gita_tickers = []
        for c in relaxed_candidates:
            industry = ind_cache.get(c['ticker'], '?')
            sector_counts[industry] += 1
            if industry in ('?', '\uae30\ud0c0'):
                gita_tickers.append(c['ticker'])

        sorted_sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)
        print(f"\n  {'Industry':<25} {'Count':>6} {'Bar'}")
        print("  " + "-" * 50)
        for ind, cnt in sorted_sectors[:20]:
            bar = '#' * min(cnt, 40)
            print(f"  {ind:<25} {cnt:>6}  {bar}")

        # Note about unmapped tickers
        gita_count = sector_counts.get('\uae30\ud0c0', 0) + sector_counts.get('?', 0)
        if gita_count > 0:
            print(f"\n  Note: {gita_count} stocks have unmapped industry ('기타'/'?').")
            if gita_tickers:
                print(f"  Tickers: {', '.join(gita_tickers[:15])}"
                      f"{'...' if len(gita_tickers) > 15 else ''}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # FORWARD RETURN ANALYSIS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'=' * 100}")
    print(f"  FORWARD RETURN ANALYSIS")
    print(f"  Checking if short candidates from earlier dates actually declined")
    print(f"{'=' * 100}")

    # Get all dates
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date ASC')
    all_dates = [r[0] for r in cur.fetchall()]

    if len(all_dates) >= 10:
        signal_idx = max(0, len(all_dates) - 11)
        signal_date = all_dates[signal_idx]
        eval_date = all_dates[-1]
        days_held = len(all_dates) - signal_idx - 1

        for label, filter_fn in [("STRICT", apply_short_filters), ("RELAXED", apply_relaxed_short_filters)]:
            returns = _backtest_from_date(conn, all_dates, signal_date, eval_date, filter_fn, top_n=20)

            if returns:
                print(f"\n  [{label}] Signal: {signal_date} -> Eval: {eval_date} (~{days_held} trading days)")
                print(f"  Top {min(20, len(returns))} candidates:\n")

                print(f"  {'#':>3} {'Ticker':<8} {'ShortSc':>7} {'Entry':>8} {'Exit':>8} "
                      f"{'PxRet%':>8} {'ShortP%':>9} {'w_gap':>8} {'min_seg':>8} {'Rcv?':>5}")
                print("  " + "-" * 90)

                for i, r in enumerate(returns[:20], 1):
                    rcv = 'Y' if r.get('recovering', False) else ''
                    print(f"  {i:>3} {r['ticker']:<8} {r['short_score']:>7.1f} "
                          f"{r['entry_price']:>8.2f} {r['exit_price']:>8.2f} "
                          f"{r['price_return']:>+7.1f}% {r['short_return']:>+8.1f}% "
                          f"{r['w_gap']:>8.1f} {r['min_seg']:>7.1f}% {rcv:>5}")

                _print_backtest_summary(returns, label, days_held)
            else:
                print(f"\n  [{label}] No candidates from {signal_date}.")
    else:
        print(f"\n  Only {len(all_dates)} dates available. Need >= 10 for forward analysis.")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # MULTI-PERIOD BACKTEST
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'=' * 100}")
    print(f"  MULTI-PERIOD FORWARD RETURNS (Strict vs Relaxed, Top 10)")
    print(f"{'=' * 100}")

    if len(all_dates) >= 8:
        test_offsets = [5, 10, 15, 20, 25]
        test_offsets = [o for o in test_offsets if o < len(all_dates) - 2]

        for label, filter_fn in [("STRICT", apply_short_filters), ("RELAXED", apply_relaxed_short_filters)]:
            print(f"\n  --- {label} ---")
            print(f"  {'Start Date':<14} {'End Date':<14} {'Days':>5} {'AvgShortRet':>12} {'WinRate':>9} {'N':>4}")
            print("  " + "-" * 65)

            period_rets = []
            for offset in test_offsets:
                sig_idx = len(all_dates) - 1 - offset
                sig_date = all_dates[sig_idx]
                end_date = all_dates[-1]

                results = _backtest_from_date(conn, all_dates, sig_date, end_date, filter_fn, top_n=10)
                rets = [r['short_return'] for r in results]

                if rets:
                    avg_ret = sum(rets) / len(rets)
                    wr = sum(1 for r in rets if r > 0) / len(rets) * 100
                    period_rets.append(avg_ret)
                    print(f"  {sig_date:<14} {end_date:<14} {offset:>5} "
                          f"{avg_ret:>+11.2f}% {wr:>8.1f}% {len(rets):>4}")
                else:
                    print(f"  {sig_date:<14} {end_date:<14} {offset:>5} "
                          f"{'N/A':>12} {'N/A':>9} {'0':>4}")

            if period_rets:
                overall_avg = sum(period_rets) / len(period_rets)
                print(f"  {'':>14} {'':>14} {'AVG':>5} {overall_avg:>+11.2f}%")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # DEEP DIVE: TOP 5 SHORT CANDIDATES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    top_list = strict_candidates if strict_candidates else relaxed_candidates
    if top_list:
        label = "STRICT" if strict_candidates else "RELAXED"
        print(f"\n{'=' * 100}")
        print(f"  DEEP DIVE: TOP 5 SHORT CANDIDATES ({label})")
        print(f"{'=' * 100}")

        for i, c in enumerate(top_list[:5], 1):
            _print_deep_dive(c, i, ind_cache, full_cache, data_by_date, dates)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SHORT SCORE METHODOLOGY
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'=' * 100}")
    print(f"  SHORT SCORE METHODOLOGY")
    print(f"{'=' * 100}")
    print("""
  Short Score = composite conviction score (0-100), higher = stronger short case
  Components:
    - w_gap overvaluation  (0-35 pts): Price overvaluation vs EPS-justified level
    - EPS decline severity (0-25 pts): Magnitude of 90-day NTM EPS drop
    - Recent trend penalty (0-20 pts): Recency-weighted segment decline
                                       (seg1*40% + seg2*30% + seg3*20% + seg4*10%)
    - min_seg severity     (0-10 pts): Worst single segment change rate
    - MA confirmation      (0-10 pts): Distance below MA120/MA60

  Recovery penalty: -30% if both seg1 and seg2 are positive (EPS recovering)
  This catches stocks like BA where min_seg is very negative due to a past drop,
  but recent EPS trend is actually improving -- bad shorts.

  Filter Tiers:
    STRICT:   EPS declining + score<0 + adj_gap>0 + price<MA + min_seg<-2% + price>=$10
    RELAXED:  EPS declining + score<0 + adj_gap>0 + price>=$10
""")

    print(f"{'=' * 100}")
    print(f"  END OF SHORT SCREENING REPORT")
    print(f"{'=' * 100}\n")

    conn.close()


# ──────────────────────────────────────────────────────────────
# Output Helpers
# ──────────────────────────────────────────────────────────────

def _print_candidates_table(candidates, ind_cache, show_segments=True):
    """Print a formatted table of short candidates."""
    if show_segments:
        print(f"\n  {'#':>3} {'Ticker':<7} {'ShortSc':>7} {'Price':>8} {'MA120':>8} {'w_gap':>7} "
              f"{'Score':>7} {'min_seg':>8} {'EPS chg':>8} {'seg1':>6} {'seg2':>6} {'seg3':>6} {'seg4':>6} "
              f"{'Neg':>4} {'Rcv':>4} {'Industry':<12}")
        print("  " + "-" * 122)
    else:
        print(f"\n  {'#':>3} {'Ticker':<7} {'ShortSc':>7} {'Price':>8} {'MA120':>8} {'w_gap':>7} "
              f"{'Score':>7} {'min_seg':>8} {'EPS chg':>8} {'P<MA?':>6} {'Rcv':>4} {'Industry':<12}")
        print("  " + "-" * 105)

    for i, c in enumerate(candidates, 1):
        eps_chg = ((c['ntm_current'] - c['ntm_90d']) / abs(c['ntm_90d']) * 100
                   if abs(c['ntm_90d']) > 0.01 else 0)
        industry = ind_cache.get(c['ticker'], '?')
        neg_segs = sum(1 for s in [c['seg1'], c['seg2'], c['seg3'], c['seg4']] if s < 0)
        rcv = 'Y' if is_recovering(c['seg1'], c['seg2']) else ''

        if show_segments:
            ma_disp = f"{c['ma120']:.1f}" if c['ma120'] > 0 else f"{c['ma60']:.1f}*"
            print(f"  {i:>3} {c['ticker']:<7} {c['short_score']:>7.1f} {c['price']:>8.2f} {ma_disp:>8} "
                  f"{c['w_gap']:>7.1f} "
                  f"{c['score']:>7.1f} {c['min_seg']:>7.1f}% {eps_chg:>7.1f}% "
                  f"{c['seg1']:>5.1f} {c['seg2']:>5.1f} {c['seg3']:>5.1f} {c['seg4']:>5.1f} "
                  f"{neg_segs:>3}/4 {rcv:>4} {industry:<12}")
        else:
            ma_val = c['ma120'] if c['ma120'] > 0 else c['ma60']
            ma_disp = f"{ma_val:.1f}" if ma_val > 0 else "N/A"
            below_ma = 'Y' if (ma_val > 0 and c['price'] < ma_val) else 'N'
            print(f"  {i:>3} {c['ticker']:<7} {c['short_score']:>7.1f} {c['price']:>8.2f} {ma_disp:>8} "
                  f"{c['w_gap']:>7.1f} "
                  f"{c['score']:>7.1f} {c['min_seg']:>7.1f}% {eps_chg:>7.1f}% "
                  f"{below_ma:>6} {rcv:>4} {industry:<12}")


def _print_backtest_summary(returns, label, days_held):
    """Print summary statistics for a backtest run."""
    avg_ret = sum(r['short_return'] for r in returns) / len(returns)
    wr = sum(1 for r in returns if r['short_return'] > 0) / len(returns) * 100
    winners = [r for r in returns if r['short_return'] > 0]
    losers = [r for r in returns if r['short_return'] <= 0]
    avg_w = sum(r['short_return'] for r in winners) / len(winners) if winners else 0
    avg_l = sum(r['short_return'] for r in losers) / len(losers) if losers else 0

    # Check recovering stocks performance
    rcv_rets = [r for r in returns if r.get('recovering', False)]
    non_rcv_rets = [r for r in returns if not r.get('recovering', False)]

    print(f"\n  {label} SUMMARY (N={len(returns)}, {days_held} days)")
    print(f"  " + "-" * 50)
    print(f"  {'Avg Short Return:':<30} {avg_ret:>+.2f}%")
    print(f"  {'Win Rate:':<30} {wr:.1f}% ({len(winners)}/{len(returns)})")
    print(f"  {'Avg Winner:':<30} {avg_w:>+.2f}%")
    print(f"  {'Avg Loser:':<30} {avg_l:>+.2f}%")

    if rcv_rets:
        rcv_avg = sum(r['short_return'] for r in rcv_rets) / len(rcv_rets)
        print(f"  {'Recovering (seg1,seg2 > 0):':<30} {rcv_avg:>+.2f}% (N={len(rcv_rets)})")
    if non_rcv_rets:
        non_rcv_avg = sum(r['short_return'] for r in non_rcv_rets) / len(non_rcv_rets)
        print(f"  {'Not recovering:':<30} {non_rcv_avg:>+.2f}% (N={len(non_rcv_rets)})")
    elif rcv_rets:
        print(f"  {'Not recovering:':<30} N/A (all recovering)")


def _print_deep_dive(c, rank, ind_cache, full_cache, data_by_date, dates):
    """Print detailed analysis for a single short candidate."""
    ticker = c['ticker']
    name = full_cache.get(ticker, {}).get('shortName', 'N/A')
    industry = ind_cache.get(ticker, '?')
    eps_chg = ((c['ntm_current'] - c['ntm_90d']) / abs(c['ntm_90d']) * 100
               if abs(c['ntm_90d']) > 0.01 else 0)
    ma_val = c['ma120'] if c['ma120'] > 0 else c['ma60']
    ma_label = 'MA120' if c['ma120'] > 0 else 'MA60'

    # Count negative segments
    neg_segs = sum(1 for s in [c['seg1'], c['seg2'], c['seg3'], c['seg4']] if s < 0)
    recovering = is_recovering(c['seg1'], c['seg2'])

    print(f"\n  #{rank} {ticker} - {name}  [Short Score: {c['short_score']:.1f}]"
          f"{'  ** RECOVERING **' if recovering else ''}")
    print(f"  " + "-" * 70)
    print(f"    Industry:      {industry}")

    if ma_val > 0:
        ma_pct = (c['price'] - ma_val) / ma_val * 100
        print(f"    Price:         ${c['price']:.2f}  |  {ma_label}: ${ma_val:.2f}  |  "
              f"{'BELOW' if c['price'] < ma_val else 'ABOVE'} MA ({ma_pct:+.1f}%)")
    else:
        print(f"    Price:         ${c['price']:.2f}  |  MA: N/A")

    print(f"    NTM EPS:       Current={c['ntm_current']:.3f}  90d={c['ntm_90d']:.3f}  "
          f"Change={eps_chg:+.1f}%")
    print(f"    Score:         {c['score']:.1f}  |  adj_score: {c['adj_score']:.1f}")
    print(f"    adj_gap:       {c['adj_gap']:+.1f}  |  w_gap: {c['w_gap']:+.1f}")
    print(f"    Segments:      seg1={c['seg1']:+.1f}%  seg2={c['seg2']:+.1f}%  "
          f"seg3={c['seg3']:+.1f}%  seg4={c['seg4']:+.1f}%  ({neg_segs}/4 declining)")
    print(f"    min_seg:       {c['min_seg']:+.1f}%")
    print(f"    recent_seg:    {c.get('recent_seg', 0):+.1f}  "
          f"(recency-weighted: seg1*40% + seg2*30% + seg3*20% + seg4*10%)")

    if c['market_cap'] > 0:
        if c['market_cap'] > 1e9:
            print(f"    Market Cap:    ${c['market_cap']/1e9:.1f}B")
        else:
            print(f"    Market Cap:    ${c['market_cap']/1e6:.0f}M")
    if c['beta'] > 0:
        print(f"    Beta:          {c['beta']:.2f}")

    # Recovery warning
    if recovering:
        print(f"    ** WARNING:    Recent segments (seg1={c['seg1']:+.1f}%, seg2={c['seg2']:+.1f}%) "
              f"are positive.")
        print(f"                   EPS trend may be reversing. "
              f"min_seg={c['min_seg']:+.1f}% reflects past decline, not current direction.")

    # Short thesis summary
    thesis = []
    if eps_chg < -30:
        thesis.append(f"severe EPS decline ({eps_chg:+.0f}%)")
    elif eps_chg < -15:
        thesis.append(f"significant EPS decline ({eps_chg:+.0f}%)")
    else:
        thesis.append(f"EPS declining ({eps_chg:+.0f}%)")

    if c['w_gap'] > 20:
        thesis.append(f"heavily overvalued (w_gap={c['w_gap']:+.0f})")
    elif c['w_gap'] > 10:
        thesis.append(f"overvalued (w_gap={c['w_gap']:+.0f})")
    else:
        thesis.append(f"mildly overvalued (w_gap={c['w_gap']:+.0f})")

    if ma_val > 0 and c['price'] < ma_val:
        thesis.append("confirmed downtrend")

    if recovering:
        thesis.append("CAUTION: EPS recovering")

    print(f"    SHORT THESIS:  {'; '.join(thesis)}")

    # Historical adj_gap trend from multiple dates
    gap_trend = []
    for d in dates:
        if ticker in data_by_date[d]:
            gap_trend.append(f"{d}: {data_by_date[d][ticker]['adj_gap']:+.1f}")
    if gap_trend:
        print(f"    adj_gap trend: {' -> '.join(gap_trend)}")


def _backtest_from_date(conn, all_dates, signal_date, eval_date, filter_fn, top_n=10):
    """Run short candidates from signal_date and evaluate at eval_date.
    Computes w_gap using up to 3 dates preceding signal_date.
    Returns list of dicts with return info, or empty list.
    """
    sig_data = {r['ticker']: r for r in load_date_data(conn, signal_date)}
    end_data = {r['ticker']: r for r in load_date_data(conn, eval_date)}

    # Find dates for w_gap calculation (signal_date + up to 2 prior dates)
    try:
        sig_idx = all_dates.index(signal_date)
    except ValueError:
        sig_idx = 0
    prior_dates = []
    for i in range(sig_idx - 1, max(sig_idx - 3, -1), -1):
        if i >= 0:
            prior_dates.append(all_dates[i])

    # Load prior date data for w_gap
    prior_data = {}
    for d in prior_dates:
        prior_data[d] = {r['ticker']: r for r in load_date_data(conn, d)}

    candidates = []
    for ticker, row in sig_data.items():
        # Calculate w_gap using available dates
        gaps = [row['adj_gap']]
        for d in prior_dates:
            if ticker in prior_data.get(d, {}):
                gaps.append(prior_data[d][ticker]['adj_gap'])
            else:
                gaps.append(row['adj_gap'])
        while len(gaps) < 3:
            gaps.append(gaps[-1])

        row['w_gap'] = calc_w_gap(gaps[0], gaps[1], gaps[2])
        row['short_score'] = calc_short_score(row)
        passed, _ = filter_fn(row)
        if passed:
            candidates.append(row)

    candidates.sort(key=lambda x: x['short_score'], reverse=True)

    results = []
    for c in candidates[:top_n]:
        ticker = c['ticker']
        if ticker in end_data and c['price'] > 0:
            ret = (end_data[ticker]['price'] - c['price']) / c['price'] * 100
            results.append({
                'ticker': ticker,
                'entry_price': c['price'],
                'exit_price': end_data[ticker]['price'],
                'price_return': ret,
                'short_return': -ret,
                'adj_gap': c['adj_gap'],
                'w_gap': c.get('w_gap', c['adj_gap']),
                'short_score': c['short_score'],
                'min_seg': c['min_seg'],
                'recovering': is_recovering(c['seg1'], c['seg2']),
            })
    return results


if __name__ == '__main__':
    run_short_screening()
