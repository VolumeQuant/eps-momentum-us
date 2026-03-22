"""과거 날짜의 DB 데이터로 메시지 생성 + 텔레그램 발송 (재수집 없음)

Usage: python send_historical_messages.py 2026-03-11 2026-03-12
"""
import sys
import json
import sqlite3
import time
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))

from daily_runner import (
    load_config, DB_PATH,
    get_part2_candidates, select_display_top5,
    classify_exit_reasons,
    create_signal_message, create_ai_risk_message, create_watchlist_message,
    send_telegram_long, _build_score_100_map,
)
from eps_momentum_system import get_trend_lights


def load_from_db(target_date):
    """특정 날짜의 DB 데이터 로드 (재수집 없음)"""
    import pandas as pd

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 데이터 로드
    df = pd.read_sql_query(
        'SELECT * FROM ntm_screening WHERE date = ?',
        conn, params=(target_date,)
    )
    if df.empty:
        conn.close()
        return None

    # computed columns
    df['fwd_pe'] = df.apply(
        lambda r: r['price'] / r['ntm_current'] if r.get('ntm_current') and r['ntm_current'] > 0 else 0,
        axis=1
    )
    if 'eps_change_90d' not in df.columns:
        df['eps_change_90d'] = df.apply(
            lambda r: ((r['ntm_current'] - r['ntm_90d']) / abs(r['ntm_90d']) * 100)
            if r.get('ntm_90d') and abs(r.get('ntm_90d', 0)) > 0.01 else 0,
            axis=1
        )

    def _calc_seg_chg(curr, prev):
        if prev and abs(prev) > 0.01 and curr:
            return (curr - prev) / abs(prev) * 100
        return 0.0

    def _calc_trend(row):
        try:
            seg4 = _calc_seg_chg(row.get('ntm_60d', 0), row.get('ntm_90d', 0))
            seg3 = _calc_seg_chg(row.get('ntm_30d', 0), row.get('ntm_60d', 0))
            seg2 = _calc_seg_chg(row.get('ntm_7d', 0), row.get('ntm_30d', 0))
            seg1 = _calc_seg_chg(row.get('ntm_current', 0), row.get('ntm_7d', 0))
            lights, desc = get_trend_lights(seg4, seg3, seg2, seg1)
            return lights, desc, seg1, seg2, seg3, seg4
        except:
            return '', '', 0, 0, 0, 0

    trends = df.apply(_calc_trend, axis=1)
    df['trend_lights'] = [t[0] for t in trends]
    df['trend_desc'] = [t[1] for t in trends]
    df['seg1'] = [t[2] for t in trends]
    df['seg2'] = [t[3] for t in trends]
    df['seg3'] = [t[4] for t in trends]
    df['seg4'] = [t[5] for t in trends]

    # ticker_info_cache
    cache_path = Path(__file__).parent.parent / 'ticker_info_cache.json'
    if cache_path.exists():
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        for idx, row in df.iterrows():
            ticker = row['ticker']
            info = cache.get(ticker, {})
            if not row.get('industry') or pd.isna(row.get('industry', '')):
                df.at[idx, 'industry'] = info.get('industry', '')
            if not row.get('short_name') or pd.isna(row.get('short_name', '')):
                df.at[idx, 'short_name'] = info.get('shortName', info.get('short_name', ticker))

    # part2_rank + weighted_ranks (target_date 기준 3일)
    all_dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE date <= ? ORDER BY date DESC LIMIT 3',
        (target_date,)
    ).fetchall()]

    c.execute('''
        SELECT ticker, composite_rank, part2_rank
        FROM ntm_screening WHERE date = ? AND part2_rank IS NOT NULL
        ORDER BY part2_rank
    ''', (target_date,))
    part2_rows = c.fetchall()
    today_tickers = [r[0] for r in part2_rows]

    weighted_ranks = {}
    for ticker in today_tickers:
        r0, r1, r2 = 50, 50, 50
        for i, d in enumerate(all_dates):
            c.execute('SELECT composite_rank FROM ntm_screening WHERE date = ? AND ticker = ?', (d, ticker))
            row = c.fetchone()
            if row and row[0] is not None:
                if i == 0: r0 = int(row[0])
                elif i == 1: r1 = int(row[0])
                elif i == 2: r2 = int(row[0])
        weighted_ranks[ticker] = {'r0': r0, 'r1': r1, 'r2': r2, 'weighted': r0}

    status_map = {}
    for ticker in today_tickers:
        count = 0
        for d in all_dates:
            c.execute('SELECT part2_rank FROM ntm_screening WHERE date = ? AND ticker = ? AND part2_rank IS NOT NULL', (d, ticker))
            if c.fetchone():
                count += 1
        if count >= 3: status_map[ticker] = '✅'
        elif count == 2: status_map[ticker] = '⏳'
        else: status_map[ticker] = '🆕'

    exited_tickers = {}
    if len(all_dates) >= 2:
        prev_date = all_dates[1]
        c.execute('SELECT ticker, part2_rank FROM ntm_screening WHERE date = ? AND part2_rank IS NOT NULL', (prev_date,))
        prev_top30 = {r[0]: int(r[1]) for r in c.fetchall()}
        for t, rank in prev_top30.items():
            if t not in today_tickers:
                exited_tickers[t] = rank

    conn.close()
    return df, today_tickers, weighted_ranks, status_map, exited_tickers


def build_score_100_map_for_date(target_date):
    """특정 날짜 기준 w_gap 맵"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    all_dates = [r[0] for r in c.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE date <= ? AND adj_gap IS NOT NULL ORDER BY date DESC LIMIT 3',
        (target_date,)
    ).fetchall()]
    all_dates = sorted(all_dates)

    gap_by_date = {}
    for d in all_dates:
        rows = c.execute(
            'SELECT ticker, adj_gap FROM ntm_screening WHERE date=? AND adj_gap IS NOT NULL', (d,)
        ).fetchall()
        gap_by_date[d] = {r[0]: r[1] for r in rows}

    weights = [0.2, 0.3, 0.5]
    if len(all_dates) == 2: weights = [0.4, 0.6]
    elif len(all_dates) == 1: weights = [1.0]

    result = {}
    all_tickers = set()
    for d in all_dates:
        all_tickers.update(gap_by_date[d].keys())

    for tk in all_tickers:
        wg = 0
        for i, d in enumerate(all_dates):
            wg += gap_by_date.get(d, {}).get(tk, 0) * weights[i]
        result[tk] = wg

    conn.close()
    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: python send_historical_messages.py 2026-03-11 2026-03-12")
        return 1

    dates = sys.argv[1:]
    config = load_config()
    private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')
    if not private_id:
        print("ERROR: telegram_private_id not configured")
        return 1

    for target_date in dates:
        print(f"\n{'='*60}")
        print(f"  {target_date} 메시지 생성 (DB 기존 데이터)")
        print(f"{'='*60}")

        result = load_from_db(target_date)
        if result is None:
            print(f"  ERROR: {target_date} 데이터 없음!")
            continue

        results_df, today_tickers, weighted_ranks, status_map, exited_tickers, = result
        biz_day = datetime.strptime(target_date, '%Y-%m-%d')

        score_100_map = build_score_100_map_for_date(target_date)
        earnings_map = {}
        risk_status = {
            'hy': {'quadrant': 'Q2', 'quadrant_label': '', 'quadrant_icon': '',
                   'q_days': 0, 'hy_spread': 0, 'direction': 'stable'},
            'vix': {'vix_current': 0, 'vix_percentile': 0, 'regime': '', 'direction': 'stable'},
            'concordance': 'both_stable',
            'final_action': '',
            'portfolio_mode': 'normal',
        }

        # Signal
        selected = select_display_top5(
            results_df, status_map, weighted_ranks, earnings_map, risk_status,
            score_100_map=score_100_map
        )
        exit_reasons = classify_exit_reasons(exited_tickers, results_df)
        filter_count = len(get_part2_candidates(results_df)) if not results_df.empty else 0

        ai_content = {'market_summary': '', 'narratives': {}}

        print(f"  추천: {len(selected)}종목, 이탈: {len(exit_reasons)}종목, 필터통과: {filter_count}개")
        for s in selected:
            print(f"    {s['ticker']}({s.get('part2_rank', '?')}위)")

        msg_signal = create_signal_message(
            selected, earnings_map, exit_reasons, biz_day, ai_content,
            'normal', '',
            weighted_ranks=weighted_ranks, filter_count=filter_count,
            score_100_map=score_100_map,
        )

        msg_watchlist = create_watchlist_message(
            results_df, status_map, exit_reasons, today_tickers, biz_day,
            weighted_ranks=weighted_ranks, score_100_map=score_100_map
        )

        # 발송 (Signal + Watchlist만, AI Risk는 mock이라 스킵)
        if msg_signal:
            send_telegram_long(msg_signal, config, chat_id=private_id)
            print(f"  Signal 전송 완료")
            time.sleep(1)

        if msg_watchlist:
            send_telegram_long(msg_watchlist, config, chat_id=private_id)
            print(f"  Watchlist 전송 완료")
            time.sleep(1)

        print(f"  {target_date} 완료!")
        time.sleep(2)

    print(f"\n전체 완료!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
