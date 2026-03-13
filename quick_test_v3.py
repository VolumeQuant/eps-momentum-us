"""v3 메시지 Quick Test — DB + cache에서 데이터 로드 후 텔레그램 발송

v3: Signal + AI Risk + Watchlist (3개 메시지)
"""
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# daily_runner 모듈 import
sys.path.insert(0, str(Path(__file__).parent))
from daily_runner import (
    load_config, log, DB_PATH,
    get_part2_candidates, select_display_top5, select_portfolio_stocks,
    classify_exit_reasons,
    create_signal_message, create_ai_risk_message, create_watchlist_message,
    send_telegram_long, _clean_company_name, _build_score_100_map,
)
from eps_momentum_system import get_trend_lights


def load_latest_from_db():
    """DB에서 최신 날짜 데이터 로드 + computed columns 추가"""
    import pandas as pd

    conn = sqlite3.connect(DB_PATH)

    # 최신 날짜
    c = conn.cursor()
    c.execute('SELECT MAX(date) FROM ntm_screening')
    latest_date = c.fetchone()[0]
    print(f"DB 최신 날짜: {latest_date}")

    # 데이터 로드
    df = pd.read_sql_query(
        'SELECT * FROM ntm_screening WHERE date = ?',
        conn, params=(latest_date,)
    )
    print(f"종목 수: {len(df)}")

    # ── computed columns ──
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

    # trend_lights + trend_desc
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

    # ── ticker_info_cache.json에서 industry, short_name 보강 ──
    cache_path = Path(__file__).parent / 'ticker_info_cache.json'
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

    # ── part2_rank + weighted_ranks ──
    c.execute('''
        SELECT ticker, composite_rank, part2_rank
        FROM ntm_screening WHERE date = ? AND part2_rank IS NOT NULL
        ORDER BY part2_rank
    ''', (latest_date,))
    part2_rows = c.fetchall()
    today_tickers = [r[0] for r in part2_rows]
    print(f"Top 30 종목: {len(today_tickers)}")

    # weighted_ranks
    dates_q = c.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC LIMIT 3').fetchall()
    date_list = [d[0] for d in dates_q]

    weighted_ranks = {}
    for ticker in today_tickers:
        r0, r1, r2 = 50, 50, 50
        for i, d in enumerate(date_list):
            c.execute('SELECT composite_rank FROM ntm_screening WHERE date = ? AND ticker = ?', (d, ticker))
            row = c.fetchone()
            if row and row[0] is not None:
                if i == 0: r0 = int(row[0])
                elif i == 1: r1 = int(row[0])
                elif i == 2: r2 = int(row[0])
        weighted_ranks[ticker] = {'r0': r0, 'r1': r1, 'r2': r2, 'weighted': r0}

    # ── status_map ──
    status_map = {}
    for ticker in today_tickers:
        count = 0
        for d in date_list:
            c.execute('SELECT part2_rank FROM ntm_screening WHERE date = ? AND ticker = ? AND part2_rank IS NOT NULL', (d, ticker))
            if c.fetchone():
                count += 1
        if count >= 3: status_map[ticker] = '✅'
        elif count == 2: status_map[ticker] = '⏳'
        else: status_map[ticker] = '🆕'

    # ── exited_tickers ──
    exited_tickers = {}
    if len(date_list) >= 2:
        prev_date = date_list[1]
        c.execute('SELECT ticker, part2_rank FROM ntm_screening WHERE date = ? AND part2_rank IS NOT NULL', (prev_date,))
        prev_top30 = {r[0]: int(r[1]) for r in c.fetchall()}
        for t, rank in prev_top30.items():
            if t not in today_tickers:
                exited_tickers[t] = rank

    earnings_map = {}
    conn.close()

    return df, latest_date, today_tickers, weighted_ranks, status_map, exited_tickers, earnings_map


def mock_risk_status():
    return {
        'hy': {
            'quadrant': 'Q2', 'quadrant_label': '여름(성장)',
            'quadrant_icon': '☀️', 'q_days': 35,
            'hy_spread': 2.88, 'direction': 'stable',
        },
        'vix': {
            'vix_current': 20.2, 'vix_percentile': 73,
            'regime': '정상', 'direction': 'stable',
        },
        'concordance': 'both_stable',
        'final_action': '과거 30년 이 구간 연평균 +9.4%',
        'portfolio_mode': 'normal',
    }


def mock_market_lines():
    return [
        '🟢 S&P 500 6,013.13 (+0.22%)',
        '🟢 Nasdaq 19,524.01 (+0.07%)',
    ]


def main():
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8', errors='replace')

    config = load_config()
    private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')
    if not private_id:
        print("ERROR: telegram_private_id not configured")
        return 1

    print("=" * 50)
    print("v3 Quick Test — Signal + AI Risk + Watchlist")
    print("=" * 50)

    # 1. DB에서 데이터 로드
    results_df, latest_date, today_tickers, weighted_ranks, status_map, exited_tickers, earnings_map = load_latest_from_db()
    biz_day = datetime.strptime(latest_date, '%Y-%m-%d')

    # 2. Mock data
    risk_status = mock_risk_status()
    market_lines = mock_market_lines()

    # 3. 디스플레이 Top 5 (메시지용)
    concordance = risk_status.get('concordance', 'both_stable')
    final_action = risk_status.get('final_action', '')
    portfolio_mode = risk_status.get('portfolio_mode', 'normal')

    score_100_map = _build_score_100_map()
    selected = select_display_top5(
        results_df, status_map, weighted_ranks, earnings_map, risk_status,
        score_100_map=score_100_map
    )
    print(f"디스플레이 추천: {len(selected)}종목, mode={portfolio_mode}")

    # 4. 이탈 사유 (v3: 태그 통일)
    exit_reasons = classify_exit_reasons(exited_tickers, results_df)
    print(f"이탈: {len(exit_reasons)}종목")
    for t, cur_rank, reason in exit_reasons:
        print(f"  {t}: rank={cur_rank} [{reason}]")

    # 5. 필터 통과 수
    filter_count = len(get_part2_candidates(results_df)) if not results_df.empty else 0
    print(f"필터 통과: {filter_count}개")

    # 6. AI mock
    ai_content = {
        'market_summary': '대법원의 관세 무효 판결에 시장이 안도하며 기술주 중심으로 상승 마감했어요. 다만 트럼프가 별도 법적 근거로 10% 글로벌 관세를 재추진해 불확실성은 남아있어요.',
        'narratives': {}
    }
    mock_narratives = {
        'SNDK': '낸드 가격 상승과 데이터센터 수요로 수익성 급등.',
        'NVDA': 'AI 인프라 투자 확대 수혜. 2/26 실적 발표 주의.',
        'APH': 'AI 서버 커넥터 수요 폭발. 매출 성장 전체 1위.',
        'CMC': '북미 건설 투자 확대와 철강 수요로 마진 개선.',
        'ANET': '클라우드·AI 네트워킹 수요 증가로 고성장 지속.',
        'MU': 'HBM 메모리 수요 급증. 순위 소폭 하락 추세.',
        'DAR': '재생에너지 원료 수요 증가와 저평가 매력.',
        'DY': '5G·광통신 인프라 확장 수요로 실적 성장.',
    }
    for s in selected:
        t = s['ticker']
        if t in mock_narratives:
            ai_content['narratives'][t] = mock_narratives[t]

    # 7. 메시지 생성
    print(f"\n가중 괴리율: {len(score_100_map)}종목")

    print("\n" + "=" * 50)
    print("=== Message 1: Signal ===")
    print("=" * 50)
    msg_signal = create_signal_message(
        selected, earnings_map, exit_reasons, biz_day, ai_content,
        portfolio_mode, final_action,
        weighted_ranks=weighted_ranks, filter_count=filter_count,
        score_100_map=score_100_map,
    )
    if msg_signal:
        # HTML 태그 제거해서 콘솔 출력
        import re
        clean = re.sub(r'<[^>]+>', '', msg_signal)
        print(clean)

    print("\n" + "=" * 50)
    print("=== Message 2: AI Risk ===")
    print("=" * 50)
    msg_ai_risk = create_ai_risk_message(
        config, selected, biz_day, risk_status, market_lines,
        earnings_map, ai_content
    )
    if msg_ai_risk:
        import re
        clean = re.sub(r'<[^>]+>', '', msg_ai_risk)
        print(clean)

    print("\n" + "=" * 50)
    print("=== Message 3: Watchlist ===")
    print("=" * 50)
    msg_watchlist = create_watchlist_message(
        results_df, status_map, exit_reasons, today_tickers, biz_day,
        weighted_ranks=weighted_ranks, score_100_map=score_100_map
    )
    if msg_watchlist:
        import re
        clean = re.sub(r'<[^>]+>', '', msg_watchlist)
        # 처음 1000자만 출력
        print(clean[:1500] + '\n...(truncated)' if len(clean) > 1500 else clean)

    # 8. 텔레그램 발송 여부 확인
    print("\n")
    answer = input("텔레그램으로 전송할까요? (y/n): ").strip().lower()
    if answer == 'y':
        if msg_signal:
            send_telegram_long(msg_signal, config, chat_id=private_id)
            print("Signal 전송 완료")
        if msg_ai_risk:
            send_telegram_long(msg_ai_risk, config, chat_id=private_id)
            print("AI Risk 전송 완료")
        if msg_watchlist:
            send_telegram_long(msg_watchlist, config, chat_id=private_id)
            print("Watchlist 전송 완료")
    else:
        print("전송 건너뜀")

    print("\nDone!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
