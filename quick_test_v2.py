"""v2 메시지 Quick Test — DB + cache에서 데이터 로드 후 텔레그램 발송"""
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# daily_runner 모듈 import
sys.path.insert(0, str(Path(__file__).parent))
from daily_runner import (
    load_config, log, DB_PATH,
    get_part2_candidates, select_portfolio_stocks,
    classify_exit_reasons, get_forward_test_summary,
    run_v2_ai_analysis, compute_factor_ranks,
    create_v2_signal_message, create_v2_watchlist_message,
    send_telegram_long, get_rank_change_tags,
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
    # fwd_pe (DB에 없음)
    df['fwd_pe'] = df.apply(
        lambda r: r['price'] / r['ntm_current'] if r.get('ntm_current') and r['ntm_current'] > 0 else 0,
        axis=1
    )

    # eps_change_90d (DB에 없을 수 있음)
    if 'eps_change_90d' not in df.columns:
        df['eps_change_90d'] = df.apply(
            lambda r: ((r['ntm_current'] - r['ntm_90d']) / abs(r['ntm_90d']) * 100)
            if r.get('ntm_90d') and abs(r.get('ntm_90d', 0)) > 0.01 else 0,
            axis=1
        )

    # trend_lights + trend_desc — seg_chg는 DB에 없으므로 ntm 값에서 계산
    def _calc_seg_chg(curr, prev):
        """세그먼트 변화율 계산 (NTM EPS 기준)"""
        if prev and abs(prev) > 0.01 and curr:
            return (curr - prev) / abs(prev) * 100
        return 0.0

    def _calc_trend(row):
        try:
            seg1 = _calc_seg_chg(row.get('ntm_60d', 0), row.get('ntm_90d', 0))
            seg2 = _calc_seg_chg(row.get('ntm_30d', 0), row.get('ntm_60d', 0))
            seg3 = _calc_seg_chg(row.get('ntm_7d', 0), row.get('ntm_30d', 0))
            seg4 = _calc_seg_chg(row.get('ntm_current', 0), row.get('ntm_7d', 0))
            lights, desc = get_trend_lights(seg1, seg2, seg3, seg4)
            return lights, desc
        except:
            return '', ''

    trends = df.apply(_calc_trend, axis=1)
    df['trend_lights'] = [t[0] for t in trends]
    df['trend_desc'] = [t[1] for t in trends]

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
                # 캐시는 'shortName' (camelCase) 사용
                df.at[idx, 'short_name'] = info.get('shortName', info.get('short_name', ticker))
    else:
        print("WARNING: ticker_info_cache.json 없음 — industry/short_name 비어있을 수 있음")

    # ── part2_rank + weighted_ranks ──
    c.execute('''
        SELECT ticker, composite_rank, part2_rank
        FROM ntm_screening WHERE date = ? AND part2_rank IS NOT NULL
        ORDER BY part2_rank
    ''', (latest_date,))
    part2_rows = c.fetchall()

    today_tickers = [r[0] for r in part2_rows]
    print(f"Top 30 종목: {len(today_tickers)}")

    # weighted_ranks 로드
    # T0, T-1, T-2 composite_rank
    dates_q = c.execute('SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC LIMIT 3').fetchall()
    date_list = [d[0] for d in dates_q]

    weighted_ranks = {}
    for ticker in today_tickers:
        r0, r1, r2 = 50, 50, 50
        for i, d in enumerate(date_list):
            c.execute('SELECT composite_rank FROM ntm_screening WHERE date = ? AND ticker = ?', (d, ticker))
            row = c.fetchone()
            if row and row[0] is not None:
                if i == 0:
                    r0 = int(row[0])
                elif i == 1:
                    r1 = int(row[0])
                elif i == 2:
                    r2 = int(row[0])
        weighted_ranks[ticker] = {'r0': r0, 'r1': r1, 'r2': r2, 'weighted': r0}

    # ── status_map (✅, ⏳, 🆕) ──
    status_map = {}
    for ticker in today_tickers:
        count = 0
        for d in date_list:
            c.execute('SELECT part2_rank FROM ntm_screening WHERE date = ? AND ticker = ? AND part2_rank IS NOT NULL', (d, ticker))
            if c.fetchone():
                count += 1
        if count >= 3:
            status_map[ticker] = '✅'
        elif count == 2:
            status_map[ticker] = '⏳'
        else:
            status_map[ticker] = '🆕'

    # ── exited_tickers (어제는 Top 30이었는데 오늘 빠진 것) ──
    exited_tickers = {}
    if len(date_list) >= 2:
        prev_date = date_list[1]
        c.execute('SELECT ticker, part2_rank FROM ntm_screening WHERE date = ? AND part2_rank IS NOT NULL', (prev_date,))
        prev_top30 = {r[0]: int(r[1]) for r in c.fetchall()}
        for t, rank in prev_top30.items():
            if t not in today_tickers:
                exited_tickers[t] = rank

    # ── earnings_map ──
    earnings_map = {}  # quick test에서는 빈 값

    conn.close()

    return df, latest_date, today_tickers, weighted_ranks, status_map, exited_tickers, earnings_map


def mock_risk_status():
    """간단한 mock risk status — 실제 API 호출 안 함
    get_market_risk_status()와 동일한 구조 반환
    """
    return {
        'hy': {
            'quadrant': 'Q2',
            'quadrant_label': '여름(성장)',
            'quadrant_icon': '☀️',
            'q_days': 35,
            'hy_spread': 2.88,
            'direction': 'stable',
        },
        'vix': {
            'vix_current': 20.2,
            'vix_percentile': 73,
            'regime': '정상',
            'direction': 'stable',
        },
        'concordance': 'both_stable',
        'final_action': '과거 30년 이 구간 연평균 +9.4%',
        'portfolio_mode': 'normal',
    }


def mock_market_lines():
    """간단한 mock market lines"""
    return [
        '🟢 S&P 500 6,013.13 (+0.22%)',
        '🟢 Nasdaq 19,524.01 (+0.07%)',
    ]


def main():
    config = load_config()
    private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')
    if not private_id:
        print("ERROR: telegram_private_id not configured")
        return 1

    print("=" * 50)
    print("v2 Quick Test — DB + cache 기반")
    print("=" * 50)

    # 1. DB에서 데이터 로드
    results_df, latest_date, today_tickers, weighted_ranks, status_map, exited_tickers, earnings_map = load_latest_from_db()
    biz_day = datetime.strptime(latest_date, '%Y-%m-%d')

    # 2. Mock data (API 호출 없이)
    risk_status = mock_risk_status()
    market_lines = mock_market_lines()

    # 3. rank_change_tags + factor_ranks
    rank_change_tags = get_rank_change_tags(today_tickers, weighted_ranks)
    print(f"순위변동 태그: {sum(1 for v in rank_change_tags.values() if v)}개")
    factor_ranks = compute_factor_ranks(results_df, today_tickers)
    print(f"팩터등수: {len(factor_ranks)}종목")

    # 4. 포트폴리오 종목 선정
    selected, portfolio_mode, concordance, final_action = select_portfolio_stocks(
        results_df, status_map, weighted_ranks, earnings_map, risk_status
    )
    print(f"포트폴리오: {len(selected)}종목, mode={portfolio_mode}")

    # 5. 이탈 종목 사유
    exit_reasons = classify_exit_reasons(exited_tickers, results_df)

    # 6. 필터 통과 수
    filter_count = len(get_part2_candidates(results_df)) if not results_df.empty else 0
    print(f"필터 통과: {filter_count}개")

    # 7. 포워드 테스트
    forward_test = None
    try:
        forward_test = get_forward_test_summary(latest_date)
        if forward_test:
            print(f"포워드테스트: {forward_test['n_days']}일째, 누적 {forward_test['cumulative_return']:+.1f}%")
    except Exception as e:
        print(f"포워드테스트 실패: {e}")

    # 8. AI — quick test에서는 mock (API 호출 안 함)
    ai_content = {
        'market_summary': '대법원의 관세 무효 판결에 시장이 안도하며 기술주 중심으로 상승 마감했어요. 다만 트럼프가 별도 법적 근거로 10% 글로벌 관세를 재추진해 불확실성은 남아있어요. NVDA 실적발표(2/26)가 다음 주 최대 변수예요.',
        'narratives': {}
    }
    # 선정된 종목에 대해 mock narrative
    mock_narratives = {
        'SNDK': 'SNDK는 데이터센터와 AI 워크로드 급증에 따른 스토리지 수요 확대에 힘입어 WD에서 분리 후 실적이 크게 성장하고 있어요.',
        'NVDA': 'NVDA는 AI 데이터센터 GPU 수요 폭증과 블랙웰 아키텍처 출시에 힘입어 역대급 매출 성장을 이어가고 있어요.',
        'APH': 'APH는 AI 서버와 데이터센터 인프라 확장에 따라 IT 데이터 통신 전 부문에서 유기적 성장을 달성하고 있어요.',
        'CMC': 'CMC는 북미 건설·인프라 투자 확대와 철강 수요 견조에 힘입어 마진이 개선되며 실적 성장을 이끌고 있어요.',
        'ANET': 'ANET은 클라우드 대기업과 AI 클러스터의 네트워킹 솔루션 수요 증가에 따라 고성장을 지속하고 있어요.',
        'MU': 'MU는 AI 서버용 HBM 수요 급증과 데이터센터 메모리 업그레이드 사이클에 힘입어 매출이 빠르게 회복되고 있어요.',
        'DAR': 'DAR는 지속가능 항공연료(SAF) 시장 확대와 안정적인 원료 조달에 힘입어 실적 개선이 이어지고 있어요.',
    }
    for s in selected:
        t = s['ticker']
        if t in mock_narratives:
            ai_content['narratives'][t] = mock_narratives[t]

    # 9. 메시지 생성
    print("\n--- Signal Message ---")
    msg_signal = create_v2_signal_message(
        selected, risk_status, market_lines, earnings_map,
        exit_reasons, biz_day, ai_content, portfolio_mode,
        concordance, final_action,
        weighted_ranks=weighted_ranks, rank_change_tags=rank_change_tags,
        forward_test=forward_test, filter_count=filter_count,
        factor_ranks=factor_ranks
    )
    if msg_signal:
        print(msg_signal[:500] + '...' if len(msg_signal) > 500 else msg_signal)

    print("\n--- Watchlist Message ---")
    msg_watchlist, msg_exit = create_v2_watchlist_message(
        results_df, status_map, exited_tickers, today_tickers, biz_day,
        weighted_ranks=weighted_ranks, rank_change_tags=rank_change_tags,
        filter_count=filter_count, factor_ranks=factor_ranks
    )
    if msg_watchlist:
        print(msg_watchlist[:500] + '...' if len(msg_watchlist) > 500 else msg_watchlist)
    if msg_exit:
        print("\n--- Exit Message ---")
        print(msg_exit[:300] + '...' if len(msg_exit) > 300 else msg_exit)

    # 10. 텔레그램 발송
    print("\n\n=== 텔레그램 전송 ===")
    if msg_signal:
        send_telegram_long(msg_signal, config, chat_id=private_id)
        print("Signal 전송 완료")
    if msg_exit:
        send_telegram_long(msg_exit, config, chat_id=private_id)
        print("Exit 전송 완료")
    if msg_watchlist:
        send_telegram_long(msg_watchlist, config, chat_id=private_id)
        print("Watchlist 전송 완료")

    print("\nDone!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
