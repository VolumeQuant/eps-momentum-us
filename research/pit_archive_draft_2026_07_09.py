"""
PIT(Point-In-Time) 추정치 아카이빙 — 독립 초안 수집기 (2026-07-09)

★ 이 스크립트는 daily_runner.py / production DB(eps_momentum_data.db)를 전혀
   건드리지 않는다. 별도 파일(research/pit_archive/*.parquet)만 쓴다.
   설계 배경·근거는 research/PIT_ARCHIVE_DESIGN_2026_07_09.md 참조.

목적: 우리가 매일 fetch만 하고 버리는 yfinance eps_trend 계열 raw payload
     (_analysis._earnings_trend)를 그대로 보존한다. 이 payload 하나에
     earnings_estimate/revenue_estimate/eps_revisions가 전부 포함돼 있어
     추가 API 호출 없이(비용 0) 아카이빙 가능 — 실측은 main() 리포트 참조.
     recommendations(애널리스트 등급)는 별도 HTTP 호출이 필요해 opt-in.

사용법:
  python research/pit_archive_draft_2026_07_09.py --sample 30
      DB(eps_momentum_data.db)의 최신 날짜 상위 30종목으로 시험 실행 (읽기 전용 조회만, 미변경)
  python research/pit_archive_draft_2026_07_09.py --tickers AAPL,MSFT,NVDA
      지정 종목만 실행
  python research/pit_archive_draft_2026_07_09.py --full
      전종목(DB 최신 날짜 전체) — 운영 편입 전 단독 실행은 비권장, 확인 프롬프트 있음
  --with-recommendations
      애널리스트 등급(strongBuy~strongSell) 추가 수집 — 종목당 HTTP 호출 +1 (비용 증가, opt-in)

출력: research/pit_archive/{date}.parquet (하루 1파일, 종목 = row)
      콘솔에 API 호출수 / 소요시간 / 파일크기 / 실패종목 리포트
"""
import argparse
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
DB_PATH = os.path.join(REPO_ROOT, 'eps_momentum_data.db')
OUT_DIR = os.path.join(HERE, 'pit_archive')

# raw_trend에 실제 나타나는 기간 코드. '+'/'-' 는 컬럼명에 못 쓰므로 접미사로 치환.
PERIOD_SUFFIX = {'0q': '0q', '+1q': 'p1q', '0y': '0y', '+1y': 'p1y',
                 '-1q': 'm1q', '-1y': 'm1y'}

# recommendations(권고 등급) period 코드
REC_PERIOD_SUFFIX = {'0m': '0m', '-1m': 'm1m', '-2m': 'm2m', '-3m': 'm3m'}


def _raw(d, key):
    """dict-of-{'raw':..,'fmt':..} 구조에서 raw 값만 안전 추출."""
    if not isinstance(d, dict):
        return None
    v = d.get(key)
    if isinstance(v, dict):
        return v.get('raw')
    return v


def flatten_earnings_trend(raw_trend):
    """stock._analysis._earnings_trend (list[dict]) → 1행 dict로 평탄화.

    raw_trend 구조 (yfinance 0.2.66 실측, 2026-07-09):
      각 item: {maxAge, period, endDate, growth,
                earningsEstimate{avg,low,high,yearAgoEps,numberOfAnalysts,growth},
                revenueEstimate{avg,low,high,numberOfAnalysts,yearAgoRevenue,growth},
                epsTrend{current,7daysAgo,30daysAgo,60daysAgo,90daysAgo},
                epsRevisions{upLast7days,upLast30days,downLast30days,downLast7Days,downLast90days}}
    """
    row = {}
    if not raw_trend:
        return row
    for item in raw_trend:
        period = item.get('period')
        suf = PERIOD_SUFFIX.get(period)
        if suf is None:
            continue  # 알려지지 않은 신규 period 코드 — 조용히 스킵(스키마 안정성 우선)

        row[f'{suf}_end_date'] = item.get('endDate')
        row[f'{suf}_growth'] = _raw(item.get('growth'), 'raw') if isinstance(item.get('growth'), dict) else None

        ee = item.get('earningsEstimate', {}) or {}
        row[f'{suf}_eps_avg'] = _raw(ee, 'avg')
        row[f'{suf}_eps_low'] = _raw(ee, 'low')
        row[f'{suf}_eps_high'] = _raw(ee, 'high')
        row[f'{suf}_eps_year_ago'] = _raw(ee, 'yearAgoEps')
        row[f'{suf}_eps_n_analysts'] = _raw(ee, 'numberOfAnalysts')
        row[f'{suf}_eps_growth'] = _raw(ee, 'growth')

        re_ = item.get('revenueEstimate', {}) or {}
        row[f'{suf}_rev_avg'] = _raw(re_, 'avg')
        row[f'{suf}_rev_low'] = _raw(re_, 'low')
        row[f'{suf}_rev_high'] = _raw(re_, 'high')
        row[f'{suf}_rev_n_analysts'] = _raw(re_, 'numberOfAnalysts')
        row[f'{suf}_rev_year_ago'] = _raw(re_, 'yearAgoRevenue')
        row[f'{suf}_rev_growth'] = _raw(re_, 'growth')

        et = item.get('epsTrend', {}) or {}
        row[f'{suf}_epstrend_cur'] = _raw(et, 'current')
        row[f'{suf}_epstrend_7d'] = _raw(et, '7daysAgo')
        row[f'{suf}_epstrend_30d'] = _raw(et, '30daysAgo')
        row[f'{suf}_epstrend_60d'] = _raw(et, '60daysAgo')
        row[f'{suf}_epstrend_90d'] = _raw(et, '90daysAgo')

        er = item.get('epsRevisions', {}) or {}
        row[f'{suf}_rev_up7'] = _raw(er, 'upLast7days')
        row[f'{suf}_rev_up30'] = _raw(er, 'upLast30days')
        row[f'{suf}_rev_down7'] = _raw(er, 'downLast7Days')
        row[f'{suf}_rev_down30'] = _raw(er, 'downLast30days')
        row[f'{suf}_rev_down90'] = _raw(er, 'downLast90days')
    return row


def flatten_recommendations(rec_df):
    """stock.recommendations (DataFrame: period x strongBuy/buy/hold/sell/strongSell) → 1행 dict."""
    row = {}
    if rec_df is None or len(rec_df) == 0:
        return row
    for _, r in rec_df.iterrows():
        period = r.get('period')
        suf = REC_PERIOD_SUFFIX.get(period)
        if suf is None:
            continue
        for col in ('strongBuy', 'buy', 'hold', 'sell', 'strongSell'):
            if col in r:
                row[f'rec_{suf}_{col.lower()}'] = r[col]
    return row


def fetch_one(ticker, with_recommendations=False):
    """종목 1개 스냅샷. 반환: (ticker, row_dict, n_api_calls, error_or_None)

    API 호출 수 (실측, 2026-07-09 yfinance 0.2.66):
      - eps_trend 계열 전부(eps_trend/earnings_estimate/revenue_estimate/eps_revisions)는
        전부 stock._analysis._earnings_trend 캐시 하나에서 파생 → HTTP 호출 1회.
      - recommendations는 별도 quoteSummary 모듈(recommendationTrend) → HTTP 호출 +1회.
    """
    n_calls = 0
    row = {'ticker': ticker}
    try:
        stock = yf.Ticker(ticker)
        # eps_trend 프로퍼티를 먼저 건드려 캐시를 채운 뒤 raw dict를 읽는다
        # (calculate_ntm_eps와 동일 패턴 — daily_runner.py의 _prefetch_eps 참고)
        _ = stock.eps_trend
        n_calls += 1
        raw_trend = stock._analysis._earnings_trend
        row.update(flatten_earnings_trend(raw_trend))

        if with_recommendations:
            rec_df = stock.recommendations
            n_calls += 1
            row.update(flatten_recommendations(rec_df))

        return ticker, row, n_calls, None
    except Exception as e:
        return ticker, row, n_calls, str(e)


def get_sample_tickers(n, db_path=DB_PATH):
    """production DB에서 최신 날짜 상위 n종목 조회 (읽기 전용, DB 미변경)."""
    if not os.path.exists(db_path):
        # DB 없는 환경(예: 신규 clone) 대비 폴백 샘플
        return ['AAPL', 'MSFT', 'NVDA', 'AVGO', 'AMZN'][:n]
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    try:
        cur = conn.cursor()
        cur.execute('SELECT MAX(date) FROM ntm_screening')
        latest = cur.fetchone()[0]
        cur.execute(
            'SELECT ticker FROM ntm_screening WHERE date=? ORDER BY rank LIMIT ?',
            (latest, n)
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_full_universe(db_path=DB_PATH):
    if not os.path.exists(db_path):
        raise SystemExit(f'DB 없음: {db_path} — --full은 production DB 필요')
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    try:
        cur = conn.cursor()
        cur.execute('SELECT MAX(date) FROM ntm_screening')
        latest = cur.fetchone()[0]
        cur.execute('SELECT ticker FROM ntm_screening WHERE date=? ORDER BY rank', (latest,))
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def run(tickers, with_recommendations=False, workers=2, batch_size=30, sleep_between=1.5):
    """daily_runner.py의 배치+쓰로틀 패턴을 그대로 따름 (rate-limit 회피, 기존 검증된 리듬)."""
    rows = []
    errors = []
    total_calls = 0
    t0 = time.time()

    for start in range(0, len(tickers), batch_size):
        batch = tickers[start:start + batch_size]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(fetch_one, t, with_recommendations): t for t in batch}
            for fut in as_completed(futures):
                ticker, row, n_calls, err = fut.result()
                total_calls += n_calls
                if err:
                    errors.append((ticker, err))
                else:
                    rows.append(row)
        done = start + len(batch)
        print(f'  진행: {done}/{len(tickers)} (누적 API 호출 {total_calls})')
        if start + batch_size < len(tickers):
            time.sleep(sleep_between)

    elapsed = time.time() - t0
    return rows, errors, total_calls, elapsed


def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
    ap = argparse.ArgumentParser(description='PIT 추정치 아카이빙 초안 수집기 (독립 실행, production 미변경)')
    ap.add_argument('--sample', type=int, help='DB 최신일 상위 N종목 시험 실행')
    ap.add_argument('--tickers', type=str, help='쉼표구분 지정 종목')
    ap.add_argument('--full', action='store_true', help='전종목 실행 (비권장, 확인 프롬프트)')
    ap.add_argument('--with-recommendations', action='store_true', help='애널리스트 등급도 수집(HTTP +1/종목)')
    ap.add_argument('--workers', type=int, default=2)
    ap.add_argument('--batch-size', type=int, default=30)
    ap.add_argument('--sleep', type=float, default=1.5)
    ap.add_argument('--yes', action='store_true', help='--full 확인 프롬프트 생략')
    ap.add_argument('--suffix', type=str, default='', help='출력 파일명 접미사(시험용, 예: _rectest)')
    args = ap.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()]
    elif args.full:
        tickers = get_full_universe()
        if not args.yes:
            resp = input(f'전종목 {len(tickers)}개 실행 — 계속? (y/N): ')
            if resp.strip().lower() != 'y':
                print('취소됨')
                return
    elif args.sample:
        tickers = get_sample_tickers(args.sample)
    else:
        print('--sample N 또는 --tickers A,B,C 또는 --full 중 하나 지정 필요')
        sys.exit(1)

    print(f'대상 {len(tickers)}종목: {tickers[:10]}{"..." if len(tickers) > 10 else ""}')
    print(f'recommendations 포함: {args.with_recommendations}')

    rows, errors, total_calls, elapsed = run(
        tickers, with_recommendations=args.with_recommendations,
        workers=args.workers, batch_size=args.batch_size, sleep_between=args.sleep
    )

    today_str = datetime.now().strftime('%Y-%m-%d')
    df = pd.DataFrame(rows)
    if not df.empty:
        df.insert(0, 'date', today_str)

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, f'{today_str}{args.suffix}.parquet')
    df.to_parquet(out_path, engine='pyarrow', compression='snappy', index=False)
    file_size = os.path.getsize(out_path)

    n_ok = len(rows)
    n_err = len(errors)
    n_cols = df.shape[1] if not df.empty else 0

    print()
    print('=' * 60)
    print('PIT 아카이빙 시험 실행 리포트')
    print('=' * 60)
    print(f'  종목 수 (성공/실패): {n_ok}/{len(tickers)} (실패 {n_err})')
    print(f'  컬럼 수: {n_cols}')
    print(f'  API 호출 총계: {total_calls}회 ({total_calls / max(len(tickers), 1):.2f}회/종목)')
    print(f'  소요 시간: {elapsed:.1f}초 ({elapsed / max(len(tickers), 1):.3f}초/종목)')
    print(f'  출력 파일: {out_path}')
    print(f'  파일 크기: {file_size:,} bytes ({file_size / 1024:.1f} KB)')
    if n_ok:
        print(f'  종목당 평균 바이트(압축후): {file_size / n_ok:.0f} bytes/종목')
        est_full = file_size / n_ok * 1300
        print(f'  전종목(~1300) 추정 파일크기: {est_full / 1024:.0f} KB/일, '
              f'{est_full * 252 / 1024 / 1024:.1f} MB/년(252거래일) 추정')
    if errors:
        print(f'  실패 종목(최대 10개): {errors[:10]}')
    print('=' * 60)


if __name__ == '__main__':
    main()
