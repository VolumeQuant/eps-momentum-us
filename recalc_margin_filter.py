"""v42.1 구조적 저마진 필터 적용 후 과거 DB 재계산

기존 composite_rank에서 저마진 종목(OM<10% AND GM<30%)을 제거하고
순위를 재번호한 뒤, part2_rank(가중순위)를 재계산한다.

사용법: py -3 recalc_margin_filter.py
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import sqlite3
import pandas as pd

DB_PATH = 'eps_momentum_data.db'
PENALTY = 50


def recalc_all():
    conn = sqlite3.connect(DB_PATH)

    # 마진 데이터가 있는 날짜 찾기
    dates_df = pd.read_sql('''
        SELECT DISTINCT date FROM ntm_screening
        WHERE operating_margin IS NOT NULL AND composite_rank IS NOT NULL
        ORDER BY date
    ''', conn)
    dates = dates_df['date'].tolist()
    print(f"재계산 대상 날짜: {dates}")

    # Step 1: 각 날짜의 composite_rank에서 저마진 종목 제거 + 재번호
    for date in dates:
        print(f"\n=== {date} composite_rank 재계산 ===")
        df = pd.read_sql(f'''
            SELECT ticker, composite_rank, operating_margin, gross_margin
            FROM ntm_screening
            WHERE date='{date}' AND composite_rank IS NOT NULL
            ORDER BY composite_rank
        ''', conn)

        if df.empty:
            continue

        # 저마진 종목 식별
        om = df['operating_margin']
        gm = df['gross_margin']
        low_margin = om.notna() & gm.notna() & (om < 0.10) & (gm < 0.30)
        excluded = df[low_margin]['ticker'].tolist()

        if excluded:
            print(f"  저마진 제외: {', '.join(excluded)}")
        else:
            print(f"  저마진 종목 없음")
            continue

        # 저마진 제외 후 순위 재번호 (1, 2, 3, ...)
        remaining = df[~low_margin].reset_index(drop=True)
        new_ranks = {row['ticker']: i + 1 for i, (_, row) in enumerate(remaining.iterrows())}

        # DB 업데이트: 저마진 종목은 composite_rank = NULL, 나머지는 재번호
        cursor = conn.cursor()
        for t in excluded:
            cursor.execute(
                'UPDATE ntm_screening SET composite_rank=NULL WHERE date=? AND ticker=?',
                (date, t)
            )
        for ticker, new_rank in new_ranks.items():
            cursor.execute(
                'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                (new_rank, date, ticker)
            )
        conn.commit()
        print(f"  {len(df)}개 → {len(remaining)}개 (제외 {len(excluded)}개), 순위 재번호 완료")

    # Step 2: 대상 날짜의 part2_rank 재계산 (순서대로)
    for date in dates:
        print(f"\n=== {date} part2_rank 재계산 (가중순위) ===")
        cursor = conn.cursor()

        # 오늘의 composite_rank
        cursor.execute(
            'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
            (date,)
        )
        composite_ranks = {r[0]: r[1] for r in cursor.fetchall()}
        if not composite_ranks:
            print(f"  composite_rank 없음 — 스킵")
            continue

        # 이전 날짜의 composite_rank
        cursor.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL AND date < ? ORDER BY date DESC LIMIT 2',
            (date,)
        )
        prev_dates = sorted([r[0] for r in cursor.fetchall()])

        rank_by_date = {}
        for d in prev_dates:
            cursor.execute(
                'SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
                (d,)
            )
            rank_by_date[d] = {r[0]: r[1] for r in cursor.fetchall()}

        t1 = prev_dates[-1] if len(prev_dates) >= 1 else None
        t2 = prev_dates[-2] if len(prev_dates) >= 2 else None

        # 가중순위
        weighted = {}
        for ticker, r0 in composite_ranks.items():
            r1 = rank_by_date.get(t1, {}).get(ticker, PENALTY) if t1 else PENALTY
            r2 = rank_by_date.get(t2, {}).get(ticker, PENALTY) if t2 else PENALTY
            weighted[ticker] = r0 * 0.5 + r1 * 0.3 + r2 * 0.2

        sorted_tickers = sorted(weighted.items(), key=lambda x: x[1])
        top30 = sorted_tickers[:30]

        # part2_rank 저장
        cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date,))
        top30_tickers = []
        for rank, (ticker, w) in enumerate(top30, 1):
            cursor.execute(
                'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                (rank, date, ticker)
            )
            top30_tickers.append(ticker)

        conn.commit()
        print(f"  T1={t1}, T2={t2}")
        print(f"  Top 30: {', '.join(top30_tickers[:10])}...")

    # Step 3: 최종 확인
    print("\n\n=== 최종 확인: 날짜별 Top 10 ===")
    for date in dates:
        final = pd.read_sql(f'''
            SELECT ticker, composite_rank, part2_rank
            FROM ntm_screening
            WHERE date='{date}' AND part2_rank IS NOT NULL AND part2_rank <= 10
            ORDER BY part2_rank
        ''', conn)
        tickers = [f"{r['ticker']}(#{int(r['part2_rank'])})" for _, r in final.iterrows()]
        print(f"  {date}: {', '.join(tickers)}")

    # 저마진 종목 확인
    print("\n=== 저마진 종목 확인 ===")
    for t in ['DAR', 'THO', 'ARW', 'AVT']:
        check = pd.read_sql(f'''
            SELECT date, composite_rank, part2_rank
            FROM ntm_screening WHERE ticker='{t}' AND date >= '2026-02-18'
            ORDER BY date
        ''', conn)
        print(f"\n{t}:")
        for _, r in check.iterrows():
            cr = f"#{int(r['composite_rank'])}" if pd.notna(r['composite_rank']) else 'NULL'
            pr = f"#{int(r['part2_rank'])}" if pd.notna(r['part2_rank']) else 'NULL'
            print(f"  {r['date']}: composite={cr}, part2={pr}")

    conn.close()
    print("\n완료!")


if __name__ == '__main__':
    recalc_all()
