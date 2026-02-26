# -*- coding: utf-8 -*-
"""v44 순위 재계산 — 2/23, 2/24, 2/25 composite_rank + part2_rank
원자재 업종 제외 + OP<5% 제외 적용
"""
import sqlite3
import json
import numpy as np
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / 'eps_momentum_data.db'
CACHE_PATH = Path(__file__).parent / 'ticker_info_cache.json'

COMMODITY_INDUSTRIES = {
    '금', '귀금속', '산업금속', '구리', '철강', '알루미늄',
    '농업', '석유가스', '석유종합', '석유정제', '목재',
    'Gold', 'Other Precious Metals & Mining',
    'Other Industrial Metals & Mining', 'Copper', 'Steel', 'Aluminum',
    'Agricultural Inputs', 'Oil & Gas E&P', 'Oil & Gas Integrated',
    'Oil & Gas Refining & Marketing', 'Lumber & Wood Production',
}

PENALTY = 50
TARGET_DATES = ['2026-02-23', '2026-02-24', '2026-02-25']


def load_industry_map():
    """ticker_info_cache.json에서 ticker→industry 매핑"""
    with open(CACHE_PATH, 'r', encoding='utf-8') as f:
        cache = json.load(f)
    return {t: info.get('industry', '') for t, info in cache.items()}


def recalculate_composite_rank(conn, date, industry_map):
    """v44 필터 적용 후 composite_rank 재계산"""
    df = pd.read_sql(
        "SELECT * FROM ntm_screening WHERE date=?", conn, params=(date,)
    )
    if df.empty:
        print(f"  {date}: 데이터 없음, 스킵")
        return 0

    # industry 컬럼 추가
    df['industry'] = df['ticker'].map(industry_map).fillna('')

    # fwd_pe, eps_change_90d 재구성
    df['fwd_pe'] = np.where(
        (df['ntm_current'].notna()) & (df['ntm_current'] > 0),
        df['price'] / df['ntm_current'],
        0
    )
    ntm_90d = df['ntm_90d'].fillna(0)
    df['eps_change_90d'] = np.where(
        ntm_90d.abs() > 0.01,
        (df['ntm_current'] - ntm_90d) / ntm_90d.abs(),
        np.where(df['ntm_current'] > 0, 1.0, 0.0)
    )

    # MA120 우선, NULL이면 MA60 fallback
    if 'ma120' in df.columns:
        ma_col = df['ma120'].where(df['ma120'].notna(), df['ma60'])
    else:
        ma_col = df['ma60']

    # === 기본 필터 ===
    filtered = df[
        (df['adj_score'] > 9) &
        (df['adj_gap'].notna()) &
        (df['fwd_pe'].notna()) & (df['fwd_pe'] > 0) &
        (df['eps_change_90d'] > 0) &
        (df['price'].notna()) & (df['price'] >= 10) &
        (ma_col.notna()) & (df['price'] > ma_col)
    ].copy()

    # === 매출 필터 ===
    has_rev = 'rev_growth' in filtered.columns and filtered['rev_growth'].notna().sum() >= 10
    if has_rev:
        filtered = filtered[filtered['rev_growth'].notna()].copy()
        filtered = filtered[filtered['rev_growth'] >= 0.10].copy()

    # === 애널리스트 필터 ===
    if 'num_analysts' in filtered.columns:
        filtered = filtered[filtered['num_analysts'].fillna(0) >= 3].copy()

    if 'rev_up30' in filtered.columns and 'rev_down30' in filtered.columns:
        up = filtered['rev_up30'].fillna(0)
        dn = filtered['rev_down30'].fillna(0)
        total = up + dn
        down_ratio = dn / total.replace(0, float('nan'))
        filtered = filtered[~(down_ratio > 0.3)].copy()

    # === 구조적 저마진 (OM<10% AND GM<30%) ===
    if 'operating_margin' in filtered.columns and 'gross_margin' in filtered.columns:
        om = filtered['operating_margin']
        gm = filtered['gross_margin']
        filtered = filtered[~(om.notna() & gm.notna() & (om < 0.10) & (gm < 0.30))].copy()

    # === v44: OP<5% 제외 ===
    if 'operating_margin' in filtered.columns:
        om = filtered['operating_margin']
        excluded_op = filtered[om.notna() & (om < 0.05)]
        if len(excluded_op) > 0:
            print(f"  {date} OP<5% 제외: {', '.join(excluded_op['ticker'].tolist())}")
        filtered = filtered[~(om.notna() & (om < 0.05))].copy()

    # === v44: 원자재 제외 ===
    commodity = filtered[filtered['industry'].isin(COMMODITY_INDUSTRIES)]
    if len(commodity) > 0:
        print(f"  {date} 원자재 제외: {', '.join(commodity['ticker'].tolist())}")
    filtered = filtered[~filtered['industry'].isin(COMMODITY_INDUSTRIES)].copy()

    # === composite score 계산 ===
    if has_rev and len(filtered) > 1:
        gap_mean, gap_std = filtered['adj_gap'].mean(), filtered['adj_gap'].std()
        rev_mean, rev_std = filtered['rev_growth'].mean(), filtered['rev_growth'].std()

        if gap_std > 0 and rev_std > 0:
            z_gap = (filtered['adj_gap'] - gap_mean) / gap_std
            z_rev = (filtered['rev_growth'] - rev_mean) / rev_std
            filtered['composite'] = (-z_gap) * 0.7 + z_rev * 0.3
            filtered = filtered.sort_values('composite', ascending=False)
        else:
            filtered = filtered.sort_values('adj_gap', ascending=True)
    else:
        filtered = filtered.sort_values('adj_gap', ascending=True)

    # === DB 업데이트: composite_rank ===
    cursor = conn.cursor()
    cursor.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (date,))

    composite_ranks = {}
    for i, (_, row) in enumerate(filtered.iterrows()):
        rank = i + 1
        ticker = row['ticker']
        composite_ranks[ticker] = rank
        cursor.execute(
            'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
            (rank, date, ticker)
        )
    conn.commit()
    print(f"  {date}: composite_rank {len(composite_ranks)}개 저장")
    return len(composite_ranks)


def recalculate_part2_rank(conn, date):
    """가중순위 기반 part2_rank 재계산"""
    cursor = conn.cursor()

    # 오늘 composite_rank
    today_df = pd.read_sql(
        "SELECT ticker, composite_rank FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",
        conn, params=(date,)
    )
    if today_df.empty:
        print(f"  {date}: composite_rank 없음, part2_rank 스킵")
        return

    composite_ranks = dict(zip(today_df['ticker'], today_df['composite_rank']))

    # 이전 2일 날짜 조회
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

    # 가중순위 계산
    weighted = {}
    for ticker, r0 in composite_ranks.items():
        r1 = rank_by_date.get(t1, {}).get(ticker, PENALTY) if t1 else PENALTY
        r2 = rank_by_date.get(t2, {}).get(ticker, PENALTY) if t2 else PENALTY
        weighted[ticker] = r0 * 0.5 + r1 * 0.3 + r2 * 0.2

    sorted_tickers = sorted(weighted.items(), key=lambda x: x[1])
    top30 = sorted_tickers[:30]

    # DB 업데이트
    cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (date,))
    for rank, (ticker, w) in enumerate(top30, 1):
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
            (rank, date, ticker)
        )
    conn.commit()
    print(f"  {date}: part2_rank Top 30 저장 (t1={t1}, t2={t2})")


def main():
    print("=== v44 순위 재계산 시작 ===")
    industry_map = load_industry_map()
    print(f"Industry cache: {len(industry_map)}개 종목")

    conn = sqlite3.connect(DB_PATH)

    # 1단계: composite_rank 재계산 (순서대로)
    print("\n[1] composite_rank 재계산")
    for date in TARGET_DATES:
        recalculate_composite_rank(conn, date, industry_map)

    # 2단계: part2_rank 재계산 (순서대로 — T-1/T-2 의존)
    print("\n[2] part2_rank 재계산")
    for date in TARGET_DATES:
        recalculate_part2_rank(conn, date)

    # 검증
    print("\n[3] 검증")
    cursor = conn.cursor()
    for date in TARGET_DATES:
        cursor.execute(
            "SELECT COUNT(*) FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL", (date,)
        )
        comp = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL", (date,)
        )
        p2 = cursor.fetchone()[0]

        # 원자재 체크
        for t in ['CF', 'NEM', 'CMC']:
            cursor.execute(
                "SELECT composite_rank, part2_rank FROM ntm_screening WHERE date=? AND ticker=?",
                (date, t)
            )
            row = cursor.fetchone()
            if row and (row[0] is not None or row[1] is not None):
                print(f"  WARNING: {date} {t} still ranked! comp={row[0]} p2={row[1]}")

        # Top 5
        cursor.execute(
            "SELECT part2_rank, ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 5",
            (date,)
        )
        top5 = cursor.fetchall()
        top5_str = ', '.join(f"#{r[0]}{r[1]}" for r in top5)
        print(f"  {date}: comp={comp}, p2={p2}, Top5=[{top5_str}]")

    conn.close()
    print("\n=== 완료 ===")


if __name__ == '__main__':
    main()
