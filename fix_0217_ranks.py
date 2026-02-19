"""2/17, 2/18 part2_rank 재계산 스크립트
매출 10% + 애널리스트 품질 필터 적용, Top 30, 버퍼존 없음
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import sqlite3
import pandas as pd
from daily_runner import (
    get_part2_candidates, fetch_revenue_growth, log, DB_PATH
)
from eps_momentum_system import (
    calculate_ntm_score, calculate_eps_change_90d, get_trend_lights
)
import json
from pathlib import Path

TARGET_DATES = ['2026-02-17', '2026-02-18']

cache_path = Path('ticker_info_cache.json')
ticker_cache = {}
if cache_path.exists():
    with open(cache_path, 'r', encoding='utf-8') as f:
        ticker_cache = json.load(f)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

for TARGET_DATE in TARGET_DATES:
    print(f"\n{'='*50}")
    print(f"=== {TARGET_DATE} part2_rank 재계산 ===")
    print(f"{'='*50}")

    rows = cursor.execute('''
        SELECT ticker, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
               adj_score, adj_gap, price, ma60, is_turnaround,
               rev_up30, rev_down30, num_analysts
        FROM ntm_screening WHERE date=? AND adj_score IS NOT NULL
    ''', (TARGET_DATE,)).fetchall()

    print(f"DB 로드: {len(rows)}개 종목")

    results = []
    for r in rows:
        ticker = r[0]
        ntm = {'current': r[2], '7d': r[3], '30d': r[4], '60d': r[5], '90d': r[6]}
        score_val, seg1, seg2, seg3, seg4, is_turn, adj_score_val, direction = calculate_ntm_score(ntm)
        eps_change_90d = calculate_eps_change_90d(ntm)
        trend_lights, trend_desc = get_trend_lights(seg1, seg2, seg3, seg4)
        cached = ticker_cache.get(ticker, {})
        row_dict = {
            'ticker': ticker,
            'short_name': cached.get('shortName', ticker),
            'industry': cached.get('industry', ''),
            'score': r[1],
            'adj_score': r[7],
            'direction': direction,
            'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
            'ntm_cur': ntm['current'], 'ntm_7d': ntm['7d'],
            'ntm_30d': ntm['30d'], 'ntm_60d': ntm['60d'], 'ntm_90d': ntm['90d'],
            'eps_change_90d': eps_change_90d,
            'trend_lights': trend_lights,
            'trend_desc': trend_desc,
            'price_chg': None, 'price_chg_weighted': None, 'eps_chg_weighted': None,
            'fwd_pe': (r[9] / ntm['current']) if ntm['current'] and ntm['current'] > 0 and r[9] else None,
            'fwd_pe_chg': None,
            'adj_gap': r[8],
            'is_turnaround': r[11],
            'rev_up30': r[12] or 0, 'rev_down30': r[13] or 0, 'num_analysts': r[14] or 0,
            'price': r[9],
            'ma60': r[10],
        }
        if not r[11]:
            results.append(row_dict)

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('adj_score', ascending=False).reset_index(drop=True)
    results_df['rank'] = results_df.index + 1
    print(f"메인 종목: {len(results_df)}개")

    # 매출 성장률 수집
    print("매출 성장률 수집 중...")
    results_df = fetch_revenue_growth(results_df)

    # 새 필터(매출 10% + 애널리스트 품질)로 Top 30
    candidates = get_part2_candidates(results_df, top_n=30)
    print(f"\n필터 적용 후 Top {len(candidates)}:")
    for i, (_, row) in enumerate(candidates.iterrows()):
        rg = row.get('rev_growth', 0) or 0
        up = int(row.get('rev_up30', 0) or 0)
        dn = int(row.get('rev_down30', 0) or 0)
        na = int(row.get('num_analysts', 0) or 0)
        print(f"  {i+1:2d}. {row['ticker']:6s} gap={row['adj_gap']:+7.1f} rev={rg*100:+5.1f}% ↑{up}↓{dn} ({na}명)")

    # part2_rank 저장 (Top 30, 버퍼존 없음)
    cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (TARGET_DATE,))

    saved_count = 0
    for i, (_, row) in enumerate(candidates.iterrows()):
        rank = i + 1
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
            (rank, TARGET_DATE, row['ticker'])
        )
        saved_count += 1

    conn.commit()

    # 검증
    saved = cursor.execute(
        'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
        (TARGET_DATE,)
    ).fetchall()
    print(f"\n저장 완료: {saved_count}개")

conn.close()
print(f"\n모든 날짜 복구 완료!")
