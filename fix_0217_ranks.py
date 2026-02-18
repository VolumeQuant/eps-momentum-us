"""2/17 part2_rank 복구 스크립트
매출 10% 필터 적용된 상태로 part2_rank 재계산
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

TARGET_DATE = '2026-02-17'

# 1. DB에서 2/17 데이터 로드 (run_ntm_collection 데이터 보호 경로와 동일)
print(f"=== {TARGET_DATE} part2_rank 복구 시작 ===")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cache_path = Path('ticker_info_cache.json')
ticker_cache = {}
if cache_path.exists():
    with open(cache_path, 'r', encoding='utf-8') as f:
        ticker_cache = json.load(f)

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

# 2. 매출 성장률 수집
print("매출 성장률 수집 중...")
results_df = fetch_revenue_growth(results_df)

# 3. 새 필터로 후보 확인
candidates = get_part2_candidates(results_df, top_n=35)
print(f"\n매출 10% 필터 적용 후 Top 35:")
for i, (_, row) in enumerate(candidates.iterrows()):
    rg = row.get('rev_growth', 0) or 0
    print(f"  {i+1:2d}. {row['ticker']:6s} adj_gap={row['adj_gap']:+7.1f} rev={rg*100:+5.1f}%")

# 4. part2_rank 저장 (buffer zone 적용)
# 어제 = 2/12 (직전 part2_rank 날짜)
cursor.execute('''
    SELECT ticker FROM ntm_screening
    WHERE date = (SELECT MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL AND date < ?)
    AND part2_rank IS NOT NULL
''', (TARGET_DATE,))
yesterday_tickers = {r[0] for r in cursor.fetchall()}
prev_date = cursor.execute(
    'SELECT MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL AND date < ?',
    (TARGET_DATE,)
).fetchone()[0]
print(f"\n버퍼존 기준 직전 날짜: {prev_date} ({len(yesterday_tickers)}종목)")

# 기존 part2_rank 초기화
cursor.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (TARGET_DATE,))

saved_count = 0
for i, (_, row) in enumerate(candidates.iterrows()):
    rank = i + 1
    ticker = row['ticker']

    if rank <= 20:
        pass  # Top 20: 무조건
    elif rank <= 35:
        if ticker not in yesterday_tickers:
            continue  # 신규는 버퍼존 진입 불가
    else:
        break

    cursor.execute(
        'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
        (rank, TARGET_DATE, ticker)
    )
    saved_count += 1

conn.commit()

# 5. 검증
cursor.execute(
    'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank',
    (TARGET_DATE,)
)
saved = cursor.fetchall()
print(f"\n=== 저장 완료: {saved_count}개 ===")
for t, r in saved:
    print(f"  #{r:2d}: {t}")

conn.close()
print(f"\n{TARGET_DATE} part2_rank 복구 완료!")
