"""v19 핵심 로직 단위 테스트"""
import sys
import io
import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import sqlite3
import pandas as pd
from pathlib import Path

# daily_runner 임포트 시 stdout 재설정됨 — 그 후 다시 설정
from daily_runner import (
    DB_PATH, init_ntm_database,
    get_part2_candidates, save_part2_ranks,
    get_3day_status, get_death_list,
    create_part2_message,
)

# 임포트 후 stdout 재확인
if sys.stdout.closed:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

print("=" * 50)
print("v19 단위 테스트")
print("=" * 50)

# 1. DB 스키마 테스트
print("\n[1] DB 스키마 확장 테스트...")
init_ntm_database()
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(ntm_screening)")
cols = {row[1] for row in cursor.fetchall()}
new_cols = {'adj_score', 'adj_gap', 'price', 'ma60', 'part2_rank'}
missing = new_cols - cols
if missing:
    print(f"  FAIL: 누락 컬럼: {missing}")
else:
    print(f"  OK: 새 컬럼 5개 확인 (총 {len(cols)}개)")

# 기존 데이터 확인
cursor.execute("SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC")
dates = [r[0] for r in cursor.fetchall()]
print(f"  DB dates: {dates}")

# 2. 기존 데이터로 Part 2 필터 테스트 (가격/MA60 없는 기존 데이터)
print("\n[2] get_part2_candidates() 테스트...")
if dates:
    latest = dates[0]
    cursor.execute(f"SELECT COUNT(*) FROM ntm_screening WHERE date='{latest}'")
    total = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM ntm_screening WHERE date='{latest}' AND adj_score IS NOT NULL")
    has_adj = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM ntm_screening WHERE date='{latest}' AND price IS NOT NULL")
    has_price = cursor.fetchone()[0]
    print(f"  최신 날짜 {latest}: 총 {total}개, adj_score {has_adj}개, price {has_price}개")

# 더미 DataFrame으로 필터 테스트
print("\n[3] 더미 데이터로 필터 로직 테스트...")
dummy = pd.DataFrame([
    {'ticker': 'AAPL', 'adj_score': 15, 'adj_gap': -5.0, 'fwd_pe': 25, 'eps_change_90d': 10, 'price': 180, 'ma60': 170},
    {'ticker': 'MSFT', 'adj_score': 12, 'adj_gap': -3.0, 'fwd_pe': 30, 'eps_change_90d': 8, 'price': 400, 'ma60': 390},
    {'ticker': 'GOOG', 'adj_score': 20, 'adj_gap': 2.0, 'fwd_pe': 22, 'eps_change_90d': 15, 'price': 170, 'ma60': 160},   # adj_gap > 0 → 제외
    {'ticker': 'AMZN', 'adj_score': 8, 'adj_gap': -8.0, 'fwd_pe': 50, 'eps_change_90d': 20, 'price': 190, 'ma60': 180},    # adj_score < 9 → 제외
    {'ticker': 'META', 'adj_score': 18, 'adj_gap': -6.0, 'fwd_pe': 20, 'eps_change_90d': 12, 'price': 550, 'ma60': 560},    # price < MA60 → 제외
    {'ticker': 'NVDA', 'adj_score': 25, 'adj_gap': -10.0, 'fwd_pe': 40, 'eps_change_90d': 25, 'price': 8, 'ma60': 7},       # price < $10 → 제외
    {'ticker': 'TSLA', 'adj_score': 11, 'adj_gap': -4.0, 'fwd_pe': 60, 'eps_change_90d': 5, 'price': 250, 'ma60': 240},
])

candidates = get_part2_candidates(dummy)
passed = list(candidates['ticker'])
print(f"  통과: {passed}")
expected = ['AAPL', 'TSLA', 'MSFT']  # adj_gap 오름차순: -5, -4, -3
assert passed == expected, f"Expected {expected}, got {passed}"
print(f"  OK: 필터 정상 (GOOG=괴리+, AMZN=점수↓, META=MA60↓, NVDA=$10↓ 제외)")

# 4. 3일 교집합 테스트
print("\n[4] get_3day_status() 테스트...")
status = get_3day_status(['AAPL', 'MSFT', 'TSLA'])
print(f"  status: {status}")
# DB에 part2_rank 없으므로 3일 미만이거나 전부 🆕
for t, s in status.items():
    print(f"  {t}: {s}")

# 5. Death List 테스트
print("\n[5] get_death_list() 테스트...")
death = get_death_list('2099-01-01', ['AAPL', 'MSFT'], dummy)
print(f"  탈락: {death} (기존 part2_rank 없으면 빈 리스트 정상)")

# 6. 메시지 생성 테스트
print("\n[6] create_part2_message() 테스트...")
# 더미에 필요한 컬럼 추가
for col in ['short_name', 'industry', 'trend_lights', 'trend_desc', 'price_chg',
            'eps_chg_weighted', 'price_chg_weighted', 'rev_up30', 'rev_down30', 'rank']:
    if col not in dummy.columns:
        dummy[col] = '' if col in ['short_name', 'industry', 'trend_lights', 'trend_desc'] else 0

dummy['short_name'] = dummy['ticker']
dummy['industry'] = 'Tech'
dummy['trend_lights'] = '☀️🌤️🔥☀️'
dummy['trend_desc'] = '상향 가속'

status_map = {'AAPL': '✅', 'MSFT': '🆕', 'TSLA': '🆕'}
death_list = [('GOOG', '괴리+'), ('META', 'MA60↓')]

msg = create_part2_message(dummy, status_map=status_map, death_list=death_list)
print(f"  메시지 길이: {len(msg)}자")

# 핵심 항목 확인
checks = {
    '[1/3]': '[1/3]' in msg,
    '✅ 마커': '✅' in msg,
    '🆕 마커': '🆕' in msg,
    '🚨 탈락': '🚨' in msg,
    'GOOG 탈락': 'GOOG' in msg,
    'META 탈락': 'META' in msg,
    '👉 [2/3]': '[2/3]' in msg,
}
for name, ok in checks.items():
    print(f"  {'OK' if ok else 'FAIL'}: {name}")

conn.close()

print("\n" + "=" * 50)
print("테스트 완료!")
print("=" * 50)
