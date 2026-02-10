"""v19 메시지 생성 테스트 (백필 데이터 사용, 텔레그램 미발송)"""
import sys, io, os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / 'eps_momentum_data.db'

from daily_runner import (
    get_part2_candidates, get_3day_status, get_death_list,
    create_part2_message,
)
from eps_momentum_system import (
    calculate_ntm_score, calculate_eps_change_90d, get_trend_lights,
)

if sys.stdout.closed:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def log(msg):
    print(f"[TEST] {msg}")

# DB에서 최신 날짜 데이터 로드
conn = sqlite3.connect(DB_PATH)
latest_date = conn.execute("SELECT MAX(date) FROM ntm_screening").fetchone()[0]
log(f"최신 날짜: {latest_date}")

# 전체 데이터 로드
rows = conn.execute("""
    SELECT ticker, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
           is_turnaround, adj_score, adj_gap, price, ma60, part2_rank
    FROM ntm_screening WHERE date=? AND is_turnaround=0
""", (latest_date,)).fetchall()
log(f"메인 종목: {len(rows)}개")

# 종목 캐시 로드
import json
cache_path = PROJECT_ROOT / 'ticker_info_cache.json'
ticker_cache = {}
if cache_path.exists():
    with open(cache_path, 'r', encoding='utf-8') as f:
        ticker_cache = json.load(f)

# DataFrame 구성 (create_part2_message에 필요한 컬럼 전부)
results = []
for ticker, score, ntm_cur, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turn, adj_score_db, adj_gap_db, price_db, ma60_db, p2rank in rows:
    ntm = {'current': ntm_cur, '7d': ntm_7d, '30d': ntm_30d, '60d': ntm_60d, '90d': ntm_90d}

    # adj_score / direction 재계산
    try:
        _, seg1, seg2, seg3, seg4, _, adj_score, direction = calculate_ntm_score(ntm)
    except:
        continue

    eps_change_90d = calculate_eps_change_90d(ntm)
    trend_lights, trend_desc = get_trend_lights(seg1, seg2, seg3, seg4)

    # 종목 정보
    short_name = ticker
    industry = ''
    if ticker in ticker_cache:
        short_name = ticker_cache[ticker].get('shortName', ticker)
        industry = ticker_cache[ticker].get('industry', '')

    # 90일 주가변화율 (간이)
    price_chg = None

    results.append({
        'ticker': ticker,
        'short_name': short_name,
        'industry': industry,
        'score': score,
        'adj_score': adj_score_db or adj_score,
        'direction': direction,
        'seg1': seg1, 'seg2': seg2, 'seg3': seg3, 'seg4': seg4,
        'eps_change_90d': eps_change_90d,
        'trend_lights': trend_lights,
        'trend_desc': trend_desc,
        'price_chg': price_chg,
        'price_chg_weighted': None,
        'eps_chg_weighted': None,
        'fwd_pe': (price_db / ntm_cur) if (price_db and ntm_cur and ntm_cur > 0) else None,
        'fwd_pe_chg': None,
        'adj_gap': adj_gap_db,
        'price': price_db,
        'ma60': ma60_db,
        'is_turnaround': False,
        'rev_up30': 0,
        'rev_down30': 0,
        'num_analysts': 5,
        'rank': 0,
    })

results_df = pd.DataFrame(results)
results_df = results_df.sort_values('adj_score', ascending=False).reset_index(drop=True)
results_df['rank'] = results_df.index + 1
log(f"DataFrame: {len(results_df)}개 종목")

# Part 2 후보
candidates = get_part2_candidates(results_df, top_n=30)
today_tickers = list(candidates['ticker'])
log(f"Part 2 후보: {len(today_tickers)}개")

# 3일 교집합
status_map = get_3day_status(today_tickers)
verified = sum(1 for v in status_map.values() if v == '✅')
new = sum(1 for v in status_map.values() if v == '🆕')
log(f"✅ {verified}개, 🆕 {new}개")

# Death List
death_list = get_death_list(latest_date, today_tickers, results_df)
log(f"🚨 탈락: {len(death_list)}개")

# =====================
# [1/3] 매수 후보 메시지
# =====================
print("\n" + "=" * 60)
print("[1/3] 매수 후보 메시지 미리보기")
print("=" * 60)
msg = create_part2_message(results_df, status_map=status_map, death_list=death_list)
# HTML 태그 제거해서 콘솔에서 읽기 쉽게
msg_clean = msg.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
print(msg_clean)

print(f"\n(메시지 길이: {len(msg)}자)")

# =====================
# [3/3] 포트폴리오 미리보기 (Gemini 없이 종목 선정만)
# =====================
print("\n" + "=" * 60)
print("[3/3] 포트폴리오 후보 (Gemini 미호출)")
print("=" * 60)

verified_tickers = {t for t, s in status_map.items() if s == '✅'}
port_candidates = candidates[candidates['ticker'].isin(verified_tickers)]
log(f"✅ 종목 중 포트폴리오 후보: {len(port_candidates)}개")

if port_candidates.empty:
    print("→ 검증된 종목 없음 → 관망 권장")
else:
    # 상위 5개
    top5 = port_candidates.head(5)
    gaps = [abs(r['adj_gap']) for _, r in top5.iterrows()]
    total = sum(gaps)
    for i, (_, r) in enumerate(top5.iterrows()):
        raw_w = gaps[i] / total * 100
        weight = round(raw_w / 5) * 5
        name = r.get('short_name', r['ticker'])
        print(f"  {i+1}. {name} ({r['ticker']}) — 비중 ~{weight}%, 괴리 {r['adj_gap']:+.1f}")

conn.close()
print("\n테스트 완료!")
