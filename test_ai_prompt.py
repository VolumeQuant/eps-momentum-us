"""AI 리스크 스캐너 프롬프트 테스트 - DB 데이터 기반"""
import json
import sqlite3
import pandas as pd
from pathlib import Path

# daily_runner import (handles Windows UTF-8 wrapping)
from eps_momentum_system import calculate_ntm_score, calculate_eps_change_90d
from daily_runner import (
    load_config, run_ai_analysis, send_telegram_long, log,
    DB_PATH, PROJECT_ROOT,
)

# 설정 로드
config = load_config()

# 최신 DB 데이터 로드
conn = sqlite3.connect(DB_PATH)
latest_date = conn.execute("SELECT MAX(date) FROM ntm_screening").fetchone()[0]
log(f"DB 최신 날짜: {latest_date}")

rows = conn.execute("""
    SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turnaround
    FROM ntm_screening WHERE date = ? AND is_turnaround = 0
""", (latest_date,)).fetchall()
conn.close()
log(f"메인 종목 로드: {len(rows)}개")

# 종목 정보 캐시 로드
cache_path = PROJECT_ROOT / 'ticker_info_cache.json'
with open(cache_path, 'r', encoding='utf-8') as f:
    ticker_cache = json.load(f)

# results_df 재구성 (DB NTM 데이터 → score/adj_score 재계산)
results = []
for ticker, nc, n7, n30, n60, n90, is_turn in rows:
    ntm = {'current': nc, '7d': n7, '30d': n30, '60d': n60, '90d': n90}
    score, seg1, seg2, seg3, seg4, is_turnaround, adj_score, direction = calculate_ntm_score(ntm)
    eps_change_90d = calculate_eps_change_90d(ntm)

    industry = ticker_cache.get(ticker, {}).get('industry', 'N/A')
    short_name = ticker_cache.get(ticker, {}).get('shortName', ticker)

    # fwd_pe, fwd_pe_chg: AI 프롬프트 테스트용 더미값 (필터 통과용)
    passes_filter = adj_score > 9 and eps_change_90d and eps_change_90d > 0 and nc > 0
    results.append({
        'ticker': ticker,
        'short_name': short_name,
        'industry': industry,
        'adj_score': adj_score,
        'eps_change_90d': eps_change_90d,
        'fwd_pe': 20.0 if nc > 0 else None,
        'fwd_pe_chg': -adj_score if passes_filter else None,  # adj_score 역순으로 정렬 대용
    })

results_df = pd.DataFrame(results)
results_df = results_df.sort_values('adj_score', ascending=False).reset_index(drop=True)

# 필터 결과 확인
filtered = results_df[
    (results_df['adj_score'] > 9) &
    results_df['fwd_pe_chg'].notna() &
    results_df['fwd_pe'].notna() &
    (results_df['fwd_pe'] > 0) &
    (results_df['eps_change_90d'] > 0)
].sort_values('fwd_pe_chg').head(30)

log(f"Part 2 후보: {len(filtered)}종목")
log(f"Top 30: {', '.join(filtered['ticker'].tolist())}")

# 업종 분포 미리보기
sectors = {}
for _, row in filtered.iterrows():
    sectors.setdefault(row['industry'], []).append(row['ticker'])
for s, tickers in sorted(sectors.items(), key=lambda x: -len(x[1])):
    log(f"  {s}: {len(tickers)}개 ({', '.join(tickers)})")

# AI 분석 실행
log("AI 분석 시작...")
msg_ai = run_ai_analysis(None, None, None, config, results_df=results_df)

if msg_ai:
    print("\n" + "=" * 60)
    print("AI 분석 결과:")
    print("=" * 60)
    print(msg_ai)
else:
    log("AI 분석 실패 (None 반환)", "ERROR")
