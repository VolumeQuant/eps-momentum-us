"""v19 백필: 기존 DB 3일치에 adj_score, adj_gap, price, ma60, part2_rank 채우기"""
import sys, io, os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / 'eps_momentum_data.db'

# 임포트
from eps_momentum_system import calculate_ntm_score, calculate_eps_change_90d
from daily_runner import get_part2_candidates, init_ntm_database

if sys.stdout.closed:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def log(msg):
    print(f"[BACKFILL] {msg}")

# DB 초기화 (새 컬럼 추가)
init_ntm_database()

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 기존 날짜 확인
cursor.execute("SELECT DISTINCT date FROM ntm_screening ORDER BY date")
dates = [r[0] for r in cursor.fetchall()]
log(f"DB 날짜: {dates}")

# 전체 티커 목록
cursor.execute("SELECT DISTINCT ticker FROM ntm_screening")
all_tickers = sorted([r[0] for r in cursor.fetchall()])
log(f"총 {len(all_tickers)}개 종목")

# Step 1: 가격 데이터 다운로드
log("가격 데이터 다운로드 중... (6개월)")
import yfinance as yf
hist_all = yf.download(all_tickers, period='6mo', threads=True, progress=False)
log("다운로드 완료")

# Step 2: 각 날짜별 백필
for date_str in dates:
    log(f"\n=== {date_str} 백필 시작 ===")

    # 해당 날짜의 종목 데이터 로드
    cursor.execute("""
        SELECT ticker, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turnaround
        FROM ntm_screening WHERE date=? AND is_turnaround=0
    """, (date_str,))
    rows = cursor.fetchall()
    log(f"  메인 종목: {len(rows)}개")

    eval_date = datetime.strptime(date_str, '%Y-%m-%d')
    results = []
    updated = 0

    for ticker, score, ntm_cur, ntm_7d, ntm_30d, ntm_60d, ntm_90d, is_turn in rows:
        # NTM dict 재구성
        ntm = {'current': ntm_cur, '7d': ntm_7d, '30d': ntm_30d, '60d': ntm_60d, '90d': ntm_90d}

        # adj_score 재계산
        try:
            _, seg1, seg2, seg3, seg4, _, adj_score, direction = calculate_ntm_score(ntm)
        except Exception:
            continue

        eps_change_90d = calculate_eps_change_90d(ntm)

        # 가격 데이터
        current_price = None
        ma60_val = None
        fwd_pe_now = None
        fwd_pe_chg = None
        adj_gap = None

        try:
            hist = hist_all['Close'][ticker].dropna()
            if len(hist) >= 60:
                # 해당 날짜의 가격 찾기
                hist_dt = hist.index.tz_localize(None) if hist.index.tz else hist.index
                idx = (hist_dt - eval_date).map(lambda x: abs(x.days)).argmin()
                p_now = float(hist.iloc[idx])
                current_price = p_now

                # MA60 (해당 날짜 기준)
                if idx >= 59:
                    ma60_val = float(hist.iloc[max(0, idx-59):idx+1].mean())

                # Fwd PE
                nc = ntm_cur
                if nc and nc > 0:
                    fwd_pe_now = p_now / nc

                # adj_gap 간이 계산 (가중평균 PE 변화)
                if fwd_pe_now and nc > 0:
                    weights = {'7d': 0.4, '30d': 0.3, '60d': 0.2, '90d': 0.1}
                    weighted_sum = 0.0
                    total_weight = 0.0

                    for key, w in weights.items():
                        days = {'7d': 7, '30d': 30, '60d': 60, '90d': 90}[key]
                        ntm_val = ntm[key]
                        target = eval_date - timedelta(days=days)
                        pidx = (hist_dt - target).map(lambda x: abs(x.days)).argmin()
                        p_then = float(hist.iloc[pidx])

                        if ntm_val and ntm_val > 0 and p_then > 0:
                            fwd_pe_then = p_then / ntm_val
                            pe_chg = (fwd_pe_now - fwd_pe_then) / fwd_pe_then * 100
                            weighted_sum += w * pe_chg
                            total_weight += w

                    if total_weight > 0:
                        fwd_pe_chg = weighted_sum / total_weight

                if fwd_pe_chg is not None and direction is not None:
                    dir_factor = max(-0.3, min(0.3, direction / 30))
                    adj_gap = fwd_pe_chg * (1 + dir_factor)
        except Exception as e:
            pass

        # DB 업데이트
        cursor.execute("""
            UPDATE ntm_screening SET adj_score=?, adj_gap=?, price=?, ma60=?
            WHERE date=? AND ticker=?
        """, (adj_score, adj_gap, current_price, ma60_val, date_str, ticker))
        updated += 1

        results.append({
            'ticker': ticker, 'adj_score': adj_score, 'adj_gap': adj_gap,
            'fwd_pe': fwd_pe_now, 'eps_change_90d': eps_change_90d,
            'price': current_price, 'ma60': ma60_val,
        })

    conn.commit()
    log(f"  {updated}개 종목 adj_score/adj_gap/price/ma60 업데이트")

    # Part 2 rank 부여
    df = pd.DataFrame(results)
    if not df.empty:
        candidates = get_part2_candidates(df)
        log(f"  Part 2 후보: {len(candidates)}개")

        for i, (_, row) in enumerate(candidates.iterrows()):
            cursor.execute(
                "UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?",
                (i + 1, date_str, row['ticker'])
            )
        conn.commit()

        # Top 10 출력
        for i, (_, row) in enumerate(candidates.head(10).iterrows()):
            log(f"  {i+1}위 {row['ticker']}: adj_gap={row['adj_gap']:.1f}")

log("\n=== 백필 완료 ===")

# 3일 교집합 테스트
log("\n=== 3일 교집합 테스트 ===")
from daily_runner import get_3day_status, get_death_list

# 가장 최근 날짜의 Part 2 티커
latest = dates[-1]
cursor.execute("SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank", (latest,))
latest_tickers = [r[0] for r in cursor.fetchall()]
log(f"최신({latest}) Part 2: {len(latest_tickers)}개")

status = get_3day_status(latest_tickers)
verified = [t for t, s in status.items() if s == '✅']
new = [t for t, s in status.items() if s == '🆕']
log(f"✅ 검증: {len(verified)}개")
log(f"🆕 신규: {len(new)}개")

if verified:
    log(f"✅ Top 10: {verified[:10]}")
if new:
    log(f"🆕 Top 10: {new[:10]}")

# Death List (2번째 날짜 → 3번째 날짜 기준)
if len(dates) >= 2:
    death = get_death_list(latest, latest_tickers, df)
    log(f"🚨 탈락: {len(death)}개")
    for t, reason in death[:10]:
        log(f"  {t}: {reason}")

conn.close()
log("\n완료!")
