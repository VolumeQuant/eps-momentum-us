# -*- coding: utf-8 -*-
"""DB 마이그레이션 + 전 기간 backfill: dollar_volume_30d 컬럼 추가

ntm_screening 테이블에 dollar_volume_30d ($M) 컬럼 추가.
각 시점의 직전 30일 평균 거래대금. point-in-time (future leak 제거).

backfill 단계:
1. 등장 모든 종목 (cr Top 30 + composite 안) yfinance history fetch (1년치)
2. 각 종목별 일별 거래대금 = Volume × Close
3. 매 일자 시점의 직전 30일 rolling 평균 ($M)
4. DB UPDATE
"""
import sys, sqlite3, time
from pathlib import Path
from collections import defaultdict
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')

t0 = time.time()
con = sqlite3.connect(DB)
cur = con.cursor()

# Step 1: 컬럼 추가
print('[Step 1] dollar_volume_30d 컬럼 추가')
cols = [r[1] for r in cur.execute('PRAGMA table_info(ntm_screening)').fetchall()]
if 'dollar_volume_30d' not in cols:
    cur.execute('ALTER TABLE ntm_screening ADD COLUMN dollar_volume_30d REAL')
    con.commit()
    print('  ✅ 컬럼 추가 완료')
else:
    print('  ℹ️  이미 존재')

# Step 2: 등장 종목 수집 (cr Top 30 한 번이라도)
print('\n[Step 2] 종목 universe 수집')
all_tickers = set()
for r in cur.execute('SELECT DISTINCT ticker FROM ntm_screening WHERE composite_rank<=30'):
    all_tickers.add(r[0])
print(f'  대상 종목: {len(all_tickers)}개')

# Step 3: yfinance bulk fetch (1년치)
print('\n[Step 3] yfinance history fetch (1년)')
ticker_list = sorted(all_tickers)
batch_size = 50
daily_dv = {}  # daily_dv[ticker][date_str] = dollar_volume_M
for i in range(0, len(ticker_list), batch_size):
    batch = ticker_list[i:i+batch_size]
    try:
        data = yf.download(' '.join(batch), start='2025-08-01', end='2026-06-15',
                          auto_adjust=False, progress=False, threads=True, group_by='ticker')
        for tk in batch:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    df = data[tk] if tk in data.columns.get_level_values(0) else None
                else:
                    df = data
                if df is not None and not df.empty:
                    dv_M = (df['Volume'] * df['Close']) / 1e6
                    daily_dv[tk] = {d.strftime('%Y-%m-%d'): v
                                   for d, v in zip(df.index, dv_M.values) if not pd.isna(v)}
            except Exception:
                pass
    except Exception as e:
        print(f'  batch {i}: 오류 {str(e)[:50]}')
    if (i // batch_size + 1) % 5 == 0:
        print(f'  진행: {min(i+batch_size, len(ticker_list))}/{len(ticker_list)}')
print(f'  데이터 확보: {len(daily_dv)}/{len(all_tickers)}')

# Step 4: 각 일자 × 종목별 직전 30일 평균 계산 + DB UPDATE
print('\n[Step 4] 30일 rolling 평균 산정 + DB 업데이트')
all_dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
print(f'  대상 일자: {len(all_dates)}일')

updates = []
for tk, dv_map in daily_dv.items():
    sorted_dv = sorted(dv_map.items())  # [(date, dv_M), ...]
    for d, _ in sorted_dv:
        # 이 일자가 ntm_screening에 있는지
        if d not in all_dates:
            continue
        # 직전 30영업일 평균 (이 날짜 제외, lookback 30일)
        prior = [v for dd, v in sorted_dv if dd < d][-30:]
        if len(prior) < 5:
            continue
        avg_dv = sum(prior) / len(prior)
        updates.append((avg_dv, d, tk))

print(f'  업데이트 row 수: {len(updates):,}')
cur.executemany('UPDATE ntm_screening SET dollar_volume_30d=? WHERE date=? AND ticker=?', updates)
con.commit()
print(f'  ✅ 업데이트 완료')

# Step 5: 검증
print('\n[Step 5] 검증')
last_d = all_dates[-1]
print(f'  마지막 일자({last_d}) 거래대금 분포:')
for tk in ['NVDA', 'MU', 'SNDK', 'AEIS', 'KEYS', 'HWM']:
    r = cur.execute("SELECT dollar_volume_30d FROM ntm_screening WHERE date=? AND ticker=?",
                   (last_d, tk)).fetchone()
    if r and r[0] is not None:
        print(f'    {tk}: ${r[0]:,.0f}M')

# composite Top 30 안 $1B+ 통과 종목 수
result = cur.execute('''SELECT COUNT(*) FROM ntm_screening WHERE date=? AND composite_rank<=30
                       AND dollar_volume_30d >= 1000''', (last_d,)).fetchone()
print(f'  composite Top 30 + $1B+: {result[0]}개')

print(f'\n총 소요: {time.time()-t0:.0f}초')
con.close()
