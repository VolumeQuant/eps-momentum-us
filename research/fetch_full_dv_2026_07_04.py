# -*- coding: utf-8 -*-
"""전종목 PIT 거래대금(dollar_volume_30d) 재구축.

문제: production은 순위(top30) 종목만 dv 기록 → 순위밖(SNDK/MU 등)은 dv=None
      → $1B 필터가 초유동성 대형주를 '데이터 없음'으로 영영 차단 (파이프라인 구멍).
해법: yfinance 히스토리(가격×거래량, 확정 과거데이터=PIT-safe)로 전종목·전일자 재구축.
산출: research/dv_full_2026_07_04.parquet  (index=date, columns=ticker, 값=$M,
      의미 = 해당일 '직전' 30거래일 평균 거래대금 — update_dollar_volumes와 동일 semantics)
      research/px_full_2026_07_04.parquet  (raw Close, 참고용)
"""
import sys, os, sqlite3, time
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import yfinance as yf
import daily_runner as dr

conn = sqlite3.connect(dr.DB_PATH)
tickers = [r[0] for r in conn.execute('SELECT DISTINCT ticker FROM ntm_screening ORDER BY ticker')]
conn.close()
print(f'universe: {len(tickers)} tickers')

CHUNK = 120
closes, vols = [], []
failed = []
for i in range(0, len(tickers), CHUNK):
    batch = tickers[i:i + CHUNK]
    for attempt in range(3):
        try:
            data = yf.download(' '.join(batch), start='2025-11-20', end='2026-07-05',
                               auto_adjust=False, progress=False, threads=2, group_by='column')
            if data is None or data.empty:
                raise RuntimeError('empty')
            closes.append(data['Close'])
            vols.append(data['Volume'])
            got = data['Close'].notna().any().sum()
            print(f'chunk {i//CHUNK+1}/{(len(tickers)+CHUNK-1)//CHUNK}: {got}/{len(batch)} ok')
            break
        except Exception as e:
            print(f'chunk {i//CHUNK+1} attempt {attempt+1} fail: {e}')
            time.sleep(20 * (attempt + 1))
    else:
        failed.extend(batch)
    time.sleep(2)

px = pd.concat(closes, axis=1)
vv = pd.concat(vols, axis=1)
px = px.loc[:, ~px.columns.duplicated()]
vv = vv.loc[:, ~vv.columns.duplicated()]
dv = (px * vv) / 1e6                       # $M daily
# PIT: 해당일 '직전' 30거래일 평균 (당일 제외) = production update_dollar_volumes와 동일
dv30 = dv.rolling(30, min_periods=5).mean().shift(1)

out_dir = os.path.dirname(os.path.abspath(__file__))
dv30.to_parquet(os.path.join(out_dir, 'dv_full_2026_07_04.parquet'))
px.to_parquet(os.path.join(out_dir, 'px_full_2026_07_04.parquet'))
ok_cols = px.notna().any().sum()
print(f'saved: dv30 {dv30.shape}, coverage {ok_cols}/{len(tickers)}, failed batches: {len(failed)}')
if failed:
    print('failed tickers:', failed[:50])
