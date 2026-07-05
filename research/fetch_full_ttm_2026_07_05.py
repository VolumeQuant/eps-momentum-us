# -*- coding: utf-8 -*-
"""전종목 PIT TTM EPS 재구축 (gap 게이트 커버리지 11%→전체, EDA 발견 후속).

gap_sleeve.py 빌더와 동일 산식: quarterly_income_stmt Diluted/Basic EPS 4분기 합,
report_date = 분기말 + 45일(보수적 공시지연, PIT-safe).
산출: research/trailing_eps_ttm_full_2026_07_05.json — ★프로덕션 캐시(data_cache/)와 별도,
     현행/페이퍼 행동변화 0. 반영 여부는 재검증(revalidate_gap_full_ttm) 결과 보고 후 사용자 결정."""
import sys, os, json, time, sqlite3
from datetime import datetime
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import pandas as pd
import yfinance as yf
import daily_runner as dr

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trailing_eps_ttm_full_2026_07_05.json')
LAG = 45

conn = sqlite3.connect(dr.DB_PATH)
tickers = [r[0] for r in conn.execute('SELECT DISTINCT ticker FROM ntm_screening ORDER BY ticker')]
conn.close()
print(f'universe: {len(tickers)}')

cache = {}
if os.path.exists(OUT):
    cache = json.load(open(OUT, encoding='utf-8'))
    print(f'resume: {len(cache)} cached')

ok = fail = 0
for i, tk in enumerate(tickers):
    if tk in cache:
        continue
    for attempt in range(2):
        try:
            qi = yf.Ticker(tk).quarterly_income_stmt
            rec = []
            if qi is not None and not qi.empty:
                row = None
                for k in ('Diluted EPS', 'Basic EPS'):
                    if k in qi.index:
                        row = qi.loc[k]
                        break
                if row is not None:
                    q = row.dropna().sort_index()
                    qe = list(q.items())
                    for j in range(3, len(qe)):
                        ttm = sum(float(qe[j - k][1]) for k in range(4))
                        rdate = (qe[j][0] + pd.Timedelta(days=LAG)).strftime('%Y-%m-%d')
                        rec.append([rdate, ttm])
            cache[tk] = rec
            ok += 1 if rec else 0
            break
        except Exception as e:
            if attempt == 0:
                time.sleep(15)
            else:
                cache[tk] = []
                fail += 1
    if (i + 1) % 50 == 0:
        json.dump(cache, open(OUT, 'w', encoding='utf-8'))
        print(f'{i+1}/{len(tickers)} (유효 {ok}, 실패 {fail})', flush=True)
    time.sleep(0.35)

json.dump(cache, open(OUT, 'w', encoding='utf-8'))
nonempty = sum(1 for v in cache.values() if v)
print(f'DONE: {len(cache)}종목 중 TTM 유효 {nonempty} ({nonempty/len(cache)*100:.0f}%), 실패 {fail}')
