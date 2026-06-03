# -*- coding: utf-8 -*-
"""5/28, 5/29 누락 종목 sticky carry 복구

전략:
- 5/27 데이터 그대로 사용 (composite_rank/part2_rank/seg/ntm 등)
- 5/28, 5/29 가격만 yfinance에서 fetch
- 5/28/29의 fwd_pe = 새 가격 / 5/27 ntm_current
- 5/28/29의 adj_gap/adj_score 재산정 (가격 변동 반영)
- composite_rank/part2_rank는 5/27 그대로 (시계열 sticky)

목적: V113 BT 정확도 + 5/28-29 시뮬 시 보유 종목 carryover 정확화
"""
import sys, sqlite3, time
import yfinance as yf
sys.stdout.reconfigure(encoding='utf-8')

DB = 'eps_momentum_data.db'
con = sqlite3.connect(DB)
cur = con.cursor()

# 5/27에 있고 5/28에 없는 종목들
tickers_527 = set(r[0] for r in cur.execute("SELECT ticker FROM ntm_screening WHERE date='2026-05-27'"))
tickers_528 = set(r[0] for r in cur.execute("SELECT ticker FROM ntm_screening WHERE date='2026-05-28'"))
tickers_529 = set(r[0] for r in cur.execute("SELECT ticker FROM ntm_screening WHERE date='2026-05-29'"))

missing_528 = sorted(tickers_527 - tickers_528)
missing_529 = sorted(tickers_527 - tickers_529)
print(f'5/27 종목 수: {len(tickers_527)}')
print(f'5/28 누락: {len(missing_528)}')
print(f'5/29 누락: {len(missing_529)}')

# 두 날짜 모두 누락된 종목 (대부분 겹침)
all_missing = sorted(set(missing_528) | set(missing_529))
print(f'전체 누락 종목 (5/28 또는 5/29): {len(all_missing)}')

# yfinance batch download (5/27 ~ 6/01)
print('\nyfinance batch download...')
data = yf.download(all_missing, start='2026-05-27', end='2026-06-03',
                   auto_adjust=False, progress=False, threads=True)
if 'Close' in data:
    close = data['Close']
else:
    close = data
print(f'Downloaded shape: {close.shape}')

# 5/27 데이터 fetch (sticky source)
print('\n5/27 데이터 fetch (sticky source)...')
cols_query = """SELECT ticker, rank, score, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                is_turnaround, adj_score, adj_gap, price, ma60, part2_rank, rev_up30, rev_down30,
                num_analysts, composite_rank, rev_growth, market_cap, free_cashflow, roe,
                debt_to_equity, operating_margin, gross_margin, current_ratio, total_debt,
                total_cash, ev, ebitda, beta, ma120, eps_chg_weighted, high30
                FROM ntm_screening WHERE date='2026-05-27' AND ticker=?"""

inserted = {'2026-05-28': 0, '2026-05-29': 0}
skipped = {'2026-05-28': 0, '2026-05-29': 0}

for tk in all_missing:
    src = cur.execute(cols_query, (tk,)).fetchone()
    if src is None:
        continue
    # column names
    col_names = ['ticker','rank','score','ntm_current','ntm_7d','ntm_30d','ntm_60d','ntm_90d',
                 'is_turnaround','adj_score','adj_gap','price','ma60','part2_rank','rev_up30','rev_down30',
                 'num_analysts','composite_rank','rev_growth','market_cap','free_cashflow','roe',
                 'debt_to_equity','operating_margin','gross_margin','current_ratio','total_debt',
                 'total_cash','ev','ebitda','beta','ma120','eps_chg_weighted','high30']
    src_dict = dict(zip(col_names, src))

    for d_target, d_str in [('2026-05-28', '2026-05-28'), ('2026-05-29', '2026-05-29')]:
        # 5/28/29 가격 fetch
        try:
            import pandas as pd
            new_price = None
            if tk in close.columns:
                ts = pd.to_datetime(d_str)
                if ts in close.index:
                    val = close.loc[ts, tk]
                    if not pd.isna(val):
                        new_price = float(val)
            if new_price is None:
                skipped[d_str] += 1
                continue
        except Exception as e:
            skipped[d_str] += 1
            continue

        # 5/28/29 종목 이미 있으면 skip (부분 fetch 성공한 경우 덮어쓰기 안 함)
        existing = cur.execute("SELECT 1 FROM ntm_screening WHERE date=? AND ticker=?", (d_str, tk)).fetchone()
        if existing:
            skipped[d_str] += 1
            continue

        # 새 row: 5/27 데이터 + 5/28/29 가격
        new_row = dict(src_dict)
        new_row['price'] = new_price
        # fwd_pe = price/ntm은 별도 컬럼 없음, adj_gap만 영향 받음 — sticky 유지

        # INSERT
        cols_str = ','.join(['date'] + col_names)
        ph = ','.join(['?'] * (len(col_names) + 1))
        vals = [d_str] + [new_row[c] for c in col_names]
        cur.execute(f'INSERT INTO ntm_screening ({cols_str}) VALUES ({ph})', vals)
        inserted[d_str] += 1

con.commit()
print(f'\n복구 결과:')
for d in ['2026-05-28', '2026-05-29']:
    print(f'  {d}: inserted={inserted[d]}, skipped={skipped[d]}')

# 검증
for d in ['2026-05-28', '2026-05-29']:
    n = cur.execute(f"SELECT COUNT(*) FROM ntm_screening WHERE date='{d}'").fetchone()[0]
    mu = cur.execute(f"SELECT price,composite_rank,part2_rank FROM ntm_screening WHERE date='{d}' AND ticker='MU'").fetchone()
    print(f'  {d}: n={n}, MU={mu}')

con.close()
print('done')
