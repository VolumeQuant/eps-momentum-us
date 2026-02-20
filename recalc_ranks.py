"""과거 part2_rank 재계산: 매출 성장률 10% 미만 제외 + composite score
이번엔 eligible 전체 티커에서 rev_growth를 수집"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import sqlite3
import numpy as np
import yfinance as yf

conn = sqlite3.connect('eps_momentum_data.db')
cursor = conn.cursor()

cursor.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')
dates = [r[0] for r in cursor.fetchall()]
print(f"대상 날짜: {dates}")

# eligible 전체 티커 수집 (모든 날짜에서 조건 통과한 종목)
cursor.execute('''
    SELECT DISTINCT ticker FROM ntm_screening
    WHERE adj_score > 9 AND adj_gap IS NOT NULL
      AND price IS NOT NULL AND price >= 10
      AND ma60 IS NOT NULL AND price > ma60
''')
all_tickers = [r[0] for r in cursor.fetchall()]
print(f"전체 eligible 티커: {len(all_tickers)}개")

print("매출 성장률 수집 중...")
rev_map = {}
for i, t in enumerate(all_tickers):
    try:
        info = yf.Ticker(t).info
        rev_map[t] = info.get('revenueGrowth')
    except Exception:
        rev_map[t] = None
    if (i + 1) % 20 == 0:
        print(f"  {i+1}/{len(all_tickers)}")

success = sum(1 for v in rev_map.values() if v is not None)
low = sorted([t for t, v in rev_map.items() if v is not None and v < 0.10])
print(f"수집 완료: {success}/{len(all_tickers)}")
print(f"매출 성장 부족(<10%): {len(low)}개 - {low}")

for date in dates:
    cursor.execute('''
        SELECT ticker, adj_gap
        FROM ntm_screening
        WHERE date = ? AND adj_score > 9 AND adj_gap IS NOT NULL
          AND price IS NOT NULL AND price >= 10
          AND ma60 IS NOT NULL AND price > ma60
    ''', (date,))
    rows = cursor.fetchall()

    # 기존 rank 전부 초기화
    cursor.execute('UPDATE ntm_screening SET part2_rank = NULL WHERE date = ?', (date,))

    valid = []
    no_rev = []
    excluded = []
    for ticker, adj_gap in rows:
        rg = rev_map.get(ticker)
        if rg is not None and rg < 0.10:
            excluded.append(ticker)
            continue
        if rg is not None:
            valid.append((ticker, adj_gap, rg))
        else:
            no_rev.append((ticker, adj_gap))

    if len(valid) >= 10:
        gaps = np.array([v[1] for v in valid])
        revs = np.array([v[2] for v in valid])
        gap_mean, gap_std = gaps.mean(), gaps.std()
        rev_mean, rev_std = revs.mean(), revs.std()

        if gap_std > 0 and rev_std > 0:
            scored = []
            for ticker, adj_gap, rg in valid:
                z_gap = (adj_gap - gap_mean) / gap_std
                z_rev = (rg - rev_mean) / rev_std
                composite = (-z_gap) * 0.7 + z_rev * 0.3
                scored.append((ticker, composite))
            scored.sort(key=lambda x: x[1], reverse=True)
            no_rev.sort(key=lambda x: x[1])
            ranked = [t for t, _ in scored] + [t for t, _ in no_rev]
        else:
            all_items = valid + [(t, g, None) for t, g in no_rev]
            all_items.sort(key=lambda x: x[1])
            ranked = [t for t, _, *_ in all_items]
    else:
        all_items = [(t, g) for t, g, *_ in valid] + no_rev
        all_items.sort(key=lambda x: x[1])
        ranked = [t for t, _ in all_items]

    # composite_rank 저장 (전체 eligible — 가중순위 계산 원본)
    cursor.execute('UPDATE ntm_screening SET composite_rank = NULL WHERE date = ?', (date,))
    for cr, ticker in enumerate(ranked, 1):
        cursor.execute(
            'UPDATE ntm_screening SET composite_rank = ? WHERE date = ? AND ticker = ?',
            (cr, date, ticker)
        )

    # part2_rank 저장 (Top 30만)
    for rank, ticker in enumerate(ranked[:30], 1):
        cursor.execute(
            'UPDATE ntm_screening SET part2_rank = ? WHERE date = ? AND ticker = ?',
            (rank, date, ticker)
        )

    # 검증
    cursor.execute('SELECT COUNT(*) FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL', (date,))
    cnt = cursor.fetchone()[0]
    print(f"{date}: eligible {len(rows)} -> 제외 {len(excluded)} -> ranked {len(ranked)} -> DB {cnt}개")

conn.commit()
conn.close()
print("완료!")
