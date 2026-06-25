import sqlite3
c = sqlite3.connect(r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db')
c.row_factory = sqlite3.Row
cur = c.cursor()

for t in ['LITE','NVDA','CAT','KEYS']:
    print(f"=== {t} ===")
    cur.execute("""SELECT date, composite_rank, part2_rank, score, adj_gap, price, ma120, high30,
      dollar_volume_30d, ntm_current, ntm_90d, rev_growth, num_analysts, rev_up30, rev_down30, vol_ratio
      FROM ntm_screening WHERE ticker=? AND date>='2026-06-18' ORDER BY date""", (t,))
    for r in cur.fetchall():
        print(f"  {r['date']} cr={r['composite_rank']} p2={r['part2_rank']} sc={r['score']} "
              f"price={r['price']} ma120={r['ma120']} high30={r['high30']} $vol={r['dollar_volume_30d']}")

print("\n=== portfolio_log recent ===")
cur.execute("SELECT * FROM portfolio_log ORDER BY date DESC LIMIT 15")
cols = [d[0] for d in cur.description]
print(cols)
for r in cur.fetchall():
    print(dict(zip(cols, r)))

print("\n=== daily_performance recent ===")
try:
    cur.execute("SELECT * FROM daily_performance ORDER BY date DESC LIMIT 12")
    cols = [d[0] for d in cur.description]
    print(cols)
    for r in cur.fetchall():
        print(tuple(r))
except Exception as e:
    print("err", e)
