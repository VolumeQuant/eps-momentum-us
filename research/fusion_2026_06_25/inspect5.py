import sqlite3
c = sqlite3.connect(r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db')
c.row_factory = sqlite3.Row
cur = c.cursor()
# CAT detail
r = cur.execute("SELECT date,ticker,adj_gap,part2_rank,composite_rank,price,ma120,high30 FROM ntm_screening WHERE ticker='CAT' AND date IN ('2026-06-23','2026-06-24') ORDER BY date").fetchall()
for x in r:
    print(dict(x))

# count universe on 06-24
print("\nrows on 06-24 total:", cur.execute("SELECT COUNT(*) FROM ntm_screening WHERE date='2026-06-24'").fetchone()[0])
print("part2_rank not null:", cur.execute("SELECT COUNT(*) FROM ntm_screening WHERE date='2026-06-24' AND part2_rank IS NOT NULL").fetchone()[0])
print("composite_rank not null:", cur.execute("SELECT COUNT(*) FROM ntm_screening WHERE date='2026-06-24' AND composite_rank IS NOT NULL").fetchone()[0])

# Which top20 on 06-23 dropped out on 06-24
y = {row[0] for row in cur.execute("SELECT ticker FROM ntm_screening WHERE date='2026-06-23' AND part2_rank<=20").fetchall()}
t = {row[0] for row in cur.execute("SELECT ticker FROM ntm_screening WHERE date='2026-06-24' AND part2_rank<=20").fetchall()}
print("\nDropped out of Top20 (06-23 in, 06-24 out):", sorted(y-t))
print("New into Top20:", sorted(t-y))
