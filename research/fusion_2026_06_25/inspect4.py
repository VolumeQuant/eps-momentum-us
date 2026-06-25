import sqlite3
c = sqlite3.connect(r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db')
c.row_factory = sqlite3.Row
cur = c.cursor()
print("=== daily_performance rows ===")
cur.execute("SELECT * FROM daily_performance ORDER BY date DESC LIMIT 15")
cols = [d[0] for d in cur.description]
for r in cur.fetchall():
    print(tuple(r))
print("count:", cur.execute("SELECT COUNT(*) FROM daily_performance").fetchone()[0])

print("\n=== performance_track rows ===")
cur.execute("SELECT * FROM performance_track ORDER BY date DESC LIMIT 15")
cols = [d[0] for d in cur.description]
print(cols)
for r in cur.fetchall():
    print(tuple(r))
print("count:", cur.execute("SELECT COUNT(*) FROM performance_track").fetchone()[0])

print("\n=== ai_analysis recent ===")
cur.execute("SELECT date, ticker, length(narrative) FROM ai_analysis ORDER BY date DESC LIMIT 6")
for r in cur.fetchall():
    print(tuple(r))
