import sqlite3
c = sqlite3.connect(r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db')
c.row_factory = sqlite3.Row
cur = c.cursor()
d = '2026-06-24'
print("=== Top 15 by part2_rank on", d, "===")
cur.execute("""SELECT ticker, part2_rank, composite_rank, score, adj_gap, ntm_current, ntm_90d,
 rev_growth, num_analysts, rev_up30, rev_down30, price, ma120, high30, dollar_volume_30d, vol_ratio,
 operating_margin, gross_margin, market_cap
 FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 15""", (d,))
for r in cur.fetchall():
    print(f"p2={r['part2_rank']:>3} cr={r['composite_rank']} {r['ticker']:6} sc={r['score']:.1f} adj_gap={r['adj_gap']:.3f} "
          f"ntm_cur={r['ntm_current']} ntm90={r['ntm_90d']} revg={r['rev_growth']} "
          f"up/dn={r['rev_up30']}/{r['rev_down30']} na={r['num_analysts']} price={r['price']} ma120={r['ma120']} "
          f"$vol={r['dollar_volume_30d']} volr={r['vol_ratio']} mcap={r['market_cap']}")
