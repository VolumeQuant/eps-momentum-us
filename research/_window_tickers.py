# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
conn = sqlite3.connect(DB); cur = conn.cursor()
dts = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
print('윈도:', dts[0], '~', dts[-1], '| 거래일수:', len(dts))
# 윈도에서 part2_rank<=30 든 적 있는 종목 (매매대상 유니버스)
cur.execute('SELECT DISTINCT ticker FROM ntm_screening WHERE part2_rank IS NOT NULL AND part2_rank<=30')
ts = sorted(r[0] for r in cur.fetchall())
print('part2<=30 경험 종목 수:', len(ts))
import json
json.dump(ts, open(r'C:\dev\claude code\eps-momentum-us\research\_window_tickers.json','w'))
print(ts)
