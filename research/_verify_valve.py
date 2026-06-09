# -*- coding: utf-8 -*-
import sys, os
sys.path.insert(0, r'C:\dev\claude code\eps-momentum-us')
import sqlite3
import daily_runner as dr

out = []
held = dr._replay_holdings()
out.append('최신 보유 (밸브 적용 후): ' + str(sorted(held)))

detail = dr._replay_holdings(return_detail=True)
out.append('\n진입가 detail:')
for tk,(ed,ep) in sorted(detail.items()):
    out.append(f'  {tk}: 진입 {ed} @ {ep}')

con=sqlite3.connect(dr.DB_PATH); cur=con.cursor()
last=cur.execute('SELECT MAX(date) FROM ntm_screening WHERE price IS NOT NULL').fetchone()[0]
out.append(f'\n최신일 {last} 기준 보유 수익률:')
for tk,(ed,ep) in sorted(detail.items()):
    cp=cur.execute('SELECT price FROM ntm_screening WHERE ticker=? AND date=?',(tk,last)).fetchone()
    if cp and ep:
        out.append(f'  {tk}: {cp[0]/ep-1:+.1%}  (진입 {ep} -> 현재 {cp[0]})')

open(r'C:\dev\claude code\eps-momentum-us\research\_valve_out.txt','w',encoding='utf-8').write('\n'.join(out))
