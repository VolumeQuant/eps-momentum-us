# -*- coding: utf-8 -*-
"""PE_gap = trailingPE/forwardPE (=기대 EPS성장률) 가설 — 현재 스냅샷 + 최근수익률 대조"""
import sqlite3, yfinance as yf, pandas as pd, pickle
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
PX = pd.read_pickle(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\pxU.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
tks=sorted({r[0] for r in cur.execute("SELECT DISTINCT ticker FROM ntm_screening WHERE part2_rank<=12")})
kr=['005930.KS','000660.KS']  # 삼성전자, SK하이닉스
rows=[]
info_cache={}
for tk in tks+kr:
    try:
        info=yf.Ticker(tk).info
        info_cache[tk]=info
        tpe=info.get('trailingPE'); fpe=info.get('forwardPE')
        teps=info.get('trailingEps'); feps=info.get('forwardEps')
        rows.append({'tk':tk,'tpe':tpe,'fpe':fpe,'teps':teps,'feps':feps,
                     'mcap':info.get('marketCap'),'sector':info.get('sector')})
    except Exception as e:
        rows.append({'tk':tk,'tpe':None,'fpe':None,'teps':None,'feps':None})
pickle.dump(info_cache, open(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\info.pkl','wb'))
df=pd.DataFrame(rows)
# gap_ratio = teps_fwd/teps_trail = tpe/fpe = 기대성장률
def gap(r):
    if r['fpe'] and r['fpe']>0 and r['tpe'] and r['tpe']>0:
        return r['tpe']/r['fpe']
    if r['feps'] and r['teps'] and r['teps']>0:
        return r['feps']/r['teps']
    return None
df['gap']=df.apply(gap,axis=1)
# 최근 수익률 (pxU 있는 종목만)
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def ret(tk,n):
    if tk not in PX.columns: return None
    s=PX[tk].dropna()
    if len(s)<n+1: return None
    return (s.iloc[-1]/s.iloc[-1-n]-1)*100
df['ret20']=df['tk'].apply(lambda t:ret(t,20))
df['ret60']=df['tk'].apply(lambda t:ret(t,60))
df.to_pickle(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\pegap.pkl')

print("=== 네가 든 예시 (삼성·SK하이닉스) ===")
for _,r in df[df.tk.isin(kr)].iterrows():
    print(f"  {r['tk']}: trailPE={r['tpe']} fwdPE={r['fpe']} gap={r['gap']:.2f}x" if r['gap'] else f"  {r['tk']}: data없음 (trailPE={r['tpe']} fwdPE={r['fpe']})")

print("\n=== US 현재 매수후보/상위 종목 PE_gap ===")
top=[t[0] for t in cur.execute("SELECT ticker FROM ntm_screening WHERE date='2026-06-24' AND part2_rank<=12 ORDER BY part2_rank")]
sub=df[df.tk.isin(top)].copy()
sub['p2']=sub['tk'].apply(lambda t:[r[0] for r in cur.execute('SELECT part2_rank FROM ntm_screening WHERE date=? AND ticker=?',('2026-06-24',t))][0])
sub=sub.sort_values('p2')
print(f"{'rk':>3} {'tk':6} {'trailPE':>8} {'fwdPE':>7} {'gap':>6} {'ret20%':>7} {'ret60%':>7}")
for _,r in sub.iterrows():
    g=f"{r['gap']:.2f}" if r['gap'] else 'NA'
    print(f"{r['p2']:>3} {r['tk']:6} {str(round(r['tpe'],1) if r['tpe'] else 'NA'):>8} {str(round(r['fpe'],1) if r['fpe'] else 'NA'):>7} {g:>6} {(f'{r.ret20:+.1f}' if pd.notna(r.ret20) else 'NA'):>7} {(f'{r.ret60:+.1f}' if pd.notna(r.ret60) else 'NA'):>7}")
