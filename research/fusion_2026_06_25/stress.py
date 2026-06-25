# -*- coding: utf-8 -*-
"""gap sleeve의 실제 스트레스 구간(3월 조정·6월 이란) 거동 측정 — bull-beta 실측"""
import sqlite3, pandas as pd, pickle, numpy as np, yfinance as yf
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
elig={}
for d in all_dates:
    elig[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,ntm_current FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def pit_te(tk,d):
    rec=pit.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    nc=elig.get(d,{}).get(tk)
    if not nc or nc<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=nc/te; return g if g<=10 else None
def navseries(mode,K=15,R=21):
    nav=1.0; hold=[]; ser={}
    for k in range(1,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1]
        if hold:
            rs=[(px(t,d)/px(t,pv)-1) for t in hold if px(t,d) and px(t,pv)]
            if rs: nav*=(1+np.mean(rs))
        ser[d]=nav
        if (k-1)%R==0:
            cands=[(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns]
            if mode=='all': hold=[t for t,_ in cands]
            else:
                cands.sort(key=lambda x:x[1],reverse=(mode=='top')); hold=[t for t,_ in cands[:K]]
    return ser
spy=yf.download('^GSPC',start='2026-02-01',end='2026-06-25',auto_adjust=False,progress=False)['Close']
spy=spy.iloc[:,0] if hasattr(spy,'columns') else spy
spyser={d.strftime('%Y-%m-%d'):float(v) for d,v in spy.items()}

top=navseries('top'); allm=navseries('all'); bot=navseries('bot')
def dd(ser):  # max drawdown
    peak=-1; m=0; cur=1
    for d in all_dates[1:]:
        if d in ser:
            peak=max(peak,ser[d]); m=min(m,ser[d]/peak-1)
    return m*100
# SPY peak-to-trough 구간 탐지
sd=[d for d in all_dates if d in spyser]
sv=[spyser[d] for d in sd]
# 최대 낙폭 구간
peaki=0; best=(0,0,0)
for i in range(len(sv)):
    if sv[i]>sv[peaki]: peaki=i
    draw=sv[i]/sv[peaki]-1
    if draw<best[0]: best=(draw,peaki,i)
ps,pe=sd[best[1]],sd[best[2]]
print(f"=== SPY 최대 낙폭 구간: {ps} → {pe} (SPY {best[0]*100:+.1f}%) ===")
def ret(ser,a,b): return (ser[b]/ser[a]-1)*100 if (a in ser and b in ser) else None
print(f"  그 구간 수익: gap상위 {ret(top,ps,pe):+.1f}% | 전체 {ret(allm,ps,pe):+.1f}% | gap하위 {ret(bot,ps,pe):+.1f}% | SPY {best[0]*100:+.1f}%")

# 주요 스트레스 윈도(달력)
for a,b,lbl in [('2026-03-01','2026-03-31','3월 조정'),('2026-06-10','2026-06-23','6월 이란/유가'),('2026-06-18','2026-06-23','6월말 기술주매도')]:
    aa=min([d for d in all_dates if d>=a], default=None); bb=max([d for d in all_dates if d<=b], default=None)
    if aa and bb:
        print(f"  [{lbl} {aa}~{bb}] gap상위 {ret(top,aa,bb):+.1f}% | 전체 {ret(allm,aa,bb):+.1f}% | SPY {(spyser[bb]/spyser[aa]-1)*100:+.1f}%")

print(f"\n=== 전체기간 최대낙폭(MDD) ===")
print(f"  gap상위 {dd(top):.1f}% | 전체동일 {dd(allm):.1f}% | gap하위 {dd(bot):.1f}%")
# 최악 5일 다운캡처
drank=sorted([(spyser[sd[i]]/spyser[sd[i-1]]-1, sd[i]) for i in range(1,len(sd))])[:5]
print("\n=== SPY 최악 5일에 gap상위 동반하락 (다운캡처) ===")
for r,d in drank:
    di=all_dates.index(d); pv=all_dates[di-1]
    tr=(top[d]/top[pv]-1)*100 if (d in top and pv in top) else None
    print(f"  {d}: SPY {r*100:+.1f}% | gap상위 {tr:+.1f}%")
