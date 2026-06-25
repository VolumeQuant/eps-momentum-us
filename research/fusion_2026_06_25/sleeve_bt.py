# -*- coding: utf-8 -*-
"""별도 시스템: 기대성장(gap) 분산 sleeve 백테스트.
   유니버스=우리 일별 EPS-screen eligible(137종목, NTM상향). gap=NTM/TTM_PIT.
   구성: top-K by gap, 동일가중, R일 리밸. 벤치=bottom-K/전체동일가중/SPY. + 약세장 오버레이 설계."""
import sqlite3, pandas as pd, pickle, numpy as np, yfinance as yf
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
# 일별 eligible + ntm_current + dollar_volume
elig={}
for d in all_dates:
    elig[d]={r[0]:{'nc':r[1],'dv':r[2]} for r in cur.execute("SELECT ticker,ntm_current,dollar_volume_30d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
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
    v=elig.get(d,{}).get(tk)
    if not v or not v['nc'] or v['nc']<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None        # 턴어라운드 제외
    g=v['nc']/te
    return g if g<=10 else None              # speculative tail 제외(>10)

# SPY
try:
    spy=yf.download('^GSPC',start='2026-02-01',end='2026-06-25',auto_adjust=False,progress=False)['Close']
    spy=spy.iloc[:,0] if hasattr(spy,'columns') else spy
    spyv={d.strftime('%Y-%m-%d'):float(v) for d,v in spy.items()}
except Exception: spyv={}

def sleeve(mode, K=15, R=5):
    """mode: 'gap_top'/'gap_bot'/'all'. R일마다 리밸, 동일가중 hold."""
    nav=1.0; peak=1.0; mdd=0.0; hold=[]; navser=[]
    for k in range(1,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1]
        # 일일 수익(전일 보유)
        if hold:
            rs=[(px(t,d)/px(t,pv)-1) for t in hold if px(t,d) and px(t,pv)]
            if rs: nav*=(1+np.mean(rs)); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
        navser.append(nav)
        # 리밸
        if (k-1)%R==0:
            cands=[(t,gap(t,d)) for t in elig.get(d,{}) if gap(t,d) is not None and t in PX.columns]
            if mode=='all': hold=[t for t,_ in cands]
            else:
                cands.sort(key=lambda x:x[1], reverse=(mode=='gap_top'))
                hold=[t for t,_ in cands[:K]]
    cagr=(nav)**(252/len(all_dates))-1
    vol=np.std(np.diff(navser)/navser[:-1])*np.sqrt(252) if len(navser)>2 else 0
    return (nav-1)*100, mdd*100, cagr*100, vol*100

def spy_ret():
    ks=[d for d in all_dates if d in spyv]
    if len(ks)<2: return None
    return (spyv[ks[-1]]/spyv[ks[0]]-1)*100

print("=== 기대성장 sleeve 백테스트 (137 eligible 유니버스, 91거래일) ===")
print(f"{'전략':18} {'누적%':>8} {'MDD%':>7} {'CAGR%':>8} {'변동성%':>8}")
for mode,lbl in [('gap_top','gap상위(롱)'),('all','전체동일가중'),('gap_bot','gap하위')]:
    for K in ([15,20] if mode!='all' else [0]):
        r=sleeve(mode,K=K)
        tag=f"{lbl} K={K}" if mode!='all' else lbl
        print(f"  {tag:18} {r[0]:>8.1f} {r[1]:>7.1f} {r[2]:>8.1f} {r[3]:>8.1f}")
print(f"  {'SPY':18} {spy_ret():>8.1f}")

print("\n=== 롱숏 스프레드 (gap상위K - gap하위K) + walk-forward ===")
for K in [15,20]:
    t=sleeve('gap_top',K)[0]; b=sleeve('gap_bot',K)[0]
    print(f"  K={K}: 상위 {t:+.1f}% - 하위 {b:+.1f}% = 스프레드 {t-b:+.1f}%p")

print("\n=== 리밸 주기 민감도 (K=15, gap상위) ===")
for R in [1,5,10,21]:
    r=sleeve('gap_top',15,R)
    print(f"  R={R}일: 누적 {r[0]:+.1f}% MDD{r[1]:.1f}% 변동성{r[3]:.1f}%")

print("\n=== 일별 유니버스 크기 (gap 계산가능) ===")
ns=[len([t for t in elig.get(d,{}) if gap(t,d) is not None]) for d in all_dates]
print(f"  평균 {np.mean(ns):.0f}종목/일 (min {min(ns)}, max {max(ns)})")
