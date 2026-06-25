# -*- coding: utf-8 -*-
"""2월 시작일부터: gap sleeve(K7월간) vs 2슬롯 모멘텀 — 거래내역·현재잔고·수익률·MDD"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\px137.pkl'); pit=pickle.load(open(SP+r'\pit137.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date").fetchall()]
# 데이터: 둘 다 필요한 필드
D={}
for d in all_dates:
    D[d]={r[0]:{'p2':r[1],'nc':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6],'dv':r[7]}
          for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def seg(a,b): return (a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0.0
def minseg(v): return min(seg(v['nc'],v['n7']),seg(v['n7'],v['n30']),seg(v['n30'],v['n60']),seg(v['n60'],v['n90']))
def pit_te(tk,d):
    rec=pit.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def gap(tk,d):
    v=D.get(d,{}).get(tk)
    if not v or not v['nc'] or v['nc']<=0: return None
    te=pit_te(tk,d)
    if not te or te<0.5: return None
    g=v['nc']/te; return g if g<=10 else None

# ===== 1. GAP SLEEVE (K=7 월간 strict) =====
print("="*60); print("① 기대성장 Sleeve (K=7, 월간, strict)"); print("="*60)
K=7; nav=1.0;peak=1.0;mdd=0.0;hold=[];lastpx={};reb_month=None;log=[]
for d in all_dates:
    if hold and lastpx:
        rs=[(px(t,d)/lastpx[t]-1) for t in hold if px(t,d) and lastpx.get(t)]
        if rs:
            nav*=(1+np.mean(rs));peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
            for t in hold:
                if px(t,d):lastpx[t]=px(t,d)
    m=d[:7]
    if m!=reb_month or not hold:
        ranked=sorted([(t,gap(t,d)) for t in D.get(d,{}) if gap(t,d) is not None and t in PX.columns],key=lambda x:-x[1])
        new=[t for t,_ in ranked[:K] if px(t,d)]
        if new:
            sells=[t for t in hold if t not in new]; buys=[t for t in new if t not in hold]
            log.append((d,round((nav-1)*100,1),sells,buys,new[:])); hold=new;lastpx={t:px(t,d) for t in new};reb_month=m
print(f"{'날짜':10} {'NAV%':>6}  매도→매수")
for d,nv,s,b,h in log:
    print(f"{d:10} {nv:>+6.1f}  OUT[{','.join(s) if s else '-'}] IN[{','.join(b) if b else '-'}]")
print(f"\n현재 잔고(06-24, 7종목 동일가중): {', '.join(hold)}")
print(f"수익률 {(nav-1)*100:+.1f}%  |  MDD {mdd*100:.1f}%")
sleeve_nav,sleeve_mdd=nav,mdd

# ===== 2. 2슬롯 모멘텀 =====
print("\n"+"="*60); print("② 2슬롯 EPS 모멘텀 (진입rank≤5·이탈>12+PER≥30·$1B)"); print("="*60)
EXIT_RANK=12;PE_HOLD=30
nav=1.0;peak=1.0;mdd=0.0;pf={};log2=[]
for k in range(2,len(all_dates)):
    d,pv=all_dates[k],all_dates[k-1];dd=D.get(d,{})
    ms={t:minseg(v) for t,v in dd.items()}
    wrank={t:v['p2'] for t,v in dd.items() if v.get('p2')}
    elig=sorted([(t,v['p2']) for t,v in dd.items() if ms.get(t,0)>=-2 and v.get('p2')],key=lambda x:x[1])
    dr=0.0
    for t,info in pf.items():
        w=info['w']/100;cu,pp=px(t,d),px(t,pv)
        if cu and pp and pp>0: dr+=w*(cu-pp)/pp*100
    nav*=(1+dr/100);peak=max(peak,nav);mdd=min(mdd,nav/peak-1)
    for t in list(pf.keys()):
        cp=px(t,d)
        if cp is None: continue
        it=dd.get(t)
        if it is None or it.get('p2') is None: continue   # top30 밖 → carryover(보유), production 정합
        rk=it['p2'];nc=it['nc'];mg=minseg(it);sell=False;why=''
        if mg<-2: sell=True;why='EPS꺾임'
        elif rk>EXIT_RANK:
            pe=(cp/nc) if (nc and nc>0) else 999
            if pe>=PE_HOLD: sell=True;why=f'순위{rk}+PER{pe:.0f}'
        if sell:
            ret=(cp/pf[t]['ep']-1)*100
            log2.append((d,'매도',t,f'{ret:+.0f}%',why));del pf[t]
    if len(pf)<2:
        free=sorted([s for s in range(2) if s not in {i['slot'] for i in pf.values()}])
        cands=[t for t,_ in elig if t not in pf and t in PX.columns and ms.get(t,-9)>=0 and wrank.get(t,999)<=5 and (dd.get(t,{}).get('dv') or 0)>=1000]
        cands.sort(key=lambda t:wrank.get(t,999))
        for t in cands:
            if len(pf)>=2: break
            ip=px(t,d);ix=free.pop(0) if free else len(pf)
            if ip: pf[t]={'ep':ip,'slot':ix,'w':0,'ed':d};log2.append((d,'매수',t,f'rank{wrank.get(t)}',''))
        n=len(pf)
        for i in pf.values(): i['w']=100/n if n else 0
print(f"{'날짜':10} {'구분':>4} {'종목':6} {'수익/순위':>9} {'사유':12}")
for e in log2: print(f"{e[0]:10} {e[1]:>4} {e[2]:6} {e[3]:>9} {e[4]:12}")
print(f"\n현재 잔고(06-24): {', '.join(f'{t}({(px(t,all_dates[-1])/pf[t]['ep']-1)*100:+.0f}%)' for t in pf)}")
print(f"수익률 {(nav-1)*100:+.1f}%  |  MDD {mdd*100:.1f}%")

# ===== 비교 =====
print("\n"+"="*60); print("③ 비교 (2026-02-12 ~ 06-24, 거래비용 0 가정 BT)"); print("="*60)
print(f"{'시스템':22} {'수익률':>8} {'MDD':>7} {'거래수':>6}")
print(f"{'기대성장 Sleeve (K7월간)':22} {(sleeve_nav-1)*100:>+7.1f}% {sleeve_mdd*100:>6.1f}% {len(log)*2:>6}")
print(f"{'2슬롯 EPS 모멘텀':22} {(nav-1)*100:>+7.1f}% {mdd*100:>6.1f}% {len(log2):>6}")
