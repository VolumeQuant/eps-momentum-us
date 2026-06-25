# -*- coding: utf-8 -*-
"""전문가 veto 아이디어 2슬롯 BT+LOWO: breadth<0 veto / gap>7 speculative veto / accel<0 veto
   + 사이징(gap/accel/breadth 높은쪽 100%) — within-pool 역전 가설 직접 BT확인"""
import sqlite3, pandas as pd, pickle, numpy as np
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
PX = pd.read_pickle(SP+r'\pxU.pkl'); pit_eps=pickle.load(open(SP+r'\pit_eps.pkl','rb'))
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12; PE_HOLD=30
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
data={}
for d in all_dates:
    data[d]={r[0]:{'p2':r[1],'nc':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6],'dv':r[7],'u':r[8],'dn':r[9],'na':r[10]}
             for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,rev_up30,rev_down30,num_analysts FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,))}
pi={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pi.get(d)
    if i is None or tk not in PX.columns: return None
    v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
def seg(a,b): return (a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0.0
def minseg(v): return min(seg(v['nc'],v['n7']),seg(v['n7'],v['n30']),seg(v['n30'],v['n60']),seg(v['n60'],v['n90']))
def pit_teps(tk,d):
    rec=pit_eps.get(tk); val=None
    if not rec: return None
    for rd,e in rec:
        if rd<=d: val=e
        else: break
    return val
def feat(tk,d):
    v=data.get(d,{}).get(tk)
    if not v: return {}
    te=pit_teps(tk,d)
    return {'gap':(v['nc']/te if (te and te>0.5 and v['nc']>0) else np.nan),
            'accel':1.5*seg(v['nc'],v['n7'])+0.5*seg(v['n7'],v['n30'])-0.5*seg(v['n30'],v['n60'])-1.5*seg(v['n60'],v['n90']),
            'breadth':(((v['u'] or 0)-(v['dn'] or 0))/v['na'] if v['na'] else np.nan)}

def run(veto=None, size_by=None, ban=()):
    tradable=set(PX.columns); pf={}; nav=1.0; peak=1.0; mdd=0.0
    for k in range(2,len(all_dates)):
        d,pv=all_dates[k],all_dates[k-1]; dd=data.get(d,{})
        ms={tk:minseg(v) for tk,v in dd.items()}
        wrank={tk:v['p2'] for tk,v in dd.items() if v.get('p2')}
        elig=sorted([(tk,v['p2']) for tk,v in dd.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
        dr=0.0
        for tk,info in pf.items():
            w=info['weight']/100; cu,pp=px(tk,d),px(tk,pv)
            if cu and pp and pp>0: dr+=w*(cu-pp)/pp*100
        nav*=(1+dr/100); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
        for tk in list(pf.keys()):
            cp=px(tk,d)
            if cp is None: continue
            it=dd.get(tk)
            if it is None: continue
            rk,nc,m=it['p2'],it['nc'],minseg(it)
            sell=False
            if m<-2: sell=True
            elif rk>EXIT_RANK:
                pe=(cp/nc) if (nc and nc>0) else 999
                if pe>=PE_HOLD: sell=True
            if sell: del pf[tk]
        if len(pf)<2:
            free=sorted([s for s in range(2) if s not in {info['slot_idx'] for info in pf.values()}])
            cands=[tk for tk,_ in elig if tk not in pf and tk in tradable and tk not in ban
                   and ms.get(tk,-9)>=0 and wrank.get(tk,999)<=5 and (dd.get(tk,{}).get('dv') or 0)>=1000]
            if veto:
                kept=[]
                for tk in cands:
                    f=feat(tk,d)
                    if veto=='breadth' and (f.get('breadth') is not None and f['breadth']<0): continue
                    if veto=='gapcap' and (f.get('gap') is not None and f['gap']>7): continue
                    if veto=='accel' and (f.get('accel',0)<0): continue
                    kept.append(tk)
                cands=kept
            cands.sort(key=lambda t:wrank.get(t,999))
            for tk in cands:
                if len(pf)>=2: break
                ip=px(tk,d); ix=free.pop(0) if free else len(pf)
                if ip: pf[tk]={'entry_price':ip,'slot_idx':ix,'weight':0,'tk':tk}
            # 사이징
            held=list(pf.keys())
            if size_by and len(held)==2:
                fs={tk:feat(tk,d).get(size_by,np.nan) for tk in held}
                if all(pd.notna(list(fs.values()))):
                    hi=max(fs,key=fs.get)
                    for tk in pf: pf[tk]['weight']=100 if tk==hi else 0
                else:
                    for tk in pf: pf[tk]['weight']=50
            else:
                n=len(pf)
                for info in pf.values(): info['weight']=100/n if n else 0
    return (nav-1)*100, mdd*100
base=run()
print(f"baseline {base[0]:+.1f}% MDD{base[1]:.1f}%\n")
print("=== veto / 사이징 변형 ===")
for lbl,kw in [('breadth<0 veto',{'veto':'breadth'}),('gap>7 veto',{'veto':'gapcap'}),('accel<0 veto',{'veto':'accel'}),
               ('size:gap 高100%',{'size_by':'gap'}),('size:accel 高100%',{'size_by':'accel'}),('size:breadth 高100%',{'size_by':'breadth'})]:
    r=run(**kw); print(f"  {lbl:18}: {r[0]:+8.1f}% (Δ{r[0]-base[0]:+6.1f}p) MDD{r[1]:.1f}%")
print("\n=== 최선 후보 LOWO ===")
for lbl,kw in [('gap>7 veto',{'veto':'gapcap'}),('breadth<0 veto',{'veto':'breadth'})]:
    worst=min(run(**kw,ban=(w,))[0]-run(ban=(w,))[0] for w in ['MU','SNDK','STX','NVDA','LITE','AVGO'])
    print(f"  {lbl}: worst-LOWO {worst:+.1f}p → {'통과' if worst>=0 else '기각'}")
