# -*- coding: utf-8 -*-
"""저마진 필터 GM 임계(현행 30%) 최적성 검증.
필터: OM<10% AND GM<T → 제외. T를 스윕(none/20/25/30/35/40)하며 유니버스 재구성→재랭킹→시뮬.
production w_gap 공식 정확 재현(_apply_conviction + z-score + 3일가중 penalty30, 시간순).
baseline(T=30)이 실제 production part2_rank 재현하는지 검증 후 변형 비교."""
import sqlite3, random, statistics as st, numpy as np, json
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10; PE_HOLD=30.0
# 원자재 제외 (production COMMODITY_INDUSTRIES/TICKERS, industry는 ticker_info_cache)
COMMODITY_IND={'금','귀금속','산업금속','구리','철강','알루미늄','농업','석유가스','석유종합','석유정제','목재',
    'Gold','Other Precious Metals & Mining','Other Industrial Metals & Mining','Copper','Steel','Aluminum',
    'Agricultural Inputs','Oil & Gas E&P','Oil & Gas Integrated','Oil & Gas Refining & Marketing','Lumber & Wood Production'}
COMMODITY_TK={'SQM','ALB'}
_cache=json.load(open('ticker_info_cache.json',encoding='utf-8'))
IND={tk:v.get('industry') for tk,v in _cache.items()}
def is_commodity(tk): return tk in COMMODITY_TK or IND.get(tk) in COMMODITY_IND
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')]
raw={}; prod_p2={}
cols='ticker,adj_score,adj_gap,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,price,ma120,ma60,high30,rev_growth,num_analysts,rev_up30,rev_down30,operating_margin,gross_margin,composite_rank,part2_rank,dollar_volume_30d'
for d in dates:
    raw[d]={}; prod_p2[d]={}
    for r in cur.execute(f'SELECT {cols} FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0]; o=dict(tk=tk,ads=r[1],ag=r[2],nc=r[3],n7=r[4],n30=r[5],n60=r[6],n90=r[7],price=r[8],
                        ma=r[9] if r[9] is not None else r[10],h30=r[11],rg=r[12],na=r[13],
                        ru=r[14],rd=r[15],om=r[16],gm=r[17],dv=r[20])
        raw[d][tk]=o
        if r[19] is not None: prod_p2[d][tk]=r[19]
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()

def minseg(o):
    s=[]
    for a,b in [(o['nc'],o['n7']),(o['n7'],o['n30']),(o['n30'],o['n60']),(o['n60'],o['n90'])]:
        s.append((a-b)/abs(b)*100 if b and abs(b)>0.01 else 0)
    return min(s)
def conv(o):
    adj_gap=o['ag']; ratio=0
    if o['na'] and o['na']>0 and o['ru'] is not None: ratio=o['ru']/o['na']
    epsf=0
    if o['nc'] is not None and o['n90'] and abs(o['n90'])>0.01: epsf=min(abs((o['nc']-o['n90'])/o['n90']),3.0)
    bc=max(ratio,epsf)
    rb=min(min(o['rg'],0.5)*0.6,0.3) if o['rg'] is not None else 0.0
    return adj_gap*(1+bc+rb)
def eligible(o,T):
    if is_commodity(o['tk']): return False
    if o['ads'] is None or o['ads']<=9: return False
    if o['ag'] is None: return False
    if not o['nc'] or o['nc']<=0 or not o['price'] or o['price']<10: return False
    if o['n90'] and o['nc']/o['n90']-1<=0: return False
    if o['ma'] is None or o['price']<=o['ma']: return False
    if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: return False
    if o['rg'] is None or o['rg']<0.10: return False
    if o['na'] is None or o['na']<3: return False
    if o['ru'] is None or o['ru']<3: return False
    tot=(o['ru'] or 0)+(o['rd'] or 0)
    if tot>0 and (o['rd'] or 0)/tot>0.3: return False
    if o['om'] is not None and o['gm'] is not None and o['om']<0.10 and o['gm']<T: return False
    if o['om'] is not None and o['om']<0.05: return False
    if minseg(o)<-2: return False
    return True

# 시간순으로 cr/p2 재계산 (T별). penalty엔 직전까지 계산된 p2 Top30 사용.
def build_ranks(T):
    cr_by={}; p2_by={}; p2top30={}; score_by={}
    for d in dates:
        elig={tk:o for tk,o in raw[d].items() if eligible(o,T)}
        # composite_rank = adj_gap 오름차순
        cr_sorted=sorted(elig, key=lambda tk: elig[tk]['ag'])
        cr_by[d]={tk:i+1 for i,tk in enumerate(cr_sorted)}
        # daily score (z of conviction-gap, 낮을수록 고득점)
        cg={tk:conv(o) for tk,o in elig.items()}
        vals=list(cg.values())
        if len(vals)>=2 and np.std(vals)>0:
            m,s=np.mean(vals),np.std(vals)
            score_by[d]={tk:max(30.0,65+(-(v-m)/s)*15) for tk,v in cg.items()}
        else:
            score_by[d]={tk:65 for tk in cg}
    # 2nd pass: w_gap (시간순, penalty용 p2top30 누적)
    for idx,d in enumerate(dates):
        recent=[dd for dd in dates[max(0,idx-2):idx+1]]
        w=[0.2,0.3,0.5][-len(recent):] if len(recent)==3 else ([0.4,0.6] if len(recent)==2 else [1.0])
        wg={}
        for tk in score_by[d]:  # T0 eligible만 후보
            g=0
            for i,dd in enumerate(recent):
                if dd==d: sc=score_by[d].get(tk,30)
                elif tk not in p2top30.get(dd,set()): sc=30
                else: sc=score_by[dd].get(tk,30)
                g+=sc*w[i]
            wg[tk]=g
        order=sorted(wg, key=lambda tk:-wg[tk])
        p2_by[d]={tk:i+1 for i,tk in enumerate(order)}
        p2top30[d]=set(order[:30])
    return cr_by,p2_by

def verified(cr_by,t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        r=cr_by[dates[j]].get(t)
        if r is None or r>30: return False
    return True

def sim(cr_by,p2_by,exclude=(),start=0,slots=2):
    held={};prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,w in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        o_d=raw[d]; p2d=p2_by[d]
        for tk in list(held):
            o=o_d.get(tk)
            if o and minseg(o)<-2: del held[tk];continue
            if o is None: continue
            p2=p2d.get(tk)
            if not (p2 is None or p2>10): continue
            _pe=o['price']/o['nc'] if o.get('nc',0)>0 else 999
            if _pe>=PE_HOLD: del held[tk]
        if len(held)<slots:
            cands=[]
            for tk,p2 in p2d.items():
                if tk in held or tk in exclude or p2>5: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(cr_by,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:slots-len(held)]: held[tk]=1.0/slots
        prev=dict(held)
    return (val-1)*100,mdd

# === 검증: baseline(T=30) 재현도 ===
cr30,p2_30=build_ranks(0.30)
d=dates[-1]
mine=sorted(p2_30[d],key=lambda tk:p2_30[d][tk])[:10]
prod=sorted(prod_p2[d],key=lambda tk:prod_p2[d][tk])[:10]
ov=len(set(mine)&set(prod))
print(f'검증 {d} 내 part2_rank Top10 vs production Top10 일치: {ov}/10')
print(f'  내것: {mine}')
print(f'  prod: {prod}\n')

elig30=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig30,SAMP) for s in range(N)]
def metrics(T):
    cr_by,p2_by=build_ranks(T)
    cs=[];ms=[]
    for ch in seeds:
        for s in ch: c,m=sim(cr_by,p2_by,(),s);cs.append(c);ms.append(m)
    full,fmdd=sim(cr_by,p2_by)
    worst=st.mean(cs)
    for w in WINNERS:
        a=st.mean([sim(cr_by,p2_by,(w,),s)[0] for ch in seeds for s in ch])
        worst=min(worst,a)
    return full,fmdd,st.mean(cs),st.mean(ms),worst

print(f'=== 저마진 GM 임계 스윕 (OM<10% AND GM<T 제외), 현행 T=30% ===')
print(f'{"GM임계":>8}{"전기간":>9}{"paired":>9}{"pMDD":>7}{"LOWO최악":>10}')
for T,lbl in [(0.0,'없음'),(0.20,'20%'),(0.25,'25%'),(0.30,'30%'),(0.35,'35%'),(0.40,'40%')]:
    full,fmdd,pavg,pmdd,worst=metrics(T)
    tag=' ← 현행' if T==0.30 else (' (필터제거)' if T==0.0 else '')
    print(f'{lbl:>8}{full:>+8.1f}%{pavg:>+8.1f}%{pmdd:>+6.1f}%{worst:>+9.1f}%{tag}',flush=True)
print('\n해석: 현행30%가 robust(paired+LOWO) 상위면 유지. 다른 임계가 명확우위면 재검토.')
