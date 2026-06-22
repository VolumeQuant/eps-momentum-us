# -*- coding: utf-8 -*-
"""메인 연구: 유니버스 필터(거래량/시총/조합)를 순위단계에 적용 → "작은종목 제외 + 성능 robust 개선" 있나.
production w_gap 정확재현(full universe로 production part2_rank 10/10 검증). 거래량=화면정리축, 시총=성능축 분리 테스트.
판정=paired(100×3)+LOWO최악+walk-forward (raw수익 아님). full universe baseline 대비."""
import sqlite3, random, statistics as st, numpy as np, json
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE','MPWR','AVGO'}
COMMODITY_IND={'금','귀금속','산업금속','구리','철강','알루미늄','농업','석유가스','석유종합','석유정제','목재',
    'Gold','Other Precious Metals & Mining','Other Industrial Metals & Mining','Copper','Steel','Aluminum',
    'Agricultural Inputs','Oil & Gas E&P','Oil & Gas Integrated','Oil & Gas Refining & Marketing','Lumber & Wood Production'}
COMMODITY_TK={'SQM','ALB'}
_cache=json.load(open('ticker_info_cache.json',encoding='utf-8'))
IND={tk:v.get('industry') for tk,v in _cache.items()}
def is_commodity(tk): return tk in COMMODITY_TK or IND.get(tk) in COMMODITY_IND
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')]
raw={}; prod_p2={}
cols='ticker,adj_score,adj_gap,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,price,ma120,ma60,high30,rev_growth,num_analysts,rev_up30,rev_down30,operating_margin,gross_margin,composite_rank,part2_rank,dollar_volume_30d,market_cap'
for d in dates:
    raw[d]={}; prod_p2[d]={}
    for r in cur.execute(f'SELECT {cols} FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0]; o=dict(tk=tk,ads=r[1],ag=r[2],nc=r[3],n7=r[4],n30=r[5],n60=r[6],n90=r[7],price=r[8],
                        ma=r[9] if r[9] is not None else r[10],h30=r[11],rg=r[12],na=r[13],
                        ru=r[14],rd=r[15],om=r[16],gm=r[17],dv=r[20],mc=(r[21] or 0)/1e9)
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
    ratio=o['ru']/o['na'] if (o['na'] and o['ru'] is not None) else 0
    epsf=min(abs((o['nc']-o['n90'])/o['n90']),3.0) if (o['nc'] and o['n90'] and abs(o['n90'])>0.01) else 0
    rb=min(min(o['rg'],0.5)*0.6,0.3) if o['rg'] is not None else 0
    return o['ag']*(1+max(ratio,epsf)+rb)
def eligible(o,volthr,mcthr):
    if is_commodity(o['tk']): return False
    if o['ads'] is None or o['ads']<=9 or o['ag'] is None: return False
    if not o['nc'] or o['nc']<=0 or not o['price'] or o['price']<10: return False
    if o['n90'] and o['nc']/o['n90']-1<=0: return False
    if o['ma'] is None or o['price']<=o['ma']: return False
    if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: return False
    if o['rg'] is None or o['rg']<0.10 or o['na'] is None or o['na']<3 or o['ru'] is None or o['ru']<3: return False
    tot=(o['ru'] or 0)+(o['rd'] or 0)
    if tot>0 and (o['rd'] or 0)/tot>0.3: return False
    if o['om'] is not None and o['gm'] is not None and o['om']<0.10 and o['gm']<0.30: return False
    if o['om'] is not None and o['om']<0.05 or minseg(o)<-2: return False
    if volthr and (o['dv'] or 0)<volthr*1000: return False     # 거래량 $XB
    if mcthr and o['mc']<mcthr: return False                    # 시총 $XB
    return True
def build_ranks(volthr,mcthr):
    cr_by={};score_by={};p2_by={};p2top30={}
    for d in dates:
        eg={tk:o for tk,o in raw[d].items() if eligible(o,volthr,mcthr)}
        cr_by[d]={tk:i+1 for i,tk in enumerate(sorted(eg,key=lambda t:eg[t]['ag']))}
        cg={tk:conv(o) for tk,o in eg.items()};v=list(cg.values())
        score_by[d]=({tk:max(30.,65+(-(x-np.mean(v))/np.std(v))*15) for tk,x in cg.items()} if len(v)>=2 and np.std(v)>0 else {tk:65 for tk in cg})
    for idx,d in enumerate(dates):
        rc=dates[max(0,idx-2):idx+1];w=[0.2,0.3,0.5][-len(rc):] if len(rc)==3 else([0.4,0.6] if len(rc)==2 else[1.])
        wg={}
        for tk in score_by[d]:
            g=sum((score_by[d].get(tk,30) if dd==d else (score_by[dd].get(tk,30) if tk in p2top30.get(dd,set()) else 30))*w[i] for i,dd in enumerate(rc))
            wg[tk]=g
        order=sorted(wg,key=lambda t:-wg[t]);p2_by[d]={tk:i+1 for i,tk in enumerate(order)};p2top30[d]=set(order[:30])
    return cr_by,p2_by
def verified(cr_by,t,i):
    for j in (i,i-1,i-2):
        if j<0 or cr_by[dates[j]].get(t) is None or cr_by[dates[j]].get(t,99)>30: return False
    return True
def sim(cr_by,p2_by,E,H,S,P,exclude=(),start=0,end=None):
    if end is None: end=len(dates)
    held={};prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,end):
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
            if not (p2 is None or p2>H): continue
            _pe=o['price']/o['nc'] if o.get('nc',0)>0 else 999
            if _pe>=P: del held[tk]
        if len(held)<S:
            cands=[]
            for tk,p2 in p2d.items():
                if tk in held or tk in exclude or p2>E: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(cr_by,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue   # 진입 항상 $1B (production)
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]: held[tk]=1.0/S
        prev=dict(held)
    return (val-1)*100,mdd
elig_s=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig_s,SAMP) for s in range(N)]
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
def battery(volthr,mcthr,E=5,H=12,S=2):
    cr,p2=build_ranks(volthr,mcthr)
    cs=[];ms=[]
    for ch in seeds:
        for s in ch: c,m=sim(cr,p2,E,H,S,30,(),s);cs.append(c);ms.append(m)
    full,fmdd=sim(cr,p2,E,H,S,30)
    worst=st.mean(cs)
    for w in WINNERS:
        a=st.mean([sim(cr,p2,E,H,S,30,(w,),s)[0] for ch in seeds for s in ch])
        worst=min(worst,a)
    seg=[sim(cr,p2,E,H,S,30,(),widx(mm)[0],widx(mm)[-1]+1)[0] for mm in (['02','03'],['04','05'],['06'])]
    npd=st.mean([len(p2[d]) for d in dates])
    return full,fmdd,st.mean(cs),st.mean(ms),worst,seg,npd
# 검증
cr0,p20=build_ranks(0,0)
d=dates[-1];mine=set(sorted(p20[d],key=lambda t:p20[d][t])[:10]);prod=set(sorted(prod_p2[d],key=lambda t:prod_p2[d][t])[:10])
print(f'[검증] full universe Top10 vs production: {len(mine&prod)}/10\n')
print(f'{"유니버스 필터":<22}{"종목":>5}{"전기간":>9}{"paired":>9}{"pMDD":>7}{"LOWO":>8}{"2-3월":>6}{"4-5월":>7}')
configs=[('현행 full(필터X)',0,0),('거래량 $1B',1.0,0),('거래량 $2B',2.0,0),
         ('시총 $10B',0,10),('시총 $30B',0,30),('시총 $50B',0,50),('시총 $100B',0,100),
         ('$1B거래량+$30B시총',1.0,30),('$1B거래량+$50B시총',1.0,50)]
for nm,vt,mt in configs:
    full,fmdd,pa,pm,lw,seg,npd=battery(vt,mt)
    tag=' ←현행' if (vt==0 and mt==0) else ''
    print(f'{nm:<22}{npd:>5.0f}{full:>+8.0f}%{pa:>+8.0f}%{pm:>+6.0f}%{lw:>+7.0f}%{seg[0]:>+5.0f}%{seg[1]:>+6.0f}%{tag}',flush=True)
print('\n판정: paired·LOWO·전기간이 현행보다 robust 명확초과(편향감안 +수%p)면 후보. 비슷하면 중립(화면정리만 OK).')
