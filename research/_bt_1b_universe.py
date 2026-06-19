# -*- coding: utf-8 -*-
"""실험: $1B+ 종목만으로 순위 재산출하는 시스템 + 진입/이탈/슬롯 재최적화.
production w_gap 공식 정확 재현(_apply_conviction+z-score+3일가중 penalty, 시간순) — full universe서
production part2_rank 10/10 일치 검증됨(_bt_margin_threshold). 여기에 dollar_volume>=1000 추가.
비교: baseline(현행 full universe, E5/H12/S2/PE30 고정) vs $1B universe 재최적화 best.
★max-selection 편향 주의: $1B쪽은 최적화 이득 받고 baseline은 고정 → $1B가 못이기면 강한 기각."""
import sqlite3, random, statistics as st, numpy as np, json
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
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
    ratio=0
    if o['na'] and o['na']>0 and o['ru'] is not None: ratio=o['ru']/o['na']
    epsf=0
    if o['nc'] is not None and o['n90'] and abs(o['n90'])>0.01: epsf=min(abs((o['nc']-o['n90'])/o['n90']),3.0)
    bc=max(ratio,epsf)
    rb=min(min(o['rg'],0.5)*0.6,0.3) if o['rg'] is not None else 0.0
    return o['ag']*(1+bc+rb)
def eligible(o,require_1b):
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
    if o['om'] is not None and o['gm'] is not None and o['om']<0.10 and o['gm']<0.30: return False
    if o['om'] is not None and o['om']<0.05: return False
    if minseg(o)<-2: return False
    if require_1b and (o['dv'] or 0)<1000: return False   # ★ $1B universe
    return True

def build_ranks(require_1b):
    cr_by={}; score_by={}; p2_by={}; p2top30={}
    for d in dates:
        elig={tk:o for tk,o in raw[d].items() if eligible(o,require_1b)}
        cr_by[d]={tk:i+1 for i,tk in enumerate(sorted(elig,key=lambda t:elig[t]['ag']))}
        cg={tk:conv(o) for tk,o in elig.items()}; vals=list(cg.values())
        if len(vals)>=2 and np.std(vals)>0:
            m,s=np.mean(vals),np.std(vals); score_by[d]={tk:max(30.0,65+(-(v-m)/s)*15) for tk,v in cg.items()}
        else: score_by[d]={tk:65 for tk in cg}
    for idx,d in enumerate(dates):
        recent=dates[max(0,idx-2):idx+1]
        w=[0.2,0.3,0.5][-len(recent):] if len(recent)==3 else ([0.4,0.6] if len(recent)==2 else [1.0])
        wg={}
        for tk in score_by[d]:
            g=0
            for i,dd in enumerate(recent):
                if dd==d: sc=score_by[d].get(tk,30)
                elif tk not in p2top30.get(dd,set()): sc=30
                else: sc=score_by[dd].get(tk,30)
                g+=sc*w[i]
            wg[tk]=g
        order=sorted(wg,key=lambda t:-wg[t]); p2_by[d]={tk:i+1 for i,tk in enumerate(order)}; p2top30[d]=set(order[:30])
    return cr_by,p2_by

def verified(cr_by,t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        r=cr_by[dates[j]].get(t)
        if r is None or r>30: return False
    return True

def sim(cr_by,p2_by,E,H,S,P,require_1b,exclude=(),start=0,end=None):
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
                if (o['dv'] or 0)<1000: continue   # 진입은 항상 $1B (production 동일)
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]: held[tk]=1.0/S
        prev=dict(held)
    return (val-1)*100,mdd

elig_s=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig_s,SAMP) for s in range(N)]
def metrics(cr_by,p2_by,E,H,S,P,require_1b,lowo=False):
    cs=[];ms=[]
    for ch in seeds:
        for s in ch: c,m=sim(cr_by,p2_by,E,H,S,P,require_1b,(),s);cs.append(c);ms.append(m)
    full,fmdd=sim(cr_by,p2_by,E,H,S,P,require_1b)
    worst=None
    if lowo:
        worst=st.mean(cs)
        for w in WINNERS:
            a=st.mean([sim(cr_by,p2_by,E,H,S,P,require_1b,(w,),s)[0] for ch in seeds for s in ch])
            worst=min(worst,a)
    return full,fmdd,st.mean(cs),st.mean(ms),worst

# === baseline: full universe 현행 prod (E5/H12/S2/PE30) ===
crF,p2F=build_ranks(False)
# 검증: full universe가 production part2_rank 재현하나
d=dates[-1]; mine=set(sorted(p2F[d],key=lambda t:p2F[d][t])[:10]); prod=set(sorted(prod_p2[d],key=lambda t:prod_p2[d][t])[:10])
print(f'[검증] full universe part2_rank Top10 vs production: {len(mine&prod)}/10\n')
bf,bm,bp,bpm,bw=metrics(crF,p2F,5,12,2,30,False,lowo=True)
print('=== baseline (현행 full universe, E5/H12/S2/PE30 고정) ===')
print(f'  전기간 {bf:+.1f}% | paired {bp:+.1f}% | pMDD {bpm:+.1f}% | LOWO최악 {bw:+.1f}% | 전MDD {bm:+.1f}%\n')

# === $1B universe 그리드서치 ===
cr1,p21=build_ranks(True)
npd=st.mean([sum(1 for _ in p21[d]) for d in dates])
print(f'=== $1B universe 재최적화 (일평균 eligible {npd:.0f}종목, full은 ~56) ===')
print(f'{"E":>2}{"H":>4}{"S":>3}{"전기간":>9}{"paired":>9}{"pMDD":>7}')
grid=[]
for S in (1,2,3):
    for E in (3,5):
        for H in (2,3,4,5,6,8,10,12):
            full,fmdd,pavg,pmdd,_=metrics(cr1,p21,E,H,S,30,True)
            grid.append((E,H,S,full,pavg,pmdd))
            print(f'{E:>2}{H:>4}{S:>3}{full:>+8.1f}%{pavg:>+8.1f}%{pmdd:>+6.1f}%',flush=True)

# 스위트스팟 탐색: 슬롯2·E3 고정, exit H 스윕 (LOWO + walk-forward)
print('\n=== $1B 스위트스팟: 슬롯2·E3 고정, exit(H) 스윕 ===')
print(f'{"H":>3}{"전기간":>9}{"paired":>9}{"pMDD":>7}{"LOWO최악":>10}{"2-3월":>7}{"4-5월":>8}{"6월":>6}')
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
for H in (3,4,5,6,8,10,12):
    full,fmdd,pavg,pmdd,w=metrics(cr1,p21,3,H,2,30,True,lowo=True)
    seg=[sim(cr1,p21,3,H,2,30,True,(),widx(mm)[0],widx(mm)[-1]+1)[0] for mm in (['02','03'],['04','05'],['06'])]
    print(f'{H:>3}{full:>+8.1f}%{pavg:>+8.1f}%{pmdd:>+6.1f}%{w:>+9.1f}%{seg[0]:>+6.0f}%{seg[1]:>+7.0f}%{seg[2]:>+5.0f}%',flush=True)
print('\n=== 슬롯 비교 (E3·H4 고정) ===')
for S in (1,2,3):
    full,fmdd,pavg,pmdd,w=metrics(cr1,p21,3,4,S,30,True,lowo=True)
    print(f'  S{S}: 전기간{full:+.0f}% paired{pavg:+.0f}% pMDD{pmdd:+.1f}% LOWO최악{w:+.0f}%')
print(f'\n  ★baseline(현행 full E5/H12/S2): 전기간{bf:+.0f}% paired{bp:+.0f}% pMDD{bpm:+.1f}% LOWO최악{bw:+.0f}%')
