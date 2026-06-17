# -*- coding: utf-8 -*-
"""파라미터 최적성 재검증: 진입(E)/이탈hold경계(H)/슬롯(S) 그리드 + PE_HOLD 스윕.
v119b production-replay. 87일 단일강세장이라 raw수익=과적합위험 → paired(100×3)+LOWO최악으로 판정.
현행 = E5/H10/S2/PE30. plateau에 있는지(=최적 근처) 확인이 목적."""
import sqlite3, random, statistics as st
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[4:9]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],min_seg=min(segs),high30=r[9],dv=r[10],ntm=nc)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def sim(E,H,S,P,exclude=(),start=0):
    held={};prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(w,) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: del held[tk];continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>H): continue   # hold 경계 H 안 = 보유
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=P: del held[tk]                  # 비싸짐 → 매도
        if len(held)<S:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>E: continue      # 진입 경계 E
                cands.append((p2,tk))
            cands.sort();pick=cands[:S-len(held)]
            for _,tk in pick: held[tk]=(1.0/S,)
        prev=dict(held)
    return (val-1)*100,mdd

elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def metrics(E,H,S,P):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch: c,m=sim(E,H,S,P,(),s);cs.append(c);ms.append(m)
    full,fmdd=sim(E,H,S,P)
    pavg=st.mean(cs);pmdd=st.mean(ms)
    worst=pavg
    for w in WINNERS:
        cw=[sim(E,H,S,P,(w,),s)[0] for ch in seeds for s in ch]
        a=st.mean(cw)
        if a<worst: worst=a
    return full,fmdd,pavg,pmdd,worst

print(f'BT {dates[0]}~{dates[-1]} ({len(dates)}일). 현행=E5/H10/S2/PE30. 판정=paired+LOWO최악(raw 아님)\n')
print('=== (1) 진입E × 이탈H × 슬롯S 그리드 (PE_HOLD=30 고정) ===')
print(f'{"E":>2}{"H":>4}{"S":>3}{"전기간":>9}{"paired":>9}{"pMDD":>7}{"LOWO최악":>10}')
grid=[]
for S in (1,2,3):
    for E in (3,4,5,6):
        for H in (8,10,12,15):
            full,fmdd,pavg,pmdd,worst=metrics(E,H,S,30)
            grid.append((E,H,S,full,pavg,pmdd,worst))
            cur_tag=' ← 현행' if (E,H,S)==(5,10,2) else ''
            print(f'{E:>2}{H:>4}{S:>3}{full:>+8.1f}%{pavg:>+8.1f}%{pmdd:>+6.1f}%{worst:>+9.1f}%{cur_tag}',flush=True)

print('\n=== robust 정렬 (LOWO최악 기준 Top8) ===')
for E,H,S,full,pavg,pmdd,worst in sorted(grid,key=lambda x:-x[6])[:8]:
    tag=' ← 현행' if (E,H,S)==(5,10,2) else ''
    print(f'  E{E}/H{H}/S{S}: LOWO최악 {worst:+.1f}% | paired {pavg:+.1f}% | 전기간 {full:+.1f}% | MDD {pmdd:+.1f}%{tag}')

print('\n=== (2) PE_HOLD 스윕 (현행 E5/H10/S2) ===')
print(f'{"PE_HOLD":>8}{"전기간":>9}{"paired":>9}{"pMDD":>7}{"LOWO최악":>10}')
for P in (20,25,30,35,40,999):
    full,fmdd,pavg,pmdd,worst=metrics(5,10,2,P)
    tag=' ← 현행' if P==30 else (' (veto없음)' if P==999 else '')
    print(f'{P:>8}{full:>+8.1f}%{pavg:>+8.1f}%{pmdd:>+6.1f}%{worst:>+9.1f}%{tag}',flush=True)

print('\n해석: 현행이 robust(paired+LOWO) 상위 plateau면 = 최적 근처, 변경 불필요.')
print('단일 config가 raw수익만 튀고 paired/LOWO 안 따라오면 과적합. 채택은 robust 명확우위만.')
