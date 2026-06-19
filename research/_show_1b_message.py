# -*- coding: utf-8 -*-
"""$1B-only 순위로 바꾸면 06-18 메시지가 어떻게 나오는지 렌더링 (full vs $1B 비교).
검증된 머신(_bt_1b_universe와 동일 공식, production 10/10) 사용."""
import sqlite3, numpy as np, json, sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; PE_HOLD=30.0; EXIT_RANK=12
COMMODITY_IND={'금','귀금속','산업금속','구리','철강','알루미늄','농업','석유가스','석유종합','석유정제','목재',
    'Gold','Other Precious Metals & Mining','Other Industrial Metals & Mining','Copper','Steel','Aluminum',
    'Agricultural Inputs','Oil & Gas E&P','Oil & Gas Integrated','Oil & Gas Refining & Marketing','Lumber & Wood Production'}
COMMODITY_TK={'SQM','ALB'}
_cache=json.load(open('ticker_info_cache.json',encoding='utf-8'))
IND={tk:v.get('industry') for tk,v in _cache.items()}
NAME={tk:v.get('shortName',v.get('short_name',tk)) for tk,v in _cache.items()}
def is_commodity(tk): return tk in COMMODITY_TK or IND.get(tk) in COMMODITY_IND
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')]
cols='ticker,adj_score,adj_gap,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,price,ma120,ma60,high30,rev_growth,num_analysts,rev_up30,rev_down30,operating_margin,gross_margin,composite_rank,part2_rank,dollar_volume_30d'
raw={}; prodp2={}
for d in dates:
    raw[d]={}; prodp2[d]={}
    for r in cur.execute(f'SELECT {cols} FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0]; raw[d][tk]=dict(tk=tk,ads=r[1],ag=r[2],nc=r[3],n7=r[4],n30=r[5],n60=r[6],n90=r[7],price=r[8],
            ma=r[9] if r[9] is not None else r[10],h30=r[11],rg=r[12],na=r[13],ru=r[14],rd=r[15],om=r[16],gm=r[17],dv=r[20])
        if r[19] is not None: prodp2[d][tk]=r[19]
con.close()
def minseg(o):
    s=[(a-b)/abs(b)*100 if b and abs(b)>0.01 else 0 for a,b in [(o['nc'],o['n7']),(o['n7'],o['n30']),(o['n30'],o['n60']),(o['n60'],o['n90'])]]
    return min(s)
def conv(o):
    ratio=o['ru']/o['na'] if (o['na'] and o['ru'] is not None) else 0
    epsf=min(abs((o['nc']-o['n90'])/o['n90']),3.0) if (o['nc'] and o['n90'] and abs(o['n90'])>0.01) else 0
    rb=min(min(o['rg'],0.5)*0.6,0.3) if o['rg'] is not None else 0
    return o['ag']*(1+max(ratio,epsf)+rb)
def elig(o,req1b):
    if is_commodity(o['tk']) or o['ads'] is None or o['ads']<=9 or o['ag'] is None: return False
    if not o['nc'] or o['nc']<=0 or not o['price'] or o['price']<10: return False
    if o['n90'] and o['nc']/o['n90']-1<=0: return False
    if o['ma'] is None or o['price']<=o['ma']: return False
    if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: return False
    if o['rg'] is None or o['rg']<0.10 or o['na'] is None or o['na']<3 or o['ru'] is None or o['ru']<3: return False
    tot=(o['ru'] or 0)+(o['rd'] or 0)
    if tot>0 and (o['rd'] or 0)/tot>0.3: return False
    if o['om'] is not None and o['gm'] is not None and o['om']<0.10 and o['gm']<0.30: return False
    if o['om'] is not None and o['om']<0.05 or minseg(o)<-2: return False
    if req1b and (o['dv'] or 0)<1000: return False
    return True
def ranks(req1b):
    p2top30={}; p2_by={}; score_by={}
    for d in dates:
        eg={tk:o for tk,o in raw[d].items() if elig(o,req1b)}
        cg={tk:conv(o) for tk,o in eg.items()}; v=list(cg.values())
        if len(v)>=2 and np.std(v)>0:
            m,s=np.mean(v),np.std(v); score_by[d]={tk:max(30.,65+(-(x-m)/s)*15) for tk,x in cg.items()}
        else: score_by[d]={tk:65 for tk in cg}
    for idx,d in enumerate(dates):
        rc=dates[max(0,idx-2):idx+1]; w=[0.2,0.3,0.5][-len(rc):] if len(rc)==3 else([0.4,0.6] if len(rc)==2 else[1.])
        wg={}
        for tk in score_by[d]:
            g=0
            for i,dd in enumerate(rc):
                sc=score_by[d].get(tk,30) if dd==d else (score_by[dd].get(tk,30) if tk in p2top30.get(dd,set()) else 30)
                g+=sc*w[i]
            wg[tk]=g
        order=sorted(wg,key=lambda t:-wg[t]); p2_by[d]={tk:i+1 for i,tk in enumerate(order)}; p2top30[d]=set(order[:30])
    return p2_by
p2_1b=ranks(True); p2_full=ranks(False)
D='2026-06-18'
def fmt(tk):
    o=raw[D][tk]; eps=(o['nc']/o['n90']-1)*100 if o['n90'] else 0
    rg=(o['rg'] or 0)*100; pe=o['price']/o['nc'] if o['nc'] else 0
    dv=(o['dv'] or 0)/1000
    nm=NAME.get(tk,tk)[:18]
    return f"{nm}({tk}) · {IND.get(tk,'?')} · EPS전망 +{eps:.0f}% 매출 +{rg:.0f}% · PER{pe:.0f} · 거래대금 ${dv:.1f}B"

print('='*60)
print(f'  {D} — 현행(full) vs $1B-순위 비교')
print('='*60)
print('\n【 현행 full universe Top 13 】 (★=거래량 미달 매수불가)')
ff=sorted(prodp2[D],key=lambda t:prodp2[D][t])[:13]
for tk in ff:
    star='★' if (raw[D][tk]['dv'] or 0)<1000 else ' '
    print(f"  {star}#{prodp2[D][tk]:>2} {fmt(tk)}")

print('\n【 $1B-순위로 바꾸면 Top 13 】 (전부 매수가능)')
bb=sorted(p2_1b[D],key=lambda t:p2_1b[D][t])[:13]
for i,tk in enumerate(bb,1):
    full_r=prodp2[D].get(tk,'-')
    print(f"   #{i:>2} {fmt(tk)}   (현행순위 {full_r})")

print('\n【 사라지는 종목 (현행 Top13 중 거래량 미달) 】')
gone=[tk for tk in ff if (raw[D][tk]['dv'] or 0)<1000]
print('  ', ', '.join(f"{tk}(#{prodp2[D][tk]})" for tk in gone))

# 매수/매도 시그널 비교 (held={BE,NVDA} 가정)
print('\n【 시그널 — 매수 후보 (Top2, 진입필터 통과) 】')
def buys(p2map):
    cands=[]
    for tk,r in sorted(p2map[D].items(),key=lambda x:x[1]):
        if r>5: break
        o=raw[D][tk]
        if minseg(o)<0 or (o['dv'] or 0)<1000: continue
        cands.append(tk)
        if len(cands)>=2: break
    return cands
print('  현행 full:', ', '.join(buys(p2_full)))
print('  $1B-순위 :', ', '.join(buys(p2_1b)))
print('\n【 보유 BE 처리 — exit 기준(H)에 따라 】 (held 가정, PER 104)')
rb=p2_1b[D].get('BE'); rf=p2_full[D].get('BE'); o=raw[D]['BE']; pe=o['price']/o['nc']
print(f"  현행 full (H12, 56종목): BE 순위 {rf} → {'매도' if (rf is None or rf>12) and pe>=PE_HOLD else '보유'}")
print(f"  $1B-순위 BE 순위 = {rb} (16종목 중)")
for H in (3,4,6,10,12):
    act='매도' if (rb is None or rb>H) and pe>=PE_HOLD else '보유'
    tag=' ← 비례 적정(full H12 등가)' if H in (3,4) else (' ← 내가 아까 잘못 쓴 값' if H==12 else '')
    print(f"    $1B H={H}: 순위{rb}>{H}? → {act}{tag}")
