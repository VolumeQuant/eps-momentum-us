# -*- coding: utf-8 -*-
"""$1B-순위 버전 미리보기를 개인봇에 발송 (production 무변경, 자체완결).
검증된 w_gap 공식(_bt_1b_universe와 동일, full은 production 10/10 일치)으로 $1B 순위 재산출.
TELEGRAM_BOT_TOKEN / TELEGRAM_PRIVATE_ID 환경변수 필요 (GH Actions secrets)."""
import sqlite3, numpy as np, json, os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')
ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB=os.path.join(ROOT,'eps_momentum_data.db'); PE_HOLD=30.0; EXIT_RANK=10
COMMODITY_IND={'금','귀금속','산업금속','구리','철강','알루미늄','농업','석유가스','석유종합','석유정제','목재',
 'Gold','Other Precious Metals & Mining','Other Industrial Metals & Mining','Copper','Steel','Aluminum',
 'Agricultural Inputs','Oil & Gas E&P','Oil & Gas Integrated','Oil & Gas Refining & Marketing','Lumber & Wood Production'}
COMMODITY_TK={'SQM','ALB'}
cache=json.load(open(os.path.join(ROOT,'ticker_info_cache.json'),encoding='utf-8'))
IND={t:v.get('industry') for t,v in cache.items()}; NAME={t:v.get('shortName',v.get('short_name',t)) for t,v in cache.items()}
def is_comm(t): return t in COMMODITY_TK or IND.get(t) in COMMODITY_IND
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')]
cols='ticker,adj_score,adj_gap,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,price,ma120,ma60,high30,rev_growth,num_analysts,rev_up30,rev_down30,operating_margin,gross_margin,part2_rank,dollar_volume_30d'
raw={}
for d in dates:
    raw[d]={}
    for r in cur.execute(f'SELECT {cols} FROM ntm_screening WHERE date=?',(d,)):
        t=r[0]; raw[d][t]=dict(tk=t,ads=r[1],ag=r[2],nc=r[3],n7=r[4],n30=r[5],n60=r[6],n90=r[7],price=r[8],
            ma=r[9] if r[9] is not None else r[10],h30=r[11],rg=r[12],na=r[13],ru=r[14],rd=r[15],om=r[16],gm=r[17],fp2=r[18],dv=r[19])
con.close()
def minseg(o):
    s=[(a-b)/abs(b)*100 if b and abs(b)>0.01 else 0 for a,b in [(o['nc'],o['n7']),(o['n7'],o['n30']),(o['n30'],o['n60']),(o['n60'],o['n90'])]]
    return min(s)
def conv(o):
    ratio=o['ru']/o['na'] if (o['na'] and o['ru'] is not None) else 0
    epsf=min(abs((o['nc']-o['n90'])/o['n90']),3.0) if (o['nc'] and o['n90'] and abs(o['n90'])>0.01) else 0
    rb=min(min(o['rg'],0.5)*0.6,0.3) if o['rg'] is not None else 0
    return o['ag']*(1+max(ratio,epsf)+rb)
def elig(o):
    if is_comm(o['tk']) or o['ads'] is None or o['ads']<=9 or o['ag'] is None: return False
    if not o['nc'] or o['nc']<=0 or not o['price'] or o['price']<10: return False
    if o['n90'] and o['nc']/o['n90']-1<=0: return False
    if o['ma'] is None or o['price']<=o['ma']: return False
    if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: return False
    if o['rg'] is None or o['rg']<0.10 or o['na'] is None or o['na']<3 or o['ru'] is None or o['ru']<3: return False
    tot=(o['ru'] or 0)+(o['rd'] or 0)
    if tot>0 and (o['rd'] or 0)/tot>0.3: return False
    if o['om'] is not None and o['gm'] is not None and o['om']<0.10 and o['gm']<0.30: return False
    if o['om'] is not None and o['om']<0.05 or minseg(o)<-2: return False
    if (o['dv'] or 0)<1000: return False   # ★ $1B
    return True
# $1B 순위 (시간순 3일가중 penalty)
cr_by={};score_by={};p2_by={};top30={}
for d in dates:
    eg={t:o for t,o in raw[d].items() if elig(o)}
    cr_by[d]={t:i+1 for i,t in enumerate(sorted(eg,key=lambda x:eg[x]['ag']))}
    cg={t:conv(o) for t,o in eg.items()};v=list(cg.values())
    score_by[d]=({t:max(30.,65+(-(x-np.mean(v))/np.std(v))*15) for t,x in cg.items()} if len(v)>=2 and np.std(v)>0 else {t:65 for t in cg})
for idx,d in enumerate(dates):
    rc=dates[max(0,idx-2):idx+1];w=[0.2,0.3,0.5][-len(rc):] if len(rc)==3 else([0.4,0.6] if len(rc)==2 else[1.])
    wg={}
    for t in score_by[d]:
        g=sum((score_by[d].get(t,30) if dd==d else (score_by[dd].get(t,30) if t in top30.get(dd,set()) else 30))*w[i] for i,dd in enumerate(rc))
        wg[t]=g
    order=sorted(wg,key=lambda x:-wg[x]);p2_by[d]={t:i+1 for i,t in enumerate(order)};top30[d]=set(order[:30])
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0 or cr_by[dates[j]].get(t) is None or cr_by[dates[j]].get(t,99)>30: return False
    return True
D=dates[-1];i=len(dates)-1
# 매수 후보 top2
buys=[]
for t,p in sorted(p2_by[D].items(),key=lambda x:x[1]):
    if p>8: break
    o=raw[D][t]
    if minseg(o)<0 or not verified(t,i) or (o['dv'] or 0)<1000: continue
    if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
    buys.append(t)
    if len(buys)>=2: break
def line(t,rank):
    o=raw[D][t];eps=(o['nc']/o['n90']-1)*100 if o['n90'] else 0;rg=(o['rg'] or 0)*100;pe=o['price']/o['nc'] if o['nc'] else 0
    nm=NAME.get(t,t)[:16]
    return f"#{rank} {nm}({t}) · {IND.get(t,'?')}\n    EPS↑{eps:+.0f}% 매출{rg:+.0f}% PER{pe:.0f}"
top=sorted(p2_by[D],key=lambda t:p2_by[D][t])[:12]
# 현행 full Top12 중 사라지는 미달주
ff=sorted([t for t in raw[D] if raw[D][t]['fp2'] is not None],key=lambda t:raw[D][t]['fp2'])[:12]
gone=[t for t in ff if (raw[D][t]['dv'] or 0)<1000]
msg=f"""🧪 <b>[$1B 순위 버전 미리보기]</b> {D}
(테스트 — 매수가능 종목만으로 순위 재산출)

🛒 <b>매수 후보</b> (각 50%)
{chr(10).join(f'{i+1}. {NAME.get(t,t)[:16]}({t})' for i,t in enumerate(buys))}

📋 <b>$1B 순위 Top 12</b> (전부 매수가능)
{chr(10).join(line(t,r+1) for r,t in enumerate(top))}

🚫 <b>현행 대비 사라진 미달주</b> (거래량&lt;$1B)
{', '.join(gone) if gone else '없음'}

ℹ️ 실거래는 현행과 동일(보유 88/88일 일치). 화면만 매수가능 종목으로 정리됨.
이탈선 EXIT_RANK={EXIT_RANK} 적용."""
tok=os.environ.get('TELEGRAM_BOT_TOKEN');pid=os.environ.get('TELEGRAM_PRIVATE_ID')
if not tok or not pid:
    print('시크릿 없음 — 메시지 미리보기만:\n'); print(msg); sys.exit(0)
data=urllib.parse.urlencode({'chat_id':pid,'text':msg,'parse_mode':'HTML'}).encode()
req=urllib.request.Request(f'https://api.telegram.org/bot{tok}/sendMessage',data=data)
r=urllib.request.urlopen(req);print('발송 결과:',r.status); print(msg)
