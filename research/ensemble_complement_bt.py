# -*- coding: utf-8 -*-
"""앙상블 보완전략 BT (2026-06-13 자율주행, 임시).
본 전략(집중 EPS모멘텀: 진입5/이탈10/50-50/PE<15 hold)의 일별수익 series를 추출,
저상관 보완 슬리브(저베타/mean-reversion/절대가치/채권/다른 horizon)와 합산해
Calmar/Sharpe/MDD 개선 여부 검증. LOWO 포함. DB 가격만 사용(+ 채권만 yf 소량)."""
import sqlite3, statistics as st, math
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(dates)}
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('''SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,
        high30,dollar_volume_30d,beta,operating_margin,free_cashflow,roe,rev_growth,market_cap,num_analysts,rev_up30
        FROM ntm_screening WHERE date=?''',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[4:9]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],min_seg=min(segs),high30=r[9],dv=r[10],
            ntm=nc,beta=r[11],opm=r[12],fcf=r[13],roe=r[14],rg=r[15] or 0,mcap=r[16],na=r[17],up30=r[18])
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def ret_at(tk,i):
    """일 i 종목 tk 전일대비 수익률 (i-1 -> i)."""
    if i<1: return None
    a=pf[dates[i]].get(tk);b=pf[dates[i-1]].get(tk)
    return a/b-1 if a and b else None
def price(tk,i): return pf[dates[i]].get(tk)

# ---------- 본 전략: 일별수익 series ----------
def main_series(exclude=()):
    held={};order=[];rets=[None]*len(dates)
    for i in range(len(dates)):
        d=dates[i];dd=data[d]
        # 수익 계산 (이전 보유로)
        if i>0 and held:
            hs=[t for t in order if t in held]; n=len(hs); w={t:1.0/n for t in hs}
            r=0
            for tk in hs:
                rr=ret_at(tk,i)
                if rr is not None: r+=w[tk]*rr
            rets[i]=r
        # 매도
        for tk in list(held):
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: held.pop(tk); order.remove(tk) if tk in order else None; continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>10): continue
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=15: held.pop(tk); order.remove(tk) if tk in order else None
        # 진입
        if len(held)<2:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=info.get('p2')
                if p2 is None or p2>5: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:2-len(held)]: held[tk]=1; order.append(tk)
    return rets

# ---------- 보완 슬리브들 (롱온리, 동일가중 N종목, M일 보유 리밸런스) ----------
def sleeve_series(selector, topn=5, hold=5, exclude=(), univ_dv=200):
    """selector(i)->정렬된 ticker 리스트(상위 우선). topn 동일가중, hold일마다 리밸런스."""
    held=[];rets=[None]*len(dates);last_rb=-999
    for i in range(len(dates)):
        if i>0 and held:
            r=0;n=len(held)
            for tk in held:
                rr=ret_at(tk,i)
                if rr is not None: r+=rr/n
            rets[i]=r
        if i-last_rb>=hold or not held:
            sel=selector(i,exclude,univ_dv)
            if sel: held=sel[:topn]; last_rb=i
    return rets

def _eligible(i,exclude,univ_dv):
    out=[]
    for tk,info in data[dates[i]].items():
        if tk in exclude or not info['price']: continue
        if (info.get('dv') or 0)<univ_dv: continue
        out.append((tk,info))
    return out

# (a) 저베타 방어 슬리브: beta 낮은 + 흑자(roe>0) 우량
def sel_lowbeta(i,exclude,univ_dv):
    c=[]
    for tk,info in _eligible(i,exclude,univ_dv):
        if info.get('beta') is None: continue
        if (info.get('roe') or -1)<=0: continue
        c.append((info['beta'],tk))
    c.sort(); return [t for _,t in c]

# (b) 단기 mean-reversion: 30일 high 대비 큰 낙폭(과대낙폭) + 흑자 + 충분 분석가
def sel_meanrev(i,exclude,univ_dv):
    c=[]
    for tk,info in _eligible(i,exclude,univ_dv):
        h=info.get('high30')
        if not h or h<=0: continue
        dd=info['price']/h-1
        if dd>=-0.15: continue              # 최소 -15% 이상 낙폭
        if (info.get('roe') or -1)<=0: continue
        if (info.get('na') or 0)<5: continue
        c.append((dd,tk))                   # 더 깊은 낙폭 우선 (오름차순)
    c.sort(); return [t for _,t in c]

# (c) 절대가치: 저 fwd_PE + 양 FCF + roe>0
def sel_value(i,exclude,univ_dv):
    c=[]
    for tk,info in _eligible(i,exclude,univ_dv):
        if info.get('ntm',0)<=0: continue
        pe=info['price']/info['ntm']
        if pe<=0 or pe>=15: continue
        if (info.get('fcf') or -1)<=0: continue
        if (info.get('roe') or -1)<=0: continue
        c.append((pe,tk))
    c.sort(); return [t for _,t in c]

# (e) 다른 horizon 모멘텀: composite_rank(당일 conviction) 상위지만 part2(매매)와 다른 시그널.
#     장기 EPS상향 추세(ntm_current vs ntm_90d 큰 폭)로 정렬 (본전략의 PE압축과 다른 축)
def sel_eps_growth(i,exclude,univ_dv):
    c=[]
    for tk,info in _eligible(i,exclude,univ_dv):
        if info.get('ntm',0)<=0: continue
        if (info.get('na') or 0)<5: continue
        # ntm_90d 대비 상향률 (다른 lookback)
        d90=data[dates[i]].get(tk)
        c.append((-(info.get('rg') or 0),tk))   # 매출성장 높은 순 (별도 팩터)
    c.sort(); return [t for _,t in c]

# ---------- 채권/현금: yfinance 소량 fetch ----------
def bond_series():
    try:
        import yfinance as yf, pandas as pd
        dl=yf.download('IEF',start=dates[0],end='2026-06-13',auto_adjust=True,progress=False,threads=False)
        cl=dl['Close'].dropna()
        m={idx.strftime('%Y-%m-%d'):float(v) for idx,v in zip(cl.index, (cl.values.flatten() if hasattr(cl.values,'flatten') else cl.values))}
        rets=[None]*len(dates)
        for i in range(1,len(dates)):
            a=m.get(dates[i]);b=m.get(dates[i-1])
            if a and b: rets[i]=a/b-1
        cov=sum(1 for x in rets if x is not None)
        return rets,cov
    except Exception as e:
        print('bond fetch err',repr(e)); return [None]*len(dates),0

# ---------- 통계 유틸 ----------
def cum_mdd_sharpe(rets):
    val=1.0;peak=1.0;mdd=0;rr=[r for r in rets if r is not None]
    for r in rets:
        if r is None: continue
        val*=(1+r);peak=max(peak,val);mdd=min(mdd,val/peak-1)
    cum=(val-1)*100; mdd*=100
    mu=st.mean(rr); sd=st.pstdev(rr) if len(rr)>1 else 0
    sharpe=(mu/sd*math.sqrt(252)) if sd>0 else 0
    # 소표본 연율화 왜곡 회피 -> 기간수익/|MDD| (return-to-pain)
    calmar=(cum/abs(mdd)) if mdd<0 else 0
    return cum,mdd,sharpe,calmar
def corr(a,b):
    pairs=[(x,y) for x,y in zip(a,b) if x is not None and y is not None]
    if len(pairs)<3: return float('nan')
    xs=[p[0] for p in pairs];ys=[p[1] for p in pairs]
    mx=st.mean(xs);my=st.mean(ys)
    num=sum((x-mx)*(y-my) for x,y in pairs)
    dx=math.sqrt(sum((x-mx)**2 for x in xs));dy=math.sqrt(sum((y-my)**2 for y in ys))
    return num/(dx*dy) if dx>0 and dy>0 else float('nan')
def blend(a,b,wa):
    out=[None]*len(a)
    for i in range(len(a)):
        x=a[i];y=b[i]
        if x is None and y is None: out[i]=None
        else: out[i]=wa*(x or 0)+(1-wa)*(y or 0)
    return out

# ================= 실행 =================
print(f'BT 기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)\n')
main=main_series()
mc,mm,msh,mcal=cum_mdd_sharpe(main)
print(f'본 전략 단독:  누적{mc:+.1f}%  MDD{mm:+.1f}%  Sharpe{msh:.2f}  Calmar{mcal:.2f}\n')

bond,bcov=bond_series()
print(f'(IEF 커버리지 {bcov}/{len(dates)}일)\n')

sleeves={
 '(a)저베타방어': sleeve_series(sel_lowbeta,topn=5,hold=5),
 '(b)과대낙폭반등': sleeve_series(sel_meanrev,topn=5,hold=5),
 '(c)절대가치저PE': sleeve_series(sel_value,topn=5,hold=10),
 '(e)매출성장모멘텀': sleeve_series(sel_eps_growth,topn=5,hold=10),
 '(d)채권IEF': bond,
}

print(f'{"보완슬리브":<16}{"단독누적":>9}{"단독MDD":>8}{"Sharpe":>7}{"Calmar":>7}{"본전략상관":>9}')
print('-'*60)
for name,s in sleeves.items():
    c,m,sh,cal=cum_mdd_sharpe(s); co=corr(main,s)
    print(f'{name:<16}{c:>+8.1f}%{m:>+7.1f}%{sh:>7.2f}{cal:>7.2f}{co:>+9.2f}')

print(f'\n{"=== 합산 포트폴리오 ===":}')
print(f'{"조합":<24}{"누적":>9}{"MDD":>8}{"Sharpe":>7}{"Calmar":>7}')
print('-'*56)
print(f'{"본100":<24}{mc:>+8.1f}%{mm:>+7.1f}%{msh:>7.2f}{mcal:>7.2f}  ←기준')
for name,s in sleeves.items():
    for wa,lbl in [(0.8,'80/20'),(0.7,'70/30')]:
        bl=blend(main,s,wa);c,m,sh,cal=cum_mdd_sharpe(bl)
        print(f'본{wa*100:.0f}/{name}{lbl:<7}'.ljust(24)+f'{c:>+8.1f}%{m:>+7.1f}%{sh:>7.2f}{cal:>7.2f}')

# ---------- LOWO: 본전략 + best 후보 합산의 robustness ----------
print(f'\n=== LOWO (단일 winner 제외 시 본전략·합산 누적·MDD) ===')
print(f'{"제외":<8}{"본단독":>10}{"본MDD":>8}')
base_c,base_m,_,_=cum_mdd_sharpe(main)
print(f'{"(없음)":<8}{base_c:>+9.1f}%{base_m:>+7.1f}%')
for w in sorted(WINNERS):
    s=main_series(exclude=(w,));c,m,_,_=cum_mdd_sharpe(s)
    print(f'{w:<8}{c:>+9.1f}%{m:>+7.1f}%')

# ---------- 합산(본80/보완20) LOWO: 보완슬리브도 winner 제외하고 재계산 ----------
print(f'\n=== 합산(본80/보완20) LOWO — 본전략·보완 동시 winner 제외 ===')
cand_sel={'(a)저베타방어':sel_lowbeta,'(b)과대낙폭반등':sel_meanrev}
cand_hold={'(a)저베타방어':5,'(b)과대낙폭반등':5}
for name,sel in cand_sel.items():
    print(f'\n[{name} 합산 80/20]')
    print(f'{"제외":<8}{"합산누적":>10}{"합산MDD":>9}{"Calmar":>8}')
    for w in [None]+sorted(WINNERS):
        ex=() if w is None else (w,)
        m_s=main_series(exclude=ex); c_s=sleeve_series(sel,topn=5,hold=cand_hold[name],exclude=ex)
        bl=blend(m_s,c_s,0.8);c,mm2,_,cal=cum_mdd_sharpe(bl)
        lbl='(없음)' if w is None else w
        print(f'{lbl:<8}{c:>+9.1f}%{mm2:>+8.1f}%{cal:>8.2f}')
print('\n해석: 합산이 본전략 대비 Calmar(=기간수익/|MDD|)↑ + MDD 완화면 robust 보완. 상관 낮을수록 분산효과.')
