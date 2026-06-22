# -*- coding: utf-8 -*-
"""LOWO +22.6p가 진짜 robustness인지 작은유니버스 artifact인지 직접 해부.
검증 질문:
 1) LOWO '최악'이 어느 winner 제거에서 나오나? (한 종목 의존?)
 2) winner별 LOWO를 full vs $1B 나란히 — $1B가 모든 winner에서 이기나 특정 종목만?
 3) 핵심 메커니즘 주장 검증: full은 winner 제거 시 슬롯 못채움(미달주가 막아서),
    $1B는 채움 → 직접 '슬롯 충원율' 측정.
 4) H10이 $1B 정점인 게 진짜인지: H 스윕에서 winner별 LOWO 분포 변동성 확인.
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
src=open('research/_bt_1b_universe.py',encoding='utf-8').read().split('# === baseline')[0]
exec(src)
import statistics as st
crF,p2F=build_ranks(False); cr1,p21=build_ranks(True)
cr_map={False:crF,True:cr1}; p2_map={False:p2F,True:p21}

# ---- (1)(2) winner별 LOWO: full vs $1B 나란히 ----
print('=== winner별 paired-LOWO (각 winner 제거 시 paired 평균) ===')
print(f'{"제외종목":<8}{"full":>10}{"$1B":>10}{"차이":>9}')
def paired_excl(req1b,E,H,S,excl):
    cr_by,p2_by=cr_map[req1b],p2_map[req1b]
    return st.mean([sim(cr_by,p2_by,E,H,S,30,req1b,excl,s)[0] for ch in seeds for s in ch])
base_full=paired_excl(False,5,12,2,())
base_1b  =paired_excl(True,3,10,2,())
print(f'{"(없음)":<8}{base_full:>+9.1f}%{base_1b:>+9.1f}%{base_1b-base_full:>+8.1f}%p')
full_worst=base_full; b1_worst=base_1b; full_w_tk=b1_w_tk=None
for w in sorted(WINNERS):
    f=paired_excl(False,5,12,2,(w,)); b=paired_excl(True,3,10,2,(w,))
    if f<full_worst: full_worst=f; full_w_tk=w
    if b<b1_worst: b1_worst=b; b1_w_tk=w
    print(f'{w:<8}{f:>+9.1f}%{b:>+9.1f}%{b-f:>+8.1f}%p')
print(f'\nfull 최악: -{full_w_tk} → {full_worst:+.1f}%')
print(f'$1B  최악: -{b1_w_tk} → {b1_worst:+.1f}%')
print(f'LOWO최악 차이: {b1_worst-full_worst:+.1f}%p (주장 +22.6p)')

# ---- (3) 슬롯 충원율 측정: winner 제거 시 평균 보유 슬롯 수 ----
print('\n=== 슬롯 충원율 (연속운영, winner 제거 시 일평균 보유종목수 / 슬롯2) ===')
def avg_held(req1b,E,H,S,excl):
    cr_by,p2_by=cr_map[req1b],p2_map[req1b]
    held={};prev=None;tot=0;cnt=0
    for i in range(len(dates)):
        d=dates[i]; o_d=raw[d]; p2d=p2_by[d]
        for tk in list(held):
            o=o_d.get(tk)
            if o and minseg(o)<-2: del held[tk];continue
            if o is None: continue
            p2=p2d.get(tk)
            if not (p2 is None or p2>H): continue
            _pe=o['price']/o['nc'] if o.get('nc',0)>0 else 999
            if _pe>=30: del held[tk]
        if len(held)<S:
            cands=[]
            for tk,p2 in p2d.items():
                if tk in held or tk in excl or p2>E: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(cr_by,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]: held[tk]=1.0/S
        tot+=len(held); cnt+=1
    return tot/cnt
print(f'{"제외":<8}{"full보유":>10}{"$1B보유":>10}')
for excl in [(),(full_w_tk,) if full_w_tk else ('MU',),('MU',),('SNDK',)]:
    lab=excl[0] if excl else '(없음)'
    print(f'{lab:<8}{avg_held(False,5,12,2,excl):>9.2f} {avg_held(True,3,10,2,excl):>9.2f}')

# ---- (4) $1B에서 H10이 진짜 정점인지: LOWO최악을 H 스윕, winner별 분산 ----
print('\n=== $1B H스윕: LOWO최악 + 어느 winner가 최악인지 ===')
print(f'{"H":>3}{"paired":>9}{"LOWO최악":>10}{"최악종목":>9}')
for H in (4,6,8,10,12):
    p=paired_excl(True,3,H,2,())
    worst=p; wtk=None
    for w in sorted(WINNERS):
        a=paired_excl(True,3,H,2,(w,))
        if a<worst: worst=a; wtk=w
    print(f'{H:>3}{p:>+8.1f}%{worst:>+9.1f}%{str(wtk):>9}')

# ---- (5) full도 H 재최적화하면? (max-selection 공정성) ----
print('\n=== full universe도 H 재최적화 (max-selection 공정 비교) ===')
print(f'{"H":>3}{"paired":>9}{"LOWO최악":>10}{"최악종목":>9}')
for H in (4,6,8,10,12):
    p=paired_excl(False,5,H,2,())
    worst=p; wtk=None
    for w in sorted(WINNERS):
        a=paired_excl(False,5,H,2,(w,))
        if a<worst: worst=a; wtk=w
    print(f'{H:>3}{p:>+8.1f}%{worst:>+9.1f}%{str(wtk):>9}')
