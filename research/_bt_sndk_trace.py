# -*- coding: utf-8 -*-
"""$1B H10 우위 = SNDK 보유경로 artifact 확정.
1) SNDK 순위궤적: full vs $1B에서 part2_rank가 H10/H12 경계 어떻게 넘나
2) H스윕에서 SNDK 보유일수 (H10에서만 길어지나)
3) H10 LOWO최악 비단조(+95→+114→+93) = 노이즈인지 winner제거 paired std로 판정
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
src=open('research/_bt_1b_universe.py',encoding='utf-8').read().split('# === baseline')[0]
exec(src)
import statistics as st
crF,p2F=build_ranks(False); cr1,p21=build_ranks(True)

# (1) SNDK part2_rank 궤적 full vs $1B
print('=== SNDK part2_rank 궤적 (full vs $1B), H10/H12 경계 ===')
print(f'{"date":<12}{"full_p2":>8}{"$1B_p2":>8}{"PER":>6}')
shown=0
for d in dates:
    rf=p2F[d].get('SNDK'); r1=p21[d].get('SNDK')
    if rf is None and r1 is None: continue
    o=raw[d].get('SNDK'); pe=(o['price']/o['nc']) if o and o.get('nc',0)>0 else None
    # 경계 근처(8~13)만 표시
    if (rf and 8<=rf<=14) or (r1 and 8<=r1<=14):
        print(f'{d:<12}{str(rf):>8}{str(r1):>8}{(f"{pe:.0f}" if pe else "-"):>6}')
        shown+=1
print(f'(경계 8~14 구간 {shown}일)')

# (2) H스윕 SNDK 보유일수
def sndk_held_days(req1b,p2_by,cr_by,E,H,S):
    held={};prev=None;days=0
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
                if tk in held or p2>E: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(cr_by,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]: held[tk]=1.0/S
        if 'SNDK' in held: days+=1
        prev=dict(held)
    return days
print('\n=== H스윕 SNDK 보유일수 (full vs $1B) ===')
print(f'{"H":>3}{"full":>7}{"$1B":>7}')
for H in (4,6,8,10,12):
    print(f'{H:>3}{sndk_held_days(False,p2F,crF,5,H,2):>7}{sndk_held_days(True,p21,cr1,3,H,2):>7}')

# (3) H10 LOWO최악(SNDK제외) paired 분포 std — 노이즈 판정
print('\n=== $1B H8/H10/H12, SNDK제외 paired: 평균±std (노이즈 판정) ===')
for H in (8,10,12):
    vals=[sim(cr1,p21,3,H,2,30,True,('SNDK',),s)[0] for ch in seeds for s in ch]
    print(f'  H{H}: {st.mean(vals):+.1f}% ± {st.pstdev(vals):.1f} (n={len(vals)})')
print('\n해석: H10만 SNDK보유 길고 인접H와 std범위 겹치면 = artifact/노이즈.')
