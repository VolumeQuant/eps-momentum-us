# -*- coding: utf-8 -*-
"""$1B 버전 최종 BT — 현행(full) vs $1B 확정 config 전체 비교.
$1B 확정: $1B종목 중 조건만족 상위2개 매수(E8≈무제한) / 이탈 H10 / 슬롯2 / PE30.
현행 baseline: full E5/H12/S2/PE30. production w_gap 정확재현(full 10/10 검증)."""
import sys; sys.stdout.reconfigure(encoding='utf-8')
src=open('research/_bt_1b_universe.py',encoding='utf-8').read().split('# === baseline')[0]
exec(src)
import statistics as st
crF,p2F=build_ranks(False); cr1,p21=build_ranks(True)
# 검증
d=dates[-1]; mine=set(sorted(p2F[d],key=lambda t:p2F[d][t])[:10]); prod=set(sorted(prod_p2[d],key=lambda t:prod_p2[d][t])[:10])
print(f'[검증] full universe Top10 vs production: {len(mine&prod)}/10\n')
npd=st.mean([sum(1 for _ in p21[d]) for d in dates])
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
def report(label,cr_by,p2_by,E,H,S,req1b):
    full,fmdd=sim(cr_by,p2_by,E,H,S,30,req1b)
    _,_,pavg,pmdd,lowo=metrics(cr_by,p2_by,E,H,S,30,req1b,lowo=True)
    seg=[sim(cr_by,p2_by,E,H,S,30,req1b,(),widx(mm)[0],widx(mm)[-1]+1)[0] for mm in (['02','03'],['04','05'],['06'])]
    return dict(label=label,full=full,fmdd=fmdd,pavg=pavg,pmdd=pmdd,lowo=lowo,seg=seg)
print('='*64)
print(f'  최종 BT — {dates[0]} ~ {dates[-1]} ({len(dates)}일)')
print(f'  현행(full ~56종목) vs $1B(~{npd:.0f}종목)')
print('='*64)
B=report('현행 full (E5/H12/S2)',crF,p2F,5,12,2,False)
V=report('$1B (E8/H10/S2)',cr1,p21,8,10,2,True)
print(f'\n{"지표":<14}{"현행 full":>14}{"$1B 버전":>14}{"차이":>10}')
rows=[('전기간 수익','full','%'),('전MDD','fmdd','%'),('paired(100×3)','pavg','%'),
      ('paired MDD','pmdd','%'),('LOWO 최악','lowo','%')]
for nm,k,u in rows:
    diff=V[k]-B[k]
    print(f'{nm:<14}{B[k]:>+13.1f}%{V[k]:>+13.1f}%{diff:>+9.1f}%p')
print(f'\n{"walk-forward":<14}{"현행":>14}{"$1B":>14}')
for i,nm in enumerate(['2-3월(조정)','4-5월(반등)','6월(이란)']):
    print(f'{nm:<14}{B["seg"][i]:>+13.0f}%{V["seg"][i]:>+13.0f}%')
# 06-18 현재 결정
print('\n=== 06-18 현재 매수/보유 결정 ===')
def decide(p2_by,E,H,req1b):
    d=dates[-1]; o_d=raw[d]; held=set()
    # 매수후보 top2
    cands=[]
    for tk,p in sorted(p2_by[d].items(),key=lambda x:x[1]):
        if p>E: break
        o=o_d[tk]
        if minseg(o)<0 or not o['price'] or not verified(cr_by_map[req1b],tk,len(dates)-1): continue
        if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
        if (o['dv'] or 0)<1000: continue
        cands.append(tk)
        if len(cands)>=2: break
    return cands
cr_by_map={False:crF,True:cr1}
print('  현행 매수후보:', ', '.join(decide(p2F,5,12,False)))
print('  $1B  매수후보:', ', '.join(decide(p21,8,10,True)))
for lbl,p2_by,H in [('현행 full',p2F,12),('$1B',p21,10)]:
    r=p2_by[dates[-1]].get('BE'); o=raw[dates[-1]]['BE']; pe=o['price']/o['nc']
    act='매도' if (r is None or r>H) and pe>=30 else '보유'
    print(f'  {lbl}: BE 순위 {r}(H{H}) PER{pe:.0f} → {act}')
print('\n해석: $1B가 paired·LOWO ≥ 현행이고 MDD 비악화면 = "성과 동등+화면 깨끗+robust↑". 채택가치.')
