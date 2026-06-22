# -*- coding: utf-8 -*-
"""맹점 ①④ 정량: $1B 유니버스 일별 크기 분포 + z-score 노이즈 영향.
1) 일별 eligible 종목수 full vs $1B (min/max/약세구간)
2) 2-3월 조정구간에서 $1B 유니버스 얼마나 쪼그라드나
3) ranking churn: 경계종목 깜빡임으로 part2_rank 변동 (full vs $1B 일별 top10 turnover)
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
src=open('research/_bt_1b_universe.py',encoding='utf-8').read().split('# === baseline')[0]
exec(src)
import statistics as st
crF,p2F=build_ranks(False); cr1,p21=build_ranks(True)

print('=== (1) 일별 eligible 종목수 분포 ===')
szF=[sum(1 for _ in p2F[d]) for d in dates]
sz1=[sum(1 for _ in p21[d]) for d in dates]
print(f'full: min{min(szF)} max{max(szF)} 평균{st.mean(szF):.0f} 중앙{st.median(szF):.0f}')
print(f'$1B : min{min(sz1)} max{max(sz1)} 평균{st.mean(sz1):.0f} 중앙{st.median(sz1):.0f}')

print('\n=== (2) 2-3월 조정구간 $1B 유니버스 크기 ===')
adj=[i for i,d in enumerate(dates) if d[5:7] in ('02','03')]
sz1_adj=[sz1[i] for i in adj]
print(f'조정구간 $1B: min{min(sz1_adj)} max{max(sz1_adj)} 평균{st.mean(sz1_adj):.0f}')
print(f'  → H10 exit가 유니버스 {min(sz1_adj)}종목일 때 의미: 순위>10 이탈조건이 거의 무력(거의 전부 보유범위)')
# H10보다 작은 유니버스 일수
small=sum(1 for s in sz1 if s<=10)
print(f'  유니버스 ≤10종목인 날: {small}/{len(dates)}일 (이 날 H10 exit 사실상 무력)')
print(f'  유니버스 ≤12종목인 날: {sum(1 for s in sz1 if s<=12)}/{len(dates)}일')

print('\n=== (3) ranking churn: top10 일별 turnover (경계 깜빡임) ===')
def turnover(p2_by,k=10):
    ts=[]
    for i in range(1,len(dates)):
        a=set(sorted(p2_by[dates[i-1]],key=lambda t:p2_by[dates[i-1]][t])[:k])
        b=set(sorted(p2_by[dates[i]],key=lambda t:p2_by[dates[i]][t])[:k])
        if a: ts.append(1-len(a&b)/k)
    return st.mean(ts)
print(f'full top10 일평균 turnover: {turnover(p2F):.1%}')
print(f'$1B  top10 일평균 turnover: {turnover(p21):.1%}')

print('\n=== (4) $1B 경계종목($1B 막 넘나드는) 깜빡임 빈도 ===')
# dv가 900~1100 구간(경계)인 종목-일 수
flick=0; total=0
for d in dates:
    for tk,o in raw[d].items():
        if not eligible(o,False): continue  # full 기준 eligible
        dv=o['dv'] or 0; total+=1
        if 800<=dv<1200: flick+=1
print(f'full-eligible 중 dv $0.8~1.2B 경계: {flick}/{total} ({flick/total:.1%}) = 깜빡임 위험 종목-일')
