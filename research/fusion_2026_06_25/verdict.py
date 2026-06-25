# -*- coding: utf-8 -*-
import importlib.util, sys
spec=importlib.util.spec_from_file_location("battery", r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\battery.py')
# battery.py prints on import; suppress by redirect
import io, contextlib
buf=io.StringIO()
with contextlib.redirect_stdout(buf):
    bt=importlib.util.module_from_spec(spec); spec.loader.exec_module(bt)
run=bt.run
print("=== 최종판정: CAP별 worst-leave-one-winner-out (프로젝트 표준) ===")
print("기준: winner(MU/SNDK/STX/NVDA) 하나라도 빼서 음수 뒤집히면 = 비robust 기각\n")
winners=['MU','SNDK','STX','NVDA','LITE','AVGO']
print(f"{'CAP':>5} {'전체Δp':>8} {'worst-LOWO':>11} {'(어느 winner)':>14} {'판정':>6}")
for cap in [20,25,30,40]:
    full=run(ROT_CAP=cap)['cum']-run()['cum']
    worst=999; worst_w=None
    for w in winners:
        b=run(ban=(w,))['cum']; r=run(ROT_CAP=cap,ban=(w,))['cum']
        delta=r-b
        if delta<worst: worst=delta; worst_w=w
    verdict='통과' if worst>0 else '기각'
    print(f"{cap:>5} {full:>+8.1f} {worst:>+11.1f} {('-'+worst_w):>14} {verdict:>6}")

print("\n=== 섹터 분산(가드) 단독 효과 — 사용자 '테마집중이 맞다' 직감 측정 ===")
b=run(); g=run(sector_guard=True)
print(f"  집중허용(base): {b['cum']:>7.1f}% MDD {b['mdd']:>6.1f}%")
print(f"  반도체분산가드: {g['cum']:>7.1f}% MDD {g['mdd']:>6.1f}%  → MDD {g['mdd']-b['mdd']:+.1f}p {'악화' if g['mdd']<b['mdd'] else '개선'}")
