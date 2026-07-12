# -*- coding: utf-8 -*-
"""적대적 검증: keep-gap-1.5 권고 — gap 임계 plateau/절벽 형상 독립 재실행.
gap_thr in {0,1.0,1.25,1.5,1.75,2.0,2.25,2.5} x phase 0~4 평균 x LOWO(none/SNDK/MU/SNDK+MU).
읽기 전용. 산출: 표 stdout."""
import sys
sys.path.insert(0, r"C:\dev\claude-code\eps-momentum-us\research")
from vm_canonical_bt import canonical_bt

THRS = [0, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5]
EXCLS = [("full", frozenset()), ("exSNDK", frozenset({'SNDK'})),
         ("exMU", frozenset({'MU'})), ("exSM", frozenset({'SNDK', 'MU'}))]
END = None  # 전기간 (DB 마지막일)

def run(thr, exc, end=END):
    rows = []
    for ph in range(5):
        r, m = canonical_bt(pe_max=30, gap_thr=thr, N=5, R=5, dv_min=1000.0,
                            phase=ph, exclude=exc, end_date=end)
        rows.append((r, m))
    avg_r = sum(r for r, _ in rows) / 5
    worst_m = min(m for _, m in rows)
    avg_m = sum(m for _, m in rows) / 5
    return avg_r, avg_m, worst_m, rows

print("=== gap 임계 스윕 (PER30/N5/dv$1B/R5, 위상0~4 평균, 전기간) ===")
print(f"{'thr':>5} | " + " | ".join(f"{nm}: ret/avgMDD" for nm, _ in EXCLS))
detail = {}
for thr in THRS:
    cells = []
    for nm, exc in EXCLS:
        ar, am, wm, rows = run(thr, exc)
        detail[(thr, nm)] = rows
        cells.append(f"{nm} {ar:+7.1f}/{am:6.1f}")
    print(f"{thr:>5} | " + " | ".join(cells))

print("\n=== 위상별 상세 (thr=1.25/1.5/1.75, full & exSM) ===")
for thr in [1.25, 1.5, 1.75]:
    for nm in ["full", "exSM"]:
        rows = detail[(thr, nm)]
        s = " ".join(f"ph{p}:{r:+.1f}/{m:.1f}" for p, (r, m) in enumerate(rows))
        print(f"thr={thr} {nm}: {s}")

print("\n=== 7/08 기준 베이스라인 대조 (권고 evidence: +103.2/-17.7, exSM +67.6) ===")
for nm, exc in [("full", frozenset()), ("exSM", frozenset({'SNDK','MU'}))]:
    ar, am, wm, rows = run(1.5, exc, end='2026-07-08')
    print(f"gap1.5 {nm} @7/08: 위상평균 {ar:+.1f} / avgMDD {am:.1f} / worstMDD {wm:.1f}")
for nm, exc in [("full", frozenset()), ("exSM", frozenset({'SNDK','MU'}))]:
    ar, am, wm, rows = run(1.25, exc, end='2026-07-08')
    print(f"gap1.25 {nm} @7/08: 위상평균 {ar:+.1f} / avgMDD {am:.1f}")
