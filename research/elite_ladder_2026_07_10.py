# -*- coding: utf-8 -*-
"""US 분모를 거래대금 상위 N 엘리트로 좁혀가는 사다리 — 사용자 가설: 'KR 202 = 엘리트니까
US도 상위 200과 비교해야'. KR 분모는 202 고정, US만 100/121/200/320/500/800/전체."""
import os, sys, sqlite3, statistics
sys.stdout.reconfigure(encoding='utf-8')
BASE = r'C:/dev/claude code/eps-momentum-us/.claude/worktrees/gap-backsolve-findings'
sys.path.insert(0, BASE)
os.chdir(BASE)
import unified_vm_track as u

# US 유니온(30일) — 종목별 최신 (rev90, dv)
c = sqlite3.connect(os.path.join(BASE, 'eps_momentum_data.db'))
dt = c.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
rows = c.execute(
    'SELECT ticker, ntm_current, ntm_90d, dollar_volume_30d FROM ntm_screening '
    "WHERE date>=date(?, '-30 day') AND ntm_current>0 AND ntm_90d>0.1 ORDER BY date", (dt,)).fetchall()
c.close()
latest = {}
for tk, nc, n90, dv in rows:
    latest[tk] = ((nc - n90) / abs(n90) * 100, dv or 0)
us_by_dv = sorted(latest.values(), key=lambda x: -x[1])  # 거래대금 내림차순

ukr = u._universe_rev90(os.path.join(BASE, 'research', 'kr_db_snapshot_2026_07_09.db'), n90_floor=100.0)
mkr = statistics.median(ukr)

cands_us = [('SNDK', 165.2), ('MU', 67.9), ('HPE', 49.2), ('FLEX', 41.8), ('MCHP', 27.4)]
cands_kr = [('하이닉스', 82.73), ('삼성', 63.82), ('이노텍', 50.65)]
kr_pct = {t: sum(1 for v in ukr if v < r) / len(ukr) * 100 for t, r in cands_kr}

print(f'KR 분모 고정: 202개, 중앙값 {mkr:+.1f}% | 하이닉스 {kr_pct["하이닉스"]:.1f} 삼성 {kr_pct["삼성"]:.1f} 이노텍 {kr_pct["이노텍"]:.1f}')
print(f'(참고: 한국 상장 ~2,600개 중 202개 = 상위 ~8% / 미국 상장 ~4,600개)')
print()
print(f"{'US 분모':16}{'중앙값':>7}{'KR/US배율':>9} | {'SNDK':>6}{'MU':>6}{'HPE':>6}{'FLEX':>6}{'MCHP':>6} | top5 (삼성?)")
for n in (100, 121, 200, 320, 500, 800, len(us_by_dv)):
    base = [v[0] for v in us_by_dv[:n]]
    med = statistics.median(base)
    us_pct = {t: sum(1 for v in base if v < r) / len(base) * 100 for t, r in cands_us}
    merged = sorted(list(us_pct.items()) + list(kr_pct.items()), key=lambda x: -x[1])
    top5 = [t for t, _ in merged[:5]]
    label = f'상위 {n}' if n < len(us_by_dv) else f'전체 {n}'
    print(f'{label:16}{med:>+7.1f}{mkr/med if med>0 else float("nan"):>8.1f}x | '
          + ''.join(f'{us_pct[t]:>6.1f}' for t, _ in cands_us)
          + f' | {top5} {"삼성IN" if "삼성" in top5 else "삼성out"}')
