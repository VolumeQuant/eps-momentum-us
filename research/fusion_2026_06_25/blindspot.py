# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; cur=c.cursor()

# A. LITE min_seg=-100 글리치 — 중간 NTM 스냅샷 값 확인
print("=== A. LITE NTM 스냅샷 (min_seg=-100 글리치 원인 추적) ===")
print(f"{'date':10} {'cur':>7} {'7d':>7} {'30d':>7} {'60d':>7} {'90d':>7}  segs")
def seg(new,old):
    if new is None or old is None or abs(old)<0.01: return 0.0
    return max(-100,min(100,(new-old)/abs(old)*100))
for tk in ['LITE','NVDA']:
    print(f"--- {tk} ---")
    for r in cur.execute("SELECT date,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,part2_rank FROM ntm_screening WHERE ticker=? AND date>='2026-06-09' ORDER BY date",(tk,)):
        s1=seg(r['ntm_current'],r['ntm_7d']);s2=seg(r['ntm_7d'],r['ntm_30d'])
        s3=seg(r['ntm_30d'],r['ntm_60d']);s4=seg(r['ntm_60d'],r['ntm_90d'])
        flag='  <-- glitch?' if min(s1,s2,s3,s4)<=-99 else ''
        v=lambda x: f'{x:.2f}' if x is not None else 'None'
        print(f"{r['date']:10} {v(r['ntm_current']):>7} {v(r['ntm_7d']):>7} {v(r['ntm_30d']):>7} {v(r['ntm_60d']):>7} {v(r['ntm_90d']):>7}  [{s1:.0f},{s2:.0f},{s3:.0f},{s4:.0f}] p2={r['part2_rank']}{flag}")

# C. 매수후보 상관(테마 집중) — Top5 업종
print("\n=== C. 매수권 종목 테마 집중도 (06-24 Top8) ===")
for r in cur.execute("SELECT ticker,part2_rank FROM ntm_screening WHERE date='2026-06-24' AND part2_rank<=8 ORDER BY part2_rank"):
    print(f"  p2={r['part2_rank']} {r['ticker']}")

# how many days did KEYS rank #1 but excluded by $1B
print("\n=== D. KEYS(#1 신호) $1B 미달로 매수 제외된 일수 ===")
n=0; tot=0
for r in cur.execute("SELECT date,part2_rank,dollar_volume_30d FROM ntm_screening WHERE ticker='KEYS' AND date>='2026-06-11' AND part2_rank IS NOT NULL ORDER BY date"):
    tot+=1
    if (r['dollar_volume_30d'] or 0)<1000 and r['part2_rank']<=3:
        n+=1
        print(f"  {r['date']} p2={r['part2_rank']} $vol={r['dollar_volume_30d']:.0f}M (<1000)")
print(f"  → {n}/{tot}일 #1~3 신호인데 거래대금 미달로 매수 제외")
