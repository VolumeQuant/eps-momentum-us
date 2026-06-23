# -*- coding: utf-8 -*-
"""production compute_grace_tickers 검증: 임시DB에 vol_ratio 백필 후 실제 함수 실행.
BT 일치 확인: AEIS/WWD(저볼륨 이탈 반등) 유예 ✅, AMZN(1.41x 고볼륨) 제외 ✅."""
import sqlite3, shutil, warnings, sys, os
warnings.filterwarnings('ignore'); sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, yfinance as yf
shutil.copy('eps_momentum_data.db', 'eps_grace_test.db')
con=sqlite3.connect('eps_grace_test.db');cur=con.cursor()
try: cur.execute('ALTER TABLE ntm_screening ADD COLUMN vol_ratio REAL')
except sqlite3.OperationalError: pass
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')]
tks=[r[0] for r in cur.execute('SELECT DISTINCT ticker FROM ntm_screening WHERE part2_rank IS NOT NULL')]
print(f'vol_ratio 백필: {len(tks)}종목 볼륨 수집...',flush=True)
vol=yf.download(tks,start='2026-01-01',end='2026-06-23',progress=False,auto_adjust=True,threads=2)['Volume']
vol.index=pd.to_datetime(vol.index)
n=0
for tk in tks:
    if tk not in vol.columns: continue
    s=vol[tk].dropna()
    for d in dates:
        idx=s.index[s.index<=pd.Timestamp(d)]
        if len(idx)<21: continue
        j=s.index.get_loc(idx[-1])
        if j<20: continue
        vr=float(s.iloc[j]/s.iloc[j-20:j].mean())
        cur.execute('UPDATE ntm_screening SET vol_ratio=? WHERE ticker=? AND date=?',(vr,tk,d)); n+=1
con.commit()
print(f'  {n}건 백필 완료')
# production 함수 import
sys.path.insert(0,'.')
import daily_runner
print(f'\nproduction compute_grace_tickers 실행 (각 날짜):')
hits={}
for d in dates:
    g=daily_runner.compute_grace_tickers(cur,d)
    if g: hits[d]=g
if not hits:
    print('  유예 발동 0건 (전 기간)')
else:
    for d,g in hits.items(): print(f'  {d}: {sorted(g)}')
con.close(); os.remove('eps_grace_test.db')
allg=set()
for g in hits.values(): allg|=g
print(f'\n유예된 종목(전체): {sorted(allg)}')
print('검증: AEIS/WWD 포함 기대(저볼륨 반등), AMZN 제외 기대(1.41x 고볼륨)')
print('  AEIS: ' + ('OK 유예됨' if 'AEIS' in allg else 'X 누락'))
print('  WWD:  ' + ('OK 유예됨' if 'WWD' in allg else 'X 누락'))
print('  AMZN: ' + ('OK 올바르게 제외(고볼륨)' if 'AMZN' not in allg else 'X 잘못 유예됨'))
