# -*- coding: utf-8 -*-
"""dv 과거 백필 — dv_full_2026_07_04.parquet(전종목 PIT 재구축, DB 대조 오차 0.00%)로
ntm_screening.dollar_volume_30d의 NULL만 채움 (기존값은 절대 덮어쓰지 않음).
실행 전 DB 파일 백업 필수. 행동변화 0은 dv_regression_snapshot 전/후 비교로 검증."""
import sys, os, sqlite3
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')

conn = sqlite3.connect(dr.DB_PATH)
c = conn.cursor()
before = c.execute('SELECT COUNT(*) FROM ntm_screening WHERE dollar_volume_30d IS NULL').fetchone()[0]
rows = c.execute('SELECT date, ticker FROM ntm_screening WHERE dollar_volume_30d IS NULL').fetchall()
print(f'NULL dv 행: {before}')

filled = 0
skipped = 0
for d, tk in rows:
    if d in DVF.index and tk in DVF.columns:
        v = DVF.loc[d, tk]
        if pd.notna(v):
            c.execute('UPDATE ntm_screening SET dollar_volume_30d=? WHERE date=? AND ticker=? AND dollar_volume_30d IS NULL',
                      (float(v), d, tk))
            filled += 1
            continue
    skipped += 1
conn.commit()
after = c.execute('SELECT COUNT(*) FROM ntm_screening WHERE dollar_volume_30d IS NULL').fetchone()[0]
# 검증: 기존 non-NULL 값이 안 바뀌었는지는 '기존값 덮어쓰기 없음' WHERE 조건으로 보장
mu = c.execute("SELECT date, dollar_volume_30d FROM ntm_screening WHERE ticker='MU' ORDER BY date DESC LIMIT 3").fetchall()
sndk = c.execute("SELECT date, dollar_volume_30d FROM ntm_screening WHERE ticker='SNDK' ORDER BY date DESC LIMIT 3").fetchall()
conn.close()
print(f'백필: {filled} 채움 / {skipped} 데이터없음 → NULL 잔여 {after}')
print('MU 최근 dv($M):', mu)
print('SNDK 최근 dv($M):', sndk)
