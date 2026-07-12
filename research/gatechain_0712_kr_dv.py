# -*- coding: utf-8 -*-
"""안건2 파트E: KR dv $0.3B 적정성 EDA (BT 불가 — 분포·통과종목수·슬리피지 논증만).

KR DB엔 dollar_volume_30d 컬럼이 없어 production(unified_vm_track.kr_candidates)과 동일하게
yf 30일 평균 거래대금을 라이브 계산. ⚠️7/10 수집확대(MA120 사전필터 OFF, ~390종목) 이후 기준.
읽기 전용.
"""
import sys, os, sqlite3
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
KR_DB = 'C:/dev/claude-code/quant_py-main/kr_eps_momentum/eps_momentum_data_kr.db'

conn = sqlite3.connect(KR_DB)
last = conn.execute('SELECT MAX(date) FROM ntm_screening').fetchone()[0]
rows = conn.execute(
    'SELECT ticker,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,market_cap,num_analysts '
    'FROM ntm_screening WHERE date=? AND price IS NOT NULL AND ntm_current>0', (last,)).fetchall()
conn.close()
print(f'KR 최신일 {last}, 유니버스(price+ntm 유효) {len(rows)}종목')

import yfinance as yf
try:
    fx = float(yf.Ticker('KRW=X').fast_info['last_price'])
except Exception:
    fx = 1380.0
print(f'FX = {fx:.0f} KRW/USD')

tks = [r[0] for r in rows]
hist = yf.download(tks, period='3mo', threads=2, progress=False, auto_adjust=False)
dv_usd = {}
for t in tks:
    try:
        cl = hist['Close'][t].dropna()
        vo = hist['Volume'][t].dropna()
        dvk = (cl * vo).tail(30).mean()
        if dvk and dvk > 0:
            dv_usd[t] = float(dvk) / fx / 1e6  # $M
    except Exception:
        pass
print(f'dv 산출 성공 {len(dv_usd)}/{len(tks)}')

vals = np.array(sorted(dv_usd.values(), reverse=True))
print('\n[유니버스 dv 분포 ($M)]')
for q in [99, 95, 90, 75, 50, 25, 10]:
    print(f'  p{q}: ${np.percentile(vals, q):,.0f}M')
print(f'  상위10% 경계(=백분위 등가 스펙): ${np.percentile(vals, 90):,.0f}M')

print('\n[임계별 통과 종목 수 (유니버스 전체)]')
for thr in [100, 150, 300, 600, 1000]:
    n = int((vals >= thr).sum())
    print(f'  dv>=${thr}M: {n}종목 ({n/len(vals)*100:.1f}%)')

# 후보 레벨: unified KR 자격(gap 제외 — DART 미접근 가능성) 통과 후 dv 임계별 후보 수
def _seg(a, b):
    return (a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0

KR_HOLDCO_IND = set()  # unified의 KR_HOLDCO/KR_IND_BLOCK은 티커 셋 — import 시도
try:
    sys.path.insert(0, 'C:/dev/claude-code/eps-momentum-us')
    import unified_vm_track as uv
    HOLD, INDB = uv.KR_HOLDCO, uv.KR_IND_BLOCK
except Exception as e:
    print(f'(unified import 실패 {e} — 블록리스트 없이 진행)')
    HOLD, INDB = set(), set()

elig = []
for tk, p, nc, n7, n30, n60, n90, mc, na in rows:
    if tk in HOLD or tk in INDB:
        continue
    if (na or 0) < 5:
        continue
    if min(_seg(nc, n7), _seg(n7, n30), _seg(n30, n60), _seg(n60, n90)) < 0:
        continue
    if nc <= 0 or (n90 or 0) <= 100:
        continue
    if p / nc > 30:
        continue
    if _seg(nc, n90) <= 0:
        continue
    elig.append((tk, _seg(nc, n90), dv_usd.get(tk), mc))
elig.sort(key=lambda x: -x[1])
print(f'\n[KR 자격+게이트 통과 후보(gap·안전필터 제외) {len(elig)}종목 — dv 임계별 생존]')
for thr in [100, 150, 300, 600]:
    surv = [e for e in elig if e[2] is not None and e[2] >= thr]
    top3 = [(t, round(r, 1)) for t, r, _, _ in surv[:3]]
    print(f'  dv>=${thr}M: {len(surv)}종목 | rev90 top3 {top3}')
# $0.3B 경계 부근 탈락자 (0.15~0.3B)
border = [(t, round(r, 1), round(d, 0), round((mc or 0)/1e12, 1)) for t, r, d, mc in elig
          if d is not None and 150 <= d < 300]
print(f'\n[경계 탈락자 $150M<=dv<$300M]: {border[:15]}')
low = [(t, round(r, 1), round(d, 0)) for t, r, d, mc in elig if d is not None and 100 <= d < 150]
print(f'[$100-150M]: {low[:10]}')

# 슬리피지 논증 수치
print('\n[슬리피지 관점 — EW 20% 포지션 규모 대비]')
for sleeve_kr in [3.7e8, 1.0e9]:  # 퀀트 슬리브 3.7억(권고안 20%) / 10억(공격 시나리오)
    pos_usd = sleeve_kr / fx * 0.2 / 1e6  # $M
    print(f'  슬리브 {sleeve_kr/1e8:.0f}억원 → 포지션 ${pos_usd:.3f}M: '
          + ' '.join(f'dv${thr}M 대비 {pos_usd/thr*100:.3f}%' for thr in [100, 300, 1000]))
print('\n완료.')
