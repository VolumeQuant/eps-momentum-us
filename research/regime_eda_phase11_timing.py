# -*- coding: utf-8 -*-
"""Phase 11 — 국면 탐지/재진입 타이밍 정밀검증 (26년, S&P500 실제).
각 방어(약세) 에피소드별: 고점대비 탐지지연 / 방어후 피한 추가낙폭 / 재진입시 놓친반등 / 재진입후 성과.
국면 = SPX<MA200(15일확인) OR VIX>36(2일). 데이터 research/regime_assets.parquet."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
A = pd.read_parquet('research/regime_assets.parquet'); A.index = pd.to_datetime(A.index)
spx = A['^GSPC'].dropna(); vix = A['^VIX'].dropna()
idx = spx.index[spx.index >= '1998-01-01']
s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill(); ma = s.rolling(200).mean()

def conf(raw, ne, nx):
    st = False; sd = sb = 0; o = []
    for d in raw.values:
        sd = sd+1 if d else 0; sb = 0 if d else sb+1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        o.append(st)
    return pd.Series(o, index=raw.index)

defense = conf(s < ma, 15, 15) | conf(v > 36, 2, 2)
# 방어 에피소드(연속 True) 추출
ep = []; inb = False
for d in idx:
    if defense[d] and not inb: st = d; inb = True
    if not defense[d] and inb: ep.append((st, prev)); inb = False
    prev = d
if inb: ep.append((st, idx[-1]))

print(f'=== 국면 탐지/재진입 타이밍 (1998~2026, 방어 에피소드 {len(ep)}개) ===')
print('주요 약세장만 (방어중 추가낙폭 5%+ 또는 20일+):\n')
hdr = f'{"방어시작":<12}{"고점대비":>8}{"방어후추가낙폭":>13}{"방어해제":<12}{"바닥후반등놓침":>13}{"재진입후60일":>12}{"평가":>6}'
print(hdr)
rows = []
for st, en in ep:
    seg = s[(s.index >= st) & (s.index <= en)]
    if len(seg) < 2: continue
    # 직전 고점(방어시작 250일 전부터 시작일까지 max)
    pk = s[(s.index >= st - pd.Timedelta(days=400)) & (s.index <= st)].max()
    lag = (s[st]/pk - 1)*100              # 고점대비 이미 떨어진 폭(탐지지연, 음수)
    seglow = seg.min(); lowdt = seg.idxmin()
    avoided = (seglow/s[st] - 1)*100       # 방어 후 추가로 더 빠진 폭(피한 손실, 음수면 보호)
    # 재진입(en 다음날)
    reenter = s[s.index > en]
    miss = post = np.nan
    if len(reenter):
        rd = reenter.index[0]; rlv = reenter.iloc[0]
        miss = (rlv/seglow - 1)*100        # 바닥 대비 재진입까지 반등(놓친 반등, 양수)
        fut = s[(s.index > rd)].head(60)
        if len(fut): post = (fut.iloc[-1]/rlv - 1)*100  # 재진입후 60일 성과
    dur = len(seg)
    major = (avoided <= -5) or (dur >= 20)
    if not major: continue
    ev = '✅보호' if avoided <= -5 else ('⚠️휩쏘' if (not np.isnan(post)) else '·')
    if avoided > -2 and not np.isnan(post): ev = '⚠️휩쏘'   # 거의 안 빠졌는데 방어 = 헛방어
    print(f'{str(st.date()):<12}{lag:>+7.1f}%{avoided:>+12.1f}%{str(en.date()):<12}{(miss if not np.isnan(miss) else 0):>+12.1f}%{(post if not np.isnan(post) else 0):>+11.1f}%{ev:>6}')
print()
print('읽는법: 고점대비=방어 들어갈때 이미 빠진 폭(작을수록 빨리 탐지) / 방어후추가낙폭=그 뒤 더 빠진 폭(클수록 잘 피함)')
print('       바닥후반등놓침=재진입까지 놓친 반등 / 재진입후60일=양수면 재진입 타이밍 good, 음수면 일찍 들어가 또 맞음')
