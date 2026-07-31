# -*- coding: utf-8 -*-
"""dv 게이트 EDA — 변수 자체가 맞는지 (2026-07-30, HPE 탈락 계기).
Q1. dv 30일 '평균'이 스파이크에 얼마나 흔들리나 (vs 중앙값)
Q2. $1B 게이트가 얼마나 자주 뒤집히나 (게이트 노이즈)
"""
import sys, os, json, sqlite3
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

dv = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
dv = dv.dropna(how='all', axis=1)
mean30 = dv.rolling(30, min_periods=20).mean()
med30 = dv.rolling(30, min_periods=20).median()

# 유효 구간만 (2026-01 이후)
m, md = mean30.loc['2026-01-02':], med30.loc['2026-01-02':]
print('=== Q1. 30일 평균 vs 중앙값 ===')
print('종목수 %d, 일수 %d' % (m.shape[1], m.shape[0]))
ratio = (m / md).replace([np.inf, -np.inf], np.nan)
print('평균/중앙값 비율 분위:', {q: round(float(np.nanpercentile(ratio.values, q)), 3)
                                 for q in (50, 75, 90, 95, 99)})
infl = (ratio > 1.25).sum().sum() / np.isfinite(ratio.values).sum()
print('평균이 중앙값보다 25%%+ 부풀려진 관측 비율: %.1f%%' % (infl * 100))

print('\n=== Q2. $1B 게이트 뒤집힘 (교차 횟수) ===')
def crossings(fr):
    ok = (fr >= 1000.0)
    valid = fr.notna()
    fl = ((ok != ok.shift(1)) & valid & valid.shift(1)).sum()
    return fl
cm, cmd = crossings(m), crossings(md)
# 게이트 경계를 실제로 오가는 종목만 (한 번이라도 양쪽 경험)
straddle = [t for t in m.columns
            if m[t].notna().sum() > 50 and (m[t] >= 1000).any() and (m[t] < 1000).any()]
print('경계를 오간 종목 수: %d / %d' % (len(straddle), m.shape[1]))
print('그 중 평균기준 총 교차: %d회, 중앙값기준: %d회' % (cm[straddle].sum(), cmd[straddle].sum()))
print('종목당 평균 교차: 평균 %.2f회 vs 중앙값 %.2f회' %
      (cm[straddle].mean(), cmd[straddle].mean()))

print('\n=== Q3. 평균 vs 중앙값으로 판정이 갈리는 관측 ===')
both = m.notna() & md.notna()
disagree = ((m >= 1000) != (md >= 1000)) & both
print('판정 불일치 비율: %.2f%% (%d / %d)' %
      (disagree.sum().sum() / both.sum().sum() * 100, disagree.sum().sum(), both.sum().sum()))
top = disagree.sum().sort_values(ascending=False).head(12)
print('불일치 최다 종목(일수):'); print(top.to_string())

print('\n=== Q4. HPE 궤적 (평균 vs 중앙값) ===')
if 'HPE' in m.columns:
    h = pd.DataFrame({'mean30': m['HPE'], 'med30': md['HPE']}).dropna()
    print(h.iloc[::7].round(1).to_string())
