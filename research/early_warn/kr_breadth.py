# -*- coding: utf-8 -*-
"""KR 섹터 브레드스 검증 (부분, yfinance ETF). KRX/pykrx 이 환경서 차단 → ETF로 대체.
⚠️ 한계: KR 섹터ETF는 2007~2011+ 상장(2000닷컴·2008일부만), 섹터 9~13개(KRX 23개보다 거침).
정의적 검증은 KR 레포(KRX 23업종) 필요. 여기선 방향성(브레드스가 KR에 먹히나/35%·2일·50% 적정?)만."""
import sys
import numpy as np
import pandas as pd
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8')
_P = Path(__file__).resolve().parent
CACHE = _P / 'kr_sectors.parquet'

SECTORS = {'091160.KS': '반도체', '091170.KS': '은행', '091180.KS': '자동차', '102960.KS': '기계장비',
           '102970.KS': '증권', '117680.KS': '철강', '140710.KS': '운송', '139260.KS': 'IT소프트',
           '139250.KS': '건설', '244580.KS': '바이오', '266370.KS': 'IT', '228810.KS': '미디어',
           '228800.KS': '여행레저'}
PROXIES = {'^KQ11': 'KOSDAQ', '^KS11': 'KOSPI'}

if CACHE.exists():
    df = pd.read_parquet(CACHE)
    df.index = pd.to_datetime(df.index)
else:
    import yfinance as yf, time
    cols = {}
    for tk in list(SECTORS) + list(PROXIES):
        for _ in range(3):
            try:
                d = yf.download(tk, period='max', auto_adjust=True, progress=False, threads=False)
                cl = d['Close']
                if hasattr(cl, 'columns'):
                    cl = cl.iloc[:, 0]
                cl = cl.dropna()
                if len(cl) > 50:
                    cl.index = pd.to_datetime(cl.index).tz_localize(None)
                    cols[tk] = cl
                    break
            except Exception:
                time.sleep(4)
        time.sleep(1)
    df = pd.DataFrame(cols).sort_index()
    df.to_parquet(CACHE)
    print('cached', CACHE, df.shape)

# 프록시 = KOSDAQ(성장/모멘텀 틸트, KR EPS모멘텀 슬리브 대용), 보조 KOSPI
kq = df['^KQ11'].dropna()
IDX = kq.index
NOTE = (1.03) ** (1 / 252) - 1
qr = kq.pct_change().fillna(0)


def breadth_frac():
    avail = above = None
    for tk in SECTORS:
        if tk not in df:
            continue
        s = df[tk].reindex(IDX).ffill()
        ma200 = s.rolling(200).mean()
        a = (s > ma200); ok = s.notna() & ma200.notna()
        above = a.astype(float).where(ok, 0) if above is None else above.add(a.astype(float).where(ok, 0), fill_value=0)
        avail = ok.astype(float) if avail is None else avail.add(ok.astype(float), fill_value=0)
    return (above / avail.replace(0, np.nan)), avail


frac, avail = breadth_frac()


def conf(raw, ne, nx):
    st = False; sd = sb = 0; o = []
    for d in np.asarray(raw.reindex(IDX).fillna(False).values, dtype=bool):
        sd = sd + 1 if d else 0; sb = 0 if d else sb + 1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        o.append(st)
    return pd.Series(o, index=IDX)


def metrics(thr=0.35, ne=2, nx=15, scale=0.5, a=None, b=None):
    """브레드스<thr 단독 → scale 비중. (KR엔 base 게이트 없이 브레드스만 — 효과 분리 측정)."""
    bdef = conf(frac < thr, ne, nx)
    w = pd.Series(1.0, index=IDX).where(~bdef, scale)
    pos = w.shift(1, fill_value=1.0)
    r = pos * qr + (1 - pos) * NOTE
    if a is not None:
        r = r[(r.index >= a) & (r.index < b)]
    if len(r) < 30:
        return 0, 0, 0, 0
    nav = (1 + r).cumprod(); yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1 / yrs) - 1; mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    trans = int((bdef.astype(int).diff().abs() == 1).sum())
    return cagr * 100, mdd * 100, (cagr / abs(mdd) if mdd < 0 else 0), trans


print(f'데이터: KOSDAQ {IDX[0].date()}~{IDX[-1].date()}, 섹터 {len([t for t in SECTORS if t in df])}개')
print(f'브레드스 산출 시작(섹터≥3): {frac.dropna().index[0].date()}  현재값 {frac.dropna().iloc[-1]*100:.0f}%')
print(f'현재 가용 섹터수: {int(avail.dropna().iloc[-1])}개')

# 1) KR이 만성 협소인가 — 브레드스 분포
fd = frac.dropna()
print(f'\n[1] KR 브레드스 분포: 평균 {fd.mean()*100:.0f}% / 중앙 {fd.median()*100:.0f}% / '
      f'<35% 비율 {(fd<0.35).mean()*100:.0f}% / <45% 비율 {(fd<0.45).mean()*100:.0f}%')

# 2) Buy&Hold vs 브레드스(임계×스케일)
print('\n[2] KOSDAQ Buy&Hold vs 브레드스 스케일 (전체구간)')
bh_nav = (1 + qr).cumprod(); yrs = (IDX[-1] - IDX[0]).days / 365.25
bh_c = bh_nav.iloc[-1] ** (1 / yrs) - 1; bh_m = ((bh_nav - bh_nav.cummax()) / bh_nav.cummax()).min()
print(f'  Buy&Hold KOSDAQ: CAGR {bh_c*100:+.1f} MDD {bh_m*100:+.1f} Calmar {bh_c/abs(bh_m):.2f}')
print(f'{"thr×scale":>14}{"CAGR%":>8}{"MDD%":>8}{"Cal":>6}{"전환":>5}')
for thr in [0.25, 0.30, 0.35, 0.45]:
    for sc in [0.0, 0.5]:
        c, m, cal, tr = metrics(thr, 2, 15, sc)
        print(f'  thr{int(thr*100)}/sc{int(sc*100):>3}{c:>8.1f}{m:>8.1f}{cal:>6.2f}{tr:>5}')

# 3) 확인일 민감도 (thr35, scale50)
print('\n[3] 확인일(ne) — thr35/scale50')
for ne in [1, 2, 3, 5]:
    c, m, cal, tr = metrics(0.35, ne, 15, 0.5)
    print(f'  ne={ne}: Cal {cal:.2f} MDD {m:+.1f} 전환 {tr}')

# 4) 약세장별 MDD (KOSDAQ 자체 vs 브레드스 thr35/sc50)
print('\n[4] 약세장별 KOSDAQ MDD (자체 vs 브레드스 thr35/scale50)')
BEARS = {'2008GFC(부분)': ('2008-05-01', '2009-03-01'), '2011유럽': ('2011-05-01', '2011-10-01'),
         '2018': ('2018-01-29', '2019-01-04'), '2020코로나': ('2020-02-17', '2020-03-19'),
         '2021-22': ('2021-08-01', '2022-10-01'), '2024-25': ('2024-07-01', '2025-04-30')}
def bear_mdd(thr, sc, a, b):
    seg = IDX[(IDX >= a) & (IDX < b)]
    if len(seg) < 5: return None, None
    raw = (df['^KQ11'].reindex(seg)); q = raw / raw.cummax() - 1
    bdef = conf(frac < thr, 2, 15)
    w = pd.Series(1.0, index=IDX).where(~bdef, sc).shift(1, fill_value=1.0)
    r = (w * qr + (1 - w) * NOTE).reindex(seg)
    nav = (1 + r).cumprod()
    return q.min() * 100, ((nav - nav.cummax()) / nav.cummax()).min() * 100
for nm, (a, b) in BEARS.items():
    raw_dd, br_dd = bear_mdd(0.35, 0.5, a, b)
    if raw_dd is not None:
        print(f'  {nm:<14} KOSDAQ {raw_dd:+.1f}%  →  브레드스 {br_dd:+.1f}%')
