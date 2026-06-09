# -*- coding: utf-8 -*-
"""거래량 universe filter BT

각 매수 후보의 진입 시점 일평균 거래량 ≥ threshold 필터.
threshold sweep: $200M, $500M, $1B, $2B
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

# 등장 종목 (cr Top 30 + 모든 보유 종목)
all_tickers = set()
for d in dates:
    for r in cur.execute('SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank<=30', (d,)):
        all_tickers.add(r[0])

print(f'전체 등장 종목 (cr Top 30): {len(all_tickers)}개')
print('yfinance에서 6개월 평균 거래량 fetch 중...')

# 6개월 평균 일거래량 ($) 가져오기
volume_avg = {}
batch_size = 50
ticker_list = sorted(all_tickers)
for i in range(0, len(ticker_list), batch_size):
    batch = ticker_list[i:i+batch_size]
    try:
        data = yf.download(' '.join(batch), period='6mo', auto_adjust=False, progress=False, threads=True, group_by='ticker')
        for tk in batch:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    df = data[tk] if tk in data.columns.get_level_values(0) else None
                else:
                    df = data
                if df is not None and not df.empty:
                    avg_dol = (df['Volume'] * df['Close']).mean() / 1e6
                    if not pd.isna(avg_dol):
                        volume_avg[tk] = avg_dol
            except Exception:
                pass
    except Exception as e:
        print(f'  batch {i}: 오류 {str(e)[:50]}')

print(f'거래량 데이터 확보: {len(volume_avg)}/{len(all_tickers)}')

# DB 데이터 로드
data_all = {}
for d in dates:
    data_all[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?', (d,)).fetchall():
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]; fpe = (r[3]/nc) if (r[3] and nc>0) else None
        peg = (fpe/(rg*100)) if (fpe and rg and rg>0) else None
        data_all[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                               min_seg=min(segs) if segs else 0, high30=r[10],
                               peg=peg, rev_growth=rg)
pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        x = data_all[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30: return False
    return True


def above_ma12(tk, i):
    """가격 > MA12 (12일 평균)"""
    if i < 12: return True
    prices = []
    for j in range(max(0, i-11), i+1):
        p = pf[dates[j]].get(tk)
        if p: prices.append(p)
    if len(prices) < 6: return True
    ma12 = sum(prices) / len(prices)
    cur_p = pf[dates[i]].get(tk)
    return cur_p > ma12 if cur_p else True


def sim(vol_threshold_M, start=0):
    """vol_threshold_M: 백만 달러 단위. 0이면 필터 없음 (baseline)"""
    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val)
            mdd = min(mdd, (val/peak - 1) * 100)
        dd = data_all[d]
        # 매도 (v114 MA12 hold)
        for tk in list(held):
            info = dd.get(tk)
            if info is None: continue
            if info.get('min_seg', 0) < -2: del held[tk]; continue
            p2 = info.get('p2')
            if (p2 is None or p2 > 10) and not above_ma12(tk, i):
                del held[tk]
        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if tk in held: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price']/info['high30'] - 1 < -0.25: continue
                p2 = info.get('p2')
                if p2 is None or p2 > 3: continue
                # 거래량 필터
                if vol_threshold_M > 0:
                    vol = volume_avg.get(tk, 0)
                    if vol < vol_threshold_M: continue
                cands.append((p2, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                for _, _, tk in pick[:2]:
                    held[tk] = (d, dd[tk]['price'], 0.5)
            elif len(held) == 0 and len(pick) == 1:
                tk = pick[0][2]
                held[tk] = (d, dd[tk]['price'], 1.0)
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5)
        prev = dict(held)
    return (val-1)*100, mdd


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(threshold):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim(threshold, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


print('\n' + '=' * 90)
print('거래량 universe filter BT — threshold sweep')
print('=' * 90)
print(f'{"threshold":<15}{"수익":>11}{"MDD":>10}{"calmar":>10}{"양수":>9}{"vs baseline":>14}')
print('-' * 70)
baseline_cums, baseline_mdds = run(0)
base_avg = statistics.mean(baseline_cums)
base_mdd = statistics.mean(baseline_mdds)
base_calmar = base_avg / abs(base_mdd) if base_mdd else 0
base_pos = sum(1 for c in baseline_cums if c > 0)
print(f'{"baseline":<15}{base_avg:>+10.1f}%{base_mdd:>+9.1f}%{base_calmar:>10.2f}{base_pos:>6}/300{"":>14}')

for thr in [200, 500, 1000, 2000, 3000]:
    cums, mdds = run(thr)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    calmar = avg / abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    diff = avg - base_avg
    print(f'${thr:>4}M+{"":<8}{avg:>+10.1f}%{mdd:>+9.1f}%{calmar:>10.2f}{pos:>6}/300{diff:>+12.1f}p')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
