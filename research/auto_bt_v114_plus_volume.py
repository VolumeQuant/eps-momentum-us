# -*- coding: utf-8 -*-
"""v114 (MA12 추세홀드 + 휩쏘 보험) + $1B 거래량 필터 결합 BT

비교:
- baseline: v114 그대로
- v114 + $1B 필터: 매수 시 $1B+ 일평균 거래대금만
- v114 + $500M, $2B 필터: threshold sweep
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10
WHIPSAW_GUARD_GAP = -0.10  # v115 휩쏘 보험: -10% gap

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

# 등장 종목 (cr Top 30)
all_tickers = set()
for d in dates:
    for r in cur.execute('SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank<=30', (d,)):
        all_tickers.add(r[0])

# 일별 거래대금 fetch
print(f'전체 등장 종목: {len(all_tickers)}개 — 일별 거래대금 fetch...')
daily_dollar_vol = {}
ticker_list = sorted(all_tickers)
batch_size = 50
for i in range(0, len(ticker_list), batch_size):
    batch = ticker_list[i:i+batch_size]
    try:
        data = yf.download(' '.join(batch), start='2025-08-01', end='2026-06-10',
                          auto_adjust=False, progress=False, threads=True, group_by='ticker')
        for tk in batch:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    df = data[tk] if tk in data.columns.get_level_values(0) else None
                else:
                    df = data
                if df is not None and not df.empty:
                    dv = (df['Volume'] * df['Close']) / 1e6
                    daily_dollar_vol[tk] = {d.strftime('%Y-%m-%d'): v
                                            for d, v in zip(df.index, dv.values) if not pd.isna(v)}
            except Exception:
                pass
    except Exception:
        pass
print(f'  데이터 확보: {len(daily_dollar_vol)}/{len(all_tickers)}')


def avg_vol_at(tk, target_date, lookback=30):
    if tk not in daily_dollar_vol: return 0
    dv = daily_dollar_vol[tk]
    sorted_dates = sorted([d for d in dv if d < target_date])
    if len(sorted_dates) < 5: return 0
    recent = sorted_dates[-lookback:]
    return sum(dv[d] for d in recent) / len(recent)


# DB 데이터
data_all = {}
for d in dates:
    data_all[d] = {}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?', (d,)).fetchall():
        tk = r[0]; nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a-b)/abs(b)*100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]
        data_all[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                               min_seg=min(segs) if segs else 0, high30=r[10], rev_growth=rg)
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
    if i < 6: return True
    prices = []
    for j in range(max(0, i-11), i+1):
        p = pf[dates[j]].get(tk)
        if p: prices.append(p)
    if len(prices) < 6: return True
    ma12 = sum(prices) / len(prices)
    cur_p = pf[dates[i]].get(tk)
    return cur_p > ma12 if cur_p else True


def today_gap(tk, i):
    """오늘 gap (어제 종가 대비 오늘 종가)"""
    if i < 1: return 0
    cur_p = pf[dates[i]].get(tk)
    prev_p = pf[dates[i-1]].get(tk)
    if not cur_p or not prev_p: return 0
    return cur_p / prev_p - 1


def sim_v114(vol_threshold_M, start=0):
    """v114 (MA12 추세홀드 + EPS꺾임 + 휩쏘 보험) + 거래량 필터"""
    held = {}  # tk -> (entry_date, entry_price, weight, whipsaw_grace_used)
    prev = None; val = 1.0; peak = 1.0; mdd = 0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w, _) in prev.items():
                pp = pf[dp].get(tk); pn = pf[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret); peak = max(peak, val)
            mdd = min(mdd, (val/peak - 1) * 100)
        dd = data_all[d]
        # 매도 (v114)
        for tk in list(held):
            info = dd.get(tk)
            ed, ep, w, grace = held[tk]
            # EPS꺾임 즉시매도
            if info and info.get('min_seg', 0) < -2:
                del held[tk]; continue
            # 데이터 없으면 carryover
            if info is None: continue
            p2 = info.get('p2')
            rank_out = (p2 is None or p2 > 10)
            below_ma = not above_ma12(tk, i)
            if rank_out and below_ma:
                # v115 휩쏘 보험: 하루 -10%+ gap + MA12 깬 첫날 → 1일 유예
                gap = today_gap(tk, i)
                if gap <= WHIPSAW_GUARD_GAP and not grace:
                    held[tk] = (ed, ep, w, True)  # grace 사용 표시
                    continue
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
                    vol = avg_vol_at(tk, d, lookback=30)
                    if vol < vol_threshold_M: continue
                cands.append((p2, info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                for _, _, tk in pick[:2]:
                    held[tk] = (d, dd[tk]['price'], 0.5, False)
            elif len(held) == 0 and len(pick) == 1:
                tk = pick[0][2]
                held[tk] = (d, dd[tk]['price'], 1.0, False)
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5, False)
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
            c, m = sim_v114(threshold, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds


print('\n' + '=' * 100)
print('v114 (production) + 거래량 필터 결합 BT')
print('=' * 100)
print(f'{"variant":<22}{"수익":>11}{"MDD":>10}{"calmar":>10}{"양수":>10}{"vs v114":>14}')
print('-' * 80)
base_c, base_m = run(0)
base_avg = statistics.mean(base_c); base_mdd = statistics.mean(base_m)
base_cal = base_avg / abs(base_mdd) if base_mdd else 0
print(f'{"v114 baseline":<22}{base_avg:>+10.1f}%{base_mdd:>+9.1f}%{base_cal:>10.2f}{sum(1 for c in base_c if c > 0):>6}/300{"":>14}')

results = {0: (base_c, base_m, base_avg, base_mdd, base_cal)}
for thr in [500, 1000, 2000, 5000]:
    cums, mdds = run(thr)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg / abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    results[thr] = (cums, mdds, avg, mdd, cal)
    print(f'v114 + ${thr}M+{"":<10}{avg:>+10.1f}%{mdd:>+9.1f}%{cal:>10.2f}{pos:>6}/300{avg-base_avg:>+12.1f}p')

print()
print('paired diff (vs v114 baseline) — wins')
for thr in [500, 1000, 2000, 5000]:
    diffs = [a-b for a, b in zip(results[thr][0], base_c)]
    avg_d = statistics.mean(diffs)
    wins = sum(1 for d in diffs if d > 0)
    print(f'  ${thr}M+: avg {avg_d:+.1f}p, wins {wins}/{len(diffs)}')

# Full period (start=0)
print()
print('Full period (start=0):')
for thr in [0, 500, 1000, 2000, 5000]:
    c, m = sim_v114(thr, start=0)
    print(f'  ${thr}M: cum {c:+.1f}% / MDD {m:.1f}%')

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
