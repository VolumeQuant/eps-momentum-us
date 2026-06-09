# -*- coding: utf-8 -*-
"""v114 + EDA 5개 옵션 통합 BT

옵션 A: 거래량 필터 (threshold 정밀 sweep)
옵션 B: 어닝 서프라이즈 (recent surprise > 20%)
옵션 C: 50일 모멘텀 (가격 > 50일 평균)
옵션 D: EPS revision 가속도 (7d/30d ≥ 50%)
옵션 E: 종합 score 결합
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
import yfinance as yf
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10
WHIPSAW_GAP = -0.10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

# 등장 종목
all_tickers = set()
for d in dates:
    for r in cur.execute('SELECT ticker FROM ntm_screening WHERE date=? AND composite_rank<=30', (d,)):
        all_tickers.add(r[0])
ticker_list = sorted(all_tickers)

# 일별 거래대금 fetch
print(f'fetching market data for {len(all_tickers)} tickers...')
daily_dollar_vol = {}
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

# 어닝 서프라이즈 fetch (각 종목별 최근 분기 surprise)
print('fetching earnings surprises...')
earnings_surprise = {}  # tk -> [(date_str, surprise_pct), ...]
for tk in ticker_list:
    try:
        t = yf.Ticker(tk)
        eh = t.earnings_history
        if eh is not None and not eh.empty and 'surprisePercent' in eh.columns:
            # surprisePercent는 0~1 또는 0~ 큰 값. SNDK 7.71 = 771%
            surprises = []
            for idx, row in eh.iterrows():
                sp = row.get('surprisePercent')
                if pd.notna(sp):
                    surprises.append((str(idx)[:10], float(sp)))
            if surprises:
                earnings_surprise[tk] = surprises
    except Exception:
        pass
print(f'  거래량: {len(daily_dollar_vol)} / 어닝 서프라이즈: {len(earnings_surprise)}')

# EPS revision 가속도 fetch
print('fetching EPS revisions...')
eps_revision_accel = {}  # tk -> 7d/30d ratio
for tk in ticker_list:
    try:
        t = yf.Ticker(tk)
        rev = t.eps_revisions
        if rev is not None and not rev.empty and 'upLast7days' in rev.columns and 'upLast30days' in rev.columns:
            # 0y (올해) 기준
            row = rev.loc['0y'] if '0y' in rev.index else rev.iloc[0]
            up7 = row.get('upLast7days', 0) or 0
            up30 = row.get('upLast30days', 0) or 0
            if up30 > 0:
                eps_revision_accel[tk] = up7 / up30
    except Exception:
        pass
print(f'  EPS revision 데이터: {len(eps_revision_accel)}')


def avg_vol_at(tk, target_date, lookback=30):
    if tk not in daily_dollar_vol: return 0
    dv = daily_dollar_vol[tk]
    sorted_dates = sorted([d for d in dv if d < target_date])
    if len(sorted_dates) < 5: return 0
    recent = sorted_dates[-lookback:]
    return sum(dv[d] for d in recent) / len(recent)


def recent_surprise_at(tk, target_date):
    """target_date 이전 최근 어닝 서프라이즈"""
    if tk not in earnings_surprise: return None
    sps = [s for d, s in earnings_surprise[tk] if d <= target_date]
    return sps[-1] if sps else None


def above_ma50_at(tk, i, all_prices):
    """가격 > 50일 평균"""
    if i < 20: return True
    prices = []
    for j in range(max(0, i-49), i+1):
        p = all_prices[dates[j]].get(tk)
        if p: prices.append(p)
    if len(prices) < 20: return True
    ma50 = sum(prices) / len(prices)
    cur_p = all_prices[dates[i]].get(tk)
    return cur_p > ma50 if cur_p else True


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
    if i < 1: return 0
    cur_p = pf[dates[i]].get(tk); prev_p = pf[dates[i-1]].get(tk)
    if not cur_p or not prev_p: return 0
    return cur_p / prev_p - 1


def sim_v114(config, start=0):
    """config dict:
       vol_thr_M, surprise_thr, ma50_filter, eps_accel_thr
    """
    vol_thr = config.get('vol_thr_M', 0)
    sup_thr = config.get('surprise_thr', 0)
    ma50_flt = config.get('ma50_filter', False)
    eps_accel = config.get('eps_accel_thr', 0)

    held = {}; prev = None; val = 1.0; peak = 1.0; mdd = 0
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
        # 매도
        for tk in list(held):
            info = dd.get(tk); ed, ep, w, grace = held[tk]
            if info and info.get('min_seg', 0) < -2:
                del held[tk]; continue
            if info is None: continue
            p2 = info.get('p2')
            if (p2 is None or p2 > 10) and not above_ma12(tk, i):
                gap = today_gap(tk, i)
                if gap <= WHIPSAW_GAP and not grace:
                    held[tk] = (ed, ep, w, True); continue
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
                # 필터들
                if vol_thr > 0:
                    if avg_vol_at(tk, d, 30) < vol_thr: continue
                if sup_thr > 0:
                    rs = recent_surprise_at(tk, d)
                    if rs is None or rs < sup_thr: continue
                if ma50_flt:
                    if not above_ma50_at(tk, i, pf): continue
                if eps_accel > 0:
                    if eps_revision_accel.get(tk, 0) < eps_accel: continue
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

def run(config):
    cums, mdds = [], []
    for ch in seeds:
        for s in ch:
            c, m = sim_v114(config, start=s)
            cums.append(c); mdds.append(m)
    return cums, mdds

def report(name, config, base_avg=None):
    cums, mdds = run(config)
    avg = statistics.mean(cums); mdd = statistics.mean(mdds)
    cal = avg / abs(mdd) if mdd else 0
    pos = sum(1 for c in cums if c > 0)
    full_c, full_m = sim_v114(config, start=0)
    diff_s = f'{avg-base_avg:>+8.1f}p' if base_avg is not None else '       -'
    print(f'{name:<30}{avg:>+9.1f}%{mdd:>+9.1f}%{cal:>8.2f}{pos:>6}/300 {full_c:>+8.1f}%  {diff_s}')
    return avg, cums

print('\n' + '=' * 110)
print('통합 BT: v114 + EDA 5개 옵션')
print('=' * 110)
print(f'{"variant":<30}{"수익":>10}{"MDD":>9}{"calmar":>8}{"양수":>10}{"full":>10}{"vs base":>11}')
print('-' * 95)

# baseline
base_avg, base_cums = report('v114 baseline', {})

# 옵션 A 정밀 sweep
print('\n[옵션 A] 거래량 정밀 sweep:')
for thr in [700, 800, 1000, 1200, 1500]:
    report(f'A. vol ${thr}M+', {'vol_thr_M': thr}, base_avg)

# 옵션 B
print('\n[옵션 B] 어닝 서프라이즈 threshold:')
for thr in [0.05, 0.10, 0.20, 0.50]:
    report(f'B. surprise>{int(thr*100)}%', {'surprise_thr': thr}, base_avg)

# 옵션 C
print('\n[옵션 C] 50일 모멘텀 (가격>MA50):')
report('C. price > MA50', {'ma50_filter': True}, base_avg)

# 옵션 D
print('\n[옵션 D] EPS revision 가속도:')
for thr in [0.3, 0.5, 0.7]:
    report(f'D. accel>{thr}', {'eps_accel_thr': thr}, base_avg)

# 옵션 E 결합
print('\n[옵션 E] 결합:')
report('A+B (1B + surprise>10%)', {'vol_thr_M': 1000, 'surprise_thr': 0.10}, base_avg)
report('A+C (1B + MA50)', {'vol_thr_M': 1000, 'ma50_filter': True}, base_avg)
report('A+D (1B + accel>0.5)', {'vol_thr_M': 1000, 'eps_accel_thr': 0.5}, base_avg)
report('A+B+C (full ensemble)', {'vol_thr_M': 1000, 'surprise_thr': 0.10, 'ma50_filter': True}, base_avg)

print(f'\n소요 {time.time()-t0:.0f}초')
con.close()
