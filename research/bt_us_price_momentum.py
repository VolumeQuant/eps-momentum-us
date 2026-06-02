# -*- coding: utf-8 -*-
"""US 가격 모멘텀 진입 필터 BT — v84 production simulator + 100×3 paired

사용자 불만: AEIS/WMG/FIVE처럼 순위만 차지하고 가격은 안 가는 종목.
rev_growth 25% 필터는 이걸 못 잡음 (AEIS 26% 통과). 진짜 lever = 가격 모멘텀.

진입 시점에 가격 모멘텀 게이트 추가:
  - mom20>0 : 최근 20거래일 수익률 > 0
  - mom10>0 : 최근 10거래일 수익률 > 0
  - >MA20   : 현재가 > 20일 이동평균
  - >MA20&mom20>0 : 둘 다

나머지 룰 동일: slot2, entry≤3, exit>10, min_seg≥0, ✅3일, dd_30_25, 2step_t15.
가격 시계열은 전체 79 거래일 캘린더 사용 (part2 75일과 별개).
"""
import sys, sqlite3, random, statistics, math
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
N_SEEDS = 100
SAMPLES_PER_SEED = 3
MIN_HOLD_DAYS = 10


def load_data():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    # part2 dates (매매 가능일)
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    data = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, composite_rank, price, score,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, high30
            FROM ntm_screening WHERE date=?''', (d,)).fetchall()
        data[d] = {}
        for r in rows:
            tk = r[0]
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a-b)/abs(b)*100)))
                else: segs.append(0)
            data[d][tk] = {
                'p2': r[1], 'cr': r[2], 'price': r[3], 'score': r[4] or 0,
                'min_seg': min(segs) if segs else 0, 'high30': r[10],
            }
    # 전체 가격 캘린더 (79일)
    all_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
    didx = {d: i for i, d in enumerate(all_dates)}
    px = defaultdict(dict)  # ticker -> {date: price}
    for tk, d, p in cur.execute(
        'SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        px[tk][d] = p
    price_full = defaultdict(dict)
    for tk in px:
        for d, p in px[tk].items():
            price_full[d][tk] = p
    conn.close()
    return dates, data, price_full, all_dates, didx, px


def mom_n(tk, d, n, all_dates, didx, px):
    """최근 n 거래일 수익률. 데이터 부족시 None."""
    i = didx.get(d)
    if i is None or i - n < 0: return None
    p_now = px[tk].get(d)
    p_ref = px[tk].get(all_dates[i-n])
    if not p_now or not p_ref: return None
    return p_now / p_ref - 1


def ma_n(tk, d, n, all_dates, didx, px):
    """최근 n 거래일 종가 평균. 데이터 부족시 None."""
    i = didx.get(d)
    if i is None or i - n + 1 < 0: return None
    vals = []
    for j in range(i-n+1, i+1):
        v = px[tk].get(all_dates[j])
        if v: vals.append(v)
    if len(vals) < max(2, n//2): return None
    return sum(vals)/len(vals)


def pass_momentum(tk, d, filt, all_dates, didx, px):
    """필터 통과 여부. 데이터 부족(초기 구간)시 통과(pass-through) — 보수적."""
    if filt == 'none':
        return True
    if filt == 'mom20>0':
        m = mom_n(tk, d, 20, all_dates, didx, px)
        return True if m is None else m > 0
    if filt == 'mom10>0':
        m = mom_n(tk, d, 10, all_dates, didx, px)
        return True if m is None else m > 0
    if filt.startswith('>MA'):
        n = int(filt[3:])
        ma = ma_n(tk, d, n, all_dates, didx, px)
        p = px[tk].get(d)
        if ma is None or not p: return True
        return p > ma
    if filt == '>MA20&mom20>0':
        ma = ma_n(tk, d, 20, all_dates, didx, px)
        p = px[tk].get(d)
        m = mom_n(tk, d, 20, all_dates, didx, px)
        ok_ma = True if (ma is None or not p) else p > ma
        ok_m = True if m is None else m > 0
        return ok_ma and ok_m
    return True


def verified_cr(t, i, dates, data):
    for j in (i, i-1, i-2):
        if j < 0: return False
        info = data[dates[j]].get(t)
        if not info or info.get('cr') is None or info['cr'] > 30: return False
    return True


def simulate(dates, data, price_full, all_dates, didx, px,
             filt='none', slots=2, entry=3, exit_=10, start_idx=0,
             use_dd_30_25=True, use_2step=True, exclude=()):
    held = {}
    prev_held = None
    value = 1.0; peak = 1.0; mdd = 0.0
    daily_rets = []; trades_log = []

    for i in range(start_idx, len(dates)):
        d = dates[i]
        if prev_held and i > start_idx:
            d_prev = dates[i-1]
            ret = 0
            for tk, (ed, ep, sidx, w) in prev_held.items():
                pp = price_full[d_prev].get(tk); pn = price_full[d].get(tk, pp)
                if pp and pn: ret += w * (pn/pp - 1)
            value *= (1 + ret); daily_rets.append(ret)
            peak = max(peak, value); mdd = max(mdd, (peak-value)/peak)

        dd = data[d]
        # Exits
        for tk in list(held):
            info = dd.get(tk)
            ep = held[tk][1]
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > exit_:
                sell_p = (info.get('price') if info else None) or price_full[d].get(tk, ep)
                trades_log.append({'ticker': tk, 'buy_date': held[tk][0], 'sell_date': d,
                                   'ret_pct': (sell_p/ep-1)*100})
                del held[tk]
            elif info.get('min_seg') is not None and info['min_seg'] < -2:
                sell_p = info['price'] or ep
                trades_log.append({'ticker': tk, 'buy_date': held[tk][0], 'sell_date': d,
                                   'ret_pct': (sell_p/ep-1)*100})
                del held[tk]

        # Entries
        if len(held) < slots:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > entry: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg') is not None and info['min_seg'] < 0: continue
                if not info['price']: continue
                if not verified_cr(tk, i, dates, data): continue
                if use_dd_30_25 and info.get('high30') and info['price']:
                    if info['price']/info['high30'] - 1 < -0.25: continue
                # 가격 모멘텀 게이트
                if not pass_momentum(tk, d, filt, all_dates, didx, px): continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            picked = cands[:slots]
            if len(picked) == 1:
                _, _, tk = picked[0]
                held[tk] = (d, dd[tk]['price'], 0, 1.0)
            elif len(picked) >= 2:
                if use_2step:
                    s1, s2 = picked[0][1], picked[1][1]
                    weights = [1.0, 0.0] if (s1-s2) >= 15 else [0.5, 0.5]
                else:
                    weights = [0.5, 0.5]
                for idx_s, (_, _, tk) in enumerate(picked[:2]):
                    if weights[idx_s] > 0:
                        held[tk] = (d, dd[tk]['price'], idx_s, weights[idx_s])
        prev_held = dict(held)

    return {'cum': (value-1)*100, 'mdd': mdd*100, 'trades': trades_log, 'daily_rets': daily_rets}


def main():
    print('=' * 100)
    print('US 가격 모멘텀 진입 필터 paired BT (v84 simulator + 100×3)')
    print('=' * 100)
    dates, data, price_full, all_dates, didx, px = load_data()
    print(f'part2 dates: {len(dates)} ({dates[0]}~{dates[-1]})  |  price calendar: {len(all_dates)}일')

    eligible = dates[:-MIN_HOLD_DAYS]
    seed_starts = []
    for s in range(N_SEEDS):
        random.seed(s)
        seed_starts.append(random.sample(range(len(eligible)), SAMPLES_PER_SEED))

    variants = ['none', '>MA10', '>MA15', '>MA20', '>MA25', '>MA30', '>MA40', 'mom10>0']
    print('\n--- Paired BT 결과 ---')
    print(f'{"filter":<18}{"avg":>10}{"med":>9}{"mdd":>9}{"sharpe":>9}{"cal":>8}')
    print('-' * 70)
    all_results = {}
    for filt in variants:
        all_rets, all_mdds, seed_avgs = [], [], []
        for chosen in seed_starts:
            sr = []
            for s_idx in chosen:
                r = simulate(dates, data, price_full, all_dates, didx, px,
                             filt=filt, start_idx=s_idx)
                all_rets.append(r['cum']); all_mdds.append(r['mdd']); sr.append(r['cum'])
            seed_avgs.append(sum(sr)/len(sr))
        all_results[filt] = {'rets': all_rets, 'mdds': all_mdds, 'seed_avgs': seed_avgs}
        avg = sum(all_rets)/len(all_rets)
        med = sorted(all_rets)[len(all_rets)//2]
        mdd = max(all_mdds); std = statistics.pstdev(all_rets)
        sh = avg/std if std > 0 else 0; cal = avg/mdd if mdd > 0 else 0
        mk = ' ★' if filt == 'none' else '  '
        print(f'{mk}{filt:<16}{avg:>+9.1f}%{med:>+8.1f}%{mdd:>+8.2f}%{sh:>8.2f}{cal:>8.2f}')

    base = all_results['none']['seed_avgs']
    print('\n--- Paired vs baseline(none) ---')
    print(f'  {"filter":<16}{"avg_lift":>11}{"med_lift":>11}{"min":>10}{"max":>10}{"wins":>10}  verdict')
    print('  ' + '-' * 86)
    for filt in variants:
        if filt == 'none': continue
        new = all_results[filt]['seed_avgs']
        lifts = [b - a for a, b in zip(base, new)]
        wins = sum(1 for l in lifts if l > 0)
        avg_lift = sum(lifts)/len(lifts); med_lift = statistics.median(lifts)
        verdict = ('✓✓ 우월' if wins >= 70 else '✓ 우월' if wins >= 60 else
                   '~ 동등' if 40 <= wins <= 60 else '✗ 열세')
        print(f'  {filt:<16}{avg_lift:>+9.2f}%p{med_lift:>+9.2f}%p{min(lifts):>+9.2f}%p{max(lifts):>+9.2f}%p{wins:>6}/{N_SEEDS}  {verdict}')

    # full 74일 trade 비교 (none vs best filter)
    print('\n--- Full BT (start_idx=0) 거래 비교 ---')
    for filt in variants:
        r = simulate(dates, data, price_full, all_dates, didx, px, filt=filt, start_idx=0)
        wins = sum(1 for t in r['trades'] if t['ret_pct'] > 0)
        n = len(r['trades'])
        wr = f'{wins}/{n}' if n else '0/0'
        print(f'  {filt:<16} cum {r["cum"]:>+7.1f}%  MDD {r["mdd"]:>5.1f}%  거래 {n:>2}건  승 {wr}')

    # AEIS가 각 필터에서 매수되는지 진단
    print('\n--- 진단: AEIS/WMG/FIVE 매수 여부 (full 74일) ---')
    for filt in ['none', '>MA10', '>MA20', '>MA30']:
        r = simulate(dates, data, price_full, all_dates, didx, px, filt=filt, start_idx=0)
        bought = defaultdict(list)
        for t in r['trades']:
            bought[t['ticker']].append(round(t['ret_pct'], 1))
        tgt = {tk: bought.get(tk, []) for tk in ['AEIS', 'WMG', 'FIVE']}
        print(f'  [{filt:<14}] AEIS={tgt["AEIS"]}  WMG={tgt["WMG"]}  FIVE={tgt["FIVE"]}')

    # leave-one-winner-out: SNDK+MU 제외 시 >MA20 edge가 유지되는가
    print('\n--- leave-winner-out: SNDK+MU 제외 paired (>MA20, >MA30 vs none) ---')
    for excl_name, excl in [('전체', ()), ('-MU', ('MU',)), ('-SNDK', ('SNDK',)),
                            ('-MU-SNDK', ('MU', 'SNDK'))]:
        # baseline (none) seed_avgs under this exclusion
        base_avgs = []
        for chosen in seed_starts:
            sr = [simulate(dates, data, price_full, all_dates, didx, px,
                           filt='none', start_idx=s, exclude=excl)['cum'] for s in chosen]
            base_avgs.append(sum(sr)/len(sr))
        line = f'  [{excl_name:<9}] '
        for filt in ['>MA20', '>MA30']:
            new_avgs = []
            for chosen in seed_starts:
                sr = [simulate(dates, data, price_full, all_dates, didx, px,
                               filt=filt, start_idx=s, exclude=excl)['cum'] for s in chosen]
                new_avgs.append(sum(sr)/len(sr))
            lifts = [b-a for a, b in zip(base_avgs, new_avgs)]
            wins = sum(1 for l in lifts if l > 0)
            line += f'{filt}: {sum(lifts)/len(lifts):+7.1f}%p ({wins:>3}/100)   '
        print(line)


if __name__ == '__main__':
    main()
