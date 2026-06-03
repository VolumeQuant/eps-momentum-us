# -*- coding: utf-8 -*-
"""V113 — 데이터 fetch 실패 robust + eligible 무관 carryover

V112 BT에서 확인:
- 05-28 n=600 (절반 누락), 05-29 n=315 (75% 누락) — yfinance fetch 부분 실패
- MU 가격 데이터 그 이틀 누락 → sim '매도' 가짜 신호
- 진짜 가격: 05-26 $895 → 06-02 $1064 (+19%) 못 잡음

V113 새 logic:
1. info is None (데이터 fetch 실패) → carryover (어제 holding 유지)
2. 가격 fallback: 어제 가격 사용 (sim 차원 수익률 정확)
3. eligible 무관 메가 carryover (V112와 동일)
4. 매도 트리거: 명시적 (min_seg<-2 / 메가 시그니처 해제 + p2>10 / rev<25%)
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB = Path('eps_momentum_data.db')
N_SEEDS = 100; SAMPLES = 3; MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date').fetchall()]

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


def is_mega(info, peg_thr=0.25):
    if info is None: return False
    if info.get('peg') is None or info['peg'] >= peg_thr: return False
    if info.get('rev_growth') is None or info['rev_growth'] < 0.25: return False
    if info.get('min_seg', 0) < -2: return False
    return True


def is_mega_v86(info):
    return info is not None and info.get('peg') is not None and info['peg'] < 0.22


def last_known_price(tk, i):
    """ntm_screening에서 ticker의 마지막 알려진 가격 (i 이전)"""
    for j in range(i, -1, -1):
        p = pf[dates[j]].get(tk)
        if p: return p
    return None


def sim(variant, exclude=(), start=0):
    held = {}; prev = None; val = 1.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i-1]; ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk) or last_known_price(tk, i-1)
                pn = pf[d].get(tk) or pp  # 오늘 가격 없으면 어제 가격
                if pp and pn: ret += w * (pn/pp - 1)
            val *= (1+ret)
        dd = data_all[d]

        # 매도 logic
        for tk in list(held):
            info = dd.get(tk)

            if variant == 'v113':
                # V113: info None → carryover (skip), 매도 조건은 명시적
                if info is None:
                    continue  # 데이터 누락 = holding 유지
                if info.get('min_seg', 0) < -2:
                    del held[tk]; continue
                if is_mega(info):
                    if info.get('rev_growth') and info['rev_growth'] < 0.25:
                        del held[tk]
                    continue
                else:
                    p2 = info.get('p2')
                    if p2 is None or p2 > 10:
                        del held[tk]
            elif variant == 'v112':
                if info is None:
                    del held[tk]; continue
                if info.get('min_seg', 0) < -2:
                    del held[tk]; continue
                if is_mega(info):
                    if info.get('rev_growth') and info['rev_growth'] < 0.25:
                        del held[tk]
                    continue
                else:
                    p2 = info.get('p2')
                    if p2 is None or p2 > 10:
                        del held[tk]
            else:  # v86e+
                if info is None:
                    del held[tk]; continue
                if info.get('min_seg', 0) < -2:
                    del held[tk]; continue
                if is_mega_v86(info) and info.get('rev_growth') and info['rev_growth'] < 0.25:
                    del held[tk]; continue
                p2 = info.get('p2')
                if p2 is None or p2 > 10:
                    if is_mega_v86(info): continue
                    del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info.get('p2') is None or info['p2'] > 3: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg', 0) < 0: continue
                if not info['price']: continue
                if not verified(tk, i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2-len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1-s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0: held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return (val-1)*100


elig = list(range(2, len(dates) - MIN_HOLD))
seeds = []
for s in range(N_SEEDS):
    random.seed(s); seeds.append(random.sample(elig, SAMPLES))


def run(variant, exclude=()):
    return [sim(variant, exclude=exclude, start=s) for ch in seeds for s in ch]


print('=' * 100)
print('V113 — fetch-fail robust + eligible 무관 carryover')
print('=' * 100)

print('\n[1] Full period (start=0)')
for v in ['v86e+', 'v112', 'v113']:
    print(f'  {v}: cum {sim(v, start=0):+.1f}%')

print('\n[2] Multistart 100×3')
exclusions = [('전체', ()), ('-SNDK', ('SNDK',)), ('-MU', ('MU',)),
              ('-SNDK-MU', ('SNDK', 'MU')), ('-all5', ('SNDK', 'MU', 'BE', 'LITE', 'TTMI'))]
print(f'{"variant":<10}' + ''.join(f'{n:>11}' for n, _ in exclusions))
print('-' * 80)
for v in ['v86e+', 'v112', 'v113']:
    avgs = {n: statistics.mean(run(v, ex)) for n, ex in exclusions}
    print(f'{v:<10}' + ''.join(f'{avgs[n]:>+10.1f}%' for n, _ in exclusions))

print('\n[3] V113 vs v86e+ paired diff')
for n, ex in exclusions:
    cums_86 = run('v86e+', ex); cums_113 = run('v113', ex)
    diffs = [a-b for a, b in zip(cums_113, cums_86)]
    print(f'  {n}: {statistics.mean(diffs):+.1f}p ({sum(1 for d in diffs if d > 0)}/{len(diffs)})')

print('\n[4] V113 trace (start=0)')
held = {}; prev = None; val = 1.0; trades = []
for i in range(2, len(dates)):
    d = dates[i]
    if prev and i > 2:
        dp = dates[i-1]; ret = 0
        for tk, (ed, ep, w) in prev.items():
            pp = pf[dp].get(tk) or last_known_price(tk, i-1)
            pn = pf[d].get(tk) or pp
            if pp and pn: ret += w * (pn/pp - 1)
        val *= (1+ret)
    dd = data_all[d]
    for tk in list(held):
        info = dd.get(tk); ed, ep, w = held[tk]
        sell = None
        if info is None:
            continue  # V113 핵심: carryover
        if info.get('min_seg', 0) < -2:
            sell = 'min_seg<-2'
        elif is_mega(info):
            if info.get('rev_growth') and info['rev_growth'] < 0.25:
                sell = 'mega rev<25%'
        else:
            p2 = info.get('p2')
            if p2 is None or p2 > 10:
                sell = f'mega 해제 + p2={p2}'
        if sell:
            cp = info.get('price') or pf[d].get(tk, ep)
            trades.append((d, 'SELL', tk, cp, (cp/ep-1)*100, sell))
            del held[tk]
    if len(held) < 2:
        cands = []
        for tk, info in dd.items():
            if info.get('p2') is None or info['p2'] > 3: continue
            if tk in held: continue
            if info.get('min_seg', 0) < 0: continue
            if not info['price']: continue
            if not verified(tk, i): continue
            if info.get('high30') and info['price'] and info['price']/info['high30'] - 1 < -0.25: continue
            cands.append((info['p2'], info['score'], tk))
        cands.sort(key=lambda x: x[0])
        pick = cands[:2-len(held)]
        if len(held) == 0 and len(pick) >= 2:
            s1, s2 = pick[0][1], pick[1][1]
            w = [1.0, 0.0] if (s1-s2) >= 15 else [0.5, 0.5]
            for si, (_, _, tk) in enumerate(pick[:2]):
                if w[si] > 0:
                    held[tk] = (d, dd[tk]['price'], w[si])
                    trades.append((d, 'BUY', tk, dd[tk]['price'], 0, 'rank<=3'))
        else:
            for _, _, tk in pick:
                w_val = 0.5 if len(held) >= 1 else 1.0
                held[tk] = (d, dd[tk]['price'], w_val)
                trades.append((d, 'BUY', tk, dd[tk]['price'], 0, 'rank<=3'))
    prev = dict(held)

print(f'\n총 {len(trades)}건')
for idx, t in enumerate(trades, 1):
    d, a, tk, p, ret, r = t
    ret_s = f'{ret:+.1f}%' if a == 'SELL' else '-'
    print(f'{idx:>3} {d} {a:<5} {tk:<8} ${p:>8.2f} {ret_s:>9}  {r}')

print(f'\n현재 보유:')
last_p_map = {}
for tk in held:
    last_p_map[tk] = pf[dates[-1]].get(tk) or last_known_price(tk, len(dates)-1)
for tk, (ed, ep, w) in held.items():
    cp = last_p_map.get(tk) or ep
    print(f'  {tk} {int(w*100)}% : {ed} ${ep:.2f} → ${cp:.2f} ({(cp/ep-1)*100:+.1f}%)')

print(f'\nV113 누적: {(val-1)*100:+.1f}%')
print(f'소요 {time.time()-t0:.0f}초')
con.close()
