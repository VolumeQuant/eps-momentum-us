# -*- coding: utf-8 -*-
"""(2) EPS 풀 넓힌 gap 슬리브: 좁은137 아니라 수집된 broad 1,224 유니버스 + KR 구조.
자격 = forward PER<20 (KR plateau 정점) + $1B. 비중 = 기대성장(rev_growth)↑.
DB만으로(price·ntm_current·rev_growth) 구축. ⚠️ 95일 단일강세장(짧음) — gap broad 검증은 A(us-4factor 8년)가 함.
이 BT = 'EPS 풀 넓히기'의 구성·분산성·LOWO 확인용. forward수익=DB 가격.
"""
import sys, os, sqlite3
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr

c = sqlite3.connect(dr.DB_PATH)
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE ntm_current IS NOT NULL ORDER BY date')]
D = {}
for d in ad:
    D[d] = {r[0]: {'px': r[1], 'nc': r[2], 'rg': r[3], 'dv': r[4]}
            for r in c.execute('SELECT ticker,price,ntm_current,rev_growth,dollar_volume_30d FROM ntm_screening WHERE date=? AND ntm_current IS NOT NULL', (d,))}
c.close()
print(f'broad 유니버스 BT: {ad[0]}~{ad[-1]} ({len(ad)}일), 일평균 {int(np.mean([len(D[d]) for d in ad]))}종목')


def eligible(d, fmax=20, dv=0):
    # ⚠️ dollar_volume은 broad 미수집(30종목만) → 유동성 필터 생략. forward<fmax + rev_growth만.
    out = []
    for tk, v in D[d].items():
        if v['px'] and v['nc'] and v['nc'] > 0 and v['px'] / v['nc'] < fmax and (v['rg'] is not None) and (v['dv'] or 1e9) >= dv:
            out.append(tk)
    return out


def bt(fmax=20, K=15, weight='growth', ban=()):
    """월간 리밸. 자격(forward<fmax)+$1B 중 rev_growth 상위 K, growth가중 or equal."""
    hold = {}; nav = 1.0; peak = 1.0; mdd = 0.0; rb = None
    for i in range(1, len(ad)):
        d, pv = ad[i], ad[i - 1]
        if hold:
            dr_ = 0.0
            for tk, w in hold.items():
                cu, pp = D.get(d, {}).get(tk, {}).get('px'), D.get(pv, {}).get(tk, {}).get('px')
                if cu and pp and pp > 0:
                    dr_ += w * (cu - pp) / pp
            nav *= (1 + dr_); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        m = d[:7]
        if m != rb or not hold:
            el = [tk for tk in eligible(pv, fmax) if tk not in ban]
            el = [(tk, max(D[pv][tk]['rg'] or 0, 0)) for tk in el]
            el.sort(key=lambda x: -x[1])
            top = el[:K]
            if top:
                if weight == 'growth':
                    tot = sum(g for _, g in top) or 1
                    hold = {tk: (g / tot if tot > 0 else 1 / len(top)) for tk, g in top}
                    if tot == 0:
                        hold = {tk: 1 / len(top) for tk, _ in top}
                else:
                    hold = {tk: 1 / len(top) for tk, _ in top}
                rb = m
    cum = (nav - 1) * 100
    return cum, mdd * 100, (cum / 100) / abs(mdd) if mdd < 0 else 0, list(hold.keys())


def uni_ew():
    nav = 1.0
    for i in range(1, len(ad)):
        d, pv = ad[i], ad[i - 1]
        rs = [D[d][tk]['px'] / D[pv][tk]['px'] - 1 for tk in D[pv]
              if tk in D[d] and D[pv][tk]['px'] and D[d][tk]['px'] and D[pv][tk]['px'] > 0]
        if rs:
            nav *= (1 + np.mean(rs))
    return (nav - 1) * 100


print(f'\n유니버스(1224) 동일가중 벤치: {uni_ew():+.1f}%')
print('\n=== EPS-broad gap 슬리브 (forward<20 자격 + rev_growth 비중) ===')
print(f'{"구성":<28}{"CAGR%":>9}{"MDD%":>8}{"Cal":>7}{"종목":>6}')
for K in [10, 15, 20, 30]:
    for wt in ['growth', 'equal']:
        r = bt(K=K, weight=wt)
        print(f'  K{K} {wt:>6} 비중{"":<10}{r[0]:>+9.1f}{r[1]:>+8.1f}{r[2]:>7.2f}{len(r[3]):>6}')

print('\n=== LOWO (winner 빼도 분산 유지? broad면 버텨야) ===')
WIN = ['SNDK', 'MU', 'STX', 'NVDA', 'LITE', 'COHR', 'HPE', 'HGV']
base = bt(K=15, weight='growth')[0]
worst = 999; ww = None
for w in WIN:
    r = bt(K=15, weight='growth', ban=(w,))[0]
    d = r - base
    print(f'   -{w:5}: {r:+.1f}% (Δ{d:+.1f}p)')
    if d < worst:
        worst = d; ww = w
print(f'  → 최악 Δ {worst:+.1f}p (-{ww}) → {"분산 robust" if worst > -base*0.3 else "단일종목 의존?"}')
print('\n참고: 좁은137 gap슬리브는 LOWO -SNDK시 -183p(=SNDK단일). broad면 그게 분산돼야.')
