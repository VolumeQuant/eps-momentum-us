"""4사분면 case별 알파 측정 — 3가지 정의

정의 D1: eps_chg_weighted vs price_chg_weighted (시스템 표준 컬럼)
정의 D2: ntm_current/ntm_90d vs price 30d (장기, 사용자 직관적)
정의 D3: ntm_30d/ntm_60d vs price 30d (둘 다 30일, 시간축 일치)

분류:
  C1: EPS 상향 + 가격 상승 (정상 추세)
  C2: EPS 상향 + 가격 하락 (저평가, buy-the-dip)
  C3: EPS 하향 + 가격 상승 (고평가)
  C4: EPS 하향 + 가격 하락 (정상 약세)

데이터:
  - Step 1: 시스템 시작 ~ 5/22 실제 진입 12건 분류
  - Step 2: 매일 Top 30 후보 (1100+ 종목 × 69일)에서 case 분포 + "가상 진입" 후 수익률
"""
import sys
import sqlite3
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    daily = {}
    for d in dates:
        rows = cur.execute('''
            SELECT ticker, part2_rank, price, eps_chg_weighted,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, composite_rank
            FROM ntm_screening WHERE date=?
        ''', (d,)).fetchall()
        daily[d] = {}
        for r in rows:
            tk = r[0]
            (_, p2, px, eps_w, nc, n7, n30, n60, n90, cr) = r
            daily[d][tk] = {
                'p2': p2, 'price': px, 'eps_w': eps_w, 'cr': cr,
                'nc': nc, 'n7': n7, 'n30': n30, 'n60': n60, 'n90': n90,
            }
    # 가격 history (30일 lookback용)
    price_hist = defaultdict(dict)  # {ticker: {date: price}}
    for tk, d, px in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
        price_hist[tk][d] = px
    conn.close()
    return dates, daily, price_hist


def get_price_chg(tk, today, lookback_days, dates, price_hist):
    """today에서 lookback_days 거래일 전 가격 대비 변화율 (%)"""
    if today not in dates:
        return None
    di = dates.index(today)
    if di < lookback_days:
        return None
    past_date = dates[di - lookback_days]
    past_p = price_hist.get(tk, {}).get(past_date)
    cur_p = price_hist.get(tk, {}).get(today)
    if past_p and cur_p and past_p > 0:
        return (cur_p - past_p) / past_p * 100
    return None


def classify_d1(d):
    """D1: eps_chg_weighted vs (price 변화는 price_chg_weighted 없음 → price 30d 대용)"""
    # eps_chg_weighted는 시스템 컬럼 — NTM 가중 변화 (% 단위)
    # price_chg_weighted 컬럼 없음. price 30d 변화 대용
    return None  # placeholder; price와 같이 처리


def classify(eps_chg, price_chg):
    """EPS 변화, 가격 변화로 4사분면 분류"""
    if eps_chg is None or price_chg is None:
        return None
    if eps_chg > 0 and price_chg > 0: return 'C1'  # EPS↑ + 가격↑
    if eps_chg > 0 and price_chg < 0: return 'C2'  # EPS↑ + 가격↓ (buy-dip)
    if eps_chg < 0 and price_chg > 0: return 'C3'  # EPS↓ + 가격↑ (고평가)
    if eps_chg < 0 and price_chg < 0: return 'C4'  # EPS↓ + 가격↓ (약세)
    return None  # boundary case


def main():
    dates, daily, price_hist = load_data()
    print(f'데이터: {dates[0]} ~ {dates[-1]} ({len(dates)} 거래일)')

    # 시스템 시작 ~ 5/22 실제 진입 12건 (analyze_buy_dip_history 결과)
    actual_trades = [
        ('SNDK', '2026-02-17', '2026-03-18', +27.62),
        ('LITE', '2026-02-17', '2026-03-13',  +3.68),
        ('STX',  '2026-02-17', '2026-04-02',  +3.23),
        ('MOD',  '2026-03-13', '2026-04-01', +18.26),
        ('FORM', '2026-03-18', '2026-04-14', +33.37),
        ('MU',   '2026-04-01', '2026-05-08', +103.02),
        ('FIVE', '2026-04-14', '2026-04-28',  +5.77),
        ('LITE', '2026-04-28', '2026-05-04', +23.35),
        ('TER',  '2026-05-04', '2026-05-19',  -4.72),
        ('SNDK', '2026-04-06', '2026-05-19', +90.90),
        ('BE',   '2026-05-08', '2026-05-13', +11.01),
        ('AEIS', '2026-05-15', '2026-05-19',  -6.37),
    ]

    print()
    print('=' * 110)
    print('Step 1: 실제 진입 12건 — 3가지 정의로 4사분면 분류')
    print('=' * 110)
    print(f'{"ticker":<7} {"entry":<12} {"ret":>7} {"D1 (eps_w, p30d)":^22} {"D2 (nc/n90, p30d)":^22} {"D3 (n30/n60, p30d)":^22}')
    print('-' * 110)

    by_case = {
        'D1': defaultdict(list),
        'D2': defaultdict(list),
        'D3': defaultdict(list),
    }
    for tk, entry, exit_, ret in actual_trades:
        info = daily.get(entry, {}).get(tk)
        if not info:
            print(f'{tk:<7} {entry:<12} {ret:+6.2f}% (데이터 없음)')
            continue
        price_30d = get_price_chg(tk, entry, 30, dates, price_hist)
        # D1: eps_chg_weighted vs price 30d
        eps_w = info.get('eps_w')
        d1_eps = eps_w  # 이미 % 단위
        d1_case = classify(d1_eps, price_30d) if d1_eps is not None else None
        # D2: ntm_current vs ntm_90d
        nc = info.get('nc'); n90 = info.get('n90')
        d2_eps = ((nc - n90) / n90 * 100) if (nc and n90 and n90 > 0) else None
        d2_case = classify(d2_eps, price_30d) if d2_eps is not None else None
        # D3: ntm_30d vs ntm_60d
        n30 = info.get('n30'); n60 = info.get('n60')
        d3_eps = ((n30 - n60) / n60 * 100) if (n30 and n60 and n60 > 0) else None
        d3_case = classify(d3_eps, price_30d) if d3_eps is not None else None

        d1_str = f'{d1_case or "?"} (eps {d1_eps:+5.1f}, p {price_30d:+5.1f})' if d1_eps is not None and price_30d is not None else 'no data'
        d2_str = f'{d2_case or "?"} (eps {d2_eps:+5.1f}, p {price_30d:+5.1f})' if d2_eps is not None and price_30d is not None else 'no data'
        d3_str = f'{d3_case or "?"} (eps {d3_eps:+5.1f}, p {price_30d:+5.1f})' if d3_eps is not None and price_30d is not None else 'no data'

        if d1_case: by_case['D1'][d1_case].append(ret)
        if d2_case: by_case['D2'][d2_case].append(ret)
        if d3_case: by_case['D3'][d3_case].append(ret)

        print(f'{tk:<7} {entry:<12} {ret:+6.2f}% {d1_str:^22} {d2_str:^22} {d3_str:^22}')

    # 정의별 case 통계
    print()
    print('=' * 110)
    print('Step 2: 정의별 case 평균 수익률 (실제 진입 12건)')
    print('=' * 110)
    for d_name in ['D1', 'D2', 'D3']:
        print(f'\n[{d_name}]')
        print(f'  {"case":<5} {"의미":<30} {"건수":>5} {"평균 수익률":>12} {"min":>9} {"max":>9}')
        print(f'  ' + '-' * 80)
        order = [
            ('C1', 'EPS↑ + 가격↑ (정상 추세)'),
            ('C2', 'EPS↑ + 가격↓ (buy-the-dip)'),
            ('C3', 'EPS↓ + 가격↑ (고평가)'),
            ('C4', 'EPS↓ + 가격↓ (정상 약세)'),
        ]
        for case, label in order:
            rets = by_case[d_name].get(case, [])
            if rets:
                avg = sum(rets)/len(rets)
                print(f'  {case:<5} {label:<30} {len(rets):>4} {avg:+11.2f}% {min(rets):+8.2f}% {max(rets):+8.2f}%')
            else:
                print(f'  {case:<5} {label:<30} {0:>4} {"(없음)":>12}')

    # Step 3: 더 큰 표본 — 매일 Top 30 후보의 case 분포
    print()
    print('=' * 110)
    print('Step 3: Top 30 후보 (모든 일자) — 진입 후 5/10/20일 수익률')
    print('=' * 110)

    # 진입 후 N거래일 수익률 측정
    HOLD_PERIODS = [5, 10, 20]
    counts_by_d = {d: defaultdict(int) for d in ['D1', 'D2', 'D3']}
    rets_by_d = {d: {p: defaultdict(list) for p in HOLD_PERIODS} for d in ['D1', 'D2', 'D3']}

    for date_idx, today in enumerate(dates):
        if date_idx >= len(dates) - max(HOLD_PERIODS):
            break
        # 그 날 Top 30 (part2_rank ≤ 30)
        today_top30 = [(tk, info) for tk, info in daily[today].items()
                       if info.get('p2') is not None and info['p2'] <= 30]
        for tk, info in today_top30:
            price_30d = get_price_chg(tk, today, 30, dates, price_hist)
            if price_30d is None:
                continue
            # D1
            eps_w = info.get('eps_w')
            if eps_w is not None:
                c1 = classify(eps_w, price_30d)
                if c1:
                    counts_by_d['D1'][c1] += 1
                    for p in HOLD_PERIODS:
                        if date_idx + p < len(dates):
                            future_d = dates[date_idx + p]
                            fp = price_hist.get(tk, {}).get(future_d)
                            cp = info.get('price')
                            if fp and cp and cp > 0:
                                rets_by_d['D1'][p][c1].append((fp - cp) / cp * 100)
            # D2
            nc = info.get('nc'); n90 = info.get('n90')
            if nc and n90 and n90 > 0:
                d2e = (nc - n90) / n90 * 100
                c2 = classify(d2e, price_30d)
                if c2:
                    counts_by_d['D2'][c2] += 1
                    for p in HOLD_PERIODS:
                        if date_idx + p < len(dates):
                            future_d = dates[date_idx + p]
                            fp = price_hist.get(tk, {}).get(future_d)
                            cp = info.get('price')
                            if fp and cp and cp > 0:
                                rets_by_d['D2'][p][c2].append((fp - cp) / cp * 100)
            # D3
            n30 = info.get('n30'); n60 = info.get('n60')
            if n30 and n60 and n60 > 0:
                d3e = (n30 - n60) / n60 * 100
                c3 = classify(d3e, price_30d)
                if c3:
                    counts_by_d['D3'][c3] += 1
                    for p in HOLD_PERIODS:
                        if date_idx + p < len(dates):
                            future_d = dates[date_idx + p]
                            fp = price_hist.get(tk, {}).get(future_d)
                            cp = info.get('price')
                            if fp and cp and cp > 0:
                                rets_by_d['D3'][p][c3].append((fp - cp) / cp * 100)

    for d_name in ['D1', 'D2', 'D3']:
        print(f'\n[{d_name}] case 분포 + 보유 기간별 평균 수익률')
        print(f'  {"case":<5} {"count":>7} ', end='')
        for p in HOLD_PERIODS:
            print(f' {"+%d일 평균"%p:>12} ', end='')
        print()
        print(f'  ' + '-' * 80)
        total = sum(counts_by_d[d_name].values())
        order = [
            ('C1', 'EPS↑ 가격↑'),
            ('C2', 'EPS↑ 가격↓ (buy-dip)'),
            ('C3', 'EPS↓ 가격↑ (고평가)'),
            ('C4', 'EPS↓ 가격↓'),
        ]
        for case, label in order:
            cnt = counts_by_d[d_name].get(case, 0)
            pct = cnt/total*100 if total > 0 else 0
            print(f'  {case:<5} {cnt:>4} ({pct:>4.1f}%) ', end='')
            for p in HOLD_PERIODS:
                rs = rets_by_d[d_name][p].get(case, [])
                if rs:
                    avg = sum(rs)/len(rs)
                    print(f' {avg:+10.2f}% ', end='')
                else:
                    print(f' {"-":>12} ', end='')
            print(f' | {label}')

    # 정의별 case 알파 순위
    print()
    print('=' * 110)
    print('case 알파 순위 (10일 보유 기준)')
    print('=' * 110)
    for d_name in ['D1', 'D2', 'D3']:
        rets10 = rets_by_d[d_name][10]
        ranked = sorted(['C1','C2','C3','C4'],
                        key=lambda c: -(sum(rets10.get(c,[0]))/len(rets10.get(c,[1])) if rets10.get(c) else -999))
        print(f'\n[{d_name}] 알파 순위: {" > ".join(ranked)}')
        for c in ranked:
            rs = rets10.get(c, [])
            if rs:
                avg = sum(rs)/len(rs)
                print(f'  {c}: 평균 {avg:+.2f}% (n={len(rs)})')


if __name__ == '__main__':
    main()
