"""단일일 점프 패턴 EDA — 진짜 어닝 비트 vs 일시적 노이즈 구분

VIRT 5/1 패턴 (저커버리지 + 단일일 cr 점프) 종목들의 진입 후 수익률을 측정해
이런 패턴이 실제 알파 공급원인지 노이즈인지 판정.

조건 정의:
  - 저커버리지: num_analysts < 10
  - 점프 패턴: T-2 cr ≥ 20 AND T-1 cr ≥ 15 AND T0 cr ≤ 5
  - 매수 후보 진입: part2_rank ≤ 3

측정:
  - 진입일(T0) 종가 → T+1, T+3, T+5, T+10 일 종가 수익률
  - 같은 기간 매수 후보 평균 수익률(전체 part2_rank≤3 종목)과 비교

비교군:
  - 그룹A: 저커버리지 + 점프 (의심 패턴)
  - 그룹B: 정상 커버리지(≥10) + 점프 (점프만 있고 저커버리지 X)
  - 그룹C: 저커버리지 + 일관 (점프 없음, 저커버리지만)
  - 그룹D: 전체 part2_rank ≤ 3 매수 후보 (베이스라인)

결과 해석:
  - 그룹A 평균 수익 ≥ 그룹D → 점프 패턴이 진짜 알파 → 콤보 필터 거부
  - 그룹A 평균 수익 < 그룹D 명확하게 → 노이즈 → 콤보 필터 채택 가능
"""
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'

LOW_COV_THR = 10        # num_analysts < 10 → 저커버리지
JUMP_T2_MIN = 20        # T-2 cr ≥ 20 (=평범했음)
JUMP_T1_MIN = 15        # T-1 cr ≥ 15 (=여전히 평범)
JUMP_T0_MAX = 5         # T0 cr ≤ 5 (=갑자기 상위)
HORIZONS = [1, 3, 5, 10]


def load_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker, composite_rank, part2_rank, num_analysts, price,
               ntm_current, ntm_30d
        FROM ntm_screening WHERE composite_rank IS NOT NULL
        ORDER BY date, ticker
    ''').fetchall()
    conn.close()
    by_date = defaultdict(dict)  # date -> ticker -> row
    for d, tk, cr, p2, na, px, nc, n30 in rows:
        by_date[d][tk] = {
            'cr': cr, 'p2': p2, 'na': na, 'px': px,
            'nc': nc, 'n30': n30
        }
    return by_date


def classify_entry(by_date, dates):
    """매수 후보 진입(part2_rank ≤ 3)일에 4개 그룹 분류"""
    groups = {'A': [], 'B': [], 'C': [], 'D': []}
    # A: 저커버리지 + 점프
    # B: 정상 커버리지 + 점프
    # C: 저커버리지 + 일관 (점프 X)
    # D: 전체 매수 후보 (베이스라인)

    for i, d in enumerate(dates):
        if i < 2:
            continue  # T-2, T-1 필요
        d_t2 = dates[i - 2]
        d_t1 = dates[i - 1]
        for tk, info in by_date[d].items():
            if info['p2'] is None or info['p2'] > 3:
                continue
            if info['px'] is None:
                continue

            cr_t0 = info['cr']
            cr_t1 = by_date[d_t1].get(tk, {}).get('cr')
            cr_t2 = by_date[d_t2].get(tk, {}).get('cr')

            # 베이스라인 (전체 매수 후보)
            entry = {'date': d, 'ticker': tk, 'cr_t0': cr_t0,
                     'cr_t1': cr_t1, 'cr_t2': cr_t2,
                     'na': info['na'], 'px': info['px'],
                     'nc': info['nc'], 'n30': info['n30']}
            groups['D'].append(entry)

            # 점프 패턴 정의 (T-2, T-1 모두 데이터 있어야)
            if cr_t1 is None or cr_t2 is None:
                continue
            is_jump = (cr_t2 >= JUMP_T2_MIN and cr_t1 >= JUMP_T1_MIN
                       and cr_t0 <= JUMP_T0_MAX)
            is_low = info['na'] is not None and info['na'] < LOW_COV_THR

            if is_jump and is_low:
                groups['A'].append(entry)
            elif is_jump and not is_low:
                groups['B'].append(entry)
            elif not is_jump and is_low:
                groups['C'].append(entry)

    return groups


def measure_returns(groups, by_date, dates):
    """그룹별 진입 후 N일 수익률 측정"""
    date_idx = {d: i for i, d in enumerate(dates)}
    results = {}
    for gname, entries in groups.items():
        rets_by_h = {h: [] for h in HORIZONS}
        for e in entries:
            d0 = e['date']
            i0 = date_idx.get(d0)
            if i0 is None:
                continue
            px0 = e['px']
            for h in HORIZONS:
                if i0 + h >= len(dates):
                    continue
                d_fwd = dates[i0 + h]
                px_fwd = by_date[d_fwd].get(e['ticker'], {}).get('px')
                if px_fwd is None or px0 is None or px0 <= 0:
                    continue
                rets_by_h[h].append((px_fwd / px0 - 1) * 100)
        results[gname] = rets_by_h
    return results


def summarize(rets_by_h, n_entries):
    """수익률 통계"""
    summary = {}
    for h, rets in rets_by_h.items():
        if not rets:
            summary[h] = None
            continue
        avg = sum(rets) / len(rets)
        sorted_r = sorted(rets)
        med = sorted_r[len(rets) // 2]
        winrate = sum(1 for r in rets if r > 0) / len(rets) * 100
        summary[h] = {
            'avg': avg, 'med': med, 'wr': winrate,
            'n': len(rets), 'min': min(rets), 'max': max(rets)
        }
    return summary


def main():
    print('=' * 110)
    print('단일일 점프 패턴 EDA — 진짜 어닝 비트 vs 일시적 노이즈')
    print(f'점프 정의: T-2 cr ≥ {JUMP_T2_MIN}, T-1 cr ≥ {JUMP_T1_MIN}, T0 cr ≤ {JUMP_T0_MAX}')
    print(f'저커버리지: num_analysts < {LOW_COV_THR}')
    print('진입 정의: part2_rank ≤ 3')
    print('=' * 110)

    by_date = load_data()
    dates = sorted(by_date.keys())
    print(f'\n분석 기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)')

    groups = classify_entry(by_date, dates)
    print('\n[1] 그룹별 진입 건수')
    print(f'  A (저커버리지 + 점프):    {len(groups["A"]):>4}건')
    print(f'  B (정상커버리지 + 점프):  {len(groups["B"]):>4}건')
    print(f'  C (저커버리지 + 일관):    {len(groups["C"]):>4}건')
    print(f'  D (전체 매수 후보):       {len(groups["D"]):>4}건  [베이스라인]')

    # 그룹A 종목 리스트
    if groups['A']:
        print('\n[2] 그룹A 종목 상세 (저커버리지 + 점프)')
        for e in groups['A']:
            ntm_chg = ((e['nc'] - e['n30']) / e['n30'] * 100) if e['n30'] else 0
            print(f'  {e["date"]} {e["ticker"]:<6} '
                  f'cr={e["cr_t2"]:>3}→{e["cr_t1"]:>3}→{e["cr_t0"]:>3}, '
                  f'na={e["na"]:>3}, ntm30dΔ={ntm_chg:+.1f}%')

    # 수익률 측정
    print('\n[3] 진입 후 수익률')
    returns = measure_returns(groups, by_date, dates)
    print(f'\n{"Group":<35} {"H+1":>15} {"H+3":>15} {"H+5":>15} {"H+10":>15}')
    print('-' * 100)
    labels = {
        'A': '저커버리지+점프 (의심 패턴)',
        'B': '정상커버리지+점프',
        'C': '저커버리지+일관',
        'D': '전체 매수 후보 (베이스라인)'
    }
    for g in ['A', 'B', 'C', 'D']:
        s = summarize(returns[g], len(groups[g]))
        line = f'  {g}. {labels[g]:<30}'
        for h in HORIZONS:
            if s[h] is None:
                line += f' {"n/a":>15}'
            else:
                line += f' {s[h]["avg"]:+5.2f}%(n={s[h]["n"]:>2})'
        print(line)

    # 그룹A vs D 비교 (핵심 질문)
    print('\n[4] 그룹 A vs D 비교 (의심 패턴 vs 베이스라인)')
    sA = summarize(returns['A'], len(groups['A']))
    sD = summarize(returns['D'], len(groups['D']))
    for h in HORIZONS:
        if sA[h] and sD[h]:
            d_avg = sA[h]['avg'] - sD[h]['avg']
            verdict = '✗ 노이즈 의심 (베이스라인 < )' if d_avg < -1.0 else \
                      '✓ 알파 공급 (베이스라인 >=)' if d_avg >= 0 else '~ 미세 손실 (1%p 이내)'
            print(f'  H+{h}: A평균 {sA[h]["avg"]:+5.2f}%, D평균 {sD[h]["avg"]:+5.2f}%, '
                  f'Δ {d_avg:+5.2f}%p, win A {sA[h]["wr"]:.0f}% vs D {sD[h]["wr"]:.0f}% — {verdict}')

    # 그룹A vs B 비교 (저커버리지 효과 분리)
    print('\n[5] 그룹 A vs B 비교 (점프 종목 중 저커버리지 vs 정상)')
    sB = summarize(returns['B'], len(groups['B']))
    for h in HORIZONS:
        if sA[h] and sB[h]:
            d_avg = sA[h]['avg'] - sB[h]['avg']
            print(f'  H+{h}: A {sA[h]["avg"]:+5.2f}% vs B {sB[h]["avg"]:+5.2f}%, '
                  f'Δ {d_avg:+5.2f}%p (점프 종목 중 저커버리지 페널티)')


if __name__ == '__main__':
    main()
