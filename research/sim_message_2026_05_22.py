"""5/22 (최신 영업일) 기준 (2,10,2) dynamic_5 b=3 메시지 시뮬레이션

오늘 시점에서 사용자가 받을 메시지 모습 + 비중 결정 과정 상세.
"""
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
LOOKBACK = 30

sys.path.insert(0, str(ROOT))


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    last_d = dates[-1]
    print(f'분석 일자: {last_d}\n')

    # production score_100
    import daily_runner as dr
    _, score_cache = dr._build_score_100_map(last_d)

    # Top 30 part2_rank 조회 + 데이터
    rows = cur.execute('''
        SELECT ticker, part2_rank, price, eps_chg_weighted,
               ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
        FROM ntm_screening
        WHERE date=? AND part2_rank IS NOT NULL
        ORDER BY part2_rank
    ''', (last_d,)).fetchall()

    # 30일 전 가격 lookup
    di = dates.index(last_d)
    past_d = dates[di - LOOKBACK] if di >= LOOKBACK else None
    past_prices = {}
    if past_d:
        for tk, px in cur.execute('SELECT ticker, price FROM ntm_screening WHERE date=?', (past_d,)):
            past_prices[tk] = px

    # 종목별 case 분류 + boost 적용
    print('=' * 110)
    print(f'Step 1: {last_d} 원래 part2_rank Top 10 + C2 분류')
    print('=' * 110)
    print(f'{"rank":>4} {"ticker":<7} {"price":>10} {"eps_w":>7} {"price 30d":>11} {"case":<5} {"score_100":>10}')
    print('-' * 75)
    data_list = []
    for r in rows[:10]:
        tk, p2, px, eps_w, nc, n7, n30, n60, n90 = r
        past_p = past_prices.get(tk)
        if past_p and px and past_p > 0:
            p30 = (px - past_p) / past_p * 100
        else:
            p30 = None
        case = 'C1'
        if eps_w is not None and p30 is not None:
            if eps_w > 0 and p30 < 0:
                case = 'C2'
            elif eps_w < 0 and p30 > 0:
                case = 'C3'
            elif eps_w < 0 and p30 < 0:
                case = 'C4'
        score_100 = score_cache.get(tk, 0)
        p30_str = f'{p30:+.1f}%' if p30 is not None else '?'
        eps_w_str = f'{eps_w:+.1f}' if eps_w is not None else '?'
        print(f'{p2:>4} {tk:<7} {px:>10.2f} {eps_w_str:>7} {p30_str:>11} {case:<5} {score_100:>9.1f}')
        data_list.append({'tk': tk, 'p2': p2, 'price': px, 'case': case, 'score_100': score_100})

    # boost=3 적용 후 재정렬
    print()
    print('=' * 110)
    print('Step 2: C2 boost=3 적용 → rank 재계산')
    print('=' * 110)
    print(f'{"orig p2":>8} {"ticker":<7} {"case":<5} {"score":>7} {"new score":>10} {"new rank":>9}')
    print('-' * 60)
    BOOST = 3
    # 전체 종목 (Top 30) boost 적용
    all_for_rerank = []
    for r in rows:
        tk, p2, px, eps_w, nc, n7, n30, n60, n90 = r
        past_p = past_prices.get(tk)
        if past_p and px and past_p > 0:
            p30 = (px - past_p) / past_p * 100
        else:
            p30 = None
        is_c2 = (eps_w is not None and p30 is not None and eps_w > 0 and p30 < 0)
        score = (31 - p2) + (BOOST if is_c2 else 0)
        all_for_rerank.append((score, tk, p2, is_c2))
    all_for_rerank.sort(reverse=True)
    new_ranks = {}
    for new_r, (score, tk, p2, is_c2) in enumerate(all_for_rerank, 1):
        new_ranks[tk] = (new_r, score, is_c2)

    for r in rows[:10]:
        tk, p2, _, _, _, _, _, _, _ = r
        new_r, score, is_c2 = new_ranks[tk]
        case = 'C2' if is_c2 else '?'
        print(f'{p2:>8} {tk:<7} {case:<5} {31-p2:>7} {score:>10} {new_r:>9}')

    # New Top 10
    print()
    print('=' * 110)
    print('Step 3: 새 Top 10 (boost 적용 후 재정렬)')
    print('=' * 110)
    print(f'{"new rank":>8} {"ticker":<7} {"case":<5} {"score_100 (production)":>22}')
    print('-' * 60)
    for new_r, (score, tk, orig_p2, is_c2) in enumerate(all_for_rerank[:10], 1):
        case = 'C2 ★' if is_c2 else 'C1'
        s100 = score_cache.get(tk, 0)
        print(f'{new_r:>8} {tk:<7} {case:<5} {s100:>21.1f}')

    # Step 4: 진입 결정 (Top 2)
    print()
    print('=' * 110)
    print('Step 4: 매수 후보 선택 (entry=2, slots=2)')
    print('=' * 110)
    # 1, 2위 (자격 체크: min_seg ≥ 0 + ✅ + 리스크 필터 — 여기선 단순화)
    top1 = all_for_rerank[0]
    top2 = all_for_rerank[1]
    tk1 = top1[1]; tk2 = top2[1]
    s100_1 = score_cache.get(tk1, 0)
    s100_2 = score_cache.get(tk2, 0)
    diff = s100_1 - s100_2

    print(f'1위: {tk1}, score_100 = {s100_1:.1f}')
    print(f'2위: {tk2}, score_100 = {s100_2:.1f}')
    print(f'점수 차이: {diff:.1f}')
    print()

    # Step 5: dynamic_5 weight
    print('=' * 110)
    print('Step 5: dynamic_5 weight 결정')
    print('=' * 110)
    if diff > 5:
        weights = [80, 20]
        decision = '> 5점 → 80/20 (1위 집중)'
    else:
        weights = [50, 50]
        decision = '≤ 5점 → 50/50 (분산)'
    print(f'점수 차이 {diff:.1f}점 {decision}')
    print(f'  1위 {tk1}: {weights[0]}% 비중')
    print(f'  2위 {tk2}: {weights[1]}% 비중')

    # Step 6: 메시지 시뮬레이션
    print()
    print('=' * 110)
    print(f'Step 6: 사용자가 받을 메시지 (5/22 시뮬레이션)')
    print('=' * 110)
    print(f'''
📡 EPS Momentum US · {last_d}

📈 시스템 누적 수익률 (BT 추정 +241%)
    같은 기간 S&P500 +7.5%

━━━━━━━━━━━━━━━
🛒 EPS 모멘텀 매수 후보
━━━━━━━━━━━━━━━
1. {tk1} — 비중 {weights[0]}%
2. {tk2} — 비중 {weights[1]}%

(원래 메시지: production score_100 표시)
1. {tk1} — {s100_1:.1f}점 — 비중 {weights[0]}%
2. {tk2} — {s100_2:.1f}점 — 비중 {weights[1]}%

매수: 상위 2종목, 최대 2종목 보유
매도: 10위 밖 or 실적하락
''')

    # 비교용: baseline 메시지 (현재 production)
    print('=' * 110)
    print(f'참고: 현재 production (3,10,3) 균등 메시지')
    print('=' * 110)
    # 원래 Top 3 종목
    print(f'\n원래 Top 3 (part2_rank):')
    for r in rows[:3]:
        tk, p2, _, _, _, _, _, _, _ = r
        s100 = score_cache.get(tk, 0)
        print(f'  {p2}. {tk} — {s100:.1f}점 — 비중 33%')

    conn.close()


if __name__ == '__main__':
    main()
