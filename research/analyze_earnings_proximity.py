"""진입 종목별 어닝 근접도 분석 — 진입일과 가장 가까운 어닝 발표일 거리"""
import sys
import yfinance as yf
from datetime import datetime, date

sys.stdout.reconfigure(encoding='utf-8')

# 시스템 진입 history (analyze_buy_dip_history 결과에서)
TRADES = [
    ('SNDK', '2026-02-17', '2026-03-18', +27.62, 'rank_NULL'),
    ('LITE', '2026-02-17', '2026-03-13',  +3.68, 'rank>10'),
    ('STX',  '2026-02-17', '2026-04-02',  +3.23, 'rank>10'),
    ('MOD',  '2026-03-13', '2026-04-01', +18.26, 'rank>10'),
    ('FORM', '2026-03-18', '2026-04-14', +33.37, 'rank>10'),
    ('MU',   '2026-04-01', '2026-05-08', +103.02,'rank>10'),
    ('FIVE', '2026-04-14', '2026-04-28',  +5.77, 'rank>10'),
    ('LITE', '2026-04-28', '2026-05-04', +23.35, 'rank>10'),
    ('TER',  '2026-05-04', '2026-05-19',  -4.72, 'OPEN'),
    ('SNDK', '2026-04-06', '2026-05-19', +90.90, 'OPEN'),
    ('BE',   '2026-05-08', '2026-05-13', +11.01, 'rank>10'),
    ('AEIS', '2026-05-15', '2026-05-19',  -6.37, 'OPEN'),
]


def fetch_earnings_dates(ticker):
    """yfinance에서 최근 어닝 발표일들 가져오기"""
    try:
        t = yf.Ticker(ticker)
        # earnings_history: 과거 발표 기록 + 차후 일정
        eh = t.earnings_history
        if eh is None or eh.empty:
            return []
        dates = []
        for idx in eh.index:
            if hasattr(idx, 'date'):
                dates.append(idx.date())
        return sorted(dates)
    except Exception as e:
        print(f'  {ticker} 에러: {e}', file=sys.stderr)
        return []


def days_between(d1_str, d2_date):
    d1 = datetime.strptime(d1_str, '%Y-%m-%d').date()
    return (d2_date - d1).days


def main():
    print('=' * 110)
    print('진입일 기준 어닝 근접도 분석')
    print('=' * 110)
    print(f'{"ticker":<7} {"entry":<12} {"가장가까운 어닝":<15} {"진입 vs 어닝":>15} {"ret":>9} {"분류":<25}')
    print('-' * 110)

    earnings_cache = {}
    near_earnings = []
    safe_entries = []

    for tk, entry, exit_, ret, reason in TRADES:
        if tk not in earnings_cache:
            earnings_cache[tk] = fetch_earnings_dates(tk)
        eds = earnings_cache[tk]
        entry_d = datetime.strptime(entry, '%Y-%m-%d').date()

        # 진입일과 가장 가까운 어닝
        if not eds:
            print(f'{tk:<7} {entry:<12} (어닝 데이터 없음)')
            continue
        # 진입일 이후 첫 어닝
        future = [e for e in eds if e >= entry_d]
        past = [e for e in eds if e < entry_d]
        next_ed = future[0] if future else None
        last_ed = past[-1] if past else None

        category = ''
        # 진입 2주 이내 어닝 예정?
        if next_ed and (next_ed - entry_d).days <= 14:
            days_to = (next_ed - entry_d).days
            category = f'⚠️ {days_to}일 후 어닝'
            near_earnings.append((tk, entry, next_ed, days_to, ret))
            disp = f'{next_ed} (+{days_to}일)'
        elif last_ed and (entry_d - last_ed).days <= 7:
            days_after = (entry_d - last_ed).days
            category = f'어닝 후 {days_after}일'
            disp = f'{last_ed} (-{days_after}일)'
        else:
            if next_ed:
                disp = f'{next_ed} (+{(next_ed-entry_d).days}일)'
            else:
                disp = f'{last_ed}'
            safe_entries.append((tk, entry, ret))
            category = 'safe'

        print(f'{tk:<7} {entry:<12} {disp:<15} {category:>25} {ret:+8.2f}% {reason:<15}')

    # 요약
    print()
    print('=' * 110)
    print('요약')
    print('=' * 110)
    if near_earnings:
        print(f'어닝 2주 이내 진입 ({len(near_earnings)}건):')
        for tk, entry, ed, days, ret in near_earnings:
            print(f'  {tk} {entry} → 어닝 {ed} (+{days}일) → 결과 {ret:+.2f}%')
        avg = sum(x[4] for x in near_earnings) / len(near_earnings)
        wins = sum(1 for x in near_earnings if x[4] > 0)
        print(f'  → 평균 {avg:+.2f}%, win {wins}/{len(near_earnings)}')
    else:
        print('어닝 2주 이내 진입: 0건')


if __name__ == '__main__':
    main()
