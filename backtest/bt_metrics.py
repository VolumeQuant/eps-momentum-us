"""백테스트 종합 성과 지표 모듈

daily_returns (list[float], %) 기반으로 모든 지표를 계산.
trade_log (list[dict]) 기반으로 거래 지표를 계산.

Usage:
    from bt_metrics import report
    report(daily_returns, trade_log, label='현행 전략')
"""
import math
import sys

sys.stdout.reconfigure(encoding='utf-8')


# ── 수익률 지표 ──

def total_return(daily_rets):
    """누적 수익률 (%)"""
    cum = 1.0
    for r in daily_rets:
        cum *= (1 + r / 100)
    return (cum - 1) * 100


def cagr(daily_rets, trading_days_per_year=252):
    """연환산 수익률 CAGR (%)"""
    n = len(daily_rets)
    if n == 0:
        return 0
    cum = 1.0
    for r in daily_rets:
        cum *= (1 + r / 100)
    years = n / trading_days_per_year
    if years <= 0 or cum <= 0:
        return 0
    return (cum ** (1 / years) - 1) * 100


# ── 위험 지표 ──

def max_drawdown(daily_rets):
    """MDD (%) — 음수로 반환"""
    peak = 0
    mdd = 0
    cum = 0
    for r in daily_rets:
        cum += r
        if cum > peak:
            peak = cum
        dd = cum - peak
        if dd < mdd:
            mdd = dd
    return mdd


def max_drawdown_compound(daily_rets):
    """MDD (%) — 복리 기준, 음수로 반환"""
    cum = 1.0
    peak = 1.0
    mdd = 0
    for r in daily_rets:
        cum *= (1 + r / 100)
        if cum > peak:
            peak = cum
        dd = (cum - peak) / peak * 100
        if dd < mdd:
            mdd = dd
    return mdd


def recovery_days(daily_rets):
    """MDD 발생 후 원금 회복까지 걸린 일수 (최대 복구 기간)"""
    peak = 0
    cum = 0
    max_recovery = 0
    dd_start = 0
    in_dd = False
    for i, r in enumerate(daily_rets):
        cum += r
        if cum > peak:
            if in_dd:
                max_recovery = max(max_recovery, i - dd_start)
                in_dd = False
            peak = cum
        elif not in_dd:
            dd_start = i
            in_dd = True
    # 아직 복구 못 한 경우
    if in_dd:
        max_recovery = max(max_recovery, len(daily_rets) - dd_start)
    return max_recovery


def annualized_volatility(daily_rets, trading_days=252):
    """연환산 변동성 (%)"""
    if len(daily_rets) < 2:
        return 0
    mean = sum(daily_rets) / len(daily_rets)
    var = sum((r - mean) ** 2 for r in daily_rets) / (len(daily_rets) - 1)
    return math.sqrt(var * trading_days)


def downside_volatility(daily_rets, target=0, trading_days=252):
    """하방 변동성 (%) — 소르티노 분모"""
    neg = [min(r - target, 0) ** 2 for r in daily_rets]
    if not neg:
        return 0
    return math.sqrt(sum(neg) / len(neg) * trading_days)


# ── 위험 조정 수익 지표 ──

def sharpe_ratio(daily_rets, rf_annual=4.5, trading_days=252):
    """샤프 비율 (연환산, 무위험수익률 기본 4.5%)"""
    if len(daily_rets) < 2:
        return 0
    ann_ret = cagr(daily_rets, trading_days)
    ann_vol = annualized_volatility(daily_rets, trading_days)
    if ann_vol == 0:
        return 0
    return (ann_ret - rf_annual) / ann_vol


def sortino_ratio(daily_rets, rf_annual=4.5, trading_days=252):
    """소르티노 비율 (연환산) — 하락 변동성만 고려"""
    if len(daily_rets) < 2:
        return 0
    ann_ret = cagr(daily_rets, trading_days)
    ds_vol = downside_volatility(daily_rets, target=0, trading_days=trading_days)
    if ds_vol == 0:
        return 0
    return (ann_ret - rf_annual) / ds_vol


def calmar_ratio(daily_rets, trading_days=252):
    """칼마 비율 — CAGR / |MDD|"""
    ann = cagr(daily_rets, trading_days)
    mdd = max_drawdown_compound(daily_rets)
    if mdd == 0:
        return 0
    return ann / abs(mdd)


# ── 거래 지표 ──

def trade_stats(trade_log):
    """거래 로그 기반 통계
    trade_log: list[dict] with 'return' key (%)
    """
    if not trade_log:
        return {
            'total_trades': 0, 'win_rate': 0, 'avg_return': 0,
            'avg_win': 0, 'avg_loss': 0, 'profit_loss_ratio': 0,
            'profit_factor': 0, 'expectancy': 0, 'max_consecutive_loss': 0,
            'max_consecutive_win': 0, 'avg_hold_days': 0,
        }

    returns = [t['return'] for t in trade_log]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]

    # Profit Factor: 총 이익 / 총 손실
    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    win_rate = len(wins) / len(returns) if returns else 0
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')
    expectancy = win_rate * avg_win - (1 - win_rate) * avg_loss

    # 최대 연속 손실/승리
    max_con_loss = 0
    max_con_win = 0
    cur_loss = 0
    cur_win = 0
    for r in returns:
        if r <= 0:
            cur_loss += 1
            cur_win = 0
            max_con_loss = max(max_con_loss, cur_loss)
        else:
            cur_win += 1
            cur_loss = 0
            max_con_win = max(max_con_win, cur_win)

    # 평균 보유일수
    hold_days = []
    for t in trade_log:
        if 'entry_date' in t and 'exit_date' in t:
            try:
                from datetime import datetime
                d1 = datetime.strptime(t['entry_date'], '%Y-%m-%d')
                d2 = datetime.strptime(t['exit_date'], '%Y-%m-%d')
                hold_days.append((d2 - d1).days)
            except Exception:
                pass
    avg_hold = sum(hold_days) / len(hold_days) if hold_days else 0

    # 켈리 비율: f = W - (1-W)/R
    kelly = win_rate - (1 - win_rate) / pl_ratio if pl_ratio > 0 else 0

    return {
        'total_trades': len(returns),
        'win_rate': win_rate,
        'avg_return': sum(returns) / len(returns),
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_loss_ratio': pl_ratio,
        'profit_factor': pf,
        'expectancy': expectancy,
        'max_consecutive_loss': max_con_loss,
        'max_consecutive_win': max_con_win,
        'avg_hold_days': avg_hold,
        'kelly_fraction': kelly,
    }


def monthly_win_rate(daily_rets):
    """월별 승률 — 일별 수익률을 달력 월 기준으로 합산 후 양수 월 비율 계산.
    Returns: (winning_months, total_months, win_rate_pct)
    주의: daily_rets 리스트에 대응하는 날짜 정보가 없으므로,
          거래일 252일/년 기준으로 약 21일을 1개월로 취급.
    """
    if not daily_rets:
        return (0, 0, 0.0)
    days_per_month = 21
    monthly_sums = []
    for i in range(0, len(daily_rets), days_per_month):
        chunk = daily_rets[i:i + days_per_month]
        if len(chunk) >= 5:  # 최소 5일은 있어야 유효 월
            monthly_sums.append(sum(chunk))
    total_months = len(monthly_sums)
    if total_months == 0:
        return (0, 0, 0.0)
    winning_months = sum(1 for s in monthly_sums if s > 0)
    win_rate_pct = winning_months / total_months * 100
    return (winning_months, total_months, win_rate_pct)


def turnover(trade_log, total_days):
    """연환산 회전율 — 완료 거래 수 / 총 거래일 × 252
    trade_log: list[dict], total_days: 백테스트 총 거래일수
    """
    if not trade_log or total_days <= 0:
        return 0.0
    return len(trade_log) / total_days * 252


# ── 종합 리포트 ──

def report(daily_rets, trade_log=None, label='전략', rf_annual=4.5):
    """종합 성과 리포트 출력"""
    n = len(daily_rets)
    if n == 0:
        print(f'[{label}] 데이터 없음')
        return

    tr = total_return(daily_rets)
    ca = cagr(daily_rets)
    mdd = max_drawdown_compound(daily_rets)
    rec = recovery_days(daily_rets)
    vol = annualized_volatility(daily_rets)
    sh = sharpe_ratio(daily_rets, rf_annual)
    so = sortino_ratio(daily_rets, rf_annual)
    cal = calmar_ratio(daily_rets)

    print(f'\n{"="*50}')
    print(f'  {label} — 종합 성과 리포트')
    print(f'{"="*50}')
    print(f'  기간: {n}거래일')
    print()

    print(f'  ── 수익률 ──')
    print(f'  누적 수익률       {tr:+.2f}%')
    print(f'  연환산 (CAGR)     {ca:+.2f}%')
    print()

    print(f'  ── 위험 ──')
    print(f'  MDD              {mdd:.2f}%')
    print(f'  복구 기간         {rec}일')
    print(f'  연환산 변동성     {vol:.2f}%')
    print()

    print(f'  ── 위험 조정 수익 ──')
    print(f'  샤프 비율         {sh:.2f}')
    print(f'  소르티노 비율     {so:.2f}')
    print(f'  칼마 비율         {cal:.2f}')

    if trade_log:
        ts = trade_stats(trade_log)
        print()
        print(f'  ── 거래 ──')
        print(f'  총 거래           {ts["total_trades"]}건')
        print(f'  승률              {ts["win_rate"]:.1%}')
        print(f'  평균 수익         {ts["avg_return"]:+.2f}%')
        print(f'  평균 이익 (승)    +{ts["avg_win"]:.2f}%')
        print(f'  평균 손실 (패)    -{ts["avg_loss"]:.2f}%')
        print(f'  손익비            {ts["profit_loss_ratio"]:.2f}')
        pf = ts["profit_factor"]
        pf_str = f'{pf:.2f}' if pf != float('inf') else '∞'
        print(f'  프로핏 팩터       {pf_str}')
        print(f'  기대값            {ts["expectancy"]:+.2f}%')
        print(f'  최대 연승         {ts["max_consecutive_win"]}연승')
        print(f'  최대 연패         {ts["max_consecutive_loss"]}연패')
        if ts['avg_hold_days'] > 0:
            print(f'  평균 보유일       {ts["avg_hold_days"]:.0f}일')
        print(f'  켈리 비율         {ts["kelly_fraction"]:.1%}')

        # 연환산 회전율
        to = turnover(trade_log, n)
        print(f'  연환산 회전율     {to:.1f}회')

    # 월별 승률
    wm, tm, wr_pct = monthly_win_rate(daily_rets)
    if tm >= 2:
        print()
        print(f'  ── 월별 ──')
        print(f'  월별 승률         {wm}/{tm}개월 ({wr_pct:.1f}%)')

    print(f'{"="*50}')


def compare(results, rf_annual=4.5):
    """여러 전략 비교 테이블

    results: list of (label, daily_rets, trade_log_or_None)
    """
    print(f'\n{"="*88}')
    print(f'  전략 비교')
    print(f'{"="*88}')

    header = f'{"전략":<16} {"CAGR":>7} {"MDD":>7} {"변동성":>7} {"샤프":>6} {"소르티노":>8} {"칼마":>6} {"승률":>6} {"손익비":>6} {"PF":>6}'
    print(header)
    print('-' * 88)

    for label, daily_rets, tlog in results:
        ca = cagr(daily_rets)
        mdd = max_drawdown_compound(daily_rets)
        vol = annualized_volatility(daily_rets)
        sh = sharpe_ratio(daily_rets, rf_annual)
        so = sortino_ratio(daily_rets, rf_annual)
        cal = calmar_ratio(daily_rets)

        if tlog:
            ts = trade_stats(tlog)
            wr = f'{ts["win_rate"]:.0%}'
            plr = f'{ts["profit_loss_ratio"]:.1f}'
            pf = ts["profit_factor"]
            pf_str = f'{pf:.1f}' if pf != float('inf') else '∞'
        else:
            wr = '-'
            plr = '-'
            pf_str = '-'

        print(f'{label:<16} {ca:>+6.1f}% {mdd:>+6.1f}% {vol:>6.1f}% {sh:>6.2f} {so:>8.2f} {cal:>6.2f} {wr:>6} {plr:>6} {pf_str:>6}')

    print(f'{"="*88}')


if __name__ == '__main__':
    # 테스트: 샘플 데이터
    sample_rets = [0.5, -0.3, 1.2, -2.0, 0.8, 0.3, -0.5, 1.5, -1.0, 0.7] * 25
    sample_trades = [
        {'return': 5.2, 'entry_date': '2026-02-12', 'exit_date': '2026-02-20'},
        {'return': -3.1, 'entry_date': '2026-02-20', 'exit_date': '2026-02-25'},
        {'return': 8.5, 'entry_date': '2026-02-25', 'exit_date': '2026-03-05'},
        {'return': -10.0, 'entry_date': '2026-03-05', 'exit_date': '2026-03-06'},
        {'return': 2.3, 'entry_date': '2026-03-06', 'exit_date': '2026-03-12'},
    ]
    report(sample_rets, sample_trades, label='테스트 (샘플 데이터)')
