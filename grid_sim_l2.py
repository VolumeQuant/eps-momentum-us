"""
Level 2 시뮬 엔진
- conv_gap 계산 (conviction 재현)
- 각 날짜 z-score 변환 (30~100)
- 3일 가중 (0.5/0.3/0.2, 빈날 30점)
- 3일 검증 필터 (T-1, T-2에 eligible 종목만)
- Top N 선정 → fwd10 수익률
"""
import pandas as pd
import numpy as np


def compute_conviction(df):
    """conviction = max(rev_up/num_analysts, eps_floor) + rev_bonus
    Returns: pd.Series (conviction 값)
    """
    ratio = np.where(
        (df['num_analysts'] > 0),
        df['rev_up30'] / df['num_analysts'].replace(0, np.nan),
        0
    )
    ratio = np.nan_to_num(ratio, nan=0)

    # eps_floor = min(|ntm_current - ntm_90d| / |ntm_90d|, 1)
    eps_floor = np.where(
        (df['ntm_90d'].abs() > 0.01),
        np.minimum(np.abs((df['ntm_current'] - df['ntm_90d']) / df['ntm_90d']), 1),
        0
    )
    eps_floor = np.nan_to_num(eps_floor, nan=0)

    base_conv = np.maximum(ratio, eps_floor)
    rev_bonus_val = np.where(df['rev_growth'].fillna(0) >= 0.30, 0.3, 0)
    return base_conv + rev_bonus_val


def simulate_l2(df, mode, params, top_n=3, require_3d=True):
    """
    mode: 'baseline' / 'A_conv_add' / 'B_score_add' / 'C_conv_mult'
    params: 보너스 파라미터
    """
    d = df.copy()
    # ntm_current 회복 필요 (캐시에는 ntm_current가 있을 것)
    # 없으면 ntm_7d 기준 역산 필요 — 체크
    if 'ntm_current' not in d.columns:
        raise ValueError("ntm_current 컬럼 필요")

    # 1. 기본 conviction 계산
    d['conviction_base'] = compute_conviction(d)

    # 2. Case 1 조건 평가
    if mode != 'baseline':
        case1 = (d['ntm_7d_chg'] > params['ntm_thr']) & (d['px_7d_chg'] < params['px_thr'])
    else:
        case1 = pd.Series(False, index=d.index)

    # 3. 방식별 conv_gap 계산
    if mode in ('baseline', 'B_score_add'):
        # conv_gap = adj_gap × (1 + conviction)
        d['conv_gap'] = d['adj_gap'] * (1 + d['conviction_base'])
    elif mode == 'A_conv_add':
        case1_add = np.where(case1, params['bonus'], 0)
        d['conv_gap'] = d['adj_gap'] * (1 + d['conviction_base'] + case1_add)
    elif mode == 'C_conv_mult':
        # conv_gap × (1 - bonus) — 음수일수록 좋으니까 -를 해줘야 더 음수
        d['conv_gap_base'] = d['adj_gap'] * (1 + d['conviction_base'])
        bonus_arr = np.where(case1, params['mult'], 0)
        # 음수 값일 때 (1+mult) 곱하면 더 음수 (저평가 강화)
        d['conv_gap'] = d['conv_gap_base'] * (1 + bonus_arr)

    # 4. eligible (composite_rank NOT NULL) 내에서 z-score 변환 (30~100)
    d['score'] = np.nan
    for date, grp in d.groupby('date'):
        elig_mask = grp['comp_rank'].notna()
        if elig_mask.sum() < 2:
            continue
        vals = grp.loc[elig_mask, 'conv_gap']
        mean_v = vals.mean()
        std_v = vals.std()
        if std_v > 0:
            score = np.clip(65 + (-(vals - mean_v) / std_v) * 15, 30, 100)
        else:
            score = pd.Series(65.0, index=vals.index)
        d.loc[score.index, 'score'] = score

    # 5. 방식 B: z-score 후 덧셈
    if mode == 'B_score_add':
        case1_point = np.where(case1, params['bonus'], 0)
        d.loc[d['score'].notna(), 'score'] = d.loc[d['score'].notna(), 'score'] + case1_point[d['score'].notna()]
        d['score'] = np.clip(d['score'], 30, 100)  # 상한 유지

    # 6. 3일 가중
    d = d.sort_values(['ticker', 'date']).reset_index(drop=True)
    d['score_t1'] = d.groupby('ticker')['score'].shift(1)
    d['score_t2'] = d.groupby('ticker')['score'].shift(2)

    # 빈 날 = 30점 (MISSING_PENALTY)
    MISS = 30
    d['score'] = d['score'].fillna(MISS)
    d['score_t1'] = d['score_t1'].fillna(MISS)
    d['score_t2'] = d['score_t2'].fillna(MISS)

    d['w_gap'] = d['score']*0.5 + d['score_t1']*0.3 + d['score_t2']*0.2

    # 7. 3일 검증 필터
    d['verified_3d'] = (
        d.groupby('ticker')['comp_rank'].shift(1).notna() &
        d.groupby('ticker')['comp_rank'].shift(2).notna() &
        d['comp_rank'].notna()
    )

    # 8. 매일 Top N (3일 검증 종목만, fwd10 있는 것만)
    daily_rets = []
    for date, grp in d.groupby('date'):
        if require_3d:
            cand = grp[grp['verified_3d'] & grp['comp_rank'].notna()]
        else:
            cand = grp[grp['comp_rank'].notna()]
        if len(cand) < top_n:
            continue
        top = cand.nlargest(top_n, 'w_gap')
        ret = top['ret_10d'].mean()
        if not pd.isna(ret):
            daily_rets.append(ret)

    if not daily_rets:
        return None

    rets = np.array(daily_rets)
    cum = np.cumprod(1 + rets/100) - 1
    rolling_max = np.maximum.accumulate(cum + 1)
    dd = (cum + 1 - rolling_max) / rolling_max

    return {
        'n': len(rets),
        'mean_10d': rets.mean(),
        'median_10d': np.median(rets),
        'win_rate': (rets > 0).mean() * 100,
        'std_10d': rets.std(),
        'sharpe': rets.mean() / rets.std() if rets.std() > 0 else 0,
        'total_cum': cum[-1] * 100,
        'mdd': dd.min() * 100,
    }
