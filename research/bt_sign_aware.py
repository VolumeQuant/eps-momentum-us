"""부호 인지 dir_factor + Cap 보너스 BT.

비교 변형 (8개):
  baseline   — control (현재 production v80.2 / pre-γ)
  γ          — cap 시 dir=0 (v80.3 현재 production)
  β2         — cap 시 cap segment 부호 평균에 따라 ±0.3
  opt1       — 부호 일치 시만 (1+|dir|) 강화, 불일치 시 dir=0
  opt2       — fwd_pe_chg 부호 × dir (sign-flip)
  β2_opt1    — β2(cap) + opt1(정상)
  β2_opt2    — β2(cap) + opt2(정상)
  β2_γ       — β2(cap) + γ(no-op normal) — 그냥 cap 영역만 살림
"""
import sqlite3
import shutil
import sys
import math
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import daily_runner as dr

ROOT = Path(__file__).parent.parent
DB_ORIGINAL = Path('/tmp/db_pre_gamma.db') if Path('/tmp/db_pre_gamma.db').exists() else ROOT / 'eps_momentum_data.db'
GRID_DIR = ROOT / 'research' / 'sign_aware_dbs'
GRID_DIR.mkdir(exist_ok=True)
SEG_CAP = 100


def fmt_segments(nc, n7, n30, n60, n90):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    seg1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    seg2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    seg3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    seg4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    return seg1, seg2, seg3, seg4


def _eps_q(segs, exclude_cap=False):
    valid = [s for s in segs if abs(s) < SEG_CAP] if exclude_cap else list(segs)
    min_seg = min(valid) if valid else 0
    return 1.0 + 0.3 * max(-1, min(1, min_seg / 2))


def _direction(segs):
    return (segs[0] + segs[1]) / 2 - (segs[2] + segs[3]) / 2


def _dir_factor(direction):
    return max(-0.3, min(0.3, direction / 30))


def _has_cap(segs):
    return any(abs(s) >= SEG_CAP for s in segs)


# ─────────── 8개 변형 ───────────

def calc_baseline(segs, fwd_pe_chg):
    score = sum(segs)
    df = _dir_factor(_direction(segs))
    eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_gamma(segs, fwd_pe_chg):
    """γ: cap 시 dir=0 (현 v80.3)"""
    score = sum(segs)
    if _has_cap(segs):
        df = 0.0
        eq = _eps_q(segs, exclude_cap=True)
    else:
        df = _dir_factor(_direction(segs))
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_beta2(segs, fwd_pe_chg):
    """β2: cap 시 cap segment 부호 평균에 따라 ±0.3"""
    score = sum(segs)
    if _has_cap(segs):
        cap_signs = [(1 if s > 0 else -1) for s in segs if abs(s) >= SEG_CAP]
        avg_sign = sum(cap_signs) / len(cap_signs)
        df = 0.3 * avg_sign
        eq = _eps_q(segs, exclude_cap=True)
    else:
        df = _dir_factor(_direction(segs))
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_opt1(segs, fwd_pe_chg):
    """opt1: 부호 일치 시만 강화. cap 시 dir=0"""
    score = sum(segs)
    if _has_cap(segs):
        df = 0.0
        eq = _eps_q(segs, exclude_cap=True)
    else:
        direction = _direction(segs)
        df = _dir_factor(direction)
        # 부호 일치 검사: fwd_pe_chg<0(저평가)+direction>0(가속) 또는 그 반대
        sign_match = (fwd_pe_chg is not None and (
            (fwd_pe_chg < 0 and direction > 0) or (fwd_pe_chg > 0 and direction < 0)
        ))
        if not sign_match:
            df = 0.0  # mixed signal → 보정 없음
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_opt2(segs, fwd_pe_chg):
    """opt2: fwd_pe_chg 부호 × dir (sign-flip)"""
    score = sum(segs)
    if _has_cap(segs):
        df = 0.0
        eq = _eps_q(segs, exclude_cap=True)
    else:
        direction = _direction(segs)
        df_raw = _dir_factor(direction)
        # fwd_pe_chg<0 (저평가) 일 때 양수 dir = 강화 → df 그대로
        # fwd_pe_chg>0 (고평가) 일 때 양수 dir = 강화 (매도 강조) → df 부호 반전
        if fwd_pe_chg is not None and fwd_pe_chg > 0:
            df = -df_raw
        else:
            df = df_raw
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_beta2_opt1(segs, fwd_pe_chg):
    """β2 (cap) + opt1 (정상)"""
    score = sum(segs)
    if _has_cap(segs):
        cap_signs = [(1 if s > 0 else -1) for s in segs if abs(s) >= SEG_CAP]
        df = 0.3 * sum(cap_signs) / len(cap_signs)
        eq = _eps_q(segs, exclude_cap=True)
    else:
        direction = _direction(segs)
        df = _dir_factor(direction)
        sign_match = (fwd_pe_chg is not None and (
            (fwd_pe_chg < 0 and direction > 0) or (fwd_pe_chg > 0 and direction < 0)
        ))
        if not sign_match:
            df = 0.0
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_beta2_opt2(segs, fwd_pe_chg):
    """β2 (cap) + opt2 (정상)"""
    score = sum(segs)
    if _has_cap(segs):
        cap_signs = [(1 if s > 0 else -1) for s in segs if abs(s) >= SEG_CAP]
        df = 0.3 * sum(cap_signs) / len(cap_signs)
        eq = _eps_q(segs, exclude_cap=True)
    else:
        direction = _direction(segs)
        df_raw = _dir_factor(direction)
        if fwd_pe_chg is not None and fwd_pe_chg > 0:
            df = -df_raw
        else:
            df = df_raw
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_beta2_only(segs, fwd_pe_chg):
    """β2 only: cap 영역만 보너스. 정상은 baseline 그대로"""
    score = sum(segs)
    if _has_cap(segs):
        cap_signs = [(1 if s > 0 else -1) for s in segs if abs(s) >= SEG_CAP]
        df = 0.3 * sum(cap_signs) / len(cap_signs)
        eq = _eps_q(segs, exclude_cap=True)
    else:
        df = _dir_factor(_direction(segs))
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_beta1(segs, fwd_pe_chg):
    """β1: cap 시 무조건 +0.3 보너스 (사용자 원래 의도)"""
    score = sum(segs)
    if _has_cap(segs):
        df = +0.3  # 무조건 보너스
        eq = _eps_q(segs, exclude_cap=True)
    else:
        df = _dir_factor(_direction(segs))
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


def calc_beta1_opt2(segs, fwd_pe_chg):
    """β1 (cap 보너스) + opt2 (고평가+둔화 sign flip)"""
    score = sum(segs)
    if _has_cap(segs):
        df = +0.3
        eq = _eps_q(segs, exclude_cap=True)
    else:
        direction = _direction(segs)
        df_raw = _dir_factor(direction)
        if fwd_pe_chg is not None and fwd_pe_chg > 0:
            df = -df_raw
        else:
            df = df_raw
        eq = _eps_q(segs)
    return score, df, eq, score * (1 + df)


VARIANTS = {
    'baseline':   calc_baseline,
    'gamma':      calc_gamma,
    'beta1':      calc_beta1,
    'beta2':      calc_beta2_only,
    'opt1':       calc_opt1,
    'opt2':       calc_opt2,
    'b1_opt2':    calc_beta1_opt2,
    'b2_opt1':    calc_beta2_opt1,
    'b2_opt2':    calc_beta2_opt2,
}


def regenerate(test_db, calc_fn):
    original = dr.DB_PATH
    dr.DB_PATH = test_db
    try:
        conn = sqlite3.connect(test_db)
        cur = conn.cursor()
        dates = [r[0] for r in cur.execute(
            'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
        ).fetchall()]

        for today in dates:
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                       adj_gap, rev_up30, num_analysts, rev_growth
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
            ''', (today,)).fetchall()
            if not rows:
                continue

            new_data = []
            for r in rows:
                tk, nc, n7, n30, n60, n90, ag_old, ru, na, rg = r
                segs = fmt_segments(nc, n7, n30, n60, n90)
                if segs is None or ag_old is None:
                    continue
                # baseline 가정으로 fwd_pe_chg 역산
                _, df_base, eq_base, _ = calc_baseline(segs, None)
                denom = (1 + df_base) * eq_base
                if abs(denom) < 1e-6:
                    continue
                fwd_pe_chg = ag_old / denom

                # 변형 적용 (fwd_pe_chg 정보 사용)
                score_n, df_n, eq_n, asc_n = calc_fn(segs, fwd_pe_chg)
                ag_n = fwd_pe_chg * (1 + df_n) * eq_n
                new_data.append((tk, score_n, asc_n, ag_n, ru, na, nc, n90, rg))

            for tk, sc, asc, ag, *_ in new_data:
                cur.execute(
                    'UPDATE ntm_screening SET score=?, adj_score=?, adj_gap=? WHERE date=? AND ticker=?',
                    (sc, asc, ag, today, tk)
                )

            elig_conv = []
            for tk, _, _, ag, ru, na, nc, n90, rg in new_data:
                cg = dr._apply_conviction(ag, ru, na, nc, n90, rev_growth=rg)
                if cg is not None:
                    elig_conv.append((tk, cg))
            elig_conv.sort(key=lambda x: x[1])
            new_cr = {tk: i + 1 for i, (tk, _) in enumerate(elig_conv)}

            cur.execute('UPDATE ntm_screening SET composite_rank=NULL WHERE date=?', (today,))
            for tk, cr in new_cr.items():
                cur.execute(
                    'UPDATE ntm_screening SET composite_rank=? WHERE date=? AND ticker=?',
                    (cr, today, tk)
                )

            tickers = list(new_cr.keys())
            wmap = dr._compute_w_gap_map(cur, today, tickers)
            sorted_w = sorted(tickers, key=lambda t: wmap.get(t, 0), reverse=True)
            top30 = sorted_w[:30]
            cur.execute('UPDATE ntm_screening SET part2_rank=NULL WHERE date=?', (today,))
            for rk, tk in enumerate(top30, 1):
                cur.execute(
                    'UPDATE ntm_screening SET part2_rank=? WHERE date=? AND ticker=?',
                    (rk, today, tk)
                )
            conn.commit()
        conn.close()
    finally:
        dr.DB_PATH = original


def load_picks_prices(db):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT date, ticker FROM ntm_screening
        WHERE part2_rank IS NOT NULL AND part2_rank <= 3 ORDER BY date, part2_rank
    ''').fetchall()
    picks = defaultdict(list)
    for d, tk in rows:
        picks[d].append(tk)
    prices = {(d, tk): p for d, tk, p in cur.execute(
        'SELECT date, ticker, price FROM ntm_screening WHERE price IS NOT NULL'
    ).fetchall() if p and p > 0}
    conn.close()
    return picks, prices


def simulate(picks, prices):
    dates = sorted(picks.keys())
    nav = 1.0
    drets = []
    trades = []
    for i in range(len(dates) - 1):
        d, dn = dates[i], dates[i+1]
        rets = []
        for tk in picks[d]:
            pt, pn = prices.get((d, tk)), prices.get((dn, tk))
            if pt and pn:
                r = pn / pt - 1
                rets.append(r)
                trades.append(r)
        if rets:
            dr_v = sum(rets) / len(rets)
            drets.append(dr_v)
            nav *= (1 + dr_v)
    return nav, drets, trades


def metrics(nav, drets, trades):
    n = len(drets)
    if n == 0: return {}
    avg = sum(drets) / n
    std = math.sqrt(sum((r-avg)**2 for r in drets) / n)
    sharpe = avg/std * math.sqrt(252) if std > 0 else 0
    cum = peak = 1.0
    mdd = 0
    for r in drets:
        cum *= (1 + r)
        peak = max(peak, cum)
        mdd = min(mdd, (cum - peak) / peak)
    wins = sum(1 for r in trades if r > 0)
    return {
        'ret': (nav-1)*100, 'mdd': mdd*100, 'sharpe': sharpe,
        'days': n, 'trades': len(trades),
        'winrate': wins/len(trades)*100 if trades else 0,
    }


def main():
    print('=' * 95)
    print('Sign-Aware dir_factor + Cap 보너스 BT')
    print(f'DB: {DB_ORIGINAL}')
    print('=' * 95)

    rows = []
    for name, fn in VARIANTS.items():
        db = GRID_DIR / f'{name}.db'
        shutil.copy(DB_ORIGINAL, db)
        regenerate(db, fn)
        picks, prices = load_picks_prices(db)
        nav, drets, trades = simulate(picks, prices)
        m = metrics(nav, drets, trades)
        m['name'] = name
        rows.append(m)
        print(f'  {name:<10}: Ret {m["ret"]:+7.2f}%, MDD {m["mdd"]:+7.2f}%, '
              f'Sharpe {m["sharpe"]:.2f}, Trades {m["trades"]}, Win {m["winrate"]:.1f}%')

    print()
    print('=' * 95)
    print('정렬 (Ret 내림차순)')
    print('=' * 95)
    rows.sort(key=lambda x: -x['ret'])
    base = next(r for r in rows if r['name'] == 'baseline')
    gamma = next(r for r in rows if r['name'] == 'gamma')
    hdr = ['Variant', 'Ret%', 'MDD%', 'Sharpe', 'vs base', 'vs gamma']
    print(f'{hdr[0]:<12} {hdr[1]:>8} {hdr[2]:>8} {hdr[3]:>7} {hdr[4]:>9} {hdr[5]:>9}')
    print('-' * 95)
    for r in rows:
        d_base = r['ret'] - base['ret']
        d_gamma = r['ret'] - gamma['ret']
        marker = ''
        if r['name'] == 'baseline':
            marker = ' (control)'
        elif r['name'] == 'gamma':
            marker = ' (현 v80.3)'
        print(f'  {r["name"]:<10} {r["ret"]:+7.2f}% {r["mdd"]:+7.2f}% {r["sharpe"]:>6.2f} '
              f'{d_base:+7.2f}%p {d_gamma:+7.2f}%p{marker}')


if __name__ == '__main__':
    main()
