# -*- coding: utf-8 -*-
"""90/10 비중으로 시스템 시작일부터 적용한 매매내역 상세."""
import sqlite3, statistics, math

DB = "eps_momentum_data.db"
W_HIGH, W_LOW = 0.9, 0.1

con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute(
    "SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date")]
day = {}; price_map = {}
for d in dates:
    rows = cur.execute(
        "SELECT ticker, part2_rank, composite_rank, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price "
        "FROM ntm_screening WHERE date=?", (d,)).fetchall()
    day_d = {}; price_d = {}
    for r in rows:
        t, p2, cr, n0, n7, n30, n60, n90, px = r
        def seg(a, b):
            if a is None or b is None or b == 0: return None
            return (a - b) / abs(b) * 100
        segs = [s for s in [seg(n0,n7), seg(n7,n30), seg(n30,n60), seg(n60,n90)] if s is not None]
        ms = min(segs) if segs else None
        if p2 is not None: day_d[t] = (p2, cr, ms)
        if px is not None: price_d[t] = px
    day[d] = day_d; price_map[d] = price_d
con.close()

def verified_cr(t, i):
    for j in (i, i-1, i-2):
        if j < 0: return False
        info = day[dates[j]].get(t)
        if not info or info[1] is None or info[1] > 30: return False
    return True

# === Run simulator with detailed logging ===
trades = []        # (date, action, ticker, price, slot_at_action, entry_price_if_buy)
daily_value = []   # (date, value, held_dict)
held = {}          # ticker -> entry_price (entry date)
held_entry_date = {}
prev_held = None
value = 1.0; peak = 1.0; mdd = 0.0

for i, d in enumerate(dates):
    # 1) carry from prev day with weights
    if prev_held and i > 0:
        d_prev = dates[i-1]
        items = list(prev_held.keys())
        ranks = sorted(((t, day[d_prev].get(t,(999,None,None))[0] or 999) for t in items), key=lambda x: x[1])
        n = len(items); ret = 0
        if n == 1:
            t = items[0]
            pp = price_map[d_prev].get(t); pn = price_map[d].get(t, pp)
            if pp and pn: ret = pn/pp - 1
        elif n == 2:
            wmap = {ranks[0][0]: W_HIGH, ranks[1][0]: W_LOW}
            for t, w in wmap.items():
                pp = price_map[d_prev].get(t); pn = price_map[d].get(t, pp)
                if pp and pn: ret += w*(pn/pp - 1)
        value *= (1+ret)
        peak = max(peak, value); mdd = max(mdd, (peak-value)/peak)

    # 2) exits
    dd = day[d]
    for t in list(held):
        info = dd.get(t)
        sell_reason = None
        if info is None:
            sell_reason = "랭킹 이탈(NULL)"
            px = price_map[d].get(t, held[t])
        elif info[0] is not None and info[0] > 10:
            sell_reason = f"rank>{10} (p2={info[0]})"
            px = price_map[d].get(t, held[t])
        elif info[2] is not None and info[2] < -2:
            sell_reason = f"min_seg<{-2}% ({info[2]:.1f}%)"
            px = price_map[d].get(t, held[t])
        if sell_reason:
            entry_price = held[t]
            ret_pct = (px/entry_price - 1) * 100
            trades.append({
                "date": d, "act": "SELL", "ticker": t, "price": px,
                "entry_price": entry_price, "ret_pct": ret_pct,
                "entry_date": held_entry_date[t], "reason": sell_reason,
            })
            del held[t]; del held_entry_date[t]

    # 3) entries
    if len(held) < 2:
        cands = sorted([(info[0], t) for t, info in dd.items()
                        if info[0] is not None and info[0] <= 2
                        and (info[2] is None or info[2] >= 0)], key=lambda x: x[0])
        for p2, t in cands:
            if len(held) >= 2: break
            if t in held: continue
            if not verified_cr(t, i): continue
            entry_p = price_map[d].get(t, 0)
            held[t] = entry_p
            held_entry_date[t] = d
            trades.append({"date": d, "act": "BUY", "ticker": t, "price": entry_p,
                           "p2": p2})

    # daily weight snapshot for log
    if held:
        ranks_now = sorted(((t, day[d].get(t,(999,None,None))[0] or 999) for t in held), key=lambda x: x[1])
        if len(ranks_now) == 1:
            wts = {ranks_now[0][0]: 1.0}
        else:
            wts = {ranks_now[0][0]: W_HIGH, ranks_now[1][0]: W_LOW}
    else:
        wts = {}
    daily_value.append((d, value, dict(held), wts))
    prev_held = dict(held)

# ============================================================
# Output
# ============================================================
print("="*78)
print(f"90/10 적용 매매내역 ({dates[0]} ~ {dates[-1]}, {len(dates)}거래일)")
print("="*78)

print("\n📅 일자별 매매 타임라인")
print(f"{'date':<12}{'act':<5}{'ticker':<8}{'price':>10}{'slot/weight':<14}{'ret%':>8}  비고")
print("-"*78)
for tr in trades:
    if tr["act"] == "BUY":
        slot = "1등(90%)" if tr["p2"] == 1 else "2등(10%)"
        print(f"{tr['date']:<12}{tr['act']:<5}{tr['ticker']:<8}{tr['price']:>10.2f}  {slot:<12}{'':<8}  매수 (p2={tr['p2']})")
    else:
        held_days = (i for i in range(len(dates)) if dates[i] == tr["date"])
        # held duration
        d1 = tr["entry_date"]; d2 = tr["date"]
        n_days = dates.index(d2) - dates.index(d1)
        print(f"{tr['date']:<12}{tr['act']:<5}{tr['ticker']:<8}{tr['price']:>10.2f}  {'':<12}{tr['ret_pct']:>+7.1f}%  {tr['reason']} ({n_days}일 보유, {d1}~)")

# Per-ticker realized
print("\n💰 종목별 실현 수익률 (청산 완료된 거래)")
print(f"{'ticker':<8}{'buy date':<12}{'sell date':<12}{'days':>5}{'buy $':>10}{'sell $':>10}{'gross %':>10}")
print("-"*78)
realized = [t for t in trades if t["act"]=="SELL"]
ticker_summary = {}
for tr in realized:
    days = dates.index(tr["date"]) - dates.index(tr["entry_date"])
    print(f"{tr['ticker']:<8}{tr['entry_date']:<12}{tr['date']:<12}{days:>5}{tr['entry_price']:>10.2f}{tr['price']:>10.2f}{tr['ret_pct']:>+9.1f}%")
    ticker_summary.setdefault(tr["ticker"], []).append(tr["ret_pct"])

# Aggregate per ticker
print(f"\n  {'ticker':<8}{'trades':>8}{'mean %':>10}{'best %':>10}{'worst %':>10}")
print("-"*48)
for t, rets in sorted(ticker_summary.items(), key=lambda x: -statistics.mean(x[1])):
    print(f"  {t:<8}{len(rets):>8}{statistics.mean(rets):>+9.1f}%{max(rets):>+9.1f}%{min(rets):>+9.1f}%")

# Current holdings
print("\n📌 현재 보유 (시뮬레이션 기준)")
last_d = dates[-1]
for t, ep in held.items():
    cur_p = price_map[last_d].get(t, ep)
    unreal = (cur_p/ep - 1) * 100
    n_days = dates.index(last_d) - dates.index(held_entry_date[t])
    rank_now = day[last_d].get(t, (None,))[0]
    weight_now = "1등(90%)" if rank_now == min(day[last_d].get(x, (999,))[0] or 999 for x in held) else "2등(10%)"
    print(f"  {t:<6} 매수 {held_entry_date[t]} @ ${ep:.2f}  →  최근 ${cur_p:.2f}  {unreal:+.1f}%  ({n_days}일 보유, 현재 {weight_now}, p2={rank_now})")

# Total stats
print("\n📈 포트폴리오 누적 성과 (90/10 적용)")
final_v = daily_value[-1][1]
total_ret = (final_v - 1) * 100
print(f"  시작 자산: 1.00 (100%)")
print(f"  현재 자산: {final_v:.4f} ({total_ret:+.1f}%)")
print(f"  MDD: {mdd*100:.1f}%")
print(f"  Calmar (수익/MDD): {(total_ret/100)/mdd:.2f}")
print(f"  거래 횟수: 매수 {sum(1 for t in trades if t['act']=='BUY')}회 / 매도 {len(realized)}회")
print(f"  실현 거래 승률: {sum(1 for t in realized if t['ret_pct']>0)}/{len(realized)} ({sum(1 for t in realized if t['ret_pct']>0)/len(realized)*100:.0f}%)")
print(f"  평균 보유일: {statistics.mean([dates.index(t['date'])-dates.index(t['entry_date']) for t in realized]):.1f}일")

# Best/worst trade
best = max(realized, key=lambda x: x["ret_pct"])
worst = min(realized, key=lambda x: x["ret_pct"])
print(f"  최고: {best['ticker']} {best['ret_pct']:+.1f}% ({best['entry_date']}~{best['date']})")
print(f"  최악: {worst['ticker']} {worst['ret_pct']:+.1f}% ({worst['entry_date']}~{worst['date']})")

# 1만 달러 시작 환산
print("\n💵 1만 달러 시작 환산")
print(f"  $10,000 → ${final_v*10000:,.0f}  (수익 ${(final_v-1)*10000:+,.0f})")
