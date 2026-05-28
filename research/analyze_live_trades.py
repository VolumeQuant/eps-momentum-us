# -*- coding: utf-8 -*-
"""라이브 체결(a.csv) vs 시스템 신호(DB) 괴리 분석.
각 청산 거래를 시스템 픽 / 레이더 / 재량으로 분류하고 손익 분해."""
import sqlite3, csv, io, datetime as dt

CSV = r"C:\Users\user\Downloads\a.csv"
DB = "eps_momentum_data.db"

# --- parse CSV (cp949) ---
with open(CSV, "rb") as f:
    raw = f.read().decode("cp949")
rows = list(csv.reader(io.StringIO(raw)))
hdr = rows[0]
data = rows[1:]
# column indices (0-based)
C_DATE, C_TICK, C_BALQ = 0, 2, 4
C_BUYQ, C_BUYPX = 7, 8
C_SELLQ, C_SELLPX = 11, 12
C_AVGBUY = 18
C_PL_KRW = 22   # 정산가손익 (KRW incl FX)
C_RET = 23      # 수익률 %

trades = []
for r in data:
    if not r or not r[0].strip():
        continue
    d = r[C_DATE].strip()
    t = r[C_TICK].strip()
    sellq = float(r[C_SELLQ] or 0)
    buyq = float(r[C_BUYQ] or 0)
    if sellq > 0:  # closed (realized) leg
        trades.append({
            "date": d, "ticker": t,
            "ret": float(r[C_RET] or 0),
            "pl_krw": float(r[C_PL_KRW] or 0),
            "avgbuy": float(r[C_AVGBUY] or 0),
            "sellpx": float(r[C_SELLPX] or 0),
            "qty": sellq,
        })

# --- DB lookup ---
con = sqlite3.connect(DB)
cur = con.cursor()

def best_rank(ticker, sell_date, lookback=20):
    """매도 전 lookback일 내 최저(최선) part2_rank / composite_rank."""
    d = dt.datetime.strptime(sell_date, "%Y/%m/%d").date()
    start = (d - dt.timedelta(days=lookback)).isoformat()
    end = d.isoformat()
    q = cur.execute(
        "SELECT MIN(part2_rank), MIN(composite_rank) FROM ntm_screening "
        "WHERE ticker=? AND date BETWEEN ? AND ? AND part2_rank IS NOT NULL",
        (ticker, start, end)).fetchone()
    return q

def classify(p2):
    if p2 is None: return "DISCRETIONARY(미신호)"
    if p2 <= 3:   return "SYSTEM(매수신호)"
    if p2 <= 8:   return "RADAR(보유권)"
    if p2 <= 20:  return "WATCHLIST"
    return "DISCRETIONARY(순위밖)"

print(f"{'날짜':<11}{'종목':<6}{'수익률%':>8}{'손익(원)':>12}  {'best_p2':>7} {'best_cr':>7}  분류")
print("-"*70)
cat_pl, cat_n = {}, {}
for tr in sorted(trades, key=lambda x: x["date"]):
    p2, cr = best_rank(tr["ticker"], tr["date"])
    cat = classify(p2)
    cat_pl[cat] = cat_pl.get(cat, 0) + tr["pl_krw"]
    cat_n[cat] = cat_n.get(cat, 0) + 1
    print(f"{tr['date']:<11}{tr['ticker']:<6}{tr['ret']:>8.2f}{tr['pl_krw']:>12,.0f}  "
          f"{str(p2):>7} {str(cr):>7}  {cat}")

print("\n=== 분류별 집계 ===")
tot = sum(cat_pl.values())
for cat in sorted(cat_pl, key=lambda c: -cat_pl[c]):
    print(f"{cat:<22} {cat_n[cat]:>2}건  {cat_pl[cat]:>+13,.0f}원  ({cat_pl[cat]/tot*100:>5.1f}%)")
print(f"{'합계':<22} {sum(cat_n.values()):>2}건  {tot:>+13,.0f}원")

# win rate by category
print("\n=== 분류별 승률/평균수익률 ===")
from collections import defaultdict
byc = defaultdict(list)
for tr in trades:
    p2, _ = best_rank(tr["ticker"], tr["date"])
    byc[classify(p2)].append(tr["ret"])
for cat, rets in sorted(byc.items()):
    wins = sum(1 for x in rets if x > 0)
    print(f"{cat:<22} 승률 {wins}/{len(rets)} ({wins/len(rets)*100:.0f}%)  평균 {sum(rets)/len(rets):+.2f}%")
con.close()
