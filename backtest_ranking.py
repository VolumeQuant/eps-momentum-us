import sys, sqlite3, copy
import numpy as np
sys.stdout.reconfigure(encoding="utf-8")

DB = r"C:\dev\claude-code\eps-momentum-us\eps_momentum_data.db"

def get_eligible(cur, date):
    cur.execute("""SELECT ticker, adj_gap, rev_growth, price, ma60, adj_score,
               num_analysts, rev_up30, rev_down30
        FROM ntm_screening WHERE date = ?
          AND adj_score > 9 AND adj_gap IS NOT NULL
          AND price IS NOT NULL AND price >= 10
          AND ma60 IS NOT NULL AND price > ma60
          AND rev_growth IS NOT NULL AND rev_growth >= 0.10""", (date,))
    out = []
    for r in cur.fetchall():
        tk, ag, rg, pr, m60, asc, na, ru, rd = r
        if na is not None and na < 3: continue
        if ru is not None and rd is not None:
            tot = ru + rd
            if tot > 0 and rd / tot > 0.3: continue
        out.append(dict(ticker=tk, adj_gap=ag, rev_growth=rg, price=pr))
    return out

def rank_stocks(stocks, method):
    if not stocks: return []
    g = np.array([s["adj_gap"] for s in stocks])
    r = np.array([s["rev_growth"] for s in stocks])
    gm, gs = np.mean(g), np.std(g)
    rm, rs = np.mean(r), np.std(r)
    zg = (g - gm)/gs if gs > 0 else np.zeros_like(g)
    zr = (r - rm)/rs if rs > 0 else np.zeros_like(r)
    comp = (-zg)*0.7 + zr*0.3 if method == "A" else (-zg)*1.0
    for i, s in enumerate(stocks):
        s["comp"] = comp[i]; s["zg"] = zg[i]; s["zr"] = zr[i]
    ranked = sorted(stocks, key=lambda x: x["comp"], reverse=True)
    for i, s in enumerate(ranked): s["rank"] = i+1
    return ranked

def price_at(cur, tk, dt):
    cur.execute("SELECT price FROM ntm_screening WHERE date=? AND ticker=?", (dt, tk))
    r = cur.fetchone()
    return r[0] if r else None

def fp(v, w=8):
    s = "+" if v >= 0 else ""
    return f"{s}{v:.3%}".rjust(w)

def fp4(v, w=20):
    s = "+" if v >= 0 else ""
    return f"{s}{v:.4%}".rjust(w)

conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute("SELECT DISTINCT date FROM ntm_screening ORDER BY date")
dates = [r[0] for r in cur.fetchall()]
print(f"Dates: {len(dates)}")
for d in dates: print(f"  {d}")
print()
res = {"A": {"t5":[], "t10":[], "t30":[]}, "B": {"t5":[], "t10":[], "t30":[]}}
details = []
disagree = {"oA":[], "oB":[]}

for i in range(len(dates)-1):
    dt, dt1 = dates[i], dates[i+1]
    el = get_eligible(cur, dt)
    if len(el) < 5:
        print(f"  {dt}: {len(el)} eligible, skip")
        continue
    rA = rank_stocks(copy.deepcopy(el), "A")
    rB = rank_stocks(copy.deepcopy(el), "B")
    det = {"dt": dt, "dt1": dt1, "ne": len(el)}
    for mn, rk in [("A", rA), ("B", rB)]:
        for tl, tn in [("t5",5),("t10",10),("t30",30)]:
            an = min(tn, len(rk))
            rl = []
            for s in rk[:an]:
                p1 = price_at(cur, s["ticker"], dt1)
                if p1 and s["price"] > 0:
                    rl.append((p1 - s["price"])/s["price"])
            av = np.mean(rl) if rl else 0.0
            res[mn][tl].append(av)
            det[f"{mn}_{tl}"] = av
    t5A = set(s["ticker"] for s in rA[:5])
    t5B = set(s["ticker"] for s in rB[:5])
    oA = t5A - t5B; oB = t5B - t5A; ov = t5A & t5B
    det["ov"] = ov; det["oA"] = oA; det["oB"] = oB
    det["oAd"] = []; det["oBd"] = []
    det["At5"] = [(s["ticker"],s["rank"],s["adj_gap"],s["rev_growth"]) for s in rA[:5]]
    det["Bt5"] = [(s["ticker"],s["rank"],s["adj_gap"],s["rev_growth"]) for s in rB[:5]]
    for tk in sorted(oA):
        sx = next(x for x in rA if x["ticker"]==tk)
        p1 = price_at(cur, tk, dt1)
        ret = (p1-sx["price"])/sx["price"] if p1 and sx["price"]>0 else None
        rb = next((x["rank"] for x in rB if x["ticker"]==tk), None)
        det["oAd"].append(dict(tk=tk, ret=ret, rA=sx["rank"], rB=rb,
            ag=sx["adj_gap"], rg=sx["rev_growth"], zg=sx["zg"], zr=sx["zr"]))
        if ret is not None: disagree["oA"].append(ret)
    for tk in sorted(oB):
        sx = next(x for x in rB if x["ticker"]==tk)
        p1 = price_at(cur, tk, dt1)
        ret = (p1-sx["price"])/sx["price"] if p1 and sx["price"]>0 else None
        ra = next((x["rank"] for x in rA if x["ticker"]==tk), None)
        sA = next(x for x in rA if x["ticker"]==tk)
        det["oBd"].append(dict(tk=tk, ret=ret, rA=ra, rB=sx["rank"],
            ag=sx["adj_gap"], rg=sA["rev_growth"], zg=sx["zg"], zr=sA["zr"]))
        if ret is not None: disagree["oB"].append(ret)
    details.append(det)

SEP = chr(61) * 110
DSEP = chr(45) * 110
print(SEP)
print("BACKTEST: Method A (70% gap + 30% rev) vs Method B (100% gap)")
print(SEP)
print()
print(DSEP)
print("PER-DATE PORTFOLIO RETURNS (equal-weight)")
print(DSEP)
cols = ["Date", "Next", "#Elig", "A-Top5", "A-T10", "A-T30", "B-Top5", "B-T10", "B-T30", "A-B T5"]
hdr = f"{cols[0]:<12} {cols[1]:<12} {cols[2]:>6} | {cols[3]:>8} {cols[4]:>8} {cols[5]:>8} | {cols[6]:>8} {cols[7]:>8} {cols[8]:>8} | {cols[9]:>8}"
print(hdr)
print(DSEP)
for d in details:
    diff = d["A_t5"] - d["B_t5"]
    print(f"{d['dt']:<12} {d['dt1']:<12} {d['ne']:>6} | {fp(d['A_t5'])} {fp(d['A_t10'])} {fp(d['A_t30'])} | {fp(d['B_t5'])} {fp(d['B_t10'])} {fp(d['B_t30'])} | {fp(diff)}")

print()
print(DSEP)
print("SUMMARY STATISTICS")
print(DSEP)
for lab, tn in [("Top 5","t5"),("Top 10","t10"),("Top 30","t30")]:
    aR = np.array(res["A"][tn]); bR = np.array(res["B"][tn])
    ac = np.prod(1+aR)-1; bc = np.prod(1+bR)-1
    am = np.mean(aR); bm = np.mean(bR)
    asd = np.std(aR); bsd = np.std(bR)
    ash = am/asd if asd>0 else 0; bsh = bm/bsd if bsd>0 else 0
    aw = int(np.sum(aR>0)); bw = int(np.sum(bR>0))
    print()
    print(f"  {lab}:")
    h = f"    {'':<20} {'Method A (70/30)':>20} {'Method B (gap-only)':>20} {'Diff (A-B)':>15}"
    print(h)
    print(f"    {'Avg daily return':<20} {fp4(am)} {fp4(bm)} {fp4(am-bm,15)}")
    print(f"    {'Cumulative return':<20} {fp4(ac)} {fp4(bc)} {fp4(ac-bc,15)}")
    print(f"    {'Std dev':<20} {asd:>20.4%} {bsd:>20.4%}")
    print(f"    {'Daily Sharpe':<20} {ash:>20.3f} {bsh:>20.3f}")
    wA = str(aw)+"/"+str(len(aR)); wB = str(bw)+"/"+str(len(bR))
    print(f"    {'Win days':<20} {wA:>20} {wB:>20}")

print()
print(DSEP)
print("CUMULATIVE RETURN SERIES (Top 5)")
print(DSEP)
aCS = np.cumprod(1+np.array(res["A"]["t5"]))
bCS = np.cumprod(1+np.array(res["B"]["t5"]))
for i, d in enumerate(details):
    sp = aCS[i]-bCS[i]
    ss = "+" if sp>=0 else ""
    print(f"  {d['dt']} -> {d['dt1']}: A={aCS[i]:>8.4f}  B={bCS[i]:>8.4f}  Spread={ss}{sp:.4f}")

print()
print(DSEP)
print("TOP 5 COMPOSITION PER DATE")
print(DSEP)
for d in details:
    print()
    anames = ", ".join(t[0] for t in d["At5"])
    bnames = ", ".join(t[0] for t in d["Bt5"])
    print(f"  Date: {d['dt']} -> {d['dt1']}  (Eligible: {d['ne']})")
    print(f"    Method A Top 5: {anames}")
    print(f"    Method B Top 5: {bnames}")
    ol = ", ".join(sorted(d["ov"])) if d["ov"] else "NONE"
    print(f"    Overlap: {ol}")
    if d["oA"]:
        print("    Only in A (rev_growth lifted them):")
        for sx in d["oAd"]:
            if sx["ret"] is not None:
                sgn = "+" if sx["ret"]>=0 else ""
                rst = f"{sgn}{sx['ret']:.3%}"
            else:
                rst = "N/A"
            print(f"      {sx['tk']:<8} rkA={sx['rA']:<3} rkB={sx['rB']:<3} adj_gap={sx['ag']:>+.2f} rev_g={sx['rg']:.1%} zg={sx['zg']:>+.2f} zr={sx['zr']:>+.2f} => {rst}")
    if d["oB"]:
        print("    Only in B (gap-only picked them):")
        for sx in d["oBd"]:
            if sx["ret"] is not None:
                sgn = "+" if sx["ret"]>=0 else ""
                rst = f"{sgn}{sx['ret']:.3%}"
            else:
                rst = "N/A"
            print(f"      {sx['tk']:<8} rkA={sx['rA']:<3} rkB={sx['rB']:<3} adj_gap={sx['ag']:>+.2f} rev_g={sx['rg']:.1%} zg={sx['zg']:>+.2f} zr={sx['zr']:>+.2f} => {rst}")

print()
print(DSEP)
print("DISAGREEMENT ANALYSIS")
print(DSEP)
oAr = disagree["oA"]; oBr = disagree["oB"]
print()
print(f"  Only-A stocks (rev_growth boosted): {len(oAr)} stock-days")
if oAr:
    m=np.mean(oAr); md=np.median(oAr); w=sum(1 for r in oAr if r>0)
    ms = "+" if m>=0 else ""
    mds = "+" if md>=0 else ""
    print(f"    Avg: {ms}{m:.4%}  Median: {mds}{md:.4%}  Win: {w}/{len(oAr)}")
print()
print(f"  Only-B stocks (gap-only picked): {len(oBr)} stock-days")
if oBr:
    m=np.mean(oBr); md=np.median(oBr); w=sum(1 for r in oBr if r>0)
    ms = "+" if m>=0 else ""
    mds = "+" if md>=0 else ""
    print(f"    Avg: {ms}{m:.4%}  Median: {mds}{md:.4%}  Win: {w}/{len(oBr)}")
if oAr and oBr:
    dd = np.mean(oAr)-np.mean(oBr)
    dds = "+" if dd>=0 else ""
    print()
    print(f"  ==> Diff: {dds}{dd:.4%}/day")
    if dd > 0:
        print("      rev_growth weighting HELPED (A picks outperformed B picks)")
    else:
        print("      rev_growth weighting HURT (B picks outperformed A picks)")

print()
print(DSEP)
print("SPY BENCHMARK: No SPY data in ntm_screening. Skipped.")
print(DSEP)

print()
print(SEP)
print("FINAL COMPARISON (Top 5, equal-weight)")
print(SEP)
a5 = res["A"]["t5"]; b5 = res["B"]["t5"]
acf = np.prod(1+np.array(a5))-1; bcf = np.prod(1+np.array(b5))-1
aaf = np.mean(a5); baf = np.mean(b5)
ab = sum(1 for a,b in zip(a5,b5) if a>b)
ba = sum(1 for a,b in zip(a5,b5) if b>a)
ti = sum(1 for a,b in zip(a5,b5) if a==b)
print(f"  Days tested: {len(a5)}")
acfs = "+" if acf>=0 else ""
bcfs = "+" if bcf>=0 else ""
aafs = "+" if aaf>=0 else ""
bafs = "+" if baf>=0 else ""
print(f"  Method A cumulative: {acfs}{acf:.4%}")
print(f"  Method B cumulative: {bcfs}{bcf:.4%}")
print(f"  Method A avg daily:  {aafs}{aaf:.4%}")
print(f"  Method B avg daily:  {bafs}{baf:.4%}")
print(f"  A beat B: {ab} | B beat A: {ba} | Ties: {ti}")
print()
if acf > bcf:
    print(f"  >>> Method A (70/30) outperformed by +{acf-bcf:.4%}")
elif bcf > acf:
    print(f"  >>> Method B (gap-only) outperformed by +{bcf-acf:.4%}")
else:
    print("  >>> Both methods tied")
print()
print(f"  CAVEAT: Only {len(a5)} trading days. Directional only, not statistically significant.")
print(SEP)
conn.close()
