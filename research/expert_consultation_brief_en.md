# Expert Consultation Brief — US EPS-Revision Momentum System

> **Purpose:** Get a second opinion from a quant / investment professional on a live, self-built equity strategy. Written to be self-contained (no codebase knowledge needed). We deliberately foreground the *limitations* — we want stress-testing, not validation.

---

## TL;DR (the decision at stake)

We run a concentrated (2-position) US equity strategy that buys stocks where **analyst EPS estimates are being revised up while the forward P/E hasn't yet caught up** (forward-PE compression). It has been live with real capital for ~4 months and shows a large cumulative return — but that return is **dominated by 2 semiconductor names during an AI bull market**, and our robustness test (leave-one-winner-out) suggests much of the apparent edge may be those 2 names, not a generalizable signal.

We just made a discretionary change (excluding two consumer sectors) and want an expert view on **(a) whether the strategy has real edge or is riding AI beta + idiosyncratic luck, and (b) whether decisions like the sector exclusion are sound risk management or curve-fitting on a tiny sample.**

---

## 1. Strategy mechanics

- **Universe:** ~1,270 US large/mid-cap stocks (S&P 500 + 400 + Nasdaq 100 + dynamic $5B+ screen).
- **Core signal:** `adj_gap = (forward-PE change) × (1 + direction) × (EPS quality)`. Ranks highest the names where forward estimates are rising but price/PE lags — i.e., a revision-momentum + relative-value hybrid.
- **Portfolio:** top **2 names** by a 3-day weighted rank (high concentration). Dynamic weighting: if the #1–#2 score gap ≥ 15, go 100/0; else 50/50.
- **Entry gates:** 3 consecutive days in the top ranks; all 4 EPS-revision sub-windows positive; exclude names down >25% from 30-day high; revenue growth ≥10%; exclude structurally low-margin / loss-making names; require ≥3 analysts revising up (anti single-analyst).
- **Exit:** drops out of top 10, or earnings deterioration.
- **Regime overlay:** S&P 500 below its 200-DMA (15-day confirm) OR VIX spike → defensive mode (rotate to bonds). Currently in risk-on ("boost") mode.
- **Operation:** runs automatically after each US close, pushes signals to Telegram. ~4 months live with real money.

## 2. Performance — read with caveats

- **System cumulative +252.6% over 72 trading days** vs S&P 500 +11.1% same window.
- ⚠️ This is a **4-month figure, not annualized**, during a single regime (AI-led bull / "boost").
- The **bulk of the return comes from 2 names: MU (+103%) and SNDK (+126%).**
- Recent: peaked +336.8% (May 26) → +302.9% (Jun 1), a **−7.8% portfolio drawdown** as the two winners were trimmed and the next-tier names lagged.

## 3. Critical limitations (the binding constraint on everything)

- **Backtest = 75 trading days, single bull regime, in-sample.** (Data only goes back to Feb 2026.)
- **Two stocks (MU/SNDK) dominate the sample** (~60 top-3 appearances each).
- **Leave-one-winner-out cross-validation:** every *selection* filter we tried (low-PEG, low-PE, high-growth, price-momentum/MA20) looks excellent on the full sample (+35 to +58 ppts, 95–100/100 paired wins) — **but removing either MU or SNDK flips the edge to −25 to −67 ppts.** So the apparent alpha may largely be concentration in 2 idiosyncratic winners.
- For comparison, our sister strategy (Korean equities, same author) is validated on 7.4 years; the US system has only ~4 months.

## 4. The decision we just made (v85) — sector blocklist

- **Trigger:** Warner Music Group (WMG; revenue growth 17%, a one-off earnings-beat catalyst) surfaced as a buy candidate. The operator strongly objected — it doesn't fit the intent of "own only overwhelming-growth businesses."
- **We could not separate it with any numeric filter:** growth / PEG / PE / price-momentum / revision-concentration all either (i) collapsed under leave-one-winner-out, or (ii) also cut genuine winners. **Numerically WMG is nearly identical to a real winner, FORM** (semis, rev growth 14%, +31% realized): lower growth, higher PE, *more* back-loaded revision — yet FORM won. The only real difference is the *business* (music label vs semiconductors).
- **Action:** exclude `Entertainment` and `Specialty Retail` industries at the eligibility stage (same mechanism as a pre-existing commodity-sector blocklist).
  - Backtest: across 300 paired runs, **0 winning trades blocked, +0.00 ppt return change** — i.e., zero cost, but also zero return improvement.
  - Honest framing: this is **not a discovered statistical edge — it's a value judgment** ("these sectors are out-of-mandate"), analogous to the existing commodity exclusion.
- **Status:** validated on a branch (live test run correctly excludes WMG; new picks = KEYS + FAF). **Merge-to-production is on hold pending this consultation.**

## 5. Operator self-check — "Am I just chasing AI?"

- This window: AI-related names averaged **+32% (89% win)** vs non-AI **+11.5% (67% win)** — so the AI tilt has been *correct recently*, not irrational.
- But the non-AI average is dragged down by one consumer-retail name (FIVE, −2.2%). Industrial/financial non-AI names (MOD +24.9%, FIX +27%, VIRT — capital markets, PEG 0.27, +25.7%) performed like the AI names.
- → The real dividing line looks like **"growth/industrial/quality vs consumer-retail/media,"** not "AI vs non-AI." The two excluded sectors fall in the weak (consumer/media) cluster.

---

## 6. Questions for the expert

1. **Real edge vs AI beta + 2-stock luck?** Given that leave-one-winner-out flips the edge negative, how should we interpret the headline +252%? What live diagnostics would distinguish genuine signal from concentration luck?
2. **How much decision weight can a 75-day, single-regime, in-sample backtest bear?** Our adoption bar is: exceed noise (±0.10 Calmar), survive leave-one-winner-out, adjacent-parameter stability, no bear-market accident pattern. Is that sufficient, or naive on this sample size?
3. **Is excluding sectors by qualitative judgment** (Entertainment / Specialty Retail) **sound risk discipline or post-hoc curve-fitting / style drift?** What guardrails would you impose?
4. **Is 2-position concentration appropriate** for an operator who is highly loss/MDD-averse? Should we widen slots / diversify? (Note: widening slots backtested −170 ppts worse — concentration *helped* in this sample.)
5. **AI-concentration risk:** if the cycle cools, do we hedge, or trust the strategy's sector-agnostic signal to rotate?
6. **Next bear market:** the defensive overlay (200-DMA + VIX → bonds) was validated only on 26 years of *index* data — we have no bear-market *single-stock* data for this system. Can we trust the defense logic?

## 7. Operator context (for tailored advice)

- History: turned ~$150k into ~$750k with 3x leverage, then took heavy losses on a single leveraged position → now **strongly fixated on low drawdown and robustness.**
- Capital allocation: 80% system / 20% safe asset (T-bill ETF), rebalanced annually.
- Also runs a parallel Korean-equity system on the same philosophy (7.4-year validation, separate track).
