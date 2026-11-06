# PILUITUS Scalping Strategy v1 — Shadow Specification

**Status:** experimental / shadow-only  
**Default effect on execution:** none. This module does not approve, reject, size, place, modify, or close orders.

## Why this exists

The PAPER run on 2026-11-06 produced three BUY entries (SMLT, LSRG, UWGN), all closed by STOP_LOSS after roughly ten minutes. The local baseline mostly combines signed short-horizon price movement, position relative to VWAP, last-trade-size impulse, and a fresh spread/BBO check. Those are useful candidate filters, but they do not establish that there is enough follow-through or order-book pressure to pay the spread and other trading costs.

The new evaluator runs beside the baseline and writes `[STRATEGY SHADOW]` records. A bounded in-memory forward tracker now labels each local directional candidate that has a fresh coherent BBO at +1, +3 and +5 minutes, irrespective of whether the shadow score qualifies, logging executable-side returns, gross/net sensitivity, MFE and MAE as `[STRATEGY SHADOW OUTCOME]`. Candidate and outcome records are hypothetical; the tracker does not submit or alter orders. The score is a deterministic evidence grade, **not a probability of profit**.

## v1 features and starting weights

| Feature | Weight | Current interpretation |
|---|---:|---|
| Signed price change + VWAP agreement | 25 | Both must point in the candidate direction |
| M1 pattern / EMA trend | 20 | Matching closed M1 pattern gets full points; aligned EMA trend receives partial points |
| Best-queue imbalance | 20 | Normalized difference between current best Bid and Ask sizes |
| Last-trade-size impulse | 10 | Relative trade-size impulse, with stronger score at 2× or more |
| Spread / M1 ATR | 15 | Penalizes a spread that consumes too much of the observed M1 range |
| BBO freshness | 10 | Rewards fresher executable quote confirmations |

The provisional shadow qualification threshold is **85/100**. All weights and thresholds are initial hypotheses; they are not calibrated parameters and are not applied to the active baseline.

### Queue imbalance

`queue_imbalance = (best_bid_size - best_ask_size) / (best_bid_size + best_ask_size)`

The value is dimensionless and bounded by -1 and +1. Positive values indicate a larger visible best Bid queue; negative values indicate a larger visible best Ask queue. It is calculated only when both best-side sizes are positive, the ORDER_BOOK state is valid, and the size observations are fresh.

Finam's displayed size unit is not assumed. The ratio can be used as a relative feature only; it must not be interpreted as shares, lots, executable depth, or guaranteed fill capacity.

This is **queue imbalance**, not order-flow imbalance (OFI). OFI requires the signed sequence of changes in best bid/ask prices and sizes, including queue additions, executions and cancellations. OFI is a planned next research step, after validating the integrity and timestamps of per-symbol book events.

## Shadow hard blocks

The evaluator marks a candidate ineligible when the data or setup is unsuitable for this hypothesis:
- BBO is unavailable or stale, or the ORDER_BOOK state is not coherent.
- M1 ATR is not ready.
- Spread exceeds the existing 0.50% baseline ceiling.
- A fresh closed M1 pattern directly contradicts the local candidate side.

These are shadow-only restrictions. The baseline execution route remains unchanged.

## How to judge the strategy

A high score is not enough. For every candidate, the next data-collection stage should record:
- ticker, time, side, regime, score and each component;
- executable Ask for hypothetical BUY entry / executable Bid for hypothetical SELL entry;
- executable closing Bid for BUY / Ask for SELL;
- spread, ATR, queue imbalance, queue-age, BBO-age and order-book validity;
- forward mid-price and executable returns at fixed horizons (for example 1, 3 and 5 minutes);
- maximum favourable/adverse excursion and time to reach +0.5R, +1R or the stop;
- configured commission and slippage assumptions.

The forward-return label is emitted at the first fresh quote at or after the target horizon. If no fresh BBO arrives within the configured label-lag allowance, the horizon is recorded as `NO_FRESH_BBO`, not assigned a stale price. Do not use future data in candidate features. Hypothetical fills use Ask to enter BUY, Bid to enter SELL, Bid to exit BUY and Ask to exit SELL. The tracker applies the current `PILUITUS_PAPER_COMMISSION_PCT` and `PILUITUS_PAPER_SLIPPAGE_BPS` settings; the log records those assumptions for every label.

Report results separately by ticker, direction, tick-size/price group, spread-to-ATR bucket, time of day and market regime. Required metrics include:
- trade count and coverage of the universe;
- net expectancy in R and rubles;
- profit factor, win rate and average win / average loss;
- maximum drawdown and loss streaks;
- MFE/MAE and time-to-follow-through;
- percentage of observations rejected for stale/missing book data.

Use chronological walk-forward tests with purging/embargo for overlapping forward labels. Keep a final untouched time period for out-of-sample evaluation. Record every parameter trial; after repeated tuning, report multiple-testing-aware diagnostics such as Probability of Backtest Overfitting (PBO) and Deflated Sharpe Ratio (DSR). Do not select one exceptional parameter peak from a large search and call it an edge.

## Proposed development sequence

1. **Data quality:** align the effective BBO age shown in diagnostics with the exact TTL used by the exit guard; quantify stale-book intervals per ticker.
2. **Shadow observations:** collect baseline candidate and v1 score side by side without changing entries or exits. The tracker labels executable returns at 1/3/5 minutes and captures MFE/MAE; keep the cost assumptions recorded with each outcome.
3. **OFI:** add event-level best-quote delta features with timestamp/order checks and per-symbol normalization.
4. **Regime and economics:** evaluate trend, range and low-quality regimes; estimate whether a plausible follow-through is large enough relative to spread and the user's configured costs.
5. **Exit research:** compare current ATR stop/2R target with a time-to-follow-through exit, 1R break-even and trailing variants in a separate replay. Do not tighten the live PAPER stop based only on three losing trades.
6. **Promotion gate:** only consider allowing the new strategy to influence PAPER entries after enough observations exist, out-of-sample expectancy is positive after realistic costs, drawdown is acceptable, and results are stable across tickers/regimes. Any eventual active change gets its own tests and commit.

## Research basis

- Cont, Kukanov and Stoikov, *The Price Impact of Order Book Events*, Journal of Financial Econometrics 12(1), 2014. DOI: 10.1093/jjfinec/nbt003. Their NYSE sample found a short-horizon relationship between order-flow imbalance and price changes, with price impact inversely related to market depth.
- Gould and Bonart, *Queue Imbalance as a One-Tick-Ahead Price Predictor in a Limit Order Book*, 2015. arXiv:1512.03492. Their Nasdaq sample found out-of-sample predictive value, but the gains differed considerably between large-tick and small-tick stocks.
- Bailey, Borwein, López de Prado and Zhu, *The Probability of Backtest Overfitting*, 2017. DOI: 10.21314/JCF.2016.322. Proposes combinatorially symmetric cross-validation to assess selection/backtest overfitting.

These studies motivate features and validation discipline; they do not establish profitability on MOEX or for PILUITUS.
