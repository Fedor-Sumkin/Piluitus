# PILUITUS Scalping Strategy v1 — Shadow Specification

**Status:** experimental / shadow-only  
**Default effect on execution:** none. This module does not approve, reject, size, place, modify, or close orders.

## Why this exists

The PAPER run on 2026-11-06 produced three BUY entries (SMLT, LSRG, UWGN), all closed by STOP_LOSS after roughly ten minutes. The local baseline mostly combines signed short-horizon price movement, position relative to VWAP, last-trade-size impulse, and a fresh spread/BBO check. Those are useful candidate filters, but they do not establish that there is enough follow-through or order-book pressure to pay the spread and other trading costs.

The new evaluator runs beside the baseline and writes `[STRATEGY SHADOW]` records. A bounded in-memory forward tracker now labels each local directional candidate that has a fresh coherent two-sided BBO at +1, +3 and +5 minutes, irrespective of whether the shadow score qualifies, logging executable-side returns, gross/net sensitivity, MFE and MAE as `[STRATEGY SHADOW OUTCOME]`. Candidate and outcome records are hypothetical; the tracker does not submit or alter orders. The score is a deterministic evidence grade, **not a probability of profit**.

## v1 features and starting weights

| Feature | Weight | Current interpretation |
|---|---:|---|
| Signed price change + VWAP agreement | 25 | Both must point in the candidate direction |
| M1 pattern / EMA trend | 20 | Matching closed M1 pattern gets full points; aligned EMA trend receives partial points |
| Persistent best-queue imbalance | 12 | Time-weighted five-second average of coherent best Bid/Ask size imbalance |
| Aggressor trade-flow confirmation | 8 | 30-second signed trade-volume imbalance, freshness checked |
| Last-trade-size impulse | 10 | Relative trade-size impulse, with stronger score at 2× or more |
| Spread / M1 ATR | 15 | Penalizes a spread that consumes too much of the observed M1 range |
| BBO freshness | 10 | Rewards fresher executable quote confirmations |

The provisional shadow qualification threshold is **85/100**. A candidate is shadow-ineligible when the time-weighted queue history is not warmed/fresh, the 30-second aggressor trade flow is unavailable/stale, or the two microstructure signals materially oppose each other. All weights and thresholds are initial hypotheses; they are not calibrated parameters and are not applied to the active baseline.

### Queue imbalance

`queue_imbalance = (best_bid_size - best_ask_size) / (best_bid_size + best_ask_size)`

The value is dimensionless and bounded by -1 and +1. Positive values indicate a larger visible best Bid queue; negative values indicate a larger visible best Ask queue. PILUITUS now retains accepted coherent snapshots and calculates a time-weighted five-second average, requiring at least three observations and one second of coverage. Empty, crossed or invalid book states clear the history. The separate trade-flow component uses the 30-second aggressor-side imbalance only when trade metrics are ready and fresh.

Finam's displayed size unit is not assumed. The ratio can be used as a relative feature only; it must not be interpreted as shares, lots, executable depth, or guaranteed fill capacity.

This is **queue imbalance**, not order-flow imbalance (OFI). PILUITUS now calculates a separate event-based best-quote OFI increment from two consecutive accepted ORDER_BOOK states using the Cont–Kukanov–Stoikov price/size-change convention. The five-second diagnostic sums signed increments and normalizes them by the accumulated average displayed best-queue depth. It ignores unchanged heartbeats, rejects invalid/crossed states, requires ordered timestamps and minimum event/coverage/freshness checks, and clears its history when the book becomes invalid. This is top-of-book OFI only: it does not yet include deeper levels, explicit order IDs, or a reliable decomposition of cancellations versus executions.

OFI is exposed in Shadow diagnostics (`ofi_5s`, availability, sample count, coverage and directional alignment) but contributes **zero score points** and creates no eligibility block. It is an experimental measurement, not a validated predictive signal; its scale and usefulness must be assessed per instrument and regime before any scoring change.

## Shadow hard blocks

The evaluator marks a candidate ineligible when the data or setup is unsuitable for this hypothesis:
- BBO is unavailable or stale, or the ORDER_BOOK state is not coherent.
- M1 ATR is not ready.
- Spread exceeds the existing 0.50% baseline ceiling.
- A fresh closed M1 pattern directly contradicts the local candidate side.

These are shadow-only restrictions. The baseline execution route remains unchanged.

## Real trade-flow data (live/PAPER gateway)

The live screener treats Finam INSTRUMENT_TRADES as the authoritative source for unique trade volume, rolling 60-second trade VWAP, 15-second trade momentum, 30-second aggressor-side imbalance and 60-second realized volatility. Trade subscriptions follow the active Top-15 plus open positions, with a 60-second minimum subscription hold and bounded overlap. QUOTES.lastSize is never accumulated as a new trade in this mode. Trades are de-duplicated by exchange trade_id; a deterministic fingerprint is used only when the provider omits an ID. Trade-derived features become ready only after a complete 60-second observation window, including the prior 55-second volume baseline, and expire after five seconds without a new trade. Missing or stale trade data blocks new entries rather than falling back to quote-update counts.

M1 bars in trade-flow mode are updated only by unique instrument trades, not repeated QUOTES carrying an unchanged last price. The strategy uses rolling 60-second trade VWAP, not full-session VWAP. Full-session VWAP requires seeding from the beginning of the session; do not label this rolling value as session VWAP.

## Finam instrument metadata and protective prices

At startup PILUITUS refreshes the asset directory and account-aware trade_lot_size. Protective SL/TP levels are rounded directionally to Finam min_step/decimals. Configured LOT_SIZES remain a fallback when the API is unavailable; an instrument with no confirmed lot size is not allowed to open a position. Top-15 ranking uses the same quote-derived 30-second momentum and 60-second volatility windows for all 88 symbols; it does not reward symbols merely because they already have trade subscriptions.

## How to judge the strategy

A high score is not enough. For every candidate, the next data-collection stage should record:
- ticker, time, side, regime, score and each component;
- executable Ask for hypothetical BUY entry / executable Bid for hypothetical SELL entry;
- executable closing Bid for BUY / Ask for SELL;
- spread, ATR, queue imbalance, queue-age, BBO-age and order-book validity;
- forward mid-price and executable returns at fixed horizons (for example 1, 3 and 5 minutes);
- maximum favourable/adverse excursion and time to reach +0.5R, +1R or the stop;
- configured commission and slippage assumptions.

The forward-return label is emitted at the first fresh quote at or after the target horizon. If no fresh BBO arrives within the configured label-lag allowance, the horizon is recorded as `NO_FRESH_BBO`, not assigned a stale price. Do not use future data in candidate features. Hypothetical fills use Ask to enter BUY, Bid to enter SELL, Bid to exit BUY and Ask to exit SELL. The tracker needs a fresh executable two-sided BBO for labels; full depth is not required for return calculation (but it remains required by the shadow queue-imbalance feature). The tracker applies the current `PILUITUS_PAPER_COMMISSION_PCT` and `PILUITUS_PAPER_SLIPPAGE_BPS` settings; the log records those assumptions for every label.

MFE and MAE are updated on each fresh executable-side observation while the
candidate remains pending. The existing `mfe_executable_pct` and
`mae_executable_pct` fields are **net** of the configured two-sided
commission/slippage assumptions; separate gross excursion fields are retained.
The tracker records elapsed seconds and UTC timestamps at the observed
favorable/adverse extrema, plus valid-BBO observation count, first/last valid
observation offsets, total observation span and invalid/stale observation
count. These are observed extrema, not continuous-market guarantees: gaps
between accepted quotes can hide a more extreme price. The coverage span is
not a percentage uptime metric, and quote observations are not independent
samples. Each label records `excursion_observation_end_elapsed_sec`: when a
label is delayed, its excursion snapshot can include valid observations after
the nominal horizon, so compare it alongside `label_lag_sec`.



### LLM decision attribution

Every shadow candidate created from a fresh coherent BBO receives a `candidate_id`. The ID is carried through the local prefilter/queue into the final verdict, where the tracker records both independent decision layers:

- `llm_model_decision`: the model's contract decision, `APPROVE` or `HOLD`;
- `llm_final_decision`: the system's final directional decision, `BUY`, `SELL` or `HOLD`;
- `llm_signal_status`, model confidence, reason code and local guard outcome;
- `llm_execution_status`: updated after the asynchronous PAPER execution result.

Each subsequent forward label copies these fields, so the scorecard can compare forward executable returns by model approval/rejection, final system decision, confidence band, reason code and actual execution status. Candidates rejected by a known local prefilter are now labelled `NOT_CALLED` with a bounded reason code (for example, `ATR_NOT_READY`, `AI_DEDUPLICATED` or `AI_QUEUE_FULL`). This is kept distinct from `LINKED` and legacy/unknown attribution, so a high prefilter rejection rate cannot be mistaken for a broken LLM link. A missing candidate link is explicitly reported and is not guessed from ticker/time proximity. The tracker also reports profit factor as the sum of positive net executable percentage returns divided by the absolute sum of negative net executable percentage returns; when there are no losses the value is `N/A`, not infinity.

The scorecard also creates horizon-conditioned interaction groups (for example, `60s|APPROVE|80-89`) so confidence and decision performance at one minute are not blended with ten-minute outcomes. Each group reports positive-return rate, expectancy, average win, average loss and profit factor. `sample_quality` marks fewer than 30 valid outcomes as insufficient, 30–99 as preliminary, 100–299 as developing and 300+ as a larger sample. These are descriptive sample-size labels, not a guarantee of statistical significance; horizons from the same candidate overlap and are not independent observations.

The attribution remains research-only. An LLM `APPROVE` is not itself an executed trade, and a positive hypothetical forward label is not a realized PAPER profit. Use the separate execution status and actual closed-trade ledger when evaluating realized performance.

Each newly opened position now persists the originating `shadow_candidate_id`, `llm_verdict_id`, model/final decisions, confidence percentage, signal status and reason code in the trade record. Partial fills retain these fields, and the final closed-trade journal entry carries them forward unchanged. This establishes a stable join key for a subsequent realized-vs-hypothetical execution scorecard; it does not infer a match from ticker and timestamp.

Report results separately by ticker, direction, tick-size/price group, spread-to-ATR bucket, time of day and market regime. Required metrics include:
- trade count and coverage of the universe;
- net expectancy in R and rubles;
- profit factor, win rate and average win / average loss;
- maximum drawdown and loss streaks;
- MFE/MAE and time-to-follow-through;
- percentage of observations rejected for stale/missing book data.

Use chronological walk-forward tests with purging/embargo for overlapping forward labels. Keep a final untouched time period for out-of-sample evaluation. Record every parameter trial; after repeated tuning, report multiple-testing-aware diagnostics such as Probability of Backtest Overfitting (PBO) and Deflated Sharpe Ratio (DSR). Do not select one exceptional parameter peak from a large search and call it an edge.

## Realized PAPER trade journal and scorecard

Final CLOSED PAPER records are written to the dedicated bounded Memurai list
`piluitus:paper:closed_trades` (20,000 records). The existing
`piluitus:trade_history` remains a short dashboard history and is not used as
the primary analytics source. Partial exits remain part of the original OPEN
trade; only the final CLOSED aggregate is counted. A repeated trade ID is
ignored by the in-memory scorecard, so replaying a restored journal does not
double-count the same trade. On startup, the scorecard restores the dedicated
journal; if it is empty, it can seed itself from the legacy 100-row trade
history.

The PAPER-only endpoint `GET /api/v1/paper/performance` returns:
- gross/net realized PnL, win rate, expectancy, average win/loss and profit factor;
- maximum drawdown in RUB and as a percentage of the prior realized-PnL peak, plus maximum/current loss streak;
- average holding time and recorded commissions;
- executable-BBO outage diagnostics: trades affected, gap episodes, total and
  average gap duration, and maximum single gap; these metrics are also grouped
  by ticker and PAPER session;
- segments by independent PAPER session, BUY/SELL, ticker, model/final LLM
  decision, confidence band, LLM reason code and exit reason;
- Shadow comparison matched strictly by `shadow_candidate_id`, with separate
  forward-label statistics for each horizon, MFE/MAE, average time to extrema,
  BBO observation coverage, matched-trade coverage percentage and unmatched
  reasons: missing candidate ID, candidate with no persisted forward-label
  record, no fresh BBO at the label horizon, or late/unusable forward labels.
  It never falls back to ticker/time matching.

Only final CLOSED rows with a valid finite net `pnl` and BUY/SELL direction
enter realized statistics. The net `pnl` field is the realized RUB result
after the entry/exit commissions and configured slippage already applied by
the PAPER executor; it is not recomputed from market prices. Profit factor is
N/A when there is no realized losing trade. Shadow comparison accepts only
fresh `OK` labels with a valid `net_executable_return_pct`; duplicate
candidate/horizon labels are counted once. It never joins by ticker or time.
Shadow horizon observations are still correlated within a candidate and must
not be treated as independent realized trades.

The journal is bounded to the newest 20,000 records. An explicit PAPER-session
reset and panic journal clear also remove this dedicated journal and reset the
in-memory realized scorecard; Shadow forward labels remain a separate research
history.

### Executable BBO availability during position management

The exit monitor records each interval in which the executable side (Bid for a
BUY position, Ask for a SELL position) is unavailable or older than the same
effective TTL used by the exit guard. It does not use stale prices to trigger
or fill an exit. When a fresh executable quote returns, the interval is closed
and its duration is retained on the trade as `exit_bbo_gap_count`,
`total_exit_bbo_gap_sec`, `last_exit_bbo_gap_sec` and
`max_exit_bbo_gap_sec`. The PAPER performance scorecard aggregates these fields
overall and by ticker/session, including the share of closed trades affected.
This measures data availability during position management; it does not imply
that every measured gap delayed a stop or target crossing.

The legacy fallback is
limited to 100 recent records and is only used when the dedicated journal is
empty. A journal write failure is logged as critical while the already-closed
PAPER trade remains closed; operators should treat that run's persisted
performance coverage as incomplete and inspect the log.

## Proposed development sequence

1. **Data quality:** align the effective BBO age shown in diagnostics with the exact TTL used by the exit guard; quantify stale-book intervals per ticker.
2. **Shadow observations:** collect baseline candidate and v1 score side by side without changing entries or exits. The tracker labels executable returns at 1/3/5 minutes and captures MFE/MAE; keep the cost assumptions recorded with each outcome.
3. **OFI validation:** collect the new event-level best-quote OFI beside Queue Imbalance and trade-flow imbalance; validate event ordering, warmup, per-ticker scale and out-of-sample relationship with executable forward returns before considering score weights.
4. **Regime and economics:** evaluate trend, range and low-quality regimes; estimate whether a plausible follow-through is large enough relative to spread and the user's configured costs.
5. **Exit research:** compare current ATR stop/2R target with a time-to-follow-through exit, 1R break-even and trailing variants in a separate replay. Do not tighten the live PAPER stop based only on three losing trades.
6. **Promotion gate:** only consider allowing the new strategy to influence PAPER entries after enough observations exist, out-of-sample expectancy is positive after realistic costs, drawdown is acceptable, and results are stable across tickers/regimes. Any eventual active change gets its own tests and commit.

## Research basis

- Cont, Kukanov and Stoikov, *The Price Impact of Order Book Events*, Journal of Financial Econometrics 12(1), 2014. DOI: 10.1093/jjfinec/nbt003. Their NYSE sample found a short-horizon relationship between order-flow imbalance and price changes, with price impact inversely related to market depth.
- Gould and Bonart, *Queue Imbalance as a One-Tick-Ahead Price Predictor in a Limit Order Book*, 2015. arXiv:1512.03492. Their Nasdaq sample found out-of-sample predictive value, but the gains differed considerably between large-tick and small-tick stocks.
- Bailey, Borwein, López de Prado and Zhu, *The Probability of Backtest Overfitting*, 2017. DOI: 10.21314/JCF.2016.322. Proposes combinatorially symmetric cross-validation to assess selection/backtest overfitting.

These studies motivate features and validation discipline; they do not establish profitability on MOEX or for PILUITUS.


## Point-in-time market regime and entry economics (research-only)

Every Shadow candidate now stores a second, descriptive market-context label in
addition to the original strategy hypothesis regime:

- `market_regime`: `TREND`, `RANGE_OR_MIXED`, `HIGH_VOLATILITY`,
  `LOW_LIQUIDITY` or `INSUFFICIENT_DATA`;
- `directional_regime`: `TREND_UP`, `TREND_DOWN`,
  `RANGE_OR_MIXED` or `DIRECTION_UNAVAILABLE`;
- `volatility_regime`: relative ATR state;
- `liquidity_regime`: spread/ATR and sufficiently warmed trade-count evidence.

Volatility is compared with the median ATR percentage from up to 300 *earlier*
candidate snapshots for the same ticker. At least 20 prior observations are
required; otherwise the volatility field remains `VOLATILITY_UNCLASSIFIED`.
The current candidate is appended only after its classification, preventing
look-ahead. This reference is based on candidate snapshots, not every market
tick, and restarts with a warmup after process restart. The thresholds (1.50x
and 0.65x of prior median ATR) are exploratory grouping rules, not trading
signals. Low-liquidity classification takes precedence in the combined regime
when spread/ATR exceeds 0.50 or a fully covered 60-second trade window has
fewer than three prints. Missing evidence remains explicitly unavailable.

The entry-economics snapshot solves for the future opposite-side executable
quote at which the hypothetical entry would break even after both-side
commission and adverse slippage. BUY enters at Ask and exits at Bid; SELL enters
at Bid and exits at Ask. `minimum_favorable_move_pct` is measured from the
entry-time midpoint to that future executable quote, so it includes the cost of
crossing the entry spread, both commission charges and both slippage
assumptions. `move_beyond_entry_executable_pct` separately shows the extra
move beyond the entry-side quote. `cost_to_atr_ratio` compares the required
midpoint-to-exit move with the observed M1 ATR percentage.

The cost calculation uses the currently configured
`PILUITUS_PAPER_COMMISSION_PCT` and `PILUITUS_PAPER_SLIPPAGE_BPS` values.
If either assumption is zero, the record is marked
`INCOMPLETE_OR_ZERO_ASSUMPTION`; the output is still calculated, but must not
be interpreted as a complete Finam cost estimate. Verify the actual account
tariff and calibrate slippage from PAPER fills before drawing conclusions.

The new fields are copied into forward labels and grouped by market regime,
volatility, liquidity, direction and cost-to-ATR bucket. Queue Imbalance,
30-second aggressor Trade Flow and 5-second OFI are also grouped separately
inside each market regime and side, plus a combined confluence segment. This
allows the scorecard to compare each feature and their agreement without
silently changing strategy weights. These fields do not add score points,
change Shadow eligibility, block the LLM, change position sizing, or modify
PAPER orders. Compare net executable outcomes by ticker, side, horizon and
regime only after adequate sample sizes; overlapping horizons from one
candidate are correlated observations, not independent trades.


## PAPER executable-BBO replay journal

The PAPER process now records a bounded, ordered journal of accepted Finam
ORDER_BOOK and QUOTES best-quote states for the active Top-N universe and
currently open PAPER positions. Both streams are captured because the PAPER
exit monitor consumes the shared screener BBO, whose per-side confirmation
clocks can be refreshed by either source. Each event stores the process
session ID and sequence, ticker, source stream, exchange event time (or an
explicit receive-time fallback), receive UTC/monotonic clocks, per-side
confirmation clocks and ages, Bid/Ask, displayed best-level sizes, BBO
validity, ORDER_BOOK validity and the provider/parser invalidation reason.

Capture is observational and does not feed prices into the execution path.
Price/validity transitions are retained immediately. Size-only changes are
coalesced into the next price transition or heartbeat, avoiding a journal write
for every queue-size fluctuation. Unchanged heartbeats are sampled at a
configurable interval (default one second) so replay can distinguish an
unchanged but confirmed book from a long data gap. Invalidations are retained
as explicit events.
Stale/out-of-order events rejected by the screener are not represented as
accepted market states.

Events enter a bounded in-memory queue without waiting for Memurai. A separate
writer batches them into the bounded list
`piluitus:paper:bbo_replay` (default retention 250,000 events). The queue,
batch size, flush interval, heartbeat interval and storage cap are bounded;
queue overflow and persistence failures are counted and reported. Defaults
can be tuned with `PILUITUS_BBO_REPLAY_QUEUE_LIMIT`,
`PILUITUS_BBO_REPLAY_BATCH_SIZE`, `PILUITUS_BBO_REPLAY_FLUSH_SEC`,
`PILUITUS_BBO_REPLAY_HEARTBEAT_SEC` and
`PILUITUS_BBO_REPLAY_STORAGE_LIMIT`; hard caps remain in place. On orderly
shutdown the writer drains its queue before the database connection is closed.
The journal is process-session scoped: monotonic timestamps must never be
compared across different session IDs. Memurai's list is newest-first; replay
consumers must group by session ID and order each session by its increasing
sequence number, not by list position.

This first recorder stores top-of-book prices and sizes, not full depth or
exchange order IDs. It supports replay of executable BBO-triggered exit
decisions and quote-gap behavior, but it cannot prove queue position, passive
limit-order fills, or multi-level market impact. Replay must use Bid to exit
BUY positions and Ask to exit SELL positions, preserve event order, reject
invalid states, and enforce the same freshness TTL as the original PAPER
monitor. A replayed strategy must not be promoted to the active PAPER exit
rules until it has been compared against the unchanged baseline on identical
sessions and realistic commission/slippage assumptions.


## PAPER exit replay and scenario comparison

Every newly opened PAPER trade stores `bbo_replay_session_id`, linking it to
the recorder's process session independently of the execution-engine session
ID. When the production monitor first latches `STOP_LOSS` or `TAKE_PROFIT`,
the recorder also writes an `EXIT_TRIGGER` event with trade ID, position side,
trigger source, trigger reason/price, observed trigger price and whether a
fresh executable BBO was available at that exact monitor event. If it was not
available, the replay keeps the trigger pending and closes only at the next
fresh executable side observation.

`strategy/bbo_replay_simulator.py` is an offline, pure-Python research engine.
It accepts the persisted PAPER trade fields and the journal events for the
matching `bbo_replay_session_id`. It orders the journal by increasing session
sequence, deduplicates repeated sequence IDs, reports global sequence gaps and
per-ticker timestamp regressions, and enforces the same per-side executable
BBO freshness threshold. BUY exits use Bid; SELL exits use Ask. Invalid,
crossed, missing-side and stale observations are not used as fills.

The default comparison set is:
- `BASELINE_SL_TP`: uses actual latched production `EXIT_TRIGGER` events
  when present, then fills at the trigger-time fresh BBO or the next fresh BBO.
  Legacy sessions without trigger events use an explicitly labelled
  `BBO_THRESHOLD_PROXY`.
- `TIME_STOP_180S` and `TIME_STOP_300S`: preserve original SL/TP and close
  at the first fresh executable quote at or after the time limit.
- `BREAK_EVEN_1R`: arms a net break-even stop after +1R, including entry fee,
  exit commission and adverse slippage in the threshold.
- `TRAILING_1R_ARM_0_75R_GAP`: arms after +1R and trails the best observed
  executable-side price by 0.75R, without loosening the original stop.

All exit fills apply the configured per-side commission and adverse slippage.
Batch summaries report closed/open counts, net PnL, expectancy in RUB and R,
win rate, average win/loss, profit factor, sequential closed-trade drawdown
(not portfolio drawdown) and holding time,
overall and by ticker, side and replay session. Fewer than 30 closed outcomes
are labelled insufficient; 30–99 preliminary; 100–299 developing; 300+ larger
sample. A profit factor with no losses is reported as N/A, not infinity.
Open-at-end positions are not counted as realized PnL.

The read-only endpoint `GET /api/v1/paper/replay/{order_id}` loads the
persisted PAPER trade and its matching bounded BBO replay session, then returns
the scenario comparison. It requires a trade created after replay-session
linkage was introduced; older records without `bbo_replay_session_id` cannot
be joined safely. The endpoint does not mutate the trade or journal.

This engine is intentionally not wired into active position management and
does not alter PAPER positions. Its pure simulator does not read/write Memurai;
only the read-only API adapter loads the persisted records. It is a research
calculation over a caller-supplied set of journal events and trade records.
It does not model portfolio overlap/capital contention, queue position,
passive fills, full-depth impact or exchange latency. The time-stop is
event-driven and can only act on the first fresh BBO observed after timeout.
Results must be compared on the same closed-position cohort and same cost
assumptions; changing the number or composition of closed trades can bias a
simple scenario comparison.
