# PILUITUS architecture

## Current application layout

The repository uses a package-first layout while retaining `main.py` as the
stable root startup entry point and `config.py` as the shared configuration
module. This is intentional: moving either of these during the package-import
migration would create unnecessary startup and environment-loading risk.

| Package | Responsibility |
|---|---|
| `ai/` | LLM integration, prompt helpers, verdict formatting, news and knowledge parsing |
| `exchange/` | Finam connectivity and market-data transport |
| `execution/` | Order execution, PAPER simulation and legacy evolution support |
| `interface/` | Telegram bot, polling/runtime integration and WebSocket client management |
| `monitoring/` | Runtime logging setup, run-scoped trading-event journals, terminal status, background-task tracking, thread-safe TRADE FLOW counters, ORDER_BOOK position diagnostics, rate-limited prefilter warnings, strategy-shadow scorecard reporting, TRADE FLOW report formatting and diagnostic classification |
| `portfolio/` | Book-cost accounting, portfolio metrics and exports |
| `risk/` | Risk manager and risk limits |
| `storage/` | Redis-compatible persistence and database manager |
| `strategy/` | Screening, market context, signal guards, replay and PAPER research |
| `tools/` | Maintenance and developer utilities |

## Import direction

Application code must import implementation modules by their canonical package
paths, for example:

- `storage.db_manager`, not `db_manager`
- `risk.risk_manager`, not `risk_manager`
- `strategy.signal_guard`, not `signal_guard`
- `exchange.finam_client`, not `finam_client`
- `interface.tg_bot`, not `tg_bot`

The root-level compatibility modules are temporary public aliases. They preserve
older imports used by external scripts, existing tests and operator workflows.
They must remain thin aliases to the package implementation; business logic must
never be copied back into them.

## Safe migration policy

1. Move internal application imports to canonical package paths.
2. Keep compatibility aliases until repository-wide static and runtime checks
   show that no supported entry point depends on them.
3. Remove aliases only in a separate change with explicit compatibility review.
4. Keep trading behavior, risk thresholds, Finam subscriptions, PAPER accounting
   and order lifecycle outside pure structural refactors.
5. Keep `main.py` as the stable launcher until its global-state and lifecycle
   responsibilities have dedicated tests and can be extracted independently.

The architecture test `tests/test_architecture_imports.py` prevents package code
and the root launcher from silently regressing to legacy root-shim imports.
