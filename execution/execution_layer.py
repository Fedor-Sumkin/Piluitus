"""Stable public execution engine facade composed from focused runtime mixins."""
from __future__ import annotations

from execution.execution_shared import *  # noqa: F401,F403
from execution.execution_position_risk import ExecutionPositionRiskMixin
from execution.execution_market_session import ExecutionMarketSessionMixin
from execution.execution_paper_mechanics import ExecutionPaperMechanicsMixin
from execution.execution_order_runtime import ExecutionOrderRuntimeMixin
from execution.paper_position_exit_runtime import PaperPositionExitRuntimeMixin


class BaseExecutionEngine(
    ExecutionPositionRiskMixin,
    ExecutionMarketSessionMixin,
    ExecutionPaperMechanicsMixin,
    ExecutionOrderRuntimeMixin,
    PaperPositionExitRuntimeMixin,
):
    def __init__(
        self,
        finam_broker_client: BrokerClientProtocol | None = None,
        screener: ScreenerProtocol | None = None,
        initial_capital: float | None = None,
        max_gross_exposure_pct: float | None = None,
        max_single_position_pct: float | None = None,
        stop_loss_cooldown_sec: float | None = None,
        max_open_stop_risk_pct: float | None = None,
    ):
        self.broker = finam_broker_client
        self.screener = screener
        self.paper_mode = bool(getattr(Settings, "PAPER_TRADING", PAPER_TRADING))

        try:
            self.initial_capital = max(0.0, float(
                initial_capital if initial_capital is not None else Settings.TRADE_DEPOSIT
            ))
        except (TypeError, ValueError):
            self.initial_capital = 100000.0
        # Default committed capital is 10%; hard ceiling remains 15% of deposit.
        # Clamp explicitly configured legacy values (1.0 / 0.20) to ensure
        # stale .env settings cannot silently restore aggressive exposure.
        requested_gross_pct = (
            max_gross_exposure_pct
            if max_gross_exposure_pct is not None
            else os.getenv("PILUITUS_MAX_GROSS_EXPOSURE_PCT", "0.10")
        )
        self.max_gross_exposure_pct = self._bounded_pct(
            requested_gross_pct,
            default=0.10,
            maximum=MAX_ALLOWED_GROSS_EXPOSURE_PCT,
        )
        requested_single_pct = (
            max_single_position_pct
            if max_single_position_pct is not None
            else os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.03")
        )
        self.max_single_position_pct = self._bounded_pct(
            requested_single_pct,
            default=MAX_ALLOWED_SINGLE_POSITION_PCT,
            maximum=min(self.max_gross_exposure_pct, MAX_ALLOWED_SINGLE_POSITION_PCT),
        )
        requested_open_stop_risk_pct = (
            max_open_stop_risk_pct
            if max_open_stop_risk_pct is not None
            else os.getenv("PILUITUS_MAX_OPEN_STOP_RISK_PCT", "0.01")
        )
        self.max_open_stop_risk_pct = self._bounded_pct(
            requested_open_stop_risk_pct, default=0.01, maximum=0.05
        )
        try:
            cooldown = float(
                stop_loss_cooldown_sec if stop_loss_cooldown_sec is not None
                else os.getenv("PILUITUS_STOP_LOSS_COOLDOWN_SEC", "180")
            )
            self.stop_loss_cooldown_sec = max(0.0, cooldown) if math.isfinite(cooldown) else 180.0
        except (TypeError, ValueError):
            self.stop_loss_cooldown_sec = 180.0
        try:
            self.repeat_stop_window_sec = max(
                60.0, _finite_or_default(os.getenv("PILUITUS_REPEAT_SL_WINDOW_SEC", "1800"), 1800.0)
            )
            self.repeat_stop_limit = max(2, int(os.getenv("PILUITUS_REPEAT_SL_LIMIT", "2")))
            self.repeat_stop_cooldown_sec = max(
                60.0, _finite_or_default(os.getenv("PILUITUS_REPEAT_SL_COOLDOWN_SEC", "900"), 900.0)
            )
        except (TypeError, ValueError):
            self.repeat_stop_window_sec = 1800.0
            self.repeat_stop_limit = 2
            self.repeat_stop_cooldown_sec = 900.0
        try:
            self.shadow_be_enabled = os.getenv("PILUITUS_SHADOW_BE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
            self.shadow_be_arm_r = min(
                3.0, max(0.25, _finite_or_default(os.getenv("PILUITUS_SHADOW_BE_ARM_R", "1.0"), 1.0))
            )
        except (TypeError, ValueError):
            self.shadow_be_enabled = True
            self.shadow_be_arm_r = 1.0

        # Fees are explicit configuration: never guess the user's Finam tariff.
        # Percentage is charged on each side (entry and exit). Slippage is an
        # additional adverse adjustment beyond the executable Bid/Ask price.
        self.paper_commission_pct = min(
            1.0, max(0.0, _finite_or_default(
                os.getenv("PILUITUS_PAPER_COMMISSION_PCT", str(getattr(Settings, "PAPER_COMMISSION_PCT", 0.0354))),
                0.0354,
            ))
        )
        self.paper_slippage_bps = min(
            100.0, max(0.0, _finite_or_default(os.getenv("PILUITUS_PAPER_SLIPPAGE_BPS", "0"), 0.0))
        )
        # Depth-based partial fills stay disabled until the Finam ORDER_BOOK size
        # unit is explicitly confirmed. The official schema exposes Decimal sizes
        # but does not define shares-vs-lots semantics.
        configured_book_unit = str(getattr(Settings, "PAPER_BOOK_SIZE_UNIT", "unknown")).strip().lower()
        self.paper_book_size_unit = configured_book_unit if configured_book_unit in {"shares", "lots"} else "unknown"
        self.paper_max_book_participation_pct = min(
            100.0,
            max(1.0, _finite_or_default(
                getattr(Settings, "PAPER_MAX_BOOK_PARTICIPATION_PCT", 100.0), 10.0
            )),
        )

        logger.info(
            "[PAPER DEPTH MODEL] book_size_unit=%s max_participation=%.1f%% "
            "partial_exit=%s; unit=unknown keeps the explicitly unverified BBO fallback",
            self.paper_book_size_unit,
            self.paper_max_book_participation_pct,
            self.paper_book_size_unit in {"shares", "lots"},
        )

        # ОЗУ-структуры PAPER-позиций.
        self.paper_positions: dict[str, TradeRecord] = {}
        self.closed_trades: list[TradeRecord] = []
        self._lock_closing: set[str] = set()
        self._opening_tickers: set[str] = set()
        self._reserved_notional: dict[str, float] = {}
        self._reserved_stop_risk: dict[str, float] = {}
        self._portfolio_lock = asyncio.Lock()
        # Cooldowns are side-specific: a short stop does not suppress a valid long setup.
        self._ticker_cooldowns: dict[tuple[str, str], float] = {}
        # Repeated capital/risk-limit rejections are briefly suppressed per ticker/side.
        self._capital_reject_cooldowns: dict[tuple[str, str], float] = {}
        self._recent_stop_losses: dict[tuple[str, str], deque[float]] = {}
        self._hydrated_stop_loss_keys: set[tuple[str, str]] = set()
        self._pending_monitor_prices: dict[str, tuple[float, str, object]] = {}
        self._monitor_worker_task: asyncio.Task | None = None
        self._last_bbo_exit_warning_at: dict[str, float] = {}
        try:
            self.paper_exit_status_log_interval_sec = min(
                900.0,
                max(
                    60.0,
                    _finite_or_default(
                        os.getenv("PILUITUS_PAPER_EXIT_STATUS_LOG_INTERVAL_SEC", "300"),
                        300.0,
                    ),
                ),
            )
        except (TypeError, ValueError, OverflowError):
            self.paper_exit_status_log_interval_sec = 300.0
        # Per-order/event throttling keeps repeated BBO gaps visible in metrics
        # without flooding the console with every stale/fresh transition.
        self._last_exit_status_log_at: dict[str, float] = {}
        # Old PAPER positions cannot be resumed safely after restart because the
        # simulator does not restore their monitor state. Keep them visible as
        # unresolved committed capital and fail closed until they are reviewed.
        self._unreconciled_paper_positions: dict[str, TradeRecord] = {}
        # Explicit service lock used during a PAPER-session maintenance/reset.
        # It is fail-closed and released only by the dedicated resume endpoint.
        self._maintenance_active = False
        self._maintenance_reason = ""
        self._last_portfolio_guard_log_at = 0.0
        self.session_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")

        logger.info(
            "🛡️ [PORTFOLIO GUARD] capital=%.2f ₽ gross_cap=%.0f%% single_position_cap=%.0f%% "
            "open_stop_risk_cap=%.0f%% stop_cooldown=%.0fs repeat_SL=%d/%.0fs->%.0fs shadow_BE=%s@%.2fR",
            self.initial_capital,
            self.max_gross_exposure_pct * 100,
            self.max_single_position_pct * 100,
            self.max_open_stop_risk_pct * 100,
            self.stop_loss_cooldown_sec,
            self.repeat_stop_limit, self.repeat_stop_window_sec,
            self.repeat_stop_cooldown_sec, self.shadow_be_enabled, self.shadow_be_arm_r,
        )
        if self.paper_mode:
            logger.info("🛡️ РЕЖИМ АГЕНТА: [PAPER TRADING] — Симуляция ордеров через Redis ОЗУ.")
            logger.info(
                "🧪 [PAPER EXECUTION MODEL] Вход BUY=Ask/SELL=Bid, выход long=Bid/short=Ask; "
                "спред учитывается через стороны BBO; depth_unit=%s, participation=%.1f%%.",
                self.paper_book_size_unit, self.paper_max_book_participation_pct,
            )
            if self.paper_book_size_unit in {"shares", "lots"}:
                logger.warning(
                    "🧩 [PAPER DEPTH MODEL] Частичное исполнение включено: unit=%s, "
                    "лимит участия в каждом уровне=%.1f%%; depth-fill применяется и на выходе.",
                    self.paper_book_size_unit, self.paper_max_book_participation_pct,
                )
            else:
                logger.warning(
                    "⚠️ [PAPER DEPTH MODEL DISABLED] Единицы buy_size/sell_size Finam не подтверждены; "
                    "для входов и выходов сохраняется допущение полного fill по top-of-book. "
                    "Не устанавливайте unit=shares/lots без проверки данных."
                )
            if self.paper_commission_pct <= 0.0 or self.paper_slippage_bps <= 0.0:
                logger.warning(
                    "⚠️ [PAPER COST MODEL INCOMPLETE] комиссия=%.4f%% за сторону, доп.проскальзывание=%.2f bps. "
                    "При нулевом параметре PnL не является полностью net-of-costs; укажите подтверждённый тариф Finam "
                    "и обоснованную дополнительную модель проскальзывания в .env. Спред уже включён в Ask/Bid.",
                    self.paper_commission_pct, self.paper_slippage_bps,
                )
            else:
                logger.info(
                    "🧾 [PAPER COST MODEL] комиссия=%.4f%% за сторону, дополнительное проскальзывание=%.2f bps; "
                    "проверьте соответствие действующему тарифу Finam.",
                    self.paper_commission_pct, self.paper_slippage_bps,
                )
        else:
            logger.warning("🔥 РЕЖИМ АГЕНТА: [LIVE TRADING] — Работа на реальном счёте Finam!")
