"""Broker balance and configured-deposit synchronization mixin."""
from __future__ import annotations

from risk.risk_shared import *  # noqa: F401,F403


class RiskCapitalSyncMixin:
    async def _load_balance_from_broker(self, finam_broker_client) -> float:
        """
        🚀 ЗАГЛУШКА НА БУДУЩЕЕ: Прямой запрос баланса (Equity) у брокера Финам.
        Раскомментировать и настроить, когда перейдем на реальный счет.
        """
        try:
            # response = await finam_broker_client.get_portfolio()
            # return float(response.get("equity") or response.get("balance", self.initial_deposit))
            return self.initial_deposit
        except Exception:
            return self.initial_deposit

    async def _sync_configured_deposit(self, redis_inst) -> None:
        """Keep persisted start capital aligned with configured account capital.

        TRADING_DEPOSIT is authoritative for this run. Only the deposit metadata key
        is synchronized; trades, positions, verdicts, and risk history are untouched.
        """
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        key = "piluitus:risk:persistent_balance"
        try:
            configured = float(self.initial_deposit)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError("Некорректный стартовый депозит в конфигурации.") from exc
        if not math.isfinite(configured) or configured < 0.0:
            raise RuntimeError("Стартовый депозит должен быть конечным неотрицательным числом.")

        saved_raw = await redis_inst.get(key)
        if saved_raw is None:
            await redis_inst.set(key, f"{configured:.2f}")
            risk_logger.info(
                "🆕 [PERSISTENCE] Зафиксирован стартовый депозит из конфигурации: %.2f ₽",
                configured,
            )
            return

        try:
            saved = float(saved_raw)
        except (TypeError, ValueError, OverflowError):
            saved = float("nan")

        if not math.isfinite(saved) or saved < 0.0 or not math.isclose(
            saved, configured, rel_tol=0.0, abs_tol=0.01
        ):
            await redis_inst.set(key, f"{configured:.2f}")
            risk_logger.warning(
                "⚠️ [PERSISTENCE] Сохранённый депозит (%r ₽) отличается от конфигурации "
                "(%.2f ₽); используем TRADING_DEPOSIT и синхронизируем только ключ %s. "
                "Журнал сделок и остальные данные Redis сохранены.",
                saved_raw,
                configured,
                key,
            )
            return

        risk_logger.info(
            "💾 [PERSISTENCE] Депозит Redis согласован с конфигурацией: %.2f ₽",
            configured,
        )
