"""RAG rule-context attachment for the market-worker LLM request."""
from __future__ import annotations

import re
from typing import Any

from ai.rag_prompt_utils import select_prompt_rules


async def _get_knowledge_context(**kwargs: Any) -> str:
    # Keep the heavyweight PDF-backed parser lazy, as it was in main.py.
    from ai.knowledge_parser import TechnicalKnowledgeParser

    return await TechnicalKnowledgeParser.get_knowledge_context(**kwargs)


async def attach_llm_rule_context(
    event_payload: dict[str, Any],
    *,
    db: Any,
    ticker: str,
    candidate_direction: str,
    logger: Any,
) -> dict[str, list[str]]:
    """Attach retrieved rules and log the subset selected for the current side.

    A knowledge-store failure keeps the request fail-closed for directional
    entries, matching the previous MarketWorker fallback.
    """
    try:
        rag_context_str = await _get_knowledge_context(
            db=db,
            category="ScalpingRules",
            market_evidence=event_payload.get("metrics", {})
            if isinstance(event_payload.get("metrics"), dict) else {},
        )
        event_payload["rag_context"] = [rag_context_str]
        retrieved_rule_ids = re.findall(
            r"\[(?:RISK|BUY|SELL)\s*/\s*([A-Z0-9_]+)\]",
            rag_context_str,
        )
        # Always-on RISK rules plus only the selected local candidate's
        # directional rules. Hard entry gates remain deterministic.
        prompt_rule_text = select_prompt_rules(
            candidate_direction, rag_context_str, max_chars=900
        )
        prompt_rule_ids = re.findall(
            r"\[(?:RISK|BUY|SELL)\s*/\s*([A-Z0-9_]+)\]",
            prompt_rule_text,
        )
        logger.info(
            "[LLM CONTEXT] ticker=%s candidate=%s retrieved_rule_ids=%s prompt_rule_ids=%s",
            ticker,
            candidate_direction,
            ",".join(retrieved_rule_ids) if retrieved_rule_ids else "NONE",
            ",".join(prompt_rule_ids) if prompt_rule_ids else "NONE",
        )
        return {
            "retrieved_rule_ids": retrieved_rule_ids,
            "prompt_rule_ids": prompt_rule_ids,
        }
    except Exception as exc:
        logger.error(
            "⚠️ Ошибка сборки RAG-контекста в strategy.llm_context: %s",
            exc,
            exc_info=True,
        )
        event_payload["rag_context"] = [
            "Нет проверенного контекста правил; направленный вход должен быть HOLD."
        ]
        return {"retrieved_rule_ids": [], "prompt_rule_ids": []}
