"""Safe sizing for asynchronous AI queue consumers."""
from __future__ import annotations


def _positive_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return max(1, parsed)


def effective_ai_worker_count(
    configured_workers: object = 3,
    vllm_max_concurrency: object = 4,
    *,
    hard_max: int = 4,
) -> int:
    """Never start more AI queue workers than vLLM can accept concurrently."""
    safe_hard_max = max(1, min(4, _positive_int(hard_max, 4)))
    requested = min(safe_hard_max, _positive_int(configured_workers, 3))
    transport_capacity = min(safe_hard_max, _positive_int(vllm_max_concurrency, 4))
    return max(1, min(requested, transport_capacity))
