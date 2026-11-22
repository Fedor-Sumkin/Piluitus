"""Coalescing lifecycle for a pending BBO candidate."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .candidate_recovery_primitives import _finite


def coalesce_pending_bbo_entry(
    previous: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
    *,
    now: float,
    ttl_sec: float,
) -> tuple[dict[str, Any], str]:
    """Coalesce same-side repeats without extending the original BBO-wait TTL.

    Returns (entry, status), where status is created, coalesced, superseded,
    or expired. A direction change is a genuinely new candidate and may replace
    the prior one; repeated ticks for the same side only refresh its payload.
    """
    current = float(now)
    ttl = max(0.0, float(ttl_sec))
    new_payload = dict(payload)
    if isinstance(new_payload.get("metrics"), Mapping):
        new_payload["metrics"] = dict(new_payload["metrics"])

    if isinstance(previous, Mapping):
        created_at = _finite(previous.get("created_at"), 0.0)
        age = current - created_at
        if created_at > 0.0 and 0.0 <= age <= ttl:
            old_payload = previous.get("payload")
            old_side = (
                str(old_payload.get("candidate_direction") or "").strip().upper()
                if isinstance(old_payload, Mapping) else ""
            )
            new_side = str(new_payload.get("candidate_direction") or "").strip().upper()
            if old_side and old_side == new_side:
                return {
                    "created_at": created_at,
                    "payload": new_payload,
                }, "coalesced"
            return {"created_at": current, "payload": new_payload}, "superseded"
        return {"created_at": current, "payload": new_payload}, "expired"

    return {"created_at": current, "payload": new_payload}, "created"
