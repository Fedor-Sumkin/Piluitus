"""Candidate-id-based join of realized PAPER trades to Shadow outcome labels."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from strategy.shadow_label_quality import deduplicate_shadow_labels

from .paper_performance_metrics import _shadow_segment_metrics
from .paper_performance_primitives import _confidence_bucket, _number, _text


class PaperPerformanceShadowMixin:
    """Attach descriptive Shadow labels to realized PAPER trade cohorts."""

    @staticmethod
    def _compare_shadow(
        trades: list[dict[str, Any]], labels: object
    ) -> dict[str, Any]:
        label_rows = labels if isinstance(labels, (list, tuple)) else []
        labels_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_candidate: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        deduplicated_labels = deduplicate_shadow_labels(label_rows)
        for label in deduplicated_labels:
            candidate_id = _text(label.get("candidate_id"), "")
            if not candidate_id:
                continue
            labels_by_candidate[candidate_id].append(dict(label))
            horizon = _number(label.get("horizon_sec"))
            if horizon is not None and horizon > 0:
                by_candidate[candidate_id][int(horizon)] = dict(label)

        matched: list[dict[str, Any]] = []
        unmatched_reasons: dict[str, int] = defaultdict(int)
        horizon_returns: dict[int, list[float]] = defaultdict(list)
        horizon_observations: dict[int, list[dict[str, Any]]] = defaultdict(list)
        counted_candidate_horizons: set[tuple[str, int]] = set()
        label_coverage: dict[int, dict[str, Any]] = defaultdict(lambda: {
            "observed_rows": 0, "valid_ok_labels": 0,
            "status_counts": defaultdict(int), "rows_without_valid_net_return": 0,
        })
        for label in deduplicated_labels:
            horizon_value = _number(label.get("horizon_sec"))
            if horizon_value is None or horizon_value <= 0:
                continue
            horizon_key = int(horizon_value)
            coverage = label_coverage[horizon_key]
            coverage["observed_rows"] += 1
            status = str(label.get("result") or "UNKNOWN").upper()
            coverage["status_counts"][status] += 1
            net_value = _number(label.get("net_executable_return_pct"))
            if status == "OK" and net_value is not None:
                coverage["valid_ok_labels"] += 1
            else:
                coverage["rows_without_valid_net_return"] += 1
        matched_realized_net = 0.0
        matched_details: list[dict[str, Any]] = []
        horizon_segments: dict[str, dict[str, list[Mapping[str, Any]]]] = {
            "by_side": defaultdict(list),
            "by_model_decision": defaultdict(list),
            "by_final_decision": defaultdict(list),
            "by_confidence": defaultdict(list),
            "by_model_decision_confidence": defaultdict(list),
            "by_execution_status": defaultdict(list),
            "by_horizon_side_confidence": defaultdict(list),
            "by_horizon_decision_confidence": defaultdict(list),
        }
        for trade in trades:
            candidate_id = _text(trade.get("shadow_candidate_id"), "")
            if not candidate_id:
                unmatched_reasons["NO_SHADOW_CANDIDATE_ID"] += 1
                continue
            all_candidate_labels = labels_by_candidate.get(candidate_id, [])
            candidate_labels = by_candidate.get(candidate_id, {})
            if not all_candidate_labels:
                unmatched_reasons["CANDIDATE_NO_LABEL_RECORD"] += 1
                continue
            valid_candidate_labels = {
                horizon: label for horizon, label in candidate_labels.items()
                if str(label.get("result") or "").upper() == "OK"
                and _number(label.get("net_executable_return_pct")) is not None
            }
            if not valid_candidate_labels:
                results = {
                    str(label.get("result") or "UNKNOWN").upper()
                    for label in all_candidate_labels
                }
                if results == {"NO_FRESH_BBO"}:
                    reason = "CANDIDATE_LABEL_NO_FRESH_BBO"
                elif results == {"LABEL_LATE"}:
                    reason = "CANDIDATE_LABEL_LATE"
                else:
                    reason = "CANDIDATE_HAS_NO_VALID_OK_LABEL"
                unmatched_reasons[reason] += 1
                continue

            matched.append(trade)
            realized_net = _number(trade.get("pnl")) or 0.0
            matched_realized_net += realized_net
            matched_horizons: dict[str, float] = {}
            for horizon, label in valid_candidate_labels.items():
                value = _number(label.get("net_executable_return_pct"))
                if value is not None:
                    if (candidate_id, horizon) not in counted_candidate_horizons:
                        horizon_returns[horizon].append(value)
                        horizon_observations[horizon].append(label)
                        counted_candidate_horizons.add((candidate_id, horizon))
                        side = _text(label.get("side") or trade.get("side"))
                        model_decision = _text(label.get("llm_model_decision"))
                        final_decision = _text(label.get("llm_final_decision"))
                        confidence = _confidence_bucket(label.get("llm_confidence"))
                        execution_status = _text(label.get("llm_execution_status"))
                        segment_prefix = f"{horizon}s|"
                        horizon_segments["by_side"][segment_prefix + side].append(label)
                        horizon_segments["by_model_decision"][segment_prefix + model_decision].append(label)
                        horizon_segments["by_final_decision"][segment_prefix + final_decision].append(label)
                        horizon_segments["by_confidence"][segment_prefix + confidence].append(label)
                        horizon_segments["by_model_decision_confidence"][
                            segment_prefix + model_decision + "|" + confidence
                        ].append(label)
                        horizon_segments["by_execution_status"][
                            segment_prefix + execution_status
                        ].append(label)
                        horizon_segments["by_horizon_side_confidence"][
                            segment_prefix + side + "|" + confidence
                        ].append(label)
                        horizon_segments["by_horizon_decision_confidence"][
                            segment_prefix + model_decision + "|" + confidence
                        ].append(label)
                    matched_horizons[str(horizon)] = round(value, 6)
            matched_details.append({
                "trade_id": _text(trade.get("id"), ""),
                "candidate_id": candidate_id,
                "ticker": _text(trade.get("ticker")),
                "side": _text(trade.get("side")),
                "realized_net_pnl_rub": round(realized_net, 2),
                "shadow_net_return_pct_by_horizon": matched_horizons,
            })

        horizons = {}
        for horizon, values in sorted(horizon_returns.items()):
            positive = [value for value in values if value > 0.0]
            negative = [value for value in values if value < 0.0]
            observations = horizon_observations[horizon]

            def average_field(field: str, digits: int = 3) -> float | None:
                field_values = [
                    value for label in observations
                    if (value := _number(label.get(field))) is not None
                ]
                return round(sum(field_values) / len(field_values), digits) if field_values else None

            horizons[str(horizon)] = {
                "labels": len(values),
                "mean_net_return_pct": round(sum(values) / len(values), 6),
                "positive_rate_pct": round(len(positive) / len(values) * 100.0, 3),
                "profit_factor": (
                    round(sum(positive) / abs(sum(negative)), 4)
                    if sum(negative) < 0.0 else None
                ),
                "average_mfe_net_pct": average_field("mfe_executable_pct", 6),
                "average_mae_net_pct": average_field("mae_executable_pct", 6),
                "average_time_to_mfe_sec": average_field("time_to_mfe_sec"),
                "average_time_to_mae_sec": average_field("time_to_mae_sec"),
                "average_valid_bbo_observations": average_field("valid_bbo_observations", 2),
                "average_bbo_observation_span_sec": average_field("bbo_observation_span_sec"),
            }
        unmatched = sum(unmatched_reasons.values())
        total = len(trades)
        summarized_segments = {
            group_name: {
                key: _shadow_segment_metrics(rows)
                for key, rows in sorted(groups.items())
            }
            for group_name, groups in horizon_segments.items()
        }
        return {
            "matched_trades": len(matched),
            "unmatched_trades": unmatched,
            "matched_trade_rate_pct": round(len(matched) / total * 100.0, 3) if total else None,
            "unmatched_by_reason": dict(sorted(unmatched_reasons.items())),
            "matched_realized_net_pnl_rub": round(matched_realized_net, 2),
            "join_key": "shadow_candidate_id",
            "matched_trade_details": matched_details[-200:],
            "horizons": horizons,
            "horizon_segments": summarized_segments,
            "label_coverage_by_horizon": {
                str(horizon): {
                    "observed_rows": item["observed_rows"],
                    "valid_ok_labels": item["valid_ok_labels"],
                    "rows_without_valid_net_return": item["rows_without_valid_net_return"],
                    "status_counts": dict(sorted(item["status_counts"].items())),
                }
                for horizon, item in sorted(label_coverage.items())
            },
            "horizon_segment_note": (
                "Groups contain deduplicated candidate_id+horizon labels joined to closed PAPER trades; "
                "returns are hypothetical Shadow outcomes, not realized trade PnL. "
                "Wilson intervals describe sampling uncertainty in positive rates; model confidence "
                "is a score and is not assumed to be a calibrated probability."
            ),
        }
