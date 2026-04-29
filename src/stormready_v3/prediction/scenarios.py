from __future__ import annotations

from typing import Any


def build_forecast_scenarios(
    *,
    expected: int,
    low: int,
    high: int,
    baseline: int | None,
    attribution_breakdown: dict[str, Any],
) -> list[dict[str, Any]]:
    delta_vs_usual = expected - baseline if baseline is not None else None
    return [
        {
            "key": "likely",
            "label": "Likely",
            "covers": expected,
            "delta_vs_usual": delta_vs_usual,
            "attribution": attribution_breakdown,
        },
        {
            "key": "slower",
            "label": "Slower",
            "covers": low,
            "delta_vs_usual": (low - baseline) if baseline is not None else None,
            "attribution": {"interval": "lower_planning_edge"},
        },
        {
            "key": "busier",
            "label": "Busier",
            "covers": high,
            "delta_vs_usual": (high - baseline) if baseline is not None else None,
            "attribution": {"interval": "upper_planning_edge"},
        },
    ]
