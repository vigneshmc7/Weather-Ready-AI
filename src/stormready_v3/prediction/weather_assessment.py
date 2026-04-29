from __future__ import annotations

from dataclasses import asdict
from typing import Any

from stormready_v3.domain.models import PredictionContext, WeatherAssessment


def assess_weather(
    context: PredictionContext,
    *,
    weather_effect_pct: float,
    weather_learning_pct: float,
) -> WeatherAssessment:
    weather_signals = [
        signal
        for signal in context.normalized_signals
        if signal.dependency_group == "weather"
    ]
    if weather_effect_pct <= -0.02:
        classification = "demand_risk"
    elif weather_effect_pct >= 0.02:
        classification = "demand_support"
    elif weather_signals:
        classification = "watch"
    else:
        classification = "neutral"
    drivers: list[str] = []
    source_names: list[str] = []
    for signal in weather_signals:
        if signal.signal_type not in drivers:
            drivers.append(signal.signal_type)
        if signal.source_name not in source_names:
            source_names.append(signal.source_name)
    return WeatherAssessment(
        service_date=context.service_date,
        service_window=context.service_window,
        weather_effect_pct=round(float(weather_effect_pct), 4),
        weather_learning_pct=round(float(weather_learning_pct), 4),
        classification=classification,
        drivers=drivers[:4],
        source_names=source_names[:4],
    )


def serialize_weather_assessment(assessment: WeatherAssessment) -> dict[str, Any]:
    data = asdict(assessment)
    data["service_date"] = assessment.service_date.isoformat()
    data["service_window"] = assessment.service_window.value
    data["generated_at"] = assessment.generated_at.isoformat()
    return data
