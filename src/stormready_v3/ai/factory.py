from __future__ import annotations

from typing import Any

from stormready_v3.ai.contracts import AgentModelProvider, ExternalSourceGovernanceItem, LocationProfilingResult
from stormready_v3.ai.openai_provider import OpenAIAgentModelProvider
from stormready_v3.config.settings import (
    AGENT_MODEL_PROVIDER,
    AGENT_REASONING_EFFORT,
    AI_ENRICHMENT_TIMEOUT_SECONDS,
    AI_GOVERNANCE_TIMEOUT_SECONDS,
    AI_MAX_RETRIES,
    AI_REQUEST_TIMEOUT_SECONDS,
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_OPENAI_ENDPOINT,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_MODEL,
)


class UnavailableAgentModelProvider:
    """Provider sentinel used when AI config is missing or invalid."""

    def __init__(self, *, configured_provider: str, failure_reason: str) -> None:
        self._configured_provider = configured_provider
        self._failure_reason = failure_reason

    def is_available(self) -> bool:
        return False

    def configured_provider(self) -> str:
        return self._configured_provider

    def last_failure_reason(self) -> str:
        return self._failure_reason

    def structured_json_call(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        max_output_tokens: int = 800,
    ) -> dict[str, Any] | None:
        return None

    def external_source_governance(
        self,
        *,
        operator_context: dict[str, Any],
        source_candidates: list[dict[str, Any]],
    ) -> list[ExternalSourceGovernanceItem] | None:
        return None

    def location_profiling(
        self,
        *,
        address: str,
        city: str | None,
        neighborhood_type: str | None,
        lat: float | None,
        lon: float | None,
    ) -> LocationProfilingResult | None:
        return None


def build_agent_model_provider() -> AgentModelProvider:
    provider = OpenAIAgentModelProvider(
        api_key=OPENAI_API_KEY,
        model=OPENAI_MODEL,
        base_url=OPENAI_BASE_URL,
        azure_api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        azure_api_version=AZURE_OPENAI_API_VERSION,
        azure_deployment=AZURE_OPENAI_DEPLOYMENT,
        provider_preference=AGENT_MODEL_PROVIDER,
        reasoning_effort=AGENT_REASONING_EFFORT,
        request_timeout_seconds=AI_REQUEST_TIMEOUT_SECONDS,
        governance_timeout_seconds=AI_GOVERNANCE_TIMEOUT_SECONDS,
        enrichment_timeout_seconds=AI_ENRICHMENT_TIMEOUT_SECONDS,
        max_retries=AI_MAX_RETRIES,
    )
    if not provider.is_available():
        configured_provider = provider.configured_provider() or "unconfigured"
        failure = provider.last_failure_reason() or "provider_not_ready"
        return UnavailableAgentModelProvider(
            configured_provider=configured_provider,
            failure_reason=failure,
        )
    return provider
