from __future__ import annotations

from stormready_v3.ai import factory
from stormready_v3.ai.factory import UnavailableAgentModelProvider, build_agent_model_provider


def test_build_agent_model_provider_degrades_when_unavailable(monkeypatch):
    class MissingProvider:
        def __init__(self, **kwargs):
            pass

        def is_available(self):
            return False

        def configured_provider(self):
            return "azure"

        def last_failure_reason(self):
            return "missing_config"

    monkeypatch.setattr(factory, "OpenAIAgentModelProvider", MissingProvider)

    provider = build_agent_model_provider()

    assert isinstance(provider, UnavailableAgentModelProvider)
    assert provider.is_available() is False
    assert provider.configured_provider() == "azure"
    assert provider.last_failure_reason() == "missing_config"
    assert provider.structured_json_call(system_prompt="", user_prompt="") is None


def test_build_agent_model_provider_returns_available_provider(monkeypatch):
    class AvailableProvider:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def is_available(self):
            return True

    monkeypatch.setattr(factory, "OpenAIAgentModelProvider", AvailableProvider)

    provider = build_agent_model_provider()

    assert isinstance(provider, AvailableProvider)
