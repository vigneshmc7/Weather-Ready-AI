from __future__ import annotations

import unittest
from datetime import UTC, datetime
from typing import Any

from stormready_v3.agents.base import AgentContext, AgentRole, AgentStatus
from stormready_v3.agents.conversation_orchestrator import ConversationOrchestratorAgent
from stormready_v3.agents.policy_loader import load_policy


class FakeProvider:
    def __init__(self, response=None, raises=None):
        self.response = response
        self.raises = raises
        self.calls = 0

    def is_available(self) -> bool:
        return True

    def structured_json_call(self, *, system_prompt, user_prompt, max_output_tokens=800):
        del system_prompt, user_prompt, max_output_tokens
        self.calls += 1
        if self.raises is not None:
            raise self.raises
        return self.response


def _ctx(payload: dict[str, Any]) -> AgentContext:
    return AgentContext(
        role=AgentRole.CONVERSATION_ORCHESTRATOR,
        operator_id="op1",
        run_id="run_c",
        triggered_at=datetime.now(UTC),
        payload=payload,
    )


def _base_payload() -> dict[str, Any]:
    return {
        "operator_message": "How's dinner looking tonight?",
        "current_state_digest": {
            "reference_date": "2026-04-14",
            "headline_forecast": {"expected": 112, "low": 98, "high": 126, "confidence": "medium"},
            "near_horizon": [{"service_date": "2026-04-15", "expected": 108}],
            "pending_action": {"kind": "submit_actual", "prompt": "Submit last night's covers"},
            "current_uncertainty": "Weather widening Thursday band.",
            "active_signals_summary": ["Rain risk up for Thursday dinner"],
            "disclaimers": ["Learning is early — patterns may shift."],
        },
        "temporal_digest": {
            "conversation_state": "active",
            "recent_misses": [{"service_date": "2026-04-11", "err_pct": -0.22}],
            "active_hypotheses": [
                {"hypothesis_key": "k1", "proposition": "Rain removes patio covers", "confidence": "medium"}
            ],
            "operator_facts": [],
        },
        "digest_staleness": {"current_state_age_seconds": 60, "source_hash_match": True},
        "recent_turns": [],
        "tool_results": [],
    }


class ConversationOrchestratorAgentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_policy(AgentRole.CONVERSATION_ORCHESTRATOR)

    def test_policy_loads_banned_terms(self) -> None:
        self.assertIn("brooklyn_delta", self.policy.banned_terms)
        self.assertIn("digest", self.policy.banned_terms)

    def test_grounded_reply_passes_through(self) -> None:
        provider = FakeProvider(response={
            "text": "Tonight looks steady at about 112 covers. Rain may widen the band.",
            "tool_calls": [],
            "suggested_messages": ["Show me last Thursday", "Any open questions?"],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        self.assertEqual(result.status, AgentStatus.OK)
        out = result.outputs[0]
        self.assertIn("112", out["text"])
        self.assertEqual(len(out["suggested_messages"]), 2)

    def test_ungrounded_number_sentence_dropped(self) -> None:
        provider = FakeProvider(response={
            "text": "Tonight looks steady at about 112 covers. We had 847 last Friday. Rain may widen the band.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        text = result.outputs[0]["text"]
        self.assertIn("112", text)
        self.assertNotIn("847", text)
        self.assertIn("Rain may widen", text)

    def test_banned_vocab_in_text_drops_only_offending_sentence(self) -> None:
        provider = FakeProvider(response={
            "text": "The brooklyn_delta regime is driving things up. Tonight looks steady at about 112 covers.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        text = result.outputs[0]["text"]
        self.assertNotIn("brooklyn_delta", text.lower())
        self.assertNotIn("regime", text.lower())
        self.assertIn("112", text)

    def test_tool_calls_are_parsed(self) -> None:
        provider = FakeProvider(response={
            "text": "Let me pull the breakdown for that night.",
            "turn": {"question": "breakdown", "target_date": "2026-04-11"},
            "tool_calls": [
                {"name": "query_forecast_detail", "arguments": {"service_date": "2026-04-11"}}
            ],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        out = result.outputs[0]
        self.assertEqual(len(out["tool_calls"]), 1)
        self.assertEqual(out["tool_calls"][0]["name"], "query_forecast_detail")
        self.assertEqual(out["tool_calls"][0]["arguments"]["service_date"], "2026-04-11")
        self.assertEqual(out["turn"]["target_date"], "2026-04-11")

    def test_weather_and_conversation_context_tools_are_parsed(self) -> None:
        provider = FakeProvider(response={
            "text": "I will check the weather detail and recent context.",
            "tool_calls": [
                {"name": "query_service_weather", "arguments": {"service_date": "2026-04-14"}},
                {"name": "query_recent_conversation_context", "arguments": {"limit": 12, "topic": "weather"}},
            ],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        calls = result.outputs[0]["tool_calls"]
        self.assertEqual([call["name"] for call in calls], ["query_service_weather", "query_recent_conversation_context"])

    def test_tool_name_alias_is_parsed(self) -> None:
        provider = FakeProvider(response={
            "text": "I saved that.",
            "tool_calls": [{"tool_name": "check_readiness", "arguments": {}}],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        out = result.outputs[0]
        self.assertEqual(out["tool_calls"][0]["name"], "check_readiness")

    def test_model_requested_capture_note_tool_call_passes_through(self) -> None:
        payload = _base_payload()
        payload["operator_message"] = "We had a private buyout last Friday that filled the room."
        provider = FakeProvider(response={
            "text": "I recorded that note.",
            "tool_calls": [
                {
                    "name": "capture_note",
                    "arguments": {
                        "note": "Private buyout filled the room.",
                        "service_date": "2026-04-10",
                        "service_state": "private_event_or_buyout",
                    },
                }
            ],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        out = result.outputs[0]
        self.assertEqual(out["tool_calls"][0]["name"], "capture_note")
        self.assertEqual(out["tool_calls"][0]["arguments"]["service_date"], "2026-04-10")
        self.assertEqual(out["tool_calls"][0]["arguments"]["service_state"], "private_event_or_buyout")

    def test_model_requested_readiness_tool_call_passes_through(self) -> None:
        payload = _base_payload()
        payload["operator_message"] = "Am I ready to forecast?"
        payload["current_state_digest"]["phase"] = "setup"
        provider = FakeProvider(response={
            "text": "I will check that.",
            "tool_calls": [{"name": "check_readiness", "arguments": {}}],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        out = result.outputs[0]
        self.assertEqual(out["tool_calls"][0]["name"], "check_readiness")

    def test_model_requested_upload_tool_call_passes_through(self) -> None:
        payload = _base_payload()
        payload["current_state_digest"]["phase"] = "enrichment"
        payload["uploaded_file"] = {
            "headers": ["service_date", "covers"],
            "sample_rows": [{"service_date": "2026-04-10", "covers": 120}],
        }
        provider = FakeProvider(response={
            "text": "I can review that.",
            "tool_calls": [
                {
                    "name": "interpret_upload",
                    "arguments": {
                        "headers": ["service_date", "covers"],
                        "sample_rows": [{"service_date": "2026-04-10", "covers": 120}],
                    },
                }
            ],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        out = result.outputs[0]
        self.assertEqual(out["tool_calls"][0]["name"], "interpret_upload")
        self.assertEqual(out["tool_calls"][0]["arguments"]["headers"], ["service_date", "covers"])

    def test_malformed_tool_call_dropped(self) -> None:
        provider = FakeProvider(response={
            "text": "OK.",
            "tool_calls": ["not a dict", {"no_name": True}, {"name": "query_forecast_detail"}],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        out = result.outputs[0]
        self.assertEqual(len(out["tool_calls"]), 1)
        self.assertEqual(out["tool_calls"][0]["name"], "query_forecast_detail")

    def test_suggested_messages_capped_and_length_filtered(self) -> None:
        provider = FakeProvider(response={
            "text": "Tonight looks fine at 112 covers.",
            "tool_calls": [],
            "suggested_messages": [
                "ok",
                "Too long: " + "x" * 100,
                "Show yesterday's breakdown",
                "What's next?",
                "And one more thing",
            ],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        self.assertLessEqual(len(result.outputs[0]["suggested_messages"]), 3)

    def test_staleness_notice_prepended(self) -> None:
        payload = _base_payload()
        payload["digest_staleness"] = {"current_state_age_seconds": 7200, "source_hash_match": True}
        provider = FakeProvider(response={
            "text": "Tonight looks steady at 112 covers.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        self.assertIn("snapshot", result.outputs[0]["text"].lower())

    def test_source_hash_mismatch_triggers_notice(self) -> None:
        payload = _base_payload()
        payload["digest_staleness"] = {"current_state_age_seconds": 10, "source_hash_match": False}
        provider = FakeProvider(response={
            "text": "Tonight looks steady at 112 covers.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        self.assertIn("snapshot", result.outputs[0]["text"].lower())

    def test_provider_none_returns_ai_unavailable_only(self) -> None:
        provider = FakeProvider(response=None)
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        self.assertEqual(result.status, AgentStatus.OK)
        self.assertNotIn("112", result.outputs[0]["text"])
        self.assertEqual(result.outputs[0]["tool_calls"], [])
        self.assertEqual(result.outputs[0]["suggested_messages"], [])
        self.assertIn("model unavailable", result.rationale)

    def test_provider_none_does_not_invent_setup_tool_call(self) -> None:
        payload = _base_payload()
        payload["operator_message"] = "Am I ready to forecast?"
        payload["current_state_digest"]["phase"] = "setup"
        payload["current_state_digest"]["headline_forecast"] = None
        provider = FakeProvider(response=None)
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        self.assertEqual(result.status, AgentStatus.OK)
        self.assertEqual(result.outputs[0]["tool_calls"], [])
        self.assertEqual(result.outputs[0]["suggested_messages"], [])

    def test_provider_exception_returns_ai_unavailable_only(self) -> None:
        provider = FakeProvider(raises=RuntimeError("boom"))
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        self.assertEqual(result.status, AgentStatus.OK)
        self.assertIn("model unavailable", result.rationale)
        self.assertIn("RuntimeError", result.rationale)

    def test_provider_exception_does_not_invent_weather_lookup_tools(self) -> None:
        payload = _base_payload()
        payload["operator_message"] = "Is rain an issue tonight?"
        provider = FakeProvider(raises=RuntimeError("boom"))
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))

        names = [call["name"] for call in result.outputs[0]["tool_calls"]]
        self.assertEqual(names, [])
        self.assertNotIn("rain", result.outputs[0]["text"].lower())

    def test_provider_exception_does_not_invent_refresh_tool(self) -> None:
        payload = _base_payload()
        payload["operator_message"] = "Refresh the forecast"
        provider = FakeProvider(raises=RuntimeError("boom"))
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))

        self.assertEqual(result.outputs[0]["tool_calls"], [])

    def test_provider_exception_does_not_render_tool_result_answer(self) -> None:
        payload = _base_payload()
        payload["tool_results"] = [
            {
                "tool": "query_forecast_detail",
                "success": True,
                "message": "",
                "data": {
                    "service_date": "2026-04-14",
                    "forecast_expected": 112,
                    "range": "98-126",
                    "top_drivers": ["precip_overlap"],
                    "vs_usual_pct": -4,
                },
            }
        ]
        provider = FakeProvider(raises=RuntimeError("boom"))
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))

        text = result.outputs[0]["text"]
        self.assertNotIn("112 covers", text)
        self.assertNotIn("98-126", text)
        self.assertNotIn("-4% vs usual", text)
        self.assertEqual(result.outputs[0]["tool_calls"], [])

    def test_model_can_answer_from_tool_result_without_range_language(self) -> None:
        payload = _base_payload()
        payload["tool_results"] = [
            {
                "tool": "query_forecast_detail",
                "success": True,
                "message": "",
                "data": {
                    "service_date": "2026-04-14",
                    "forecast_expected": 112,
                    "top_drivers": ["precip_overlap"],
                    "vs_usual_pct": -4,
                },
            }
        ]
        provider = FakeProvider(response={
            "text": "Tuesday dinner is about 112 covers, 4% below usual.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))

        text = result.outputs[0]["text"]
        self.assertIn("112 covers", text)
        self.assertIn("4% below usual", text)
        self.assertNotIn("range", text.lower())

    def test_empty_text_triggers_ai_unavailable_only(self) -> None:
        provider = FakeProvider(response={"text": "", "tool_calls": [], "suggested_messages": []})
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(_base_payload()))
        self.assertEqual(result.status, AgentStatus.OK)
        self.assertNotIn("112", result.outputs[0]["text"])
        self.assertIn("could not produce a reply", result.outputs[0]["text"])

    def test_tool_result_number_is_groundable(self) -> None:
        payload = _base_payload()
        payload["tool_results"] = [{"service_date": "2026-04-11", "actual_total": 82, "forecast_expected": 110}]
        provider = FakeProvider(response={
            "text": "Last Thursday came in at 82 covers against a forecast of 110.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        text = result.outputs[0]["text"]
        self.assertIn("82", text)
        self.assertIn("110", text)

    def test_answer_packet_weather_number_is_groundable(self) -> None:
        payload = _base_payload()
        payload["answer_packet"] = {
            "forecast_detail": {
                "service_date": "2026-04-29",
                "forecast_expected": 136,
                "weather_context": {
                    "condition": "heavy rain",
                    "dinner_overlap": True,
                    "weather_effect_pct": -31,
                },
            }
        }
        provider = FakeProvider(response={
            "text": "Heavy rain overlaps dinner, and weather is pulling the forecast down about 31%.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        text = result.outputs[0]["text"]
        self.assertIn("31%", text)
        self.assertIn("Heavy rain", text)

    def test_recent_turn_number_is_groundable(self) -> None:
        payload = _base_payload()
        payload["recent_turns"] = [
            {"role": "assistant", "content": "Saturday dinner was about 135 covers."}
        ]
        provider = FakeProvider(response={
            "text": "Saturday dinner was about 135 covers.",
            "tool_calls": [],
            "suggested_messages": [],
        })
        agent = ConversationOrchestratorAgent(self.policy, provider)
        result = agent.run(_ctx(payload))
        self.assertIn("135", result.outputs[0]["text"])


if __name__ == "__main__":
    unittest.main()
