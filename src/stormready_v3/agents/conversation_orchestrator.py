"""Conversation Orchestrator — operator-facing chat surface.

See ``policies/conversation_orchestrator.md`` for the full role contract.

The retrievers (current_state_retriever, temporal_memory_retriever) hand C
typed context. C's job is to understand the operator turn, choose tools when the
answer needs more data, and compose the final operator-facing reply. The caller
may run C a second time with tool results and a compact answer packet attached.

C never invents numbers. If the model is unavailable, or if grounding drops the
entire reply, C returns only a narrow failure message instead of composing a
deterministic substitute answer.

Tool execution is the caller's job. C emits ``tool_calls`` (parsed and
validated); the wiring layer dispatches them, collects results, and may re-run
the turn with results attached.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable

from .base import (
    AgentContext,
    AgentResult,
    AgentRole,
    AgentStatus,
    BaseAgent,
)


_MAX_SUGGESTED = 3
_MAX_SUGGESTED_LEN = 60
_MAX_TEXT_CHARS = 900
_MODEL_FAILURE_TEXT = "I cannot answer in chat right now because the AI response was unavailable."
_MODEL_UNGROUNDED_TEXT = "I could not ground that answer cleanly, so I will not guess."
_NUMBER_RE = re.compile(r"(?<![\w.])(-?\d+(?:\.\d+)?)(?!\w)")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


KNOWN_TOOLS = {
    "update_profile": {
        "description": "Create or update the restaurant profile during setup.",
        "arguments": {
            "restaurant_name": "str | null",
            "canonical_address": "str | null",
            "city": "str | null",
            "timezone": "str | null",
            "neighborhood_type": "str | null",
            "demand_mix": "str | null",
            "patio_enabled": "bool | null",
            "patio_seat_capacity": "int | null",
            "patio_season_mode": "str | null",
            "weekly_baselines": "dict | null",
        },
    },
    "set_location_relevance": {
        "description": "Update nearby transit, venue, hotel, or travel relevance.",
        "arguments": {
            "transit_relevance": "bool | null",
            "venue_relevance": "bool | null",
            "hotel_travel_relevance": "bool | null",
        },
    },
    "check_readiness": {
        "description": "Check setup readiness after profile updates or readiness questions.",
        "arguments": {},
    },
    "interpret_upload": {
        "description": "Interpret uploaded historical cover data headers and sample rows.",
        "arguments": {
            "headers": "list[str]",
            "sample_rows": "list[dict]",
        },
    },
    "query_forecast_detail": {
        "description": "Single-day forecast driver breakdown.",
        "arguments": {"service_date": "str (YYYY-MM-DD)"},
    },
    "query_forecast_why": {
        "description": "Compact date-specific reason packet for forecast, demand, weather, or follow-up why questions.",
        "arguments": {"service_date": "str (YYYY-MM-DD)"},
    },
    "query_hypothesis_backlog": {
        "description": "List open/confirmed/rejected hypotheses.",
        "arguments": {"status": "str | null (open|confirmed|rejected)"},
    },
    "query_learning_state": {
        "description": "Current learning state snapshot for a cascade.",
        "arguments": {"cascade": "str | null"},
    },
    "query_actuals_history": {
        "description": "Recent submitted actuals with forecast deltas.",
        "arguments": {"limit": "int", "state_filter": "str | null"},
    },
    "query_recent_signals": {
        "description": "Recent signal log rows.",
        "arguments": {"limit": "int", "dependency_group": "str | null"},
    },
    "capture_note": {
        "description": "Record an operator note about past or upcoming service.",
        "arguments": {
            "note": "str",
            "service_date": "str | null (YYYY-MM-DD)",
            "service_state": "str | null",
        },
    },
    "request_refresh": {
        "description": "Refresh forecasts when the operator asks for an update.",
        "arguments": {"reason": "str | null"},
    },
}


class ConversationOrchestratorAgent(BaseAgent):
    role = AgentRole.CONVERSATION_ORCHESTRATOR

    def run(self, ctx: AgentContext) -> AgentResult:
        payload = dict(ctx.payload)
        operator_message = str(payload.get("operator_message") or "").strip()
        current_digest = payload.get("current_state_digest") or {}
        temporal_digest = payload.get("temporal_digest") or {}
        tool_results = payload.get("tool_results") or []
        staleness = payload.get("digest_staleness") or {}

        try:
            response = self.provider.structured_json_call(
                system_prompt=self.policy.system_prompt_body,
                user_prompt=self._build_user_prompt(payload),
                max_output_tokens=self.policy.max_tokens,
            )
        except Exception as exc:  # noqa: BLE001
            return AgentResult(
                role=self.role,
                run_id=ctx.run_id,
                status=AgentStatus.OK,
                outputs=[_model_failure_envelope()],
                rationale=f"model unavailable: {type(exc).__name__}: {exc}",
            )

        if response is None:
            return AgentResult(
                role=self.role,
                run_id=ctx.run_id,
                status=AgentStatus.OK,
                outputs=[_model_failure_envelope()],
                rationale="model unavailable: provider returned None",
            )

        envelope = self._parse_envelope(response)
        if not envelope:
            return AgentResult(
                role=self.role,
                run_id=ctx.run_id,
                status=AgentStatus.OK,
                outputs=[_model_failure_envelope()],
                rationale="model unavailable: envelope parse failed",
            )

        grounded_text = self._apply_line_grounding(
            envelope["text"],
            current_digest,
            temporal_digest,
            tool_results,
            payload.get("recent_turns") or [],
            payload.get("answer_packet") or {},
        )
        grounded_text = _scrub_banned_vocabulary(grounded_text, self.policy.banned_terms)
        if not grounded_text:
            grounded_text = _MODEL_UNGROUNDED_TEXT
        else:
            grounded_text = self._prepend_staleness_notice(grounded_text, staleness)
        grounded_text = grounded_text[:_MAX_TEXT_CHARS].strip()

        suggestions = [
            s for s in envelope["suggested_messages"]
            if _scrub_banned_vocabulary(s, self.policy.banned_terms) == s
        ][:_MAX_SUGGESTED]

        output = {
            "text": grounded_text,
            "tool_calls": envelope["tool_calls"],
            "suggested_messages": suggestions,
            "turn": envelope.get("turn") or {},
        }

        return AgentResult(
            role=self.role,
            run_id=ctx.run_id,
            status=AgentStatus.OK,
            outputs=[output],
            rationale=f"tool_calls={len(envelope['tool_calls'])} suggestions={len(suggestions)}",
        )

    def _build_user_prompt(self, payload: dict[str, Any]) -> str:
        tool_list = [
            {"name": name, **schema}
            for name, schema in KNOWN_TOOLS.items()
        ]
        compact = {
            "current_state_digest": payload.get("current_state_digest"),
            "temporal_digest": payload.get("temporal_digest"),
            "digest_staleness": payload.get("digest_staleness") or {},
            "recent_turns": payload.get("recent_turns") or [],
            "prior_turn_facts": payload.get("prior_turn_facts") or [],
            "operator_message": payload.get("operator_message"),
            "available_tools": tool_list,
            "answer_packet": payload.get("answer_packet") or {},
            "tool_results": payload.get("tool_results") or [],
        }
        return json.dumps(compact, default=str, ensure_ascii=False, indent=2)

    def _parse_envelope(self, response: dict[str, Any]) -> dict[str, Any] | None:
        text = response.get("text")
        if not isinstance(text, str) or not text.strip():
            return None
        tool_calls_raw = response.get("tool_calls") or []
        tool_calls: list[dict[str, Any]] = []
        if isinstance(tool_calls_raw, list):
            for item in tool_calls_raw:
                if not isinstance(item, dict):
                    continue
                name = item.get("name") or item.get("tool_name")
                if not isinstance(name, str):
                    continue
                if name not in KNOWN_TOOLS:
                    continue
                args = item.get("arguments") or {}
                if not isinstance(args, dict):
                    args = {}
                tool_calls.append({"name": name, "arguments": args})
        suggestions_raw = response.get("suggested_messages") or []
        suggestions: list[str] = []
        if isinstance(suggestions_raw, list):
            for s in suggestions_raw:
                if not isinstance(s, str):
                    continue
                s = s.strip()
                if not s or len(s) > _MAX_SUGGESTED_LEN:
                    continue
                suggestions.append(s)
                if len(suggestions) >= _MAX_SUGGESTED:
                    break
        return {
            "text": text.strip(),
            "tool_calls": tool_calls,
            "suggested_messages": suggestions,
            "turn": response.get("turn") if isinstance(response.get("turn"), dict) else {},
        }

    def _apply_line_grounding(
        self,
        text: str,
        current_digest: dict[str, Any],
        temporal_digest: dict[str, Any],
        tool_results: list[Any],
        recent_turns: list[Any] | None = None,
        answer_packet: Any | None = None,
    ) -> str:
        allowed_numbers = _collect_numbers(
            current_digest,
            temporal_digest,
            tool_results,
            recent_turns or [],
            answer_packet,
        )
        sentences = _split_sentences(text)
        kept: list[str] = []
        for sentence in sentences:
            numbers_in_sentence = [m.group(1) for m in _NUMBER_RE.finditer(sentence)]
            sentence_ok = True
            for raw in numbers_in_sentence:
                if raw in _CARDINAL_WORDS_STR:
                    continue
                if raw in allowed_numbers:
                    continue
                sentence_ok = False
                break
            if sentence_ok:
                kept.append(sentence)
        return " ".join(kept).strip()

    def _prepend_staleness_notice(self, text: str, staleness: dict[str, Any]) -> str:
        try:
            age = float(staleness.get("current_state_age_seconds", 0) or 0)
        except (TypeError, ValueError):
            age = 0.0
        match = staleness.get("source_hash_match", True)
        if age > 3600 or match is False:
            notice = "Working from a snapshot taken earlier — the latest refresh may shift these numbers."
            if notice not in text:
                return f"{notice} {text}".strip()
        return text



_CARDINAL_WORDS_STR = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}


def _collect_numbers(
    current_digest: dict[str, Any],
    temporal_digest: dict[str, Any],
    tool_results: Iterable[Any],
    recent_turns: Iterable[Any] = (),
    answer_packet: Any | None = None,
) -> set[str]:
    allowed: set[str] = set(_CARDINAL_WORDS_STR)
    _walk_for_numbers(current_digest, allowed)
    _walk_for_numbers(temporal_digest, allowed)
    for result in tool_results or []:
        _walk_for_numbers(result, allowed)
    for turn in recent_turns or []:
        _walk_for_numbers(turn, allowed)
    _walk_for_numbers(answer_packet, allowed)
    return allowed


def _walk_for_numbers(value: Any, out: set[str]) -> None:
    if isinstance(value, (int, float)):
        out.add(_number_key(value))
        if value < 0:
            out.add(_number_key(abs(value)))
    elif isinstance(value, str):
        for m in _NUMBER_RE.finditer(value):
            out.add(m.group(1))
    elif isinstance(value, dict):
        for v in value.values():
            _walk_for_numbers(v, out)
    elif isinstance(value, (list, tuple, set)):
        for v in value:
            _walk_for_numbers(v, out)


def _number_key(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _split_sentences(text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []
    parts = _SENTENCE_SPLIT_RE.split(text)
    return [p.strip() for p in parts if p.strip()]


def _scrub_banned_vocabulary(text: str, banned_terms: Iterable[str]) -> str:
    terms = tuple(str(term).lower() for term in banned_terms if str(term).strip())
    if not terms:
        return text
    kept: list[str] = []
    for sentence in _split_sentences(text):
        lowered = sentence.lower()
        if any(term in lowered for term in terms):
            continue
        kept.append(sentence)
    return " ".join(kept).strip()


def _model_failure_envelope() -> dict[str, Any]:
    return {"text": _MODEL_FAILURE_TEXT, "tool_calls": [], "suggested_messages": [], "turn": {}}


KNOWN_READ_TOOLS = KNOWN_TOOLS

__all__ = ["ConversationOrchestratorAgent", "KNOWN_TOOLS", "KNOWN_READ_TOOLS"]
