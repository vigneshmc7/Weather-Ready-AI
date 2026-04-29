"""Unified StormReady agent — handles onboarding and operations in one conversation.

The agent maintains phase awareness (setup vs operations). It uses tool-use to map
operator language to system contracts, with the model owning the operator-facing
conversation path.

Conversation history is persisted in DuckDB so it survives page refreshes and
feeds future context. Operator behavior preferences are loaded into the prompt
and updated from conversation signals.
"""
from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from stormready_v3.agents.base import AgentContext, AgentDispatcher, AgentRole
from stormready_v3.ai.contracts import AgentModelProvider
from stormready_v3.agents.tools import ToolExecutor, ToolResult
from stormready_v3.conversation.memory import ConversationMemoryService
from stormready_v3.conversation.promotion import LearningPromotionService
from stormready_v3.operator_text import communication_payload, driver_label, render_communication_payload
from stormready_v3.setup.readiness import summarize_setup_readiness
from stormready_v3.storage.db import Database
from stormready_v3.storage.repositories import OperatorContextDigestRepository, OperatorRepository
from stormready_v3.workflows.setup_context_digests import ensure_setup_context_digests


_CHAT_AI_UNAVAILABLE_TEXT = "I cannot answer in chat right now because the AI response was unavailable."


# ---------------------------------------------------------------------------
# Agent response model
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class SuggestedMessage:
    """Legacy response shape retained for API compatibility."""
    label: str
    value: str
    category: str = "action"  # action, yes_no, skip


@dataclass(slots=True)
class AgentResponse:
    """What the agent returns to the UI."""
    text: str
    tool_results: list[ToolResult] = field(default_factory=list)
    suggested_messages: list[SuggestedMessage] = field(default_factory=list)
    operator_id: str | None = None
    phase: str = "setup"  # setup, enrichment, operations


# ---------------------------------------------------------------------------
# Phase detection
# ---------------------------------------------------------------------------

def detect_phase(db: Database, operator_id: str | None) -> str:
    """Determine conversation phase from operator state."""
    if operator_id is None:
        return "setup"
    repo = OperatorRepository(db)
    profile = repo.load_operator_profile(operator_id)
    if profile is None:
        return "setup"
    baseline_row = db.fetchone(
        "SELECT COUNT(*) FROM operator_weekly_baselines WHERE operator_id = ? AND baseline_total_covers > 0",
        [operator_id],
    )
    has_baselines = baseline_row is not None and baseline_row[0] > 0
    summary = summarize_setup_readiness(profile, primary_window_has_baseline=has_baselines)
    if not summary.forecast_ready:
        return "setup"
    forecast_row = db.fetchone(
        "SELECT COUNT(*) FROM published_forecast_state WHERE operator_id = ?",
        [operator_id],
    )
    has_forecasts = forecast_row is not None and forecast_row[0] > 0
    return "operations" if has_forecasts else "enrichment"


def _resolve_agent_reference_date(db: Database, operator_id: str | None, reference_date: date | None) -> date:
    """Use the latest published strip start when no explicit reference date is provided.

    This keeps the agent grounded when working against historical replay databases,
    where `date.today()` would otherwise point outside the available forecast window.
    """
    if reference_date is not None or operator_id is None:
        return reference_date or date.today()
    latest_strip_row = db.fetchone(
        """
        SELECT MIN(service_date)
        FROM published_forecast_state
        WHERE operator_id = ?
          AND last_published_at = (
            SELECT MAX(last_published_at)
            FROM published_forecast_state
            WHERE operator_id = ?
          )
        """,
        [operator_id, operator_id],
    )
    if latest_strip_row is not None and latest_strip_row[0] is not None:
        return latest_strip_row[0]
    actuals_row = db.fetchone(
        "SELECT MAX(service_date) FROM operator_actuals WHERE operator_id = ?",
        [operator_id],
    )
    if actuals_row is not None and actuals_row[0] is not None:
        return actuals_row[0]
    return date.today()


# ---------------------------------------------------------------------------
# Conversation persistence
# ---------------------------------------------------------------------------

def _save_message(db: Database, operator_id: str, role: str, content: str, phase: str,
                  tool_calls: list[dict] | None = None, tool_results: list[ToolResult] | None = None) -> None:
    """Persist a conversation message to DuckDB."""
    tool_calls_json = json.dumps(tool_calls, default=str) if tool_calls else None
    results_json = None
    if tool_results:
        results_json = json.dumps(
            [{"tool": r.tool_name, "ok": r.success, "msg": r.message} for r in tool_results],
            default=str,
        )
    try:
        db.execute(
            """INSERT INTO conversation_messages (operator_id, role, content, tool_calls_json, tool_results_json, phase)
            VALUES (?, ?, ?, ?, ?, ?)""",
            [operator_id, role, content, tool_calls_json, results_json, phase],
        )
    except Exception:
        pass  # Table may not exist yet — don't break the conversation


def _load_message_page(
    db: Database,
    operator_id: str,
    *,
    limit: int = 30,
    before_id: int | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Load a chronologically ordered page of conversation messages."""
    try:
        if before_id is None:
            rows = db.fetchall(
                """
                SELECT message_id, role, content, created_at
                FROM conversation_messages
                WHERE operator_id = ?
                ORDER BY message_id DESC
                LIMIT ?
                """,
                [operator_id, limit + 1],
            )
        else:
            rows = db.fetchall(
                """
                SELECT message_id, role, content, created_at
                FROM conversation_messages
                WHERE operator_id = ?
                  AND message_id < ?
                ORDER BY message_id DESC
                LIMIT ?
                """,
                [operator_id, before_id, limit + 1],
            )
    except Exception:
        return [], False
    has_more = len(rows) > limit
    page_rows = rows[:limit]
    messages = [
        {
            "message_id": row[0],
            "role": row[1],
            "content": row[2],
            "created_at": row[3],
        }
        for row in reversed(page_rows)
    ]
    return messages, has_more


def _load_recent_history(db: Database, operator_id: str, limit: int = 10) -> list[dict[str, Any]]:
    """Load recent conversation messages from DuckDB."""
    messages, _ = _load_message_page(db, operator_id, limit=limit)
    return messages


def _format_recent_turns(db: Database, operator_id: str, limit: int = 6) -> list[dict[str, str]]:
    """Return recent turns as the {role, content} shape Agent C expects."""
    messages, _ = _load_message_page(db, operator_id, limit=limit)
    return [
        {"role": str(msg.get("role") or ""), "content": str(msg.get("content") or "")}
        for msg in messages
    ]


def _age_seconds(produced_at: Any, now: datetime) -> float:
    if produced_at is None:
        return 0.0
    if isinstance(produced_at, datetime):
        ts = produced_at
    elif isinstance(produced_at, str):
        try:
            ts = datetime.fromisoformat(produced_at)
        except ValueError:
            return 0.0
    else:
        return 0.0
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return max(0.0, (now - ts).total_seconds())


# ---------------------------------------------------------------------------
# Operator behavior preferences
# ---------------------------------------------------------------------------

def _detect_and_update_behavior(db: Database, operator_id: str, message: str, ai_text: str) -> None:
    """Detect preference signals from the conversation and update behavior state."""
    lowered = message.lower()
    updates: dict[str, Any] = {}

    # Brevity signals
    if any(phrase in lowered for phrase in ("too long", "shorter", "just the number", "tldr", "tl;dr", "keep it short")):
        updates["brevity_preference"] = "brief"
    elif any(phrase in lowered for phrase in ("tell me more", "explain more", "why exactly", "break it down", "more detail")):
        updates["brevity_preference"] = "detailed"

    # Staffing risk signals
    if any(phrase in lowered for phrase in ("i'd rather overstaff", "better safe", "staff high", "worst case")):
        updates["staffing_risk_bias"] = 0.8
    elif any(phrase in lowered for phrase in ("don't overstaff", "run lean", "keep it tight", "minimum")):
        updates["staffing_risk_bias"] = 0.3

    # Explanation style signals
    if any(phrase in lowered for phrase in ("just the numbers", "give me the numbers", "how many")):
        updates["preferred_explanation_style"] = "numbers_first"

    # Always increment conversation count
    if not updates:
        try:
            db.execute(
                """UPDATE operator_behavior_state
                SET conversation_count = COALESCE(conversation_count, 0) + 1,
                    last_updated_at = CURRENT_TIMESTAMP
                WHERE operator_id = ?""",
                [operator_id],
            )
        except Exception:
            pass
        return

    set_clauses = ["conversation_count = COALESCE(conversation_count, 0) + 1", "last_updated_at = CURRENT_TIMESTAMP"]
    params: list[Any] = []
    for col, val in updates.items():
        set_clauses.append(f"{col} = ?")
        params.append(val)
    params.append(operator_id)
    try:
        db.execute(f"UPDATE operator_behavior_state SET {', '.join(set_clauses)} WHERE operator_id = ?", params)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Conversation policy helpers
# ---------------------------------------------------------------------------

def _parse_yes_no_reply(message: str) -> bool | None:
    lowered = re.sub(r"[^a-z0-9']+", " ", message.strip().lower()).strip()
    yes_tokens = {
        "yes", "y", "yeah", "yep", "correct", "it does", "definitely", "it matters", "yes it does",
    }
    no_tokens = {
        "no", "n", "nope", "not really", "it doesn't", "it does not", "doesn't matter", "no it doesn't",
    }
    if lowered in yes_tokens or lowered.startswith("yes "):
        return True
    if lowered in no_tokens or lowered.startswith("no "):
        return False
    return None


def _parse_unsure_reply(message: str) -> bool:
    lowered = re.sub(r"[^a-z0-9']+", " ", message.strip().lower()).strip()
    unsure_tokens = {
        "not sure",
        "not certain",
        "unsure",
        "i don't know",
        "i dont know",
        "don't know",
        "dont know",
        "maybe",
        "unclear",
    }
    return lowered in unsure_tokens


def _looks_like_substantive_note_reply(message: str) -> bool:
    stripped = message.strip()
    if len(stripped) < 10:
        return False
    if stripped.endswith("?"):
        return False
    lowered = stripped.lower()
    if lowered.startswith(("what ", "why ", "how ", "show ", "tell ", "can you ", "could you ")):
        return False
    return True


def _maybe_resolve_learning_agenda_reply(
    db: Database,
    executor: ToolExecutor,
    *,
    operator_id: str | None,
    message: str,
    learning_agenda_key: str | None = None,
) -> AgentResponse | None:
    if operator_id is None:
        return None
    memory = ConversationMemoryService(db)
    promotion = LearningPromotionService(db, executor)
    explicit_agenda_key = str(learning_agenda_key or "").strip()
    item = (
        memory.learning_agenda_item(operator_id, explicit_agenda_key)
        if explicit_agenda_key
        else memory.most_recent_asked_question(operator_id)
    )
    if item is None:
        return None

    agenda_key = str(item.get("agenda_key") or "")
    question_kind = str(item.get("question_kind") or "")
    service_date = item.get("service_date")
    tool_results: list[ToolResult] = []

    if _parse_unsure_reply(message):
        if item.get("hypothesis_key"):
            memory.resolve_hypothesis(
                operator_id=operator_id,
                hypothesis_key=str(item["hypothesis_key"]),
                status="stale",
                resolution_note="operator was not sure",
            )
        memory.resolve_agenda_item(
            operator_id=operator_id,
            agenda_key=agenda_key,
            resolution_note="operator was not sure",
        )
        response_text = _learning_resolution_text(
            recorded_prefix="No problem. I will leave that out for now.",
            semantic_payload={
                "what_is_still_uncertain": "I will not treat it as a confirmed recurring pattern.",
            },
        )
        return AgentResponse(
            text=response_text,
            tool_results=tool_results,
            operator_id=operator_id,
            phase="operations",
            suggested_messages=[],
        )

    if question_kind == "yes_no":
        answer = _parse_yes_no_reply(message)
        if answer is None:
            return None
        promotion_result = promotion.resolve_yes_no(
            operator_id=operator_id,
            agenda_item=item,
            answer=answer,
        )
        tool_results.extend(promotion_result.tool_results)
        for fact in promotion_result.fact_updates:
            memory.upsert_fact(
                operator_id=operator_id,
                fact_key=str(fact["fact_key"]),
                fact_value=fact["fact_value"],
                confidence=str(fact.get("confidence") or "high"),
                provenance=str(fact.get("provenance") or "operator_confirmed"),
                source_ref=str(fact.get("source_ref") or f"learning_agenda::{agenda_key}"),
            )
        if item.get("hypothesis_key"):
            memory.resolve_hypothesis(
                operator_id=operator_id,
                hypothesis_key=str(item["hypothesis_key"]),
                status="confirmed" if answer else "rejected",
                resolution_note=f"operator answered {'yes' if answer else 'no'}",
            )
        memory.resolve_agenda_item(
            operator_id=operator_id,
            agenda_key=agenda_key,
            resolution_note=f"operator answered {'yes' if answer else 'no'}",
        )
        response_text = _learning_resolution_text(
            recorded_prefix="I recorded your answer.",
            semantic_payload=promotion_result.communication_payload,
        )
        return AgentResponse(
            text=response_text,
            tool_results=tool_results,
            operator_id=operator_id,
            phase="operations",
            suggested_messages=[],
        )

    if question_kind == "free_text" and (explicit_agenda_key or _looks_like_substantive_note_reply(message)):
        capture_args: dict[str, Any] = {"note": message.strip()}
        if service_date is not None:
            capture_args["service_date"] = service_date.isoformat()
        capture_result = executor.execute(operator_id, "capture_note", capture_args)
        tool_results.append(capture_result)
        if not capture_result.success:
            return None
        promotion_result = promotion.resolve_free_text(
            operator_id=operator_id,
            agenda_item=item,
            message=message,
        )
        tool_results.extend(promotion_result.tool_results)
        for fact in promotion_result.fact_updates:
            memory.upsert_fact(
                operator_id=operator_id,
                fact_key=str(fact["fact_key"]),
                fact_value=fact["fact_value"],
                confidence=str(fact.get("confidence") or "medium"),
                provenance=str(fact.get("provenance") or "operator_confirmed"),
                source_ref=str(fact.get("source_ref") or f"learning_agenda::{agenda_key}"),
            )
        memory.upsert_fact(
            operator_id=operator_id,
            fact_key=f"agenda_note::{agenda_key}",
            fact_value={
                "note": message.strip(),
                "service_date": service_date.isoformat() if service_date is not None else None,
            },
            confidence="medium",
            provenance="operator_note",
            source_ref=f"learning_agenda::{agenda_key}",
            valid_from_date=service_date if service_date is not None else None,
        )
        if item.get("hypothesis_key"):
            memory.resolve_hypothesis(
                operator_id=operator_id,
                hypothesis_key=str(item["hypothesis_key"]),
                status="confirmed",
                resolution_note="operator supplied a qualitative explanation",
            )
        memory.resolve_agenda_item(
            operator_id=operator_id,
            agenda_key=agenda_key,
            resolution_note="operator supplied a qualitative explanation",
        )
        date_fragment = f" for {service_date}" if service_date is not None else ""
        response_text = _learning_resolution_text(
            recorded_prefix=f"I recorded that context{date_fragment}.",
            semantic_payload=promotion_result.communication_payload,
        )
        return AgentResponse(
            text=response_text,
            tool_results=tool_results,
            operator_id=operator_id,
            phase="operations",
            suggested_messages=[],
        )
    return None


_VOICE_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")
_SAME_VALUE_MOVE_RE = re.compile(
    r"\bmoved\s+(sideways|up|down)\s+from\s+(\d+(?:\.\d+)?)\s+to\s+\2\b",
    re.IGNORECASE,
)
_COLON_PREFIX_RE = re.compile(
    r"^\s*(?:Main driver|Before service|Midday update|During service|Morning plan):\s*",
    re.IGNORECASE,
)
_SYSTEM_VERB_RE = re.compile(r"\bThe system\s+(flagged|detected|noted|recorded)\b", re.IGNORECASE)
_COVER_COUNT_RE = re.compile(
    r"\b(?:about|around|at|forecast(?:ed)?|expected(?: at)?|came in at)\s+(\d{1,4})\s+covers?\b",
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
_NAMED_DATE_RE = re.compile(
    r"\b(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)(?:,?\s+[A-Z][a-z]{2,8}\s+\d{1,2})?\b|"
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2}\b",
    re.IGNORECASE,
)


def _post_generation_guard(text: str) -> str:
    sentences = _split_voice_sentences(str(text or ""))
    kept: list[str] = []
    for sentence in sentences:
        sentence = _COLON_PREFIX_RE.sub("", sentence).strip()
        sentence = _SYSTEM_VERB_RE.sub(lambda m: f"I {m.group(1).lower()}", sentence)
        if _SAME_VALUE_MOVE_RE.search(sentence):
            continue
        if sentence:
            kept.append(sentence)
    return " ".join(kept).strip()


def _split_voice_sentences(text: str) -> list[str]:
    text = str(text or "").strip()
    if not text:
        return []
    return [part.strip() for part in _VOICE_SENTENCE_SPLIT_RE.split(text) if part.strip()]


def _extract_prior_turn_facts(recent_turns: list[dict[str, str]]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for turn in recent_turns:
        if str(turn.get("role") or "").lower() != "assistant":
            continue
        content = str(turn.get("content") or "")
        expected_match = _COVER_COUNT_RE.search(content)
        if expected_match is None:
            continue
        date_match = _ISO_DATE_RE.search(content) or _NAMED_DATE_RE.search(content)
        facts.append(
            {
                "date": date_match.group(0) if date_match is not None else None,
                "expected": int(expected_match.group(1)),
            }
        )
        if len(facts) >= 6:
            break
    return facts


def _should_auto_retrieve_context(
    *,
    operator_message: str,
    output: dict[str, Any],
    current_digest: dict[str, Any],
    recent_turns: list[dict[str, str]],
) -> bool:
    if str((current_digest or {}).get("phase") or "operations") != "operations":
        return False
    tool_calls = output.get("tool_calls") or []
    if isinstance(tool_calls, list) and tool_calls:
        return False
    text = str(output.get("text") or "").strip()
    if _is_ai_failure_text(text):
        return True
    lowered = operator_message.lower()
    context_terms = (
        "rain", "weather", "precip", "chance", "likely", "alert", "covers",
        "forecast", "demand", "low", "high", "busy", "slow", "why", "usual",
    )
    followup_terms = ("it", "that", "this", "you mean", "how likely", "why", "tomorrow", "tonight")
    looks_contextual = any(term in lowered for term in context_terms)
    looks_followup = any(term in lowered for term in followup_terms) or len(lowered.split()) <= 6
    if not looks_contextual and not looks_followup:
        return False
    service_date = _resolve_context_service_date(operator_message, current_digest, recent_turns)
    return service_date is not None or looks_contextual


def _should_preload_forecast_why(
    *,
    operator_message: str,
    current_digest: dict[str, Any],
    recent_turns: list[dict[str, str]],
) -> bool:
    if str((current_digest or {}).get("phase") or "operations") != "operations":
        return False
    if _resolve_context_service_date(operator_message, current_digest, recent_turns) is None:
        return False
    lowered = operator_message.lower()
    why_terms = (
        "why", "because", "reason", "driver", "driving", "low", "high",
        "busy", "slow", "demand", "covers", "forecast", "usual",
    )
    weather_terms = ("rain", "weather", "precip", "likely", "alert", "storm", "snow")
    short_followup = len(lowered.split()) <= 6 and any(term in lowered for term in ("why", "that", "it", "rain"))
    return any(term in lowered for term in why_terms + weather_terms) or short_followup


def _is_ai_failure_text(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    return (
        lowered.startswith("i cannot answer in chat right now")
        or lowered.startswith("i could not ground that answer")
    )


def _retrieval_topic(message: str) -> str | None:
    lowered = message.lower()
    if any(term in lowered for term in ("rain", "weather", "precip", "alert", "storm", "snow")):
        return "weather"
    if any(term in lowered for term in ("cover", "forecast", "demand", "busy", "slow", "usual")):
        return "forecast"
    if any(term in lowered for term in ("actual", "miss", "came in")):
        return "actuals"
    return None


def _resolve_context_service_date(
    message: str,
    current_digest: dict[str, Any],
    recent_turns: list[dict[str, str]],
) -> date | None:
    reference_date = _parse_date_value((current_digest or {}).get("reference_date"))
    lowered = message.lower()
    explicit = _first_date_in_text(message, reference_date=reference_date)
    if explicit is not None:
        return explicit
    if reference_date is not None:
        if any(token in lowered for token in ("tomorrow", "next night")):
            return reference_date + timedelta(days=1)
        if any(token in lowered for token in ("today", "tonight")):
            return reference_date
    for turn in reversed(recent_turns):
        content = str(turn.get("content") or "")
        parsed = _first_date_in_text(content, reference_date=reference_date)
        if parsed is not None:
            return parsed
        if reference_date is not None and "tomorrow" in content.lower():
            return reference_date + timedelta(days=1)
    headline = (current_digest or {}).get("headline_forecast")
    if isinstance(headline, dict):
        parsed = _parse_date_value(headline.get("service_date"))
        if parsed is not None:
            return parsed
    return reference_date


def _first_date_in_text(text: str, *, reference_date: date | None) -> date | None:
    iso_match = _ISO_DATE_RE.search(text)
    if iso_match:
        return _parse_date_value(iso_match.group(1))
    month_match = re.search(
        r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})\b",
        text,
        flags=re.IGNORECASE,
    )
    if month_match and reference_date is not None:
        month_names = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        month = month_names.get(month_match.group(1).lower()[:3])
        day = int(month_match.group(2))
        if month is not None:
            try:
                return date(reference_date.year, month, day)
            except ValueError:
                return None
    return None


def _parse_date_value(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _sanitize_tool_results_for_prompt(tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for result in tool_results:
        if not isinstance(result, dict):
            continue
        tool_name = str(result.get("tool") or "")
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        sanitized.append(
            {
                "tool": tool_name,
                "success": bool(result.get("success")),
                "message": str(result.get("message") or ""),
                "data": _sanitize_tool_data(tool_name, data),
            }
        )
    return sanitized


def _sanitize_tool_data(tool_name: str, data: dict[str, Any]) -> dict[str, Any]:
    if tool_name == "query_forecast_detail":
        return {
            "service_date": data.get("service_date"),
            "forecast_expected": data.get("forecast_expected"),
            "confidence_tier": data.get("confidence_tier"),
            "posture": data.get("posture"),
            "service_state": data.get("service_state"),
            "headline": data.get("headline"),
            "top_drivers": list(data.get("top_drivers") or [])[:6],
            "major_uncertainties": list(data.get("major_uncertainties") or [])[:4],
            "baseline": data.get("baseline"),
            "vs_usual_pct": data.get("vs_usual_pct"),
            "vs_usual_covers": data.get("vs_usual_covers"),
            "weather_pct": data.get("weather_pct"),
            "weather_context": data.get("weather_context") if isinstance(data.get("weather_context"), dict) else None,
            "context_pct": data.get("context_pct"),
            "seasonal_pct": data.get("seasonal_pct"),
        }
    if tool_name == "query_forecast_why":
        return {
            "service_date": data.get("service_date"),
            "service_window": data.get("service_window"),
            "forecast_expected": data.get("forecast_expected"),
            "scenario": data.get("scenario"),
            "headline": data.get("headline"),
            "baseline": data.get("baseline"),
            "vs_usual_pct": data.get("vs_usual_pct"),
            "vs_usual_covers": data.get("vs_usual_covers"),
            "confidence_tier": data.get("confidence_tier"),
            "service_state": data.get("service_state"),
            "top_drivers": list(data.get("top_drivers") or [])[:4],
            "component_effects": list(data.get("component_effects") or [])[:4],
            "weather_context": data.get("weather_context") if isinstance(data.get("weather_context"), dict) else None,
            "top_signals": list(data.get("top_signals") or [])[:3],
            "major_uncertainties": list(data.get("major_uncertainties") or [])[:3],
            "reference_status": data.get("reference_status"),
            "reference_model": data.get("reference_model"),
            "regime": data.get("regime"),
            "regime_progress": data.get("regime_progress"),
        }
    if tool_name == "query_recent_signals":
        return {
            "dependency_group": data.get("dependency_group"),
            "signals": [_compact_signal(signal) for signal in list(data.get("signals") or [])[:8] if isinstance(signal, dict)],
        }
    if tool_name == "query_service_weather":
        weather = data.get("weather") if isinstance(data.get("weather"), dict) else {}
        return {
            "service_date": data.get("service_date"),
            "service_window": data.get("service_window"),
            "weather": weather,
        }
    if tool_name == "query_forecast_card_context":
        return {
            "service_date": data.get("service_date"),
            "forecast_expected": data.get("forecast_expected"),
            "headline": data.get("headline"),
            "scenario": data.get("scenario"),
            "vs_usual_pct": data.get("vs_usual_pct"),
            "vs_usual_covers": data.get("vs_usual_covers"),
            "baseline": data.get("baseline"),
            "service_state": data.get("service_state"),
            "confidence_tier": data.get("confidence_tier"),
            "top_drivers": list(data.get("top_drivers") or [])[:4],
            "major_uncertainties": list(data.get("major_uncertainties") or [])[:4],
            "weather_context": data.get("weather_context") if isinstance(data.get("weather_context"), dict) else None,
            "weather_pct": data.get("weather_pct"),
            "context_pct": data.get("context_pct"),
            "seasonal_pct": data.get("seasonal_pct"),
        }
    if tool_name == "query_operator_attention":
        return {
            "service_date": data.get("service_date"),
            "pending_action": data.get("pending_action"),
            "current_uncertainty": data.get("current_uncertainty"),
            "active_signals_summary": list(data.get("active_signals_summary") or [])[:5],
            "disclaimers": list(data.get("disclaimers") or [])[:3],
            "service_plan": data.get("service_plan") if isinstance(data.get("service_plan"), dict) else None,
        }
    if tool_name == "query_recent_conversation_context":
        return {
            "topic": data.get("topic"),
            "turns": list(data.get("turns") or [])[:8],
        }
    return data


def _build_answer_packet(
    *,
    operator_message: str,
    current_digest: dict[str, Any],
    temporal_digest: dict[str, Any],
    tool_results: list[dict[str, Any]],
) -> dict[str, Any]:
    packet: dict[str, Any] = {
        "operator_message": operator_message,
        "current_context": _compact_current_digest(current_digest),
        "memory_context": _compact_temporal_digest(temporal_digest),
        "forecast_detail": None,
        "forecast_why": None,
        "card_context": None,
        "service_weather": None,
        "operator_attention": None,
        "conversation_context": [],
        "weather_signals": [],
        "tool_status": [],
    }
    for result in tool_results:
        tool_name = str(result.get("tool") or "")
        success = bool(result.get("success"))
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        packet["tool_status"].append(
            {
                "tool": tool_name,
                "success": success,
                "message": str(result.get("message") or ""),
            }
        )
        if not success:
            continue
        if tool_name == "query_forecast_detail":
            packet["forecast_detail"] = _forecast_answer_fact(data)
        elif tool_name == "query_forecast_why":
            packet["forecast_why"] = data
            packet["forecast_detail"] = _forecast_answer_fact(data)
        elif tool_name == "query_recent_signals":
            packet["weather_signals"] = list(data.get("signals") or [])
        elif tool_name == "query_service_weather":
            packet["service_weather"] = data
        elif tool_name == "query_forecast_card_context":
            packet["card_context"] = data
            if packet.get("forecast_detail") is None:
                packet["forecast_detail"] = _forecast_answer_fact(data)
        elif tool_name == "query_operator_attention":
            packet["operator_attention"] = data
        elif tool_name == "query_recent_conversation_context":
            packet["conversation_context"] = list(data.get("turns") or [])
    return packet


def _compact_current_digest(digest: dict[str, Any]) -> dict[str, Any]:
    headline = digest.get("headline_forecast") if isinstance(digest, dict) else None
    if isinstance(headline, dict):
        headline = {
            "service_date": headline.get("service_date"),
            "expected": headline.get("expected"),
            "confidence": headline.get("confidence"),
        }
    near_horizon: list[dict[str, Any]] = []
    for row in list((digest or {}).get("near_horizon") or [])[:5]:
        if not isinstance(row, dict):
            continue
        near_horizon.append(
            {
                "service_date": row.get("service_date") or row.get("date"),
                "expected": row.get("expected"),
                "state": row.get("state"),
            }
        )
    return {
        "reference_date": (digest or {}).get("reference_date"),
        "phase": (digest or {}).get("phase"),
        "headline_forecast": headline,
        "near_horizon": near_horizon,
        "pending_action": (digest or {}).get("pending_action"),
        "current_uncertainty": (digest or {}).get("current_uncertainty"),
        "active_signals_summary": list((digest or {}).get("active_signals_summary") or [])[:5],
        "disclaimers": list((digest or {}).get("disclaimers") or [])[:3],
    }


def _compact_temporal_digest(digest: dict[str, Any]) -> dict[str, Any]:
    return {
        "conversation_state": (digest or {}).get("conversation_state"),
        "recent_misses": list((digest or {}).get("recent_misses") or [])[:3],
        "recent_patterns": list((digest or {}).get("recent_patterns") or [])[:3],
        "operator_facts": list((digest or {}).get("operator_facts") or [])[:6],
        "active_hypotheses": list((digest or {}).get("active_hypotheses") or [])[:3],
        "open_questions": list((digest or {}).get("open_questions") or [])[:3],
        "disclaimers": list((digest or {}).get("disclaimers") or [])[:3],
    }


def _forecast_answer_fact(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "service_date": data.get("service_date"),
        "forecast_expected": data.get("forecast_expected"),
        "headline": data.get("headline"),
        "scenario": data.get("scenario") or _scenario_from_posture(data.get("posture"), data.get("service_state")),
        "vs_usual_pct": data.get("vs_usual_pct"),
        "vs_usual_covers": data.get("vs_usual_covers"),
        "confidence_tier": data.get("confidence_tier"),
        "service_state": data.get("service_state"),
        "top_drivers": _driver_labels(data.get("top_drivers") or []),
        "major_uncertainties": list(data.get("major_uncertainties") or [])[:4],
        "weather_pct": data.get("weather_pct"),
        "weather_context": data.get("weather_context") if isinstance(data.get("weather_context"), dict) else None,
        "context_pct": data.get("context_pct"),
        "seasonal_pct": data.get("seasonal_pct"),
    }


def _scenario_from_posture(posture: Any, service_state: Any) -> str:
    state = str(service_state or "").lower()
    if state and state not in {"normal", "normal_service"}:
        if "partial" in state:
            return "Partial"
        if "closed" in state:
            return "Closed"
        if "private" in state or "buyout" in state:
            return "Event"
        if "holiday" in state:
            return "Holiday"
        if "weather" in state or "disruption" in state:
            return "Slow"
    posture_text = str(posture or "").lower()
    if "elevated" in posture_text:
        return "Busy"
    if any(token in posture_text for token in ("soft", "disrupted", "cautious")):
        return "Slow"
    return "Steady"


def _driver_labels(raw_drivers: list[Any]) -> list[str]:
    labels: list[str] = []
    for raw_driver in raw_drivers:
        label = driver_label(str(raw_driver))
        if label and label not in labels:
            labels.append(label)
        if len(labels) >= 4:
            break
    return labels


def _compact_signal(signal: dict[str, Any]) -> dict[str, Any]:
    details = signal.get("details") if isinstance(signal.get("details"), dict) else {}
    if isinstance(details.get("details"), dict):
        details = details["details"]
    return {
        "signal_type": signal.get("signal_type"),
        "source_name": signal.get("source_name"),
        "dependency_group": signal.get("dependency_group"),
        "direction": signal.get("direction"),
        "strength": signal.get("strength"),
        "status": signal.get("status"),
        "event": details.get("event"),
        "headline": details.get("headline"),
        "severity": details.get("severity"),
        "start_time": signal.get("start_time") or details.get("onset") or details.get("effective"),
        "end_time": signal.get("end_time") or details.get("ends") or details.get("expires"),
    }


def _learning_resolution_text(
    *,
    recorded_prefix: str,
    semantic_payload: dict[str, Any] | None = None,
) -> str:
    payload = dict(semantic_payload or {})
    payload.setdefault("category", "learning_resolution")
    payload["what_is_true_now"] = recorded_prefix
    return render_communication_payload(
        communication_payload(**payload),
        include_question=False,
    )


# Unified agent service
# ---------------------------------------------------------------------------

class UnifiedAgentService:
    """Single agent for onboarding + operations conversations."""

    def __init__(
        self,
        db: Database,
        provider: AgentModelProvider | None = None,
        *,
        agent_dispatcher: AgentDispatcher | None = None,
    ) -> None:
        self.db = db
        self.provider = provider
        self.agent_dispatcher = agent_dispatcher
        self.executor = ToolExecutor(db, provider=provider, agent_dispatcher=agent_dispatcher)

    def respond(
        self,
        *,
        operator_id: str | None,
        message: str,
        conversation_history: list[dict[str, str]] | None = None,
        uploaded_file_data: dict[str, Any] | None = None,
        reference_date: date | None = None,
        learning_agenda_key: str | None = None,
    ) -> AgentResponse:
        """Process an operator message and return a response with suggested next steps."""
        effective_reference_date = _resolve_agent_reference_date(self.db, operator_id, reference_date)
        self.executor.set_reference_date(effective_reference_date)
        phase = detect_phase(self.db, operator_id)

        return self._respond_via_dispatcher(
            operator_id=operator_id,
            message=message,
            phase=phase,
            reference_date=effective_reference_date,
            learning_agenda_key=learning_agenda_key,
            uploaded_file_data=uploaded_file_data,
        )

    def _respond_via_dispatcher(
        self,
        *,
        operator_id: str | None,
        message: str,
        phase: str,
        reference_date: date,
        learning_agenda_key: str | None,
        uploaded_file_data: dict[str, Any] | None = None,
    ) -> AgentResponse:
        if phase == "operations":
            resolved_agenda_response = _maybe_resolve_learning_agenda_reply(
                self.db,
                self.executor,
                operator_id=operator_id,
                message=message,
                learning_agenda_key=learning_agenda_key,
            )
            if resolved_agenda_response is not None:
                self._persist_exchange(operator_id, message, resolved_agenda_response)
                if operator_id:
                    _detect_and_update_behavior(self.db, operator_id, message, resolved_agenda_response.text)
                return resolved_agenda_response

        if (
            self.agent_dispatcher is None
            or self.provider is None
            or not self.provider.is_available()
            or operator_id is None
        ):
            response = self._ai_unavailable_response(operator_id=operator_id, phase=phase)
            self._persist_exchange(operator_id, message, response)
            return response

        digest_repo = OperatorContextDigestRepository(self.db)
        if phase in {"setup", "enrichment"}:
            ensure_setup_context_digests(
                self.db,
                operator_id=operator_id,
                reference_date=reference_date,
            )
        current_row = digest_repo.fetch_latest(operator_id=operator_id, kind="current_state")
        temporal_row = digest_repo.fetch_latest(operator_id=operator_id, kind="temporal")
        if current_row is None or temporal_row is None:
            response = self._ai_unavailable_response(operator_id=operator_id, phase=phase)
            self._persist_exchange(operator_id, message, response)
            return response

        now = datetime.now(UTC)
        staleness = {
            "current_state_age_seconds": _age_seconds(current_row.get("produced_at"), now),
            "temporal_age_seconds": _age_seconds(temporal_row.get("produced_at"), now),
            "source_hash_match": True,
        }
        recent_turns = _format_recent_turns(self.db, operator_id)

        output, tool_results, final_operator_id = self._dispatch_orchestrator_turn(
            operator_id=operator_id,
            operator_message=message,
            current_digest=current_row.get("payload") or {},
            temporal_digest=temporal_row.get("payload") or {},
            staleness=staleness,
            recent_turns=recent_turns,
            uploaded_file_data=uploaded_file_data,
        )
        response_phase = detect_phase(self.db, final_operator_id) if final_operator_id else phase

        text = str(output.get("text") or "").strip()
        if not text:
            text = _CHAT_AI_UNAVAILABLE_TEXT

        note_captured = any(tr.tool_name == "capture_note" and tr.success for tr in tool_results)
        text = _post_generation_guard(text)
        if not text:
            text = _CHAT_AI_UNAVAILABLE_TEXT

        response = AgentResponse(
            text=text,
            tool_results=tool_results,
            suggested_messages=[],
            operator_id=final_operator_id,
            phase=response_phase,
        )

        self._persist_exchange(final_operator_id, message, response)
        if final_operator_id:
            _detect_and_update_behavior(self.db, final_operator_id, message, response.text)

        if note_captured:
            from stormready_v3.workflows.retriever_hooks import run_retriever_hooks

            try:
                run_retriever_hooks(
                    db=self.db,
                    dispatcher=self.agent_dispatcher,
                    operator_id=operator_id,
                    reference_date=reference_date,
                    kinds=("temporal",),
                )
            except Exception:
                pass
        if response_phase in {"setup", "enrichment"} and tool_results:
            ensure_setup_context_digests(
                self.db,
                operator_id=final_operator_id,
                reference_date=reference_date,
                force=True,
            )

        return response

    def _dispatch_orchestrator_turn(
        self,
        *,
        operator_id: str,
        operator_message: str,
        current_digest: dict[str, Any],
        temporal_digest: dict[str, Any],
        staleness: dict[str, Any],
        recent_turns: list[dict[str, str]],
        uploaded_file_data: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], list[ToolResult], str]:
        assert self.agent_dispatcher is not None  # guarded by caller
        base_payload: dict[str, Any] = {
            "operator_message": operator_message,
            "current_state_digest": current_digest,
            "temporal_digest": temporal_digest,
            "digest_staleness": staleness,
            "recent_turns": recent_turns,
            "prior_turn_facts": _extract_prior_turn_facts(recent_turns),
            "tool_results": [],
        }
        if uploaded_file_data:
            base_payload["uploaded_file"] = {
                "headers": uploaded_file_data.get("headers", []),
                "sample_rows": list(uploaded_file_data.get("sample_rows", []))[:5],
            }
        preloaded_tool_results: list[ToolResult] = []
        preloaded_serialized_results: list[dict[str, Any]] = []
        if _should_preload_forecast_why(
            operator_message=operator_message,
            current_digest=current_digest,
            recent_turns=recent_turns,
        ):
            preloaded_tool_results, preloaded_serialized_results = self._retrieve_context_for_turn(
                operator_id=operator_id,
                operator_message=operator_message,
                current_digest=current_digest,
                recent_turns=recent_turns,
            )
            if preloaded_serialized_results:
                prompt_tool_results = _sanitize_tool_results_for_prompt(preloaded_serialized_results)
                base_payload["tool_results"] = prompt_tool_results
                base_payload["answer_packet"] = _build_answer_packet(
                    operator_message=operator_message,
                    current_digest=current_digest,
                    temporal_digest=temporal_digest,
                    tool_results=prompt_tool_results,
                )
        ctx = AgentContext(
            role=AgentRole.CONVERSATION_ORCHESTRATOR,
            operator_id=operator_id,
            run_id=str(uuid.uuid4()),
            triggered_at=datetime.now(UTC),
            payload=base_payload,
        )
        result = self.agent_dispatcher.dispatch(ctx)
        output: dict[str, Any] = {}
        if result.outputs:
            first = result.outputs[0]
            if isinstance(first, dict):
                output = dict(first)

        if preloaded_tool_results and _is_ai_failure_text(str(output.get("text") or "")):
            retry_output = self._dispatch_orchestrator_with_tool_results(
                operator_id=operator_id,
                operator_message=operator_message,
                current_digest=current_digest,
                temporal_digest=temporal_digest,
                staleness=staleness,
                recent_turns=recent_turns,
                serialized_results=preloaded_serialized_results,
            )
            if retry_output:
                return retry_output, preloaded_tool_results, operator_id

        if _should_auto_retrieve_context(
            operator_message=operator_message,
            output=output,
            current_digest=current_digest,
            recent_turns=recent_turns,
        ) and not preloaded_tool_results:
            auto_tool_results, auto_serialized_results = self._retrieve_context_for_turn(
                operator_id=operator_id,
                operator_message=operator_message,
                current_digest=current_digest,
                recent_turns=recent_turns,
            )
            if auto_tool_results:
                auto_output = self._dispatch_orchestrator_with_tool_results(
                    operator_id=operator_id,
                    operator_message=operator_message,
                    current_digest=current_digest,
                    temporal_digest=temporal_digest,
                    staleness=staleness,
                    recent_turns=recent_turns,
                    serialized_results=auto_serialized_results,
                )
                if auto_output:
                    return auto_output, auto_tool_results, operator_id

        tool_calls = output.get("tool_calls") or []
        if not isinstance(tool_calls, list) or not tool_calls:
            return output, preloaded_tool_results, operator_id

        tool_results: list[ToolResult] = list(preloaded_tool_results)
        serialized_results: list[dict[str, Any]] = list(preloaded_serialized_results)
        executed_tool = False
        current_operator_id = operator_id
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            name = call.get("name") or call.get("tool_name")
            if not isinstance(name, str) or not name:
                continue
            args = call.get("arguments") or {}
            if not isinstance(args, dict):
                args = {}
            tr = self.executor.execute(current_operator_id, name, args)
            executed_tool = True
            tool_results.append(tr)
            if name == "update_profile" and tr.success and tr.data.get("operator_id"):
                current_operator_id = str(tr.data["operator_id"])
            serialized_results.append(
                {
                    "tool": tr.tool_name,
                    "success": tr.success,
                    "message": tr.message,
                    "data": tr.data if isinstance(tr.data, dict) else {},
                }
            )
            if name == "update_profile" and tr.success:
                readiness = self.executor.execute(current_operator_id, "check_readiness", {})
                tool_results.append(readiness)
                serialized_results.append(
                    {
                        "tool": readiness.tool_name,
                        "success": readiness.success,
                        "message": readiness.message,
                        "data": readiness.data if isinstance(readiness.data, dict) else {},
                    }
                )

        if not executed_tool:
            return output, preloaded_tool_results, current_operator_id

        output2 = self._dispatch_orchestrator_with_tool_results(
            operator_id=current_operator_id,
            operator_message=operator_message,
            current_digest=current_digest,
            temporal_digest=temporal_digest,
            staleness=staleness,
            recent_turns=recent_turns,
            serialized_results=serialized_results,
        )
        if output2:
            return output2, tool_results, current_operator_id
        return output, tool_results, current_operator_id

    def _dispatch_orchestrator_with_tool_results(
        self,
        *,
        operator_id: str,
        operator_message: str,
        current_digest: dict[str, Any],
        temporal_digest: dict[str, Any],
        staleness: dict[str, Any],
        recent_turns: list[dict[str, str]],
        serialized_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        prompt_tool_results = _sanitize_tool_results_for_prompt(serialized_results)
        followup_payload: dict[str, Any] = {
            "operator_message": operator_message,
            "current_state_digest": current_digest,
            "temporal_digest": temporal_digest,
            "digest_staleness": staleness,
            "recent_turns": recent_turns,
            "prior_turn_facts": _extract_prior_turn_facts(recent_turns),
            "tool_results": prompt_tool_results,
            "answer_packet": _build_answer_packet(
                operator_message=operator_message,
                current_digest=current_digest,
                temporal_digest=temporal_digest,
                tool_results=prompt_tool_results,
            ),
        }
        ctx = AgentContext(
            role=AgentRole.CONVERSATION_ORCHESTRATOR,
            operator_id=operator_id,
            run_id=str(uuid.uuid4()),
            triggered_at=datetime.now(UTC),
            payload=followup_payload,
        )
        result = self.agent_dispatcher.dispatch(ctx)
        if result.outputs:
            first = result.outputs[0]
            if isinstance(first, dict):
                return dict(first)
        return {}

    def _retrieve_context_for_turn(
        self,
        *,
        operator_id: str,
        operator_message: str,
        current_digest: dict[str, Any],
        recent_turns: list[dict[str, str]],
    ) -> tuple[list[ToolResult], list[dict[str, Any]]]:
        service_date = _resolve_context_service_date(operator_message, current_digest, recent_turns)
        if service_date is None:
            return [], []
        calls: list[tuple[str, dict[str, Any]]] = [
            ("query_forecast_why", {"service_date": service_date.isoformat()}),
        ]
        tool_results: list[ToolResult] = []
        serialized_results: list[dict[str, Any]] = []
        for tool_name, args in calls:
            result = self.executor.execute(operator_id, tool_name, args)
            tool_results.append(result)
            serialized_results.append(
                {
                    "tool": result.tool_name,
                    "success": result.success,
                    "message": result.message,
                    "data": result.data if isinstance(result.data, dict) else {},
                }
            )
        return tool_results, serialized_results

    @staticmethod
    def _ai_unavailable_response(*, operator_id: str | None, phase: str) -> AgentResponse:
        text = _CHAT_AI_UNAVAILABLE_TEXT
        return AgentResponse(
            text=text,
            operator_id=operator_id,
            phase=phase,
            suggested_messages=[],
        )

    def _persist_exchange(self, operator_id: str | None, message: str, response: AgentResponse) -> None:
        """Save both the operator message and assistant response to DuckDB."""
        if not operator_id:
            return
        _save_message(self.db, operator_id, "operator", message, response.phase)
        tool_calls = [{"tool": r.tool_name, "args": {}} for r in response.tool_results] if response.tool_results else None
        _save_message(self.db, operator_id, "assistant", response.text, response.phase,
                      tool_calls=tool_calls, tool_results=response.tool_results)
