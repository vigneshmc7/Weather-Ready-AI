# 04 - Agents

Agents are dispatcher-managed helpers around structured model calls. They do not
own the deterministic forecast math or database transaction boundaries. Callers
decide which agent to run and which outputs to persist.

## Core Files

- `agents/base.py` - `AgentRole`, `AgentContext`, `AgentResult`, `AgentDispatcher`
- `agents/factory.py` - constructs all seven agents
- `agents/policy_loader.py` - loads `agents/policies/*.md`
- `ai/contracts.py` - provider protocol
- `ai/factory.py` - returns configured provider or unavailable sentinel
- `agents/tools.py` - deterministic tool execution for chat/setup operations

## Dispatcher Rules

`AgentDispatcher.dispatch(ctx)`:

- looks up the agent by `ctx.role`
- blocks if the provider is unavailable
- catches agent exceptions and returns a failed result
- logs every invocation to `agent_run_log`

Policy files define prompts, allowed writes, output schema guidance, and banned
terms. Write enforcement is still performed by the calling workflow.

## Seven Roles

| Role | File | Runtime use |
|---|---|---|
| `signal_interpreter` | `agents/signal_interpreter.py` | Converts narrative external payloads into typed demand signals |
| `prediction_governor` | `agents/prediction_governor.py` | Adds driver emphasis and operator-facing uncertainty metadata |
| `current_state_retriever` | `agents/current_state_retriever.py` | Produces compact current forecast/workspace digest |
| `temporal_memory_retriever` | `agents/temporal_memory_retriever.py` | Produces history, facts, hypotheses, and learning digest |
| `conversation_orchestrator` | `agents/conversation_orchestrator.py` | Produces grounded chat response and tool calls |
| `conversation_note_extractor` | `agents/conversation_note_extractor.py` | Extracts structured service context from notes |
| `anomaly_explainer` | `agents/anomaly_explainer.py` | Proposes hypotheses after large normal-service misses |

## Chat Path

1. `agents/unified.py::UnifiedAgentService.respond` resolves phase and reference date.
2. Operations chat checks learning-agenda replies first.
3. For model-backed chat, it loads latest `current_state` and `temporal` digests.
4. For date-specific why/weather/follow-up questions, it can preload one compact
   `query_forecast_why` packet into `answer_packet.forecast_why`.
5. It dispatches `conversation_orchestrator`.
6. Tool calls are run by `agents/tools.py::ToolExecutor`.
7. `capture_note` routes to `conversation/notes.py::ConversationNoteService`, which may dispatch `conversation_note_extractor`.
8. Note capture can trigger a temporal retriever hook.

## Refresh Path

`orchestration/orchestrator.py` uses agents in this order when configured:

1. `signal_interpreter` after source fetch and before normalization.
2. `prediction_governor` after deterministic candidate creation and before publish.
3. `current_state_retriever` after refresh completion.

Actual submission can trigger:

1. `anomaly_explainer` after evaluation and learning updates.
2. `conversation_note_extractor` if an actual note was provided.
3. `current_state_retriever` and `temporal_memory_retriever`.

## Tool Surface

`ToolExecutor` handles deterministic actions and reads:

- setup: `update_profile`, `set_location_relevance`, `set_location_profile_hints`, `check_readiness`
- forecast reads: `get_forecast`, `explain_forecast`, `query_forecast_detail`, `query_forecast_why`
- learning reads: `query_learning_state`, `query_hypothesis_backlog`, `query_actuals_history`, `query_recent_signals`
- operations writes: `capture_note`, `request_refresh`
- upload helper: `interpret_upload`

`query_forecast_why` is the preferred chat context packet for operator questions
like "why is May 6 at 213?", "why is tomorrow low?", and short follow-ups. It
combines the date's expected covers, baseline comparison, main component
effects, weather context, top signals, and uncertainties without exposing
planning ranges.

## AI Calls Outside Dispatcher

These use the same provider contract but are not dispatcher roles:

- `external_intelligence/location_profiler.py::LocationProfiler`
- `external_intelligence/catalog.py::ExternalSourceCatalogService._provider_governance`
- `imports/history_upload.py::_ai_review_summary`

See also: [03_api_layer.md](03_api_layer.md), [05_orchestration.md](05_orchestration.md),
[06_data_layer.md](06_data_layer.md).
