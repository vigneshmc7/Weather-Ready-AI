# 03 - API Layer

The API layer is FastAPI. `api/app.py` defines HTTP routes and request models.
`api/service.py` contains most application service logic and serializers used by
the frontend.

## Key Files

- `api/app.py` - FastAPI app, lifespan, request models, route declarations, SPA mount
- `api/service.py` - workspace builder, onboarding, chat, actuals, service plan, refresh
- `api/serializers.py` - shared serialization helpers/models used by API code
- `storage/db.py` - opens DuckDB and applies migrations
- `agents/factory.py` - builds the dispatcher used by chat/refresh/note workflows

## Lifespan

On startup, `api/app.py::_lifespan`:

1. Builds an `AgentModelProvider` with `ai/factory.py::build_agent_model_provider`.
2. Opens and initializes a `Database`.
3. Builds the dispatcher with `agents/factory.py::build_agent_dispatcher`.
4. Stores the dispatcher at `app.state.agent_dispatcher`.
5. Starts the optional background supervisor loop when enabled.

Each request also receives its own `Database` instance through `_db_dependency`.

## Routes

| Route | Service function | Purpose |
|---|---|---|
| `GET /api/health` | inline | Liveness |
| `GET /api/bootstrap` | `bootstrap_state` | Initial operators/onboarding options |
| `GET /api/operators/{id}/workspace` | `build_workspace` | Full workspace payload |
| `POST /api/onboarding/complete` | `complete_onboarding` | Create/update operator setup |
| `POST /api/onboarding/review-history-upload` | `review_historical_upload` | Validate uploaded history |
| `POST /api/operators/{id}/chat` | `post_chat_message` | Chat turn and workspace refresh |
| `GET /api/operators/{id}/chat-history` | `get_chat_history` | Paginated messages |
| `POST /api/operators/{id}/actuals` | `submit_actual_entry` | Log actual covers and learning updates |
| `POST /api/operators/{id}/service-plan` | `submit_service_plan` | Save service-state/plan context |
| `POST /api/operators/{id}/refresh` | `request_refresh_now` | Operator-requested refresh |
| `POST /api/operators/{id}/setup-bootstrap` | `start_setup_bootstrap_now` | Force setup bootstrap |
| `DELETE /api/operators/{id}` | `delete_operator_profile` | Remove operator data |
| `GET /{full_path:path}` | inline | Serve built SPA |

## Common Response Pattern

Most mutations return:

- `result` or operation-specific metadata
- `workspace` from `build_workspace(...)`

This keeps the frontend state synchronized without requiring it to infer which
tables changed.

## Main API Flows

### Onboarding

`complete_onboarding` validates profile fields and four dinner baseline groups,
saves the operator through `ToolExecutor.update_profile`, saves location
relevance, writes setup digests, and may start a setup bootstrap job.

### Chat

`post_chat_message` builds `UnifiedAgentService`, runs `respond`, persists the
conversation exchange, executes any tool calls, and returns the refreshed
workspace. In operations, date-specific why/weather questions can preload a
small `forecast_why` answer packet from the published forecast and engine digest
before the Conversation Orchestrator composes the reply.

### Actuals

`submit_actual_entry` validates totals, writes actuals, evaluates the latest
published forecast, updates learning streams, captures an optional note, and
triggers retriever hooks when a dispatcher is available.

### Refresh

`request_refresh_now` queues an operator refresh through `SupervisorService` and
returns the refreshed workspace.

See also: [04_agents.md](04_agents.md), [05_orchestration.md](05_orchestration.md),
[06_data_layer.md](06_data_layer.md).
