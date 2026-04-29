# StormReady V3 Knowledge Base

This folder is the short-form map of the application. It is intended to give a
Codex agent enough context to navigate the project without re-reading the whole
codebase first.

Read in this order:

1. [01_system_overview.md](01_system_overview.md) - runtime shape, core loop, agent roles
2. [02_frontend.md](02_frontend.md) - React workspace and API contracts
3. [03_api_layer.md](03_api_layer.md) - FastAPI routes and service entry points
4. [04_agents.md](04_agents.md) - dispatcher, seven roles, tool execution
5. [05_orchestration.md](05_orchestration.md) - refresh, prediction, publish, learning triggers
6. [06_data_layer.md](06_data_layer.md) - DuckDB tables, repositories, migrations
7. [07_external_world.md](07_external_world.md) - sources, connectors, AI provider, runtime modes

## Reference Rules

- Paths without a prefix are relative to `src/stormready_v3/`.
- Paths beginning with `frontend/`, `db/`, `tests/`, or `scripts/` are repo-root paths.
- "Dispatcher" means `agents/base.py::AgentDispatcher`.
- "Digest" means a row in `operator_context_digest`; current kinds are `current_state`, `temporal`, and setup/enrichment digests.
- "Operator" means the restaurant account being forecasted.
- The primary runtime service window is dinner. MVP scope helpers live in `mvp_scope.py`.

## Current Runtime Shape

- Backend: FastAPI monolith plus optional background supervisor thread.
- Frontend: React/Vite SPA served separately in dev and mounted from `frontend/dist` in production.
- Data: embedded DuckDB at `runtime_data/local/stormready_v3.duckdb` by default.
- Intelligence: deterministic prediction engine plus seven dispatcher-managed agent roles.
- External inputs: weather, local context sources, optional connector truth, history uploads, and operator-entered plans/actuals/notes.
