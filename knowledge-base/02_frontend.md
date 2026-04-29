# 02 - Frontend

The frontend is a React 18 + TypeScript SPA built with Vite. It is a single
workspace view with conditional panels rather than route-based pages.

## Key Files

- `frontend/src/App.tsx` - application state, onboarding, workspace panels, chat, forms
- `frontend/src/api.ts` - typed fetch helpers for `/api/*`
- `frontend/src/types.ts` - frontend contracts mirroring backend payloads
- `frontend/src/components/WeatherGlyph.tsx` - weather/risk glyph rendering
- `frontend/src/components/StatusPip.tsx` and `GlyphLegend.tsx` - small UI primitives
- `frontend/src/styles.css` - app styling
- `frontend/tests/*.spec.ts` - Playwright smoke/inspection tests
- `frontend/src/App.test.tsx` - Vitest unit coverage

## Workspace Shape

The UI starts from `GET /api/bootstrap`, then loads a selected operator with
`GET /api/operators/{operator_id}/workspace`.

Workspace mode is either:

- `onboarding` - setup wizard, upload review, bootstrap status
- `operations` - dashboard, forecast cards, plan/actual forms, chat, learning prompts

The operations dashboard reads:

- `dashboard.cards` - 14-day actionable forecast cards
- `dashboard.latestRefresh` - last completed refresh summary
- `dashboard.learningAgenda` - open operator questions/reminders
- `dashboard.missingActuals` - recent dinners that need actuals
- `dashboard.servicePlanWindow` - near-term planning needs
- `dashboard.operatorAttentionSummary` - compact queue summary

## Mutations

All mutations call `frontend/src/api.ts` and expect a refreshed workspace in the
response:

- `sendChatMessage`
- `submitActuals`
- `submitServicePlan`
- `requestRefresh`
- `completeOnboarding`
- `reviewHistoryUpload`
- `startSetupBootstrap`
- `deleteOperator`

## UI Contracts To Keep Aligned

- `ForecastCard` in `frontend/src/types.ts` should match `_serialize_card` in `api/service.py`.
- `LearningAgendaItem` should match `_serialize_learning_agenda_item`.
- `ActualEntryDraft` should match `ActualEntryRequest` in `api/app.py`.
- `ServicePlanDraft` should match `ServicePlanRequest`.
- Chat response shape in `frontend/src/api.ts` should match `api/service.py::post_chat_message`.

See also: [03_api_layer.md](03_api_layer.md), [05_orchestration.md](05_orchestration.md).
