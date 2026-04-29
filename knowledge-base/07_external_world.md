# 07 - External World

External-world code fetches weather, local context, connector truth, model
responses, and uploaded historical references. Runtime behavior is controlled by
environment variables in `config/settings.py`.

## Source Registry

`sources/factory.py::build_source_registry` chooses source adapters by
`STORMREADY_V3_SOURCE_MODE`:

- `live` - Open-Meteo, NWS alerts, live local-context adapters
- `hybrid` - live adapters plus mock transit/curated/broad/narrative sources
- `detailed_mock` - detailed offline weather and local-context mocks
- `mock` - simple mock weather source

Source payload contracts live in `sources/contracts.py`. Normalization into
`NormalizedSignal` happens in `sources/normalization.py`.

## Weather

- `sources/open_meteo.py` - forecast weather
- `sources/nws.py` - active weather alerts
- `sources/weather_archive.py` - multi-year weather baseline and Brooklyn/reference comparison
- `prediction/weather_effect_mapping.py` - weather effect mapping
- `prediction/weather_assessment.py` - weather assessment summary

Weather can affect expected covers, interval width, confidence, scenarios, and
forecast-card weather display.

## Local Context

- `sources/local_context_live.py` - live GBFS/bikeshare and local context adapters
- `sources/mock.py` - transit, curated local, broad proxy, and narrative mock sources
- `external_intelligence/catalog.py` - source catalog discovery and governance
- `external_intelligence/location_profiler.py` - onboarding location profiling
- `prediction/context_effect_mapping.py` - deterministic context effect mapping

Local context can enter the forecast as normalized signals, source coverage,
confidence/posture evidence, and operator-facing drivers.

## Connectors

Connector code is separate from source adapters:

- `connectors/contracts.py` - connector truth contract
- `connectors/factory.py` - registry by `STORMREADY_V3_CONNECTOR_MODE`
- `connectors/snapshot.py` - local snapshot connector mode
- `connectors/live.py` - live connector shell
- `connectors/mapping.py` - canonical field mapping
- `connectors/readiness.py` - readiness checks

Connector truth is used to resolve target/source quality for POS, reservations,
outside covers, and related actual fields when available.

## AI Provider

- `ai/contracts.py::AgentModelProvider` - provider protocol
- `ai/openai_provider.py` - OpenAI/Azure wrapper
- `ai/factory.py::build_agent_model_provider` - returns configured provider or unavailable sentinel

Relevant settings:

- `STORMREADY_V3_AGENT_MODEL_PROVIDER` - `auto`, `azure`, or `openai`
- `STORMREADY_V3_OPENAI_MODEL` / `OPENAI_MODEL`
- `OPENAI_API_KEY` or `STORMREADY_V3_OPENAI_API_KEY`
- `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_VERSION`, `AZURE_OPENAI_DEPLOYMENT`
- AI timeout and retry settings in `config/settings.py`

## Horizon Settings

Defined in `config/settings.py`:

- `ACTIONABLE_HORIZON_DAYS = 14`
- `WORKING_HORIZON_DAYS = 21`
- `NOTIFICATION_HORIZON_DAYS = 3`
- `SCHEDULED_REFRESH_WINDOWS = ("morning", "midday", "pre_dinner")`

See also: [04_agents.md](04_agents.md), [05_orchestration.md](05_orchestration.md),
[06_data_layer.md](06_data_layer.md).
