# pei-nwdaf-data-processor

Data processing service for 5G network telemetry - aggregates raw measurements into time-aligned sliding windows and publishes them to Kafka.

## Technologies

- **Python** 3.13
- **Apache Kafka** (`confluent_kafka`) - message streaming
- **httpx** - async HTTP calls to the Storage API

## How It Works

1. A watermark task ticks every `SLIDE_INTERVAL` seconds, advancing event time
2. On each tick, `TimeWindowManager` fetches the list of known cells from the Storage API, then fetches the new data slice for each cell in parallel
3. Each cell's data is appended to an in-memory `CellWindowBuffer`; records older than `window_start` are evicted
4. `MetricProfile` aggregates the buffered window - once grouped by `cell_index`, once grouped by `(cell_index, ip_src)` - computing `min`, `max`, `mean`, `std`, and `samples` for every numeric field
5. Empty windows (no measurements in the buffer) are passed to the configured `EmptyWindowStrategy`
6. Completed window records are published to `network.data.processed` via Kafka, optionally filtered by the Policy Service

Two processor instances run in parallel via `docker-compose`: `data-processor-60s` (60 s window, 20 s lateness) and `data-processor-300s` (300 s window, 30 s lateness).

## Integrations

| Service | Role |
|---|---|
| **Storage API** | Source of raw measurements and cell list |
| **Kafka** | Input topic `network.data.ingested` (metadata only); output topic `network.data.processed` |
| **Policy Service** | Optional per-record filtering before Kafka publish |

### Storage API contract

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/cell` | Returns `[cell_index, ...]` |
| `GET /api/v1/raw?cell_index=&start_time=&end_time=&batch_number=` | Returns `{ "data": [...], "has_next": bool }` |

## Empty Window Strategies

| Strategy | Behaviour |
|---|---|
| `SKIP` | Emit nothing |
| `ZERO_FILL` | Emit a record with all metric stats set to `null` |
| `FORWARD_FILL` | Copy the last non-empty window's values |
| `KNN` | Impute from temporally similar historical windows (hour + weekday/weekend distance); falls back to `FORWARD_FILL` then `ZERO_FILL` |

## Output Record Schema

```json
{
  "cell_index": 1,
  "type": "metric",
  "sample_count": 42,
  "window_start": 1700000000,
  "window_end": 1700000060,
  "window_duration_seconds": 60,
  "network": "...",
  "physical_cellid": "...",
  "<metric>": { "min": 0.0, "max": 1.0, "mean": 0.5, "std": 0.2, "samples": 42 }
}
```

Records covering a `(cell_index, ip_src)` grouping also include `ip_src`.
Empty-window records carry `"is_empty_window": true` and a fill-method flag (`"forward_filled"`, `"knn_filled"`, or `"knn_fallback"`).

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DATA_STORAGE_API_URL` | *(required)* | Base URL of the Storage API |
| `KAFKA_HOST` | `kafka` | Kafka broker hostname |
| `KAFKA_PORT` | `9092` | Kafka broker port |
| `WINDOW_DURATION` | `60` | Window size in seconds |
| `SLIDE_INTERVAL` | `WINDOW_DURATION` | How often to advance the watermark (seconds) |
| `ALLOWED_LATENESS` | `10` | Grace period before the first watermark tick (seconds) |
| `EMPTY_WINDOW_STRATEGY` | `ZERO_FILL` | `SKIP`, `ZERO_FILL`, `FORWARD_FILL`, or `KNN` |
| `KNN_K_NEIGHBORS` | `5` | Neighbours to use for KNN imputation |
| `KNN_MAX_HISTORY_HOURS` | `168` | How far back KNN searches (default: 1 week) |
| `START_TIME` | *(unset)* | Unix timestamp - if set, replays history from that point before going live |
| `POLICY_SERVICE_URL` | `http://policy-service:8788` | Policy enforcement service URL |
| `POLICY_ENABLED` | `false` | Enable policy-based data filtering |
| `POLICY_FAILOPEN` | `true` | Allow data through if the Policy Service is unreachable |

## Running

```bash
cp .env.example .env
docker-compose up --build
```
