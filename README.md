# pei-nwdaf-data-processor

Consumes raw NEF records from Kafka, aggregates them into tumbling time windows, and publishes windowed stats to Kafka.

## How it works

1. Consumes records from `network.data.ingested`
2. Groups records by `(snssai_sst, snssai_sd, dnn, event)` and accumulates them in time windows
3. On each watermark tick, closes completed windows and computes `min`, `max`, `mean`, `std`, `count` per metric
4. Applies policy transformations (filtering, hashing, redaction) if enabled
5. Publishes completed windows to `network.data.processed`

## Running

```bash
cp .env.example .env
docker compose up -d
```

## Output format

```json
{
  "tags": {"snssai_sst": "1", "snssai_sd": "000001", "dnn": "internet", "event": "PERF_DATA"},
  "window_start": 1700000000,
  "window_end": 1700000060,
  "window_duration_seconds": 60,
  "sample_count": 12,
  "metrics": {
    "thrputUl_mbps": {"mean": 10.5, "min": 5.0, "max": 20.0, "std": 3.2, "count": 12}
  }
}
```

## Environment

| Variable | Default | Description |
|---|---|---|
| `KAFKA_HOST` | `kafka` | Kafka broker host |
| `KAFKA_PORT` | `9092` | Kafka broker port |
| `WINDOW_DURATION` | `60` | Window size in seconds |
| `SLIDE_INTERVAL` | `WINDOW_DURATION` | Watermark advance interval in seconds |
| `ALLOWED_LATENESS` | `10` | Grace period before first tick in seconds |
| `EMPTY_WINDOW_STRATEGY` | `SKIP` | `SKIP` or `ZERO_FILL` |
| `GROUP_TTL_SECONDS` | unset | Prune inactive groups after N seconds of silence |
| `START_TIME` | unset | Unix timestamp for historical replay |
| `POLICY_SERVICE_URL` | `http://policy-service:8788` | Policy service URL |
| `POLICY_ENABLED` | `false` | Enable policy enforcement |
