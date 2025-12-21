# pei-nwdaf-processor

> Project for PEI evaluation 25/26

## Overview

Data processing service that aggregates and processes network telemetry measurements in time-aligned windows. Consumes raw network data from Kafka, groups measurements by cell and time window, applies statistical processing, and publishes aggregated results back to Kafka.

## Technologies

- **Python** 3.13 with async/await patterns
- **Apache Kafka** (confluent_kafka client) - Message streaming
- **FastAPI/Uvicorn** ecosystem
- **httpx** - Async HTTP requests
- **Docker** - Containerization
- **pytest** with asyncio support

## Key Features

- **Time-windowed processing**: Event-time aligned windows (configurable: 60s, 300s)
- **Watermark-driven lifecycle**: Manages window completion with configurable allowed lateness
- **Processing profiles**: Extensible ProfileBase classes
  - **LatencyProfile**: Aggregates RSRP, SINR, RSRQ, mean_latency, CQI with statistics (min, max, mean, std dev)
- **Empty window strategies**: Pluggable StrategyBase patterns
  - **SkipStrategy**: Ignore empty windows
  - **ZeroFillStrategy**: Generate zero/null-filled records
  - **ForwardFillStrategy**: Replicate last known values
- **Parallel processing**: Async cell data fetching and windowing
- **Integration**: Storage API for cell metadata, Kafka for input/output

## How to Test

### 1. Launch Kafka via Docker

```bash
docker run -p 9092:9092 apache/kafka:4.1.1
```

### 2. Create Required Topics

```bash
utils/topic.sh [container] "network.data.ingested" -c
utils/topic.sh [container] "network.data.processed" -c
```

### 3. Start the FastAPI Server (Ingestion Component)

```bash
uvicorn receiver:app --reload --host 0.0.0.0 --port 8000
```

### 4. Run the Processor Component

```bash
python3 producer/main.py -a "http://localhost:8000/receive" -f dataset/hbahn/latency_data.csv
```

## Docker Deployment

```bash
docker-compose up
```

Runs two processor instances for different time scales (60s and 300s windows).

## Architecture

- **Modular design**: Extensible ProfileBase and StrategyBase classes
- **Kafka topics**: Consumes from `network.data.ingested`, produces to `network.data.processed`
- **Cell-level aggregation**: Validates cell consistency within windows
- **Batch pagination**: Supports large result sets

## Directory Structure

```
processor/
├── main.py                      # Entry point - Kafka consumer/watermark coordination
├── src/
│   ├── time_window_manager.py  # Core windowing logic
│   ├── empty_window_strategy.py # Empty window handling
│   └── profiles/
│       ├── processing_profile.py # Abstract profile interface
│       └── latency_profile.py    # Network latency aggregation
└── tests/                       # pytest test suite
```
