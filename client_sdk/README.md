(Note: This guide was generated using AI)

# Policy Client SDK

Client SDK for components to interact with the Policy Enforcement Service.

## Installation

```bash
# in requirements.txt
git+https://github.com/ATNoG/pei-nwdaf-policy.git#subdirectory=client_sdk
```

## Quick Start

### Basic Usage

```python
from policy_client import PolicyClient

# Initialize the client
client = PolicyClient(
    service_url="http://policy-service:8000",
    component_id="my-component-1",
    enable_policy=True  # Set to False for policyless mode
)

# Check if access is permitted
allowed = await client.check_access(
    source_id="source-component",
    sink_id="my-component-1",
    resource="data",
    action="read"
)

# Process data (check + transform in one call)
result = await client.process_data(
    source_id="source-component",
    sink_id="my-component-1",
    data={"user_id": 123, "email": "user@example.com"}
)

if result.allowed:
    print(f"Transformed data: {result.data}")
    print(f"Transformations: {result.transformations}")
```

### Kafka Integration

```python
from policy_client.kafka_interceptor import create_policy_consumer

# Create a policy-enabled Kafka consumer
consumer = create_policy_consumer(
    bootstrap_servers="kafka:9092",
    group_id="my-consumer-group",
    topics=["network.data.ingested"],
    policy_service_url="http://policy-service:8000",
    component_id="data-processor-60s",
    source_component="ingestion-service",
    enable_policy=True  # Set to False for policyless mode
)

# Use like a regular Kafka consumer
for message in consumer:
    data = json.loads(message.value())
    # Message already filtered/transformed by policy
    process(data)
```

### FastAPI Middleware

```python
from policy_client.middleware import PolicyMiddleware
from fastapi import FastAPI

app = FastAPI()

# Add policy middleware
app.add_middleware(
    PolicyMiddleware,
    policy_service_url="http://policy-service:8000",
    component_id="ml-service",
    enable_policy=True
)

@app.get("/api/models/{model_id}")
async def get_model(model_id: str):
    # Middleware checks access automatically
    return {"model_id": model_id, "data": "..."}
```

## Configuration

The client can be configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `POLICY_SERVICE_URL` | URL of Policy Service | `http://localhost:8000` |
| `POLICY_COMPONENT_ID` | This component's ID | `unknown_component` |
| `POLICY_ENABLED` | Enable policy enforcement | `false` |
| `POLICY_CACHE_TTL` | Cache TTL in seconds | `60` |
| `POLICY_CACHE_SIZE` | Maximum cache size | `1000` |
| `POLICY_TIMEOUT` | Request timeout in seconds | `5` |
| `POLICY_FAIL_OPEN` | Allow on policy failures | `true` |

### Using Environment Variables

```python
from policy_client import PolicyClient
from policy_client.config import ClientConfig

# Load from environment
config = ClientConfig.from_env()

client = PolicyClient(
    service_url=config.POLICY_SERVICE_URL,
    component_id=config.COMPONENT_ID,
    enable_policy=config.ENABLE_POLICY,
    cache_ttl=config.CACHE_TTL,
    cache_size=config.CACHE_SIZE,
    timeout=config.TIMEOUT,
    fail_open=config.FAIL_OPEN
)
```

## Features

- **Zero Shared State**: Each component pair has independent policy context
- **Policyless Mode**: Components can run without policy enforcement
- **Local Caching**: Decision caching with TTL to reduce latency
- **Fail-Open**: Policy failures don't block data flow in production
- **Data Transformation**: Automatic field filtering and data transformation

## API Reference

### PolicyClient

Main client class for policy enforcement.

#### Methods

- `check_access(source_id, sink_id, resource, action)` - Check if access is permitted
- `process_data(source_id, sink_id, data, action)` - Check policy and transform data
- `register_component(component_type, role, ...)` - Register component with policy service
- `register_ml_model(model_id, model_name, ...)` - Register ML model
- `clear_cache()` - Clear decision cache

### KafkaPolicyInterceptor

Wraps Kafka Consumer for policy enforcement.

#### Methods

- `__iter__()` - Iterate over messages with policy applied
- `poll(timeout)` - Poll for single message with policy
- `get_stats()` - Get interceptor statistics

### PolicyMiddleware

FastAPI middleware for HTTP policy enforcement.

#### Configuration

- `policy_service_url` - URL of Policy Service
- `component_id` - This component's ID
- `enable_policy` - Enable policy enforcement
- `source_header` - Header name for source component ID (default: "X-Component-ID")
