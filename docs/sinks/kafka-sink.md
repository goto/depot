# Kafka Sink

Kafka sink produces transformed protobuf records to an output Kafka topic. Implementation lives in `com.gotocompany.depot.kafka` (four classes).

## Required environment variables

| Variable | Description |
|----------|-------------|
| `SINK_TYPE` | `KAFKA` (Firehose) |
| `INPUT_SCHEMA_PROTO_CLASS` | Source proto class for incoming records |
| `SINK_KAFKA_BROKERS` | Output broker list (`host:port`, comma-separated) |
| `SINK_KAFKA_TOPIC` | Output topic name |
| `SINK_KAFKA_PROTO_MESSAGE` | Output message proto FQN |
| `SINK_KAFKA_PROTO_KEY` | Output key proto FQN |
| `SINK_KAFKA_PROTO_MAPPING` | JSON map of output field → CEL expression (source bound as `source`) |
| `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS` | Stencil URL for sink/output descriptors |

## Optional

- `SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE` — sets `max.request.size=20971520`, `compression.type=snappy`
- `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_*` — sink Stencil client tuning (mirror source Stencil vars)
- `SINK_KAFKA_STREAM` — orchestrator-only; resolved to `SINK_KAFKA_BROKERS` by Odin/Entropy
- `SINK_KAFKA_<PRODUCER_PROP>` — passed through to the Kafka producer (e.g. `SINK_KAFKA_LINGER_MS` → `linger.ms`)

## Deployment order

1. Publish **depot** (≥ 0.10.23)
2. Build **firehose** image with updated depot + `SINK_TYPE=KAFKA`
3. Deploy **odin** / **entropy** (broker resolution) and **dex** API
4. Release **datlantis** UI (ODS + GTF plugins)

## Rollback

Revert Firehose deployment to previous image; sink type `KAFKA` is inactive on older images.
