# Kafka Sink

Kafka sink consumes Protobuf messages from an input Kafka topic, maps the required fields from the source Proto schema to an output Proto schema using a configurable proto-to-proto mapping function, and produces the mapped messages to an output Kafka topic.

Kafka sink is created in Depot using `com.gotocompany.depot.kafka.KafkaSinkFactory`.

### Architecture

Following are the components of the Kafka sink:

- `KafkaSinkFactory`: Creates and initializes all the components, i.e. KafkaSink, Stencil clients, KafkaProducer, etc.
- `KafkaSinkConfig`: Loads all the `SINK_KAFKA_*` environment variables required by the sink.
- `KafkaSink`: Implements Depot's `Sink` interface with the `pushToSink(List<Message>)` method.
- `ProtoMessageParser`: Parses the raw `byte[]` of the incoming Kafka message into a `DynamicMessage` using the source Proto schema fetched via the source Stencil client.
- `ProtoMappingFunction`: The core mapping component. Maps source Proto fields to sink Proto fields based on `SINK_KAFKA_PROTO_MAPPING` using CEL (Common Expression Language) expressions.
- `KafkaMessageSerializer`: Serializes the mapped output Proto messages back to `byte[]`.
- `KafkaProducerClient`: Produces the serialized key and value to the output Kafka topic and handles the producer acknowledgements.
- `KafkaTopicCreator`: Ensures the output Kafka topic exists at startup, creating it when missing using the optional topic creation configs.
- `SinkStencilClient`: A separate Stencil client, configured with `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_*` variables, which fetches the sink/output Proto schemas used by the mapping function.
- `KafkaResponseParser`: Collects errors from the producer responses and maps them to Depot's per message `ErrorInfo` in the `SinkResponse`.

### Proto-to-Proto Mapping

The config `SINK_KAFKA_PROTO_MAPPING` should be a JSON object where keys are the field names from the output Proto class, defined by `SINK_KAFKA_PROTO_MESSAGE` or `SINK_KAFKA_PROTO_KEY`, and the corresponding values are CEL expressions which will be evaluated against the incoming source Proto message for producing the value for that output field. The source Proto message is available in the CEL expressions as the variable `source`.

Only the fields which are specified in `SINK_KAFKA_PROTO_MAPPING` are set in the output Proto messages. Rest of the Proto fields will have default values according to their data types. A mapped field which exists in both the key Proto and the value Proto is set on both.

### Supported Transformation Features

- Direct field mapping: `{"user_id": "source.account_go_id"}`
- String concatenation: `{"order_id": "\"wee\" + string(source.order_number)"}`
- Explicit type casting using `string()`, `int()`, `double()` and `bool()` functions: `{"order_id": "string(source.order_number)"}`
- Array index extraction: `{"order_id": "source.order_list[2]"}`
- Nested field access: `{"order_lat": "source.order_location.latitude"}`
- Ternary operator: `{"status": "source.current_status == \"active\" ? \"running\" : \"stopped\""}`
- Message type field mapping, both same type passthrough and different type construction: `{"origin": "com.gojek.esb.SinkLocation{lat: source.pickup_location.latitude, lng: source.pickup_location.longitude}"}`
- Field presence check using the `has()` function: `{"city": "has(source.address) ? source.address.city : \"unknown\""}`
- Static/constant values: `{"service_area_id": "34", "order_labels": "[2343, 4434, 6454]", "is_active": "true"}`

### CEL Expression Compilation and Evaluation

At the startup time, the Kafka sink parses the `SINK_KAFKA_PROTO_MAPPING` JSON config and compiles each CEL expression against a CEL environment built from the source and sink Proto descriptors. Any CEL compilation errors, e.g. invalid expression syntax, unknown field names or type mismatches between the expression result type and the output Proto field type, fail the sink creation at startup itself, before any messages are consumed.

Runtime evaluation errors, e.g. index out of bounds on a repeated field, are reported per message via `SinkResponse` with the error type `INVALID_MESSAGE_ERROR`.

When Stencil cache auto-refresh is enabled, the mapping function gets rebuilt automatically whenever the source or the sink Proto schemas are updated in the schema registry. If a rebuild triggered by a schema refresh fails (for example, an incompatible schema change), the failure is logged, the `application_sink_kafka_schema_update_total` metric is incremented with `state=failure`, and the sink keeps using the previously built good mapping function so that message processing is not interrupted.

### Error Handling

The errors from the Kafka sink are reported in the `SinkResponse` with the following error types:

| Error                                                   | Error Type                |
| ------------------------------------------------------- | ------------------------- |
| Deserialization failures of the incoming message        | `DESERIALIZATION_ERROR`   |
| CEL mapping evaluation failures                         | `INVALID_MESSAGE_ERROR`   |
| Retriable producer errors, e.g. broker timeout          | `SINK_RETRYABLE_ERROR`    |
| Non retriable Kafka errors, e.g. too large records      | `SINK_NON_RETRYABLE_ERROR`|
| Any other unexpected parsing, mapping or producer errors | `SINK_UNKNOWN_ERROR`      |

The errors are correlated back to the originating message by index, so a single batch can contain a mix of successfully produced records, parsing failures and producer failures, each reported against its own message index. Interrupted producer waits preserve the thread interrupt flag and are reported as failures rather than being swallowed.

### Metrics

The Kafka sink emits the following metrics (all prefixed with `application_sink_kafka_`). See the [Metrics reference](../reference/metrics.md#kafka-sink) for descriptions:

- `success_response_total`, `failure_response_total`: per record produce outcomes.
- `messages_produced_total`: records acknowledged by the broker per batch.
- `produce_batch_size`: distribution of the valid batch size pushed to the producer.
- `produce_latency_milliseconds`: time taken to produce a batch.
- `errors_total`: producer errors tagged by `error_type`.
- `record_parse_errors_total`: parsing/mapping/serialization failures tagged by `error_type`.
- `schema_update_total`: proto mapping rebuilds tagged by `state` (`success`/`failure`).
- `large_message_mode`: gauge indicating whether large message mode is enabled.

### Logging

The sink logs the key lifecycle events without logging any sensitive producer configuration (e.g. SASL or bearer token values):

- Sink initialization, the resolved non-sensitive configuration, the proto mapping function build/refresh, producer creation and producer close.
- A per batch summary at `info` level with the counts of valid, parse failed and produce failed records. Per record logging is kept at `debug`/`error` level to avoid noise on the hot path.
