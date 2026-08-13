# Kafka Sink

A Kafka sink in Depot requires the following environment variables to be set along with the Generic ones.

### `SINK_KAFKA_BROKERS`

The list of ips/dns with port of the Kafka brokers hosting the output Kafka topic. Should be comma separated.

- Example value: `ods-kafka-products-dagstream.p.gojek.com:6668`
- Type: `required`

### `SINK_KAFKA_TOPIC`

The Kafka topic where the output messages will be produced by the Kafka sink. If the topic does not already exist on the Kafka broker, the sink creates it at startup using `SINK_KAFKA_TOPIC_PARTITION_COUNT`, `SINK_KAFKA_TOPIC_REPLICATION_FACTOR` and `SINK_KAFKA_TOPIC_RETENTION_HR`. Those topic creation configs are ignored when the topic already exists.

- Example value: `output-topic`
- Type: `required`

### `SINK_KAFKA_PROTO_MESSAGE`

The fully qualified name of the output Proto class which would define the schema of the value part of the output Kafka messages of the Kafka sink. Kafka sink will set the values of only those fields which are specified in `SINK_KAFKA_PROTO_MAPPING`. Rest of the Proto fields will have default values according to their data types.

- Example value: `gojek.esb.gofood.TalosFeaturesLogMessage`
- Type: `required`

### `SINK_KAFKA_PROTO_KEY`

The fully qualified name of the Protobuf class which would define the schema of the key part of the output Kafka records of the Kafka sink. Kafka sink will set the values of only those fields which are specified in `SINK_KAFKA_PROTO_MAPPING`. Rest of the Proto fields will have default values according to their data types. If this config is not set, the output records are produced with a null key.

- Example value: `gojek.esb.gofood.TalosFeaturesLogKey`
- Type: `optional`

### `SINK_KAFKA_PROTO_MAPPING`

Defines the mapping of the required fields from the source Proto schema class to the sink Proto classes. This map should be a JSON object where keys are fields from the output Proto classes, and their corresponding values are CEL (Common Expression Language) expressions which will get evaluated against the source Proto message for producing the output field value. The source Proto message is available in the CEL expressions as the variable `source`.

- Example value: `{"order_id": "\"wee\" + string(source.order_number)", "user_id": "source.account_go_id", "approval_status": "source.status.last_status", "order_labels": "[2343, 4434, 6454]", "service_area_id": "34"}`
- Type: `required`

### `SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE`

Enable/Disable to produce large messages to Kafka. By default, this configuration uses the default value of the `max.request.size` Kafka config. If set to enable, then Kafka sink will set the `max.request.size=20971520` and `compression.type=snappy`.

- Example value: `true`
- Type: `optional`
- Default value: `false`

### `SINK_KAFKA_ACKS`

The number of acknowledgments the Kafka producer requires the leader to have received before considering a request complete. Maps to the Kafka producer `acks` config.

- Example value: `all`
- Type: `optional`
- Default value: `all`

### `SINK_KAFKA_BATCH_SIZE`

The Kafka producer batch size in bytes. Maps to the Kafka producer `batch.size` config.

- Example value: `16384`
- Type: `optional`
- Default value: `16384`

### `SINK_KAFKA_BUFFER_MEMORY`

The total bytes of memory the Kafka producer can use to buffer records waiting to be sent to the server. Maps to the Kafka producer `buffer.memory` config.

- Example value: `33554432`
- Type: `optional`
- Default value: `33554432`

### `SINK_KAFKA_KEY_SERIALIZER`

The serializer class for Kafka record keys. Maps to the Kafka producer `key.serializer` config.

- Example value: `org.apache.kafka.common.serialization.ByteArraySerializer`
- Type: `optional`
- Default value: `org.apache.kafka.common.serialization.ByteArraySerializer`

### `SINK_KAFKA_LINGER_MS`

The time in milliseconds the Kafka producer waits for additional records before sending a batch. Maps to the Kafka producer `linger.ms` config.

- Example value: `1000`
- Type: `optional`
- Default value: `1000`

### `SINK_KAFKA_RETRIES`

The number of retries the Kafka producer will attempt when a send fails with a potentially transient error. Maps to the Kafka producer `retries` config.

- Example value: `2147483647`
- Type: `optional`
- Default value: `2147483647`

### `SINK_KAFKA_VALUE_SERIALIZER`

The serializer class for Kafka record values. Maps to the Kafka producer `value.serializer` config.

- Example value: `org.apache.kafka.common.serialization.ByteArraySerializer`
- Type: `optional`
- Default value: `org.apache.kafka.common.serialization.ByteArraySerializer`

### `SINK_KAFKA_TOPIC_PARTITION_COUNT`

The number of partitions to use when the sink auto-creates the output Kafka topic. This config is ignored if the topic already exists on the broker.

- Example value: `3`
- Type: `optional`
- Default value: `3`

### `SINK_KAFKA_TOPIC_REPLICATION_FACTOR`

The replication factor to use when the sink auto-creates the output Kafka topic. This config is ignored if the topic already exists on the broker. If unset and the topic does not exist, the Kafka broker default replication factor is used.

- Example value: `2`
- Type: `optional`

### `SINK_KAFKA_TOPIC_RETENTION_HR`

The topic retention period in hours to use when the sink auto-creates the output Kafka topic. Converted to `retention.ms` during topic creation. This config is ignored if the topic already exists on the broker. If unset and the topic does not exist, the Kafka broker default retention is used.

- Example value: `24`
- Type: `optional`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE`

Defines whether to enable the Stencil schema registry for fetching the descriptors of the output Proto classes. If disabled, the output Proto descriptors are loaded from the classpath.

- Example value: `false`
- Type: `optional`
- Default value: `true`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS`

The url of the Stencil server hosting the Proto descriptors of the output Proto classes defined by the environment variables `SINK_KAFKA_PROTO_MESSAGE` and `SINK_KAFKA_PROTO_KEY`. This config is required and must not be empty when `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE` is set to `true`; otherwise the sink fails fast at startup.

- Example value: `http://p-godata-systems-stencil-v1beta1-ingress.golabs.io/v1beta1/namespaces/gojek/schemas/esb-log-entities/versions/1045`
- Type: `required` when `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE` is `true`, otherwise `optional`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH`

Defines whether to enable the auto refresh of the sink Stencil cache. This will allow the schema version of the output Proto classes to be updated automatically.

- Example value: `true`
- Type: `optional`
- Default value: `true`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY`

Defines the schema refresh strategy of the sink Stencil client. Can be either `LONG_POLLING` or `VERSION_BASED_REFRESH`.

- Example value: `VERSION_BASED_REFRESH`
- Type: `optional`
- Default value: `VERSION_BASED_REFRESH`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN`

The bearer token to be passed in the `Authorization` header while fetching the descriptors from the Stencil server.

- Example value: `token-123`
- Type: `optional`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS`

The TTL of the sink Stencil cache in milliseconds.

- Example value: `900000`
- Type: `optional`
- Default value: `900000`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS`

The HTTP timeout in milliseconds while fetching the descriptors from the Stencil server.

- Example value: `10000`
- Type: `optional`
- Default value: `10000`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS`

The minimum backoff in milliseconds between the retries while fetching the descriptors from the Stencil server.

- Example value: `60000`
- Type: `optional`
- Default value: `60000`

### `SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES`

The number of retries while fetching the descriptors from the Stencil server.

- Example value: `4`
- Type: `optional`
- Default value: `4`

### `SINK_KAFKA_(.*)`

Any other environment variables starting with `SINK_KAFKA_` that are not reserved sink, stencil or topic-creation configs will be passed to the Kafka producer after converting the variable name to the corresponding producer property name. This is useful for setting any Kafka producer property that is not available as an explicit configuration. Please refer to the [Kafka producer configs documentation](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html) for all the available Kafka producer properties.

- Example: `SINK_KAFKA_SASL_JAAS_CONFIG`, `SINK_KAFKA_SECURITY_PROTOCOL`, etc
- Type: `optional`
