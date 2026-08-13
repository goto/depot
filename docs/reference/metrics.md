# Metrics

Depot library has built-in instrumentation with statsd support.
Sinks can have their own metrics, and they will be emmited while using sink connector library into other applications.

## Table of Contents

* [Bigquery Sink](metrics.md#bigquery-sink)
* [Bigtable Sink](metrics.md#bigtable-sink)
* [Redis Sink](metrics.md#redis-sink)
* [Http Sink](metrics.md#http-sink)
* [Kafka Sink](metrics.md#kafka-sink)


## Bigquery Sink

### `Bigquery Operation Total`

Total number of bigquery API operation performed

### `Bigquery Operation Latency`

Time taken for bigquery API operation performed

### `Bigquery Errors Total`

Total numbers of error occurred on bigquery insert operation

## Bigtable Sink

### `Bigtable Operation Total`

Total number of bigtable insert/update operation performed

### `Bigtable Operation Latency`

Time taken for bigtable insert/update operation performed

### `Bigtable Errors Total`

Total numbers of error occurred on bigtable insert/update operation

## Redis Sink

### `Redis Success Response Total`

Total number of successful records pushed to the Redis server

### `Redis No Response Total`

Total number of records which could not be pushed to the Redis server due to broken connection,client timeout,etc.

### `Redis Connection Retry Total`

Total number of attempts to recreate the connection to Redis server from the Jedis client.

## Http Sink

### `Http Response Code Total`

Total count of each response code, i.e the total number of Kafka records under each response code. So in case of batch request mode, each message in the batch will be counted individually.

## Kafka Sink

All Kafka sink metrics are emitted with the prefix `application_sink_kafka_`.

### `Kafka Success Response Total`

`application_sink_kafka_success_response_total`. Total number of records that were successfully produced to the output Kafka topic.

### `Kafka Failure Response Total`

`application_sink_kafka_failure_response_total`. Total number of records that failed to be produced to the output Kafka topic.

### `Kafka Messages Produced Total`

`application_sink_kafka_messages_produced_total`. Total number of records that were acknowledged by the broker per batch, i.e. valid records minus the ones that failed producing.

### `Kafka Produce Batch Size`

`application_sink_kafka_produce_batch_size`. Distribution of the number of valid records handed to the producer per `pushToSink` batch.

### `Kafka Produce Latency`

`application_sink_kafka_produce_latency_milliseconds`. Time taken to produce a batch of records, including the producer flush and acknowledgement resolution.

### `Kafka Errors Total`

`application_sink_kafka_errors_total`. Total number of producer errors, tagged by `error_type` (e.g. `SINK_RETRYABLE_ERROR`, `SINK_NON_RETRYABLE_ERROR`, `SINK_UNKNOWN_ERROR`).

### `Kafka Record Parse Errors Total`

`application_sink_kafka_record_parse_errors_total`. Total number of records that failed during parsing, mapping or serialization, tagged by `error_type` (e.g. `DESERIALIZATION_ERROR`, `INVALID_MESSAGE_ERROR`, `SINK_UNKNOWN_ERROR`).

### `Kafka Schema Update Total`

`application_sink_kafka_schema_update_total`. Total number of proto mapping function rebuilds triggered by Stencil schema refreshes, tagged by `state` (`success` or `failure`).

### `Kafka Large Message Mode`

`application_sink_kafka_large_message_mode`. Gauge indicating whether the sink was initialized with large message mode enabled (`1`) or disabled (`0`).
