# HTTP Sink

## Overview
Firehose [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) sink allows users to read data from Kafka and write to an HTTP endpoint. it requires the following [variables](../sinks/http-sink.md#http-sink) to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it.

## Supported methods

Firehose supports `PUT` and `POST` verbs in its HTTP sink. The method can be configured using [`SINK_HTTP_REQUEST_METHOD`](../sinks/http-sink.md#sink_http_request_method).

## Authentication

Firehose HTTP sink supports [OAuth](https://en.wikipedia.org/wiki/OAuth) authentication. OAuth can be enabled for the HTTP sink by setting [`SINK_HTTP_OAUTH2_ENABLE`](../sinks/http-sink.md#sink_http_oauth2_enable)

```text
SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL: https://sample-oauth.my-api.com/oauth2/token  # OAuth2 Token Endpoint.
SINK_HTTPV2_OAUTH2_CLIENT_NAME: client-name  # OAuth2 identifier issued to the client.
SINK_HTTPV2_OAUTH2_CLIENT_SECRET: client-secret # OAuth2 secret issued for the client.
SINK_HTTPV2_OAUTH2_SCOPE: User:read, sys:info  # Space-delimited scope overrides.
```

## Retries

Firehose allows for retrying to sink messages in case of failure of HTTP service. The HTTP error code ranges to retry can be configured with [`SINK_HTTP_RETRY_STATUS_CODE_RANGES`](../sinks/http-sink.md#sink_http_retry_status_code_ranges). HTTP request timeout can be configured with [`SINK_HTTP_REQUEST_TIMEOUT_MS`](../sinks/http-sink.md#sink_http_request_timeout_ms)


## Templating

Firehose HTTP sink supports payload templating using [`SINK_HTTPV2_JSON_BODY_TEMPLATE`](../sinks/http-sink.md#sink_http_json_body_template) configuration. It uses [JsonPath](https://github.com/json-path/JsonPath) for creating Templates which is a DSL for basic JSON parsing. Playground for this: [https://jsonpath.com/](https://jsonpath.com/), where users can play around with a given JSON to extract out the elements as required and validate the `jsonpath`. The template works only when the output data format [`SINK_HTTPV2_DATA_FORMAT`](../sinks/http-sink.md#sink_http_data_format) is JSON.

_**Creating Templates:**_

This is really simple. Find the paths you need to extract using the JSON path. Create a valid JSON template with the static field names + the paths that need to extract. \(Paths name starts with $.\). Firehose will simply replace the paths with the actual data in the path of the message accordingly. Paths can also be used on keys, but be careful that the element in the key must be a string data type.

One sample configuration\(On XYZ proto\) : `{"test":"$.routes[0]", "$.order_number" : "xxx"}` If you want to dump the entire JSON as it is in the backend, use `"$._all_"` as a path.

Limitations:

- Works when the input DATA TYPE is a protobuf, not a JSON.
- Supports only on messages, not keys.
- validation on the level of valid JSON template. But after data has been replaced the resulting string may or may not be a valid JSON. Users must do proper testing/validation from the service side.
- If selecting fields from complex data types like repeated/messages/map of proto, the user must do filtering based first as selecting a field that does not exist would fail.

