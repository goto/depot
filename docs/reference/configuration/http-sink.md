# HTTP Sink

An HTTP sink requires the following variables to be set along with Generic ones

## `SINK_HTTPV2_SERVICE_URL`


The HTTP endpoint of the service to which this consumer should PUT/POST/PATCH/DELETE data. This can be configured as per the requirement, a constant or a dynamic one \(which extract given field values from each message and use that as the endpoint\)
If service url is constant, messages will be sent as batches while in case of dynamic one each message will be sent as a separate request \(Since they’d be having different endpoints\).

- Example value: `http://http-service.test.io`
- Example value: `http://http-service.test.io/test-field/%%s,6` This will take the value with index 6 from proto and create the endpoint as per the template
- Type: `required`

## `SINK_HTTPV2_REQUEST_METHOD`

Defines the HTTP verb supported by the endpoint, Supports PUT, POST, PATCH and DELETE verbs as of now.

- Example value: `post`
- Type: `required`
- Default value: `put`

## `SINK_HTTPV2_HEADERS`

Defines the HTTP headers required to push the data to the above URL.

- Example value: `Authorization:auth_token, Accept:text/plain`
- Type: `optional`

## `SINK_HTTPV2_HEADERS_TEMPLATE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the
bigquery table. Bigquery table partitioning config can only be set once, on the table creation and the partitioning
cannot be disabled once created. Changing this value of this config later will cause error when the application trying
to update the bigquery table. Here is further documentation of
bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`

## `SINK_HTTPV2_HEADERS_PARAMETER_SOURCE`

Defines the source from which the fields should be parsed. This field should be present in order to use this feature.

- Example value: `Key`
- Example value: `Message`
- Type: `optional`
- Default value: `None`

## `SINK_HTTPV2_QUERY_TEMPLATE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the
bigquery table. Bigquery table partitioning config can only be set once, on the table creation and the partitioning
cannot be disabled once created. Changing this value of this config later will cause error when the application trying
to update the bigquery table. Here is further documentation of
bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`

## `SINK_HTTPV2_QUERY_PARAMETER_SOURCE`

Defines the source from which the fields should be parsed. This field should be present in order to use this feature.

- Example value: `Key`
- Example value: `Message`
- Type: `optional`
- Default value: `None`

## `SINK_HTTPV2_REQUEST_MODE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the
bigquery table. Bigquery table partitioning config can only be set once, on the table creation and the partitioning
cannot be disabled once created. Changing this value of this config later will cause error when the application trying
to update the bigquery table. Here is further documentation of
bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`


## `SINK_HTTPV2_REQUEST_BODY_MODE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the
bigquery table. Bigquery table partitioning config can only be set once, on the table creation and the partitioning
cannot be disabled once created. Changing this value of this config later will cause error when the application trying
to update the bigquery table. Here is further documentation of
bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`


## `SINK_HTTPV2_REQUEST_LOG_STATUS_CODE_RANGES`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the
bigquery table. Bigquery table partitioning config can only be set once, on the table creation and the partitioning
cannot be disabled once created. Changing this value of this config later will cause error when the application trying
to update the bigquery table. Here is further documentation of
bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`


## `SINK_HTTPV2_RETRY_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which retry will be attempted. Please remove 404 from retry code range in case of HTTP DELETE otherwise it might try to retry to delete already deleted resources.

- Example value: `400-600`
- Type: `optional`
- Default value: `400-600`

## `SINK_HTTPV2_JSON_BODY_TEMPLATE`


Deifnes a template for creating a custom request body from the fields of a protobuf message. This should be a valid JSON itself.

- Example value: `{"test":"$.routes[0]", "$.order_number" : "xxx"}`
- Type: `optional`

## `SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the
bigquery table. Bigquery table partitioning config can only be set once, on the table creation and the partitioning
cannot be disabled once created. Changing this value of this config later will cause error when the application trying
to update the bigquery table. Here is further documentation of
bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`

## `SINK_HTTPV2_DELETE_BODY_ENABLE`

This config if set to true will allow body for the HTTP DELETE method, otherwise no payload will be sent with DELETE request.

- Example value: `false`
- Type: `optional`
- Default value: `true`


## `SINK_HTTPV2_MAX_CONNECTIONS`

Defines the maximum number of HTTP connections.

- Example value: `10`
- Type: `required`
- Default value: `10`

## `SINK_HTTPV2_REQUEST_TIMEOUT_MS`

Defines the connection timeout for the request in millis.

- Example value: `10000`
- Type: `required`
- Default value: `10000`

## `SINK_HTTPV2_OAUTH2_ENABLE`

Enable/Disable OAuth2 support for HTTP sink.

- Example value: `true`
- Type: `optional`
- Default value: `false`

## `SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL`

Defines the OAuth2 Token Endpoint.

- Example value: `https://sample-oauth.my-api.com/oauth2/token`
- Type: `optional`

## `SINK_HTTPV2_OAUTH2_CLIENT_NAME`


Defines the OAuth2 identifier issued to the client.

- Example value: `client-name`
- Type: `optional`


## `SINK_HTTPV2_OAUTH2_CLIENT_SECRET`


Defines the OAuth2 secret issued for the client.

- Example value: `client-secret`
- Type: `optional`


## `SINK_HTTPV2_OAUTH2_SCOPE`


Space-delimited scope overrides. If scope override is not provided, no scopes will be granted to the token.

- Example value: `User:read, sys:info`
- Type: `optional`