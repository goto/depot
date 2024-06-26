# Redis

A Redis sink in Depot requires the following environment variables to be set along with Generic ones

### `SINK_REDIS_URLS`

REDIS server instance hostname/IP address followed by its port.

- Example value: `localhost:6379,localhost:6380`
- Type: `required`

### `SINK_REDIS_DATA_TYPE`

To select whether you want to push your data as a `KEYVALUE`, `HASHSET` or as a `LIST` data type.

- Example value: `Hashset`
- Type: `required`
- Default value: `Hashset`

### `SINK_REDIS_KEY_TEMPLATE`

The string that will act as the key for each Redis entry. This key can be configured as per the requirement, a constant or can extract value from each message and use that as the Redis key.

- Example value: `Service_%s,order_number`

  This will take the value of the proto field `order_number` from the proto and create the Redis keys as per the template.

- Type: `required`

### `SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING`

This is the field that decides what all data will be stored in the HashSet for each message.
- Example value: `{"order_number":"ORDER_NUMBER","event_timestamp":"TIMESTAMP"}`
- Type: `required (For Hashset)`

### `SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME`

This field decides what data will be stored in the value part of key-value pair

- Example value: `customer_id`

  This will get the value of the field with name `customer_id` in your proto and push that to the Redis as value with the corresponding keyTemplate

- Type: `required (For KeyValue)`

### `SINK_REDIS_LIST_DATA_FIELD_NAME`

This field decides what all data will be stored in the List for each message.

- Example value: `customer_id`

  This will get the value of the field with name `customer_id` in your proto and push that to the Redis list with the corresponding keyTemplate

- Type: `required (For List)`

### `SINK_REDIS_TTL_TYPE`

- Example value: `DURATION`
- Type: `optional`
- Default value: `DISABLE`
- Choice of Redis TTL type.It can be:
    - `DURATION`: After which the Key will be expired and removed from Redis \(UNIT- seconds\)
    - `EXACT_TIME`: Precise UNIX timestamp after which the Key will be expired

### `SINK_REDIS_TTL_VALUE`

Redis TTL value in Unix Timestamp for `EXACT_TIME` TTL type, In Seconds for `DURATION` TTL type.

- Example value: `100000`
- Type: `optional`
- Default value: `0`

### `SINK_REDIS_DEPLOYMENT_TYPE`

The Redis deployment you are using. At present, we support `Standalone` and `Cluster` types.

- Example value: `Standalone`
- Type: `required`
- Default value: `Standalone`

### `SINK_REDIS_DEFAULT_FIELD_VALUE_ENABLE`

Defines whether to send the default values  for fields which are not present or null in the input Proto message

* Example value: `false`
* Type: `optional`
* Default value: `true`

### `SINK_REDIS_SOCKET_TIMEOUT_MS`

The max time in milliseconds that the Redis client will wait for response from the Redis server.

- Example value: `4000`
- Type: `optional`
- Default value: `2000`

### `SINK_REDIS_CONNECTION_TIMEOUT_MS`

The max time in milliseconds that the Redis client will wait for establishing connection to the Redis server.

- Example value: `4000`
- Type: `optional`
- Default value: `2000`

### `SINK_REDIS_CONNECTION_RETRY_BACKOFF_MS`

The constant backoff time in milliseconds between subsequent retries to reestablish the Redis connection

- Example value: `4000`
- Type: `optional`
- Default value: `2000`

### `SINK_REDIS_CONNECTION_MAX_RETRIES`

The max no. of retries to reestablish the connection between Redis client and server.

- Example value: `5`
- Type: `optional`
- Default value: `2`

