# MaxCompute Sink

A MaxCompute sink requires these configurations to be passed on alongside with generic ones

## SINK_MAXCOMPUTE_ODPS_URL

Contains the URL of the MaxCompute endpoint. Further documentation on MaxCompute [ODPS URL](https://www.alibabacloud.com/help/en/maxcompute/user-guide/endpoints).
* Example value: `http://service.ap-southeast-5.maxcompute.aliyun.com/api`
* Type: `required`

## SINK_MAXCOMPUTE_ACCESS_ID

Contains the access id of the MaxCompute project. Further documentation on MaxCompute [Access ID](https://www.alibabacloud.com/help/en/tablestore/support/obtain-an-accesskey-pair).
* Example value: `access-id`
* Type: `required`

## SINK_MAXCOMPUTE_ACCESS_KEY

Contains the access key of the MaxCompute project. Further documentation on MaxCompute [Access Key](https://www.alibabacloud.com/help/en/tablestore/support/obtain-an-accesskey-pair).
* Example value: `access-key`
* Type: `required`

## SINK_MAXCOMPUTE_PROJECT_ID

Contains the identifier of a MaxCompute project. Further documentation on MaxCompute [Project ID](https://www.alibabacloud.com/help/en/maxcompute/product-overview/project).
* Example value: `project-id`
* Type: `required`

## SINK_MAXCOMPUTE_ADD_METADATA_ENABLED

Configuration for enabling metadata in top of the record. This config will be used for adding metadata information to the record. Metadata information will be added to the record in the form of key-value pair.
* Example value: `false`
* Type: `required`
* Default value: `true`

## SINK_MAXCOMPUTE_METADATA_NAMESPACE

Configuration for wrapping the metadata fields under a specific namespace. This will result in the metadata fields contained in a struct. 
Empty string will result in the metadata fields being added directly to the root level.
* Example value: `__kafka_metadata`
* Type: `optional`

## SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES

Configuration for defining the metadata columns and their types. This config will be used for defining the metadata columns and their types. The format of this config is `column1=type1,column2=type2`.
Supported types are `string`, `integer`, `long`, `timestamp`, `float`, `double`, `boolean`.

* Example value: `topic=string,partition=integer,offset=integer,timestamp=timestamp`
* Type: `optional`

## SINK_MAXCOMPUTE_SCHEMA

Contains the schema of the MaxCompute table. Schema is a dataset grouping of table columns. Further documentation on MaxCompute [Schema](https://www.alibabacloud.com/help/en/maxcompute/user-guide/schemas).
* Example value: `your_dataset_name`
* Type: `required`
* Default value: `default`

## SINK_MAXCOMPUTE_PROTO_TIMESTAMP_TO_MAXCOMPUTE_TYPE

Contains the time unit type for the timestamp field. This config will be used for setting the time unit maxcompute type for the proto timestamp field.
Once table is created with certain timestamp type, it cannot be changed. Changing the timestamp type will result in error. Table needs to be recreated if timestamp type is changed.
Supported values are TIMESTAMP and TIMESTAMP_NTZ.

* Example value: `TIMESTAMP_NTZ`
* Type: `required`
* Default value: `TIMESTAMP`

## SINK_MAXCOMPUTE_TABLE_PARTITIONING_ENABLE

Configuration for enabling partitioning in the MaxCompute table. This config will be used for enabling partitioning in the MaxCompute table.
* Example value: `true`
* Type: `required`
* Default value: `false`

## SINK_MAXCOMPUTE_TABLE_PARTITION_KEY

Contains the partition key of the MaxCompute table. Partition key is referring to the payload field that will be used as partition key in the MaxCompute table.
Supported MaxCompute type for partition key is `string`, `tinyint`, `smallint`, `int`, `bigint`, `timestamp_ntz`.
* Example value: `column1`
* Type: `optional`
* Default value: `default`

## SINK_MAXCOMPUTE_TABLE_PARTITION_BY_TIMESTAMP_TIME_UNIT

Contains the time unit for partitioning by timestamp. This config will be used for setting the time unit for partitioning by timestamp.
Supported time units are `YEAR`, `MONTH`, `DAY`, `HOUR`. Configuration is case-sensitive.

* Example value: `DAYS`
* Type: `required`
* Default value: `DAYS`

## SINK_MAXCOMPUTE_TIMESTAMP_TRUNCATE_MODE

Contains the chrono unit for truncating the timestamp. This config will be used for truncating the timestamp.
Values supported are NANOS, MICROS, MILLIS, SECONDS, MINUTES, HOURS, DAYS, WEEKS, MONTHS, YEARS.

* Example value: `NANOS`
* Type: `required`
* Default value: `MICROS`

## SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME

Contains the partition column name of the MaxCompute table. This could be the same as the partition key or different. This will reflect the column name in the MaxCompute table.
Here the SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME is differentiated with SINK_MAXCOMPUTE_TABLE_PARTITION_KEY to allow the user to have a different column name in the MaxCompute table.
This is used for timestamp auto-partitioning feature where the partition column coexists with the original column.

* Example value: `column1`
* Type: `optional`

## SINK_MAXCOMPUTE_TABLE_NAME

Contains the name of the MaxCompute table. Further documentation on MaxCompute [Table Name](https://www.alibabacloud.com/help/en/maxcompute/user-guide/tables).
* Example value: `table_name`
* Type: `required`

## SINK_MAXCOMPUTE_TABLE_LIFECYCLE_DAYS

Contains the lifecycle of the MaxCompute table. This config will be used for setting the lifecycle of the MaxCompute table.
Not setting this config will result in table with lifecycle. Lifecycle is applied at partition level. Further documentation on MaxCompute [Table Lifecycle](https://www.alibabacloud.com/help/en/maxcompute/product-overview/lifecycle).
* Example value: `30`
* Type: `optional`

## SINK_MAXCOMPUTE_RECORD_PACK_FLUSH_TIMEOUT_MS

Contains the timeout for flushing the record pack in milliseconds. This config will be used for setting the timeout for flushing the record pack. Negative value indicates no timeout.
* Example value: `1000`
* Type: `required`
* Default value: `-1`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ENABLED

Configuration for enabling compression in the streaming insert operation. This config will be used for enabling compression in the streaming insert operation.
* Example value: `false`
* Type: `required`
* Default value: `true`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ALGORITHM

Configuration for defining the compression algorithm in the streaming insert operation. This config will be used for defining the compression algorithm in the streaming insert operation.
Supported values are ODPS_RAW, ODPS_ZLIB, ODPS_LZ4_FRAME, ODPS_ARROW_LZ4_FRAME, ODPS_ARROW_ZSTD
* Example value: `ODPS_ZLIB`
* Type: `required`
* Default value: `ODPS_LZ4_FRAME`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_LEVEL

Configuration for defining the compression level in the streaming insert operation. This config will be used for defining the compression level in the streaming insert operation.
Further documentation on MaxCompute [Compression](https://www.alibabacloud.com/help/en/maxcompute/user-guide/sdk-interfaces#section-cg2-7mb-849).
* Example value: `1`
* Type: `required`
* Default value: `1`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_STRATEGY

Configuration for defining the compression strategy in the streaming insert operation. This config will be used for defining the compression strategy in the streaming insert operation.
Further documentation on MaxCompute [Compression](https://www.alibabacloud.com/help/en/maxcompute/user-guide/sdk-interfaces#section-cg2-7mb-849).

* Example value: `1`
* Type: `required`
* Default value: `0`

## SINK_MAXCOMPUTE_STREAMING_INSERT_MAXIMUM_SESSION_COUNT

Contains the maximum session cached count for the streaming insert operation. This config will be used for setting the maximum session cache capacity for the streaming insert operation.
Least recently used session will be removed if the cache is full.

* Example value: `7`
* Type: `required`
* Default value: `2`

# SINK_MAXCOMPUTE_STREAMING_INSERT_TUNNEL_SLOT_COUNT_PER_SESSION

Contains the slot count per session for the streaming insert operation. This config will be used for setting the slot count per session for the streaming insert operation.
Check the official documentation https://www.alibabacloud.com/help/en/maxcompute/user-guide/overview-of-dts 

* Example value: `2`
* Type: `required`
* Default value: `1`

## SINK_MAXCOMPUTE_ZONE_ID

Contains ZoneID used for parsing the timestamp in the record. This config will be used for parsing the timestamp in the record.

* Example value: `Asia/Bangkok`
* Type: `required`
* Default value: `Asia/Bangkok`

## SINK_MAXCOMPUTE_MAX_DDL_RETRY_COUNT

Contains the maximum retry count for DDL operations. This config will be used for setting the maximum retry count for DDL operations (create and update table schema).

* Example value: `3`
* Type: `required`
* Default value: `3`

## SINK_MAXCOMPUTE_DDL_RETRY_BACKOFF_MILLIS

Contains the backoff time in milliseconds for DDL operations. This config will be used for setting the backoff time in milliseconds for DDL operations (create and update table schema).

* Example value: `10000`
* Type: `required`
* Default value: `1000`

## SINK_MAXCOMPUTE_ODPS_GLOBAL_SETTINGS

Contains the global settings for the MaxCompute sink. This config will be used for setting the global settings for the MaxCompute sink. The format of this config is `key1=value1,key2=value2`.

* Example value: `odps.schema.evolution.enable=true,odps.namespace.schema=true,odps.sql.type.system.odps2=true`
* Type: `optional`
* Default value: `odps.schema.evolution.enable=true,odps.namespace.schema=true`

## SINK_MAXCOMPUTE_TABLE_VALIDATOR_NAME_REGEX

Contains the regex pattern for the table name validation. This config will be used for validating the table name. The table name should match the regex pattern.
Check the official documentation for the [table name](https://www.alibabacloud.com/help/en/maxcompute/product-overview/limits-4#:~:text=A%20table%20can%20contain%20a%20maximum%20of%2060%2C000%20partitions.&text=A%20table%20can%20contain%20a%20maximum%20of%20six%20levels%20of%20partitions.&text=A%20SELECT%20statement%20can%20return%20a%20maximum%20of%2010%2C000%20rows.&text=A%20MULTI%2DINSERT%20statement%20allows,tables%20at%20the%20same%20time.) for more information.

* Example value: `^[a-zA-Z_][a-zA-Z0-9_]*$`
* Type: `required`
* Default value: `^[A-Za-z][A-Za-z0-9_]{0,127}$`

## SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_COLUMNS_PER_TABLE

Contains the maximum number of columns allowed in the table. This config will be used for setting the maximum number of columns allowed in the table.
Check the official documentation for the [table name](https://www.alibabacloud.com/help/en/maxcompute/product-overview/limits-4#:~:text=A%20table%20can%20contain%20a%20maximum%20of%2060%2C000%20partitions.&text=A%20table%20can%20contain%20a%20maximum%20of%20six%20levels%20of%20partitions.&text=A%20SELECT%20statement%20can%20return%20a%20maximum%20of%2010%2C000%20rows.&text=A%20MULTI%2DINSERT%20statement%20allows,tables%20at%20the%20same%20time.) for more information.

* Example value: `1000`
* Type: `required`
* Default value: `1200`

## SINK_MAXCOMPUTE_TABLE_VALIDATOR_MAX_PARTITION_KEYS_PER_TABLE

Contains the maximum number of partition keys allowed in the table. This config will be used for setting the maximum number of partition keys allowed in the table.
Check the official documentation for the [table name](https://www.alibabacloud.com/help/en/maxcompute/product-overview/limits-4#:~:text=A%20table%20can%20contain%20a%20maximum%20of%2060%2C000%20partitions.&text=A%20table%20can%20contain%20a%20maximum%20of%20six%20levels%20of%20partitions.&text=A%20SELECT%20statement%20can%20return%20a%20maximum%20of%2010%2C000%20rows.&text=A%20MULTI%2DINSERT%20statement%20allows,tables%20at%20the%20same%20time.) for more information.

* Example value: `6`
* Type: `required`
* Default value: `6`

## SINK_MAXCOMPUTE_VALID_MIN_TIMESTAMP

Contains the minimum valid timestamp. Records with timestamp field less than this value will be considered as invalid message.
Timestamp should be in the format of `yyyy-MM-ddTHH:mm:ss`.

* Example value: `0`
* Type: `required`
* Default value: `1970-01-01T00:00:00`

## SINK_MAXCOMPUTE_VALID_MAX_TIMESTAMP

Contains the maximum valid timestamp. Records with timestamp field more than this value will be considered as invalid message.
Timestamp should be in the format of `yyyy-MM-ddTHH:mm:ss`.

* Example value: `0`
* Type: `required`
* Default value: `9999-12-31T23:59:59`

## SINK_MAXCOMPUTE_MAX_PAST_EVENT_TIME_DIFFERENCE_YEAR

Contains the maximum past event time difference in years. Records with event time difference more than this value will be considered as invalid message.

* Example value: `1`
* Type: `required`
* Default value: `5`

## SINK_MAXCOMPUTE_MAX_FUTURE_EVENT_TIME_DIFFERENCE_YEAR

Contains the maximum future event time difference in years. Records with event time difference more than this value will be considered as invalid message.

* Example value: `1`
* Type: `required`
* Default value: `1`

## SINK_MAXCOMPUTE_PROTO_INTEGER_TYPES_TO_BIGINT_ENABLED

Configuration for enabling the conversion of proto integer types to bigint. This config will be used for enabling the conversion of all proto integer types to bigint. 
Otherwise proto integer types will be converted to corresponding MaxCompute types ( 32 bit -> INT, 64 bit -> BIGINT).

* Example value: `true`
* Type: `required`
* Default value: `false`

## SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DOUBLE_ENABLED

Configuration for enabling the conversion of proto float types to double. This config will be used for enabling the conversion of proto float types to MaxCompute double ( 64 bit ).
This configuration takes precedence over the SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_ENABLED configuration. 

* Example value: `true`
* Type: `required`
* Default value: `false`

## SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_ENABLED

Configuration for enabling the conversion of proto float types to maxcompute decimal. There are possibilities of precision loss when using plain float type in MaxCompute.
For further information, check the official documentation [here](https://www.alibabacloud.com/help/en/maxcompute/user-guide/maxcompute-v2-0-data-type-edition).

* Example value: `true`
* Type: `required`
* Default value: `false`

## SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_PRECISION

Contains the precision for the conversion of proto float types to maxcompute decimal. This config will be used for setting the precision for the conversion of proto float types to maxcompute decimal.

* Example value: `38`
* Type: `required when SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_ENABLED is true`
* Default value: `38`

## SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_SCALE

Contains the scale for the conversion of proto float types to maxcompute decimal. This config will be used for setting the scale for the conversion of proto float types to maxcompute decimal.

* Example value: `18`
* Type: `required when SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_ENABLED is true`
* Default value: `18`

## SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_ENABLED

Configuration for enabling the conversion of proto double types to maxcompute decimal. There are possibilities of precision loss when using plain double type in MaxCompute.
For further information, check the official documentation [here](https://www.alibabacloud.com/help/en/maxcompute/user-guide/maxcompute-v2-0-data-type-edition).

* Example value: `true`
* Type: `required`
* Default value: `false`

## SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_PRECISION

Contains the precision for the conversion of proto double types to maxcompute decimal. This config will be used for setting the precision for the conversion of proto double types to maxcompute decimal.

* Example value: `38`
* Type: `required when SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_ENABLED is true`
* Default value: `38`

## SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_SCALE

Contains the scale for the conversion of proto double types to maxcompute decimal. This config will be used for setting the scale for the conversion of proto double types to maxcompute decimal.

* Example value: `18`
* Type: `required when SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_ENABLED is true`
* Default value: `18`

## SINK_MAXCOMPUTE_DECIMAL_ROUNDING_MODE

Contains the rounding mode for the conversion of proto float and double types to maxcompute decimal. 
This config will be used for setting the rounding mode for the conversion of proto float and double types to maxcompute decimal.
Supported values are `HALF_UP`, `HALF_DOWN`, `HALF_EVEN`, `UP`, `DOWN`, `CEILING`, `FLOOR`, `UNNECESSARY`.

* Example value: `HALF_UP`
* Type: `required when any of SINK_MAXCOMPUTE_PROTO_FLOAT_TYPE_TO_DECIMAL_ENABLED or SINK_MAXCOMPUTE_PROTO_DOUBLE_TYPE_TO_DECIMAL_ENABLED is true`
* Default value: `UNNECESSARY`

## SINK_MAXCOMPUTE_TABLE_PROPERTIES

Properties for the MaxCompute table. This config will be used for setting the properties for the MaxCompute table. 
The format of this config is `key1=value1,key2=value2,...`. Further documentation on table properties [here](https://www.alibabacloud.com/help/en/maxcompute/user-guide/table-operations).

* Example value: `table.format.version=2`
* Type: `optional`
* Default value: ``

## SINK_MAXCOMPUTE_NANO_HANDLING_ENABLED

This configuration is used to handle nano data values. Its default value is true.

If it is enabled:
1. If nano is less than 0, it will be set to 0.
2. If nano exceeds its range (> 999,999,999), the extra value will be added to the seconds.

If it is disabled and nano is outside the default range, firehose will crash.

* Example value: `true`
* Type: `optional`
* Default value: `true`

## SINK_MAXCOMPUTE_PROTO_MAX_NESTED_MESSAGE_DEPTH

This configuration is used to limit the nested depth of table schema inferred from the proto schema.
Beyond the specified depth, any proto message type will be omitted.
Value set should be more than 0.

* Example value: `15`
* Type: `optional`
* Default value: `15`

## SINK_MAXCOMPUTE_IGNORE_NEGATIVE_SECOND_TIMESTAMP_ENABLED

This configuration is to ignore the negative second timestamp. If it is enabled, the negative second timestamp will be set to null.

* Example value: `false`
* Type: `optional`
* Default value: `true`

## SINK_MAXCOMPUTE_ALLOW_SCHEMA_MISMATCH_ENABLED

This configuration is to set whether schema mismatch restriction is enabled or not for StreamingSessionManager.
Allow schema version mismatch to be inserted if set to true. If set to false, throws SchemaMismatchException when local schema mismatched with metadata server.

* Example value: `true`
* Type: `optional`
* Default value: `false`