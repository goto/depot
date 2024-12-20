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

## SINK_MAXCOMPUTE_TIME_UNIT_TYPE

Contains the time unit type for the timestamp field. This config will be used for setting the time unit type for the timestamp field.
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

