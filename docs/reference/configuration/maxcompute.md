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
* Example value: `true`
* Type: `optional`
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
* Example value: `column1=string,column2=integer,column3=double`
* Type: `required`
* Default value: `default`

## SINK_MAXCOMPUTE_TABLE_PARTITIONING_ENABLE

Configuration for enabling partitioning in the MaxCompute table. This config will be used for enabling partitioning in the MaxCompute table.
* Example value: `true`
* Type: `optional`
* Default value: `false`

## SINK_MAXCOMPUTE_TABLE_PARTITION_KEY

Contains the partition key of the MaxCompute table. Partition key is referring to the payload field that will be used as partition key in the MaxCompute table.
Supported MaxCompute type for partition key is `string`, `tinyint`, `smallint`, `int`, `bigint`, `timestamp_ntz`.
* Example value: `column1`
* Type: `optional`
* Default value: `default`

## SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME

Contains the partition column name of the MaxCompute table. This could be the same as the partition key or different. This will reflect the column name in the MaxCompute table.
* Example value: `column1`
* Type: `required`

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

Contains the timeout for flushing the record pack in milliseconds. This config will be used for setting the timeout for flushing the record pack.
* Example value: `1000`
* Type: `optional`
* Default value: `-1`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ENABLED

Configuration for enabling compression in the streaming insert operation. This config will be used for enabling compression in the streaming insert operation.
* Example value: `true`
* Type: `optional`
* Default value: `false`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_ALGORITHM
Configuration for defining the compression algorithm in the streaming insert operation. This config will be used for defining the compression algorithm in the streaming insert operation.
Supported algorithms are ODPS_RAW, ODPS_ZLIB, ODPS_LZ4_FRAME, ODPS_ARROW_LZ4_FRAME, ODPS_ARROW_ZSTD
* Example value: `ODPS_ZLIB`
* Type: `optional`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_LEVEL
Configuration for defining the compression level in the streaming insert operation. This config will be used for defining the compression level in the streaming insert operation.
* Example value: `1`
* Type: `optional`
* Default value: `1`

## SINK_MAXCOMPUTE_STREAMING_INSERT_COMPRESSION_STRATEGY
Configuration for defining the compression strategy in the streaming insert operation. This config will be used for defining the compression strategy in the streaming insert operation.
* Example value: `1`
* Type: `optional`
* Default value: `0`