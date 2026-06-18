package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.ConfToListConverter;
import com.gotocompany.depot.config.converter.ConverterUtils;
import com.gotocompany.depot.config.converter.LabelMapConverter;
import com.gotocompany.depot.common.TupleString;

import java.util.List;
import java.util.Map;

/**
 * Owner configuration interface for the Google BigQuery sink.
 *
 * <p>{@code BigQuerySinkConfig} captures everything Depot needs to write to BigQuery: the target
 * project, dataset, and table (with their labels and location), the credentials, partitioning and
 * clustering options, row insert-id and client-timeout settings, partition expiry, metadata columns,
 * and a group of JSON-schema-oriented options (event-timestamp injection, default columns and data
 * types, dynamic schema evolution, and selection of the BigQuery Storage Write API). It extends
 * {@link SinkConfig} so the shared sink and schema settings are also available.
 */
public interface BigQuerySinkConfig extends SinkConfig {

    /**
     * Returns the Google Cloud project ID that owns the target BigQuery dataset.
     *
     * <p>Bound to the {@code SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID} property; has no default.
     *
     * @return the Google Cloud project ID
     */
    @Key("SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID")
    String getGCloudProjectID();

    /**
     * Returns the name of the BigQuery table that records are written to.
     *
     * <p>Bound to the {@code SINK_BIGQUERY_TABLE_NAME} property; has no default.
     *
     * @return the BigQuery table name
     */
    @Key("SINK_BIGQUERY_TABLE_NAME")
    String getTableName();

    /**
     * Returns the labels applied to the BigQuery dataset.
     *
     * <p>Bound to the {@code SINK_BIGQUERY_DATASET_LABELS} property, split on
     * {@link ConverterUtils#ELEMENT_SEPARATOR}, and parsed by {@link LabelMapConverter} into a
     * {@code Map<String, String>} of label key to value. Has no default.
     *
     * @return the dataset labels keyed by label name
     */
    @Key("SINK_BIGQUERY_DATASET_LABELS")
    @Separator(ConverterUtils.ELEMENT_SEPARATOR)
    @ConverterClass(LabelMapConverter.class)
    Map<String, String> getDatasetLabels();

    /**
     * Returns the labels applied to the BigQuery table.
     *
     * <p>Bound to the {@code SINK_BIGQUERY_TABLE_LABELS} property, split on
     * {@link ConverterUtils#ELEMENT_SEPARATOR}, and parsed by {@link LabelMapConverter} into a
     * {@code Map<String, String>} of label key to value. Has no default.
     *
     * @return the table labels keyed by label name
     */
    @Key("SINK_BIGQUERY_TABLE_LABELS")
    @Separator(ConverterUtils.ELEMENT_SEPARATOR)
    @ConverterClass(LabelMapConverter.class)
    Map<String, String> getTableLabels();

    /**
     * Returns the name of the BigQuery dataset that contains the target table.
     *
     * <p>Bound to the {@code SINK_BIGQUERY_DATASET_NAME} property; has no default.
     *
     * @return the BigQuery dataset name
     */
    @Key("SINK_BIGQUERY_DATASET_NAME")
    String getDatasetName();

    /**
     * Returns the filesystem path to the Google Cloud service-account credentials JSON file.
     *
     * <p>The credentials authenticate Depot with BigQuery. Bound to the
     * {@code SINK_BIGQUERY_CREDENTIAL_PATH} property; has no default.
     *
     * @return the path to the service-account credential file
     */
    @Key("SINK_BIGQUERY_CREDENTIAL_PATH")
    String getBigQueryCredentialPath();

    /**
     * Indicates whether the BigQuery table is partitioned.
     *
     * <p>When {@code true}, the table is created or maintained with partitioning on
     * {@link #getTablePartitionKey()}. Bound to the {@code SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE}
     * property; defaults to {@code false}.
     *
     * @return {@code true} if table partitioning is enabled, {@code false} otherwise
     */
    @Key("SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE")
    @DefaultValue("false")
    Boolean isTablePartitioningEnabled();

    /**
     * Returns the column used to partition the BigQuery table.
     *
     * <p>Applies when {@link #isTablePartitioningEnabled()} is {@code true}. Bound to the
     * {@code SINK_BIGQUERY_TABLE_PARTITION_KEY} property; has no default.
     *
     * @return the partitioning column name
     */
    @Key("SINK_BIGQUERY_TABLE_PARTITION_KEY")
    String getTablePartitionKey();

    /**
     * Indicates whether the BigQuery table is clustered.
     *
     * <p>When {@code true}, the table is clustered on {@link #getTableClusteringKeys()}. Bound to the
     * {@code SINK_BIGQUERY_TABLE_CLUSTERING_ENABLE} property; defaults to {@code false}.
     *
     * @return {@code true} if table clustering is enabled, {@code false} otherwise
     */
    @Key("SINK_BIGQUERY_TABLE_CLUSTERING_ENABLE")
    @DefaultValue("false")
    Boolean isTableClusteringEnabled();

    /**
     * Returns the ordered list of columns the BigQuery table is clustered by.
     *
     * <p>Applies when {@link #isTableClusteringEnabled()} is {@code true}. Bound to the
     * {@code SINK_BIGQUERY_TABLE_CLUSTERING_KEYS} property and split on commas; has no default.
     *
     * @return the clustering column names in order
     */
    @Key("SINK_BIGQUERY_TABLE_CLUSTERING_KEYS")
    @Separator(",")
    List<String> getTableClusteringKeys();

    /**
     * Indicates whether a per-row insert ID is sent with each BigQuery streaming insert.
     *
     * <p>Insert IDs enable BigQuery best-effort de-duplication. Bound to the
     * {@code SINK_BIGQUERY_ROW_INSERT_ID_ENABLE} property; defaults to {@code true}.
     *
     * @return {@code true} if row insert IDs are enabled, {@code false} otherwise
     */
    @Key("SINK_BIGQUERY_ROW_INSERT_ID_ENABLE")
    @DefaultValue("true")
    Boolean isRowInsertIdEnabled();

    /**
     * Returns the read timeout, in milliseconds, for the BigQuery client.
     *
     * <p>A value of {@code -1} leaves the client's built-in default timeout in effect. Bound to the
     * {@code SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS} property; defaults to {@code -1}.
     *
     * @return the BigQuery client read timeout in milliseconds, or {@code -1} for the client default
     */
    @Key("SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS")
    @DefaultValue("-1")
    int getBqClientReadTimeoutMS();

    /**
     * Returns the connect timeout, in milliseconds, for the BigQuery client.
     *
     * <p>A value of {@code -1} leaves the client's built-in default timeout in effect. Bound to the
     * {@code SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS} property; defaults to {@code -1}.
     *
     * @return the BigQuery client connect timeout in milliseconds, or {@code -1} for the client
     *         default
     */
    @Key("SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS")
    @DefaultValue("-1")
    int getBqClientConnectTimeoutMS();

    /**
     * Returns the partition expiry, in milliseconds, for partitioned BigQuery tables.
     *
     * <p>A value of {@code -1} disables partition expiry. Bound to the
     * {@code SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS} property; defaults to {@code -1}.
     *
     * @return the partition expiry in milliseconds, or {@code -1} when no expiry is set
     */
    @Key("SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS")
    @DefaultValue("-1")
    Long getBigQueryTablePartitionExpiryMS();

    /**
     * Returns the geographic location in which the BigQuery dataset resides.
     *
     * <p>Bound to the {@code SINK_BIGQUERY_DATASET_LOCATION} property; defaults to
     * {@code asia-southeast1}.
     *
     * @return the BigQuery dataset location
     */
    @Key("SINK_BIGQUERY_DATASET_LOCATION")
    @DefaultValue("asia-southeast1")
    String getBigQueryDatasetLocation();

    /**
     * Returns the namespace under which record metadata columns are grouped.
     *
     * <p>When non-empty, metadata columns are nested within a record column of this name rather than
     * added at the top level. Bound to the {@code SINK_BIGQUERY_METADATA_NAMESPACE} property; defaults
     * to an empty string.
     *
     * @return the metadata namespace, or an empty string for top-level metadata columns
     */
    @Key("SINK_BIGQUERY_METADATA_NAMESPACE")
    @DefaultValue("")
    String getBqMetadataNamespace();

    /**
     * Indicates whether record metadata columns are added to each BigQuery row.
     *
     * <p>When {@code true}, the metadata columns described by {@link #getMetadataColumnsTypes()} are
     * appended to every row. Bound to the {@code SINK_BIGQUERY_ADD_METADATA_ENABLED} property; defaults
     * to {@code true}.
     *
     * @return {@code true} if metadata columns should be added, {@code false} otherwise
     */
    @DefaultValue("true")
    @Key("SINK_BIGQUERY_ADD_METADATA_ENABLED")
    boolean shouldAddMetadata();

    /**
     * Returns the metadata columns to add, each expressed as a name-to-type pair.
     *
     * <p>Bound to the {@code SINK_BIGQUERY_METADATA_COLUMNS_TYPES} property and parsed by
     * {@link ConfToListConverter} (using {@link ConfToListConverter#ELEMENT_SEPARATOR} to split
     * entries) into a list of {@link TupleString} values, where each tuple's first element is the
     * column name and its second element is the BigQuery type. It defaults to an empty string and is
     * honoured only when {@link #shouldAddMetadata()} is {@code true}.
     *
     * @return the list of metadata column name/type pairs
     */
    @DefaultValue("")
    @Key("SINK_BIGQUERY_METADATA_COLUMNS_TYPES")
    @ConverterClass(ConfToListConverter.class)
    @Separator(ConfToListConverter.ELEMENT_SEPARATOR)
    List<TupleString> getMetadataColumnsTypes();

    // Json schema related configs
    /**
     * Indicates whether an event-timestamp column is added to each row.
     *
     * <p>When {@code true}, Depot injects an event timestamp into the row, which is useful for
     * JSON-schema ingestion. Bound to the {@code SINK_BIGQUERY_ADD_EVENT_TIMESTAMP_ENABLE} property;
     * defaults to {@code false}.
     *
     * @return {@code true} if an event timestamp should be added, {@code false} otherwise
     */
    @DefaultValue("false")
    @Key("SINK_BIGQUERY_ADD_EVENT_TIMESTAMP_ENABLE")
    boolean getSinkBigqueryAddEventTimestampEnable();

    /**
     * Returns the default columns to seed the BigQuery schema with, each as a name-to-type pair.
     *
     * <p>Used primarily with JSON schemas to pre-declare columns. Bound to the
     * {@code SINK_BIGQUERY_DEFAULT_COLUMNS} property and parsed by {@link ConfToListConverter} (using
     * {@link ConfToListConverter#ELEMENT_SEPARATOR} to split entries) into a list of
     * {@link TupleString} values, where each tuple pairs a column name with its type. Defaults to an
     * empty string, yielding no default columns.
     *
     * @return the list of default column name/type pairs
     */
    @DefaultValue("")
    @Key("SINK_BIGQUERY_DEFAULT_COLUMNS")
    @ConverterClass(ConfToListConverter.class)
    @Separator(ConfToListConverter.ELEMENT_SEPARATOR)
    List<TupleString> getSinkBigqueryDefaultColumns();

    /**
     * Indicates whether columns of unspecified type default to the BigQuery {@code STRING} type.
     *
     * <p>Applies to JSON-schema ingestion where a field's type is not otherwise determined. Bound to
     * the {@code SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE} property; defaults to {@code true}.
     *
     * @return {@code true} if untyped columns default to {@code STRING}, {@code false} otherwise
     */
    @DefaultValue("true")
    @Key("SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE")
    boolean getSinkBigqueryDefaultDatatypeStringEnable();

    /**
     * Indicates whether the BigQuery table schema is updated dynamically from incoming records.
     *
     * <p>When {@code true}, Depot evolves the table schema as new fields appear. Bound to the
     * {@code SINK_BIGQUERY_DYNAMIC_SCHEMA_ENABLE} property; defaults to {@code true}.
     *
     * @return {@code true} if dynamic schema updates are enabled, {@code false} otherwise
     */
    @DefaultValue("true")
    @Key("SINK_BIGQUERY_DYNAMIC_SCHEMA_ENABLE")
    boolean getSinkBigqueryDynamicSchemaEnable();

    /**
     * Indicates whether the BigQuery Storage Write API is used instead of the legacy streaming API.
     *
     * <p>When {@code true}, records are written through the higher-throughput BigQuery Storage Write
     * API. Bound to the {@code SINK_BIGQUERY_STORAGE_API_ENABLE} property; defaults to {@code false}.
     *
     * @return {@code true} if the BigQuery Storage Write API should be used, {@code false} otherwise
     */
    @DefaultValue("false")
    @Key("SINK_BIGQUERY_STORAGE_API_ENABLE")
    boolean getSinkBigqueryStorageAPIEnable();
}

