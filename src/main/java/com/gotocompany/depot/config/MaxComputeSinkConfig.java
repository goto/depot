package com.gotocompany.depot.config;

import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.converter.ConfToListConverter;
import org.aeonbits.owner.Config;

import java.util.List;

public interface MaxComputeSinkConfig extends Config {
    @Key("SINK_MAXCOMPUTE_ODPS_URL")
    String getMaxComputeOdpsUrl();

    @Key("SINK_MAXCOMPUTE_TUNNEL_URL")
    String getMaxComputeTunnelUrl();

    @Key("SINK_MAXCOMPUTE_ACCESS_ID")
    String getMaxComputeAccessId();

    @Key("SINK_MAXCOMPUTE_ACCESS_KEY")
    String getMaxComputeAccessKey();

    @Key("SINK_MAXCOMPUTE_PROJECT_ID")
    String getMaxComputeProjectId();

    @Key("SINK_MAXCOMPUTE_METADATA_NAMESPACE")
    @DefaultValue("")
    String getMaxcomputeMetadataNamespace();

    @DefaultValue("true")
    @Key("SINK_MAXCOMPUTE_ADD_METADATA_ENABLED")
    boolean shouldAddMetadata();

    @DefaultValue("")
    @Key("SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES")
    @ConverterClass(ConfToListConverter.class)
    @Separator(ConfToListConverter.ELEMENT_SEPARATOR)
    List<TupleString> getMetadataColumnsTypes();

    @Key("SINK_MAXCOMPUTE_SCHEMA")
    @DefaultValue("default")
    String getMaxComputeSchema();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITIONING_ENABLE")
    @DefaultValue("false")
    Boolean isTablePartitioningEnabled();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_KEY")
    String getTablePartitionKey();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_COLUMN_NAME")
    String getTablePartitionColumnName();

    @Key("SINK_MAXCOMPUTE_TABLE_PARTITION_BY_TIMESTAMP_TIMEZONE")
    @DefaultValue("UTC+7")
    String getTablePartitionByTimestampTimezone();

    @Key("SINK_MAX_COMPUTE_TABLE_PARTITION_BY_TIMESTAMP_ZONE_OFFSET")
    @DefaultValue("+07:00")
    String getTablePartitionByTimestampZoneOffset();

    @Key("SINK_MAXCOMPUTE_TABLE_NAME")
    String getMaxComputeTableName();

    @Key("SINK_MAXCOMPUTE_TABLE_LIFECYCLE_DAYS")
    Long getMaxComputeTableLifecycleDays();

}
