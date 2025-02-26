package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.type.TypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultPartitioningStrategy implements PartitioningStrategy {

    private static final String PARTITION_SPEC_FORMAT = "%s=%s";
    private static final String DEFAULT_PARTITION = "__NULL__";

    private final TypeInfo typeInfo;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    /**
     * Get the original partition column name which is the key in the message payload.
     *
     * @return original partition column name
     */
    @Override
    public String getOriginalPartitionColumnName() {
        return maxComputeSinkConfig.getTablePartitionKey();
    }

    /**
     * Default partitioning strategy replaces the original column hence no additional column is added.
     *
     * @return true
     */
    @Override
    public boolean shouldReplaceOriginalColumn() {
        return true;
    }

    /**
     * Get the partition column.
     *
     * @return partition column
     */
    @Override
    public Column getPartitionColumn() {
        return Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), typeInfo)
                .build();
    }

    /**
     * To get the partitionSpec with the format of partitionColumnName=partitionValue.
     *
     * @param object the object for which the partition spec is to be generated
     * @return partition spec
     */
    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        if (object == null) {
            return new PartitionSpec(String.format(PARTITION_SPEC_FORMAT, maxComputeSinkConfig.getTablePartitionColumnName(), DEFAULT_PARTITION));
        }
        return new PartitionSpec(String.format(PARTITION_SPEC_FORMAT, maxComputeSinkConfig.getTablePartitionColumnName(), object));
    }

}
