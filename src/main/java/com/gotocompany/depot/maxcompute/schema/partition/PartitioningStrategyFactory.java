package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import lombok.RequiredArgsConstructor;

import java.util.Set;

/**
 * Factory that selects and constructs the appropriate {@link PartitioningStrategy} for a table based
 * on the sink configuration and the Protobuf descriptor of the partition key.
 *
 * <p>When partitioning is enabled, the factory validates that the partition key exists and that its
 * resolved MaxCompute type is supported, then returns a {@link TimestampPartitioningStrategy} for
 * timestamp keys or a {@link DefaultPartitioningStrategy} for the other supported scalar types.</p>
 */
@RequiredArgsConstructor
public class PartitioningStrategyFactory {

    /**
     * Set of MaxCompute types permitted as partition keys. A partition key whose resolved type is not
     * contained in this set is rejected.
     */
    private static final Set<TypeInfo> ALLOWED_PARTITION_KEY_TYPE_INFO = Sets.newHashSet(
            TypeInfoFactory.TIMESTAMP_NTZ,
            TypeInfoFactory.TIMESTAMP,
            TypeInfoFactory.STRING,
            TypeInfoFactory.TINYINT,
            TypeInfoFactory.SMALLINT,
            TypeInfoFactory.INT,
            TypeInfoFactory.BIGINT
    );

    /**
     * Create a partitioning strategy based on the max compute sink config and the descriptor.
     * Create default partitioning strategy if schema key is non timestamp type.
     * Create timestamp partitioning strategy if schema key is timestamp type.
     *
     * <p>If partitioning is enabled, the configured partition key is looked up in the descriptor and
     * its MaxCompute type is resolved. The type is checked against the set of supported partition
     * types; a timestamp type yields a {@link TimestampPartitioningStrategy}, while any other supported
     * type yields a {@link DefaultPartitioningStrategy}.</p>
     *
     * @param protobufConverterOrchestrator to check the type of the partition key
     * @param maxComputeSinkConfig sink config
     * @param descriptor descriptor of the protobuf message
     * @return partitioning strategy
     * @throws IllegalArgumentException if the partition key is not found in the descriptor or its
     *         resolved type is not a supported partition key type
     */
    public static PartitioningStrategy createPartitioningStrategy(
            ProtobufConverterOrchestrator protobufConverterOrchestrator,
            MaxComputeSinkConfig maxComputeSinkConfig,
            Descriptors.Descriptor descriptor) {
        if (!maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return null;
        }
        String partitionKey = maxComputeSinkConfig.getTablePartitionKey();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor
                .findFieldByName(partitionKey);

        if (fieldDescriptor == null) {
            throw new IllegalArgumentException("Partition key not found in the descriptor: " + partitionKey);
        }
        TypeInfo partitionKeyTypeInfo = protobufConverterOrchestrator.toMaxComputeTypeInfo(new ProtoPayload(fieldDescriptor));
        checkPartitionTypePrecondition(partitionKeyTypeInfo);
        if (TypeInfoFactory.TIMESTAMP_NTZ.equals(partitionKeyTypeInfo) || TypeInfoFactory.TIMESTAMP.equals(partitionKeyTypeInfo)) {
            return new TimestampPartitioningStrategy(maxComputeSinkConfig);
        } else {
            return new DefaultPartitioningStrategy(partitionKeyTypeInfo, maxComputeSinkConfig);
        }
    }

    /**
     * Verifies that the resolved partition key type is one of the supported partition types.
     *
     * @param typeInfo the resolved MaxCompute type of the partition key
     * @throws IllegalArgumentException if {@code typeInfo} is not a supported partition key type
     */
    private static void checkPartitionTypePrecondition(TypeInfo typeInfo) {
        if (!ALLOWED_PARTITION_KEY_TYPE_INFO.contains(typeInfo)) {
            throw new IllegalArgumentException("Partition key type not supported: " + typeInfo.getTypeName());
        }
    }

}
