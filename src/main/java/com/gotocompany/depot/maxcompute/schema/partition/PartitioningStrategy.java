package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;

/**
 * Interface to define the partitioning strategy for a table.
 *
 * <p>The MaxCompute sink supports more than one partitioning scheme (for example direct partitioning
 * on a value-typed key versus deriving a partition from a timestamp). Implementations encapsulate the
 * differences: the partition column to add to the table schema, whether that column replaces the
 * original field, and how to compute the {@link PartitionSpec} for a given record or value.</p>
 *
 * @see DefaultPartitioningStrategy
 * @see TimestampPartitioningStrategy
 * @see PartitioningStrategyFactory
 */
public interface PartitioningStrategy {

    /**
     * Get the original partition column name, typically the field name in the object.
     * @return the original partition column name.
     */
    String getOriginalPartitionColumnName();

    /**
     * Flag to indicates that the partitioned column should replace the original column in the schema.
     * Currently MaxCompute requires an additional column to be added to the schema for time-partitioning, hence this flag.
     *
     * <p>MaxCompute time-based partitioning requires an extra generated column to be added to the
     * schema (so the original field is retained), whereas value-based partitioning reuses the field
     * itself as the partition column; this flag distinguishes the two cases.</p>
     *
     * @return true if the partitioned column should replace the original column in the schema
     *        false if the partitioned column should be added to the schema
     */
    boolean shouldReplaceOriginalColumn();

    /**
     * Get the partition column.
     * @return the partition column
     */
    Column getPartitionColumn();

    /**
     * Get the partition spec for the object.
     * Method will generate the partition spec based on the partitioning strategy.
     *
     * <p>The supplied object is interpreted according to the concrete strategy: a value-based strategy
     * reads it as the partition value, while a timestamp-based strategy expects the populated record so
     * it can apply its generation expression.</p>
     *
     * @param object the object for which the partition spec is to be generated
     * @return the partition spec
     */
    PartitionSpec getPartitionSpec(Object object);
}
