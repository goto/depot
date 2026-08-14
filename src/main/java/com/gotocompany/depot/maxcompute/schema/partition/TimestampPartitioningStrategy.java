package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

/**
 * Partitioning strategy for timestamp partition keys, which derives a string partition column from a
 * timestamp field using a truncation expression.
 *
 * <p>This strategy is selected when the partition key resolves to a MaxCompute timestamp type. Unlike
 * value-based partitioning, the original timestamp field is retained (see
 * {@link #shouldReplaceOriginalColumn()}) and an additional generated {@code STRING} column is added,
 * whose value is produced by truncating the timestamp to the configured time unit via a
 * {@link com.aliyun.odps.expression.TruncTime} expression.</p>
 *
 * @see PartitioningStrategy
 * @see DefaultPartitioningStrategy
 */
public class TimestampPartitioningStrategy implements PartitioningStrategy {

    /**
     * Expression that truncates the timestamp key to the configured time unit to generate the
     * partition value.
     */
    private final GenerateExpression generateExpression;
    /**
     * Name of the generated partition column added to the table schema.
     */
    private final String partitionColumnName;
    /**
     * Name of the source timestamp field used as the partition key.
     */
    private final String partitionColumnKey;
    /**
     * Time unit (for example day or hour) to which the timestamp is truncated when generating the
     * partition value.
     */
    private final String tablePartitionByTimestampTimeUnit;

    /**
     * Creates a timestamp partitioning strategy from the sink configuration.
     *
     * <p>The partition column name, source partition key, and truncation time unit are read from the
     * configuration, and the {@link GenerateExpression} used to derive partition values is
     * initialised.</p>
     *
     * @param maxComputeSinkConfig the sink configuration providing the partition column name, partition
     *        key, and timestamp truncation time unit
     */
    public TimestampPartitioningStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.partitionColumnName = maxComputeSinkConfig.getTablePartitionColumnName();
        this.partitionColumnKey = maxComputeSinkConfig.getTablePartitionKey();
        this.tablePartitionByTimestampTimeUnit = maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit();
        this.generateExpression = initializeGenerateExpression();
    }

    /**
     * Get the original partition column name which is the key in the message payload.
     *
     * @return original partition column name
     */
    @Override
    public String getOriginalPartitionColumnName() {
        return partitionColumnKey;
    }

    /**
     * Timestamp partitioning strategy does not replace the original column.
     * Original timestamp field is retained.
     *
     * @return false
     */
    @Override
    public boolean shouldReplaceOriginalColumn() {
        return false;
    }

    /**
     * Get the partition column.
     *
     * @return partition column
     */
    @Override
    public Column getPartitionColumn() {
        Column column = Column.newBuilder(partitionColumnName, TypeInfoFactory.STRING)
                .build();
        column.setGenerateExpression(new TruncTime(partitionColumnKey, tablePartitionByTimestampTimeUnit));
        return column;
    }

    /**
     * To get the PartitionSpec that uses built in spec generator based on the payload.
     *
     * <p>When the supplied object is a MaxCompute {@link com.aliyun.odps.data.Record}, the generate
     * expression is evaluated against it to produce the partition value; otherwise an empty partition
     * specification is returned.</p>
     *
     * @param object the object for which the partition spec is to be generated
     * @return partition spec
     */
    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        PartitionSpec partitionSpec = new PartitionSpec();
        if (object instanceof Record) {
            Record record = (Record) object;
            partitionSpec.set(partitionColumnName, generateExpression.generate(record));
        }
        return partitionSpec;
    }

    /**
     * Creates the {@link GenerateExpression} that truncates the timestamp key to the configured time
     * unit.
     *
     * @return a {@link com.aliyun.odps.expression.TruncTime} expression over the partition key
     */
    private GenerateExpression initializeGenerateExpression() {
        return new TruncTime(partitionColumnKey, tablePartitionByTimestampTimeUnit);
    }

}
