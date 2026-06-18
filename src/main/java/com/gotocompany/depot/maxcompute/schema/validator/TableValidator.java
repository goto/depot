package com.gotocompany.depot.maxcompute.schema.validator;

import com.aliyun.odps.TableSchema;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Validates a MaxCompute table's name, lifecycle, and schema against the limits configured for the
 * sink before the table is created or updated.
 *
 * <p>The validator checks the table name against a configurable regular expression, ensures the
 * lifecycle (in days) is non-negative, and enforces the configured maximum number of columns and
 * partition keys. All detected problems are accumulated and reported together as a single
 * {@link IllegalArgumentException}.</p>
 */
public class TableValidator {

    /**
     * Sink configuration providing the validation limits (name pattern, column and partition-key
     * maximums).
     */
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    /**
     * Compiled pattern that a valid table name must match, derived from configuration.
     */
    private final Pattern validTableNamePattern;

    /**
     * Creates a validator whose rules are taken from the supplied configuration.
     *
     * <p>The configured table-name regular expression is compiled once for reuse across
     * validations.</p>
     *
     * @param maxComputeSinkConfig the sink configuration providing the table-name pattern and table
     *        limits
     */
    public TableValidator(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.validTableNamePattern = Pattern.compile(maxComputeSinkConfig.getTableValidatorNameRegex());
    }

    /**
     * Validates the given table name, lifecycle, and schema, throwing if any rule is violated.
     *
     * <p>All individual checks are run and their error messages collected; if any errors were
     * recorded, they are joined and thrown together so the caller sees every problem at once.</p>
     *
     * @param tableName the table name to validate
     * @param lifecycleDays the table lifecycle in days, or {@code null} when no lifecycle is set
     * @param tableSchema the table schema to validate
     * @throws IllegalArgumentException if the name, lifecycle, or schema violates any configured rule
     */
    public void validate(String tableName, Long lifecycleDays, TableSchema tableSchema) {
        List<String> errorHolder = new ArrayList<>();
        validateTableName(tableName, errorHolder);
        validateLifecycleDays(lifecycleDays, errorHolder);
        validateTableSchema(tableSchema, errorHolder);
        if (!errorHolder.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errorHolder));
        }
    }

    /**
     * Checks the table name against the configured valid-name pattern, recording an error if it does
     * not match.
     *
     * @param tableName the table name to validate
     * @param errorHolder the mutable list to which a validation message is added on failure
     */
    private void validateTableName(String tableName, List<String> errorHolder) {
        if (!validTableNamePattern.matcher(tableName).matches()) {
            errorHolder.add("Table name should match the pattern: " + validTableNamePattern.pattern());
        }
    }

    /**
     * Checks that the lifecycle, when provided, is not negative, recording an error otherwise.
     *
     * @param lifecycleDays the lifecycle in days, or {@code null} when not set
     * @param errorHolder the mutable list to which a validation message is added on failure
     */
    private void validateLifecycleDays(Long lifecycleDays, List<String> errorHolder) {
        if (Objects.nonNull(lifecycleDays) && lifecycleDays < 0) {
            errorHolder.add("Lifecycle days should be a positive integer");
        }
    }

    /**
     * Checks that the schema does not exceed the configured maximum number of columns and partition
     * keys, recording an error for each limit that is exceeded.
     *
     * @param tableSchema the schema whose column and partition-key counts are checked
     * @param errorHolder the mutable list to which validation messages are added on failure
     */
    private void validateTableSchema(TableSchema tableSchema, List<String> errorHolder) {
        if (tableSchema.getAllColumns().size() > maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable()) {
            errorHolder.add("Table schema should have less or equal than " + maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable() + " columns");
        }
        if (tableSchema.getPartitionColumns().size() > maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable()) {
            errorHolder.add("Table schema should have less or equal than " + maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable() + " partition keys");
        }
    }

}
