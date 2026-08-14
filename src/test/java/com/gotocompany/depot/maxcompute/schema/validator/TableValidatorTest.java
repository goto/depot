package com.gotocompany.depot.maxcompute.schema.validator;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TableValidator}.
 *
 * <p>These tests verify that a table's name, lifecycle, and schema are checked against the configured limits. A
 * {@link MaxComputeSinkConfig} mock supplies a name pattern requiring an identifier of up to 30 characters
 * starting with a letter or underscore, a maximum of {@code 1200} columns, and a maximum of {@code 6} partition
 * keys. Each test builds a {@link TableSchema} and invokes
 * {@link TableValidator#validate(String, Long, TableSchema)}, asserting either success or an
 * {@link IllegalArgumentException} via the {@code expected} attribute.</p>
 *
 * @see TableValidator
 */
public class TableValidatorTest {

    /**
     * The validator under test, constructed in {@link #init()} from the mocked validation limits.
     */
    private TableValidator tableValidator;

    /**
     * Builds the validator from a {@link MaxComputeSinkConfig} mock before each test.
     *
     * <p>Stubs the name regular expression (a 1-to-30 character identifier starting with a letter or
     * underscore), a maximum of {@code 1200} columns, and a maximum of {@code 6} partition keys, then
     * constructs the {@link TableValidator}.</p>
     */
    @Before
    public void init() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getTableValidatorNameRegex()).thenReturn("^[a-zA-Z_][a-zA-Z0-9_]{0,29}$");
        when(maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable()).thenReturn(1200);
        when(maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable()).thenReturn(6);
        tableValidator = new TableValidator(maxComputeSinkConfig);
    }

    /**
     * Verifies that a well-formed table and schema pass validation without error.
     *
     * <p>Given a single-column schema and the valid name {@code ValidTableName} with no lifecycle, when
     * {@link TableValidator#validate(String, Long, TableSchema)} is called, then it returns normally,
     * indicating no validation errors were detected.</p>
     */
    @Test
    public void shouldValidateValidTableName() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        tableValidator.validate("ValidTableName", null, tableSchema);
    }

    /**
     * Verifies that a table name violating the configured pattern is rejected.
     *
     * <p>Given the name {@code 1InvalidTableName}, which begins with a digit and therefore fails the name
     * pattern, when {@link TableValidator#validate(String, Long, TableSchema)} is called, then an
     * {@link IllegalArgumentException} is thrown, as asserted by the {@code expected} attribute of the
     * {@link Test} annotation.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateInvalidTableName() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        tableValidator.validate("1InvalidTableName", 30L, tableSchema);
    }

    /**
     * Verifies that a negative lifecycle is rejected.
     *
     * <p>Given a valid name and schema but a lifecycle of {@code -1} days, when
     * {@link TableValidator#validate(String, Long, TableSchema)} is called, then an
     * {@link IllegalArgumentException} is thrown, as asserted by the {@code expected} attribute of the
     * {@link Test} annotation.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateNegativeLifecycleDays() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        tableValidator.validate("ValidTableName", -1L, tableSchema);
    }

    /**
     * Verifies that exceeding the maximum number of columns is rejected.
     *
     * <p>Given a schema with {@code 1201} columns, one more than the configured maximum of {@code 1200}, when
     * {@link TableValidator#validate(String, Long, TableSchema)} is called, then an
     * {@link IllegalArgumentException} is thrown, as asserted by the {@code expected} attribute of the
     * {@link Test} annotation.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateExceedMaxColumns() {
        TableSchema tableSchema = new TableSchema();
        for (int i = 0; i < 1201; i++) {
            tableSchema.addColumn(new Column("column" + i, TypeInfoFactory.STRING));
        }
        tableValidator.validate("ValidTableName", 30L, tableSchema);
    }

    /**
     * Verifies that exceeding the maximum number of partition keys is rejected.
     *
     * <p>Given a schema with {@code 7} partition columns, one more than the configured maximum of {@code 6},
     * when {@link TableValidator#validate(String, Long, TableSchema)} is called, then an
     * {@link IllegalArgumentException} is thrown, as asserted by the {@code expected} attribute of the
     * {@link Test} annotation.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateExceedMaxPartitionKeys() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("column1", TypeInfoFactory.STRING));
        for (int i = 0; i < 7; i++) {
            tableSchema.addPartitionColumn(new Column("partition" + i, TypeInfoFactory.STRING));
        }
        tableValidator.validate("ValidTableName", 30L, tableSchema);
    }

}
