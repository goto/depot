package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.maxcompute.schema.SchemaDifferenceUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SchemaDifferenceUtils}.
 *
 * <p>These tests verify the additive schema-difference computation that renders MaxCompute
 * {@code ALTER TABLE ... ADD COLUMN} statements from two {@link TableSchema} instances. Schemas are built with
 * the ODPS {@link TableSchema.Builder}, exercising newly added top-level columns as well as fields added deep
 * inside structs and arrays of structs. The tests also confirm that unsupported in-place primitive type
 * changes are rejected with an {@link UnsupportedOperationException}.</p>
 *
 * @see SchemaDifferenceUtils
 */
public class SchemaDifferenceUtilsTest {

    /**
     * Verifies that the additive difference between two schemas is rendered as the expected set of DDL
     * statements.
     *
     * <p>Given an old schema and a new schema that adds several columns and nested struct fields (including
     * fields inside structs and arrays of structs), when
     * {@link SchemaDifferenceUtils#getSchemaDifferenceSql(TableSchema, TableSchema, String, String)} is called
     * for {@code test_schema.test_table}, then the returned statements equal, in any order, the expected set of
     * {@code alter table ... add column if not exists} statements for each added column and field. Columns whose
     * names differ only in case (for example {@code col2} versus {@code COL2}) are treated as unchanged and
     * produce no statement.</p>
     */
    @Test
    public void testGetSchemaDifferenceDdl() {
        TableSchema oldTableSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("metadata_1", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("col2", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BOOLEAN)).build())
                .withColumn(Column.newBuilder("col3", TypeInfoFactory.getStructTypeInfo(Arrays.asList("f1", "f2"),
                        Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.BIGINT))).build())
                .withColumn(Column.newBuilder("col4", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(Arrays.asList("f41"), Arrays.asList(TypeInfoFactory.INT)))).build())
                .build();
        TableSchema newTableSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("metadata_1", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("COL2", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BOOLEAN)).build())
                .withColumn(Column.newBuilder("col3", TypeInfoFactory.getStructTypeInfo(Arrays.asList("F1", "F2", "f3"),
                        Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.BIGINT, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING)))).build())
                .withColumn(Column.newBuilder("metadata_2", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("col4", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(Arrays.asList("f41", "f42"), Arrays.asList(TypeInfoFactory.INT, TypeInfoFactory.getStructTypeInfo(Arrays.asList("f421"), Arrays.asList(TypeInfoFactory.STRING)))))).build())
                .withColumn(Column.newBuilder("col5", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(Arrays.asList("f51"), Arrays.asList(TypeInfoFactory.INT))))
                        .build())
                .withColumn(Column.newBuilder("col6", TypeInfoFactory.getStructTypeInfo(Arrays.asList("f61"), Arrays.asList(TypeInfoFactory.STRING)))
                        .build())
                .withColumn(Column.newBuilder("col7", TypeInfoFactory.getStructTypeInfo(Arrays.asList("order", "end"), Arrays.asList(
                        TypeInfoFactory.getStructTypeInfo(Arrays.asList("f711", "f712"), Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.INT)),
                        TypeInfoFactory.STRING
                ))).build())
                .build();
        Set<String> expectedMetadataColumns = new HashSet<>(Arrays.asList(
                "alter table test_schema.test_table add column if not exists `col3`.`f3` array<string>;",
                "alter table test_schema.test_table add column if not exists `col4`.element.`f42` struct<`f421`:string>;",
                "alter table test_schema.test_table add column if not exists `col5` array<struct<`f51`:int>>;",
                "alter table test_schema.test_table add column if not exists `col6` struct<`f61`:string>;",
                "alter table test_schema.test_table add column if not exists `metadata_2` string;",
                "alter table test_schema.test_table add column if not exists `col7` struct<`order`:struct<`f711`:string,`f712`:int>,`end`:string>;",
                "alter table test_schema.test_table add column if not exists `col7`.`order`.`f711` string;",
                "alter table test_schema.test_table add column if not exists `col7`.`order`.`f712` int;"
        ));

        Set<String> actualMetadataColumns = new HashSet<>(SchemaDifferenceUtils.getSchemaDifferenceSql(oldTableSchema, newTableSchema, "test_schema", "test_table"));

        assertEquals(actualMetadataColumns.size(), expectedMetadataColumns.size());
        assertTrue(expectedMetadataColumns.containsAll(actualMetadataColumns));
    }

    /**
     * Verifies that changing an existing primitive column's type is rejected.
     *
     * <p>Given an old schema with an {@code INT} column and a new schema that redefines the same column as
     * {@code BIGINT}, when
     * {@link SchemaDifferenceUtils#getSchemaDifferenceSql(TableSchema, TableSchema, String, String)} is called,
     * then an {@link UnsupportedOperationException} is thrown, as asserted by the {@code expected} attribute of
     * the {@link Test} annotation.</p>
     */
    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionWhenChangingPrimitiveType() {
        TableSchema oldSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.INT).build())
                .build();
        TableSchema newSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.BIGINT).build())
                .build();

        SchemaDifferenceUtils.getSchemaDifferenceSql(oldSchema, newSchema, "test_schema", "test_table");
    }

    /**
     * Verifies that changing the element type of a primitive array column is rejected.
     *
     * <p>Given an old schema with an {@code ARRAY<INT>} column and a new schema that redefines it as
     * {@code ARRAY<BIGINT>}, when
     * {@link SchemaDifferenceUtils#getSchemaDifferenceSql(TableSchema, TableSchema, String, String)} is called,
     * then an {@link UnsupportedOperationException} is thrown, as asserted by the {@code expected} attribute of
     * the {@link Test} annotation.</p>
     */
    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionWhenChangingArrayPrimitiveTypeToDifferentArrayType() {
        TableSchema oldSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT)).build())
                .build();
        TableSchema newSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT)).build())
                .build();

        SchemaDifferenceUtils.getSchemaDifferenceSql(oldSchema, newSchema, "test_schema", "test_table");
    }

    /**
     * Verifies that changing a primitive array column into a scalar column is rejected.
     *
     * <p>Given an old schema with an {@code ARRAY<INT>} column and a new schema that redefines the same column
     * as a scalar {@code INT}, when
     * {@link SchemaDifferenceUtils#getSchemaDifferenceSql(TableSchema, TableSchema, String, String)} is called,
     * then an {@link UnsupportedOperationException} is thrown, as asserted by the {@code expected} attribute of
     * the {@link Test} annotation.</p>
     */
    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionWhenChangingArrayPrimitiveTypeToNonArrayType() {
        TableSchema oldSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT)).build())
                .build();
        TableSchema newSchema = TableSchema.builder()
                .withColumn(Column.newBuilder("col1", TypeInfoFactory.INT).build())
                .build();

        SchemaDifferenceUtils.getSchemaDifferenceSql(oldSchema, newSchema, "test_schema", "test_table");
    }

}
