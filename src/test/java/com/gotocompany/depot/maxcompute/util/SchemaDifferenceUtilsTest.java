package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.maxcompute.schema.SchemaDifferenceUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaDifferenceUtilsTest {

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
                .withColumn(Column.newBuilder("col2", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BOOLEAN)).build())
                .withColumn(Column.newBuilder("col3", TypeInfoFactory.getStructTypeInfo(Arrays.asList("f1", "f2", "f3"),
                        Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.BIGINT, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING)))).build())
                .withColumn(Column.newBuilder("metadata_2", TypeInfoFactory.STRING).build())
                .withColumn(Column.newBuilder("col4", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(Arrays.asList("f41", "f42"), Arrays.asList(TypeInfoFactory.INT, TypeInfoFactory.getStructTypeInfo(Arrays.asList("f421"), Arrays.asList(TypeInfoFactory.STRING)))))).build())
                .withColumn(Column.newBuilder("col5", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(Arrays.asList("f51"), Arrays.asList(TypeInfoFactory.INT))))
                        .build())
                .withColumn(Column.newBuilder("col6", TypeInfoFactory.getStructTypeInfo(Arrays.asList("f61"), Arrays.asList(TypeInfoFactory.STRING)))
                        .build())
                .build();
        Set<String> expectedMetadataColumns = new HashSet<>(Arrays.asList(
                "ALTER TABLE test_schema.test_table ADD COLUMN IF NOT EXISTS col3.f3 ARRAY<STRING>;",
                "ALTER TABLE test_schema.test_table ADD COLUMN IF NOT EXISTS col4.element.f42 STRUCT<f421:STRING>;",
                "ALTER TABLE test_schema.test_table ADD COLUMN IF NOT EXISTS col5 ARRAY<STRUCT<f51:INT>>;",
                "ALTER TABLE test_schema.test_table ADD COLUMN IF NOT EXISTS col6 STRUCT<f61:STRING>;",
                "ALTER TABLE test_schema.test_table ADD COLUMN IF NOT EXISTS metadata_2 STRING;"
        ));

        Set<String> actualMetadataColumns = new HashSet<>(SchemaDifferenceUtils.getSchemaDifferenceSql(oldTableSchema, newTableSchema, "test_schema", "test_table"));

        assertEquals(actualMetadataColumns.size(), expectedMetadataColumns.size());
        assertTrue(expectedMetadataColumns.containsAll(actualMetadataColumns.stream()
                .map(s -> s.replace("`", ""))
                .collect(Collectors.toSet())));
    }

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
