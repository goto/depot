package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.gotocompany.depot.maxcompute.model.MaxComputeColumnDetail;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isPrimitiveArrayType;
import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isPrimitiveType;
import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isStructArrayType;
import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isStructType;

/**
 * Utility class to get the schema difference between two {@link TableSchema} objects.
 * This class is deprecated and will be removed in future releases once official support for schema evolution is added.
 *
 * <p>The schemas are flattened into maps of fully-qualified column paths (descending into structs and
 * arrays of structs) and compared. Only newly added columns and struct fields are emitted; a change to
 * the type of an existing primitive (or primitive-array) column is rejected because MaxCompute does not
 * support such in-place type changes. One {@code ADD COLUMN} statement is produced per added column.</p>
 *
 * @deprecated This class is deprecated and will be removed in a future release once official support
 *         for schema evolution is added.
 */
@Deprecated
public class SchemaDifferenceUtils {

    /**
     * Template for the {@code ALTER TABLE ... ADD COLUMN IF NOT EXISTS} statement, formatted with the
     * schema name, table name, and column DDL.
     */
    private static final String ALTER_TABLE_QUERY_TEMPLATE = "ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s;";

    /**
     * Computes the list of {@code ALTER TABLE ... ADD COLUMN} statements needed to bring the old schema
     * up to the new schema.
     *
     * <p>The additive column difference is computed and each added column is rendered into a lower-cased
     * {@code ALTER TABLE} statement targeting the given schema and table.</p>
     *
     * @param oldSchema the existing table schema
     * @param newSchema the desired table schema
     * @param schemaName the MaxCompute schema (namespace) that contains the table
     * @param tableName the name of the table being altered
     * @return the list of DDL statements, one per added column (empty when there are no additions)
     * @throws UnsupportedOperationException if an existing primitive column's type has changed
     */
    public static List<String> getSchemaDifferenceSql(TableSchema oldSchema, TableSchema newSchema, String schemaName, String tableName) {
        List<MaxComputeColumnDetail> maxComputeColumnDetailDifference = getMaxComputeColumnDetailDifference(oldSchema, newSchema, tableName);

        return maxComputeColumnDetailDifference.stream()
                .map(maxComputeColumnDetail -> String.format(ALTER_TABLE_QUERY_TEMPLATE, schemaName, tableName, maxComputeColumnDetail.getDDL()).toLowerCase())
                .collect(Collectors.toList());
    }

    /**
     * Computes the columns present in the new schema but absent from the old one, descending into
     * struct types.
     *
     * <p>Both schemas are flattened into maps keyed by fully-qualified column path. Each new column is
     * compared against the old: if a matching old column exists and is a primitive (or primitive array)
     * whose type changed, an {@link UnsupportedOperationException} is thrown, because MaxCompute does
     * not support changing an existing column's type. Columns missing from the old schema are collected
     * as additions; when an added column is itself a struct (or array of structs), its nested fields are
     * skipped so that only the top-level added column is emitted.</p>
     *
     * @param oldSchema the existing table schema
     * @param newSchema the desired table schema
     * @param tableName the name of the table being altered
     * @return the list of added column details
     * @throws UnsupportedOperationException if an existing primitive column's type has changed
     */
    private static List<MaxComputeColumnDetail> getMaxComputeColumnDetailDifference(TableSchema oldSchema, TableSchema newSchema, String tableName) {
        Map<String, MaxComputeColumnDetail> oldMaxComputeColumnDetail = buildMaxComputeColumnDetailMap(oldSchema);
        Map<String, MaxComputeColumnDetail> newMaxComputeColumnDetail = buildMaxComputeColumnDetailMap(newSchema);
        Iterator<Map.Entry<String, MaxComputeColumnDetail>> newMaxComputeColumnDetailIterator = newMaxComputeColumnDetail.entrySet().iterator();
        List<MaxComputeColumnDetail> changedMetadata = new ArrayList<>();

        while (newMaxComputeColumnDetailIterator.hasNext()) {
            Map.Entry<String, MaxComputeColumnDetail> entry = newMaxComputeColumnDetailIterator.next();
            String columnName = entry.getKey();
            MaxComputeColumnDetail oldMetadata = oldMaxComputeColumnDetail.get(columnName);
            if (!Objects.isNull(oldMetadata) && (isPrimitiveType(oldMetadata.getTypeInfo()) || isPrimitiveArrayType(oldMetadata.getTypeInfo())) && !entry.getValue().getTypeInfo().equals(oldMetadata.getTypeInfo())) {
                throw new UnsupportedOperationException(String.format("Cannot change column type for column %s from %s to %s", columnName, oldMetadata.getTypeInfo(), entry.getValue().getTypeInfo()));
            }
            if (Objects.isNull(oldMetadata)) { //handle new column / struct field
                changedMetadata.add(entry.getValue());
                if (isStructType(entry.getValue().getTypeInfo()) || isStructArrayType(entry.getValue().getTypeInfo())) {
                    skipStructFields(entry, newMaxComputeColumnDetailIterator);
                }
            }
        }
        return changedMetadata;
    }

    /**
     * Advances the iterator past the nested fields of a newly added struct (or array-of-struct) column.
     *
     * <p>Because the flattened map lists each struct field as its own entry, an added struct column is
     * emitted as a single addition and its constituent fields must be skipped to avoid emitting them
     * individually.</p>
     *
     * @param entry the map entry for the added struct (or array-of-struct) column
     * @param newMaxComputeColumnDetailIterator the iterator over the new schema's flattened columns,
     *        advanced past the struct's fields
     */
    private static void skipStructFields(Map.Entry<String, MaxComputeColumnDetail> entry, Iterator<Map.Entry<String, MaxComputeColumnDetail>> newMaxComputeColumnDetailIterator) {
        StructTypeInfo structTypeInfo = isStructType(entry.getValue().getTypeInfo()) ? (StructTypeInfo) entry.getValue().getTypeInfo()
                : ((StructTypeInfo) ((ArrayTypeInfo) entry.getValue().getTypeInfo()).getElementTypeInfo());
        for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
            newMaxComputeColumnDetailIterator.next();
        }
    }

    /**
     * Flattens a table schema into an ordered map from fully-qualified column path to its column detail.
     *
     * <p>Each top-level column is expanded recursively so that nested struct fields appear as their own
     * entries. A {@link TreeMap} is used so the resulting paths are kept in a stable, sorted order.</p>
     *
     * @param schema the table schema to flatten
     * @return a sorted map of column path to {@link MaxComputeColumnDetail}
     */
    private static Map<String, MaxComputeColumnDetail> buildMaxComputeColumnDetailMap(TableSchema schema) {
        Map<String, MaxComputeColumnDetail> maxComputeColumnDetailMap = new TreeMap<>();
        schema.getColumns().forEach(column -> fieldMetadataHelper(column.getTypeInfo(), "", column.getName(), maxComputeColumnDetailMap, false));
        return maxComputeColumnDetailMap;
    }

    /**
     * Recursively records a column (and, for struct types, its nested fields) into the supplied map.
     *
     * <p>Primitive and primitive-array types are recorded as a single entry. Struct and
     * array-of-struct types are recorded as an entry for the struct itself and then expanded, recursing
     * into each struct field with an updated path prefix; array-of-struct fields contribute an
     * {@code .element} segment to the path.</p>
     *
     * @param typeInfo the type of the column or field being recorded
     * @param prefix the path prefix accumulated from enclosing struct(s)
     * @param name the name of the column or field
     * @param result the map into which column details are accumulated
     * @param isArrayElement whether the field is reached as an array element
     */
    private static void fieldMetadataHelper(TypeInfo typeInfo, String prefix, String name, Map<String, MaxComputeColumnDetail> result, boolean isArrayElement) {
        if (isPrimitiveType(typeInfo) || isPrimitiveArrayType(typeInfo)) {
            result.put(getPathName(prefix, name, isArrayElement), new MaxComputeColumnDetail(prefix, name, typeInfo, isArrayElement));
        }
        if (isStructType(typeInfo) || isStructArrayType(typeInfo)) {
            StructTypeInfo structTypeInfo = isStructType(typeInfo) ? (StructTypeInfo) typeInfo : ((StructTypeInfo) ((ArrayTypeInfo) typeInfo).getElementTypeInfo());
            result.put(getPathName(prefix, name, isArrayElement), new MaxComputeColumnDetail(prefix, name, typeInfo, isArrayElement));

            for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
                TypeInfo fieldType = structTypeInfo.getFieldTypeInfos().get(i);
                String fieldName = structTypeInfo.getFieldNames().get(i);
                fieldMetadataHelper(fieldType, getPathName(prefix, name, isArrayElement), fieldName, result, isStructArrayType(typeInfo));
            }
        }
    }

    /**
     * Builds the lower-cased, backtick-quoted path used as the map key for a column or struct field.
     *
     * <p>Top-level columns yield simply the backtick-quoted name; nested fields are prefixed with the
     * enclosing path and, for array elements, an {@code .element} segment.</p>
     *
     * @param prefix the path prefix accumulated from enclosing struct(s)
     * @param name the name of the column or field
     * @param isArrayElement whether the field is reached as an array element
     * @return the lower-cased, backtick-quoted column path
     */
    private static String getPathName(String prefix, String name, boolean isArrayElement) {
        return StringUtils.isBlank(prefix) ? String.format("`%s`", name).toLowerCase() : String.format("%s%s.`%s`", prefix, isArrayElement ? ".element" : "", name).toLowerCase();
    }
}
