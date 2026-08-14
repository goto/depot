package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ReorderableStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.Message;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Decorator to convert protobuf message to maxcompute record.
 * Populates the metadata column based on the depot Message payload.
 *
 * <p>Depot can enrich each row with metadata taken from the Depot {@link Message} (for example
 * timestamps and identifiers). Depending on configuration this decorator writes that metadata in one
 * of two layouts: when a metadata namespace is configured all metadata is nested into a single
 * {@code STRUCT} column of that name; otherwise each metadata entry is written to its own top-level
 * column. Metadata values are coerced to their declared MaxCompute types via {@link MetadataUtil},
 * and the target column types are read from the cached {@link MaxComputeSchema}.</p>
 *
 * <p>This decorator typically wraps a {@link ProtoDataColumnRecordDecorator} so that metadata is
 * added after the data columns have been populated.</p>
 *
 * @see RecordDecorator
 * @see RecordDecoratorFactory
 */
public class ProtoMetadataColumnRecordDecorator extends RecordDecorator {

    /**
     * Cache providing the current {@link MaxComputeSchema}, from which the metadata column types are
     * read.
     */
    private final MaxComputeSchemaCache maxComputeSchemaCache;
    /**
     * Lookup of metadata column name to its declared type name, used to coerce each metadata value.
     */
    private final Map<String, String> metadataTypePairs;
    /**
     * Name of the struct column that holds all metadata when namespaced metadata is enabled; blank
     * when metadata is written as individual top-level columns.
     */
    private final String maxcomputeMetadataNamespace;
    /**
     * Ordered metadata column definitions (name and type) as declared in configuration.
     */
    private final List<TupleString> metadataColumnsTypes;
    /**
     * Helper used to coerce raw metadata values into valid MaxCompute values.
     */
    private final MetadataUtil metadataUtil;

    /**
     * Creates a metadata-column decorator from the sink configuration and supporting collaborators.
     *
     * <p>The constructor pre-computes the metadata name-to-type lookup and caches the configured
     * metadata namespace and metadata column definitions.</p>
     *
     * @param recordDecorator the next decorator in the chain (typically the data-column decorator), or
     *        {@code null} if this decorator terminates the chain
     * @param maxComputeSinkConfig configuration supplying the metadata column definitions and the
     *        optional metadata namespace
     * @param maxComputeSchemaCache cache providing the current table schema used to locate metadata
     *        column types
     * @param metadataUtil helper used to coerce metadata values into valid MaxCompute values
     */
    public ProtoMetadataColumnRecordDecorator(RecordDecorator recordDecorator,
                                              MaxComputeSinkConfig maxComputeSinkConfig,
                                              MaxComputeSchemaCache maxComputeSchemaCache,
                                              MetadataUtil metadataUtil) {
        super(recordDecorator);
        this.maxComputeSchemaCache = maxComputeSchemaCache;
        this.metadataUtil = metadataUtil;
        this.metadataTypePairs = maxComputeSinkConfig.getMetadataColumnsTypes()
                .stream()
                .collect(Collectors.toMap(TupleString::getFirst, TupleString::getSecond));
        this.maxcomputeMetadataNamespace = maxComputeSinkConfig.getMaxcomputeMetadataNamespace();
        this.metadataColumnsTypes = maxComputeSinkConfig.getMetadataColumnsTypes();
    }

    /**
     * Process the record and message to append metadata to the record.
     * Apply namespaced metadata if maxcomputeMetadataNamespace is not empty.
     *
     * <p>If a metadata namespace is configured the metadata is written as a single struct column via
     * {@link #appendNamespacedMetadata(Record, Message)}; otherwise each metadata entry is written to
     * its own column via {@link #appendMetadata(Record, Message)}. A new wrapper is returned that
     * carries the updated record while preserving the original index, error info, and partition
     * spec.</p>
     *
     * @param recordWrapper record to be populated
     * @param message depot message to get the metadata from
     * @return recordWrapper with metadata appended
     * @throws IOException if an error occurs while processing the record, such as descriptor mismatch
     */
    @Override
    public RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException {
        if (StringUtils.isNotBlank(maxcomputeMetadataNamespace)) {
            appendNamespacedMetadata(recordWrapper.getRecord(), message);
        } else {
            appendMetadata(recordWrapper.getRecord(), message);
        }
        return new RecordWrapper(recordWrapper.getRecord(), recordWrapper.getIndex(), recordWrapper.getErrorInfo(), recordWrapper.getPartitionSpec());
    }

    /**
     * Writes all message metadata into a single struct column named by the configured namespace.
     *
     * <p>The struct's field order is taken from the {@link StructTypeInfo} of the namespace column in
     * the cached schema. For each struct field the corresponding metadata value is looked up by field
     * name and coerced to a valid MaxCompute value via {@link MetadataUtil}, and the assembled values
     * are set on the record as a {@link ReorderableStruct}.</p>
     *
     * @param record the MaxCompute record to populate
     * @param message the Depot message supplying the metadata values
     */
    private void appendNamespacedMetadata(Record record, Message message) {
        Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();
        StructTypeInfo typeInfo = (StructTypeInfo) maxComputeSchema.getTableSchema()
                .getColumn(maxcomputeMetadataNamespace)
                .getTypeInfo();
        List<Object> values = IntStream.range(0, typeInfo.getFieldCount())
                .mapToObj(index -> {
                    Object metadataValue = metadata.get(typeInfo.getFieldNames().get(index));
                    return metadataUtil.getValidMetadataValue(metadataTypePairs.get(typeInfo.getFieldNames().get(index)), metadataValue);
                }).collect(Collectors.toList());
        record.set(maxcomputeMetadataNamespace, new ReorderableStruct(typeInfo, values));
    }

    /**
     * Writes each metadata entry to its own top-level column on the record.
     *
     * <p>Iterates the metadata columns declared in the cached {@link MaxComputeSchema}, looks up each
     * value from the message metadata by column name, coerces it to a valid MaxCompute value via
     * {@link MetadataUtil}, and sets it on the record.</p>
     *
     * @param record the MaxCompute record to populate
     * @param message the Depot message supplying the metadata values
     */
    private void appendMetadata(Record record, Message message) {
        Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
        for (Map.Entry<String, TypeInfo> entry : maxComputeSchemaCache.getMaxComputeSchema()
                .getMetadataColumns()
                .entrySet()) {
            Object value = metadata.get(entry.getKey());
            record.set(entry.getKey(), metadataUtil.getValidMetadataValue(metadataTypePairs.get(entry.getKey()), value));
        }
    }

}
