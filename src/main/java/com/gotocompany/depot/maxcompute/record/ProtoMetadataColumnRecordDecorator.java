package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.message.Message;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProtoMetadataColumnRecordDecorator extends RecordDecorator {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final MaxComputeSchemaCache maxComputeSchemaCache;

    public ProtoMetadataColumnRecordDecorator(RecordDecorator recordDecorator,
                                              MaxComputeSinkConfig maxComputeSinkConfig,
                                              MaxComputeSchemaCache maxComputeSchemaCache) {
        super(recordDecorator);
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.maxComputeSchemaCache = maxComputeSchemaCache;
    }

    @Override
    public void append(Record record, Message message) throws IOException {
        if (StringUtils.isNotBlank(maxComputeSinkConfig.getMaxcomputeMetadataNamespace())) {
            appendNamespacedMetadata(record, message);
            return;
        }
        appendMetadata(record, message);
    }

    private void appendNamespacedMetadata(Record record, Message message) {
        Map<String, Object> metadata = message.getMetadata(maxComputeSinkConfig.getMetadataColumnsTypes());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();
        StructTypeInfo typeInfo = (StructTypeInfo) maxComputeSchema.getMetadataColumns().get(maxComputeSinkConfig.getMaxcomputeMetadataNamespace());
        List<Object> values = IntStream.range(0, typeInfo.getFieldCount())
                .mapToObj(index -> {
                    Object metadataValue = metadata.get(typeInfo.getFieldNames().get(index));
                    if (typeInfo.getFieldTypeInfos().get(index).getOdpsType() == OdpsType.TIMESTAMP) {
                        return new Timestamp((long) metadataValue);
                    }
                    return metadataValue;
                }).collect(Collectors.toList());
        record.set(maxComputeSinkConfig.getMaxcomputeMetadataNamespace(), new SimpleStruct(typeInfo, values));
    }

    private void appendMetadata(Record record, Message message) {
        Map<String, Object> metadata = message.getMetadata(maxComputeSinkConfig.getMetadataColumnsTypes());

        for (Map.Entry<String, TypeInfo> entry : maxComputeSchemaCache.getMaxComputeSchema()
                .getMetadataColumns()
                .entrySet()) {
            Object value = metadata.get(entry.getKey());
            if (entry.getValue().getOdpsType() == OdpsType.TIMESTAMP) {
                record.set(entry.getKey(), new Timestamp((long) value));
            } else {
                record.set(entry.getKey(), value);
            }
        }
    }

}
