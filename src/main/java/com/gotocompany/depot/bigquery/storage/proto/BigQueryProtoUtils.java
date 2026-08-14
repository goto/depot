package com.gotocompany.depot.bigquery.storage.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoFieldFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Utility for injecting message metadata columns into a Protobuf row destined for BigQuery.
 *
 * <p>When metadata enrichment is enabled, Depot can append configured metadata fields (for example
 * the message timestamp, offset or partition) to each row written to BigQuery. This helper reads the
 * configured metadata columns and their declared types and sets the corresponding fields on the
 * {@link DynamicMessage.Builder} that represents the outgoing row, either as top-level columns or
 * nested under a configured namespace field.</p>
 *
 * @see com.gotocompany.depot.config.BigQuerySinkConfig#shouldAddMetadata()
 */
public class BigQueryProtoUtils {

    /**
     * Adds the configured metadata columns to the supplied row builder, if metadata enrichment is
     * enabled.
     *
     * <p>When {@code config.shouldAddMetadata()} is {@code true}, the behaviour depends on the
     * configured metadata namespace:</p>
     * <ul>
     *     <li>If the namespace is empty, the metadata fields are set directly on the row builder as
     *     top-level columns.</li>
     *     <li>Otherwise, the namespace must correspond to a nested message field on the table
     *     descriptor; the metadata fields are set on a sub-builder for that nested message which is
     *     then attached to the row. If the namespace field is absent from the descriptor, no metadata
     *     is added.</li>
     * </ul>
     * If metadata enrichment is disabled this method does nothing.
     *
     * @param metadata        the metadata key/value pairs available for the current message
     * @param messageBuilder  the builder for the outgoing BigQuery row to populate
     * @param tableDescriptor the descriptor of the destination table message, used to resolve fields
     * @param config          the sink configuration providing the metadata toggle, column types and
     *                        namespace
     */
    public static void addMetadata(
            Map<String, Object> metadata,
            DynamicMessage.Builder messageBuilder,
            Descriptors.Descriptor tableDescriptor,
            BigQuerySinkConfig config) {
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            if (config.getBqMetadataNamespace().isEmpty()) {
                setMetadata(metadata, messageBuilder, tableDescriptor, metadataColumnsTypes);
            } else {
                String namespace = config.getBqMetadataNamespace();
                Descriptors.FieldDescriptor metadataFieldDescriptor = tableDescriptor.findFieldByName(namespace);
                if (metadataFieldDescriptor != null) {
                    Descriptors.Descriptor metadataDescriptor = metadataFieldDescriptor.getMessageType();
                    DynamicMessage.Builder metadataBuilder = DynamicMessage.newBuilder(metadataDescriptor);
                    setMetadata(metadata, metadataBuilder, metadataDescriptor, metadataColumnsTypes);
                    messageBuilder.setField(metadataFieldDescriptor, metadataBuilder.build());
                }
            }
        }
    }

    /**
     * Sets each configured metadata column on the given builder, applying timestamp conversion where
     * required.
     *
     * <p>For every configured {@code (name, type)} pair, the matching field on the descriptor is
     * located and, if both the field and a non-null metadata value exist, the value is converted to a
     * Protobuf-compatible value through {@link ProtoFieldFactory}. When the configured type is
     * {@code "timestamp"} and the resolved value is a {@link Long} in milliseconds, it is converted to
     * microseconds as expected by BigQuery before being set.</p>
     *
     * @param metadata            the metadata key/value pairs available for the current message
     * @param messageBuilder      the builder (top-level row or nested namespace message) to populate
     * @param descriptor          the descriptor used to resolve metadata field descriptors by name
     * @param metadataColumnsTypes the ordered list of metadata column names paired with their declared
     *                            types
     */
    private static void setMetadata(Map<String, Object> metadata,
                                    DynamicMessage.Builder messageBuilder,
                                    Descriptors.Descriptor descriptor,
                                    List<TupleString> metadataColumnsTypes) {
        metadataColumnsTypes.forEach(tuple -> {
            String name = tuple.getFirst();
            String type = tuple.getSecond();
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(name);
            if (field != null && metadata.get(name) != null) {
                ProtoField protoField = ProtoFieldFactory.getField(field, metadata.get(name));
                Object fieldValue = protoField.getValue();
                if ("timestamp".equals(type) && fieldValue instanceof Long) {
                    fieldValue = TimeUnit.MILLISECONDS.toMicros((Long) fieldValue);
                }
                messageBuilder.setField(field, fieldValue);
            }
        });
    }
}
